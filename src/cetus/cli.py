"""Command-line interface for Cetus."""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import click
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, TextColumn

from . import __version__
from .client import CetusClient
from .config import Config, get_config_file
from .exceptions import CetusError
from .formatters import get_formatter
from .markers import MarkerStore

console = Console(stderr=True)


def _generate_timestamped_filename(prefix: str, output_format: str) -> Path:
    """Generate a filename with current timestamp.

    Args:
        prefix: File path prefix (can include directory)
        output_format: Format extension (json, jsonl, csv, table)

    Returns:
        Path with timestamp and appropriate extension
    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    # Map format to extension
    ext_map = {"json": "json", "jsonl": "jsonl", "csv": "csv", "table": "txt"}
    ext = ext_map.get(output_format, output_format)
    return Path(f"{prefix}_{timestamp}.{ext}")


def _file_has_content(path: Path) -> bool:
    """Check if file exists and has content."""
    return path.exists() and path.stat().st_size > 0


def _append_jsonl(data: list[dict], output_file: Path) -> int:
    """Append records to a JSONL file."""
    with open(output_file, "a", encoding="utf-8") as f:
        for item in data:
            f.write(json.dumps(item))
            f.write("\n")
    return len(data)


def _append_csv(data: list[dict], output_file: Path) -> int:
    """Append records to a CSV file (without repeating header)."""
    if not data:
        return 0

    # Get fieldnames from existing file or from data
    if _file_has_content(output_file):
        # Read existing header
        with open(output_file, encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            fieldnames = next(reader, None)
        if not fieldnames:
            fieldnames = list(data[0].keys())
        # Append without header
        with open(output_file, "a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            for row in data:
                writer.writerow(row)
    else:
        # New file, write with header
        fieldnames = list(data[0].keys())
        with open(output_file, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in data:
                writer.writerow(row)
    return len(data)


def _append_json(data: list[dict], output_file: Path) -> int:
    """Append records to a JSON array file by rewriting the footer."""
    if not data:
        return 0

    if _file_has_content(output_file):
        # Read existing data, extend, rewrite
        with open(output_file, encoding="utf-8") as f:
            try:
                existing = json.load(f)
                if not isinstance(existing, list):
                    existing = [existing]
            except json.JSONDecodeError:
                existing = []
        existing.extend(data)
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2)
            f.write("\n")
    else:
        # New file
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
            f.write("\n")
    return len(data)


def _append_table(data: list[dict], output_file: Path) -> int:
    """Append records to a table file (rewrites entire file)."""
    if not data and not _file_has_content(output_file):
        return 0

    # Table format requires full rewrite - read existing, merge, rewrite
    # Note: Table format is not ideal for file accumulation
    if _file_has_content(output_file):
        # For table format, we can't easily parse Rich tables back
        # Just warn and overwrite with new data only
        console.print(
            "[yellow]Warning: table format cannot append to existing file. "
            "Use jsonl or csv for incremental queries.[/yellow]"
        )

    # Write new data (or empty table)
    formatter = get_formatter("table")
    with open(output_file, "w", encoding="utf-8") as f:
        formatter.format_stream(data, f)
    return len(data)


def _write_or_append(
    data: list[dict],
    output_file: Path,
    output_format: str,
    is_incremental: bool,
) -> int:
    """Write data to file, appending if incremental mode and file exists.

    Args:
        data: Records to write
        output_file: Target file path
        output_format: Format (json, jsonl, csv, table)
        is_incremental: True if using markers (incremental query mode)

    Returns:
        Number of records written, or -1 if no file was written (incremental, no data)
    """
    # If no data in incremental mode, don't touch the file at all
    # (neither create nor modify)
    if not data and is_incremental:
        return -1

    # If incremental and file exists, append
    if is_incremental and _file_has_content(output_file):
        if output_format == "jsonl":
            return _append_jsonl(data, output_file)
        elif output_format == "csv":
            return _append_csv(data, output_file)
        elif output_format == "json":
            return _append_json(data, output_file)
        elif output_format == "table":
            return _append_table(data, output_file)

    # Fresh query or new file - overwrite
    formatter = get_formatter(output_format)
    # Use newline="" for CSV to let csv module handle line endings
    newline = "" if output_format == "csv" else None
    with open(output_file, "w", encoding="utf-8", newline=newline) as f:
        formatter.format_stream(data, f)
    return len(data)


def _output_formatted_data(
    data: list[dict],
    output_format: str,
    output_file: Path | None,
    item_name: str = "records",
) -> None:
    """Output data in the specified format to file or stdout.

    This is a common helper for commands that output formatted data
    (alerts list, alerts results, etc.).

    Args:
        data: List of dicts to output
        output_format: Format (json, jsonl, csv, table)
        output_file: Optional file path, or None for stdout
        item_name: Name for the items in success message (e.g., "alerts", "results")
    """
    formatter = get_formatter(output_format)

    if output_file:
        newline = "" if output_format == "csv" else None
        with open(output_file, "w", encoding="utf-8", newline=newline) as f:
            formatter.format_stream(data, f)
        console.print(f"[green]Wrote {len(data)} {item_name} to {output_file}[/green]")
    else:
        # Write to stdout with UTF-8 encoding
        if output_format == "table":
            # Table format uses Rich console
            stdout_console = Console(force_terminal=sys.stdout.isatty())
            formatter.format_stream(data, stdout_console.file)
        else:
            newline = "" if output_format == "csv" else None
            stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", newline=newline)
            formatter.format_stream(data, stdout)
            stdout.flush()
            stdout.detach()


def execute_query_and_output(
    ctx: click.Context,
    search: str,
    index: str,
    media: str,
    output_format: str,
    output_file: Path | None,
    since_days: int | None,
    no_marker: bool,
    api_key: str | None,
    host: str | None,
    output_prefix: str | None = None,
) -> None:
    """Common query execution logic used by both 'query' and 'alerts backtest' commands.

    Uses async for responsive Ctrl+C handling.

    Args:
        ctx: Click context
        search: The search query string
        index: Index to search (dns, certstream, alerting)
        media: Storage tier (nvme or all)
        output_format: Output format (json, jsonl, csv, table)
        output_file: Optional file to write output to
        since_days: Days to look back (None uses config default)
        no_marker: If True, don't use or save markers
        api_key: Optional API key override
        host: Optional host override
        output_prefix: Optional prefix for timestamped output files
    """
    from .client import QueryResult

    config = Config.load(api_key=api_key, host=host)
    if since_days is None:
        since_days = config.since_days

    # Validate since_days is non-negative
    if since_days is not None and since_days < 0:
        raise ValueError("since-days cannot be negative")

    # Handle output_prefix: generate timestamped filename
    # output_prefix mode creates new files each run, still uses markers
    use_prefix_mode = output_prefix is not None
    if use_prefix_mode:
        output_file = _generate_timestamped_filename(output_prefix, output_format)

    marker_store = MarkerStore()
    # Use markers in file mode (both -o and --output-prefix), not stdout mode
    # Different modes have separate markers to prevent data gaps
    marker_mode = "prefix" if use_prefix_mode else "file" if output_file else None
    marker = None
    if not no_marker and output_file:
        marker = marker_store.get(search, index, marker_mode)

    # Show marker/file info before query starts
    if use_prefix_mode:
        console.print(f"[dim]Output file: {output_file}[/dim]")
    if marker:
        # Show marker info so user knows we're resuming
        ts_display = marker.last_timestamp[:19]
        console.print(f"[dim]Resuming from: {ts_display}[/dim]")

    formatter = get_formatter(output_format)

    # Progress state shared between callback and display
    progress_state = {"records": 0, "pages": 0, "task_id": None, "progress": None}

    def on_progress(records: int, pages: int) -> None:
        """Update progress display with current record count."""
        progress_state["records"] = records
        progress_state["pages"] = pages
        if progress_state["progress"] and progress_state["task_id"] is not None:
            progress_state["progress"].update(
                progress_state["task_id"],
                description=f"Fetched {records:,} records (page {pages})...",
            )

    async def run_query() -> QueryResult:
        """Async inner function for responsive interrupt handling."""
        client = CetusClient.from_config(config)
        try:
            return await client.query_async(
                search=search,
                index=index,
                media=media,
                since_days=since_days,
                marker=marker,
                progress_callback=on_progress,
            )
        finally:
            client.close()

    # Run with progress indicator
    start_time = time.perf_counter()
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
        transient=True,
    ) as progress:
        task_id = progress.add_task("Querying...", total=None)
        progress_state["progress"] = progress
        progress_state["task_id"] = task_id

        try:
            result = asyncio.run(run_query())
        except KeyboardInterrupt:
            console.print("\n[yellow]Interrupted[/yellow]")
            raise
    elapsed = time.perf_counter() - start_time

    # Output results
    is_incremental = marker is not None
    if output_file:
        # In prefix mode, we always create new files (never append)
        # but still use markers to only fetch new records
        if use_prefix_mode:
            if not result.data:
                # No new records - don't create empty file
                console.print(f"[dim]No new records (no file created) in {elapsed:.2f}s[/dim]")
            else:
                # Write to new timestamped file
                formatter = get_formatter(output_format)
                newline = "" if output_format == "csv" else None
                with open(output_file, "w", encoding="utf-8", newline=newline) as f:
                    formatter.format_stream(result.data, f)
                console.print(
                    f"[green]Wrote {len(result.data)} records to {output_file} "
                    f"in {elapsed:.2f}s[/green]"
                )
        else:
            # Standard -o mode with append support
            file_existed = _file_has_content(output_file)
            records_written = _write_or_append(
                result.data, output_file, output_format, is_incremental
            )
            if records_written == -1:
                # Incremental mode with no new data - no file written/changed
                if file_existed:
                    console.print(f"[dim]No new records (file unchanged) in {elapsed:.2f}s[/dim]")
                else:
                    console.print(
                        f"[dim]No new records since last query (no file written) "
                        f"in {elapsed:.2f}s[/dim]"
                    )
            elif is_incremental and file_existed:
                console.print(
                    f"[green]Appended {records_written} records to {output_file} "
                    f"in {elapsed:.2f}s[/green]"
                )
            else:
                console.print(
                    f"[green]Wrote {records_written} records to {output_file} "
                    f"in {elapsed:.2f}s[/green]"
                )
    else:
        # Write to stdout - use UTF-8 wrapper on Windows (cp1252 default can't handle Unicode)
        # Use newline="" for CSV to let csv module handle line endings, None for others
        newline = "" if output_format == "csv" else None
        stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", newline=newline)
        formatter.format_stream(result.data, stdout)
        stdout.flush()
        stdout.detach()  # Detach so wrapper doesn't close sys.stdout.buffer
        console.print(
            f"\n[dim]{result.total_fetched} records in {elapsed:.2f}s[/dim]", highlight=False
        )

    # Save marker for next incremental query (only in file mode, not stdout)
    if output_file and not no_marker and result.last_uuid and result.last_timestamp:
        marker_store.save(search, index, result.last_timestamp, result.last_uuid, marker_mode)
        if ctx.obj.get("verbose"):
            console.print("[dim]Saved marker for next incremental query[/dim]")


def execute_streaming_query(
    ctx: click.Context,
    search: str,
    index: str,
    media: str,
    output_format: str | None,
    output_file: Path | None,
    since_days: int | None,
    no_marker: bool,
    api_key: str | None,
    host: str | None,
    output_prefix: str | None = None,
) -> None:
    """Execute a streaming query, outputting results as they arrive.

    Uses the async streaming API for responsive Ctrl+C handling.
    Results are written immediately as they're received from the server.

    Args:
        ctx: Click context
        search: The search query string
        index: Index to search (dns, certstream, alerting)
        media: Storage tier (nvme or all)
        output_format: Output format (json, jsonl, csv, table). If None, defaults to jsonl.
        output_file: Optional file to write output to
        since_days: Days to look back (None uses config default)
        no_marker: If True, don't use or save markers
        api_key: Optional API key override
        host: Optional host override
        output_prefix: Optional prefix for timestamped output files
    """
    config = Config.load(api_key=api_key, host=host)
    if since_days is None:
        since_days = config.since_days

    # Validate since_days is non-negative
    if since_days is not None and since_days < 0:
        raise ValueError("since-days cannot be negative")

    # --stream implies jsonl format unless explicitly specified
    if output_format is None:
        output_format = "jsonl"

    # Handle output_prefix: generate timestamped filename
    use_prefix_mode = output_prefix is not None
    if use_prefix_mode:
        output_file = _generate_timestamped_filename(output_prefix, output_format)

    marker_store = MarkerStore()
    # Use markers in file mode (both -o and --output-prefix), not stdout mode
    # Different modes have separate markers to prevent data gaps
    marker_mode = "prefix" if use_prefix_mode else "file" if output_file else None
    marker = None
    if not no_marker and output_file:
        marker = marker_store.get(search, index, marker_mode)
    is_incremental = marker is not None

    # Show marker/file info before query starts
    if use_prefix_mode:
        console.print(f"[dim]Output file: {output_file}[/dim]")
    if marker:
        # Show marker info so user knows we're resuming
        ts_display = marker.last_timestamp[:19]
        console.print(f"[dim]Resuming from: {ts_display}[/dim]")

    timestamp_field = f"{index}_timestamp"

    # Check if file exists before we start (for append detection)
    # In prefix mode, file_existed is always False since we just generated a new filename
    file_existed = (not use_prefix_mode) and output_file and _file_has_content(output_file)

    # Determine if we can truly stream or need to buffer
    # json and table formats require buffering; jsonl and csv can truly stream
    # In prefix mode, we also buffer to avoid creating empty files when there's no data
    needs_buffering = output_format in ("json", "table") or use_prefix_mode

    # Table format requires buffering for column width calculation
    if output_format == "table":
        console.print(
            "[yellow]Warning: --stream with --format table requires buffering. "
            "Use --format csv or jsonl for true streaming.[/yellow]"
        )

    async def stream_results() -> tuple[int, str | None, str | None, bool, list[dict]]:
        """Async inner function for streaming with responsive interrupt handling.

        Returns: (count, last_uuid, last_timestamp, interrupted, buffered_data)
        buffered_data is only populated for json/table formats or when we need to merge.
        """
        count = 0
        last_uuid = None
        last_timestamp = None
        interrupted = False
        buffered_data: list[dict] = []

        client = CetusClient.from_config(config)

        # For formats that need buffering (json, table), we buffer all data
        # jsonl and csv can truly stream, even in append mode
        buffer_all = needs_buffering

        # Set up output destination for streaming formats
        out_file = None
        csv_writer = None
        csv_fieldnames = None

        if not buffer_all:
            if output_file:
                # For incremental jsonl/csv with existing file, use append mode
                if is_incremental and file_existed:
                    if output_format == "csv":
                        # Read existing header for CSV append
                        with open(output_file, encoding="utf-8", newline="") as f:
                            reader = csv.reader(f)
                            csv_fieldnames = next(reader, None)
                    out_file = open(output_file, "a", encoding="utf-8", newline="")
                else:
                    out_file = open(output_file, "w", encoding="utf-8", newline="")
            else:
                out_file = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", newline="")

        try:
            if not buffer_all and output_format == "json":
                # JSON array format - stream but need wrapper (fresh file only)
                out_file.write("[\n")
                first = True
            else:
                first = False

            # Show streaming indicator
            console.print("[dim]Streaming results...[/dim]", highlight=False)

            async for record in client.query_stream_async(
                search=search,
                index=index,
                media=media,
                since_days=since_days,
                marker=marker,
            ):
                count += 1
                last_uuid = record.get("uuid")
                last_timestamp = record.get(timestamp_field)

                if buffer_all:
                    buffered_data.append(record)
                elif output_format == "jsonl":
                    out_file.write(json.dumps(record) + "\n")
                    out_file.flush()
                elif output_format == "json":
                    if not first:
                        out_file.write(",\n")
                    out_file.write("  " + json.dumps(record))
                    first = False
                elif output_format == "csv":
                    # Initialize CSV writer with headers from first record
                    if csv_writer is None:
                        if csv_fieldnames is None:
                            csv_fieldnames = list(record.keys())
                            # Write header only for new files
                            if not (is_incremental and file_existed):
                                temp_writer = csv.DictWriter(
                                    out_file, fieldnames=csv_fieldnames, extrasaction="ignore"
                                )
                                temp_writer.writeheader()
                                out_file.flush()
                        csv_writer = csv.DictWriter(
                            out_file, fieldnames=csv_fieldnames, extrasaction="ignore"
                        )
                    csv_writer.writerow(record)
                    out_file.flush()

            if not buffer_all and output_format == "json":
                out_file.write("\n]\n")

        except asyncio.CancelledError:
            interrupted = True
            console.print("\n[yellow]Interrupted[/yellow]")
        except KeyboardInterrupt:
            interrupted = True
            console.print("\n[yellow]Interrupted[/yellow]")
        finally:
            if out_file is not None:
                if output_file:
                    out_file.close()
                else:
                    out_file.flush()
                    out_file.detach()  # Detach so wrapper doesn't close sys.stdout.buffer
            client.close()

        return count, last_uuid, last_timestamp, interrupted, buffered_data

    # Run the async streaming function
    start_time = time.perf_counter()
    try:
        count, last_uuid, last_timestamp, interrupted, buffered_data = asyncio.run(stream_results())
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted[/yellow]")
        sys.exit(130)
    elapsed = time.perf_counter() - start_time

    # Handle buffered data (for json/table formats)
    if buffered_data and output_file:
        if use_prefix_mode:
            # In prefix mode, always create new file (never append)
            formatter = get_formatter(output_format)
            newline = "" if output_format == "csv" else None
            with open(output_file, "w", encoding="utf-8", newline=newline) as f:
                formatter.format_stream(buffered_data, f)
        else:
            _write_or_append(buffered_data, output_file, output_format, is_incremental)
    elif buffered_data and not output_file:
        # Stdout with buffered format - use UTF-8 wrapper for all formats
        formatter = get_formatter(output_format)
        newline = "" if output_format == "csv" else None
        stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", newline=newline)
        formatter.format_stream(buffered_data, stdout)
        stdout.flush()
        stdout.detach()

    # Clean up empty files created in incremental mode with no results
    # (streaming formats open the file before knowing if there will be data)
    created_empty_file = (
        count == 0
        and is_incremental
        and not file_existed
        and output_file
        and output_file.exists()
        and output_file.stat().st_size == 0
    )
    if created_empty_file:
        output_file.unlink()

    # Report results
    if output_file:
        if use_prefix_mode:
            if count == 0:
                console.print(f"[dim]No new records (no file created) in {elapsed:.2f}s[/dim]")
            else:
                console.print(
                    f"[green]Streamed {count} records to {output_file} in {elapsed:.2f}s[/green]"
                )
        elif count == 0 and is_incremental and not file_existed:
            # First incremental run with no results - no file written
            console.print(
                f"[dim]No new records since last query (no file written) in {elapsed:.2f}s[/dim]"
            )
        elif count == 0 and is_incremental and file_existed:
            console.print(f"[dim]No new records (file unchanged) in {elapsed:.2f}s[/dim]")
        elif is_incremental and file_existed:
            console.print(
                f"[green]Appended {count} records to {output_file} in {elapsed:.2f}s[/green]"
            )
        else:
            console.print(
                f"[green]Streamed {count} records to {output_file} in {elapsed:.2f}s[/green]"
            )
    elif not interrupted:
        console.print(f"\n[dim]Streamed {count} records in {elapsed:.2f}s[/dim]", highlight=False)

    if interrupted:
        sys.exit(130)

    # Save marker for next incremental query (only in file mode, not stdout)
    if output_file and not no_marker and last_uuid and last_timestamp:
        marker_store.save(search, index, last_timestamp, last_uuid, marker_mode)
        if ctx.obj.get("verbose"):
            console.print("[dim]Saved marker for next incremental query[/dim]")


def setup_logging(verbose: bool) -> None:
    """Configure logging based on verbosity."""
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(message)s",
        handlers=[RichHandler(console=console, show_time=False, show_path=False)],
    )


@click.group(invoke_without_command=True)
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose output")
@click.option("--version", is_flag=True, help="Show version and exit")
@click.pass_context
def main(ctx: click.Context, verbose: bool, version: bool) -> None:
    """Cetus - CLI client for the Cetus threat intelligence API.

    Query DNS records, certificate streams, and alerting data from the
    Cetus security platform.

    \b
    Examples:
        cetus query "host:*.example.com"
        cetus query "A:192.168.1.1" --index dns --format table
        cetus config set api-key YOUR_API_KEY
    """
    setup_logging(verbose)
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose

    if version:
        click.echo(f"cetus {__version__}")
        ctx.exit(0)

    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


@main.command()
@click.argument("search")
@click.option(
    "--index",
    "-i",
    type=click.Choice(["dns", "certstream", "alerting"]),
    default="dns",
    help="Index to search (default: dns)",
)
@click.option(
    "--media",
    "-m",
    type=click.Choice(["nvme", "all"]),
    default="nvme",
    help="Storage tier (default: nvme for fast results)",
)
@click.option(
    "--format",
    "-f",
    "output_format",
    type=click.Choice(["json", "jsonl", "csv", "table"]),
    default=None,
    help="Output format (default: json, or jsonl with --stream)",
)
@click.option(
    "--output",
    "-o",
    "output_file",
    type=click.Path(dir_okay=False, writable=True, path_type=Path),
    help="Collector mode: write to file (enables incremental markers)",
)
@click.option(
    "--output-prefix",
    "-p",
    "output_prefix",
    type=str,
    help="Collector mode: timestamped files (e.g., -p results -> results_<timestamp>.jsonl)",
)
@click.option(
    "--since-days",
    "-d",
    type=int,
    default=None,
    help="Look back N days (default: 7, ignored if marker exists)",
)
@click.option(
    "--no-marker",
    is_flag=True,
    help="Ignore existing marker and don't save a new one",
)
@click.option(
    "--stream",
    is_flag=True,
    help="Use streaming mode for faster first results on large queries",
)
@click.option(
    "--api-key",
    envvar="CETUS_API_KEY",
    help="API key (or set CETUS_API_KEY env var)",
)
@click.option(
    "--host",
    envvar="CETUS_HOST",
    help="API host (default: alerting.sparkits.ca)",
)
@click.pass_context
def query(
    ctx: click.Context,
    search: str,
    index: str,
    media: str,
    output_format: str | None,
    output_file: Path | None,
    output_prefix: str | None,
    since_days: int | None,
    no_marker: bool,
    stream: bool,
    api_key: str | None,
    host: str | None,
) -> None:
    """Execute a search query against the Cetus API.

    SEARCH is a Lucene query string. Examples:

    \b
        host:*.example.com          # Wildcard domain match
        A:192.168.1.1               # DNS A record lookup
        host:example.com AND A:*    # Combined conditions

    \b
    OPERATING MODES:
      Direct mode (default): Results to stdout, no state tracking.
      Collector mode (-o/-p): Results to file with incremental markers.

    In collector mode, markers track your position so subsequent runs
    fetch only new records. First run fetches the last 7 days (or
    --since-days). Use --no-marker for a full re-query.

    Use --stream for large queries to see results as they arrive.
    """
    # Validate mutually exclusive options
    if output_file and output_prefix:
        console.print("[red]Error:[/red] --output and --output-prefix are mutually exclusive")
        sys.exit(1)

    try:
        if stream:
            # --stream implies jsonl unless format explicitly specified
            execute_streaming_query(
                ctx=ctx,
                search=search,
                index=index,
                media=media,
                output_format=output_format,  # None defaults to jsonl in execute_streaming_query
                output_file=output_file,
                since_days=since_days,
                no_marker=no_marker,
                api_key=api_key,
                host=host,
                output_prefix=output_prefix,
            )
        else:
            # Default to json for non-streaming
            execute_query_and_output(
                ctx=ctx,
                search=search,
                index=index,
                media=media,
                output_format=output_format or "json",
                output_file=output_file,
                since_days=since_days,
                no_marker=no_marker,
                api_key=api_key,
                host=host,
                output_prefix=output_prefix,
            )
    except ValueError as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)
    except CetusError as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)
    except OSError as e:
        console.print(f"[red]Error:[/red] Cannot write to output file: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted[/yellow]")
        sys.exit(130)


@main.group()
def config() -> None:
    """Manage Cetus configuration."""


@config.command("show")
def config_show() -> None:
    """Display current configuration."""
    try:
        cfg = Config.load()
        console.print("[bold]Current Configuration[/bold]\n")
        for key, value in cfg.as_dict().items():
            console.print(f"  [cyan]{key}:[/cyan] {value}")
        console.print(f"\n[dim]Config file: {get_config_file()}[/dim]")
    except CetusError as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)


@config.command("set")
@click.argument("key", type=click.Choice(["api-key", "host", "timeout", "since-days"]))
@click.argument("value")
def config_set(key: str, value: str) -> None:
    """Set a configuration value.

    \b
    Keys:
        api-key     Your Cetus API key
        host        API hostname (default: alerting.sparkits.ca)
        timeout     Request timeout in seconds (default: 60)
        since-days  Default lookback period in days (default: 7)
    """
    try:
        cfg = Config.load()

        if key == "api-key":
            cfg.api_key = value
        elif key == "host":
            cfg.host = value
        elif key == "timeout":
            cfg.timeout = int(value)
        elif key == "since-days":
            days = int(value)
            if days < 0:
                raise ValueError("since-days cannot be negative")
            cfg.since_days = days

        cfg.save()
        console.print(f"[green]Set {key} successfully[/green]")

    except ValueError as e:
        console.print(f"[red]Invalid value:[/red] {e}")
        sys.exit(1)
    except CetusError as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)


@config.command("path")
def config_path() -> None:
    """Show the path to the config file."""
    click.echo(get_config_file())


@main.group()
def markers() -> None:
    """Manage query markers for incremental updates."""


@markers.command("list")
def markers_list() -> None:
    """List all stored markers."""
    store = MarkerStore()
    all_markers = store.list_all()

    if not all_markers:
        console.print("[dim]No markers stored[/dim]")
        return

    from rich.table import Table

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Index")
    table.add_column("Query")
    table.add_column("Last Timestamp")
    table.add_column("Updated")

    for m in all_markers:
        query_display = m.query if len(m.query) <= 40 else m.query[:37] + "..."
        table.add_row(m.index, query_display, m.last_timestamp, m.updated_at[:19])

    console.print(table)


@markers.command("clear")
@click.option("--index", "-i", help="Only clear markers for this index")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
def markers_clear(index: str | None, yes: bool) -> None:
    """Clear stored markers."""
    store = MarkerStore()

    if not yes:
        target = f"all {index} markers" if index else "all markers"
        if not click.confirm(f"Clear {target}?"):
            console.print("[yellow]Cancelled[/yellow]")
            return

    count = store.clear(index)
    console.print(f"[green]Cleared {count} marker(s)[/green]")


@main.group()
def alerts() -> None:
    """View and manage alert definitions."""


@alerts.command("list")
@click.option("--owned/--no-owned", default=True, help="Include alerts you own (default: yes)")
@click.option("--shared/--no-shared", default=False, help="Include alerts shared with you")
@click.option(
    "--type",
    "-t",
    "alert_type",
    type=click.Choice(["raw", "terms", "structured"]),
    help="Filter by alert type",
)
@click.option(
    "--format",
    "-f",
    "output_format",
    type=click.Choice(["json", "jsonl", "csv", "table"]),
    default="table",
    help="Output format (default: table)",
)
@click.option(
    "--output",
    "-o",
    "output_file",
    type=click.Path(dir_okay=False, writable=True, path_type=Path),
    help="Write output to file instead of stdout",
)
@click.option("--api-key", envvar="CETUS_API_KEY", help="API key")
@click.option("--host", envvar="CETUS_HOST", help="API host")
def alerts_list(
    owned: bool,
    shared: bool,
    alert_type: str | None,
    output_format: str,
    output_file: Path | None,
    api_key: str | None,
    host: str | None,
) -> None:
    """List alert definitions.

    By default, shows alerts you own. Use --shared to include alerts
    shared with you, or --no-owned --shared to see only shared alerts.

    \b
    Examples:
        cetus alerts list                       # Your alerts (table)
        cetus alerts list --shared              # Your alerts + shared
        cetus alerts list --no-owned --shared   # Only shared alerts
        cetus alerts list --type raw            # Only raw query alerts
        cetus alerts list --format json         # JSON output
        cetus alerts list -f csv -o alerts.csv  # Export to CSV
    """
    try:
        config = Config.load(api_key=api_key, host=host)

        if not owned and not shared:
            console.print(
                "[yellow]Warning: Both --no-owned and --no-shared results in no alerts[/yellow]"
            )
            return

        with CetusClient.from_config(config) as client:
            alerts_data = client.list_alerts(owned=owned, shared=shared, alert_type=alert_type)

        if not alerts_data:
            console.print("[dim]No alerts found[/dim]")
            return

        # Convert Alert objects to dicts for output
        alerts_as_dicts = [
            {
                "id": alert.id,
                "type": alert.alert_type,
                "title": alert.title,
                "description": alert.description,
                "owned": alert.owned,
                "shared_by": alert.shared_by,
                "query_preview": alert.query_preview,
            }
            for alert in alerts_data
        ]

        if output_format == "table" and not output_file:
            # Special handling for table to stdout - use Rich table with colors
            from rich.table import Table

            table = Table(show_header=True, header_style="bold cyan")
            table.add_column("ID", style="dim")
            table.add_column("Type")
            table.add_column("Title")
            table.add_column("Description")
            table.add_column("Owner/Shared By")

            type_colors = {"raw": "green", "terms": "blue", "structured": "cyan"}

            for alert in alerts_data:
                type_color = type_colors.get(alert.alert_type, "white")
                owner_col = "You" if alert.owned else f"[dim]{alert.shared_by}[/dim]"
                desc = alert.description
                if len(desc) > 40:
                    desc = desc[:40] + "..."
                table.add_row(
                    str(alert.id),
                    f"[{type_color}]{alert.alert_type}[/{type_color}]",
                    alert.title,
                    desc,
                    owner_col,
                )

            console.print(table)
            console.print(f"\n[dim]Total: {len(alerts_data)} alert(s)[/dim]")
        else:
            # Use common helper for all other cases
            _output_formatted_data(alerts_as_dicts, output_format, output_file, "alerts")

    except CetusError as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)
    except OSError as e:
        console.print(f"[red]Error:[/red] Cannot write to output file: {e}")
        sys.exit(1)


@alerts.command("results")
@click.argument("alert_id", type=int)
@click.option(
    "--since",
    "-s",
    help="Only show results since this timestamp (ISO 8601 format)",
)
@click.option(
    "--format",
    "-f",
    "output_format",
    type=click.Choice(["json", "jsonl", "csv", "table"]),
    default="json",
    help="Output format (default: json)",
)
@click.option(
    "--output",
    "-o",
    "output_file",
    type=click.Path(dir_okay=False, writable=True, path_type=Path),
    help="Write output to file instead of stdout",
)
@click.option("--api-key", envvar="CETUS_API_KEY", help="API key")
@click.option("--host", envvar="CETUS_HOST", help="API host")
def alerts_results(
    alert_id: int,
    since: str | None,
    output_format: str,
    output_file: Path | None,
    api_key: str | None,
    host: str | None,
) -> None:
    """Get results for an alert definition.

    ALERT_ID is the numeric ID of the alert (see 'cetus alerts list').

    \b
    Examples:
        cetus alerts results 123
        cetus alerts results 123 --format table
        cetus alerts results 123 --since 2025-01-01T00:00:00Z
        cetus alerts results 123 -o results.json
    """
    try:
        config = Config.load(api_key=api_key, host=host)

        with CetusClient.from_config(config) as client:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
                transient=True,
            ) as progress:
                progress.add_task("Fetching alert results...", total=None)
                results = client.get_alert_results(alert_id, since=since)

        if not results:
            console.print("[dim]No results found for this alert[/dim]")
            return

        # Output results using common helper
        _output_formatted_data(results, output_format, output_file, "results")

    except CetusError as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)
    except OSError as e:
        console.print(f"[red]Error:[/red] Cannot write to output file: {e}")
        sys.exit(1)


@alerts.command("backtest")
@click.argument("alert_id", type=int)
@click.option(
    "--index",
    "-i",
    type=click.Choice(["dns", "certstream", "alerting"]),
    default="dns",
    help="Index to search (default: dns)",
)
@click.option(
    "--media",
    "-m",
    type=click.Choice(["nvme", "all"]),
    default="nvme",
    help="Storage tier (default: nvme for fast results)",
)
@click.option(
    "--format",
    "-f",
    "output_format",
    type=click.Choice(["json", "jsonl", "csv", "table"]),
    default=None,
    help="Output format (default: json, or jsonl with --stream)",
)
@click.option(
    "--output",
    "-o",
    "output_file",
    type=click.Path(dir_okay=False, writable=True, path_type=Path),
    help="Write output to file instead of stdout",
)
@click.option(
    "--output-prefix",
    "-p",
    "output_prefix",
    type=str,
    help="Timestamped files (e.g., -p results -> results_<timestamp>.jsonl)",
)
@click.option(
    "--since-days",
    "-d",
    type=int,
    default=None,
    help="Look back N days (default: 7, ignored if marker exists)",
)
@click.option(
    "--no-marker",
    is_flag=True,
    help="Ignore existing marker and don't save a new one",
)
@click.option(
    "--stream",
    is_flag=True,
    help="Use streaming mode for faster first results on large queries",
)
@click.option("--api-key", envvar="CETUS_API_KEY", help="API key")
@click.option("--host", envvar="CETUS_HOST", help="API host")
@click.pass_context
def alerts_backtest(
    ctx: click.Context,
    alert_id: int,
    index: str,
    media: str,
    output_format: str | None,
    output_file: Path | None,
    output_prefix: str | None,
    since_days: int | None,
    no_marker: bool,
    stream: bool,
    api_key: str | None,
    host: str | None,
) -> None:
    """Backtest an alert against the full database.

    Fetches the alert's query and runs it against the query endpoint,
    returning matching records from the database. This is useful for
    testing alert definitions against historical data.

    ALERT_ID is the numeric ID of the alert (see 'cetus alerts list').

    \b
    Examples:
        cetus alerts backtest 123
        cetus alerts backtest 123 --index dns
        cetus alerts backtest 123 --format table
        cetus alerts backtest 123 -o results.json --since-days 30
        cetus alerts backtest 123 -p results --since-days 30
        cetus alerts backtest 123 --stream
    """
    # Validate mutually exclusive options
    if output_file and output_prefix:
        console.print("[red]Error:[/red] --output and --output-prefix are mutually exclusive")
        sys.exit(1)

    try:
        config = Config.load(api_key=api_key, host=host)

        # Fetch the alert to get its query
        with CetusClient.from_config(config) as client:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
                transient=True,
            ) as progress:
                progress.add_task("Fetching alert...", total=None)
                alert = client.get_alert(alert_id)

        if not alert:
            console.print(f"[red]Error:[/red] Alert {alert_id} not found")
            sys.exit(1)

        if not alert.query_preview:
            console.print(f"[red]Error:[/red] Alert {alert_id} has no query defined")
            sys.exit(1)

        if ctx.obj.get("verbose"):
            console.print(f"[dim]Backtesting alert: {alert.title}[/dim]")
            console.print(f"[dim]Query: {alert.query_preview}[/dim]")

        # Run the query using the appropriate helper
        if stream:
            # --stream implies jsonl unless format explicitly specified
            execute_streaming_query(
                ctx=ctx,
                search=alert.query_preview,
                index=index,
                media=media,
                output_format=output_format,  # None defaults to jsonl in execute_streaming_query
                output_file=output_file,
                since_days=since_days,
                no_marker=no_marker,
                api_key=api_key,
                host=host,
                output_prefix=output_prefix,
            )
        else:
            # Default to json for non-streaming
            execute_query_and_output(
                ctx=ctx,
                search=alert.query_preview,
                index=index,
                media=media,
                output_format=output_format or "json",
                output_file=output_file,
                since_days=since_days,
                no_marker=no_marker,
                api_key=api_key,
                host=host,
                output_prefix=output_prefix,
            )

    except ValueError as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)
    except CetusError as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)
    except OSError as e:
        console.print(f"[red]Error:[/red] Cannot write to output file: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted[/yellow]")
        sys.exit(130)


@main.group()
def completion() -> None:
    """Generate shell completion scripts.

    Shell completion provides tab-completion for commands, options,
    and arguments. After generating a script, follow the instructions
    to install it for your shell.
    """


@completion.command("bash")
def completion_bash() -> None:
    """Generate bash completion script.

    \b
    To install, add this to ~/.bashrc:
        eval "$(_CETUS_COMPLETE=bash_source cetus)"

    Or save to a file:
        cetus completion bash > ~/.local/share/bash-completion/completions/cetus
    """
    import subprocess

    result = subprocess.run(
        ["cetus"],
        env={**dict(__import__("os").environ), "_CETUS_COMPLETE": "bash_source"},
        capture_output=True,
        text=True,
    )
    click.echo(result.stdout)


@completion.command("zsh")
def completion_zsh() -> None:
    """Generate zsh completion script.

    \b
    To install, add this to ~/.zshrc:
        eval "$(_CETUS_COMPLETE=zsh_source cetus)"

    Or save to a file:
        cetus completion zsh > ~/.zfunc/_cetus
    """
    import subprocess

    result = subprocess.run(
        ["cetus"],
        env={**dict(__import__("os").environ), "_CETUS_COMPLETE": "zsh_source"},
        capture_output=True,
        text=True,
    )
    click.echo(result.stdout)


@completion.command("fish")
def completion_fish() -> None:
    """Generate fish completion script.

    \b
    To install:
        cetus completion fish > ~/.config/fish/completions/cetus.fish
    """
    import subprocess

    result = subprocess.run(
        ["cetus"],
        env={**dict(__import__("os").environ), "_CETUS_COMPLETE": "fish_source"},
        capture_output=True,
        text=True,
    )
    click.echo(result.stdout)


if __name__ == "__main__":
    main()
