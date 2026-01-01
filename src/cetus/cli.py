"""Command-line interface for Cetus."""

from __future__ import annotations

import asyncio
import io
import logging
import sys
import time
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
    """
    from .client import QueryResult

    config = Config.load(api_key=api_key, host=host)
    if since_days is None:
        since_days = config.since_days

    marker_store = MarkerStore()
    # Only use markers in file mode, not stdout mode
    marker = None if (no_marker or not output_file) else marker_store.get(search, index)

    formatter = get_formatter(output_format)

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
        progress.add_task("Querying...", total=None)

        try:
            result = asyncio.run(run_query())
        except KeyboardInterrupt:
            console.print("\n[yellow]Interrupted[/yellow]")
            raise
    elapsed = time.perf_counter() - start_time

    # Output results
    if output_file:
        with open(output_file, "w", encoding="utf-8") as f:
            formatter.format_stream(result.data, f)
        console.print(f"[green]Wrote {result.total_fetched} records to {output_file} in {elapsed:.2f}s[/green]")
    else:
        # Write to stdout - use stream for proper encoding handling
        # For table format, use Rich console directly to handle Unicode
        if output_format == "table":
            stdout_console = Console(force_terminal=sys.stdout.isatty())
            formatter.format_stream(result.data, stdout_console.file)
        else:
            # For other formats, use UTF-8 wrapper on stdout
            stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
            formatter.format_stream(result.data, stdout)
            stdout.flush()
        console.print(f"\n[dim]{result.total_fetched} records in {elapsed:.2f}s[/dim]", highlight=False)

    # Save marker for next incremental query (only in file mode, not stdout)
    if output_file and not no_marker and result.last_uuid and result.last_timestamp:
        marker_store.save(search, index, result.last_timestamp, result.last_uuid)
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
    """
    import csv
    import json

    config = Config.load(api_key=api_key, host=host)
    if since_days is None:
        since_days = config.since_days

    # --stream implies jsonl format unless explicitly specified
    if output_format is None:
        output_format = "jsonl"

    marker_store = MarkerStore()
    # Only use markers in file mode, not stdout mode
    marker = None if (no_marker or not output_file) else marker_store.get(search, index)

    timestamp_field = f"{index}_timestamp"

    # Table format requires buffering for column width calculation
    if output_format == "table":
        console.print(
            "[yellow]Warning: --stream with --format table requires buffering. "
            "Use --format csv or jsonl for true streaming.[/yellow]"
        )

    async def stream_results() -> tuple[int, str | None, str | None, bool]:
        """Async inner function for streaming with responsive interrupt handling."""
        count = 0
        last_uuid = None
        last_timestamp = None
        interrupted = False

        client = CetusClient.from_config(config)

        # Set up output destination
        if output_file:
            out_file = open(output_file, "w", encoding="utf-8", newline="")
        else:
            out_file = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", newline="")

        csv_writer = None
        table_buffer = []  # Only used for table format

        try:
            if output_format == "json":
                # JSON array format - stream but need wrapper
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

                if output_format == "jsonl":
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
                        fieldnames = list(record.keys())
                        csv_writer = csv.DictWriter(out_file, fieldnames=fieldnames, extrasaction="ignore")
                        csv_writer.writeheader()
                        out_file.flush()
                    csv_writer.writerow(record)
                    out_file.flush()
                elif output_format == "table":
                    # Buffer for table format
                    table_buffer.append(record)

            if output_format == "json":
                out_file.write("\n]\n")

            # Handle table format - output buffered data
            if output_format == "table" and table_buffer:
                formatter = get_formatter("table")
                formatter.format_stream(table_buffer, out_file)

        except asyncio.CancelledError:
            interrupted = True
            console.print("\n[yellow]Interrupted[/yellow]")
        except KeyboardInterrupt:
            interrupted = True
            console.print("\n[yellow]Interrupted[/yellow]")
        finally:
            if output_file:
                out_file.close()
            else:
                out_file.flush()
            client.close()

        return count, last_uuid, last_timestamp, interrupted

    # Run the async streaming function
    start_time = time.perf_counter()
    try:
        count, last_uuid, last_timestamp, interrupted = asyncio.run(stream_results())
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted[/yellow]")
        sys.exit(130)
    elapsed = time.perf_counter() - start_time

    # Report results
    if output_file:
        console.print(f"[green]Streamed {count} records to {output_file} in {elapsed:.2f}s[/green]")
    elif not interrupted:
        console.print(f"\n[dim]Streamed {count} records in {elapsed:.2f}s[/dim]", highlight=False)

    if interrupted:
        sys.exit(130)

    # Save marker for next incremental query (only in file mode, not stdout)
    if output_file and not no_marker and last_uuid and last_timestamp:
        marker_store.save(search, index, last_timestamp, last_uuid)
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
    help="Write output to file instead of stdout",
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

    By default, results are written to stdout as JSON. Use --output to
    write to a file, or --format to change the output format.

    Incremental queries are supported via markers. On first run, data
    from the last 7 days is fetched. Subsequent runs fetch only new
    data since the last query. Use --no-marker to disable this behavior.

    Use --stream for large queries to see results as they arrive rather
    than waiting for all data to be fetched. Streaming defaults to jsonl format.
    """
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
            )
    except CetusError as e:
        console.print(f"[red]Error:[/red] {e}")
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
            cfg.since_days = int(value)

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
@click.option("--api-key", envvar="CETUS_API_KEY", help="API key")
@click.option("--host", envvar="CETUS_HOST", help="API host")
def alerts_list(
    owned: bool,
    shared: bool,
    alert_type: str | None,
    api_key: str | None,
    host: str | None,
) -> None:
    """List alert definitions.

    By default, shows alerts you own. Use --shared to include alerts
    shared with you, or --no-owned --shared to see only shared alerts.

    \b
    Examples:
        cetus alerts list                    # Your alerts
        cetus alerts list --shared           # Your alerts + shared
        cetus alerts list --no-owned --shared  # Only shared alerts
        cetus alerts list --type raw         # Only raw query alerts
    """
    try:
        config = Config.load(api_key=api_key, host=host)

        if not owned and not shared:
            console.print("[yellow]Warning: Both --no-owned and --no-shared results in no alerts[/yellow]")
            return

        with CetusClient.from_config(config) as client:
            alerts_data = client.list_alerts(owned=owned, shared=shared, alert_type=alert_type)

        if not alerts_data:
            console.print("[dim]No alerts found[/dim]")
            return

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
            desc = alert.description[:40] + "..." if len(alert.description) > 40 else alert.description
            table.add_row(
                str(alert.id),
                f"[{type_color}]{alert.alert_type}[/{type_color}]",
                alert.title,
                desc,
                owner_col,
            )

        console.print(table)
        console.print(f"\n[dim]Total: {len(alerts_data)} alert(s)[/dim]")

    except CetusError as e:
        console.print(f"[red]Error:[/red] {e}")
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
        formatter = get_formatter(output_format)

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

        # Output results
        if output_file:
            with open(output_file, "w", encoding="utf-8") as f:
                formatter.format_stream(results, f)
            console.print(f"[green]Wrote {len(results)} results to {output_file}[/green]")
        else:
            # Write to stdout
            if output_format == "table":
                stdout_console = Console(force_terminal=sys.stdout.isatty())
                formatter.format_stream(results, stdout_console.file)
            else:
                stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
                formatter.format_stream(results, stdout)
                stdout.flush()

    except CetusError as e:
        console.print(f"[red]Error:[/red] {e}")
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
        cetus alerts backtest 123 --stream
    """
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
            )

    except CetusError as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted[/yellow]")
        sys.exit(130)


if __name__ == "__main__":
    main()
