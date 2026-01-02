"""Output formatters for query results."""

from __future__ import annotations

import csv
import io
import json
import sys
from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import IO

from rich.console import Console
from rich.table import Table


class Formatter(ABC):
    """Base class for output formatters."""

    @abstractmethod
    def format(self, data: list[dict]) -> str:
        """Format data as a string."""

    @abstractmethod
    def format_stream(self, data: Iterable[dict], output: IO[str]) -> int:
        """Stream formatted data to output. Returns count of records written."""


class JSONFormatter(Formatter):
    """Format output as a JSON array (pretty-printed)."""

    def __init__(self, indent: int = 2):
        self.indent = indent

    def format(self, data: list[dict]) -> str:
        return json.dumps(data, indent=self.indent)

    def format_stream(self, data: Iterable[dict], output: IO[str]) -> int:
        # For JSON array format, we need to collect all data first
        items = list(data)
        output.write(self.format(items))
        output.write("\n")
        return len(items)


class JSONLinesFormatter(Formatter):
    """Format output as JSON Lines (one JSON object per line).

    This format is ideal for streaming and processing with tools like jq.
    """

    def format(self, data: list[dict]) -> str:
        return "\n".join(json.dumps(item) for item in data)

    def format_stream(self, data: Iterable[dict], output: IO[str]) -> int:
        count = 0
        for item in data:
            output.write(json.dumps(item))
            output.write("\n")
            count += 1
        return count


class CSVFormatter(Formatter):
    """Format output as CSV."""

    def __init__(self, fields: list[str] | None = None):
        self.fields = fields

    def _get_fields(self, data: list[dict]) -> list[str]:
        """Determine fields from data if not specified."""
        if self.fields:
            return self.fields
        if not data:
            return []
        # Use fields from first record, with common ones first
        priority_fields = ["uuid", "host", "A", "dns_timestamp", "certstream_timestamp"]
        first_keys = list(data[0].keys())
        ordered = [f for f in priority_fields if f in first_keys]
        ordered.extend(f for f in first_keys if f not in ordered)
        return ordered

    def format(self, data: list[dict]) -> str:
        output = io.StringIO()
        self._write_csv(data, output)
        return output.getvalue()

    def format_stream(self, data: Iterable[dict], output: IO[str]) -> int:
        # For CSV we need to peek at first record for headers
        items = list(data)
        if items:
            self._write_csv(items, output)
        return len(items)

    def _write_csv(self, data: list[dict], output: IO[str]) -> None:
        fields = self._get_fields(data)
        writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in data:
            writer.writerow(row)


class TableFormatter(Formatter):
    """Format output as a rich table for terminal display."""

    MAX_ROWS = 100  # Limit rows for table display
    MAX_COL_WIDTH = 50

    def __init__(self, fields: list[str] | None = None):
        self.fields = fields

    def _get_display_fields(self, data: list[dict]) -> list[str]:
        """Get fields to display, prioritizing useful ones."""
        if self.fields:
            return self.fields
        if not data:
            return []

        # Prioritize commonly useful fields
        priority = ["host", "A", "AAAA", "CNAME", "uuid", "dns_timestamp"]
        first_keys = list(data[0].keys())
        ordered = [f for f in priority if f in first_keys]
        ordered.extend(f for f in first_keys if f not in ordered)
        return ordered[:8]  # Limit columns for readability

    def _truncate(self, value: str, max_len: int = MAX_COL_WIDTH) -> str:
        """Truncate long values."""
        if len(value) > max_len:
            return value[: max_len - 3] + "..."
        return value

    def format(self, data: list[dict]) -> str:
        console = Console(file=io.StringIO(), force_terminal=True)
        self._write_table(data, console)
        return console.file.getvalue()

    def format_stream(self, data: Iterable[dict], output: IO[str]) -> int:
        items = list(data)
        console = Console(file=output, force_terminal=sys.stdout.isatty())
        self._write_table(items, console)
        return len(items)

    def _write_table(self, data: list[dict], console: Console) -> None:
        if not data:
            console.print("[dim]No results[/dim]")
            return

        fields = self._get_display_fields(data)
        truncated = len(data) > self.MAX_ROWS

        table = Table(show_header=True, header_style="bold cyan")
        for field in fields:
            table.add_column(field)

        for row in data[: self.MAX_ROWS]:
            values = [self._truncate(str(row.get(f, ""))) for f in fields]
            table.add_row(*values)

        console.print(table)
        if truncated:
            console.print(
                f"[dim]... and {len(data) - self.MAX_ROWS} more rows "
                "(use --format json or jsonl to see all)[/dim]"
            )


def get_formatter(format_name: str) -> Formatter:
    """Get a formatter by name."""
    formatters = {
        "json": JSONFormatter,
        "jsonl": JSONLinesFormatter,
        "csv": CSVFormatter,
        "table": TableFormatter,
    }
    if format_name not in formatters:
        raise ValueError(f"Unknown format: {format_name}. Choose from: {list(formatters.keys())}")
    return formatters[format_name]()
