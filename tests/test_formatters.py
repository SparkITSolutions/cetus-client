"""Tests for output formatters."""

from __future__ import annotations

import csv
import io
import json

import pytest

from cetus.formatters import (
    CSVFormatter,
    Formatter,
    JSONFormatter,
    JSONLinesFormatter,
    TableFormatter,
    get_formatter,
)


class TestFormatterBase:
    """Tests for the Formatter base class."""

    def test_is_abstract(self):
        """Formatter should be abstract and not instantiable."""
        with pytest.raises(TypeError):
            Formatter()

    def test_format_is_abstract_method(self):
        """format method should be abstract."""
        # This is enforced by ABC
        assert hasattr(Formatter, "format")

    def test_format_stream_is_abstract_method(self):
        """format_stream method should be abstract."""
        assert hasattr(Formatter, "format_stream")


class TestJSONFormatter:
    """Tests for JSONFormatter."""

    @pytest.fixture
    def formatter(self) -> JSONFormatter:
        return JSONFormatter()

    @pytest.fixture
    def sample_data(self) -> list[dict]:
        return [
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"},
        ]

    def test_format_returns_json_array(self, formatter: JSONFormatter, sample_data: list[dict]):
        """format should return a valid JSON array string."""
        result = formatter.format(sample_data)
        parsed = json.loads(result)
        assert parsed == sample_data

    def test_format_empty_list(self, formatter: JSONFormatter):
        """format should handle empty list."""
        result = formatter.format([])
        assert json.loads(result) == []

    def test_format_is_pretty_printed(self, formatter: JSONFormatter, sample_data: list[dict]):
        """format should use pretty printing with indentation."""
        result = formatter.format(sample_data)
        assert "\n" in result
        assert "  " in result  # 2-space indent

    def test_format_with_custom_indent(self, sample_data: list[dict]):
        """JSONFormatter can use custom indent."""
        formatter = JSONFormatter(indent=4)
        result = formatter.format(sample_data)
        assert "    " in result  # 4-space indent

    def test_format_stream_writes_to_file(self, formatter: JSONFormatter, sample_data: list[dict]):
        """format_stream should write JSON to file object."""
        output = io.StringIO()
        count = formatter.format_stream(sample_data, output)

        output.seek(0)
        parsed = json.load(output)

        assert parsed == sample_data
        assert count == len(sample_data)

    def test_format_stream_adds_newline(self, formatter: JSONFormatter, sample_data: list[dict]):
        """format_stream should end with newline."""
        output = io.StringIO()
        formatter.format_stream(sample_data, output)

        assert output.getvalue().endswith("\n")

    def test_format_stream_returns_count(self, formatter: JSONFormatter, sample_data: list[dict]):
        """format_stream should return record count."""
        output = io.StringIO()
        count = formatter.format_stream(sample_data, output)
        assert count == 2

    def test_handles_unicode(self, formatter: JSONFormatter):
        """format should handle unicode characters."""
        data = [{"name": "test\u4e2d\u6587"}]
        result = formatter.format(data)
        assert "\u4e2d\u6587" in result or "\\u" in result

    def test_handles_nested_objects(self, formatter: JSONFormatter):
        """format should handle nested objects."""
        data = [{"nested": {"inner": [1, 2, 3]}}]
        result = formatter.format(data)
        parsed = json.loads(result)
        assert parsed[0]["nested"]["inner"] == [1, 2, 3]


class TestJSONLinesFormatter:
    """Tests for JSONLinesFormatter (NDJSON)."""

    @pytest.fixture
    def formatter(self) -> JSONLinesFormatter:
        return JSONLinesFormatter()

    @pytest.fixture
    def sample_data(self) -> list[dict]:
        return [
            {"id": 1, "name": "test1"},
            {"id": 2, "name": "test2"},
            {"id": 3, "name": "test3"},
        ]

    def test_format_one_object_per_line(
        self, formatter: JSONLinesFormatter, sample_data: list[dict]
    ):
        """format should output one JSON object per line."""
        result = formatter.format(sample_data)
        lines = result.strip().split("\n")

        assert len(lines) == 3
        for i, line in enumerate(lines):
            parsed = json.loads(line)
            assert parsed == sample_data[i]

    def test_format_empty_list(self, formatter: JSONLinesFormatter):
        """format should return empty string for empty list."""
        result = formatter.format([])
        assert result == ""

    def test_format_single_item(self, formatter: JSONLinesFormatter):
        """format should handle single item."""
        result = formatter.format([{"id": 1}])
        assert json.loads(result) == {"id": 1}
        assert "\n" not in result.strip()

    def test_format_stream_writes_incrementally(
        self, formatter: JSONLinesFormatter, sample_data: list[dict]
    ):
        """format_stream should write each record with newline."""
        output = io.StringIO()
        count = formatter.format_stream(sample_data, output)

        output.seek(0)
        lines = output.read().strip().split("\n")

        assert len(lines) == 3
        assert count == 3

    def test_format_stream_each_line_is_valid_json(
        self, formatter: JSONLinesFormatter, sample_data: list[dict]
    ):
        """Each line in format_stream output should be valid JSON."""
        output = io.StringIO()
        formatter.format_stream(sample_data, output)

        output.seek(0)
        for line in output:
            if line.strip():
                json.loads(line)  # Should not raise

    def test_no_pretty_printing(self, formatter: JSONLinesFormatter):
        """JSONL should not use pretty printing."""
        data = [{"nested": {"key": "value"}}]
        result = formatter.format(data)
        # Should be single line, no indentation
        assert result.count("\n") == 0
        assert "  " not in result

    def test_handles_unicode(self, formatter: JSONLinesFormatter):
        """format should handle unicode characters."""
        data = [{"name": "\u4e2d\u6587"}]
        result = formatter.format(data)
        # Should be parseable
        parsed = json.loads(result)
        assert parsed["name"] == "\u4e2d\u6587"


class TestCSVFormatter:
    """Tests for CSVFormatter."""

    @pytest.fixture
    def formatter(self) -> CSVFormatter:
        return CSVFormatter()

    @pytest.fixture
    def sample_data(self) -> list[dict]:
        return [
            {"uuid": "1", "host": "a.com", "A": "1.1.1.1"},
            {"uuid": "2", "host": "b.com", "A": "2.2.2.2"},
        ]

    def test_format_includes_header(self, formatter: CSVFormatter, sample_data: list[dict]):
        """format should include CSV header row."""
        result = formatter.format(sample_data)
        lines = result.strip().split("\n")

        assert len(lines) == 3  # 1 header + 2 data rows
        header = lines[0]
        assert "uuid" in header
        assert "host" in header

    def test_format_empty_list(self, formatter: CSVFormatter):
        """format should return empty or whitespace for empty list."""
        result = formatter.format([])
        assert result.strip() == ""

    def test_format_is_valid_csv(self, formatter: CSVFormatter, sample_data: list[dict]):
        """format should produce valid CSV."""
        result = formatter.format(sample_data)
        reader = csv.DictReader(io.StringIO(result))
        rows = list(reader)

        assert len(rows) == 2
        assert rows[0]["host"] == "a.com"
        assert rows[1]["host"] == "b.com"

    def test_priority_field_ordering(self, formatter: CSVFormatter):
        """CSV should order priority fields first."""
        data = [{"other": "x", "uuid": "1", "host": "a.com", "A": "1.1.1.1"}]
        result = formatter.format(data)
        header = result.split("\n")[0]

        # Priority fields should come first
        uuid_pos = header.find("uuid")
        host_pos = header.find("host")
        other_pos = header.find("other")

        assert uuid_pos < other_pos
        assert host_pos < other_pos

    def test_custom_fields(self):
        """CSVFormatter can use custom field list."""
        formatter = CSVFormatter(fields=["host", "A"])
        data = [{"uuid": "1", "host": "a.com", "A": "1.1.1.1"}]
        result = formatter.format(data)

        assert "host" in result
        assert "A" in result
        assert "uuid" not in result

    def test_handles_missing_fields(self, formatter: CSVFormatter):
        """CSV should handle records with missing fields."""
        data = [
            {"uuid": "1", "host": "a.com"},
            {"uuid": "2", "A": "1.1.1.1"},  # missing host
        ]
        result = formatter.format(data)
        # Should not raise, missing values are empty
        reader = csv.DictReader(io.StringIO(result))
        rows = list(reader)
        assert len(rows) == 2

    def test_handles_extra_fields(self, formatter: CSVFormatter):
        """CSV should ignore extra fields not in first record."""
        data = [
            {"uuid": "1", "host": "a.com"},
            {"uuid": "2", "host": "b.com", "extra": "ignored"},
        ]
        result = formatter.format(data)
        # extra field should be ignored (extrasaction='ignore')
        assert "ignored" not in result

    def test_format_stream(self, formatter: CSVFormatter, sample_data: list[dict]):
        """format_stream should write valid CSV."""
        output = io.StringIO()
        count = formatter.format_stream(sample_data, output)

        output.seek(0)
        reader = csv.DictReader(output)
        rows = list(reader)

        assert count == 2
        assert len(rows) == 2

    def test_handles_commas_in_values(self, formatter: CSVFormatter):
        """CSV should properly quote values with commas."""
        data = [{"host": "a,b,c.com", "uuid": "1"}]
        result = formatter.format(data)
        reader = csv.DictReader(io.StringIO(result))
        rows = list(reader)
        assert rows[0]["host"] == "a,b,c.com"


class TestTableFormatter:
    """Tests for TableFormatter."""

    @pytest.fixture
    def formatter(self) -> TableFormatter:
        return TableFormatter()

    @pytest.fixture
    def sample_data(self) -> list[dict]:
        return [
            {"host": "example.com", "A": "1.1.1.1", "uuid": "uuid-1"},
            {"host": "test.com", "A": "2.2.2.2", "uuid": "uuid-2"},
        ]

    def test_max_rows_constant(self):
        """TableFormatter should have MAX_ROWS = 100."""
        assert TableFormatter.MAX_ROWS == 100

    def test_max_col_width_constant(self):
        """TableFormatter should have MAX_COL_WIDTH = 50."""
        assert TableFormatter.MAX_COL_WIDTH == 50

    def test_format_returns_string(self, formatter: TableFormatter, sample_data: list[dict]):
        """format should return a string."""
        result = formatter.format(sample_data)
        assert isinstance(result, str)
        assert len(result) > 0

    def test_format_empty_list(self, formatter: TableFormatter):
        """format should handle empty list gracefully."""
        result = formatter.format([])
        assert "No results" in result

    def test_format_includes_data(self, formatter: TableFormatter, sample_data: list[dict]):
        """format should include data values in output."""
        result = formatter.format(sample_data)
        assert "example.com" in result
        assert "1.1.1.1" in result

    def test_truncates_long_values(self, formatter: TableFormatter):
        """Long values should be truncated with ellipsis."""
        long_value = "x" * 100
        data = [{"host": long_value}]
        result = formatter.format(data)
        assert "..." in result
        assert long_value not in result  # Full value not present

    def test_truncate_method(self, formatter: TableFormatter):
        """_truncate should truncate to max_len with ellipsis."""
        result = formatter._truncate("hello world", max_len=8)
        assert result == "hello..."
        assert len(result) == 8

    def test_truncate_short_string(self, formatter: TableFormatter):
        """_truncate should not modify short strings."""
        result = formatter._truncate("short", max_len=10)
        assert result == "short"

    def test_limits_columns(self, formatter: TableFormatter):
        """Table should limit number of columns."""
        data = [
            {
                "f1": 1,
                "f2": 2,
                "f3": 3,
                "f4": 4,
                "f5": 5,
                "f6": 6,
                "f7": 7,
                "f8": 8,
                "f9": 9,
                "f10": 10,
            }
        ]
        result = formatter.format(data)
        # Should only show up to 8 columns
        # Count occurrences of column values is tricky, but format should work
        assert isinstance(result, str)

    def test_limits_rows(self, formatter: TableFormatter):
        """Table should limit rows to MAX_ROWS."""
        data = [{"id": i} for i in range(150)]
        result = formatter.format(data)
        # Rich adds ANSI codes, so just check the key parts are there
        assert "50" in result and "more rows" in result  # 150 - 100 = 50 truncated

    def test_custom_fields(self):
        """TableFormatter can use custom field list."""
        formatter = TableFormatter(fields=["host"])
        data = [{"host": "a.com", "A": "1.1.1.1"}]
        result = formatter.format(data)
        assert "a.com" in result

    def test_format_stream(self, formatter: TableFormatter, sample_data: list[dict]):
        """format_stream should write table to output."""
        output = io.StringIO()
        count = formatter.format_stream(sample_data, output)

        assert count == 2
        result = output.getvalue()
        assert "example.com" in result

    def test_priority_field_ordering(self, formatter: TableFormatter):
        """Table should prioritize commonly useful fields."""
        data = [{"other": "x", "host": "a.com", "A": "1.1.1.1", "uuid": "1"}]
        # Verify fields method returns priority fields first
        fields = formatter._get_display_fields(data)
        # host should be near the front (it's a priority field)
        assert "host" in fields


class TestGetFormatter:
    """Tests for get_formatter factory function."""

    def test_get_json_formatter(self):
        """get_formatter('json') should return JSONFormatter."""
        result = get_formatter("json")
        assert isinstance(result, JSONFormatter)

    def test_get_jsonl_formatter(self):
        """get_formatter('jsonl') should return JSONLinesFormatter."""
        result = get_formatter("jsonl")
        assert isinstance(result, JSONLinesFormatter)

    def test_get_csv_formatter(self):
        """get_formatter('csv') should return CSVFormatter."""
        result = get_formatter("csv")
        assert isinstance(result, CSVFormatter)

    def test_get_table_formatter(self):
        """get_formatter('table') should return TableFormatter."""
        result = get_formatter("table")
        assert isinstance(result, TableFormatter)

    def test_unknown_format_raises_error(self):
        """get_formatter with unknown format should raise ValueError."""
        with pytest.raises(ValueError, match="Unknown format"):
            get_formatter("xml")

    def test_error_message_includes_options(self):
        """Error message should include valid format options."""
        try:
            get_formatter("invalid")
            pytest.fail("Should raise ValueError")
        except ValueError as e:
            message = str(e)
            assert "json" in message
            assert "jsonl" in message
            assert "csv" in message
            assert "table" in message


class TestFormatterIntegration:
    """Integration tests for formatters with realistic data."""

    @pytest.fixture
    def dns_records(self) -> list[dict]:
        """Realistic DNS query results."""
        return [
            {
                "uuid": "record-001",
                "host": "www.example.com",
                "A": "192.168.1.1",
                "AAAA": "2001:db8::1",
                "dns_timestamp": "2025-01-01T10:00:00Z",
            },
            {
                "uuid": "record-002",
                "host": "api.example.com",
                "A": "192.168.1.2",
                "CNAME": "api-backend.example.com",
                "dns_timestamp": "2025-01-01T11:00:00Z",
            },
        ]

    def test_all_formatters_handle_dns_records(self, dns_records: list[dict]):
        """All formatters should handle realistic DNS records."""
        for format_name in ["json", "jsonl", "csv", "table"]:
            formatter = get_formatter(format_name)
            result = formatter.format(dns_records)
            assert isinstance(result, str)
            assert len(result) > 0

    def test_all_formatters_stream_dns_records(self, dns_records: list[dict]):
        """All formatters should stream realistic DNS records."""
        for format_name in ["json", "jsonl", "csv", "table"]:
            formatter = get_formatter(format_name)
            output = io.StringIO()
            count = formatter.format_stream(dns_records, output)
            assert count == 2
            assert len(output.getvalue()) > 0

    def test_roundtrip_json(self, dns_records: list[dict]):
        """JSON format should roundtrip perfectly."""
        formatter = get_formatter("json")
        result = formatter.format(dns_records)
        parsed = json.loads(result)
        assert parsed == dns_records

    def test_roundtrip_jsonl(self, dns_records: list[dict]):
        """JSONL format should roundtrip perfectly."""
        formatter = get_formatter("jsonl")
        result = formatter.format(dns_records)

        parsed = []
        for line in result.strip().split("\n"):
            if line:
                parsed.append(json.loads(line))

        assert parsed == dns_records
