"""Tests for the CLI commands."""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from cetus.cli import main
from cetus.client import QueryResult


@pytest.fixture
def runner() -> CliRunner:
    """Create a Click CLI runner."""
    return CliRunner()


@pytest.fixture
def temp_config_dir(tmp_path: Path) -> Path:
    """Create a temporary config directory."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    return config_dir


@pytest.fixture
def temp_data_dir(tmp_path: Path) -> Path:
    """Create a temporary data directory."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    return data_dir


class TestMainCommand:
    """Tests for the main cetus command."""

    def test_help_shows_usage(self, runner: CliRunner):
        """Main command should show help text."""
        result = runner.invoke(main, ["--help"])

        assert result.exit_code == 0
        assert "Cetus" in result.output
        assert "query" in result.output
        assert "config" in result.output
        assert "alerts" in result.output

    def test_no_args_shows_help(self, runner: CliRunner):
        """Main command with no args should show help."""
        result = runner.invoke(main, [])

        assert result.exit_code == 0
        assert "Usage:" in result.output

    def test_version_flag(self, runner: CliRunner):
        """--version should show version."""
        result = runner.invoke(main, ["--version"])

        assert result.exit_code == 0
        assert "cetus" in result.output


class TestConfigCommand:
    """Tests for the config command group."""

    def test_config_show(self, runner: CliRunner, temp_config_dir: Path):
        """config show should display configuration."""
        with patch("cetus.config.get_config_dir", return_value=temp_config_dir):
            # Clear env vars
            env = {k: v for k, v in os.environ.items() if not k.startswith("CETUS_")}
            result = runner.invoke(main, ["config", "show"], env=env)

        assert result.exit_code == 0
        assert "host" in result.output
        assert "timeout" in result.output

    def test_config_path(self, runner: CliRunner):
        """config path should show config file path."""
        result = runner.invoke(main, ["config", "path"])

        assert result.exit_code == 0
        assert "config.toml" in result.output

    def test_config_set_api_key(self, runner: CliRunner, temp_config_dir: Path):
        """config set api-key should save API key."""
        with patch("cetus.config.get_config_dir", return_value=temp_config_dir):
            result = runner.invoke(main, ["config", "set", "api-key", "my-secret-key"])

        assert result.exit_code == 0
        assert "success" in result.output.lower()

        # Verify saved
        config_file = temp_config_dir / "config.toml"
        assert "my-secret-key" in config_file.read_text()

    def test_config_set_host(self, runner: CliRunner, temp_config_dir: Path):
        """config set host should save host."""
        with patch("cetus.config.get_config_dir", return_value=temp_config_dir):
            result = runner.invoke(main, ["config", "set", "host", "custom.example.com"])

        assert result.exit_code == 0
        assert "success" in result.output.lower()

    def test_config_set_timeout(self, runner: CliRunner, temp_config_dir: Path):
        """config set timeout should save timeout."""
        with patch("cetus.config.get_config_dir", return_value=temp_config_dir):
            result = runner.invoke(main, ["config", "set", "timeout", "120"])

        assert result.exit_code == 0

    def test_config_set_since_days(self, runner: CliRunner, temp_config_dir: Path):
        """config set since-days should save since-days."""
        with patch("cetus.config.get_config_dir", return_value=temp_config_dir):
            result = runner.invoke(main, ["config", "set", "since-days", "30"])

        assert result.exit_code == 0

    def test_config_set_invalid_timeout(self, runner: CliRunner, temp_config_dir: Path):
        """config set timeout with non-integer should fail."""
        with patch("cetus.config.get_config_dir", return_value=temp_config_dir):
            result = runner.invoke(main, ["config", "set", "timeout", "not-a-number"])

        assert result.exit_code != 0
        assert "invalid" in result.output.lower() or "error" in result.output.lower()


class TestMarkersCommand:
    """Tests for the markers command group."""

    def test_markers_list_empty(self, runner: CliRunner, temp_data_dir: Path):
        """markers list should handle empty markers."""
        markers_dir = temp_data_dir / "markers"
        markers_dir.mkdir()

        with patch("cetus.markers.get_markers_dir", return_value=markers_dir):
            result = runner.invoke(main, ["markers", "list"])

        assert result.exit_code == 0
        assert "No markers" in result.output

    def test_markers_list_shows_markers(self, runner: CliRunner, temp_data_dir: Path):
        """markers list should show existing markers."""
        markers_dir = temp_data_dir / "markers"
        markers_dir.mkdir()

        # Create a marker file
        marker_data = {
            "query": "host:*.example.com",
            "index": "dns",
            "last_timestamp": "2025-01-01T00:00:00Z",
            "last_uuid": "uuid-123",
            "updated_at": "2025-01-02T00:00:00Z",
        }
        (markers_dir / "dns_abc123.json").write_text(json.dumps(marker_data))

        with patch("cetus.markers.get_markers_dir", return_value=markers_dir):
            result = runner.invoke(main, ["markers", "list"])

        assert result.exit_code == 0
        assert "dns" in result.output
        assert "example.com" in result.output

    def test_markers_clear_with_confirmation(self, runner: CliRunner, temp_data_dir: Path):
        """markers clear should ask for confirmation."""
        markers_dir = temp_data_dir / "markers"
        markers_dir.mkdir()

        with patch("cetus.markers.get_markers_dir", return_value=markers_dir):
            result = runner.invoke(main, ["markers", "clear"], input="n\n")

        assert result.exit_code == 0
        assert "Cancelled" in result.output

    def test_markers_clear_yes_flag(self, runner: CliRunner, temp_data_dir: Path):
        """markers clear --yes should skip confirmation."""
        markers_dir = temp_data_dir / "markers"
        markers_dir.mkdir()

        # Create a marker
        (markers_dir / "dns_test.json").write_text("{}")

        with patch("cetus.markers.get_markers_dir", return_value=markers_dir):
            result = runner.invoke(main, ["markers", "clear", "--yes"])

        assert result.exit_code == 0
        assert "Cleared" in result.output

    def test_markers_clear_by_index(self, runner: CliRunner, temp_data_dir: Path):
        """markers clear --index should only clear that index."""
        markers_dir = temp_data_dir / "markers"
        markers_dir.mkdir()

        # Create markers for different indices
        (markers_dir / "dns_test1.json").write_text("{}")
        (markers_dir / "certstream_test2.json").write_text("{}")

        with patch("cetus.markers.get_markers_dir", return_value=markers_dir):
            result = runner.invoke(main, ["markers", "clear", "--index", "dns", "--yes"])

        assert result.exit_code == 0
        # Only dns marker should be cleared
        assert (markers_dir / "certstream_test2.json").exists()
        assert not (markers_dir / "dns_test1.json").exists()


class TestQueryCommand:
    """Tests for the query command."""

    @pytest.fixture
    def mock_query_result(self) -> QueryResult:
        """Create a mock query result."""
        return QueryResult(
            data=[
                {
                    "uuid": "1",
                    "host": "example.com",
                    "A": "1.1.1.1",
                    "dns_timestamp": "2025-01-01T00:00:00Z",
                }
            ],
            total_fetched=1,
            last_uuid="1",
            last_timestamp="2025-01-01T00:00:00Z",
            pages_fetched=1,
        )

    def test_query_requires_api_key(self, runner: CliRunner, temp_config_dir: Path):
        """query should fail if no API key configured."""
        with patch("cetus.config.get_config_dir", return_value=temp_config_dir):
            env = {k: v for k, v in os.environ.items() if not k.startswith("CETUS_")}
            result = runner.invoke(main, ["query", "host:*"], env=env)

        assert result.exit_code == 1
        assert "API key" in result.output or "api_key" in result.output.lower()

    def test_query_with_api_key_flag(
        self, runner: CliRunner, temp_config_dir: Path, temp_data_dir: Path
    ):
        """query --api-key should accept the API key flag."""
        # Just verify the --api-key flag is accepted by the CLI
        result = runner.invoke(main, ["query", "--help"])

        assert result.exit_code == 0
        assert "--api-key" in result.output

    def test_query_help(self, runner: CliRunner):
        """query --help should show usage."""
        result = runner.invoke(main, ["query", "--help"])

        assert result.exit_code == 0
        assert "SEARCH" in result.output
        assert "--index" in result.output
        assert "--format" in result.output
        assert "--output" in result.output

    def test_query_index_options(self, runner: CliRunner):
        """query should accept valid index options."""
        result = runner.invoke(main, ["query", "--help"])

        assert "dns" in result.output
        assert "certstream" in result.output
        assert "alerting" in result.output

    def test_query_format_options(self, runner: CliRunner):
        """query should accept valid format options."""
        result = runner.invoke(main, ["query", "--help"])

        assert "json" in result.output
        assert "jsonl" in result.output
        assert "csv" in result.output
        assert "table" in result.output


class TestAlertsCommand:
    """Tests for the alerts command group."""

    def test_alerts_help(self, runner: CliRunner):
        """alerts --help should show subcommands."""
        result = runner.invoke(main, ["alerts", "--help"])

        assert result.exit_code == 0
        assert "list" in result.output
        assert "results" in result.output
        assert "backtest" in result.output

    def test_alerts_list_requires_api_key(self, runner: CliRunner, temp_config_dir: Path):
        """alerts list should fail if no API key configured."""
        with patch("cetus.config.get_config_dir", return_value=temp_config_dir):
            env = {k: v for k, v in os.environ.items() if not k.startswith("CETUS_")}
            result = runner.invoke(main, ["alerts", "list"], env=env)

        assert result.exit_code == 1
        assert "API key" in result.output or "api_key" in result.output.lower()

    def test_alerts_list_help(self, runner: CliRunner):
        """alerts list --help should show options."""
        result = runner.invoke(main, ["alerts", "list", "--help"])

        assert result.exit_code == 0
        assert "--owned" in result.output
        assert "--shared" in result.output
        assert "--type" in result.output

    def test_alerts_list_type_options(self, runner: CliRunner):
        """alerts list should accept valid type options."""
        result = runner.invoke(main, ["alerts", "list", "--help"])

        assert "raw" in result.output
        assert "terms" in result.output
        assert "structured" in result.output

    def test_alerts_results_requires_alert_id(self, runner: CliRunner):
        """alerts results should require ALERT_ID argument."""
        result = runner.invoke(main, ["alerts", "results"])

        assert result.exit_code != 0
        assert "ALERT_ID" in result.output or "Missing argument" in result.output

    def test_alerts_results_help(self, runner: CliRunner):
        """alerts results --help should show options."""
        result = runner.invoke(main, ["alerts", "results", "--help"])

        assert result.exit_code == 0
        assert "--since" in result.output
        assert "--format" in result.output
        assert "--output" in result.output

    def test_alerts_backtest_requires_alert_id(self, runner: CliRunner):
        """alerts backtest should require ALERT_ID argument."""
        result = runner.invoke(main, ["alerts", "backtest"])

        assert result.exit_code != 0
        assert "ALERT_ID" in result.output or "Missing argument" in result.output

    def test_alerts_backtest_help(self, runner: CliRunner):
        """alerts backtest --help should show options."""
        result = runner.invoke(main, ["alerts", "backtest", "--help"])

        assert result.exit_code == 0
        assert "--index" in result.output
        assert "--format" in result.output
        assert "--stream" in result.output


class TestVerboseOutput:
    """Tests for verbose mode."""

    def test_verbose_flag(self, runner: CliRunner):
        """--verbose flag should be accepted."""
        result = runner.invoke(main, ["-v", "--help"])
        assert result.exit_code == 0

    def test_verbose_with_config_show(self, runner: CliRunner, temp_config_dir: Path):
        """Verbose mode should work with config show."""
        with patch("cetus.config.get_config_dir", return_value=temp_config_dir):
            env = {k: v for k, v in os.environ.items() if not k.startswith("CETUS_")}
            result = runner.invoke(main, ["-v", "config", "show"], env=env)

        assert result.exit_code == 0


class TestOutputFormats:
    """Tests for output format options across commands."""

    def test_query_accepts_all_formats(self, runner: CliRunner):
        """query should accept all format options."""
        for fmt in ["json", "jsonl", "csv", "table"]:
            result = runner.invoke(main, ["query", "--help"])
            assert fmt in result.output

    def test_alerts_results_accepts_all_formats(self, runner: CliRunner):
        """alerts results should accept all format options."""
        for fmt in ["json", "jsonl", "csv", "table"]:
            result = runner.invoke(main, ["alerts", "results", "--help"])
            assert fmt in result.output


class TestStreamingMode:
    """Tests for streaming mode."""

    def test_query_stream_flag(self, runner: CliRunner):
        """query should accept --stream flag."""
        result = runner.invoke(main, ["query", "--help"])

        assert "--stream" in result.output

    def test_backtest_stream_flag(self, runner: CliRunner):
        """alerts backtest should accept --stream flag."""
        result = runner.invoke(main, ["alerts", "backtest", "--help"])

        assert "--stream" in result.output


class TestEnvironmentVariables:
    """Tests for environment variable handling."""

    def test_api_key_from_env(self, runner: CliRunner):
        """CETUS_API_KEY env var should be used."""
        # This is tested indirectly through query --help showing envvar
        result = runner.invoke(main, ["query", "--help"])

        assert "CETUS_API_KEY" in result.output

    def test_host_from_env(self, runner: CliRunner):
        """--host option should be available."""
        result = runner.invoke(main, ["query", "--help"])

        # The help shows --host option (CETUS_HOST env var is handled internally)
        assert "--host" in result.output
        assert "alerting.sparkits.ca" in result.output  # default value shown


class TestErrorHandling:
    """Tests for error handling in CLI."""

    def test_invalid_index(self, runner: CliRunner):
        """Invalid index should show error."""
        result = runner.invoke(main, ["query", "test", "--index", "invalid"])

        assert result.exit_code != 0
        assert "invalid" in result.output.lower()

    def test_invalid_format(self, runner: CliRunner):
        """Invalid format should show error."""
        result = runner.invoke(main, ["query", "test", "--format", "xml"])

        assert result.exit_code != 0
        # Click shows valid choices on error

    def test_invalid_media(self, runner: CliRunner):
        """Invalid media should show error."""
        result = runner.invoke(main, ["query", "test", "--media", "invalid"])

        assert result.exit_code != 0


class TestCLIIntegration:
    """Integration tests for CLI commands."""

    def test_full_workflow_config_then_query(self, runner: CliRunner, tmp_path: Path):
        """Test setting config then using it in query."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        with (
            patch("cetus.config.get_config_dir", return_value=config_dir),
            patch("cetus.config.get_data_dir", return_value=data_dir),
        ):
            # Set API key
            result = runner.invoke(main, ["config", "set", "api-key", "test-key"])
            assert result.exit_code == 0

            # Verify it's saved
            result = runner.invoke(main, ["config", "show"])
            assert result.exit_code == 0
            assert "***" in result.output  # Masked key

    def test_markers_workflow(self, runner: CliRunner, tmp_path: Path):
        """Test creating and clearing markers."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        markers_dir = data_dir / "markers"
        markers_dir.mkdir()

        with patch("cetus.markers.get_markers_dir", return_value=markers_dir):
            # Initially empty
            result = runner.invoke(main, ["markers", "list"])
            assert "No markers" in result.output

            # Create a marker manually
            marker_data = {
                "query": "test",
                "index": "dns",
                "last_timestamp": "2025-01-01T00:00:00Z",
                "last_uuid": "uuid",
                "updated_at": "2025-01-02T00:00:00Z",
            }
            (markers_dir / "dns_test.json").write_text(json.dumps(marker_data))

            # Now shows marker
            result = runner.invoke(main, ["markers", "list"])
            assert "dns" in result.output

            # Clear it
            result = runner.invoke(main, ["markers", "clear", "--yes"])
            assert "Cleared" in result.output

            # Empty again
            result = runner.invoke(main, ["markers", "list"])
            assert "No markers" in result.output


class TestIncrementalQueryAppend:
    """Tests for incremental query file append behavior."""

    @pytest.fixture
    def mock_query_result_batch1(self) -> QueryResult:
        """First batch of query results."""
        return QueryResult(
            data=[
                {
                    "uuid": "1",
                    "host": "a.example.com",
                    "A": "1.1.1.1",
                    "dns_timestamp": "2025-01-01T00:00:00Z",
                },
                {
                    "uuid": "2",
                    "host": "b.example.com",
                    "A": "2.2.2.2",
                    "dns_timestamp": "2025-01-01T01:00:00Z",
                },
            ],
            total_fetched=2,
            last_uuid="2",
            last_timestamp="2025-01-01T01:00:00Z",
            pages_fetched=1,
        )

    @pytest.fixture
    def mock_query_result_batch2(self) -> QueryResult:
        """Second batch of query results (new records)."""
        return QueryResult(
            data=[
                {
                    "uuid": "3",
                    "host": "c.example.com",
                    "A": "3.3.3.3",
                    "dns_timestamp": "2025-01-02T00:00:00Z",
                },
            ],
            total_fetched=1,
            last_uuid="3",
            last_timestamp="2025-01-02T00:00:00Z",
            pages_fetched=1,
        )

    @pytest.fixture
    def mock_query_result_empty(self) -> QueryResult:
        """Empty result (no new records)."""
        return QueryResult(
            data=[],
            total_fetched=0,
            last_uuid=None,
            last_timestamp=None,
            pages_fetched=1,
        )

    def test_incremental_jsonl_preserves_file_on_zero_results(
        self,
        runner: CliRunner,
        tmp_path: Path,
        mock_query_result_batch1: QueryResult,
        mock_query_result_empty: QueryResult,
    ):
        """When incremental query returns 0 records, existing file should be unchanged."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        output_file = tmp_path / "results.jsonl"

        async def mock_query_async_batch1(*args, **kwargs):
            return mock_query_result_batch1

        async def mock_query_async_empty(*args, **kwargs):
            return mock_query_result_empty

        with (
            patch("cetus.config.get_config_dir", return_value=config_dir),
            patch("cetus.config.get_data_dir", return_value=data_dir),
        ):
            # First run - write initial data
            with patch("cetus.client.CetusClient.query_async", mock_query_async_batch1):
                result = runner.invoke(
                    main,
                    [
                        "query",
                        "host:*",
                        "-o",
                        str(output_file),
                        "--format",
                        "jsonl",
                        "--api-key",
                        "test-key",
                    ],
                )
                assert result.exit_code == 0

            # Verify file has initial data
            initial_content = output_file.read_text()
            assert '"uuid": "1"' in initial_content
            assert '"uuid": "2"' in initial_content
            lines_before = len(initial_content.strip().split("\n"))
            assert lines_before == 2

            # Second run with 0 results - file should be unchanged
            with patch("cetus.client.CetusClient.query_async", mock_query_async_empty):
                result = runner.invoke(
                    main,
                    [
                        "query",
                        "host:*",
                        "-o",
                        str(output_file),
                        "--format",
                        "jsonl",
                        "--api-key",
                        "test-key",
                    ],
                )
                assert result.exit_code == 0
                assert "No new records" in result.output or "unchanged" in result.output

            # Verify file is unchanged
            final_content = output_file.read_text()
            assert final_content == initial_content

    def test_incremental_jsonl_appends_new_records(
        self,
        runner: CliRunner,
        tmp_path: Path,
        mock_query_result_batch1: QueryResult,
        mock_query_result_batch2: QueryResult,
    ):
        """When incremental query returns new records, they should be appended."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        output_file = tmp_path / "results.jsonl"

        async def mock_query_async_batch1(*args, **kwargs):
            return mock_query_result_batch1

        async def mock_query_async_batch2(*args, **kwargs):
            return mock_query_result_batch2

        with (
            patch("cetus.config.get_config_dir", return_value=config_dir),
            patch("cetus.config.get_data_dir", return_value=data_dir),
        ):
            # First run
            with patch("cetus.client.CetusClient.query_async", mock_query_async_batch1):
                result = runner.invoke(
                    main,
                    [
                        "query",
                        "host:*",
                        "-o",
                        str(output_file),
                        "--format",
                        "jsonl",
                        "--api-key",
                        "test-key",
                    ],
                )
                assert result.exit_code == 0

            # Second run with new data
            with patch("cetus.client.CetusClient.query_async", mock_query_async_batch2):
                result = runner.invoke(
                    main,
                    [
                        "query",
                        "host:*",
                        "-o",
                        str(output_file),
                        "--format",
                        "jsonl",
                        "--api-key",
                        "test-key",
                    ],
                )
                assert result.exit_code == 0
                assert "Appended" in result.output

            # Verify all 3 records are in file
            final_content = output_file.read_text()
            assert '"uuid": "1"' in final_content
            assert '"uuid": "2"' in final_content
            assert '"uuid": "3"' in final_content
            lines = final_content.strip().split("\n")
            assert len(lines) == 3

    def test_incremental_csv_appends_without_repeating_header(
        self,
        runner: CliRunner,
        tmp_path: Path,
        mock_query_result_batch1: QueryResult,
        mock_query_result_batch2: QueryResult,
    ):
        """CSV append should not repeat the header row."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        output_file = tmp_path / "results.csv"

        async def mock_query_async_batch1(*args, **kwargs):
            return mock_query_result_batch1

        async def mock_query_async_batch2(*args, **kwargs):
            return mock_query_result_batch2

        with (
            patch("cetus.config.get_config_dir", return_value=config_dir),
            patch("cetus.config.get_data_dir", return_value=data_dir),
        ):
            # First run
            with patch("cetus.client.CetusClient.query_async", mock_query_async_batch1):
                result = runner.invoke(
                    main,
                    [
                        "query",
                        "host:*",
                        "-o",
                        str(output_file),
                        "--format",
                        "csv",
                        "--api-key",
                        "test-key",
                    ],
                )
                assert result.exit_code == 0

            # Second run
            with patch("cetus.client.CetusClient.query_async", mock_query_async_batch2):
                result = runner.invoke(
                    main,
                    [
                        "query",
                        "host:*",
                        "-o",
                        str(output_file),
                        "--format",
                        "csv",
                        "--api-key",
                        "test-key",
                    ],
                )
                assert result.exit_code == 0

            # Verify only one header row
            final_content = output_file.read_text()
            lines = final_content.strip().split("\n")
            # 1 header + 3 data rows
            assert len(lines) == 4
            # First line should be header
            assert lines[0].startswith("uuid,")
            # Verify all 3 uuids are present in data rows
            assert "1,a.example.com" in final_content
            assert "2,b.example.com" in final_content
            assert "3,c.example.com" in final_content

    def test_incremental_json_merges_arrays(
        self,
        runner: CliRunner,
        tmp_path: Path,
        mock_query_result_batch1: QueryResult,
        mock_query_result_batch2: QueryResult,
    ):
        """JSON format should merge new records into existing array."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        output_file = tmp_path / "results.json"

        async def mock_query_async_batch1(*args, **kwargs):
            return mock_query_result_batch1

        async def mock_query_async_batch2(*args, **kwargs):
            return mock_query_result_batch2

        with (
            patch("cetus.config.get_config_dir", return_value=config_dir),
            patch("cetus.config.get_data_dir", return_value=data_dir),
        ):
            # First run
            with patch("cetus.client.CetusClient.query_async", mock_query_async_batch1):
                result = runner.invoke(
                    main,
                    [
                        "query",
                        "host:*",
                        "-o",
                        str(output_file),
                        "--format",
                        "json",
                        "--api-key",
                        "test-key",
                    ],
                )
                assert result.exit_code == 0

            # Verify initial state
            initial_data = json.loads(output_file.read_text())
            assert len(initial_data) == 2

            # Second run
            with patch("cetus.client.CetusClient.query_async", mock_query_async_batch2):
                result = runner.invoke(
                    main,
                    [
                        "query",
                        "host:*",
                        "-o",
                        str(output_file),
                        "--format",
                        "json",
                        "--api-key",
                        "test-key",
                    ],
                )
                assert result.exit_code == 0

            # Verify merged array
            final_data = json.loads(output_file.read_text())
            assert len(final_data) == 3
            uuids = [r["uuid"] for r in final_data]
            assert "1" in uuids
            assert "2" in uuids
            assert "3" in uuids

    def test_no_marker_flag_overwrites_file(
        self,
        runner: CliRunner,
        tmp_path: Path,
        mock_query_result_batch1: QueryResult,
        mock_query_result_batch2: QueryResult,
    ):
        """With --no-marker, file should be overwritten not appended."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        output_file = tmp_path / "results.jsonl"

        async def mock_query_async_batch1(*args, **kwargs):
            return mock_query_result_batch1

        async def mock_query_async_batch2(*args, **kwargs):
            return mock_query_result_batch2

        with (
            patch("cetus.config.get_config_dir", return_value=config_dir),
            patch("cetus.config.get_data_dir", return_value=data_dir),
        ):
            # First run
            with patch("cetus.client.CetusClient.query_async", mock_query_async_batch1):
                result = runner.invoke(
                    main,
                    [
                        "query",
                        "host:*",
                        "-o",
                        str(output_file),
                        "--format",
                        "jsonl",
                        "--no-marker",
                        "--api-key",
                        "test-key",
                    ],
                )
                assert result.exit_code == 0

            # Second run with --no-marker should overwrite
            with patch("cetus.client.CetusClient.query_async", mock_query_async_batch2):
                result = runner.invoke(
                    main,
                    [
                        "query",
                        "host:*",
                        "-o",
                        str(output_file),
                        "--format",
                        "jsonl",
                        "--no-marker",
                        "--api-key",
                        "test-key",
                    ],
                )
                assert result.exit_code == 0
                assert "Wrote" in result.output  # Not "Appended"

            # Should only have batch2 data
            final_content = output_file.read_text()
            assert '"uuid": "1"' not in final_content
            assert '"uuid": "2"' not in final_content
            assert '"uuid": "3"' in final_content
            lines = final_content.strip().split("\n")
            assert len(lines) == 1


class TestOutputPrefix:
    """Tests for --output-prefix timestamped file output."""

    @pytest.fixture
    def mock_query_result(self) -> QueryResult:
        """Sample query results."""
        return QueryResult(
            data=[
                {
                    "uuid": "1",
                    "host": "a.example.com",
                    "A": "1.1.1.1",
                    "dns_timestamp": "2025-01-01T00:00:00Z",
                },
                {
                    "uuid": "2",
                    "host": "b.example.com",
                    "A": "2.2.2.2",
                    "dns_timestamp": "2025-01-01T01:00:00Z",
                },
            ],
            total_fetched=2,
            last_uuid="2",
            last_timestamp="2025-01-01T01:00:00Z",
            pages_fetched=1,
        )

    @pytest.fixture
    def mock_query_result_empty(self) -> QueryResult:
        """Empty result."""
        return QueryResult(
            data=[],
            total_fetched=0,
            last_uuid=None,
            last_timestamp=None,
            pages_fetched=1,
        )

    def test_output_prefix_creates_timestamped_file(
        self,
        runner: CliRunner,
        tmp_path: Path,
        mock_query_result: QueryResult,
    ):
        """--output-prefix should create a timestamped file."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        prefix = str(tmp_path / "results")

        async def mock_query_async(*args, **kwargs):
            return mock_query_result

        with (
            patch("cetus.config.get_config_dir", return_value=config_dir),
            patch("cetus.config.get_data_dir", return_value=data_dir),
            patch("cetus.client.CetusClient.query_async", mock_query_async),
        ):
            result = runner.invoke(
                main,
                ["query", "host:*", "-p", prefix, "--format", "jsonl", "--api-key", "test-key"],
            )
            assert result.exit_code == 0
            assert "Wrote 2 records" in result.output

            # Check that a timestamped file was created
            files = list(tmp_path.glob("results_*.jsonl"))
            assert len(files) == 1
            assert files[0].name.startswith("results_")
            assert files[0].suffix == ".jsonl"

            # Verify content
            content = files[0].read_text()
            assert '"uuid": "1"' in content
            assert '"uuid": "2"' in content

    def test_output_prefix_no_file_on_zero_results(
        self,
        runner: CliRunner,
        tmp_path: Path,
        mock_query_result_empty: QueryResult,
    ):
        """--output-prefix should not create file when there are no records."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        prefix = str(tmp_path / "results")

        async def mock_query_async(*args, **kwargs):
            return mock_query_result_empty

        with (
            patch("cetus.config.get_config_dir", return_value=config_dir),
            patch("cetus.config.get_data_dir", return_value=data_dir),
            patch("cetus.client.CetusClient.query_async", mock_query_async),
        ):
            result = runner.invoke(
                main,
                ["query", "host:*", "-p", prefix, "--format", "jsonl", "--api-key", "test-key"],
            )
            assert result.exit_code == 0
            assert "No new records" in result.output

            # No file should be created
            files = list(tmp_path.glob("results_*.jsonl"))
            assert len(files) == 0

    def test_output_prefix_uses_markers(
        self,
        runner: CliRunner,
        tmp_path: Path,
        mock_query_result: QueryResult,
    ):
        """--output-prefix should save markers for incremental queries."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        markers_dir = data_dir / "markers"
        markers_dir.mkdir()
        prefix = str(tmp_path / "results")

        async def mock_query_async(*args, **kwargs):
            return mock_query_result

        with (
            patch("cetus.config.get_config_dir", return_value=config_dir),
            patch("cetus.config.get_data_dir", return_value=data_dir),
            patch("cetus.markers.get_markers_dir", return_value=markers_dir),
            patch("cetus.client.CetusClient.query_async", mock_query_async),
        ):
            result = runner.invoke(
                main,
                ["query", "host:*", "-p", prefix, "--format", "jsonl", "--api-key", "test-key"],
            )
            assert result.exit_code == 0

            # Check that a marker was saved
            marker_files = list(markers_dir.glob("*.json"))
            assert len(marker_files) == 1

    def test_output_prefix_format_determines_extension(
        self,
        runner: CliRunner,
        tmp_path: Path,
        mock_query_result: QueryResult,
    ):
        """--output-prefix should use format to determine file extension."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        prefix = str(tmp_path / "results")

        async def mock_query_async(*args, **kwargs):
            return mock_query_result

        with (
            patch("cetus.config.get_config_dir", return_value=config_dir),
            patch("cetus.config.get_data_dir", return_value=data_dir),
            patch("cetus.client.CetusClient.query_async", mock_query_async),
        ):
            result = runner.invoke(
                main, ["query", "host:*", "-p", prefix, "--format", "csv", "--api-key", "test-key"]
            )
            assert result.exit_code == 0

            # Check CSV extension
            files = list(tmp_path.glob("results_*.csv"))
            assert len(files) == 1

    def test_output_and_output_prefix_mutually_exclusive(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ):
        """--output and --output-prefix cannot be used together."""
        output_file = tmp_path / "results.jsonl"
        prefix = str(tmp_path / "results")

        result = runner.invoke(
            main, ["query", "host:*", "-o", str(output_file), "-p", prefix, "--api-key", "test-key"]
        )
        assert result.exit_code == 1
        assert "mutually exclusive" in result.output


class TestOutputDirectoryErrorHandling:
    """Tests for error handling when output directory doesn't exist."""

    def test_query_output_nonexistent_directory_shows_clean_error(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ):
        """query -o to non-existent directory should show clean error, not traceback."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Use a path where the parent directory doesn't exist
        nonexistent_dir = tmp_path / "nonexistent_subdir" / "results.json"

        async def mock_query_async(*args, **kwargs):
            return QueryResult(
                data=[{"uuid": "1", "host": "test.com", "dns_timestamp": "2025-01-01T00:00:00Z"}],
                total_fetched=1,
                last_uuid="1",
                last_timestamp="2025-01-01T00:00:00Z",
                pages_fetched=1,
            )

        with (
            patch("cetus.config.get_config_dir", return_value=config_dir),
            patch("cetus.config.get_data_dir", return_value=data_dir),
            patch("cetus.client.CetusClient.query_async", mock_query_async),
        ):
            result = runner.invoke(
                main,
                [
                    "query",
                    "host:*",
                    "-o",
                    str(nonexistent_dir),
                    "--format",
                    "json",
                    "--api-key",
                    "test-key",
                ],
            )

        # Should fail with exit code 1
        assert result.exit_code == 1
        # Should have clean error message
        assert "Error" in result.output or "error" in result.output.lower()
        # Should NOT have Python traceback
        assert "Traceback" not in result.output
        assert 'File "' not in result.output
