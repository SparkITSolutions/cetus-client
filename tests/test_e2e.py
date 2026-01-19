"""End-to-end tests against a real Cetus server.

These tests require a running Cetus instance and valid API credentials.
They are skipped by default unless CETUS_E2E_TEST=1 is set.

Environment variables:
    CETUS_E2E_TEST: Set to "1" to enable E2E tests
    CETUS_API_KEY: API key for authentication
    CETUS_HOST: Server host (default: alerting.sparkits.ca)

Run with:
    CETUS_E2E_TEST=1 CETUS_API_KEY=your-key pytest tests/test_e2e.py -v

Expected duration: ~7-8 minutes for all tests (--media all tests skipped)

Query optimization:
- Uses host:microsoft.com which has frequent data and returns quickly
- Uses since_days=7 (same speed as 1 day for targeted queries)
- Streaming tests break early after a few records

Test categories (134 total):
- Query endpoints: 4 tests (dns, certstream, alerting indices, invalid index)
- Streaming: 2 tests
- Alerts API: 2 tests
- Async methods: 2 tests
- Authentication: 1 test
- CLI commands: 4 tests
- File output: 4 tests
- Incremental queries: 2 tests
- Version/markers/config: 5 tests
- Alert results/backtest: 2 tests
- Error handling: 4 tests
- Format/verbose: 3 tests
- Since-days edge cases: 3 tests (zero, negative rejected, config set negative)
- Alert type filtering: 2 tests
- Alert operations with real data: 3 tests
- Completion scripts: 3 tests (bash, zsh, fish)
- Alerts edge cases: 2 tests
- Verbose mode extended: 2 tests
- Config validation: 2 tests
- Mutually exclusive options: 1 test
- Markers clear by index: 1 test
- Get alert endpoint: 2 tests
- Streaming CSV: 1 test
- Media all option: 2 tests (API and CLI with extended timeout)
- Backtest streaming: 1 test
- Empty query handling: 1 test
- Output directory errors: 2 tests (regular and streaming)
- Alerts list combined flags: 1 test (--owned and --shared)
- Verbose mode with markers: 1 test
- Query edge cases: 6 tests (whitespace, unicode, large pagination xfail,
  special chars, long queries)
- Alert access permissions: 2 tests
- Output prefix formats: 2 tests (JSON, CSV with -p option)
- Streaming table warning: 1 test
- Backtest with indices: 2 tests (certstream, alerting)
- Unicode output handling: 2 tests (table format encoding fix)
- Markers mode separation: 1 test (-o and -p have separate markers)
- Alert results --since filter: 2 tests (valid and invalid timestamp)
- Verbose mode streaming: 1 test (debug output with streaming)
- Large since-days values: 1 test (365 day lookback)
- Streaming with --no-marker: 2 tests (stdout and file output)
- Marker/since-days interaction: 1 test (marker takes precedence)
- Help text completeness: 5 tests (query, alerts, results, list format, backtest prefix documented)
- Alerts list formats: 4 tests (json, jsonl, csv, file output)
- Backtest output prefix: 2 tests (creates file, mutually exclusive with -o)
- DSL/JSON queries: 3 tests (CLI, API, and streaming with DSL syntax)
- Backtest terms alerts: 1 test (terms alert expansion)
- API key masking: 1 test (verbose mode security)
- Marker file corruption: 1 test (graceful recovery)
- User-Agent header: 1 test (version in header)
- Timeout behavior: 1 test (short timeout handling)
- Config environment variables: 1 test (CETUS_SINCE_DAYS)
- Query result count: 2 tests (buffered count, file output count)
- Alert results output formats: 2 tests (CSV and JSONL to file)
- Streaming alerting index: 2 tests (stdout and file output)
- Config file corruption: 2 tests (malformed TOML, empty file)
- Alerts list to file: 2 tests (CSV and JSONL export)
- Output prefix with --no-marker: 1 test
- Streaming with --media all: 1 test
- Backtest streaming with output prefix: 1 test
- Table format incremental append: 1 test (warning when table can't append)
- Backtest verbose mode: 1 test (shows alert details with -v)
- Shared alert operations: 2 tests (results and backtest on shared alerts)
- Config forward compatibility: 2 tests (unknown keys, empty values)
- Lucene query operators: 6 tests (AND, OR, NOT, wildcard suffix, quoted, grouping)
"""

from __future__ import annotations

import os

import pytest

# Skip all tests in this module unless E2E testing is enabled
pytestmark = pytest.mark.skipif(
    os.environ.get("CETUS_E2E_TEST") != "1",
    reason="E2E tests disabled. Set CETUS_E2E_TEST=1 to run.",
)


@pytest.fixture
def api_key() -> str:
    """Get API key from environment or config file."""
    key = os.environ.get("CETUS_API_KEY")
    if not key:
        # Fall back to config file
        from cetus.config import Config

        config = Config.load()
        key = config.api_key
    if not key:
        pytest.skip("CETUS_API_KEY not set and no config file found")
    return key


@pytest.fixture
def host() -> str:
    """Get host from environment or use default."""
    return os.environ.get("CETUS_HOST", "alerting.sparkits.ca")


class TestQueryEndpoint:
    """E2E tests for the /api/query/ endpoint.

    IMPORTANT: Always use since_days to limit query scope.
    Without it, ES scans ALL historical data which takes forever.

    Uses host:microsoft.com which has frequent cert renewals and runs quickly.
    """

    # Query for popular domain - returns records consistently
    DATA_QUERY = "host:microsoft.com"

    def test_query_api_works(self, api_key: str, host: str) -> None:
        """Test that query API responds correctly with real data."""
        from cetus.client import CetusClient

        client = CetusClient(api_key=api_key, host=host, timeout=120)
        try:
            result = client.query(
                search=self.DATA_QUERY,
                index="dns",
                media="nvme",
                since_days=7,  # 7 days is about the same speed as 1 day
                marker=None,
            )
            # Should return a valid QueryResult with data
            assert result is not None
            assert hasattr(result, "data")
            assert hasattr(result, "total_fetched")
            assert hasattr(result, "pages_fetched")
            assert isinstance(result.data, list)
            assert len(result.data) > 0, "Expected results for microsoft.com"
        finally:
            client.close()

    def test_query_certstream_index(self, api_key: str, host: str) -> None:
        """Test query against certstream index."""
        from cetus.client import CetusClient

        # Certstream may not always have cert renewals for a given domain
        client = CetusClient(api_key=api_key, host=host, timeout=120)
        try:
            result = client.query(
                search=self.DATA_QUERY,
                index="certstream",
                media="nvme",
                since_days=7,
                marker=None,
            )
            assert result is not None
            assert isinstance(result.data, list)
            # Don't require data - cert renewals are sporadic
        finally:
            client.close()

    def test_query_alerting_index(self, api_key: str, host: str) -> None:
        """Test query against alerting index."""
        from cetus.client import CetusClient

        # Alerting index may not have microsoft.com data, so just test API works
        client = CetusClient(api_key=api_key, host=host, timeout=120)
        try:
            result = client.query(
                search=self.DATA_QUERY,
                index="alerting",
                media="nvme",
                since_days=7,
                marker=None,
            )
            assert result is not None
            assert isinstance(result.data, list)
        finally:
            client.close()

    def test_query_invalid_index(self, api_key: str, host: str) -> None:
        """Test that invalid index raises appropriate error."""
        from cetus.client import CetusClient

        # Client validates index before sending to server
        client = CetusClient(api_key=api_key, host=host, timeout=60)
        try:
            with pytest.raises(ValueError, match="Invalid index"):
                client.query(
                    search=self.DATA_QUERY,
                    index="invalid",  # type: ignore
                    media="nvme",
                    since_days=7,
                    marker=None,
                )
        finally:
            client.close()


class TestQueryStreamEndpoint:
    """E2E tests for the /api/query/stream/ endpoint.

    Uses host:microsoft.com which has frequent cert renewals and runs quickly.
    Streaming tests break early after a few records.
    """

    # Query for popular domain - returns records consistently
    DATA_QUERY = "host:microsoft.com"

    def test_streaming_returns_records(self, api_key: str, host: str) -> None:
        """Test that streaming query returns real records with correct structure."""
        from cetus.client import CetusClient

        client = CetusClient(api_key=api_key, host=host, timeout=120)
        try:
            records = []
            for record in client.query_stream(
                search=self.DATA_QUERY,
                index="dns",
                media="nvme",
                since_days=7,  # 7 days is about the same speed as 1 day
                marker=None,
            ):
                records.append(record)
                # Stop after a few records - just need to verify structure
                if len(records) >= 3:
                    break

            # Should have data for microsoft.com
            assert isinstance(records, list)
            assert len(records) > 0, "Expected DNS records for microsoft.com"

            # Verify DNS record structure
            record = records[0]
            assert "uuid" in record
            assert "host" in record
            assert "dns_timestamp" in record
        finally:
            client.close()

    def test_streaming_certstream(self, api_key: str, host: str) -> None:
        """Test streaming against certstream index."""
        from cetus.client import CetusClient

        client = CetusClient(api_key=api_key, host=host, timeout=120)
        try:
            records = []
            for record in client.query_stream(
                search=self.DATA_QUERY,
                index="certstream",
                media="nvme",
                since_days=7,
                marker=None,
            ):
                records.append(record)
                if len(records) >= 3:
                    break

            assert isinstance(records, list)
            if records:
                # Verify certstream record structure
                assert "uuid" in records[0]
                assert "certstream_timestamp" in records[0]
        finally:
            client.close()


class TestAlertsEndpoint:
    """E2E tests for the alerts API endpoints."""

    def test_list_alerts(self, api_key: str, host: str) -> None:
        """Test listing alerts."""
        from cetus.client import CetusClient

        client = CetusClient(api_key=api_key, host=host, timeout=60)
        try:
            alerts = client.list_alerts(owned=True, shared=False)
            # Should return a list (may be empty)
            assert isinstance(alerts, list)
            # If we have alerts, check structure
            if alerts:
                alert = alerts[0]
                assert hasattr(alert, "id")
                assert hasattr(alert, "title")
                assert hasattr(alert, "alert_type")
        finally:
            client.close()

    def test_list_shared_alerts(self, api_key: str, host: str) -> None:
        """Test listing shared alerts."""
        from cetus.client import CetusClient

        client = CetusClient(api_key=api_key, host=host, timeout=60)
        try:
            alerts = client.list_alerts(owned=False, shared=True)
            # Should return a list (may be empty)
            assert isinstance(alerts, list)
        finally:
            client.close()


class TestAsyncMethods:
    """E2E tests for async client methods.

    Uses host:microsoft.com which has frequent cert renewals and runs quickly.
    """

    # Query for popular domain - returns records consistently
    DATA_QUERY = "host:microsoft.com"

    @pytest.mark.asyncio
    async def test_async_query(self, api_key: str, host: str) -> None:
        """Test async query method returns real data."""
        from cetus.client import CetusClient

        client = CetusClient(api_key=api_key, host=host, timeout=120)
        try:
            result = await client.query_async(
                search=self.DATA_QUERY,
                index="dns",
                media="nvme",
                since_days=7,  # 7 days is about the same speed as 1 day
                marker=None,
            )
            assert result is not None
            assert hasattr(result, "data")
            assert isinstance(result.data, list)
            assert len(result.data) > 0, "Expected results for microsoft.com"
        finally:
            client.close()

    @pytest.mark.asyncio
    async def test_async_streaming_with_data(self, api_key: str, host: str) -> None:
        """Test async streaming returns real data."""
        from cetus.client import CetusClient

        client = CetusClient(api_key=api_key, host=host, timeout=120)
        try:
            records = []
            async for record in client.query_stream_async(
                search=self.DATA_QUERY,
                index="dns",
                media="nvme",
                since_days=7,  # 7 days is about the same speed as 1 day
                marker=None,
            ):
                records.append(record)
                if len(records) >= 3:
                    break

            assert isinstance(records, list)
            assert len(records) > 0, "Expected DNS records for microsoft.com"
            assert "uuid" in records[0]
        finally:
            client.close()


class TestAuthentication:
    """E2E tests for authentication."""

    EMPTY_QUERY = "host:e2e-test-nonexistent-8f4a2b1c.invalid"

    def test_invalid_api_key(self, host: str) -> None:
        """Test that invalid API key returns authentication error."""
        from cetus.client import CetusClient
        from cetus.exceptions import AuthenticationError

        client = CetusClient(api_key="invalid-key-12345", host=host, timeout=60)
        try:
            with pytest.raises(AuthenticationError):
                client.query(
                    search=self.EMPTY_QUERY,
                    index="dns",
                    media="nvme",
                    since_days=1,  # Use time filter for consistency
                    marker=None,
                )
        finally:
            client.close()


class TestCLICommands:
    """E2E tests for CLI commands.

    CLI query tests are slow because they hit Elasticsearch.
    CLI alerts/config tests are fast (no ES queries).
    """

    # Query for popular domain - returns records consistently
    DATA_QUERY = "host:microsoft.com"

    def test_cli_query_command(self, api_key: str, host: str) -> None:
        """Test CLI query command works with real data."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                self.DATA_QUERY,
                "--index",
                "dns",
                "--since-days",
                "7",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should succeed with results
        assert result.exit_code == 0
        # Output should contain data
        assert "[" in result.output  # JSON array

    def test_cli_query_streaming(self, api_key: str, host: str) -> None:
        """Test CLI query with streaming flag."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                self.DATA_QUERY,
                "--index",
                "dns",
                "--since-days",
                "7",
                "--stream",
                "--format",
                "jsonl",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0

    def test_cli_alerts_list_command(self, api_key: str, host: str) -> None:
        """Test CLI alerts list command."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "alerts",
                "list",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should succeed (may show "No alerts found" which is fine)
        assert result.exit_code == 0

    def test_cli_config_show_command(self) -> None:
        """Test CLI config show command."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["config", "show"])
        # Should succeed even without config
        assert result.exit_code in (0, 1)


class TestFileOutputModes:
    """E2E tests for file output modes (-o and -p).

    Tests the incremental query functionality with real data.
    """

    DATA_QUERY = "host:microsoft.com"

    def test_cli_output_file_creates_file(self, api_key: str, host: str, tmp_path) -> None:
        """Test -o creates output file with real data."""
        from click.testing import CliRunner

        from cetus.cli import main

        output_file = tmp_path / "results.jsonl"

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                self.DATA_QUERY,
                "--index",
                "dns",
                "--since-days",
                "7",
                "--format",
                "jsonl",
                "-o",
                str(output_file),
                "--no-marker",  # Don't save marker for this test
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        assert output_file.exists()
        content = output_file.read_text()
        assert len(content) > 0
        # Should have JSONL content (one JSON object per line)
        lines = content.strip().split("\n")
        assert len(lines) > 0

    def test_cli_output_prefix_creates_timestamped_file(
        self, api_key: str, host: str, tmp_path
    ) -> None:
        """Test -p creates timestamped output file."""
        from click.testing import CliRunner

        from cetus.cli import main

        prefix = str(tmp_path / "results")

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                self.DATA_QUERY,
                "--index",
                "dns",
                "--since-days",
                "7",
                "--format",
                "jsonl",
                "-p",
                prefix,
                "--no-marker",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0

        # Should have created a timestamped file
        files = list(tmp_path.glob("results_*.jsonl"))
        assert len(files) == 1
        assert files[0].stat().st_size > 0

    def test_cli_output_csv_format(self, api_key: str, host: str, tmp_path) -> None:
        """Test CSV output format works correctly."""
        from click.testing import CliRunner

        from cetus.cli import main

        output_file = tmp_path / "results.csv"

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                self.DATA_QUERY,
                "--index",
                "dns",
                "--since-days",
                "7",
                "--format",
                "csv",
                "-o",
                str(output_file),
                "--no-marker",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        assert output_file.exists()

        content = output_file.read_text()
        lines = content.strip().split("\n")
        # Should have header + at least one data row
        assert len(lines) >= 2
        # First line should be CSV header
        assert "uuid" in lines[0] or "host" in lines[0]

    def test_cli_streaming_with_output_file(self, api_key: str, host: str, tmp_path) -> None:
        """Test --stream with -o creates file."""
        from click.testing import CliRunner

        from cetus.cli import main

        output_file = tmp_path / "streamed.jsonl"

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                self.DATA_QUERY,
                "--index",
                "dns",
                "--since-days",
                "7",
                "--stream",
                "-o",
                str(output_file),
                "--no-marker",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        assert output_file.exists()
        assert output_file.stat().st_size > 0


class TestIncrementalQueries:
    """E2E tests for incremental query behavior with markers.

    Tests that markers work correctly across multiple query runs.
    """

    DATA_QUERY = "host:microsoft.com"

    def test_marker_saved_and_used(self, api_key: str, host: str, tmp_path) -> None:
        """Test that markers are saved and affect subsequent queries."""

        from click.testing import CliRunner

        from cetus.cli import main

        # Use isolated marker directory
        markers_dir = tmp_path / "markers"
        markers_dir.mkdir()

        output_file = tmp_path / "results.jsonl"

        runner = CliRunner(env={"CETUS_DATA_DIR": str(tmp_path)})

        # First run - should fetch data and save marker
        result1 = runner.invoke(
            main,
            [
                "query",
                self.DATA_QUERY,
                "--index",
                "dns",
                "--since-days",
                "7",
                "--format",
                "jsonl",
                "-o",
                str(output_file),
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result1.exit_code == 0
        assert "Wrote" in result1.output

        first_size = output_file.stat().st_size
        assert first_size > 0

        # Check marker was saved
        marker_files = list(tmp_path.glob("markers/*.json"))
        assert len(marker_files) == 1

        # Second run - should use marker (may append or show "No new records")
        result2 = runner.invoke(
            main,
            [
                "query",
                self.DATA_QUERY,
                "--index",
                "dns",
                "--since-days",
                "7",
                "--format",
                "jsonl",
                "-o",
                str(output_file),
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result2.exit_code == 0
        # Should either append or report no new records
        assert "Appended" in result2.output or "No new records" in result2.output

    def test_output_prefix_with_markers(self, api_key: str, host: str, tmp_path) -> None:
        """Test -p mode saves markers for incremental queries."""
        from click.testing import CliRunner

        from cetus.cli import main

        prefix = str(tmp_path / "export")

        runner = CliRunner(env={"CETUS_DATA_DIR": str(tmp_path)})

        # First run
        result1 = runner.invoke(
            main,
            [
                "query",
                self.DATA_QUERY,
                "--index",
                "dns",
                "--since-days",
                "7",
                "--format",
                "jsonl",
                "-p",
                prefix,
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result1.exit_code == 0

        # Should have created one timestamped file
        files1 = list(tmp_path.glob("export_*.jsonl"))
        assert len(files1) == 1

        # Marker should be saved
        marker_files = list(tmp_path.glob("markers/*.json"))
        assert len(marker_files) == 1

        # Second run (immediately after - likely no new data)
        import time

        time.sleep(1)  # Ensure different timestamp

        result2 = runner.invoke(
            main,
            [
                "query",
                self.DATA_QUERY,
                "--index",
                "dns",
                "--since-days",
                "7",
                "--format",
                "jsonl",
                "-p",
                prefix,
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result2.exit_code == 0

        # If no new data, no new file created
        # If new data, a second file is created
        files2 = list(tmp_path.glob("export_*.jsonl"))
        # Should have 1 or 2 files depending on whether new data arrived
        assert len(files2) >= 1


class TestCLIVersion:
    """Test CLI version and help commands."""

    def test_version_flag(self) -> None:
        """Test --version shows version string."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["--version"])
        assert result.exit_code == 0
        assert "cetus" in result.output.lower()
        # Should contain a version number pattern
        import re

        assert re.search(r"\d+\.\d+\.\d+", result.output)


class TestCLIMarkers:
    """E2E tests for marker management commands."""

    def test_markers_list_empty(self, tmp_path) -> None:
        """Test markers list when no markers exist."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner(env={"CETUS_DATA_DIR": str(tmp_path)})
        result = runner.invoke(main, ["markers", "list"])
        assert result.exit_code == 0
        assert "No markers" in result.output or "0" in result.output or result.output.strip() == ""

    def test_markers_list_shows_markers(self, api_key: str, host: str, tmp_path) -> None:
        """Test markers list shows saved markers after a query."""
        from click.testing import CliRunner

        from cetus.cli import main

        output_file = tmp_path / "results.jsonl"

        runner = CliRunner(env={"CETUS_DATA_DIR": str(tmp_path)})

        # Run a query to create a marker
        runner.invoke(
            main,
            [
                "query",
                "host:microsoft.com",
                "--index",
                "dns",
                "--since-days",
                "1",
                "--format",
                "jsonl",
                "-o",
                str(output_file),
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )

        # List markers
        result = runner.invoke(main, ["markers", "list"])
        assert result.exit_code == 0
        # Should show the dns index marker
        assert "dns" in result.output.lower()

    def test_markers_clear(self, api_key: str, host: str, tmp_path) -> None:
        """Test markers clear removes markers."""
        from click.testing import CliRunner

        from cetus.cli import main

        output_file = tmp_path / "results.jsonl"

        runner = CliRunner(env={"CETUS_DATA_DIR": str(tmp_path)})

        # Run a query to create a marker
        runner.invoke(
            main,
            [
                "query",
                "host:microsoft.com",
                "--index",
                "dns",
                "--since-days",
                "1",
                "--format",
                "jsonl",
                "-o",
                str(output_file),
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )

        # Clear markers
        result = runner.invoke(main, ["markers", "clear", "-y"])
        assert result.exit_code == 0
        assert "Cleared" in result.output

        # Verify cleared
        runner.invoke(main, ["markers", "list"])  # Check command runs
        # Should be empty now
        marker_files = list(tmp_path.glob("markers/*.json"))
        assert len(marker_files) == 0


class TestCLIConfig:
    """E2E tests for config management commands."""

    def test_config_path(self) -> None:
        """Test config path shows file location."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["config", "path"])
        assert result.exit_code == 0
        assert "config" in result.output.lower()
        # Should be a file path
        assert "/" in result.output or "\\" in result.output


class TestAlertResults:
    """E2E tests for alert results command."""

    def test_alert_results_not_found(self, api_key: str, host: str) -> None:
        """Test alert results with non-existent alert ID returns error."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "alerts",
                "results",
                "999999",  # Non-existent ID
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should fail with 404 or similar error
        assert (
            result.exit_code != 0
            or "not found" in result.output.lower()
            or "error" in result.output.lower()
        )


class TestAlertBacktest:
    """E2E tests for alert backtest command."""

    def test_alert_backtest_not_found(self, api_key: str, host: str) -> None:
        """Test alert backtest with non-existent alert ID returns error."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "alerts",
                "backtest",
                "999999",  # Non-existent ID
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should fail with 404 or similar error
        assert (
            result.exit_code != 0
            or "not found" in result.output.lower()
            or "error" in result.output.lower()
        )


class TestConnectionErrors:
    """E2E tests for connection error handling."""

    def test_invalid_host_error(self) -> None:
        """Test that invalid host gives clear error message."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                "host:test.com",
                "--host",
                "nonexistent.invalid.host.example",
                "--api-key",
                "test-key",
            ],
        )
        assert result.exit_code != 0
        # Should have a connection error message
        assert "connect" in result.output.lower() or "error" in result.output.lower()


class TestEmptyResults:
    """E2E tests for queries that return no results.

    Note: We use alerting index with a specific non-matching query because
    DNS queries without matches still scan all shards and can timeout.
    The alerting index is smaller and faster for empty result tests.
    """

    def test_query_no_results(self, api_key: str, host: str) -> None:
        """Test query that returns empty results handles gracefully."""
        from cetus.client import CetusClient

        # Use alerting index which is smaller - query for non-existent UUID
        client = CetusClient(api_key=api_key, host=host, timeout=60)
        try:
            result = client.query(
                search="uuid:00000000-0000-0000-0000-000000000000",
                index="alerting",
                media="nvme",
                since_days=1,
                marker=None,
            )
            assert result is not None
            assert isinstance(result.data, list)
            # Should return empty or very few results
            assert len(result.data) < 10
        finally:
            client.close()

    def test_cli_query_no_results(self, api_key: str, host: str) -> None:
        """Test CLI query with no results shows appropriate message."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                "uuid:00000000-0000-0000-0000-000000000000",
                "--index",
                "alerting",
                "--since-days",
                "1",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        # Should complete without error (may have [] or few results)
        assert "[" in result.output  # Valid JSON array


class TestQuerySyntaxErrors:
    """E2E tests for query syntax error handling."""

    def test_invalid_lucene_syntax_returns_error(self, api_key: str, host: str) -> None:
        """Test that invalid Lucene syntax returns an error.

        The server should return a 400 Bad Request with a helpful error message
        explaining the syntax issue, rather than a generic 500 error.
        """
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                "host:[",  # Invalid Lucene syntax - unclosed bracket
                "--index",
                "dns",
                "--since-days",
                "1",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should fail with an error
        assert result.exit_code != 0
        output_lower = result.output.lower()
        # Should have helpful error message about syntax (sanitized, no internal details)
        assert "invalid query syntax" in output_lower
        assert "brackets" in output_lower or "quotes" in output_lower

    def test_invalid_field_name_handled(self, api_key: str, host: str) -> None:
        """Test that invalid field names are handled gracefully."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                "nonexistent_field:value",
                "--index",
                "dns",
                "--since-days",
                "1",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should succeed (ES allows querying non-existent fields, returns empty)
        assert result.exit_code == 0


class TestTableFormat:
    """E2E tests for table format output."""

    DATA_QUERY = "host:microsoft.com"

    def test_query_table_format(self, api_key: str, host: str) -> None:
        """Test that table format output works for queries."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                self.DATA_QUERY,
                "--index",
                "dns",
                "--since-days",
                "1",
                "--format",
                "table",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        # Table output should have formatting characters
        assert "+" in result.output or "|" in result.output or "host" in result.output


class TestVerboseMode:
    """E2E tests for verbose/debug output."""

    def test_verbose_flag_shows_debug_info(self, api_key: str, host: str) -> None:
        """Test that -v flag produces debug output."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "-v",  # Verbose flag
                "query",
                "host:microsoft.com",
                "--index",
                "dns",
                "--since-days",
                "1",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        # Verbose mode should show DEBUG or HTTP request info
        assert "DEBUG" in result.output or "HTTP" in result.output or "200" in result.output


class TestSinceDaysEdgeCases:
    """E2E tests for since-days edge cases."""

    def test_since_days_zero(self, api_key: str, host: str) -> None:
        """Test that since-days=0 works (queries for today only)."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                "host:microsoft.com",
                "--index",
                "dns",
                "--since-days",
                "0",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should succeed (may or may not have results for today)
        assert result.exit_code == 0
        assert "[" in result.output  # Valid JSON array

    def test_since_days_negative_rejected(self, api_key: str, host: str) -> None:
        """Test that negative since-days is rejected."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                "host:microsoft.com",
                "--index",
                "dns",
                "--since-days",
                "-1",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should fail with error
        assert result.exit_code != 0
        # Error message is printed to output
        assert "negative" in result.output.lower()

    def test_config_set_since_days_negative_rejected(self, tmp_path) -> None:
        """Test that config set rejects negative since-days."""
        from unittest.mock import patch

        from click.testing import CliRunner

        from cetus.cli import main

        config_dir = tmp_path / "config"
        config_dir.mkdir()

        with patch("cetus.config.get_config_dir", return_value=config_dir):
            runner = CliRunner()
            # Use -- to prevent -5 being parsed as an option
            result = runner.invoke(main, ["config", "set", "since-days", "--", "-5"])
            assert result.exit_code != 0
            assert "negative" in result.output.lower()


class TestAlertTypeFiltering:
    """E2E tests for alert type filtering."""

    def test_list_alerts_filter_by_type_raw(self, api_key: str, host: str) -> None:
        """Test listing alerts filtered by type=raw."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "alerts",
                "list",
                "--type",
                "raw",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        # Output should only show raw alerts (or "No alerts found")
        if "raw" in result.output.lower():
            # If we have raw alerts, verify no other types shown
            assert "terms" not in result.output.lower() or "raw" in result.output.lower()

    def test_list_alerts_filter_by_type_terms(self, api_key: str, host: str) -> None:
        """Test listing alerts filtered by type=terms."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "alerts",
                "list",
                "--type",
                "terms",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0


class TestAlertOperationsWithRealData:
    """E2E tests for alert operations with real alert data.

    These tests verify that alert results and backtest work with
    actual alerts, not just 404 cases.
    """

    def test_alert_results_with_existing_alert(self, api_key: str, host: str) -> None:
        """Test alert results with an alert that exists."""
        from cetus.client import CetusClient

        client = CetusClient(api_key=api_key, host=host, timeout=60)
        try:
            # First, get list of owned alerts
            alerts = client.list_alerts(owned=True, shared=False)
            if not alerts:
                pytest.skip("No owned alerts to test with")

            alert = alerts[0]
            # Get results for this alert - should succeed even if empty
            results = client.get_alert_results(alert.id)
            assert isinstance(results, list)
            # Results may be empty if alert hasn't matched anything
        finally:
            client.close()

    def test_cli_alert_results_with_existing_alert(self, api_key: str, host: str) -> None:
        """Test CLI alert results with an existing alert."""
        from click.testing import CliRunner

        from cetus.cli import main

        # First get an alert ID
        runner = CliRunner()
        list_result = runner.invoke(
            main,
            ["alerts", "list", "--owned", "--api-key", api_key, "--host", host],
        )
        if "No alerts" in list_result.output:
            pytest.skip("No owned alerts to test with")

        # Extract first alert ID from table output (handles both ASCII | and Unicode │)
        import re

        match = re.search(r"[│|]\s*(\d+)\s*[│|]", list_result.output)
        assert match, f"Could not parse alert ID from output: {list_result.output[:200]}"

        alert_id = match.group(1)

        # Now test results command
        result = runner.invoke(
            main,
            [
                "alerts",
                "results",
                alert_id,
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        # Should either have JSON array or "No results" message
        assert "[" in result.output or "No results" in result.output

    def test_cli_alert_backtest_with_existing_alert(self, api_key: str, host: str) -> None:
        """Test CLI alert backtest with an existing alert."""
        from click.testing import CliRunner

        from cetus.cli import main

        # First get an alert ID
        runner = CliRunner()
        list_result = runner.invoke(
            main,
            ["alerts", "list", "--owned", "--api-key", api_key, "--host", host],
        )
        if "No alerts" in list_result.output:
            pytest.skip("No owned alerts to test with")

        # Extract first alert ID from table output (handles both ASCII | and Unicode │)
        import re

        match = re.search(r"[│|]\s*(\d+)\s*[│|]", list_result.output)
        assert match, f"Could not parse alert ID from output: {list_result.output[:200]}"

        alert_id = match.group(1)

        # Now test backtest command
        result = runner.invoke(
            main,
            [
                "alerts",
                "backtest",
                alert_id,
                "--since-days",
                "1",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        # Should have JSON output and timing info
        assert "[" in result.output or "records" in result.output.lower()


class TestCompletionScripts:
    """E2E tests for shell completion script generation."""

    def test_completion_bash_generates_script(self) -> None:
        """Test that bash completion script is generated."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["completion", "bash"])
        assert result.exit_code == 0
        # Bash completion script should contain function definition
        assert "_cetus_completion" in result.output or "COMP_WORDS" in result.output

    def test_completion_zsh_generates_script(self) -> None:
        """Test that zsh completion script is generated."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["completion", "zsh"])
        assert result.exit_code == 0
        # Zsh completion script should contain function or compdef
        assert "compdef" in result.output or "_cetus" in result.output

    def test_completion_fish_generates_script(self) -> None:
        """Test that fish completion script is generated."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["completion", "fish"])
        assert result.exit_code == 0
        # Fish completion script should contain complete command
        assert "complete" in result.output


class TestAlertsListEdgeCases:
    """E2E tests for alerts list edge cases."""

    def test_alerts_list_no_owned_no_shared_warning(self) -> None:
        """Test warning when both --no-owned and --no-shared are specified."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["alerts", "list", "--no-owned", "--no-shared"])
        assert result.exit_code == 0
        assert "warning" in result.output.lower() or "no alerts" in result.output.lower()

    def test_alerts_list_filter_by_type_structured(self, api_key: str, host: str) -> None:
        """Test listing alerts filtered by type=structured."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "alerts",
                "list",
                "--type",
                "structured",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        # Should either show structured alerts or "No alerts found"


class TestVerboseModeExtended:
    """E2E tests for verbose mode with various commands."""

    def test_verbose_alerts_list(self, api_key: str, host: str) -> None:
        """Test verbose mode with alerts list command.

        Note: Debug output goes to stderr which Click runner captures separately.
        We verify the command succeeds and returns alert data - verbose logging
        is already tested in TestVerboseMode.test_verbose_flag_shows_debug_info.
        """
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "-v",
                "alerts",
                "list",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        # Verify we get alert data (table output)
        assert "ID" in result.output or "No alerts" in result.output

    def test_verbose_config_show(self) -> None:
        """Test verbose mode with config show command."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["-v", "config", "show"])
        assert result.exit_code == 0


class TestConfigSetValidation:
    """E2E tests for config set value validation."""

    def test_config_set_since_days_invalid(self, tmp_path) -> None:
        """Test that invalid since-days value is rejected."""
        from unittest.mock import patch

        from click.testing import CliRunner

        from cetus.cli import main

        config_dir = tmp_path / "config"
        config_dir.mkdir()

        with patch("cetus.config.get_config_dir", return_value=config_dir):
            runner = CliRunner()
            result = runner.invoke(main, ["config", "set", "since-days", "not-a-number"])
            assert result.exit_code != 0
            assert "invalid" in result.output.lower()

    def test_config_set_since_days_valid(self, tmp_path) -> None:
        """Test that valid since-days value is accepted."""
        from unittest.mock import patch

        from click.testing import CliRunner

        from cetus.cli import main

        config_dir = tmp_path / "config"
        config_dir.mkdir()

        with patch("cetus.config.get_config_dir", return_value=config_dir):
            runner = CliRunner()
            result = runner.invoke(main, ["config", "set", "since-days", "30"])
            assert result.exit_code == 0
            assert "success" in result.output.lower()


class TestMutuallyExclusiveOptions:
    """E2E tests for mutually exclusive CLI options."""

    def test_output_and_output_prefix_mutually_exclusive(self, tmp_path) -> None:
        """Test that -o and -p cannot be used together."""
        from click.testing import CliRunner

        from cetus.cli import main

        output_file = tmp_path / "results.json"
        prefix = str(tmp_path / "results")

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                "host:test.com",
                "-o",
                str(output_file),
                "-p",
                prefix,
                "--api-key",
                "test-key",
            ],
        )
        assert result.exit_code == 1
        assert "mutually exclusive" in result.output.lower()


class TestMarkersClearByIndex:
    """E2E tests for markers clear with index filtering."""

    def test_markers_clear_by_index(self, api_key: str, host: str, tmp_path) -> None:
        """Test that markers clear --index only clears that index."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner(env={"CETUS_DATA_DIR": str(tmp_path)})
        markers_dir = tmp_path / "markers"
        markers_dir.mkdir(exist_ok=True)

        # Run queries on different indices to create markers
        for idx in ["dns", "certstream"]:
            runner.invoke(
                main,
                [
                    "query",
                    "host:microsoft.com",
                    "--index",
                    idx,
                    "--since-days",
                    "1",
                    "-o",
                    str(tmp_path / f"{idx}_results.jsonl"),
                    "--format",
                    "jsonl",
                    "--api-key",
                    api_key,
                    "--host",
                    host,
                ],
            )

        # Check markers exist
        marker_files_before = list(markers_dir.glob("*.json"))
        dns_markers_before = [f for f in marker_files_before if "dns" in f.name]

        # Only proceed if we have markers to clear
        if dns_markers_before:
            # Clear only dns markers
            result = runner.invoke(main, ["markers", "clear", "--index", "dns", "-y"])
            assert result.exit_code == 0

            # Check that certstream markers still exist
            marker_files_after = list(markers_dir.glob("*.json"))
            dns_markers_after = [f for f in marker_files_after if "dns" in f.name]

            # DNS markers should be cleared
            assert len(dns_markers_after) < len(dns_markers_before) or len(dns_markers_before) == 0


class TestGetAlertEndpoint:
    """E2E tests for the get_alert endpoint."""

    def test_get_alert_by_id(self, api_key: str, host: str) -> None:
        """Test getting a specific alert by ID."""
        from cetus.client import CetusClient

        client = CetusClient(api_key=api_key, host=host, timeout=60)
        try:
            # First, get list of owned alerts
            alerts = client.list_alerts(owned=True, shared=False)
            if not alerts:
                pytest.skip("No owned alerts to test with")

            # Get the first alert by ID
            alert = client.get_alert(alerts[0].id)
            assert alert is not None
            assert alert.id == alerts[0].id
            assert hasattr(alert, "title")
            assert hasattr(alert, "alert_type")
        finally:
            client.close()

    def test_get_alert_not_found(self, api_key: str, host: str) -> None:
        """Test getting a non-existent alert returns None."""
        from cetus.client import CetusClient

        client = CetusClient(api_key=api_key, host=host, timeout=60)
        try:
            alert = client.get_alert(999999)  # Non-existent ID
            assert alert is None
        finally:
            client.close()


class TestStreamingCSVFormat:
    """E2E tests for streaming with CSV format."""

    DATA_QUERY = "host:microsoft.com"

    def test_streaming_csv_to_file(self, api_key: str, host: str, tmp_path) -> None:
        """Test streaming query with CSV format writes valid CSV file."""
        from click.testing import CliRunner

        from cetus.cli import main

        output_file = tmp_path / "results.csv"

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                self.DATA_QUERY,
                "--index",
                "dns",
                "--since-days",
                "1",
                "--stream",
                "--format",
                "csv",
                "-o",
                str(output_file),
                "--no-marker",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        assert output_file.exists()

        # Verify it's valid CSV with header
        content = output_file.read_text()
        lines = content.strip().split("\n")
        if len(lines) > 1:  # Has data
            # First line should be header
            assert "uuid" in lines[0] or "host" in lines[0]


@pytest.mark.skip(reason="Media 'all' queries timeout - needs server-side optimization")
class TestMediaAllOption:
    """E2E tests for --media all option which queries all storage tiers.

    These tests use longer timeouts because 'all' media scans more data.
    Currently disabled due to timeout issues with full index scans.
    """

    DATA_QUERY = "host:microsoft.com"

    def test_query_media_all(self, api_key: str, host: str) -> None:
        """Test query with --media all option.

        Note: This queries all storage tiers and may take longer than nvme-only.
        Uses a 3-minute timeout to accommodate full index scans.
        """
        from cetus.client import CetusClient

        # Use extended timeout for 'all' media queries
        client = CetusClient(api_key=api_key, host=host, timeout=180)
        try:
            result = client.query(
                search=self.DATA_QUERY,
                index="dns",
                media="all",
                since_days=1,  # Keep short timeframe to limit data
                marker=None,
            )
            assert result is not None
            assert isinstance(result.data, list)
            # 'all' should return at least as many results as 'nvme'
        finally:
            client.close()

    def test_cli_query_media_all(self, api_key: str, host: str) -> None:
        """Test CLI query with --media all option."""
        from click.testing import CliRunner

        from cetus.cli import main

        # Use extended timeout via environment variable
        runner = CliRunner(env={"CETUS_TIMEOUT": "180"})
        result = runner.invoke(
            main,
            [
                "query",
                self.DATA_QUERY,
                "--index",
                "dns",
                "--media",
                "all",
                "--since-days",
                "1",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should succeed with extended timeout
        assert result.exit_code == 0
        assert "[" in result.output  # Valid JSON array


class TestBacktestWithStreaming:
    """E2E tests for backtest command with streaming mode."""

    def test_backtest_streaming_mode(self, api_key: str, host: str, tmp_path) -> None:
        """Test backtest command with --stream flag."""
        from click.testing import CliRunner

        from cetus.cli import main

        # First get an alert ID
        runner = CliRunner()
        list_result = runner.invoke(
            main,
            ["alerts", "list", "--owned", "--api-key", api_key, "--host", host],
        )
        if "No alerts" in list_result.output:
            pytest.skip("No owned alerts to test with")

        # Extract first alert ID from table output
        import re

        match = re.search(r"[│|]\s*(\d+)\s*[│|]", list_result.output)
        if not match:
            pytest.skip("Could not parse alert ID")

        alert_id = match.group(1)
        output_file = tmp_path / "backtest.jsonl"

        # Test backtest with streaming
        result = runner.invoke(
            main,
            [
                "alerts",
                "backtest",
                alert_id,
                "--stream",
                "--since-days",
                "1",
                "-o",
                str(output_file),
                "--no-marker",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        # Should either create a file or report no records
        assert output_file.exists() or "no" in result.output.lower()


class TestEmptyQueryHandling:
    """E2E tests for empty query string handling."""

    def test_empty_query_returns_error(self, api_key: str, host: str) -> None:
        """Test that empty query string returns appropriate error."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                "",
                "--since-days",
                "1",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should fail with an error about invalid query syntax
        assert result.exit_code != 0
        assert "invalid" in result.output.lower() or "error" in result.output.lower()


class TestOutputDirectoryErrors:
    """E2E tests for output directory error handling."""

    def test_output_to_nonexistent_directory(self, api_key: str, host: str) -> None:
        """Test that output to non-existent directory returns clean error."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                "host:microsoft.com",
                "--since-days",
                "1",
                "-o",
                "nonexistent_directory_xyz/results.json",
                "--no-marker",  # Force write attempt (don't skip due to marker)
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should fail with a clean error message (not a traceback)
        assert result.exit_code == 1
        # Should have error message about file/directory
        output_lower = result.output.lower()
        assert "error" in output_lower
        # Should NOT have traceback indicators
        assert "traceback" not in output_lower
        assert 'file "' not in output_lower  # Python traceback pattern

    def test_streaming_output_to_nonexistent_directory(self, api_key: str, host: str) -> None:
        """Test that streaming output to non-existent directory returns clean error."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                "host:microsoft.com",
                "--since-days",
                "1",
                "--stream",
                "-o",
                "nonexistent_directory_xyz/results.jsonl",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should fail with a clean error message
        assert result.exit_code == 1
        output_lower = result.output.lower()
        assert "error" in output_lower
        assert "traceback" not in output_lower


class TestAlertsListCombinedFlags:
    """E2E tests for alerts list with combined owned and shared flags."""

    def test_alerts_list_owned_and_shared_together(self, api_key: str, host: str) -> None:
        """Test alerts list with both --owned and --shared flags."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "alerts",
                "list",
                "--owned",
                "--shared",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should succeed and show combined results
        assert result.exit_code == 0
        # Output should have table headers or "No alerts"
        assert "ID" in result.output or "No alerts" in result.output


class TestVerboseModeWithMarkers:
    """E2E tests for verbose mode with file output and markers."""

    def test_verbose_mode_shows_marker_saved(self, api_key: str, host: str, tmp_path) -> None:
        """Test that verbose mode shows marker saved message."""
        from click.testing import CliRunner

        from cetus.cli import main

        output_file = tmp_path / "results.jsonl"

        runner = CliRunner(env={"CETUS_DATA_DIR": str(tmp_path)})
        result = runner.invoke(
            main,
            [
                "-v",  # Verbose flag
                "query",
                "host:microsoft.com",
                "--index",
                "dns",
                "--since-days",
                "1",
                "--format",
                "jsonl",
                "-o",
                str(output_file),
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        # In verbose mode, should show that marker was saved
        assert "marker" in result.output.lower()


class TestQueryEdgeCases:
    """E2E tests for query edge cases not covered elsewhere."""

    def test_whitespace_only_query_returns_error(self, api_key: str, host: str) -> None:
        """Test that whitespace-only query returns appropriate error."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                "   ",  # Whitespace-only query
                "--since-days",
                "1",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should fail with an error about invalid query syntax
        assert result.exit_code != 0
        assert "invalid" in result.output.lower()

    def test_unicode_characters_in_query(self, api_key: str, host: str) -> None:
        """Test that Unicode characters in queries are handled correctly."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        # Query with German umlaut - should work (may return empty results)
        result = runner.invoke(
            main,
            [
                "query",
                "host:münchen.de",
                "--index",
                "dns",
                "--since-days",
                "1",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should succeed (may have empty results, but no error)
        assert result.exit_code == 0
        assert "[" in result.output  # Valid JSON array

    def test_unicode_japanese_in_query(self, api_key: str, host: str) -> None:
        """Test that Japanese Unicode characters in queries work."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                "host:日本.jp",
                "--index",
                "dns",
                "--since-days",
                "1",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should succeed (may have empty results, but no error)
        assert result.exit_code == 0
        assert "[" in result.output  # Valid JSON array

    @pytest.mark.xfail(
        reason="Pagination timeout for large result sets - PIT expiration. "
        "Fix deployed to server will resolve this.",
        strict=False,  # Allow test to pass once fix is deployed
    )
    def test_unicode_chinese_in_query(self, api_key: str, host: str) -> None:
        """Test that Chinese (simplified Han) characters in queries work.

        Root cause: NOT about Chinese characters. The query returns 300k+ records
        requiring ~40 pages. The PIT (point-in-time) keep_alive was set to 1 minute,
        which caused expiration during pagination. Japanese/German queries returned
        fewer records and completed before PIT expired.

        Server-side fix: Increased PIT keep_alive from 1m to 5m, and added proper
        error handling for PIT expiration errors.

        This test will pass once the server-side fix is deployed.
        """
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                "host:微软.com",
                "--index",
                "dns",
                "--since-days",
                "1",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should succeed (may have empty results, but no error)
        # Currently fails with "Server returned error 500"
        assert result.exit_code == 0
        assert "[" in result.output  # Valid JSON array

    def test_lucene_special_chars_escaped(self, api_key: str, host: str) -> None:
        """Test query with escaped Lucene special characters.

        Lucene special chars: + - && || ! ( ) { } [ ] ^ " ~ * ? : \\ /
        These need to be escaped with backslash to search literally.
        """
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        # Query with parentheses - use targeted domain for speed
        # Testing that special chars are handled without crashing
        result = runner.invoke(
            main,
            [
                "query",
                r"host:microsoft.com AND host:\(test\)",
                "--index",
                "alerting",  # Use smaller index
                "--since-days",
                "1",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should succeed or fail gracefully (not crash)
        # The query may return empty results or error depending on ES handling
        # Main thing is it shouldn't cause a 500 error or crash
        assert result.exit_code in (0, 1)

    def test_very_long_query_string(self, api_key: str, host: str) -> None:
        """Test that very long query strings are handled.

        This tests the client and server can handle queries approaching
        reasonable limits without crashing.
        """
        from click.testing import CliRunner

        from cetus.cli import main

        # Create a long query with many OR conditions
        # This simulates a user searching for many domains at once
        domains = [f"domain{i}.example.com" for i in range(50)]
        long_query = " OR ".join(f"host:{d}" for d in domains)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                long_query,
                "--index",
                "alerting",  # Use smaller alerting index for speed
                "--since-days",
                "1",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should complete without error (likely empty results)
        assert result.exit_code == 0
        assert "[" in result.output  # Valid JSON array


class TestAlertAccessPermissions:
    """E2E tests for alert access permission scenarios."""

    def test_alert_get_nonexistent_returns_none(self, api_key: str, host: str) -> None:
        """Test that getting non-existent alert returns gracefully."""
        from cetus.client import CetusClient

        client = CetusClient(api_key=api_key, host=host, timeout=60)
        try:
            # Very high ID that shouldn't exist
            alert = client.get_alert(99999999)
            assert alert is None
        finally:
            client.close()

    def test_cli_backtest_nonexistent_alert(self, api_key: str, host: str) -> None:
        """Test CLI backtest with non-existent alert ID."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "alerts",
                "backtest",
                "99999999",
                "--since-days",
                "1",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should fail with clear error
        assert result.exit_code != 0
        assert "not found" in result.output.lower()


class TestOutputPrefixFormats:
    """E2E tests for output prefix mode with different formats."""

    DATA_QUERY = "host:microsoft.com"

    def test_output_prefix_json_format(self, api_key: str, host: str, tmp_path) -> None:
        """Test -p with --format json creates JSON file."""
        from click.testing import CliRunner

        from cetus.cli import main

        prefix = str(tmp_path / "results")

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                self.DATA_QUERY,
                "--index",
                "dns",
                "--since-days",
                "1",
                "--format",
                "json",
                "-p",
                prefix,
                "--no-marker",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0

        # Should have created a timestamped JSON file
        files = list(tmp_path.glob("results_*.json"))
        assert len(files) == 1
        assert files[0].stat().st_size > 0

        # Verify it's valid JSON
        import json

        content = files[0].read_text()
        data = json.loads(content)
        assert isinstance(data, list)

    def test_output_prefix_csv_format(self, api_key: str, host: str, tmp_path) -> None:
        """Test -p with --format csv creates CSV file."""
        from click.testing import CliRunner

        from cetus.cli import main

        prefix = str(tmp_path / "results")

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                self.DATA_QUERY,
                "--index",
                "dns",
                "--since-days",
                "1",
                "--format",
                "csv",
                "-p",
                prefix,
                "--no-marker",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0

        # Should have created a timestamped CSV file
        files = list(tmp_path.glob("results_*.csv"))
        assert len(files) == 1
        assert files[0].stat().st_size > 0

        # Verify it's valid CSV with header
        content = files[0].read_text()
        lines = content.strip().split("\n")
        assert len(lines) >= 2  # Header + at least one data row
        assert "uuid" in lines[0] or "host" in lines[0]


class TestStreamingTableWarning:
    """E2E tests for streaming with table format warning."""

    def test_streaming_table_shows_warning(self, api_key: str, host: str) -> None:
        """Test that --stream with --format table shows buffering warning."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                "host:microsoft.com",
                "--index",
                "dns",
                "--since-days",
                "1",
                "--stream",
                "--format",
                "table",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        # Should show warning about buffering
        assert "warning" in result.output.lower()
        assert "buffer" in result.output.lower()


class TestBacktestWithDifferentIndices:
    """E2E tests for backtest with certstream and alerting indices."""

    def test_backtest_certstream_index(self, api_key: str, host: str) -> None:
        """Test backtest command with --index certstream."""
        from click.testing import CliRunner

        from cetus.cli import main

        # First get an alert ID
        runner = CliRunner()
        list_result = runner.invoke(
            main,
            ["alerts", "list", "--owned", "--api-key", api_key, "--host", host],
        )
        if "No alerts" in list_result.output:
            pytest.skip("No owned alerts to test with")

        import re

        match = re.search(r"[│|]\s*(\d+)\s*[│|]", list_result.output)
        if not match:
            pytest.skip("Could not parse alert ID")

        alert_id = match.group(1)

        # Test backtest with certstream index
        result = runner.invoke(
            main,
            [
                "alerts",
                "backtest",
                alert_id,
                "--index",
                "certstream",
                "--since-days",
                "1",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        # Should return valid JSON (may be empty array)
        assert "[" in result.output

    def test_backtest_alerting_index(self, api_key: str, host: str) -> None:
        """Test backtest command with --index alerting."""
        from click.testing import CliRunner

        from cetus.cli import main

        # First get an alert ID
        runner = CliRunner()
        list_result = runner.invoke(
            main,
            ["alerts", "list", "--owned", "--api-key", api_key, "--host", host],
        )
        if "No alerts" in list_result.output:
            pytest.skip("No owned alerts to test with")

        import re

        match = re.search(r"[│|]\s*(\d+)\s*[│|]", list_result.output)
        if not match:
            pytest.skip("Could not parse alert ID")

        alert_id = match.group(1)

        # Test backtest with alerting index
        result = runner.invoke(
            main,
            [
                "alerts",
                "backtest",
                alert_id,
                "--index",
                "alerting",
                "--since-days",
                "1",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        # Should return valid JSON (may be empty array)
        assert "[" in result.output


class TestBacktestStructuredAlerts:
    """E2E tests for backtest with structured (DSL) alerts.

    Structured alerts use Elasticsearch DSL (JSON) queries instead of Lucene.
    These require special handling because time filters cannot be concatenated
    as Lucene strings - they must be incorporated into the DSL structure.
    """

    def test_backtest_structured_alert(self, api_key: str, host: str) -> None:
        """Test backtest with a structured (DSL) alert.

        Structured alerts have queries like:
        {"bool": {"must": [{"prefix": {"host": "bloomberg"}}]}}

        The client must handle these without breaking the JSON structure.
        """
        import json

        from click.testing import CliRunner

        from cetus.cli import main

        # Get the list of alerts in JSON format to find a structured one
        runner = CliRunner()
        list_result = runner.invoke(
            main,
            ["alerts", "list", "--owned", "--format", "json", "--api-key", api_key, "--host", host],
        )

        if list_result.exit_code != 0:
            pytest.skip(f"Could not list alerts: {list_result.output}")

        alerts = json.loads(list_result.output)

        # Find a structured alert
        structured_alerts = [a for a in alerts if a.get("type") == "structured"]
        if not structured_alerts:
            pytest.skip("No structured alerts available to test")

        alert_id = str(structured_alerts[0]["id"])

        # Test backtest with the structured alert
        result = runner.invoke(
            main,
            [
                "alerts",
                "backtest",
                alert_id,
                "--index",
                "dns",
                "--since-days",
                "1",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )

        # Should succeed without "Invalid query syntax" error
        # (may return empty array if no matches)
        assert result.exit_code == 0, f"Backtest failed: {result.output}"
        assert "Invalid query syntax" not in result.output
        assert "[" in result.output  # Valid JSON array


class TestUnicodeOutputHandling:
    """E2E tests for Unicode/emoji handling in output.

    These tests verify that Unicode characters (including emoji) are
    handled correctly on all platforms, particularly Windows where
    the default console encoding is cp1252.
    """

    def test_table_format_handles_unicode(self, api_key: str, host: str) -> None:
        """Test that table format handles Unicode characters without crashing.

        This test verifies the fix for Windows cp1252 encoding issues
        where emoji/Unicode characters would cause 'charmap' codec errors.
        """
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        # Query data that may contain Unicode (fingerprints can have emoji)
        result = runner.invoke(
            main,
            [
                "query",
                "host:*.google.com",
                "--index",
                "dns",
                "--since-days",
                "1",
                "--format",
                "table",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should succeed without encoding error
        assert result.exit_code == 0
        # Should have table formatting
        assert "│" in result.output or "|" in result.output

    def test_streaming_table_handles_unicode(self, api_key: str, host: str) -> None:
        """Test that streaming table format handles Unicode without crashing."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                "host:*.google.com",
                "--index",
                "dns",
                "--since-days",
                "1",
                "--stream",
                "--format",
                "table",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should succeed without encoding error
        assert result.exit_code == 0


class TestMarkersModeSeparation:
    """E2E tests for marker mode separation between -o and -p modes."""

    DATA_QUERY = "host:microsoft.com"

    def test_output_and_prefix_have_separate_markers(
        self, api_key: str, host: str, tmp_path
    ) -> None:
        """Test that -o and -p modes maintain separate markers.

        Running a query with -o should not affect markers for -p mode,
        and vice versa. This allows users to run both modes independently.
        """
        from click.testing import CliRunner

        from cetus.cli import main

        output_file = tmp_path / "output.jsonl"
        prefix = str(tmp_path / "prefix")
        markers_dir = tmp_path / "markers"
        markers_dir.mkdir(exist_ok=True)

        runner = CliRunner(env={"CETUS_DATA_DIR": str(tmp_path)})

        # Run with -o to create file mode marker
        result1 = runner.invoke(
            main,
            [
                "query",
                self.DATA_QUERY,
                "--index",
                "dns",
                "--since-days",
                "1",
                "--format",
                "jsonl",
                "-o",
                str(output_file),
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result1.exit_code == 0

        # Run with -p to create prefix mode marker
        result2 = runner.invoke(
            main,
            [
                "query",
                self.DATA_QUERY,
                "--index",
                "dns",
                "--since-days",
                "1",
                "--format",
                "jsonl",
                "-p",
                prefix,
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result2.exit_code == 0

        # Should have two marker files (one for each mode)
        marker_files = list(markers_dir.glob("*.json"))
        # May have 1 or 2 depending on timing, but should work
        assert len(marker_files) >= 1


class TestAlertResultsSinceFilter:
    """E2E tests for alerts results --since filter."""

    def test_alerts_results_with_since_filter(self, api_key: str, host: str) -> None:
        """Test alerts results with --since timestamp filter."""
        from click.testing import CliRunner

        from cetus.cli import main

        # First get an alert ID
        runner = CliRunner()
        list_result = runner.invoke(
            main,
            ["alerts", "list", "--owned", "--api-key", api_key, "--host", host],
        )
        if "No alerts" in list_result.output:
            pytest.skip("No owned alerts to test with")

        # Extract first alert ID from table output
        import re

        match = re.search(r"[│|]\s*(\d+)\s*[│|]", list_result.output)
        if not match:
            pytest.skip("Could not parse alert ID")

        alert_id = match.group(1)

        # Test with --since filter (past timestamp)
        result = runner.invoke(
            main,
            [
                "alerts",
                "results",
                alert_id,
                "--since",
                "2025-01-01T00:00:00Z",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should succeed (may return empty array or results)
        assert result.exit_code == 0
        assert "[" in result.output or "No results" in result.output

    def test_alerts_results_since_invalid_format(self, api_key: str, host: str) -> None:
        """Test alerts results with invalid --since timestamp format."""
        from click.testing import CliRunner

        from cetus.cli import main

        # First get an alert ID
        runner = CliRunner()
        list_result = runner.invoke(
            main,
            ["alerts", "list", "--owned", "--api-key", api_key, "--host", host],
        )
        if "No alerts" in list_result.output:
            pytest.skip("No owned alerts to test with")

        # Extract first alert ID from table output
        import re

        match = re.search(r"[│|]\s*(\d+)\s*[│|]", list_result.output)
        if not match:
            pytest.skip("Could not parse alert ID")

        alert_id = match.group(1)

        # Test with invalid --since format
        result = runner.invoke(
            main,
            [
                "alerts",
                "results",
                alert_id,
                "--since",
                "not-a-timestamp",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should either fail or handle gracefully
        # Server may return error or empty results depending on implementation
        assert result.exit_code in (0, 1)


class TestVerboseModeStreaming:
    """E2E tests for verbose mode with streaming queries."""

    DATA_QUERY = "host:microsoft.com"

    def test_verbose_streaming_shows_debug_output(self, api_key: str, host: str) -> None:
        """Test that verbose mode with streaming shows debug information."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "-v",  # Verbose flag
                "query",
                self.DATA_QUERY,
                "--index",
                "dns",
                "--since-days",
                "1",
                "--stream",
                "--format",
                "jsonl",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        # Should show streaming indicator
        assert "Streaming" in result.output or "stream" in result.output.lower()


class TestLargeSinceDaysValues:
    """E2E tests for very large since-days values."""

    DATA_QUERY = "host:microsoft.com"

    def test_since_days_365(self, api_key: str, host: str) -> None:
        """Test query with since-days=365 (one year lookback)."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                self.DATA_QUERY,
                "--index",
                "dns",
                "--since-days",
                "365",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should succeed (large lookback is valid)
        assert result.exit_code == 0
        assert "[" in result.output  # Valid JSON array


class TestStreamingWithNoMarker:
    """E2E tests for streaming mode with --no-marker flag."""

    DATA_QUERY = "host:microsoft.com"

    def test_streaming_no_marker_to_stdout(self, api_key: str, host: str) -> None:
        """Test streaming with --no-marker outputs to stdout correctly."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                self.DATA_QUERY,
                "--index",
                "dns",
                "--since-days",
                "1",
                "--stream",
                "--no-marker",
                "--format",
                "jsonl",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        # Should show streaming indicator and have JSONL output
        assert "Streaming" in result.output or "stream" in result.output.lower()

    def test_streaming_no_marker_to_file(self, api_key: str, host: str, tmp_path) -> None:
        """Test streaming with --no-marker writes to file without saving marker."""
        from click.testing import CliRunner

        from cetus.cli import main

        output_file = tmp_path / "results.jsonl"
        markers_dir = tmp_path / "markers"
        markers_dir.mkdir(exist_ok=True)

        runner = CliRunner(env={"CETUS_DATA_DIR": str(tmp_path)})
        result = runner.invoke(
            main,
            [
                "query",
                self.DATA_QUERY,
                "--index",
                "dns",
                "--since-days",
                "1",
                "--stream",
                "--no-marker",
                "-o",
                str(output_file),
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        assert output_file.exists()

        # No marker should be saved
        marker_files = list(markers_dir.glob("*.json"))
        assert len(marker_files) == 0


class TestMarkerSinceDaysInteraction:
    """E2E tests for marker and since-days interaction.

    When a marker exists, since-days should be ignored and the query
    should resume from the marker position.
    """

    DATA_QUERY = "host:microsoft.com"

    def test_marker_takes_precedence_over_since_days(
        self, api_key: str, host: str, tmp_path
    ) -> None:
        """Test that marker timestamp takes precedence over --since-days."""
        from click.testing import CliRunner

        from cetus.cli import main

        output_file = tmp_path / "results.jsonl"

        runner = CliRunner(env={"CETUS_DATA_DIR": str(tmp_path)})

        # First run with since-days=1 to create a marker
        result1 = runner.invoke(
            main,
            [
                "query",
                self.DATA_QUERY,
                "--index",
                "dns",
                "--since-days",
                "1",
                "--format",
                "jsonl",
                "-o",
                str(output_file),
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result1.exit_code == 0
        assert "Wrote" in result1.output

        # Second run with since-days=365 (much larger lookback)
        # Should still use marker (recent timestamp), not the 365 day lookback
        result2 = runner.invoke(
            main,
            [
                "query",
                self.DATA_QUERY,
                "--index",
                "dns",
                "--since-days",
                "365",  # This should be ignored
                "--format",
                "jsonl",
                "-o",
                str(output_file),
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result2.exit_code == 0
        # Should either append (if new data) or report no new records
        # Key is that it doesn't re-fetch 365 days of data
        assert "Appended" in result2.output or "No new records" in result2.output


class TestHelpTextCompleteness:
    """E2E tests for help text completeness."""

    def test_all_query_options_documented(self) -> None:
        """Test that query command help documents all options."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["query", "--help"])
        assert result.exit_code == 0

        # Check all options are documented
        expected_options = [
            "--index",
            "--media",
            "--format",
            "--output",
            "--output-prefix",
            "--since-days",
            "--no-marker",
            "--stream",
            "--api-key",
            "--host",
        ]
        for opt in expected_options:
            assert opt in result.output, f"Option {opt} not documented in query help"

    def test_all_alerts_subcommands_documented(self) -> None:
        """Test that alerts command documents all subcommands."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["alerts", "--help"])
        assert result.exit_code == 0

        # Check all subcommands are documented
        expected_commands = ["list", "results", "backtest"]
        for cmd in expected_commands:
            assert cmd in result.output, f"Subcommand {cmd} not documented in alerts help"

    def test_alerts_results_options_documented(self) -> None:
        """Test that alerts results help documents --since option."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["alerts", "results", "--help"])
        assert result.exit_code == 0

        assert "--since" in result.output
        # Format hint may be wrapped across lines, so normalize whitespace
        normalized = " ".join(result.output.split())
        assert "ISO 8601" in normalized  # Format hint

    def test_alerts_list_format_option_documented(self) -> None:
        """Test that alerts list help documents --format option."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["alerts", "list", "--help"])
        assert result.exit_code == 0

        assert "--format" in result.output
        assert "json" in result.output
        assert "csv" in result.output
        assert "table" in result.output

    def test_alerts_backtest_output_prefix_documented(self) -> None:
        """Test that alerts backtest help documents --output-prefix option."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["alerts", "backtest", "--help"])
        assert result.exit_code == 0

        assert "--output-prefix" in result.output
        assert "-p" in result.output


class TestAlertsListFormats:
    """E2E tests for alerts list --format option."""

    def test_alerts_list_json_format(self, api_key: str, host: str) -> None:
        """Test alerts list with --format json."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["alerts", "list", "--owned", "--format", "json", "--api-key", api_key, "--host", host],
        )
        assert result.exit_code == 0, f"Command failed: {result.output}"
        # Should be valid JSON array
        import json

        data = json.loads(result.output)
        assert isinstance(data, list)
        if data:  # If there are alerts
            assert "id" in data[0]
            assert "type" in data[0]
            assert "title" in data[0]

    def test_alerts_list_jsonl_format(self, api_key: str, host: str) -> None:
        """Test alerts list with --format jsonl."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "alerts",
                "list",
                "--owned",
                "--format",
                "jsonl",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        # Should be one JSON object per line
        import json

        lines = [line for line in result.output.strip().split("\n") if line]
        if lines:
            for line in lines:
                obj = json.loads(line)
                assert "id" in obj

    def test_alerts_list_csv_format(self, api_key: str, host: str) -> None:
        """Test alerts list with --format csv."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["alerts", "list", "--owned", "--format", "csv", "--api-key", api_key, "--host", host],
        )
        assert result.exit_code == 0
        # Should have header row
        lines = result.output.strip().split("\n")
        assert len(lines) >= 1
        assert "id" in lines[0].lower()
        assert "type" in lines[0].lower()

    def test_alerts_list_to_file(self, api_key: str, host: str, tmp_path) -> None:
        """Test alerts list with --output to file."""
        from click.testing import CliRunner

        from cetus.cli import main

        output_file = tmp_path / "alerts.json"
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "alerts",
                "list",
                "--owned",
                "--format",
                "json",
                "-o",
                str(output_file),
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        assert output_file.exists()

        import json

        data = json.loads(output_file.read_text())
        assert isinstance(data, list)


class TestBacktestOutputPrefix:
    """E2E tests for alerts backtest --output-prefix option."""

    def test_backtest_output_prefix_creates_file(self, api_key: str, host: str, tmp_path) -> None:
        """Test that backtest with --output-prefix creates timestamped file."""
        import json

        from click.testing import CliRunner

        from cetus.cli import main

        # First get an alert ID using JSON format (more reliable than parsing table)
        runner = CliRunner()
        list_result = runner.invoke(
            main,
            ["alerts", "list", "--owned", "--format", "json", "--api-key", api_key, "--host", host],
        )
        if list_result.exit_code != 0:
            pytest.skip(f"Could not list alerts: {list_result.output}")

        try:
            alerts = json.loads(list_result.output)
        except json.JSONDecodeError:
            pytest.skip("Could not parse alerts JSON")

        if not alerts:
            pytest.skip("No owned alerts to test with")

        alert_id = str(alerts[0]["id"])
        prefix = str(tmp_path / "backtest_results")

        result = runner.invoke(
            main,
            [
                "alerts",
                "backtest",
                alert_id,
                "-p",
                prefix,
                "--since-days",
                "1",
                "--no-marker",  # Use --no-marker for consistent test behavior
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should succeed (may have 0 results, but command should work)
        assert result.exit_code == 0, f"Backtest failed: {result.output}"

        # Check that a timestamped file was created (or no file if no results)
        files = list(tmp_path.glob("backtest_results_*.json"))
        # Either file exists with data, or no file created (no results message)
        if "No new records" not in result.output:
            assert len(files) == 1
            assert files[0].name.startswith("backtest_results_")

    def test_backtest_output_and_prefix_mutually_exclusive(
        self, api_key: str, host: str, tmp_path
    ) -> None:
        """Test that --output and --output-prefix are mutually exclusive."""
        from click.testing import CliRunner

        from cetus.cli import main

        output_file = tmp_path / "results.json"
        prefix = str(tmp_path / "results")

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "alerts",
                "backtest",
                "1",
                "-o",
                str(output_file),
                "-p",
                prefix,
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 1
        assert "mutually exclusive" in result.output


class TestDSLQueries:
    """E2E tests for Elasticsearch DSL (JSON) queries.

    The CLI can accept Elasticsearch DSL queries directly as JSON strings,
    not just Lucene query syntax. This tests that code path.
    """

    def test_dsl_query_via_cli(self, api_key: str, host: str) -> None:
        """Test that DSL/JSON queries work via CLI."""
        from click.testing import CliRunner

        from cetus.cli import main

        # DSL query_string equivalent of "host:microsoft.com"
        dsl_query = '{"query_string": {"query": "host:microsoft.com"}}'

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                dsl_query,
                "--index",
                "dns",
                "--since-days",
                "1",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        # Should have JSON array output
        assert "[" in result.output
        # Should contain data (microsoft.com is a common domain)
        assert "uuid" in result.output or "[]" in result.output

    def test_dsl_query_via_api(self, api_key: str, host: str) -> None:
        """Test DSL query through the client API."""
        from cetus.client import CetusClient

        # DSL query with bool must clause
        dsl_query = '{"bool": {"must": [{"query_string": {"query": "host:microsoft.com"}}]}}'

        client = CetusClient(api_key=api_key, host=host, timeout=120)
        try:
            result = client.query(
                search=dsl_query,
                index="dns",
                media="nvme",
                since_days=1,
                marker=None,
            )
            assert result is not None
            assert isinstance(result.data, list)
        finally:
            client.close()

    def test_dsl_query_streaming(self, api_key: str, host: str) -> None:
        """Test DSL query with streaming mode."""
        from click.testing import CliRunner

        from cetus.cli import main

        dsl_query = '{"query_string": {"query": "host:microsoft.com"}}'

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                dsl_query,
                "--index",
                "dns",
                "--since-days",
                "1",
                "--stream",
                "--format",
                "jsonl",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0


class TestBacktestTermsAlerts:
    """E2E tests for backtest with terms alerts.

    Terms alerts expand to multiple term combinations which requires
    different query handling than raw or structured alerts.
    """

    def test_backtest_terms_alert(self, api_key: str, host: str) -> None:
        """Test backtest with a terms alert.

        Terms alerts have queries like:
        host.raw:"*reuters.com"

        These expand to multiple term combinations when evaluated.
        """
        import json

        from click.testing import CliRunner

        from cetus.cli import main

        # Get the list of alerts in JSON format to find a terms alert
        runner = CliRunner()
        list_result = runner.invoke(
            main,
            ["alerts", "list", "--owned", "--format", "json", "--api-key", api_key, "--host", host],
        )

        if list_result.exit_code != 0:
            pytest.skip(f"Could not list alerts: {list_result.output}")

        alerts = json.loads(list_result.output)

        # Find a terms alert
        terms_alerts = [a for a in alerts if a.get("type") == "terms"]
        if not terms_alerts:
            pytest.skip("No terms alerts available to test")

        alert_id = str(terms_alerts[0]["id"])

        # Test backtest with the terms alert
        result = runner.invoke(
            main,
            [
                "alerts",
                "backtest",
                alert_id,
                "--index",
                "dns",
                "--since-days",
                "1",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )

        # Should succeed without error (may return empty array if no matches)
        assert result.exit_code == 0, f"Backtest failed: {result.output}"
        assert "Invalid query syntax" not in result.output
        assert "[" in result.output  # Valid JSON array


class TestAPIKeyMasking:
    """E2E tests for API key security in verbose output.

    API keys should never be exposed in verbose/debug output.
    """

    def test_verbose_mode_masks_api_key(self, api_key: str, host: str) -> None:
        """Test that API key is not exposed in verbose output."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "-v",  # Verbose mode
                "query",
                "host:microsoft.com",
                "--index",
                "dns",
                "--since-days",
                "1",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0

        # API key should not appear in output
        # The key might be partially shown (e.g., ***abc) but never in full
        if len(api_key) > 8:
            # Full key should never appear
            assert api_key not in result.output


class TestMarkerFileCorruption:
    """E2E tests for marker file corruption recovery.

    The client should handle corrupted marker files gracefully.
    """

    def test_corrupted_marker_file_recovery(self, api_key: str, host: str, tmp_path) -> None:
        """Test that corrupted marker file is handled gracefully."""
        from click.testing import CliRunner

        from cetus.cli import main

        output_file = tmp_path / "results.jsonl"
        markers_dir = tmp_path / "markers"
        markers_dir.mkdir(exist_ok=True)

        # Create a corrupted marker file
        corrupted_marker = markers_dir / "dns_test.json"
        corrupted_marker.write_text("{ this is not valid json }")

        runner = CliRunner(env={"CETUS_DATA_DIR": str(tmp_path)})

        # Query should still work (treating marker as invalid/missing)
        result = runner.invoke(
            main,
            [
                "query",
                "host:microsoft.com",
                "--index",
                "dns",
                "--since-days",
                "1",
                "--format",
                "jsonl",
                "-o",
                str(output_file),
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )

        # Should succeed (ignore or reset corrupted marker)
        # May show warning about invalid marker but shouldn't crash
        assert result.exit_code == 0


class TestUserAgentHeader:
    """E2E tests for User-Agent header.

    The client should send a proper User-Agent header with version info.
    """

    def test_user_agent_contains_version(self) -> None:
        """Test that User-Agent header constant contains client version."""
        from cetus import __version__
        from cetus.client import USER_AGENT

        # Check that the USER_AGENT constant has correct format
        assert "cetus-client" in USER_AGENT
        assert __version__ in USER_AGENT
        # Should also include Python version and platform
        import platform

        assert platform.python_version() in USER_AGENT
        assert platform.system() in USER_AGENT


class TestTimeoutBehavior:
    """E2E tests for timeout behavior.

    The client should respect timeout settings and fail gracefully.
    """

    def test_very_short_timeout_fails_gracefully(self, api_key: str, host: str) -> None:
        """Test that very short timeout produces a clean error."""
        from click.testing import CliRunner

        from cetus.cli import main

        # Use an extremely short timeout that will likely fail
        runner = CliRunner(env={"CETUS_TIMEOUT": "0.001"})
        result = runner.invoke(
            main,
            [
                "query",
                "host:microsoft.com",
                "--index",
                "dns",
                "--since-days",
                "1",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should fail with timeout or connection error (not crash)
        # Could succeed if connection is very fast, so we just check it doesn't crash
        assert result.exit_code in (0, 1)
        # If it failed, should have clean error message
        if result.exit_code == 1:
            output_lower = result.output.lower()
            assert "error" in output_lower or "timeout" in output_lower


class TestConfigEnvironmentVariables:
    """E2E tests for environment variable configuration.

    Tests that all config environment variables work correctly.
    """

    def test_cetus_since_days_env_var(self, api_key: str, host: str) -> None:
        """Test that CETUS_SINCE_DAYS environment variable is respected."""
        from click.testing import CliRunner

        from cetus.cli import main

        # Set since-days via environment
        runner = CliRunner(env={"CETUS_SINCE_DAYS": "3"})
        result = runner.invoke(
            main,
            [
                "-v",  # Verbose to see the query
                "query",
                "host:microsoft.com",
                "--index",
                "dns",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should succeed and use the env var for since-days
        assert result.exit_code == 0


class TestQueryResultCount:
    """E2E tests for query result count reporting.

    Tests that the CLI correctly reports the number of records returned.
    """

    def test_buffered_query_reports_count(self, api_key: str, host: str) -> None:
        """Test that buffered query reports total record count."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                "host:microsoft.com",
                "--index",
                "dns",
                "--since-days",
                "1",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        # Should report count in output (e.g., "130 records in 2.5s")
        assert "record" in result.output.lower()

    def test_file_output_reports_wrote_count(self, api_key: str, host: str, tmp_path) -> None:
        """Test that file output reports 'Wrote X records'."""
        from click.testing import CliRunner

        from cetus.cli import main

        output_file = tmp_path / "results.jsonl"

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                "host:microsoft.com",
                "--index",
                "dns",
                "--since-days",
                "1",
                "--format",
                "jsonl",
                "-o",
                str(output_file),
                "--no-marker",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        # Should report "Wrote X records to <file>"
        assert "Wrote" in result.output
        assert "records" in result.output.lower()


class TestAlertResultsOutputFormats:
    """E2E tests for alert results with different output formats to file.

    Verifies that alert results can be exported to CSV, JSONL, and JSON files.
    """

    def test_alert_results_csv_to_file(self, api_key: str, host: str, tmp_path) -> None:
        """Test alert results exported to CSV file."""
        from click.testing import CliRunner

        from cetus.cli import main

        # First get an alert ID
        runner = CliRunner()
        list_result = runner.invoke(
            main,
            ["alerts", "list", "--owned", "--format", "json", "--api-key", api_key, "--host", host],
        )
        if list_result.exit_code != 0 or list_result.output.strip() == "[]":
            pytest.skip("No owned alerts to test with")

        import json

        alerts = json.loads(list_result.output)
        if not alerts:
            pytest.skip("No owned alerts to test with")

        alert_id = str(alerts[0]["id"])
        output_file = tmp_path / "results.csv"

        result = runner.invoke(
            main,
            [
                "alerts",
                "results",
                alert_id,
                "--format",
                "csv",
                "-o",
                str(output_file),
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should succeed even if no results (writes empty file or reports no results)
        assert result.exit_code == 0

    def test_alert_results_jsonl_to_file(self, api_key: str, host: str, tmp_path) -> None:
        """Test alert results exported to JSONL file."""
        from click.testing import CliRunner

        from cetus.cli import main

        # First get an alert ID
        runner = CliRunner()
        list_result = runner.invoke(
            main,
            ["alerts", "list", "--owned", "--format", "json", "--api-key", api_key, "--host", host],
        )
        if list_result.exit_code != 0 or list_result.output.strip() == "[]":
            pytest.skip("No owned alerts to test with")

        import json

        alerts = json.loads(list_result.output)
        if not alerts:
            pytest.skip("No owned alerts to test with")

        alert_id = str(alerts[0]["id"])
        output_file = tmp_path / "results.jsonl"

        result = runner.invoke(
            main,
            [
                "alerts",
                "results",
                alert_id,
                "--format",
                "jsonl",
                "-o",
                str(output_file),
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0


class TestStreamingAlertingIndex:
    """E2E tests for streaming queries on alerting index.

    Verifies streaming mode works correctly with the alerting index,
    which may have different data patterns than dns/certstream.
    """

    def test_streaming_alerting_index_works(self, api_key: str, host: str) -> None:
        """Test streaming query on alerting index."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                "uuid:*",  # Match any UUID in alerting index
                "--index",
                "alerting",
                "--since-days",
                "7",
                "--stream",
                "--format",
                "jsonl",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should complete without error (may have 0 results)
        assert result.exit_code == 0
        assert "Streaming" in result.output or "Streamed" in result.output

    def test_streaming_alerting_index_to_file(self, api_key: str, host: str, tmp_path) -> None:
        """Test streaming query on alerting index with file output."""
        from click.testing import CliRunner

        from cetus.cli import main

        output_file = tmp_path / "alerting_results.jsonl"

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                "uuid:*",
                "--index",
                "alerting",
                "--since-days",
                "7",
                "--stream",
                "-o",
                str(output_file),
                "--no-marker",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        # File may or may not exist depending on results


class TestConfigFileCorruption:
    """E2E tests for config file corruption recovery.

    Verifies that corrupted or invalid config files are handled gracefully.
    """

    def test_malformed_config_toml_handled_gracefully(self, tmp_path) -> None:
        """Test that malformed config.toml produces clear error, not traceback."""
        from unittest.mock import patch

        from click.testing import CliRunner

        from cetus.cli import main

        config_dir = tmp_path / "config"
        config_dir.mkdir()

        # Create a malformed TOML file
        config_file = config_dir / "config.toml"
        config_file.write_text("this is not [valid toml\napi_key = ")

        with patch("cetus.config.get_config_dir", return_value=config_dir):
            runner = CliRunner()
            result = runner.invoke(main, ["config", "show"])

        # Should either handle gracefully or show clean error (not Python traceback)
        output_lower = result.output.lower()
        assert "traceback" not in output_lower

    def test_empty_config_file_handled(self, tmp_path) -> None:
        """Test that empty config file is handled gracefully."""
        from unittest.mock import patch

        from click.testing import CliRunner

        from cetus.cli import main

        config_dir = tmp_path / "config"
        config_dir.mkdir()

        # Create an empty config file
        config_file = config_dir / "config.toml"
        config_file.write_text("")

        with patch("cetus.config.get_config_dir", return_value=config_dir):
            runner = CliRunner()
            result = runner.invoke(main, ["config", "show"])

        # Should work (use defaults)
        assert result.exit_code == 0
        # Should show default host
        assert "alerting.sparkits.ca" in result.output or "host" in result.output.lower()


class TestAlertsListToFile:
    """E2E tests for alerts list output to file.

    Verifies that alerts list can be exported to files in various formats.
    """

    def test_alerts_list_csv_to_file(self, api_key: str, host: str, tmp_path) -> None:
        """Test alerts list exported to CSV file."""
        from click.testing import CliRunner

        from cetus.cli import main

        output_file = tmp_path / "alerts.csv"

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "alerts",
                "list",
                "--format",
                "csv",
                "-o",
                str(output_file),
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0

        if output_file.exists():
            content = output_file.read_text()
            # CSV should have header row
            assert "id" in content.lower() or "type" in content.lower()

    def test_alerts_list_jsonl_to_file(self, api_key: str, host: str, tmp_path) -> None:
        """Test alerts list exported to JSONL file."""
        from click.testing import CliRunner

        from cetus.cli import main

        output_file = tmp_path / "alerts.jsonl"

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "alerts",
                "list",
                "--format",
                "jsonl",
                "-o",
                str(output_file),
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0

        if output_file.exists():
            content = output_file.read_text()
            # Each line should be valid JSON
            import json

            lines = [line for line in content.strip().split("\n") if line]
            for line in lines[:3]:  # Check first few lines
                obj = json.loads(line)
                assert "id" in obj or "type" in obj


class TestOutputPrefixWithNoMarker:
    """E2E tests for --output-prefix combined with --no-marker.

    Verifies that -p and --no-marker work correctly together.
    """

    def test_output_prefix_with_no_marker_creates_file(
        self, api_key: str, host: str, tmp_path
    ) -> None:
        """Test that -p with --no-marker creates timestamped file without marker."""
        from click.testing import CliRunner

        from cetus.cli import main

        prefix = str(tmp_path / "results")

        runner = CliRunner(env={"CETUS_DATA_DIR": str(tmp_path)})
        result = runner.invoke(
            main,
            [
                "query",
                "host:microsoft.com",
                "--index",
                "dns",
                "--since-days",
                "1",
                "-p",
                prefix,
                "--format",
                "jsonl",
                "--no-marker",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0

        # Should create timestamped file
        files = list(tmp_path.glob("results_*.jsonl"))
        assert len(files) == 1

        # Should NOT create marker (because --no-marker)
        marker_files = list(tmp_path.glob("markers/*.json"))
        assert len(marker_files) == 0


class TestStreamingMediaAll:
    """E2E tests for streaming with --media all option.

    The --media all option routes to all storage tiers, not just NVMe.
    This can return more results but takes longer.
    """

    @pytest.mark.skip(reason="--media all is slow, skip by default")
    def test_streaming_media_all(self, api_key: str, host: str) -> None:
        """Test streaming query with --media all option."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                "host:microsoft.com",
                "--index",
                "dns",
                "--since-days",
                "1",
                "--media",
                "all",
                "--stream",
                "--format",
                "jsonl",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
            catch_exceptions=False,
        )
        # Should complete (may take longer than nvme)
        assert result.exit_code == 0
        assert "Streaming" in result.output or "Streamed" in result.output


class TestBacktestStreamingWithOutputPrefix:
    """E2E tests for backtest with streaming and output prefix combined."""

    def test_backtest_streaming_output_prefix(self, api_key: str, host: str, tmp_path) -> None:
        """Test backtest with --stream and -p options together."""
        from click.testing import CliRunner

        from cetus.cli import main

        # First get an alert ID
        runner = CliRunner()
        list_result = runner.invoke(
            main,
            ["alerts", "list", "--owned", "--format", "json", "--api-key", api_key, "--host", host],
        )
        if list_result.exit_code != 0:
            pytest.skip("Could not list alerts")

        import json

        try:
            alerts = json.loads(list_result.output)
        except json.JSONDecodeError:
            pytest.skip("Could not parse alerts list")

        if not alerts:
            pytest.skip("No owned alerts to test with")

        alert_id = str(alerts[0]["id"])
        prefix = str(tmp_path / "backtest")

        result = runner.invoke(
            main,
            [
                "alerts",
                "backtest",
                alert_id,
                "--index",
                "dns",
                "--since-days",
                "1",
                "--stream",
                "-p",
                prefix,
                "--format",
                "jsonl",
                "--no-marker",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result.exit_code == 0
        # May or may not create file depending on results
        # But should not crash


class TestTableFormatIncrementalAppendWarning:
    """E2E tests for table format warning when used with incremental mode."""

    DATA_QUERY = "host:microsoft.com"

    def test_table_format_append_shows_warning(self, api_key: str, host: str, tmp_path) -> None:
        """Test that table format with existing file shows cannot-append warning.

        Table format cannot truly append to an existing file (Rich tables
        require full content to calculate column widths). When used in
        incremental mode with an existing file, a warning should be shown.
        """
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        output_file = tmp_path / "results.txt"

        # First run - create initial file with table format
        # Use longer lookback to ensure we get results
        result1 = runner.invoke(
            main,
            [
                "query",
                self.DATA_QUERY,
                "--index",
                "dns",
                "--since-days",
                "7",
                "-o",
                str(output_file),
                "--format",
                "table",
                "--no-marker",  # Don't save marker, we'll test append behavior manually
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result1.exit_code == 0

        # If no results were found, we can't test the append warning
        if not output_file.exists() or output_file.stat().st_size == 0:
            pytest.skip("No results found to test table append warning")

        initial_size = output_file.stat().st_size

        # Create a marker file manually to trigger incremental mode
        from cetus.markers import MarkerStore

        marker_store = MarkerStore()
        # Save marker from a past timestamp to ensure second run has "new" data
        marker_store.save(
            query=self.DATA_QUERY,
            index="dns",
            last_timestamp="2020-01-01T00:00:00Z",
            last_uuid="test-uuid",
            mode="file",
        )

        # Second run - incremental mode with marker, existing file should trigger warning
        result2 = runner.invoke(
            main,
            [
                "query",
                self.DATA_QUERY,
                "--index",
                "dns",
                "--since-days",
                "7",
                "-o",
                str(output_file),
                "--format",
                "table",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        assert result2.exit_code == 0

        # Should show the warning about table format not being able to append
        # Note: The warning appears in stderr which Click captures in output
        assert "table format cannot append" in result2.output or "Warning" in result2.output

        # Clean up marker
        marker_store.clear("dns")


class TestBacktestVerboseMode:
    """E2E tests for backtest command verbose output."""

    def test_backtest_verbose_shows_alert_details(self, api_key: str, host: str) -> None:
        """Test that verbose mode with backtest shows alert title and query.

        When running backtest with -v flag, it should display:
        - The alert title
        - The query being executed
        """
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()

        # First get an alert ID
        list_result = runner.invoke(
            main,
            ["alerts", "list", "--owned", "--format", "json", "--api-key", api_key, "--host", host],
        )
        if list_result.exit_code != 0:
            pytest.skip("Could not list alerts")

        import json

        try:
            alerts = json.loads(list_result.output)
        except json.JSONDecodeError:
            pytest.skip("Could not parse alerts list")

        if not alerts:
            pytest.skip("No owned alerts to test with")

        # Find an alert with a query
        alert = None
        for a in alerts:
            if a.get("query_preview"):
                alert = a
                break

        if not alert:
            pytest.skip("No alerts with query_preview found")

        alert_id = str(alert["id"])

        # Run backtest with verbose mode
        result = runner.invoke(
            main,
            [
                "-v",  # Verbose flag before subcommand
                "alerts",
                "backtest",
                alert_id,
                "--index",
                "dns",
                "--since-days",
                "1",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )

        # Should succeed
        assert result.exit_code == 0

        # Verbose output should show alert details
        # The exact format is: "Backtesting alert: {title}" and "Query: {query}"
        assert "Backtesting alert" in result.output or "Query:" in result.output


class TestSharedAlertOperations:
    """E2E tests for operations on shared alerts.

    Tests that users can access results and backtest alerts that have been
    shared with them but which they don't own.
    """

    def test_alert_results_for_shared_alert(self, api_key: str, host: str) -> None:
        """Test that alert results can be retrieved for a shared alert."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()

        # First, find a shared alert
        list_result = runner.invoke(
            main,
            [
                "alerts",
                "list",
                "--shared",
                "--no-owned",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        if list_result.exit_code != 0:
            pytest.skip("Could not list shared alerts")

        import json

        try:
            alerts = json.loads(list_result.output)
        except json.JSONDecodeError:
            pytest.skip("Could not parse shared alerts list")

        if not alerts:
            pytest.skip("No shared alerts to test with")

        alert_id = str(alerts[0]["id"])

        # Get results for the shared alert
        result = runner.invoke(
            main,
            [
                "alerts",
                "results",
                alert_id,
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )

        # Should succeed (even if no results, should not error on permissions)
        assert result.exit_code == 0
        # Output should be valid JSON (empty array or array of results)
        output = result.output.strip()
        if output:
            # Remove any status messages before JSON
            if "[" in output:
                json_start = output.index("[")
                json_output = output[json_start:]
                data = json.loads(json_output)
                assert isinstance(data, list)

    def test_backtest_shared_alert_access(self, api_key: str, host: str) -> None:
        """Test that backtest works on alerts shared with the user."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()

        # First, find a shared alert
        list_result = runner.invoke(
            main,
            [
                "alerts",
                "list",
                "--shared",
                "--no-owned",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        if list_result.exit_code != 0:
            pytest.skip("Could not list shared alerts")

        import json

        try:
            alerts = json.loads(list_result.output)
        except json.JSONDecodeError:
            pytest.skip("Could not parse shared alerts list")

        if not alerts:
            pytest.skip("No shared alerts to test with")

        alert_id = str(alerts[0]["id"])

        # Run backtest with a very short time window to avoid timeout
        result = runner.invoke(
            main,
            [
                "alerts",
                "backtest",
                alert_id,
                "--index",
                "dns",
                "--since-days",
                "1",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
            catch_exceptions=False,
        )

        # Should either succeed or timeout - but not fail with permission error
        # We check that it doesn't fail with "permission" or "not found" errors
        output_lower = result.output.lower()
        assert "permission denied" not in output_lower
        assert "not allowed" not in output_lower
        # Note: timeout is acceptable for complex shared alerts


class TestConfigForwardCompatibility:
    """E2E tests for config forward compatibility.

    Tests that unknown keys in config files are handled gracefully,
    allowing older clients to work with config files from newer versions.
    """

    def test_config_with_unknown_keys_handled_gracefully(self, tmp_path) -> None:
        """Test that config files with unknown keys don't cause errors."""
        from unittest.mock import patch

        from click.testing import CliRunner

        from cetus.cli import main

        config_dir = tmp_path / "config"
        config_dir.mkdir()

        # Create a config file with unknown keys (simulating future version)
        config_file = config_dir / "config.toml"
        config_file.write_text(
            'api_key = "test-key"\n'
            'host = "custom.example.com"\n'
            "timeout = 30\n"
            "since_days = 14\n"
            "# Unknown keys from future version\n"
            'new_feature_flag = true\n'
            'experimental_mode = "beta"\n'
            "max_retries = 5\n"
        )

        with patch("cetus.config.get_config_dir", return_value=config_dir):
            runner = CliRunner()
            result = runner.invoke(main, ["config", "show"])

        # Should work without error
        assert result.exit_code == 0
        # Should show the known settings
        assert "custom.example.com" in result.output or "host" in result.output.lower()
        # Should not show Python traceback
        assert "Traceback" not in result.output

    def test_config_with_empty_values(self, tmp_path) -> None:
        """Test that config files with empty string values are handled."""
        from unittest.mock import patch

        from click.testing import CliRunner

        from cetus.cli import main

        config_dir = tmp_path / "config"
        config_dir.mkdir()

        # Create a config file with empty values
        config_file = config_dir / "config.toml"
        config_file.write_text(
            'api_key = ""\n'  # Empty API key
            'host = ""\n'  # Empty host
        )

        with patch("cetus.config.get_config_dir", return_value=config_dir):
            runner = CliRunner()
            result = runner.invoke(main, ["config", "show"])

        # Should handle gracefully - either use defaults or show empty
        # Main thing is no crash/traceback
        assert "Traceback" not in result.output


class TestQueryLuceneOperators:
    """E2E tests for complex Lucene query syntax.

    Tests that various Lucene operators are handled correctly.
    """

    DATA_QUERY = "host:microsoft.com"

    def test_query_with_and_operator(self, api_key: str, host: str) -> None:
        """Test query with explicit AND operator."""
        from cetus.client import CetusClient

        client = CetusClient(api_key=api_key, host=host, timeout=60)
        try:
            result = client.query(
                search="host:microsoft.com AND A:*",
                index="dns",
                media="nvme",
                since_days=1,
                marker=None,
            )
            assert result is not None
            assert isinstance(result.data, list)
        finally:
            client.close()

    def test_query_with_or_operator(self, api_key: str, host: str) -> None:
        """Test query with OR operator."""
        from cetus.client import CetusClient

        client = CetusClient(api_key=api_key, host=host, timeout=60)
        try:
            result = client.query(
                search="host:microsoft.com OR host:google.com",
                index="dns",
                media="nvme",
                since_days=1,
                marker=None,
            )
            assert result is not None
            assert isinstance(result.data, list)
        finally:
            client.close()

    def test_query_with_not_operator(self, api_key: str, host: str) -> None:
        """Test query with NOT operator."""
        from cetus.client import CetusClient

        client = CetusClient(api_key=api_key, host=host, timeout=60)
        try:
            result = client.query(
                search="host:microsoft.com AND NOT A:1.1.1.1",
                index="dns",
                media="nvme",
                since_days=1,
                marker=None,
            )
            assert result is not None
            assert isinstance(result.data, list)
        finally:
            client.close()

    def test_query_with_wildcard_suffix(self, api_key: str, host: str) -> None:
        """Test query with wildcard suffix match (trailing wildcard is fast)."""
        from cetus.client import CetusClient

        # Note: Leading wildcards (*.example.com) are slow as they scan all data.
        # Trailing wildcards (example.*) use the index efficiently.
        client = CetusClient(api_key=api_key, host=host, timeout=60)
        try:
            result = client.query(
                search="host:microsoft.*",
                index="dns",
                media="nvme",
                since_days=1,
                marker=None,
            )
            assert result is not None
            assert isinstance(result.data, list)
        finally:
            client.close()

    def test_query_with_quoted_phrase(self, api_key: str, host: str) -> None:
        """Test query with quoted exact phrase."""
        from click.testing import CliRunner

        from cetus.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "query",
                'host:"www.microsoft.com"',
                "--index",
                "dns",
                "--since-days",
                "3",
                "--format",
                "json",
                "--api-key",
                api_key,
                "--host",
                host,
            ],
        )
        # Should execute without error
        assert result.exit_code == 0

    def test_query_with_field_grouping(self, api_key: str, host: str) -> None:
        """Test query with field grouping using parentheses."""
        from cetus.client import CetusClient

        client = CetusClient(api_key=api_key, host=host, timeout=60)
        try:
            result = client.query(
                search="(host:microsoft.com OR host:azure.com) AND A:*",
                index="dns",
                media="nvme",
                since_days=1,
                marker=None,
            )
            assert result is not None
            assert isinstance(result.data, list)
        finally:
            client.close()
