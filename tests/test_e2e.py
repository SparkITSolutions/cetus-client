"""End-to-end tests against a real Cetus server.

These tests require a running Cetus instance and valid API credentials.
They are skipped by default unless CETUS_E2E_TEST=1 is set.

Environment variables:
    CETUS_E2E_TEST: Set to "1" to enable E2E tests
    CETUS_API_KEY: API key for authentication
    CETUS_HOST: Server host (default: alerting.sparkits.ca)

Run with:
    CETUS_E2E_TEST=1 CETUS_API_KEY=your-key pytest tests/test_e2e.py -v

Expected duration: ~60-90 seconds for all 21 tests

Query optimization:
- Uses host:microsoft.com which has frequent data and returns quickly
- Uses since_days=7 (same speed as 1 day for targeted queries)
- Streaming tests break early after a few records
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
                "--index", "dns",
                "--since-days", "7",
                "--format", "json",
                "--api-key", api_key,
                "--host", host,
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
                "--index", "dns",
                "--since-days", "7",
                "--stream",
                "--format", "jsonl",
                "--api-key", api_key,
                "--host", host,
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
                "alerts", "list",
                "--api-key", api_key,
                "--host", host,
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

    def test_cli_output_file_creates_file(
        self, api_key: str, host: str, tmp_path
    ) -> None:
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
                "--index", "dns",
                "--since-days", "7",
                "--format", "jsonl",
                "-o", str(output_file),
                "--no-marker",  # Don't save marker for this test
                "--api-key", api_key,
                "--host", host,
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
                "--index", "dns",
                "--since-days", "7",
                "--format", "jsonl",
                "-p", prefix,
                "--no-marker",
                "--api-key", api_key,
                "--host", host,
            ],
        )
        assert result.exit_code == 0

        # Should have created a timestamped file
        files = list(tmp_path.glob("results_*.jsonl"))
        assert len(files) == 1
        assert files[0].stat().st_size > 0

    def test_cli_output_csv_format(
        self, api_key: str, host: str, tmp_path
    ) -> None:
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
                "--index", "dns",
                "--since-days", "7",
                "--format", "csv",
                "-o", str(output_file),
                "--no-marker",
                "--api-key", api_key,
                "--host", host,
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

    def test_cli_streaming_with_output_file(
        self, api_key: str, host: str, tmp_path
    ) -> None:
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
                "--index", "dns",
                "--since-days", "7",
                "--stream",
                "-o", str(output_file),
                "--no-marker",
                "--api-key", api_key,
                "--host", host,
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

    def test_marker_saved_and_used(
        self, api_key: str, host: str, tmp_path
    ) -> None:
        """Test that markers are saved and affect subsequent queries."""
        from pathlib import Path

        from click.testing import CliRunner

        from cetus.cli import main

        # Use isolated marker directory
        markers_dir = tmp_path / "markers"
        markers_dir.mkdir()

        output_file = tmp_path / "results.jsonl"

        runner = CliRunner(
            env={"CETUS_DATA_DIR": str(tmp_path)}
        )

        # First run - should fetch data and save marker
        result1 = runner.invoke(
            main,
            [
                "query",
                self.DATA_QUERY,
                "--index", "dns",
                "--since-days", "7",
                "--format", "jsonl",
                "-o", str(output_file),
                "--api-key", api_key,
                "--host", host,
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
                "--index", "dns",
                "--since-days", "7",
                "--format", "jsonl",
                "-o", str(output_file),
                "--api-key", api_key,
                "--host", host,
            ],
        )
        assert result2.exit_code == 0
        # Should either append or report no new records
        assert "Appended" in result2.output or "No new records" in result2.output

    def test_output_prefix_with_markers(
        self, api_key: str, host: str, tmp_path
    ) -> None:
        """Test -p mode saves markers for incremental queries."""
        from click.testing import CliRunner

        from cetus.cli import main

        prefix = str(tmp_path / "export")

        runner = CliRunner(
            env={"CETUS_DATA_DIR": str(tmp_path)}
        )

        # First run
        result1 = runner.invoke(
            main,
            [
                "query",
                self.DATA_QUERY,
                "--index", "dns",
                "--since-days", "7",
                "--format", "jsonl",
                "-p", prefix,
                "--api-key", api_key,
                "--host", host,
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
                "--index", "dns",
                "--since-days", "7",
                "--format", "jsonl",
                "-p", prefix,
                "--api-key", api_key,
                "--host", host,
            ],
        )
        assert result2.exit_code == 0

        # If no new data, no new file created
        # If new data, a second file is created
        files2 = list(tmp_path.glob("export_*.jsonl"))
        # Should have 1 or 2 files depending on whether new data arrived
        assert len(files2) >= 1
