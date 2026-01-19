"""Tests for the Cetus API client."""

from __future__ import annotations

from datetime import datetime, timedelta

import httpx
import pytest

from cetus.client import Alert, CetusClient, QueryResult
from cetus.config import Config
from cetus.exceptions import APIError, AuthenticationError, ConnectionError
from cetus.markers import Marker


class TestQueryResult:
    """Tests for QueryResult dataclass."""

    def test_create_query_result(self):
        """QueryResult can be created with all fields."""
        result = QueryResult(
            data=[{"id": 1}],
            total_fetched=1,
            last_uuid="uuid-123",
            last_timestamp="2025-01-01T00:00:00Z",
            pages_fetched=1,
        )
        assert result.data == [{"id": 1}]
        assert result.total_fetched == 1
        assert result.last_uuid == "uuid-123"
        assert result.last_timestamp == "2025-01-01T00:00:00Z"
        assert result.pages_fetched == 1

    def test_empty_result(self):
        """QueryResult can represent empty results."""
        result = QueryResult(
            data=[],
            total_fetched=0,
            last_uuid=None,
            last_timestamp=None,
            pages_fetched=1,
        )
        assert result.data == []
        assert result.total_fetched == 0
        assert result.last_uuid is None


class TestAlert:
    """Tests for Alert dataclass."""

    def test_create_alert(self):
        """Alert can be created with all fields."""
        alert = Alert(
            id=123,
            alert_type="raw",
            title="Test Alert",
            description="A test alert",
            query_preview="host:*.example.com",
            owned=True,
            shared_by=None,
        )
        assert alert.id == 123
        assert alert.alert_type == "raw"
        assert alert.title == "Test Alert"
        assert alert.owned is True
        assert alert.shared_by is None

    def test_from_dict(self):
        """Alert.from_dict should create Alert from dictionary."""
        data = {
            "id": 456,
            "alert_type": "terms",
            "title": "Dict Alert",
            "description": "From dict",
            "query_preview": "A:*",
            "owned": False,
            "shared_by": "other_user",
        }
        alert = Alert.from_dict(data)

        assert alert.id == 456
        assert alert.alert_type == "terms"
        assert alert.title == "Dict Alert"
        assert alert.owned is False
        assert alert.shared_by == "other_user"

    def test_from_dict_with_defaults(self):
        """Alert.from_dict should handle missing optional fields."""
        data = {"id": 1, "alert_type": "raw"}
        alert = Alert.from_dict(data)

        assert alert.id == 1
        assert alert.title == ""
        assert alert.description == ""
        assert alert.owned is False
        assert alert.shared_by is None


class TestCetusClientInit:
    """Tests for CetusClient initialization."""

    def test_init_with_required_args(self):
        """CetusClient can be created with required arguments."""
        client = CetusClient(api_key="test-key")
        assert client.api_key == "test-key"
        assert client.host == "alerting.sparkits.ca"
        assert client.timeout == 60

    def test_init_with_all_args(self):
        """CetusClient can be created with all arguments."""
        client = CetusClient(
            api_key="my-key",
            host="custom.host.com",
            timeout=120,
        )
        assert client.api_key == "my-key"
        assert client.host == "custom.host.com"
        assert client.timeout == 120

    def test_client_is_lazy_initialized(self):
        """HTTP client should not be created until first use."""
        client = CetusClient(api_key="test")
        assert client._client is None

    def test_from_config(self):
        """CetusClient.from_config should create client from Config."""
        config = Config(api_key="config-key", host="config.host.com", timeout=90)
        client = CetusClient.from_config(config)

        assert client.api_key == "config-key"
        assert client.host == "config.host.com"
        assert client.timeout == 90


class TestCetusClientHTTPClient:
    """Tests for CetusClient HTTP client property."""

    def test_client_property_creates_httpx_client(self):
        """Accessing client property should create httpx.Client."""
        client = CetusClient(api_key="test")
        http_client = client.client

        assert isinstance(http_client, httpx.Client)
        client.close()

    def test_client_property_is_cached(self):
        """Client property should return same instance on multiple calls."""
        client = CetusClient(api_key="test")
        http_client1 = client.client
        http_client2 = client.client

        assert http_client1 is http_client2
        client.close()

    def test_client_adds_https_prefix(self):
        """Client should add https:// prefix to host."""
        client = CetusClient(api_key="test", host="example.com")
        # Access client to trigger creation
        http_client = client.client
        assert http_client.base_url.scheme == "https"
        client.close()

    def test_client_respects_http_prefix(self):
        """Client should respect explicit http:// prefix."""
        client = CetusClient(api_key="test", host="http://localhost:8000")
        http_client = client.client
        assert http_client.base_url.scheme == "http"
        client.close()

    def test_client_respects_https_prefix(self):
        """Client should respect explicit https:// prefix."""
        client = CetusClient(api_key="test", host="https://secure.example.com")
        http_client = client.client
        assert http_client.base_url.scheme == "https"
        client.close()

    def test_close_sets_client_to_none(self):
        """close() should set _client to None."""
        client = CetusClient(api_key="test")
        _ = client.client  # Initialize client
        client.close()

        assert client._client is None

    def test_context_manager(self):
        """CetusClient should work as context manager."""
        with CetusClient(api_key="test") as client:
            _ = client.client  # Initialize
            assert client._client is not None

        assert client._client is None


class TestCetusClientBuildTimeFilter:
    """Tests for CetusClient._build_time_filter()."""

    @pytest.fixture
    def client(self) -> CetusClient:
        return CetusClient(api_key="test")

    def test_no_filter_when_no_since_days_and_no_marker(self, client: CetusClient):
        """Should return empty string when no time filter needed."""
        result = client._build_time_filter("dns", since_days=None, marker=None)
        assert result == ""

    def test_since_days_filter(self, client: CetusClient):
        """Should build time filter from since_days."""
        result = client._build_time_filter("dns", since_days=7, marker=None)

        assert "dns_timestamp" in result
        assert "AND" in result
        assert "TO *" in result

    def test_since_days_uses_correct_date(self, client: CetusClient):
        """Time filter should use correct date range."""
        result = client._build_time_filter("dns", since_days=7, marker=None)

        # Should contain a date from roughly 7 days ago
        expected_date = (datetime.today() - timedelta(days=7)).date().isoformat()
        assert expected_date in result

    def test_marker_overrides_since_days(self, client: CetusClient):
        """Marker timestamp should be used instead of since_days."""
        marker = Marker(
            query="test",
            index="dns",
            last_timestamp="2025-01-01T00:00:00Z",
            last_uuid="uuid",
            updated_at="2025-01-02T00:00:00Z",
        )

        result = client._build_time_filter("dns", since_days=7, marker=marker)

        assert "2025-01-01T00:00:00Z" in result
        # Should not contain a date from 7 days ago
        week_ago = (datetime.today() - timedelta(days=7)).date().isoformat()
        assert week_ago not in result

    def test_uses_correct_timestamp_field_for_index(self, client: CetusClient):
        """Should use index-specific timestamp field."""
        assert "dns_timestamp" in client._build_time_filter("dns", since_days=7, marker=None)
        assert "certstream_timestamp" in client._build_time_filter(
            "certstream", since_days=7, marker=None
        )
        assert "alerting_timestamp" in client._build_time_filter(
            "alerting", since_days=7, marker=None
        )


class TestCetusClientFetchPage:
    """Tests for CetusClient._fetch_page()."""

    @pytest.fixture
    def client(self) -> CetusClient:
        return CetusClient(api_key="test-key", host="http://localhost")

    def test_fetch_page_makes_post_request(self, client: CetusClient, httpx_mock):
        """_fetch_page should make POST request to /api/query/."""
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            json={"data": [], "has_more": False},
        )

        client._fetch_page("host:*", "dns", "nvme")

        requests = httpx_mock.get_requests()
        assert len(requests) == 1
        assert requests[0].method == "POST"
        client.close()

    def test_fetch_page_sends_correct_body(self, client: CetusClient, httpx_mock):
        """_fetch_page should send query, index, media in body."""
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            json={"data": [], "has_more": False},
        )

        client._fetch_page("host:*.example.com", "dns", "nvme")

        request = httpx_mock.get_requests()[0]
        import json

        body = json.loads(request.content)

        assert body["query"] == "host:*.example.com"
        assert body["index"] == "dns"
        assert body["media"] == "nvme"
        client.close()

    def test_fetch_page_includes_pagination_params(self, client: CetusClient, httpx_mock):
        """_fetch_page should include pit_id and search_after when provided."""
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            json={"data": [], "has_more": False},
        )

        client._fetch_page(
            "host:*",
            "dns",
            "nvme",
            pit_id="test-pit-id",
            search_after=["2025-01-01", "uuid-123"],
        )

        request = httpx_mock.get_requests()[0]
        import json

        body = json.loads(request.content)

        assert body["pit_id"] == "test-pit-id"
        assert body["search_after"] == ["2025-01-01", "uuid-123"]
        client.close()

    def test_fetch_page_raises_auth_error_on_401(self, client: CetusClient, httpx_mock):
        """_fetch_page should raise AuthenticationError on 401."""
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            status_code=401,
            text="Unauthorized",
        )

        with pytest.raises(AuthenticationError, match="Invalid API key"):
            client._fetch_page("host:*", "dns", "nvme")
        client.close()

    def test_fetch_page_raises_auth_error_on_403(self, client: CetusClient, httpx_mock):
        """_fetch_page should raise AuthenticationError on 403."""
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            status_code=403,
            text="Forbidden",
        )

        with pytest.raises(AuthenticationError, match="Access denied"):
            client._fetch_page("host:*", "dns", "nvme")
        client.close()

    def test_fetch_page_raises_api_error_on_other_errors(self, client: CetusClient, httpx_mock):
        """_fetch_page should raise APIError on other 4xx/5xx errors."""
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            status_code=500,
            text="Internal Server Error",
        )

        with pytest.raises(APIError) as exc_info:
            client._fetch_page("host:*", "dns", "nvme")

        assert exc_info.value.status_code == 500
        client.close()

    def test_fetch_page_extracts_error_detail_on_400(self, client: CetusClient, httpx_mock):
        """_fetch_page should extract error detail from 400 response JSON.

        When server returns 400 with {"detail": "error message"}, the client
        should include that message in the APIError for helpful user feedback.
        """
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            status_code=400,
            json={"detail": "Invalid query syntax: Cannot parse 'host:['"},
        )

        with pytest.raises(APIError) as exc_info:
            client._fetch_page("host:[", "dns", "nvme")

        # Should include the detail from the response
        assert "Invalid query syntax" in str(exc_info.value)
        assert exc_info.value.status_code == 400
        client.close()

    def test_fetch_page_handles_400_without_json(self, client: CetusClient, httpx_mock):
        """_fetch_page should handle 400 response without JSON body gracefully."""
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            status_code=400,
            text="Bad Request",
        )

        with pytest.raises(APIError) as exc_info:
            client._fetch_page("host:*", "dns", "nvme")

        assert exc_info.value.status_code == 400
        assert "Bad request" in str(exc_info.value)
        client.close()

    def test_fetch_page_raises_connection_error_on_connect_failure(
        self, client: CetusClient, httpx_mock
    ):
        """_fetch_page should raise ConnectionError on connection failure."""
        httpx_mock.add_exception(
            httpx.ConnectError("Connection refused"),
            method="POST",
            url="http://localhost/api/query/",
        )

        with pytest.raises(ConnectionError, match="Failed to connect"):
            client._fetch_page("host:*", "dns", "nvme")
        client.close()

    def test_fetch_page_raises_connection_error_on_timeout(self, client: CetusClient, httpx_mock):
        """_fetch_page should raise ConnectionError on timeout."""
        httpx_mock.add_exception(
            httpx.TimeoutException("Timeout"),
            method="POST",
            url="http://localhost/api/query/",
        )

        with pytest.raises(ConnectionError, match="timed out"):
            client._fetch_page("host:*", "dns", "nvme")
        client.close()


class TestCetusClientQuery:
    """Tests for CetusClient.query()."""

    @pytest.fixture
    def client(self) -> CetusClient:
        return CetusClient(api_key="test-key", host="http://localhost")

    def test_query_returns_query_result(self, client: CetusClient, httpx_mock):
        """query should return QueryResult."""
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            json={
                "data": [{"uuid": "1", "host": "a.com", "dns_timestamp": "2025-01-01T00:00:00Z"}],
                "has_more": False,
            },
        )

        result = client.query("host:*", index="dns")

        assert isinstance(result, QueryResult)
        assert len(result.data) == 1
        assert result.total_fetched == 1
        assert result.pages_fetched == 1
        client.close()

    def test_query_handles_empty_response(self, client: CetusClient, httpx_mock):
        """query should handle empty results."""
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            json={"data": [], "has_more": False},
        )

        result = client.query("host:nonexistent", index="dns")

        assert result.data == []
        assert result.total_fetched == 0
        assert result.last_uuid is None
        client.close()

    def test_query_tracks_last_uuid_and_timestamp(self, client: CetusClient, httpx_mock):
        """query should track last_uuid and last_timestamp."""
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            json={
                "data": [
                    {"uuid": "first", "dns_timestamp": "2025-01-01T00:00:00Z"},
                    {"uuid": "last", "dns_timestamp": "2025-01-01T12:00:00Z"},
                ],
                "has_more": False,
            },
        )

        result = client.query("host:*", index="dns")

        assert result.last_uuid == "last"
        assert result.last_timestamp == "2025-01-01T12:00:00Z"
        client.close()

    def test_query_paginates(self, client: CetusClient, httpx_mock):
        """query should paginate through multiple pages."""
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            json={
                "data": [{"uuid": "1", "dns_timestamp": "2025-01-01T00:00:00Z"}],
                "has_more": True,
                "pit_id": "pit-1",
                "search_after": ["2025-01-01T00:00:00Z", "1"],
            },
        )
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            json={
                "data": [{"uuid": "2", "dns_timestamp": "2025-01-01T01:00:00Z"}],
                "has_more": False,
            },
        )

        result = client.query("host:*", index="dns")

        assert len(result.data) == 2
        assert result.pages_fetched == 2
        assert result.total_fetched == 2
        client.close()

    def test_query_with_marker_skips_to_position(self, client: CetusClient, httpx_mock):
        """query with marker should skip records before marker position."""
        marker = Marker(
            query="host:*",
            index="dns",
            last_timestamp="2025-01-01T00:00:00Z",
            last_uuid="skip-me",
            updated_at="2025-01-02T00:00:00Z",
        )

        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            json={
                "data": [
                    {"uuid": "skip-me", "dns_timestamp": "2025-01-01T00:00:00Z"},
                    {"uuid": "keep-this", "dns_timestamp": "2025-01-01T01:00:00Z"},
                ],
                "has_more": False,
            },
        )

        result = client.query("host:*", index="dns", marker=marker)

        assert len(result.data) == 1
        assert result.data[0]["uuid"] == "keep-this"
        client.close()


class TestCetusClientQueryIter:
    """Tests for CetusClient.query_iter()."""

    @pytest.fixture
    def client(self) -> CetusClient:
        return CetusClient(api_key="test-key", host="http://localhost")

    def test_query_iter_yields_records(self, client: CetusClient, httpx_mock):
        """query_iter should yield individual records."""
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            json={
                "data": [
                    {"uuid": "1"},
                    {"uuid": "2"},
                    {"uuid": "3"},
                ],
                "has_more": False,
            },
        )

        records = list(client.query_iter("host:*", index="dns"))

        assert len(records) == 3
        assert records[0]["uuid"] == "1"
        assert records[1]["uuid"] == "2"
        assert records[2]["uuid"] == "3"
        client.close()

    def test_query_iter_paginates(self, client: CetusClient, httpx_mock):
        """query_iter should paginate automatically."""
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            json={
                "data": [{"uuid": "1"}],
                "has_more": True,
                "pit_id": "pit-1",
                "search_after": ["1"],
            },
        )
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            json={
                "data": [{"uuid": "2"}],
                "has_more": False,
            },
        )

        records = list(client.query_iter("host:*", index="dns"))

        assert len(records) == 2
        client.close()


class TestCetusClientListAlerts:
    """Tests for CetusClient.list_alerts()."""

    @pytest.fixture
    def client(self) -> CetusClient:
        return CetusClient(api_key="test-key", host="http://localhost")

    def test_list_alerts_returns_alert_list(self, client: CetusClient, httpx_mock):
        """list_alerts should return list of Alert objects."""
        httpx_mock.add_response(
            method="GET",
            url="http://localhost/alerts/api/unified/?owned=true&length=1000",
            json={
                "data": [
                    {"id": 1, "alert_type": "raw", "title": "Alert 1", "owned": True},
                    {"id": 2, "alert_type": "terms", "title": "Alert 2", "owned": True},
                ]
            },
        )

        alerts = client.list_alerts(owned=True)

        assert len(alerts) == 2
        assert all(isinstance(a, Alert) for a in alerts)
        assert alerts[0].id == 1
        assert alerts[1].id == 2
        client.close()

    def test_list_alerts_handles_empty(self, client: CetusClient, httpx_mock):
        """list_alerts should handle empty response."""
        httpx_mock.add_response(
            method="GET",
            url="http://localhost/alerts/api/unified/?owned=true&length=1000",
            json={"data": []},
        )

        alerts = client.list_alerts(owned=True)

        assert alerts == []
        client.close()

    def test_list_alerts_with_type_filter(self, client: CetusClient, httpx_mock):
        """list_alerts should filter by type when specified."""
        httpx_mock.add_response(
            method="GET",
            url="http://localhost/alerts/api/unified/?owned=true&type_filter=raw&length=1000",
            json={"data": []},
        )

        client.list_alerts(owned=True, alert_type="raw")

        request = httpx_mock.get_requests()[0]
        assert "type_filter=raw" in str(request.url)
        client.close()


class TestCetusClientGetAlert:
    """Tests for CetusClient.get_alert()."""

    @pytest.fixture
    def client(self) -> CetusClient:
        return CetusClient(api_key="test-key", host="http://localhost")

    def test_get_alert_returns_alert(self, client: CetusClient, httpx_mock):
        """get_alert should return Alert object."""
        httpx_mock.add_response(
            method="GET",
            url="http://localhost/alerts/api/unified/123/",
            json={
                "id": 123,
                "alert_type": "raw",
                "title": "My Alert",
                "query": "host:*.example.com",
            },
        )

        alert = client.get_alert(123)

        assert isinstance(alert, Alert)
        assert alert.id == 123
        assert alert.title == "My Alert"
        client.close()

    def test_get_alert_returns_none_on_404(self, client: CetusClient, httpx_mock):
        """get_alert should return None when alert not found."""
        httpx_mock.add_response(
            method="GET",
            url="http://localhost/alerts/api/unified/999/",
            status_code=404,
        )

        alert = client.get_alert(999)

        assert alert is None
        client.close()


class TestCetusClientGetAlertResults:
    """Tests for CetusClient.get_alert_results()."""

    @pytest.fixture
    def client(self) -> CetusClient:
        return CetusClient(api_key="test-key", host="http://localhost")

    def test_get_alert_results_returns_list(self, client: CetusClient, httpx_mock):
        """get_alert_results should return list of result records."""
        httpx_mock.add_response(
            method="GET",
            url="http://localhost/api/alert_results/123",
            json={
                "data": [
                    {"uuid": "r1", "host": "match.com"},
                    {"uuid": "r2", "host": "match2.com"},
                ]
            },
        )

        results = client.get_alert_results(123)

        assert len(results) == 2
        assert results[0]["uuid"] == "r1"
        client.close()

    def test_get_alert_results_with_since(self, client: CetusClient, httpx_mock):
        """get_alert_results should pass since parameter."""
        httpx_mock.add_response(
            method="GET",
            url="http://localhost/api/alert_results/123?since=2025-01-01T00%3A00%3A00Z",
            json={"data": []},
        )

        client.get_alert_results(123, since="2025-01-01T00:00:00Z")

        request = httpx_mock.get_requests()[0]
        assert "since=" in str(request.url)
        client.close()

    def test_get_alert_results_handles_empty(self, client: CetusClient, httpx_mock):
        """get_alert_results should handle empty results."""
        httpx_mock.add_response(
            method="GET",
            url="http://localhost/api/alert_results/123",
            json={"data": []},
        )

        results = client.get_alert_results(123)

        assert results == []
        client.close()
