"""Tests for async client methods."""

from __future__ import annotations

import pytest

from cetus.client import CetusClient, QueryResult
from cetus.exceptions import AuthenticationError


class TestQueryAsync:
    """Tests for query_async method."""

    @pytest.fixture
    def client(self) -> CetusClient:
        return CetusClient(api_key="test-key", host="http://localhost")

    @pytest.mark.asyncio
    async def test_query_async_returns_query_result(self, client: CetusClient, httpx_mock):
        """query_async should return QueryResult."""
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            json={
                "data": [{"uuid": "1", "host": "a.com", "dns_timestamp": "2025-01-01T00:00:00Z"}],
                "has_more": False,
                "pit_id": "pit123",
            },
        )

        result = await client.query_async("host:*")

        assert isinstance(result, QueryResult)
        assert len(result.data) == 1
        assert result.data[0]["host"] == "a.com"
        client.close()

    @pytest.mark.asyncio
    async def test_query_async_handles_empty_response(self, client: CetusClient, httpx_mock):
        """query_async should handle empty results."""
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            json={"data": [], "has_more": False},
        )

        result = await client.query_async("host:nonexistent")

        assert isinstance(result, QueryResult)
        assert len(result.data) == 0
        client.close()

    @pytest.mark.asyncio
    async def test_query_async_paginates(self, client: CetusClient, httpx_mock):
        """query_async should fetch multiple pages."""
        # First page
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            json={
                "data": [{"uuid": "1", "dns_timestamp": "2025-01-01T00:00:00Z"}],
                "has_more": True,
                "pit_id": "pit1",
                "search_after": ["2025-01-01T00:00:00Z", "1"],
            },
        )
        # Second page
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            json={
                "data": [{"uuid": "2", "dns_timestamp": "2025-01-01T00:01:00Z"}],
                "has_more": False,
                "pit_id": "pit1",
            },
        )

        result = await client.query_async("host:*")

        assert len(result.data) == 2
        assert result.pages_fetched == 2
        client.close()

    @pytest.mark.asyncio
    async def test_query_async_validates_params(self, client: CetusClient, httpx_mock):
        """query_async should validate parameters."""
        with pytest.raises(ValueError) as exc_info:
            await client.query_async("test", index="invalid")

        assert "Invalid index" in str(exc_info.value)
        assert len(httpx_mock.get_requests()) == 0
        client.close()

    @pytest.mark.asyncio
    async def test_query_async_handles_auth_error(self, client: CetusClient, httpx_mock):
        """query_async should raise AuthenticationError on 401."""
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            status_code=401,
        )

        with pytest.raises(AuthenticationError):
            await client.query_async("host:*")

        client.close()

    @pytest.mark.asyncio
    async def test_query_async_handles_rate_limit(self, client: CetusClient, httpx_mock):
        """query_async should handle 429 with retry."""
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            status_code=429,
            headers={"Retry-After": "0"},
        )
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            json={"data": [], "has_more": False},
        )

        result = await client.query_async("host:*")

        assert isinstance(result, QueryResult)
        assert len(httpx_mock.get_requests()) == 2
        client.close()


class TestFetchPageAsync:
    """Tests for _fetch_page_async method."""

    @pytest.fixture
    def client(self) -> CetusClient:
        return CetusClient(api_key="test-key", host="http://localhost")

    @pytest.mark.asyncio
    async def test_fetch_page_async_makes_request(self, client: CetusClient, httpx_mock):
        """_fetch_page_async should make POST request."""
        import httpx as httpx_lib

        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            json={"data": [], "has_more": False},
        )

        async with httpx_lib.AsyncClient(timeout=60) as async_client:
            result = await client._fetch_page_async(async_client, "host:*", "dns", "nvme")

        assert result == {"data": [], "has_more": False}
        client.close()

    @pytest.mark.asyncio
    async def test_fetch_page_async_includes_user_agent(self, client: CetusClient, httpx_mock):
        """_fetch_page_async should include User-Agent header."""
        import httpx as httpx_lib

        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            json={"data": [], "has_more": False},
        )

        async with httpx_lib.AsyncClient(timeout=60) as async_client:
            await client._fetch_page_async(async_client, "host:*", "dns", "nvme")

        request = httpx_mock.get_requests()[0]
        assert "User-Agent" in request.headers
        assert "cetus-client" in request.headers["User-Agent"]
        client.close()

    @pytest.mark.asyncio
    async def test_fetch_page_async_rate_limit_retry(self, client: CetusClient, httpx_mock):
        """_fetch_page_async should retry on 429."""
        import httpx as httpx_lib

        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            status_code=429,
            headers={"Retry-After": "0"},
        )
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            json={"data": [{"id": 1}], "has_more": False},
        )

        async with httpx_lib.AsyncClient(timeout=60) as async_client:
            result = await client._fetch_page_async(async_client, "host:*", "dns", "nvme")

        assert result["data"] == [{"id": 1}]
        assert len(httpx_mock.get_requests()) == 2
        client.close()


class TestQueryStreamAsync:
    """Tests for query_stream_async method."""

    @pytest.fixture
    def client(self) -> CetusClient:
        return CetusClient(api_key="test-key", host="http://localhost")

    @pytest.mark.asyncio
    async def test_query_stream_async_validates_params(self, client: CetusClient, httpx_mock):
        """query_stream_async should validate parameters."""
        with pytest.raises(ValueError) as exc_info:
            async for _ in client.query_stream_async("test", index="invalid"):
                pass

        assert "Invalid index" in str(exc_info.value)
        client.close()

    @pytest.mark.asyncio
    async def test_query_stream_async_yields_records(self, client: CetusClient, httpx_mock):
        """query_stream_async should yield individual records."""
        # Mock streaming response with NDJSON
        ndjson_content = '{"uuid": "1", "host": "a.com"}\n{"uuid": "2", "host": "b.com"}\n'
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/stream/",
            content=ndjson_content.encode(),
            headers={"Content-Type": "application/x-ndjson"},
        )

        records = []
        async for record in client.query_stream_async("host:*"):
            records.append(record)

        assert len(records) == 2
        assert records[0]["host"] == "a.com"
        assert records[1]["host"] == "b.com"
        client.close()

    @pytest.mark.asyncio
    async def test_query_stream_async_handles_auth_error(self, client: CetusClient, httpx_mock):
        """query_stream_async should raise AuthenticationError on 401."""
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/stream/",
            status_code=401,
        )

        with pytest.raises(AuthenticationError):
            async for _ in client.query_stream_async("host:*"):
                pass

        client.close()

    @pytest.mark.asyncio
    async def test_query_stream_async_includes_user_agent(self, client: CetusClient, httpx_mock):
        """query_stream_async should include User-Agent header."""
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/stream/",
            content=b'{"uuid": "1"}\n',
        )

        async for _ in client.query_stream_async("host:*"):
            pass

        request = httpx_mock.get_requests()[0]
        assert "User-Agent" in request.headers
        assert "cetus-client" in request.headers["User-Agent"]
        client.close()


class TestUserAgentHeader:
    """Tests for User-Agent header in all request types."""

    @pytest.fixture
    def client(self) -> CetusClient:
        return CetusClient(api_key="test-key", host="http://localhost")

    def test_sync_client_includes_user_agent(self, client: CetusClient, httpx_mock):
        """Sync HTTP client should include User-Agent."""
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/",
            json={"data": [], "has_more": False},
        )

        client._fetch_page("host:*", "dns", "nvme")

        request = httpx_mock.get_requests()[0]
        assert "User-Agent" in request.headers
        user_agent = request.headers["User-Agent"]
        from cetus import __version__

        assert f"cetus-client/{__version__}" in user_agent
        assert "Python" in user_agent
        client.close()

    def test_stream_includes_user_agent(self, client: CetusClient, httpx_mock):
        """Streaming requests should include User-Agent."""
        httpx_mock.add_response(
            method="POST",
            url="http://localhost/api/query/stream/",
            content=b'{"uuid": "1"}\n',
        )

        for _ in client.query_stream("host:*"):
            pass

        request = httpx_mock.get_requests()[0]
        assert "User-Agent" in request.headers
        assert "cetus-client" in request.headers["User-Agent"]
        client.close()
