"""Cetus API client."""

from __future__ import annotations

import logging
import platform
import time
from collections.abc import AsyncIterator, Callable, Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Literal
from urllib.parse import urlparse

import httpx

from . import __version__
from .exceptions import APIError, AuthenticationError, ConfigurationError, ConnectionError
from .markers import Marker

if TYPE_CHECKING:
    from .config import Config

logger = logging.getLogger(__name__)

Index = Literal["dns", "certstream", "alerting"]
Media = Literal["nvme", "all"]
AlertType = Literal["raw", "terms", "structured"]

# Valid parameter values for runtime validation
VALID_INDICES: frozenset[str] = frozenset({"dns", "certstream", "alerting"})
VALID_MEDIA: frozenset[str] = frozenset({"nvme", "all"})

# Hosts allowed to use HTTP (development only)
LOCALHOST_HOSTS: frozenset[str] = frozenset({"localhost", "127.0.0.1", "::1"})

# Rate limit retry settings
MAX_RATE_LIMIT_RETRIES = 3
DEFAULT_RETRY_AFTER = 60  # seconds

# Progress callback type: receives (records_fetched, pages_fetched)
ProgressCallback = Callable[[int, int], None]

# User-Agent for API requests
USER_AGENT = f"cetus-client/{__version__} (Python {platform.python_version()}; {platform.system()})"


@dataclass
class QueryResult:
    """Result from a query operation."""

    data: list[dict]
    total_fetched: int
    last_uuid: str | None
    last_timestamp: str | None
    pages_fetched: int


@dataclass
class Alert:
    """Represents an alert definition."""

    id: int
    alert_type: str
    title: str
    description: str
    query_preview: str
    owned: bool
    shared_by: str | None

    @classmethod
    def from_dict(cls, data: dict) -> Alert:
        return cls(
            id=data["id"],
            alert_type=data["alert_type"],
            title=data.get("title", ""),
            description=data.get("description", ""),
            query_preview=data.get("query_preview", ""),
            owned=data.get("owned", False),
            shared_by=data.get("shared_by"),
        )


class CetusClient:
    """Client for the Cetus alerting API."""

    PAGE_SIZE = 10000  # API returns up to 10k records per request

    def __init__(
        self,
        api_key: str,
        host: str = "alerting.sparkits.ca",
        timeout: int = 60,
    ):
        self.api_key = api_key
        self.host = host
        self.timeout = timeout
        self._client: httpx.Client | None = None
        self._base_url: str | None = None

    @classmethod
    def from_config(cls, config: Config) -> CetusClient:
        """Create a client from a Config object."""
        return cls(
            api_key=config.require_api_key(),
            host=config.host,
            timeout=config.timeout,
        )

    @property
    def _masked_api_key(self) -> str:
        """Return masked API key for logging (shows first 4 chars only)."""
        if not self.api_key:
            return "<none>"
        if len(self.api_key) <= 4:
            return "***"
        return f"{self.api_key[:4]}***"

    def _get_base_url(self) -> str:
        """Get the base URL, validating protocol security.

        HTTP is only allowed for localhost addresses (development).
        All other hosts require HTTPS.

        Raises:
            ConfigurationError: If HTTP is used with a non-localhost host
        """
        if self._base_url is not None:
            return self._base_url

        if self.host.startswith("http://"):
            parsed = urlparse(self.host)
            hostname = parsed.hostname or ""
            if hostname not in LOCALHOST_HOSTS:
                raise ConfigurationError(
                    f"HTTP is not allowed for remote hosts (got {hostname}). "
                    f"Use https://{parsed.netloc} instead."
                )
            logger.warning(
                "Using insecure HTTP connection to %s (development only)",
                hostname,
            )
            self._base_url = self.host
        elif self.host.startswith("https://"):
            self._base_url = self.host
        else:
            self._base_url = f"https://{self.host}"

        return self._base_url

    def _validate_params(self, index: str, media: str) -> None:
        """Validate query parameters.

        Raises:
            ValueError: If index or media is not a valid value
        """
        if index not in VALID_INDICES:
            raise ValueError(
                f"Invalid index: {index!r}. Must be one of: {', '.join(sorted(VALID_INDICES))}"
            )
        if media not in VALID_MEDIA:
            raise ValueError(
                f"Invalid media: {media!r}. Must be one of: {', '.join(sorted(VALID_MEDIA))}"
            )

    @property
    def client(self) -> httpx.Client:
        """Lazy-initialize the HTTP client."""
        if self._client is None:
            base_url = self._get_base_url()
            self._client = httpx.Client(
                base_url=base_url,
                headers={
                    "Authorization": f"Api-Key {self.api_key}",
                    "Accept": "application/json",
                    "User-Agent": USER_AGENT,
                },
                timeout=self.timeout,
                # Explicit TLS verification (httpx default, but being explicit)
                verify=True,
            )
        return self._client

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client is not None:
            self._client.close()
            self._client = None

    def __enter__(self) -> CetusClient:
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def _is_dsl_query(self, query: str) -> bool:
        """Check if a query is Elasticsearch DSL (JSON) rather than Lucene.

        DSL queries start with '{' and are valid JSON objects.
        """
        import json

        stripped = query.strip()
        if not stripped.startswith("{"):
            return False
        try:
            parsed = json.loads(stripped)
            return isinstance(parsed, dict)
        except (json.JSONDecodeError, ValueError):
            return False

    def _build_time_filter(
        self,
        index: Index,
        since_days: int | None,
        marker: Marker | None,
    ) -> str:
        """Build the timestamp filter suffix for Lucene queries."""
        timestamp_field = f"{index}_timestamp"

        if marker:
            # Resume from marker position
            return f" AND {timestamp_field}:[{marker.last_timestamp} TO *]"
        elif since_days:
            # Look back N days
            since_date = (datetime.today() - timedelta(days=since_days)).replace(microsecond=0)
            return f" AND {timestamp_field}:[{since_date.isoformat()} TO *]"
        else:
            return ""

    def _build_full_query(
        self,
        search: str,
        index: Index,
        since_days: int | None,
        marker: Marker | None,
    ) -> str:
        """Build the full query with time filter.

        For Lucene queries: wraps with parentheses and appends time filter.
        For DSL queries: wraps the DSL in a bool query with time filter.
        """
        import json

        timestamp_field = f"{index}_timestamp"

        if self._is_dsl_query(search):
            # DSL query - need to wrap in bool with time filter
            parsed_query = json.loads(search.strip())

            # If the query has a top-level "query" key (full ES query format),
            # extract the inner query body. ES expects just the query body.
            if "query" in parsed_query and len(parsed_query) == 1:
                parsed_query = parsed_query["query"]

            # Determine the time constraint
            if marker:
                time_value = marker.last_timestamp
            elif since_days:
                since_date = (datetime.today() - timedelta(days=since_days)).replace(microsecond=0)
                time_value = since_date.isoformat()
            else:
                # No time filter needed, return unwrapped query
                return json.dumps(parsed_query)

            # Build a DSL query with both the original query and time filter
            wrapped_query = {
                "bool": {
                    "must": [parsed_query],
                    "filter": [{"range": {timestamp_field: {"gte": time_value}}}],
                }
            }
            return json.dumps(wrapped_query)
        else:
            # Lucene query - use string concatenation
            time_filter = self._build_time_filter(index, since_days, marker)
            return f"({search}){time_filter}"

    def _handle_error_response(self, response: httpx.Response) -> None:
        """Handle error responses, sanitizing error messages.

        Raises appropriate exceptions for error status codes.
        For 400 errors (bad request), attempts to extract the error detail
        from the response to provide helpful feedback.
        """
        if response.status_code == 401:
            raise AuthenticationError(
                "Invalid API key. The API key system was updated - you may need to "
                "generate a new key at Profile -> Manage API Key"
            )
        elif response.status_code == 403:
            raise AuthenticationError("Access denied - check your permissions")
        elif response.status_code == 400:
            # For 400 errors, try to extract the error detail from JSON response
            # DRF returns {"detail": "error message"} for ParseError
            logger.debug("API error response: %s", response.text[:500])
            try:
                error_data = response.json()
                detail = error_data.get("detail", "")
                if detail:
                    raise APIError(detail, status_code=400)
            except (ValueError, KeyError):
                pass
            raise APIError("Bad request", status_code=400)
        elif response.status_code >= 400:
            # Log full error for debugging, but don't expose to user
            logger.debug("API error response: %s", response.text[:500])
            # Provide sanitized error message
            raise APIError(
                f"Server returned error {response.status_code}",
                status_code=response.status_code,
            )

    def _fetch_page(
        self,
        query: str,
        index: Index,
        media: Media,
        pit_id: str | None = None,
        search_after: list | None = None,
    ) -> dict:
        """Fetch a single page of results from the API.

        Includes rate limit handling with automatic retry.
        """
        body = {
            "query": query,
            "index": index,
            "media": media,
        }
        if pit_id:
            body["pit_id"] = pit_id
        if search_after:
            body["search_after"] = search_after

        logger.debug("Request body: %s", body)

        for attempt in range(MAX_RATE_LIMIT_RETRIES + 1):
            try:
                response = self.client.post("/api/query/", json=body)
            except httpx.ConnectError as e:
                raise ConnectionError(f"Failed to connect to {self.host}: {e}") from e
            except httpx.TimeoutException as e:
                raise ConnectionError(f"Request timed out after {self.timeout}s: {e}") from e

            logger.debug("Response status: %d", response.status_code)

            # Handle rate limiting with retry
            if response.status_code == 429:
                if attempt >= MAX_RATE_LIMIT_RETRIES:
                    raise APIError(
                        "Rate limit exceeded - too many requests",
                        status_code=429,
                    )
                retry_after = int(response.headers.get("Retry-After", DEFAULT_RETRY_AFTER))
                logger.warning(
                    "Rate limited, waiting %d seconds before retry (attempt %d/%d)",
                    retry_after,
                    attempt + 1,
                    MAX_RATE_LIMIT_RETRIES,
                )
                time.sleep(retry_after)
                continue

            break

        self._handle_error_response(response)
        return response.json()

    def query(
        self,
        search: str,
        index: Index = "dns",
        media: Media = "nvme",
        since_days: int | None = 7,
        marker: Marker | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> QueryResult:
        """Execute a query and return all results.

        Args:
            search: The search query (Lucene syntax)
            index: Which index to query (dns, certstream, alerting)
            media: Storage tier preference (nvme for fast, all for complete)
            since_days: How many days back to search (ignored if marker is set)
            marker: Resume from this marker position
            progress_callback: Optional callback called with (records_fetched, pages_fetched)
                after each page is processed

        Returns:
            QueryResult containing all fetched data

        Raises:
            ValueError: If index or media is invalid
        """
        self._validate_params(index, media)
        all_data: list[dict] = []
        pages_fetched = 0
        last_uuid: str | None = None
        last_timestamp: str | None = None
        pit_id: str | None = None
        search_after: list | None = None
        marker_uuid = marker.last_uuid if marker else None
        timestamp_field = f"{index}_timestamp"

        # Build initial query with time filter (only needed for first request)
        full_query = self._build_full_query(search, index, since_days, marker)

        while True:
            response = self._fetch_page(full_query, index, media, pit_id, search_after)
            pages_fetched += 1

            data = response.get("data", [])
            if not data:
                break

            # If we have a marker, skip records until we pass it
            if marker_uuid:
                skip_count = 0
                for item in data:
                    skip_count += 1
                    if item.get("uuid") == marker_uuid:
                        marker_uuid = None  # Found it, stop skipping
                        break

                if skip_count == len(data):
                    # Marker record was the last one or not found in this page
                    if marker_uuid is None:
                        # Found at end of page, nothing new here
                        break
                    # Not found yet, continue to next page
                    pass
                else:
                    # Add records after the marker
                    data = data[skip_count:]

            all_data.extend(data)

            # Track last record for marker update
            if all_data:
                last_uuid = all_data[-1].get("uuid")
                last_timestamp = all_data[-1].get(timestamp_field)

            # Report progress
            if progress_callback:
                progress_callback(len(all_data), pages_fetched)

            # Check if there are more pages
            if not response.get("has_more", False):
                break

            # Get pagination cursor for next page
            pit_id = response.get("pit_id")
            search_after = response.get("search_after")

        return QueryResult(
            data=all_data,
            total_fetched=len(all_data),
            last_uuid=last_uuid,
            last_timestamp=last_timestamp,
            pages_fetched=pages_fetched,
        )

    async def _fetch_page_async(
        self,
        client: httpx.AsyncClient,
        query: str,
        index: Index,
        media: Media,
        pit_id: str | None = None,
        search_after: list | None = None,
    ) -> dict:
        """Fetch a single page of results from the API (async version).

        Includes rate limit handling with automatic retry.
        """
        import asyncio

        body = {
            "query": query,
            "index": index,
            "media": media,
        }
        if pit_id:
            body["pit_id"] = pit_id
        if search_after:
            body["search_after"] = search_after

        logger.debug("Async request body: %s", body)

        base_url = self._get_base_url()

        for attempt in range(MAX_RATE_LIMIT_RETRIES + 1):
            try:
                response = await client.post(
                    f"{base_url}/api/query/",
                    json=body,
                    headers={
                        "Authorization": f"Api-Key {self.api_key}",
                        "Accept": "application/json",
                        "User-Agent": USER_AGENT,
                    },
                )
            except httpx.ConnectError as e:
                raise ConnectionError(f"Failed to connect to {self.host}: {e}") from e
            except httpx.TimeoutException as e:
                raise ConnectionError(f"Request timed out after {self.timeout}s: {e}") from e

            logger.debug("Response status: %d", response.status_code)

            # Handle rate limiting with retry
            if response.status_code == 429:
                if attempt >= MAX_RATE_LIMIT_RETRIES:
                    raise APIError(
                        "Rate limit exceeded - too many requests",
                        status_code=429,
                    )
                retry_after = int(response.headers.get("Retry-After", DEFAULT_RETRY_AFTER))
                logger.warning(
                    "Rate limited, waiting %d seconds before retry (attempt %d/%d)",
                    retry_after,
                    attempt + 1,
                    MAX_RATE_LIMIT_RETRIES,
                )
                await asyncio.sleep(retry_after)
                continue

            break

        self._handle_error_response(response)
        return response.json()

    async def query_async(
        self,
        search: str,
        index: Index = "dns",
        media: Media = "nvme",
        since_days: int | None = 7,
        marker: Marker | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> QueryResult:
        """Execute a query and return all results (async version).

        This async version provides better interrupt handling (Ctrl+C) compared
        to the sync version, as signals are processed between await points.

        Args:
            search: The search query (Lucene syntax)
            index: Which index to query (dns, certstream, alerting)
            media: Storage tier preference (nvme for fast, all for complete)
            since_days: How many days back to search (ignored if marker is set)
            marker: Resume from this marker position
            progress_callback: Optional callback called with (records_fetched, pages_fetched)
                after each page is processed

        Returns:
            QueryResult containing all fetched data

        Raises:
            ValueError: If index or media is invalid
        """
        self._validate_params(index, media)
        all_data: list[dict] = []
        pages_fetched = 0
        last_uuid: str | None = None
        last_timestamp: str | None = None
        pit_id: str | None = None
        search_after: list | None = None
        marker_uuid = marker.last_uuid if marker else None
        timestamp_field = f"{index}_timestamp"

        # Build initial query with time filter (only needed for first request)
        full_query = self._build_full_query(search, index, since_days, marker)

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            while True:
                response = await self._fetch_page_async(
                    client, full_query, index, media, pit_id, search_after
                )
                pages_fetched += 1

                data = response.get("data", [])
                if not data:
                    break

                # If we have a marker, skip records until we pass it
                if marker_uuid:
                    skip_count = 0
                    for item in data:
                        skip_count += 1
                        if item.get("uuid") == marker_uuid:
                            marker_uuid = None  # Found it, stop skipping
                            break

                    if skip_count == len(data):
                        # Marker record was the last one or not found in this page
                        if marker_uuid is None:
                            # Found at end of page, nothing new here
                            break
                        # Not found yet, continue to next page
                        pass
                    else:
                        # Add records after the marker
                        data = data[skip_count:]

                all_data.extend(data)

                # Track last record for marker update
                if all_data:
                    last_uuid = all_data[-1].get("uuid")
                    last_timestamp = all_data[-1].get(timestamp_field)

                # Report progress
                if progress_callback:
                    progress_callback(len(all_data), pages_fetched)

                # Check if there are more pages
                if not response.get("has_more", False):
                    break

                # Get pagination cursor for next page
                pit_id = response.get("pit_id")
                search_after = response.get("search_after")

        return QueryResult(
            data=all_data,
            total_fetched=len(all_data),
            last_uuid=last_uuid,
            last_timestamp=last_timestamp,
            pages_fetched=pages_fetched,
        )

    def query_iter(
        self,
        search: str,
        index: Index = "dns",
        media: Media = "nvme",
        since_days: int | None = 7,
        marker: Marker | None = None,
    ) -> Iterator[dict]:
        """Execute a query and yield results one at a time.

        This is more memory-efficient for large result sets.
        Same arguments as query().

        Raises:
            ValueError: If index or media is invalid
        """
        self._validate_params(index, media)
        pit_id: str | None = None
        search_after: list | None = None
        marker_uuid = marker.last_uuid if marker else None

        full_query = self._build_full_query(search, index, since_days, marker)

        while True:
            response = self._fetch_page(full_query, index, media, pit_id, search_after)
            data = response.get("data", [])
            if not data:
                break

            # Skip to marker position if needed
            start_idx = 0
            if marker_uuid:
                for i, item in enumerate(data):
                    if item.get("uuid") == marker_uuid:
                        start_idx = i + 1
                        marker_uuid = None
                        break
                if marker_uuid:
                    # Marker not found in this page, skip all
                    start_idx = len(data)

            # Yield records
            for item in data[start_idx:]:
                yield item

            # Check if there are more pages
            if not response.get("has_more", False):
                break

            # Get pagination cursor for next page
            pit_id = response.get("pit_id")
            search_after = response.get("search_after")

    def query_stream(
        self,
        search: str,
        index: Index = "dns",
        media: Media = "nvme",
        since_days: int | None = 7,
        marker: Marker | None = None,
    ) -> Iterator[dict]:
        """Execute a streaming query, yielding results as they arrive from the server.

        This uses the streaming API endpoint which returns NDJSON, allowing
        results to be processed immediately as they're received rather than
        waiting for all pages to be fetched.

        Args:
            search: The search query (Lucene syntax)
            index: Which index to query (dns, certstream, alerting)
            media: Storage tier preference (nvme for fast, all for complete)
            since_days: How many days back to search (ignored if marker is set)
            marker: Resume from this marker position

        Yields:
            dict: Individual records as they arrive from the server

        Raises:
            ValueError: If index or media is invalid
        """
        import json

        self._validate_params(index, media)

        marker_uuid = marker.last_uuid if marker else None
        past_marker = marker_uuid is None

        full_query = self._build_full_query(search, index, since_days, marker)

        body = {
            "query": full_query,
            "index": index,
            "media": media,
        }

        logger.debug("Streaming request body: %s", body)

        base_url = self._get_base_url()
        url = f"{base_url}/api/query/stream/"

        try:
            # Use a timeout that allows periodic interrupt checks on Windows
            # connect/pool timeouts use self.timeout, but read uses 30s chunks
            timeout = httpx.Timeout(self.timeout, read=30.0)

            with httpx.stream(
                "POST",
                url,
                json=body,
                headers={
                    "Authorization": f"Api-Key {self.api_key}",
                    # Primary: ndjson for streaming, fallback: json for DRF error responses
                    "Accept": "application/x-ndjson, application/json;q=0.9",
                    "User-Agent": USER_AGENT,
                },
                timeout=timeout,
                verify=True,
            ) as response:
                if response.status_code == 401:
                    raise AuthenticationError(
                        "Invalid API key. The API key system was updated - you may need to "
                        "generate a new key at Profile -> Manage API Key"
                    )
                elif response.status_code == 403:
                    raise AuthenticationError("Access denied - check your permissions")
                elif response.status_code >= 400:
                    response.read()
                    # Log full error, show sanitized message
                    logger.debug("Streaming API error: %s", response.text[:500])
                    raise APIError(
                        f"Server returned error {response.status_code}",
                        status_code=response.status_code,
                    )

                # Read lines as they arrive
                for line in response.iter_lines():
                    if not line:
                        continue

                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        logger.warning("Failed to parse NDJSON line: %s", line[:100])
                        continue

                    # Skip records until we pass the marker
                    if not past_marker:
                        if record.get("uuid") == marker_uuid:
                            past_marker = True
                        continue

                    yield record

        except httpx.ConnectError as e:
            raise ConnectionError(f"Failed to connect to {self.host}: {e}") from e
        except httpx.TimeoutException as e:
            raise ConnectionError(f"Request timed out after {self.timeout}s: {e}") from e

    async def query_stream_async(
        self,
        search: str,
        index: Index = "dns",
        media: Media = "nvme",
        since_days: int | None = 7,
        marker: Marker | None = None,
    ) -> AsyncIterator[dict]:
        """Execute a streaming query asynchronously, yielding results as they arrive.

        This async version provides better interrupt handling (Ctrl+C) compared
        to the sync version, as signals are processed between await points.

        Args:
            search: The search query (Lucene syntax)
            index: Which index to query (dns, certstream, alerting)
            media: Storage tier preference (nvme for fast, all for complete)
            since_days: How many days back to search (ignored if marker is set)
            marker: Resume from this marker position

        Yields:
            dict: Individual records as they arrive from the server

        Raises:
            ValueError: If index or media is invalid
        """
        import json

        self._validate_params(index, media)

        marker_uuid = marker.last_uuid if marker else None
        past_marker = marker_uuid is None

        full_query = self._build_full_query(search, index, since_days, marker)

        body = {
            "query": full_query,
            "index": index,
            "media": media,
        }

        logger.debug("Async streaming request body: %s", body)

        base_url = self._get_base_url()
        url = f"{base_url}/api/query/stream/"

        try:
            async with httpx.AsyncClient(timeout=self.timeout, verify=True) as client:
                async with client.stream(
                    "POST",
                    url,
                    json=body,
                    headers={
                        "Authorization": f"Api-Key {self.api_key}",
                        # Primary: ndjson for streaming, fallback: json for DRF error responses
                        "Accept": "application/x-ndjson, application/json;q=0.9",
                        "User-Agent": USER_AGENT,
                    },
                ) as response:
                    if response.status_code == 401:
                        raise AuthenticationError(
                            "Invalid API key. The API key system was updated - you may need to "
                            "generate a new key at Profile -> Manage API Key"
                        )
                    elif response.status_code == 403:
                        raise AuthenticationError("Access denied - check your permissions")
                    elif response.status_code >= 400:
                        await response.aread()
                        # Log full error, show sanitized message
                        logger.debug("Async streaming API error: %s", response.text[:500])
                        raise APIError(
                            f"Server returned error {response.status_code}",
                            status_code=response.status_code,
                        )

                    # Read lines as they arrive - async iteration allows signal processing
                    async for line in response.aiter_lines():
                        if not line:
                            continue

                        try:
                            record = json.loads(line)
                        except json.JSONDecodeError:
                            logger.warning("Failed to parse NDJSON line: %s", line[:100])
                            continue

                        # Skip records until we pass the marker
                        if not past_marker:
                            if record.get("uuid") == marker_uuid:
                                past_marker = True
                            continue

                        yield record

        except httpx.ConnectError as e:
            raise ConnectionError(f"Failed to connect to {self.host}: {e}") from e
        except httpx.TimeoutException as e:
            raise ConnectionError(f"Request timed out after {self.timeout}s: {e}") from e

    def list_alerts(
        self,
        owned: bool = True,
        shared: bool = False,
        alert_type: AlertType | None = None,
    ) -> list[Alert]:
        """List alert definitions.

        Args:
            owned: Include alerts owned by the user
            shared: Include alerts shared with the user
            alert_type: Filter by alert type (raw, terms, structured)

        Returns:
            List of Alert objects
        """
        params = {}
        if owned:
            params["owned"] = "true"
        if shared:
            params["shared"] = "true"
        if alert_type:
            params["type_filter"] = alert_type
        # Request all results (large length to avoid pagination)
        params["length"] = "1000"

        logger.debug("Listing alerts with params: %s", params)

        try:
            response = self.client.get("/alerts/api/unified/", params=params)
        except httpx.ConnectError as e:
            raise ConnectionError(f"Failed to connect to {self.host}: {e}") from e
        except httpx.TimeoutException as e:
            raise ConnectionError(f"Request timed out after {self.timeout}s: {e}") from e

        if response.status_code == 401:
            raise AuthenticationError(
                "Invalid API key. The API key system was updated - you may need to "
                "generate a new key at Profile -> Manage API Key"
            )
        elif response.status_code == 403:
            raise AuthenticationError(
                "Access denied - you may need AlertingEnabled group membership"
            )
        elif response.status_code >= 400:
            logger.debug("List alerts API error: %s", response.text[:500])
            raise APIError(
                f"Server returned error {response.status_code}",
                status_code=response.status_code,
            )

        data = response.json()
        alerts_data = data.get("data", [])
        return [Alert.from_dict(a) for a in alerts_data]

    def get_alert(self, alert_id: int) -> Alert | None:
        """Get a specific alert by ID.

        Args:
            alert_id: The alert definition ID (globally unique)

        Returns:
            Alert object if found, None otherwise
        """
        url = f"/alerts/api/unified/{alert_id}/"
        logger.debug("Getting alert %d", alert_id)

        try:
            response = self.client.get(url)
        except httpx.ConnectError as e:
            raise ConnectionError(f"Failed to connect to {self.host}: {e}") from e
        except httpx.TimeoutException as e:
            raise ConnectionError(f"Request timed out after {self.timeout}s: {e}") from e

        if response.status_code == 401:
            raise AuthenticationError(
                "Invalid API key. The API key system was updated - you may need to "
                "generate a new key at Profile -> Manage API Key"
            )
        elif response.status_code == 403:
            raise AuthenticationError(
                "Access denied - you don't have permission to view this alert"
            )
        elif response.status_code == 404:
            return None
        elif response.status_code >= 400:
            logger.debug("Get alert API error: %s", response.text[:500])
            raise APIError(
                f"Server returned error {response.status_code}",
                status_code=response.status_code,
            )

        data = response.json()
        return Alert(
            id=data["id"],
            alert_type=data["alert_type"],
            title=data.get("title", ""),
            description=data.get("description", ""),
            query_preview=data.get("query", ""),
            owned=data.get("owned", False),
            shared_by=data.get("shared_by"),
        )

    def get_alert_results(
        self,
        alert_id: int,
        since: str | None = None,
    ) -> list[dict]:
        """Get results for an alert definition.

        Args:
            alert_id: The alert definition ID
            since: Optional ISO 8601 timestamp to filter results

        Returns:
            List of alert result records
        """
        url = f"/api/alert_results/{alert_id}"
        params = {}
        if since:
            params["since"] = since

        logger.debug("Getting alert results for ID %d", alert_id)

        try:
            response = self.client.get(url, params=params)
        except httpx.ConnectError as e:
            raise ConnectionError(f"Failed to connect to {self.host}: {e}") from e
        except httpx.TimeoutException as e:
            raise ConnectionError(f"Request timed out after {self.timeout}s: {e}") from e

        if response.status_code == 401:
            raise AuthenticationError(
                "Invalid API key. The API key system was updated - you may need to "
                "generate a new key at Profile -> Manage API Key"
            )
        elif response.status_code == 403:
            raise AuthenticationError(
                "Access denied - you don't have permission to view this alert"
            )
        elif response.status_code == 400:
            logger.debug("Get alert results bad request: %s", response.text[:500])
            raise APIError(
                "Bad request - check alert ID and parameters",
                status_code=response.status_code,
            )
        elif response.status_code >= 400:
            logger.debug("Get alert results API error: %s", response.text[:500])
            raise APIError(
                f"Server returned error {response.status_code}",
                status_code=response.status_code,
            )

        data = response.json()
        return data.get("data", [])
