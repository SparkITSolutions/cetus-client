"""Cetus API client."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Iterator, Literal

import httpx

from .exceptions import APIError, AuthenticationError, ConnectionError
from .markers import Marker

if TYPE_CHECKING:
    from .config import Config

logger = logging.getLogger(__name__)

Index = Literal["dns", "certstream", "alerting"]
Media = Literal["nvme", "all"]
AlertType = Literal["raw", "terms", "structured"]


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
        timeout: int = 30,
    ):
        self.api_key = api_key
        self.host = host
        self.timeout = timeout
        self._client: httpx.Client | None = None

    @classmethod
    def from_config(cls, config: Config) -> CetusClient:
        """Create a client from a Config object."""
        return cls(
            api_key=config.require_api_key(),
            host=config.host,
            timeout=config.timeout,
        )

    @property
    def client(self) -> httpx.Client:
        """Lazy-initialize the HTTP client."""
        if self._client is None:
            self._client = httpx.Client(
                base_url=f"https://{self.host}",
                headers={
                    "Authorization": f"Token {self.api_key}",
                    "Accept": "application/json",
                },
                timeout=self.timeout,
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

    def _build_time_filter(
        self,
        index: Index,
        since_days: int | None,
        marker: Marker | None,
    ) -> str:
        """Build the timestamp filter suffix for the query."""
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

    def _fetch_page(
        self,
        query: str,
        index: Index,
        media: Media,
        pit_id: str | None = None,
    ) -> dict:
        """Fetch a single page of results from the API."""
        body = {
            "query": query,
            "index": index,
            "media": media,
        }
        if pit_id:
            body["pit_id"] = pit_id

        logger.debug("Request body: %s", body)

        try:
            response = self.client.post("/api/query/", json=body)
        except httpx.ConnectError as e:
            raise ConnectionError(f"Failed to connect to {self.host}: {e}") from e
        except httpx.TimeoutException as e:
            raise ConnectionError(f"Request timed out after {self.timeout}s: {e}") from e

        logger.debug("Response status: %d", response.status_code)

        if response.status_code == 401:
            raise AuthenticationError("Invalid API key")
        elif response.status_code == 403:
            raise AuthenticationError("Access denied - check your permissions")
        elif response.status_code >= 400:
            raise APIError(
                f"API error: {response.text[:500]}",
                status_code=response.status_code,
            )

        return response.json()

    def query(
        self,
        search: str,
        index: Index = "dns",
        media: Media = "nvme",
        since_days: int | None = 7,
        marker: Marker | None = None,
    ) -> QueryResult:
        """Execute a query and return all results.

        Args:
            search: The search query (Lucene syntax)
            index: Which index to query (dns, certstream, alerting)
            media: Storage tier preference (nvme for fast, all for complete)
            since_days: How many days back to search (ignored if marker is set)
            marker: Resume from this marker position

        Returns:
            QueryResult containing all fetched data
        """
        all_data: list[dict] = []
        pages_fetched = 0
        last_uuid: str | None = None
        last_timestamp: str | None = None
        pit_id: str | None = None
        marker_uuid = marker.last_uuid if marker else None
        timestamp_field = f"{index}_timestamp"

        time_filter = self._build_time_filter(index, since_days, marker)
        full_query = f"({search}){time_filter}"

        while True:
            response = self._fetch_page(full_query, index, media, pit_id)
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

            # Check if there are more pages
            if len(response.get("data", [])) < self.PAGE_SIZE:
                break

            # Update query for next page and get PIT ID
            pit_id = response.get("pit_id")
            if last_timestamp:
                time_filter = f" AND {timestamp_field}:[{last_timestamp} TO *]"
                full_query = f"({search}){time_filter}"

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
        """
        pit_id: str | None = None
        marker_uuid = marker.last_uuid if marker else None
        timestamp_field = f"{index}_timestamp"
        last_timestamp: str | None = None

        time_filter = self._build_time_filter(index, since_days, marker)
        full_query = f"({search}){time_filter}"

        while True:
            response = self._fetch_page(full_query, index, media, pit_id)
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
                last_timestamp = item.get(timestamp_field)

            # Check for more pages
            if len(data) < self.PAGE_SIZE:
                break

            pit_id = response.get("pit_id")
            if last_timestamp:
                time_filter = f" AND {timestamp_field}:[{last_timestamp} TO *]"
                full_query = f"({search}){time_filter}"

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
            raise AuthenticationError("Invalid API key")
        elif response.status_code == 403:
            raise AuthenticationError("Access denied - you may need AlertingEnabled group membership")
        elif response.status_code >= 400:
            raise APIError(
                f"API error: {response.text[:500]}",
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
            raise AuthenticationError("Invalid API key")
        elif response.status_code == 403:
            raise AuthenticationError("Access denied - you don't have permission to view this alert")
        elif response.status_code == 404:
            return None
        elif response.status_code >= 400:
            raise APIError(
                f"API error: {response.text[:500]}",
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
            raise AuthenticationError("Invalid API key")
        elif response.status_code == 403:
            raise AuthenticationError("Access denied - you don't have permission to view this alert")
        elif response.status_code == 400:
            raise APIError(
                f"Bad request: {response.text[:500]}",
                status_code=response.status_code,
            )
        elif response.status_code >= 400:
            raise APIError(
                f"API error: {response.text[:500]}",
                status_code=response.status_code,
            )

        data = response.json()
        return data.get("data", [])
