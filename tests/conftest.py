"""Shared test fixtures for the Cetus CLI test suite."""

from __future__ import annotations

import json
import os
from collections.abc import Generator
from pathlib import Path
from unittest.mock import patch

import pytest


@pytest.fixture
def temp_config_dir(tmp_path: Path) -> Generator[Path, None, None]:
    """Create a temporary config directory for testing."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()

    with patch("cetus.config.get_config_dir", return_value=config_dir):
        yield config_dir


@pytest.fixture
def temp_data_dir(tmp_path: Path) -> Generator[Path, None, None]:
    """Create a temporary data directory for markers."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    with patch("cetus.config.get_data_dir", return_value=data_dir):
        yield data_dir


@pytest.fixture
def temp_markers_dir(tmp_path: Path) -> Generator[Path, None, None]:
    """Create a temporary markers directory."""
    markers_dir = tmp_path / "markers"
    markers_dir.mkdir()
    yield markers_dir


@pytest.fixture
def clean_env() -> Generator[None, None, None]:
    """Temporarily remove Cetus-related environment variables."""
    env_vars = ["CETUS_API_KEY", "CETUS_HOST", "CETUS_TIMEOUT", "CETUS_SINCE_DAYS"]
    old_values = {var: os.environ.pop(var, None) for var in env_vars}

    try:
        yield
    finally:
        for var, value in old_values.items():
            if value is not None:
                os.environ[var] = value


@pytest.fixture
def sample_api_key() -> str:
    """Return a sample API key for testing."""
    return "test-api-key-12345"


@pytest.fixture
def sample_host() -> str:
    """Return a sample host for testing."""
    return "test.example.com"


@pytest.fixture
def sample_dns_records() -> list[dict]:
    """Return sample DNS query results."""
    return [
        {
            "uuid": "record-uuid-001",
            "host": "www.example.com",
            "A": "192.168.1.1",
            "dns_timestamp": "2025-01-01T10:00:00Z",
        },
        {
            "uuid": "record-uuid-002",
            "host": "api.example.com",
            "A": "192.168.1.2",
            "dns_timestamp": "2025-01-01T11:00:00Z",
        },
        {
            "uuid": "record-uuid-003",
            "host": "mail.example.com",
            "A": "192.168.1.3",
            "AAAA": "2001:db8::1",
            "dns_timestamp": "2025-01-01T12:00:00Z",
        },
    ]


@pytest.fixture
def sample_certstream_records() -> list[dict]:
    """Return sample certstream query results."""
    return [
        {
            "uuid": "cert-uuid-001",
            "host": "secure.example.com",
            "issuer": "Let's Encrypt",
            "certstream_timestamp": "2025-01-01T10:00:00Z",
        },
        {
            "uuid": "cert-uuid-002",
            "host": "shop.example.com",
            "issuer": "DigiCert",
            "certstream_timestamp": "2025-01-01T11:00:00Z",
        },
    ]


@pytest.fixture
def sample_alerts() -> list[dict]:
    """Return sample alert definitions."""
    return [
        {
            "id": 1,
            "alert_type": "raw",
            "title": "Test Alert 1",
            "description": "A test raw alert",
            "query_preview": "host:*.example.com",
            "owned": True,
            "shared_by": None,
        },
        {
            "id": 2,
            "alert_type": "terms",
            "title": "Test Alert 2",
            "description": "A test terms alert",
            "query_preview": "A:192.168.1.*",
            "owned": True,
            "shared_by": None,
        },
        {
            "id": 3,
            "alert_type": "structured",
            "title": "Shared Alert",
            "description": "An alert shared with us",
            "query_preview": "host:shared.example.com",
            "owned": False,
            "shared_by": "other_user",
        },
    ]


@pytest.fixture
def sample_alert_results() -> list[dict]:
    """Return sample alert results."""
    return [
        {
            "uuid": "result-001",
            "host": "matched.example.com",
            "A": "10.0.0.1",
            "alerting_timestamp": "2025-01-01T10:00:00Z",
        },
        {
            "uuid": "result-002",
            "host": "matched2.example.com",
            "A": "10.0.0.2",
            "alerting_timestamp": "2025-01-01T11:00:00Z",
        },
    ]


@pytest.fixture
def mock_query_response(sample_dns_records: list[dict]) -> dict:
    """Return a mock API query response."""
    return {
        "data": sample_dns_records,
        "total": len(sample_dns_records),
        "has_more": False,
        "pit_id": "test-pit-id",
        "search_after": None,
    }


@pytest.fixture
def mock_query_response_paginated(sample_dns_records: list[dict]) -> list[dict]:
    """Return mock paginated API query responses (2 pages)."""
    return [
        {
            "data": sample_dns_records[:2],
            "total": len(sample_dns_records),
            "has_more": True,
            "pit_id": "test-pit-id",
            "search_after": ["2025-01-01T11:00:00Z", "record-uuid-002"],
        },
        {
            "data": sample_dns_records[2:],
            "total": len(sample_dns_records),
            "has_more": False,
            "pit_id": "test-pit-id",
            "search_after": None,
        },
    ]


@pytest.fixture
def mock_unified_alerts_response(sample_alerts: list[dict]) -> dict:
    """Return a mock unified alerts API response."""
    return {
        "data": sample_alerts,
        "recordsTotal": len(sample_alerts),
        "recordsFiltered": len(sample_alerts),
    }


def make_ndjson_response(records: list[dict]) -> str:
    """Helper to create NDJSON response body."""
    return "\n".join(json.dumps(r) for r in records)
