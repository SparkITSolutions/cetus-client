# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the CLI client for the Cetus threat intelligence alerting API. It queries DNS records, certificate transparency logs, and alert results from the Cetus platform at `alerting.sparkits.ca`.

Before running tests, ask the user for an API Key, and if provided, set the CETUS_E2E_TEST=1 and CETUS_API_KEY=<api key> environment variables so the E2E tests will run.  Otherwise if the user doesn't want to provide one, omit them and let the user know the E2E tests won't run.

## Development Commands

```bash
# Create and activate virtual environment
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/macOS

# Install in development mode
pip install -e .

# Install with dev dependencies
pip install -e ".[dev]"

# Run the CLI
cetus --help
cetus query "host:*.example.com"
cetus alerts list

# Lint
ruff check src/

# Run tests
pytest
```

## Editable Installs

When installed with `pip install -e .` (editable mode), the `cetus` command runs directly from source code in `src/cetus/`. Any changes to the source files are immediately reflected without reinstalling.

**How it works:**
- The installed package contains a `.pth` file pointing to the source directory
- Python imports modules directly from `src/cetus/` at runtime
- Entry point scripts (like `cetus.exe`) invoke `cetus.cli:main` from source

**When to use:**
- Development and testing - changes are instant
- Debugging - breakpoints and print statements work immediately

**Note:** The venv at the repo root (`alerting_app/.venv`) is shared with the Django app. Install cetus-client from the repo root:
```bash
pip install -e "./cetus-client[dev]"
```

## Version Management

Version is defined in **one place only**: `pyproject.toml`

The `src/cetus/__init__.py` uses `importlib.metadata.version()` to read it at runtime:
```python
from importlib.metadata import version
__version__ = version("cetus-client")
```

**When bumping version:** Only update `version = "X.Y.Z"` in `pyproject.toml`

**In tests:** Never hardcode version strings. Import from the package:
```python
from cetus import __version__
assert f"cetus-client/{__version__}" in user_agent
```

## Architecture

The CLI is built with Click and uses httpx for HTTP requests. All source code is in `src/cetus/`.

### Module Structure

- **cli.py** - Click command definitions. Entry point is `main()`. Commands: `query`, `config`, `markers`, `alerts`
- **client.py** - `CetusClient` class handles all API interactions. Methods: `query()`, `list_alerts()`, `get_alert_results()`
- **config.py** - Configuration management with priority: CLI flags > env vars (`CETUS_API_KEY`, `CETUS_HOST`) > config file > defaults
- **markers.py** - `MarkerStore` for tracking incremental query positions. Stores markers in XDG data directory
- **formatters.py** - Output formatters: `JSONFormatter`, `JSONLinesFormatter`, `CSVFormatter`, `TableFormatter`
- **exceptions.py** - Custom exceptions: `CetusError`, `ConfigurationError`, `AuthenticationError`, `APIError`, `ConnectionError`

### Key Behaviors

- **Default output is stdout** (not file). Use `-o FILE` to write to file
- **Markers only apply in file mode** - stdout queries don't read or save markers
- **API endpoints**:
  - `/api/query/` - Search DNS/certstream/alerting indices
  - `/alerts/api/unified/` - List alert definitions
  - `/api/alert_results/<id>` - Get results for an alert

### Marker System (Incremental Queries)

The marker system enables incremental queries - fetching only new records since the last query.

**How it works:**
1. After each query, the client saves a marker with the **newest** timestamp and UUID seen
2. On subsequent queries, the time filter uses `timestamp > marker_timestamp` (strictly greater than)
3. Only records newer than the marker are returned - no duplicates

**Key implementation details:**
- **Tracks max timestamp:** The client scans all returned records to find the maximum timestamp, regardless of server sort order. This is defensive - works even if server sort changes.
- **Uses strictly greater than (`>`):** The time filter excludes the marker timestamp entirely. With microsecond precision (6 decimal places), duplicate timestamps are extremely rare.
- **No skip logic needed:** Because the server filters with `>`, the marker record itself is never returned. No client-side deduplication required.

**Marker storage:**
- Location: `~/.local/share/cetus/markers/` (Linux) or `%APPDATA%\cetus\markers\` (Windows)
- Filename: `{index}_{query_hash}.json`
- Contents: `query`, `index`, `last_timestamp`, `last_uuid`, `updated_at`

**Rolling back a marker:**
To re-fetch previously seen data, manually edit the marker file to an older `last_timestamp`. The next query will return all records newer than that timestamp.

**E2E tests:**
- `test_query_with_marker_returns_only_newer_records` - Verifies records are strictly newer than marker
- `test_query_with_rolled_back_marker_returns_previously_seen_record` - Verifies rollback works

### Related Repository

The server-side API is in `C:\code\SparkIT\Cetus\alerting_app` - a Django REST Framework application. See its CLAUDE.md for API details.
