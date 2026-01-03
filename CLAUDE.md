# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the CLI client for the Cetus threat intelligence alerting API. It queries DNS records, certificate transparency logs, and alert results from the Cetus platform at `alerting.sparkits.ca`.

Before running tests, ask the user for an API Key, and if provided, set the CETUS_E2E=1 and CETUS_API_KEY=<api key> environment variables so the E2E tests will run.  Otherwise if the user doesn't want to provide one, omit them and let the user know the E2E tests won't run.

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

### Related Repository

The server-side API is in `C:\code\SparkIT\Cetus\alerting_app` - a Django REST Framework application. See its CLAUDE.md for API details.
