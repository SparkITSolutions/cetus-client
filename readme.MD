# Cetus Client

Command-line client for the Cetus threat intelligence alerting API.

## Installation

```bash
# From source
pip install -e .

# Or install dependencies manually
pip install click httpx rich platformdirs
```

## Quick Start

```bash
# Set your API key (one-time setup)
export CETUS_API_KEY="your-api-key"
# Or use: cetus config set api-key your-api-key

# Query DNS records
cetus query "host:*.example.com"

# Query with specific options
cetus query "A:192.168.1.1" --index dns --format table
```

## Usage

### Query Command

```bash
cetus query SEARCH [OPTIONS]
```

**Arguments:**
- `SEARCH` - Lucene query string

**Options:**
- `-i, --index` - Index to search: `dns`, `certstream`, `alerting` (default: dns)
- `-m, --media` - Storage tier: `nvme` (fast), `all` (complete) (default: nvme)
- `-f, --format` - Output format: `json`, `jsonl`, `csv`, `table` (default: json)
- `-o, --output FILE` - Write to file instead of stdout
- `-d, --since-days N` - Look back N days (default: 7)
- `--no-marker` - Disable incremental query tracking
- `--api-key KEY` - API key (overrides env/config)
- `--host HOST` - API host (default: alerting.sparkits.ca)

**Examples:**

```bash
# Basic query (outputs JSON to stdout)
cetus query "host:*.bloomberg.com"

# Pipe to jq for processing
cetus query "host:*.example.com" | jq '.[].host'

# Table format for human reading
cetus query "A:10.0.0.1" --format table

# Save to file
cetus query "host:*.example.com" --output results.json

# JSON Lines format (good for streaming/large datasets)
cetus query "host:*" --format jsonl > results.jsonl

# Query certificate transparency logs
cetus query "leaf_cert.subject.CN:*.example.com" --index certstream

# Look back 30 days instead of default 7
cetus query "host:example.com" --since-days 30
```

### Incremental Queries

The client automatically tracks your queries using markers. On first run, it fetches data from the last N days. On subsequent runs with the same query, it fetches only new data since your last query.

```bash
# First run: fetches last 7 days of data
cetus query "host:*.example.com"

# Second run: fetches only new data since first run
cetus query "host:*.example.com"

# Disable markers for a one-off full query
cetus query "host:*.example.com" --no-marker --since-days 30
```

Manage markers:
```bash
cetus markers list              # Show all markers
cetus markers clear             # Clear all markers
cetus markers clear --index dns # Clear only DNS markers
```

### Alerts

View and manage alert definitions you've created or that have been shared with you.

#### List Alerts

```bash
cetus alerts list [OPTIONS]
```

**Options:**
- `--owned/--no-owned` - Include alerts you own (default: yes)
- `--shared/--no-shared` - Include alerts shared with you (default: no)
- `-t, --type TYPE` - Filter by type: `raw`, `terms`, `structured`

**Examples:**

```bash
# List your alerts
cetus alerts list

# Include alerts shared with you
cetus alerts list --shared

# Only alerts shared with you
cetus alerts list --no-owned --shared

# Filter by alert type
cetus alerts list --type raw
cetus alerts list --type terms
cetus alerts list --type structured
```

#### Get Alert Results

```bash
cetus alerts results ALERT_ID [OPTIONS]
```

**Arguments:**
- `ALERT_ID` - The numeric ID of the alert (from `cetus alerts list`)

**Options:**
- `-s, --since TIMESTAMP` - Only results since this time (ISO 8601 format)
- `-f, --format FORMAT` - Output format: `json`, `jsonl`, `csv`, `table`
- `-o, --output FILE` - Write to file instead of stdout

**Examples:**

```bash
# Get all results for alert ID 123
cetus alerts results 123

# Table format for quick review
cetus alerts results 123 --format table

# Results since a specific time
cetus alerts results 123 --since 2025-01-01T00:00:00Z

# Save to file
cetus alerts results 123 -o alert_results.json
```

### Configuration

Configuration priority (highest to lowest):
1. CLI flags (`--api-key`, `--host`)
2. Environment variables (`CETUS_API_KEY`, `CETUS_HOST`)
3. Config file (`~/.config/cetus/config.toml`)

```bash
# View current config
cetus config show

# Set values
cetus config set api-key YOUR_API_KEY
cetus config set host custom.hostname.com
cetus config set timeout 900
cetus config set since-days 14

# Show config file location
cetus config path
```

**Environment Variables:**
- `CETUS_API_KEY` - API authentication key
- `CETUS_HOST` - API hostname
- `CETUS_TIMEOUT` - Request timeout in seconds
- `CETUS_SINCE_DAYS` - Default lookback period

## Query Syntax

Cetus uses Lucene query syntax. Common patterns:

| Query | Description |
|-------|-------------|
| `host:*.example.com` | Wildcard domain match |
| `host:example.com` | Exact domain match |
| `A:192.168.1.1` | DNS A record lookup |
| `AAAA:2001:db8::1` | IPv6 lookup |
| `CNAME:target.com` | CNAME record lookup |
| `host:example.com AND A:*` | Combined conditions |
| `host:(foo.com OR bar.com)` | Multiple values |

## Output Formats

- **json** (default) - Pretty-printed JSON array, good for files
- **jsonl** - JSON Lines, one object per line, good for streaming/piping
- **csv** - CSV format, good for spreadsheets
- **table** - Rich terminal table, good for quick human review

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Lint
ruff check src/
```
