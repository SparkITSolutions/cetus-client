# Cetus Client

Command-line client for the Cetus threat intelligence alerting API.

## Installation

### From PyPI

```bash
pip install cetus-client
```

Or with pipx for isolated installation:

```bash
pipx install cetus-client
```

### Standalone Executables

Download pre-built binaries from [GitHub Releases](https://github.com/SparkITSolutions/cetus-client/releases):

| Platform | Download |
|----------|----------|
| Windows (x64) | `cetus-windows-amd64.exe` |
| macOS (Intel) | `cetus-macos-amd64` |
| macOS (Apple Silicon) | `cetus-macos-arm64` |
| Linux (x64) | `cetus-linux-amd64` |

### From Source

```bash
git clone https://github.com/SparkITSolutions/cetus-client.git
cd cetus-client
pip install -e .
```

## Quick Start

```bash
# Set your API key (one-time setup)
cetus config set api-key YOUR_API_KEY

# Query DNS records
cetus query "host:*.example.com"

# View as table
cetus query "A:192.168.1.1" --format table

# List your alerts
cetus alerts list
```

## Commands

### query

Execute a search query against the Cetus API.

```bash
cetus query SEARCH [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `-i, --index` | Index: `dns`, `certstream`, `alerting` (default: dns) |
| `-m, --media` | Storage tier: `nvme` (fast), `all` (complete) |
| `-f, --format` | Output: `json`, `jsonl`, `csv`, `table` |
| `-o, --output FILE` | Write to file instead of stdout |
| `-d, --since-days N` | Look back N days (default: 7) |
| `--stream` | Stream results as they arrive |
| `--no-marker` | Disable incremental query tracking |

**Examples:**

```bash
# Basic query
cetus query "host:*.example.com"

# Pipe to jq for processing
cetus query "host:*.example.com" | jq '.[].host'

# Table format for human reading
cetus query "A:10.0.0.1" --format table

# Save to file
cetus query "host:*.example.com" -o results.json

# Stream large results (uses jsonl format)
cetus query "host:*" --stream -o all_records.jsonl

# Query certificate transparency logs
cetus query "leaf_cert.subject.CN:*.example.com" --index certstream

# Look back 30 days
cetus query "host:example.com" --since-days 30
```

### Incremental Queries

The client tracks your queries using markers. First run fetches N days of data; subsequent runs fetch only new records.

```bash
# First run: fetches last 7 days
cetus query "host:*.example.com" -o results.jsonl

# Later runs: fetches only new data
cetus query "host:*.example.com" -o results.jsonl

# Skip markers for a full query
cetus query "host:*.example.com" --no-marker --since-days 30
```

Manage markers:

```bash
cetus markers list              # Show all markers
cetus markers clear             # Clear all markers
cetus markers clear --index dns # Clear only DNS markers
```

### alerts list

List alert definitions.

```bash
cetus alerts list [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--owned/--no-owned` | Include alerts you own (default: yes) |
| `--shared/--no-shared` | Include alerts shared with you |
| `-t, --type TYPE` | Filter: `raw`, `terms`, `structured` |

```bash
cetus alerts list                      # Your alerts
cetus alerts list --shared             # Include shared alerts
cetus alerts list --no-owned --shared  # Only shared alerts
cetus alerts list --type raw           # Only raw query alerts
```

### alerts results

Get results for an alert.

```bash
cetus alerts results ALERT_ID [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `-s, --since` | Only results since timestamp (ISO 8601) |
| `-f, --format` | Output format |
| `-o, --output` | Write to file |

```bash
cetus alerts results 123
cetus alerts results 123 --format table
cetus alerts results 123 --since 2025-01-01T00:00:00Z
cetus alerts results 123 -o results.csv
```

### alerts backtest

Test an alert against historical data.

```bash
cetus alerts backtest ALERT_ID [OPTIONS]
```

Fetches the alert's query and runs it against the full database. Useful for testing alert definitions before deployment.

```bash
cetus alerts backtest 123
cetus alerts backtest 123 --since-days 30
cetus alerts backtest 123 --stream -o backtest.jsonl
```

### config

Manage configuration.

```bash
cetus config show               # View current config
cetus config path               # Show config file location
cetus config set api-key KEY    # Set API key
cetus config set host HOST      # Set API host
cetus config set timeout 120    # Set timeout (seconds)
cetus config set since-days 14  # Set default lookback
```

## Configuration

**Priority (highest to lowest):**
1. CLI flags (`--api-key`, `--host`)
2. Environment variables
3. Config file

**Environment Variables:**

| Variable | Description |
|----------|-------------|
| `CETUS_API_KEY` | API authentication key |
| `CETUS_HOST` | API hostname |
| `CETUS_TIMEOUT` | Request timeout in seconds |
| `CETUS_SINCE_DAYS` | Default lookback period |

**Config File Location:**

| Platform | Path |
|----------|------|
| Linux | `~/.config/cetus/config.toml` |
| macOS | `~/Library/Application Support/cetus/config.toml` |
| Windows | `%APPDATA%\cetus\config.toml` |

## Query Syntax

Cetus uses Lucene query syntax:

| Query | Description |
|-------|-------------|
| `host:*.example.com` | Wildcard domain match |
| `host:example.com` | Exact domain match |
| `A:192.168.1.1` | DNS A record lookup |
| `AAAA:2001:db8::1` | IPv6 lookup |
| `CNAME:target.com` | CNAME record lookup |
| `host:example.com AND A:*` | Combined conditions |
| `host:(foo.com OR bar.com)` | Multiple values |
| `NOT host:internal.*` | Negation |

## Output Formats

| Format | Description |
|--------|-------------|
| `json` | JSON array (default) |
| `jsonl` | JSON Lines, one object per line |
| `csv` | Comma-separated values |
| `table` | Rich terminal table |

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Lint
ruff check src/

# Build standalone executable
pyinstaller cetus.spec
```

## License

MIT License - see [LICENSE](LICENSE) for details.
