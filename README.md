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

## Operating Modes

Cetus has two primary operating modes designed for different use cases:

### Direct Mode (stdout)

**For:** Interactive exploration, piping to other tools, one-off queries

Direct mode outputs results to stdout with no state tracking. Each query is independent - you get exactly what you ask for, nothing more.

```bash
# Interactive exploration
cetus query "host:*.example.com" --format table

# Pipe to jq for processing
cetus query "host:*.example.com" | jq '.[].host'

# Chain with other tools
cetus query "A:192.168.1.*" | jq -r '.[].host' | sort -u
```

**Characteristics:**
- Results go to stdout (terminal or pipe)
- No markers - queries are stateless
- Full query results returned every time
- Default format: `json`

### Collector Mode (file output)

**For:** Data collection, scheduled exports, building datasets over time

Collector mode writes to files and tracks your position using markers. Subsequent runs fetch only new records since the last query, making it efficient for ongoing data collection.

```bash
# First run: fetches last 7 days, creates file
cetus query "host:*.example.com" -o results.jsonl
# Output: Wrote 1,523 records to results.jsonl

# Later runs: fetches only NEW records, appends to file
cetus query "host:*.example.com" -o results.jsonl
# Output: Resuming from: 2025-01-14T10:30:00
#         Appended 47 records to results.jsonl

# No new data? File unchanged
cetus query "host:*.example.com" -o results.jsonl
# Output: Resuming from: 2025-01-14T15:42:18
#         No new records (file unchanged)
```

**Characteristics:**
- Results written to file (`-o` or `-p`)
- Markers track last-seen record per query
- Incremental updates - only fetches new data
- Appends to existing files (or creates timestamped files with `-p`)
- Default format: `json` (recommended: `jsonl`)

**Two file output options:**

| Option | Behavior | Use Case |
|--------|----------|----------|
| `-o FILE` | Appends to same file | Cumulative dataset |
| `-p PREFIX` | Creates timestamped files | Export pipelines, archival |

**Important:** `-o` and `-p` maintain separate markers. You can use both modes
for the same query without data gaps - each tracks its own position independently.

```bash
# -o: Single cumulative file
cetus query "host:*.example.com" -o dns_data.jsonl
# Always writes to: dns_data.jsonl

# -p: Timestamped files per run
cetus query "host:*.example.com" -p exports/dns
# Creates: exports/dns_2025-01-14_10-30-00.jsonl
# Next run: exports/dns_2025-01-14_14-45-00.jsonl
```

**Switching modes:** Use `--no-marker` to run a collector-mode query without markers (full re-query, overwrites file):

```bash
cetus query "host:*.example.com" --no-marker --since-days 30 -o full_export.jsonl
```

---

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
| `-o, --output FILE` | Collector mode: write to file (enables markers) |
| `-p, --output-prefix PREFIX` | Collector mode: timestamped files (e.g., `prefix_2025-01-14_10-30-00.jsonl`) |
| `-d, --since-days N` | Look back N days (default: 7, ignored if marker exists) |
| `--stream` | Stream results as they arrive (large queries) |
| `--no-marker` | Disable incremental tracking (full re-query) |

**Examples:**

```bash
# Direct mode - interactive queries
cetus query "host:*.example.com"                    # JSON to stdout
cetus query "host:*.example.com" --format table     # Human-readable
cetus query "host:*.example.com" | jq '.[].host'    # Pipe to tools

# Collector mode - data collection
cetus query "host:*.example.com" -o results.jsonl   # Incremental collection
cetus query "host:*.example.com" -p exports/dns     # Timestamped exports

# Stream large results
cetus query "host:*" --stream -o all_records.jsonl

# Query other indices
cetus query "leaf_cert.subject.CN:*.example.com" --index certstream
cetus query "alert_type:dns_match" --index alerting

# Full re-query (ignore markers)
cetus query "host:*.example.com" --no-marker --since-days 30 -o full.jsonl
```

### Collector Mode Details

**Markers** track your position so subsequent queries fetch only new records:

```bash
cetus markers list              # Show all markers
cetus markers clear             # Clear all markers
cetus markers clear --index dns # Clear only DNS markers
```

**Console feedback** shows what's happening:

```
# Starting incremental query with existing marker:
Resuming from: 2025-01-14T10:30:00
Fetched 1,523 records (page 2)...
Appended 47 records to results.jsonl in 2.34s

# No new records (file exists):
Resuming from: 2025-01-14T15:42:18
No new records (file unchanged) in 0.45s

# No new records (first run, no data in time range):
No new records since last query (no file written) in 0.38s
```

**Recommended format:** `jsonl` (JSON Lines)
- Efficient append operations
- Easy to process: `wc -l`, `grep`, `jq -s`
- No rewriting of existing data

Other formats:
- `csv`: Appends rows without repeating header
- `json`: Merges into existing array (requires rewriting file)
- `table`: Not recommended for file output

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

## Security

### Credential Storage

Your API key is stored in a local configuration file:

| Platform | Location |
|----------|----------|
| Linux | `~/.config/cetus/config.toml` |
| macOS | `~/Library/Application Support/cetus/config.toml` |
| Windows | `%APPDATA%\cetus\config.toml` |

On Unix systems, the file is created with `0o600` permissions (owner read-write only).

**Alternatively**, use an environment variable to avoid storing credentials on disk:

```bash
export CETUS_API_KEY="your-key-here"
cetus query "host:*.example.com"
```

### Network Security

- All remote connections use **HTTPS with TLS verification**
- HTTP is only allowed for `localhost` (development use)
- Server errors are sanitized to prevent information leakage

### Local Data

Query markers (for incremental updates) are stored in:
- Linux: `~/.local/share/cetus/markers/`
- macOS: `~/Library/Application Support/cetus/markers/`
- Windows: `%LOCALAPPDATA%\cetus\markers/`

See [SECURITY.md](SECURITY.md) for the full security policy and vulnerability reporting.

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run security tests
pytest tests/test_security.py -v

# Lint
ruff check src/

# Build standalone executable
pyinstaller cetus.spec
```

## License

MIT License - see [LICENSE](LICENSE) for details.
