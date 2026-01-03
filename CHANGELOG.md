# Changelog

All notable changes to cetus-client will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.1] - 2026-01-02

### Added
- Initial public release
- `query` command with support for dns, certstream, and alerting indices
- Streaming mode for large queries (`--stream`)
- Multiple output formats: JSON, JSONL, CSV, table
- Incremental query markers for efficient updates
- `config` commands for managing API key and settings
- `markers` commands for managing query markers
- `alerts list` command to view alert definitions
- `alerts results` command to fetch alert matches
- `alerts backtest` command to test alerts against historical data
- Cross-platform configuration via environment variables or config file
- Rich terminal output with colors and progress indicators
- SECURITY.md with vulnerability disclosure policy

### Security
- File permission enforcement (0o600) on config and marker files (Unix)
- HTTP restricted to localhost only; HTTPS required for remote hosts
- API key masking in logs (only first 4 characters shown)
- Rate limit handling with Retry-After header support
- Error message sanitization to prevent server info leakage
- Marker hash length increased to 128-bit for collision resistance
- Marker file size limits (10KB) to prevent memory exhaustion
- Explicit TLS certificate verification
- Input validation for index/media parameters
- User-Agent header for request identification
- Dependency version pinning to prevent unexpected breakage

[Unreleased]: https://github.com/SparkITSolutions/cetus-client/compare/v0.0.1...HEAD
[0.0.1]: https://github.com/SparkITSolutions/cetus-client/releases/tag/v0.0.1
