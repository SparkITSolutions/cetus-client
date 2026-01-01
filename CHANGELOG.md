# Changelog

All notable changes to cetus-client will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- PyPI package distribution
- Standalone executables for Windows, macOS, and Linux
- CLI documentation in `docs/CLI.md`
- GitHub Actions workflows for automated releases

## [1.0.0] - 2024-12-01

### Added
- Initial release
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

[Unreleased]: https://github.com/SparkITSolutions/cetus-client/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/SparkITSolutions/cetus-client/releases/tag/v1.0.0
