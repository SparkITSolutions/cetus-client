# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.0.x   | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability in cetus-client, please report it responsibly:

1. **Do NOT** create a public GitHub issue for security vulnerabilities
2. Email security concerns to: **security@sparkits.ca**
3. Include:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Any suggested fixes (optional)

We will respond within 48 hours and work with you to understand and address the issue.

## Security Measures

### Credential Protection

- **API keys** are stored in a configuration file with restricted permissions (0o600 on Unix)
- API keys are **never logged** - only the first 4 characters appear in debug output
- HTTPS is **required** for all remote connections (HTTP only allowed for localhost development)

### Local Data Storage

The client stores data in platform-specific directories:

| Platform | Config Location | Data Location |
|----------|-----------------|---------------|
| Linux | `~/.config/cetus/` | `~/.local/share/cetus/` |
| macOS | `~/Library/Application Support/cetus/` | Same |
| Windows | `%APPDATA%\cetus\` | `%LOCALAPPDATA%\cetus\` |

**Files stored locally:**
- `config.toml` - API key and settings (protected with 0o600 permissions)
- `markers/*.json` - Query position markers (protected with 0o600 permissions)

### Network Security

- All remote API connections use HTTPS with TLS verification
- HTTP is only permitted for `localhost`, `127.0.0.1`, or `::1` (development use)
- Rate limiting is respected with automatic retry using `Retry-After` headers
- Server error details are logged but not exposed to users (prevents information leakage)

### Input Validation

- Query parameters (`index`, `media`) are validated before requests
- Marker files are size-limited (10KB max) to prevent memory exhaustion
- Marker file hashes use 128-bit identifiers to minimize collision risk

## Security Best Practices

### For Users

1. **Protect your API key**
   ```bash
   # Use environment variable instead of config file
   export CETUS_API_KEY="your-key-here"
   ```

2. **Verify file permissions** (Unix/macOS)
   ```bash
   ls -la ~/.config/cetus/config.toml
   # Should show: -rw------- (600)
   ```

3. **Don't commit credentials**
   - Never commit `.env` files or config files containing API keys
   - Add to `.gitignore`: `config.toml`, `.env`

4. **Use HTTPS in production**
   - Never use `--host http://...` with remote servers
   - HTTP is only safe for local development

### For Developers

1. **Run security tests**
   ```bash
   pytest tests/test_security.py -v
   ```

2. **Check dependencies**
   ```bash
   pip-audit
   ```

3. **Review before release**
   - Check for hardcoded credentials
   - Verify error messages don't leak sensitive info
   - Ensure file permissions are enforced

## Changelog

### Security Fixes in 0.0.1

- Added file permission enforcement (0o600) on config and marker files
- Restricted HTTP to localhost only
- Added API key masking in logs
- Added rate limit handling with Retry-After support
- Sanitized error messages to prevent server info leakage
- Increased marker hash length from 64-bit to 128-bit
- Added marker file size limits (10KB)
- Added explicit TLS verification
- Added parameter validation for index/media values
