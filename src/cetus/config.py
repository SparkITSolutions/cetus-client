"""Configuration management for Cetus CLI.

Configuration priority (highest to lowest):
1. CLI arguments
2. Environment variables
3. Config file (~/.config/cetus/config.toml or platform equivalent)
4. Defaults
"""

from __future__ import annotations

import os
import stat
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import platformdirs

from .exceptions import ConfigurationError

APP_NAME = "cetus"
DEFAULT_HOST = "alerting.sparkits.ca"
DEFAULT_TIMEOUT = 60
DEFAULT_SINCE_DAYS = 7


def _escape_toml_string(value: str) -> str:
    """Escape special characters for TOML basic string format."""
    # TOML requires escaping backslashes and double quotes in basic strings
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _set_secure_permissions(path: Path) -> None:
    """Set file permissions to owner read-write only (0o600).

    On Windows, this is a no-op as Windows uses ACLs, not Unix permissions.
    The config file is already protected by user directory permissions.
    """
    if sys.platform != "win32":
        path.chmod(stat.S_IRUSR | stat.S_IWUSR)  # 0o600


def get_config_dir() -> Path:
    """Get the platform-appropriate config directory."""
    return Path(platformdirs.user_config_dir(APP_NAME))


def get_data_dir() -> Path:
    """Get the platform-appropriate data directory (for markers).

    Respects CETUS_DATA_DIR environment variable for testing.
    """
    if env_dir := os.environ.get("CETUS_DATA_DIR"):
        return Path(env_dir)
    return Path(platformdirs.user_data_dir(APP_NAME))


def get_config_file() -> Path:
    """Get the path to the config file."""
    return get_config_dir() / "config.toml"


@dataclass
class Config:
    """Application configuration."""

    api_key: str | None = None
    host: str = DEFAULT_HOST
    timeout: int = DEFAULT_TIMEOUT
    since_days: int = DEFAULT_SINCE_DAYS

    # Resolved paths
    config_dir: Path = field(default_factory=get_config_dir)
    data_dir: Path = field(default_factory=get_data_dir)

    @classmethod
    def load(
        cls,
        api_key: str | None = None,
        host: str | None = None,
        timeout: int | None = None,
    ) -> Config:
        """Load configuration from all sources, respecting priority.

        Args:
            api_key: CLI-provided API key (highest priority)
            host: CLI-provided host
            timeout: CLI-provided timeout

        Returns:
            Merged configuration
        """
        config = cls()

        # Load from config file first (lowest priority of dynamic sources)
        config._load_from_file()

        # Override with environment variables
        config._load_from_env()

        # Override with CLI arguments (highest priority)
        if api_key is not None:
            config.api_key = api_key
        if host is not None:
            config.host = host
        if timeout is not None:
            config.timeout = timeout

        return config

    def _load_from_file(self) -> None:
        """Load configuration from TOML file if it exists."""
        config_file = get_config_file()
        if not config_file.exists():
            return

        try:
            # Use tomllib on Python 3.11+, tomli otherwise
            if sys.version_info >= (3, 11):
                import tomllib
            else:
                import tomli as tomllib

            with open(config_file, "rb") as f:
                data = tomllib.load(f)

            if "api_key" in data:
                self.api_key = data["api_key"]
            if "host" in data:
                self.host = data["host"]
            if "timeout" in data:
                self.timeout = int(data["timeout"])
            if "since_days" in data:
                self.since_days = int(data["since_days"])

        except Exception as e:
            raise ConfigurationError(f"Failed to load config file {config_file}: {e}") from e

    def _load_from_env(self) -> None:
        """Load configuration from environment variables."""
        if api_key := os.environ.get("CETUS_API_KEY"):
            self.api_key = api_key
        if host := os.environ.get("CETUS_HOST"):
            self.host = host
        if timeout := os.environ.get("CETUS_TIMEOUT"):
            try:
                self.timeout = int(timeout)
            except ValueError:
                raise ConfigurationError(f"Invalid CETUS_TIMEOUT value: {timeout}")
        if since_days := os.environ.get("CETUS_SINCE_DAYS"):
            try:
                self.since_days = int(since_days)
            except ValueError:
                raise ConfigurationError(f"Invalid CETUS_SINCE_DAYS value: {since_days}")

    def save(self) -> None:
        """Save current configuration to file.

        The config file is created with secure permissions (0o600 on Unix)
        to protect the API key from other users on the system.
        """
        config_file = get_config_file()
        config_file.parent.mkdir(parents=True, exist_ok=True)

        lines = []
        if self.api_key:
            lines.append(f'api_key = "{_escape_toml_string(self.api_key)}"')
        if self.host != DEFAULT_HOST:
            lines.append(f'host = "{_escape_toml_string(self.host)}"')
        if self.timeout != DEFAULT_TIMEOUT:
            lines.append(f"timeout = {self.timeout}")
        if self.since_days != DEFAULT_SINCE_DAYS:
            lines.append(f"since_days = {self.since_days}")

        config_file.write_text("\n".join(lines) + "\n" if lines else "")
        _set_secure_permissions(config_file)

    def require_api_key(self) -> str:
        """Get the API key, raising an error if not configured."""
        if not self.api_key:
            raise ConfigurationError(
                "No API key configured. Set it via:\n"
                "  - Environment variable: CETUS_API_KEY\n"
                "  - Config file: cetus config set api-key <key>\n"
                "  - CLI flag: --api-key <key>"
            )
        return self.api_key

    def as_dict(self) -> dict[str, Any]:
        """Return configuration as a dictionary for display."""
        return {
            "api_key": "***" + self.api_key[-4:] if self.api_key else None,
            "host": self.host,
            "timeout": self.timeout,
            "since_days": self.since_days,
            "config_dir": str(self.config_dir),
            "data_dir": str(self.data_dir),
        }
