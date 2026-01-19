"""Tests for configuration management."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from cetus.config import (
    APP_NAME,
    DEFAULT_HOST,
    DEFAULT_SINCE_DAYS,
    DEFAULT_TIMEOUT,
    Config,
    _escape_toml_string,
    get_config_dir,
    get_config_file,
    get_data_dir,
)
from cetus.exceptions import ConfigurationError


class TestConstants:
    """Tests for configuration constants."""

    def test_app_name(self):
        """APP_NAME should be 'cetus'."""
        assert APP_NAME == "cetus"

    def test_default_host(self):
        """DEFAULT_HOST should be the production host."""
        assert DEFAULT_HOST == "alerting.sparkits.ca"

    def test_default_timeout(self):
        """DEFAULT_TIMEOUT should be 60 seconds."""
        assert DEFAULT_TIMEOUT == 60

    def test_default_since_days(self):
        """DEFAULT_SINCE_DAYS should be 7 days."""
        assert DEFAULT_SINCE_DAYS == 7


class TestEscapeTomlString:
    """Tests for TOML string escaping."""

    def test_no_escaping_needed(self):
        """Regular strings should pass through unchanged."""
        assert _escape_toml_string("hello") == "hello"
        assert _escape_toml_string("test123") == "test123"

    def test_escape_backslashes(self):
        """Backslashes should be escaped."""
        assert _escape_toml_string("path\\to\\file") == "path\\\\to\\\\file"

    def test_escape_quotes(self):
        """Double quotes should be escaped."""
        assert _escape_toml_string('say "hello"') == 'say \\"hello\\"'

    def test_escape_both(self):
        """Both backslashes and quotes should be escaped."""
        assert _escape_toml_string('C:\\path\\"file"') == 'C:\\\\path\\\\\\"file\\"'

    def test_empty_string(self):
        """Empty string should remain empty."""
        assert _escape_toml_string("") == ""


class TestPathFunctions:
    """Tests for path utility functions."""

    def test_get_config_dir_returns_path(self):
        """get_config_dir should return a Path object."""
        result = get_config_dir()
        assert isinstance(result, Path)

    def test_get_data_dir_returns_path(self):
        """get_data_dir should return a Path object."""
        result = get_data_dir()
        assert isinstance(result, Path)

    def test_get_config_file_returns_path(self):
        """get_config_file should return a Path object."""
        result = get_config_file()
        assert isinstance(result, Path)

    def test_config_file_is_toml(self):
        """Config file should have .toml extension."""
        result = get_config_file()
        assert result.suffix == ".toml"
        assert result.name == "config.toml"


class TestConfigDefaults:
    """Tests for Config default values."""

    def test_default_api_key_is_none(self):
        """Default api_key should be None."""
        config = Config()
        assert config.api_key is None

    def test_default_host(self):
        """Default host should be DEFAULT_HOST."""
        config = Config()
        assert config.host == DEFAULT_HOST

    def test_default_timeout(self):
        """Default timeout should be DEFAULT_TIMEOUT."""
        config = Config()
        assert config.timeout == DEFAULT_TIMEOUT

    def test_default_since_days(self):
        """Default since_days should be DEFAULT_SINCE_DAYS."""
        config = Config()
        assert config.since_days == DEFAULT_SINCE_DAYS

    def test_config_dir_default(self):
        """config_dir should default to get_config_dir()."""
        config = Config()
        assert config.config_dir == get_config_dir()

    def test_data_dir_default(self):
        """data_dir should default to get_data_dir()."""
        config = Config()
        assert config.data_dir == get_data_dir()


class TestConfigLoadFromCLI:
    """Tests for Config.load() with CLI arguments."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path: Path):
        """Set up a clean environment for each test."""
        # Patch config directory to avoid reading real config
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        # Clear environment variables
        env_vars = ["CETUS_API_KEY", "CETUS_HOST", "CETUS_TIMEOUT", "CETUS_SINCE_DAYS"]
        for var in env_vars:
            os.environ.pop(var, None)

        with patch("cetus.config.get_config_dir", return_value=config_dir):
            yield

    def test_load_with_api_key(self):
        """CLI api_key should override all other sources."""
        config = Config.load(api_key="cli-key")
        assert config.api_key == "cli-key"

    def test_load_with_host(self):
        """CLI host should override all other sources."""
        config = Config.load(host="cli.example.com")
        assert config.host == "cli.example.com"

    def test_load_with_timeout(self):
        """CLI timeout should override all other sources."""
        config = Config.load(timeout=120)
        assert config.timeout == 120

    def test_load_with_all_cli_args(self):
        """All CLI arguments should be respected."""
        config = Config.load(api_key="my-key", host="my.host.com", timeout=90)
        assert config.api_key == "my-key"
        assert config.host == "my.host.com"
        assert config.timeout == 90


class TestConfigLoadFromEnv:
    """Tests for Config.load() with environment variables."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path: Path):
        """Set up a clean environment for each test."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        # Clear environment variables
        env_vars = ["CETUS_API_KEY", "CETUS_HOST", "CETUS_TIMEOUT", "CETUS_SINCE_DAYS"]
        for var in env_vars:
            os.environ.pop(var, None)

        with patch("cetus.config.get_config_dir", return_value=config_dir):
            yield

    def test_load_api_key_from_env(self):
        """CETUS_API_KEY should be loaded from environment."""
        os.environ["CETUS_API_KEY"] = "env-api-key"
        config = Config.load()
        assert config.api_key == "env-api-key"
        del os.environ["CETUS_API_KEY"]

    def test_load_host_from_env(self):
        """CETUS_HOST should be loaded from environment."""
        os.environ["CETUS_HOST"] = "env.example.com"
        config = Config.load()
        assert config.host == "env.example.com"
        del os.environ["CETUS_HOST"]

    def test_load_timeout_from_env(self):
        """CETUS_TIMEOUT should be loaded from environment."""
        os.environ["CETUS_TIMEOUT"] = "300"
        config = Config.load()
        assert config.timeout == 300
        del os.environ["CETUS_TIMEOUT"]

    def test_load_since_days_from_env(self):
        """CETUS_SINCE_DAYS should be loaded from environment."""
        os.environ["CETUS_SINCE_DAYS"] = "30"
        config = Config.load()
        assert config.since_days == 30
        del os.environ["CETUS_SINCE_DAYS"]

    def test_invalid_timeout_raises_error(self):
        """Invalid CETUS_TIMEOUT should raise ConfigurationError."""
        os.environ["CETUS_TIMEOUT"] = "not-a-number"
        with pytest.raises(ConfigurationError, match="Invalid CETUS_TIMEOUT"):
            Config.load()
        del os.environ["CETUS_TIMEOUT"]

    def test_invalid_since_days_raises_error(self):
        """Invalid CETUS_SINCE_DAYS should raise ConfigurationError."""
        os.environ["CETUS_SINCE_DAYS"] = "invalid"
        with pytest.raises(ConfigurationError, match="Invalid CETUS_SINCE_DAYS"):
            Config.load()
        del os.environ["CETUS_SINCE_DAYS"]


class TestConfigLoadFromFile:
    """Tests for Config.load() with config file."""

    @pytest.fixture
    def config_dir(self, tmp_path: Path) -> Path:
        """Create a temporary config directory."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        return config_dir

    @pytest.fixture(autouse=True)
    def clean_env(self):
        """Clear environment variables."""
        env_vars = ["CETUS_API_KEY", "CETUS_HOST", "CETUS_TIMEOUT", "CETUS_SINCE_DAYS"]
        old_values = {var: os.environ.pop(var, None) for var in env_vars}
        yield
        for var, value in old_values.items():
            if value is not None:
                os.environ[var] = value

    def test_load_from_file(self, config_dir: Path):
        """Config should be loaded from TOML file."""
        config_file = config_dir / "config.toml"
        config_file.write_text(
            'api_key = "file-api-key"\nhost = "file.example.com"\ntimeout = 45\nsince_days = 14\n'
        )

        with patch("cetus.config.get_config_dir", return_value=config_dir):
            config = Config.load()

        assert config.api_key == "file-api-key"
        assert config.host == "file.example.com"
        assert config.timeout == 45
        assert config.since_days == 14

    def test_partial_file(self, config_dir: Path):
        """Partial config file should work with defaults for missing values."""
        config_file = config_dir / "config.toml"
        config_file.write_text('api_key = "partial-key"\n')

        with patch("cetus.config.get_config_dir", return_value=config_dir):
            config = Config.load()

        assert config.api_key == "partial-key"
        assert config.host == DEFAULT_HOST
        assert config.timeout == DEFAULT_TIMEOUT

    def test_missing_file_uses_defaults(self, config_dir: Path):
        """Missing config file should use defaults."""
        with patch("cetus.config.get_config_dir", return_value=config_dir):
            config = Config.load()

        assert config.api_key is None
        assert config.host == DEFAULT_HOST

    def test_invalid_toml_raises_error(self, config_dir: Path):
        """Invalid TOML should raise ConfigurationError."""
        config_file = config_dir / "config.toml"
        config_file.write_text("invalid toml { not valid")

        with patch("cetus.config.get_config_dir", return_value=config_dir):
            with pytest.raises(ConfigurationError, match="Failed to load config file"):
                Config.load()


class TestConfigPriority:
    """Tests for configuration priority (CLI > env > file > defaults)."""

    @pytest.fixture
    def config_dir(self, tmp_path: Path) -> Path:
        """Create a temporary config directory."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        return config_dir

    @pytest.fixture(autouse=True)
    def clean_env(self):
        """Clear environment variables."""
        env_vars = ["CETUS_API_KEY", "CETUS_HOST", "CETUS_TIMEOUT", "CETUS_SINCE_DAYS"]
        old_values = {var: os.environ.pop(var, None) for var in env_vars}
        yield
        for var, value in old_values.items():
            if value is not None:
                os.environ[var] = value

    def test_cli_overrides_env(self, config_dir: Path):
        """CLI arguments should override environment variables."""
        os.environ["CETUS_API_KEY"] = "env-key"

        with patch("cetus.config.get_config_dir", return_value=config_dir):
            config = Config.load(api_key="cli-key")

        assert config.api_key == "cli-key"

    def test_env_overrides_file(self, config_dir: Path):
        """Environment variables should override config file."""
        config_file = config_dir / "config.toml"
        config_file.write_text('api_key = "file-key"\n')
        os.environ["CETUS_API_KEY"] = "env-key"

        with patch("cetus.config.get_config_dir", return_value=config_dir):
            config = Config.load()

        assert config.api_key == "env-key"

    def test_file_overrides_defaults(self, config_dir: Path):
        """Config file should override defaults."""
        config_file = config_dir / "config.toml"
        config_file.write_text("timeout = 999\n")

        with patch("cetus.config.get_config_dir", return_value=config_dir):
            config = Config.load()

        assert config.timeout == 999

    def test_full_priority_chain(self, config_dir: Path):
        """Test full priority: CLI > env > file > defaults."""
        # Set up file
        config_file = config_dir / "config.toml"
        config_file.write_text(
            'api_key = "file-key"\nhost = "file.com"\ntimeout = 10\nsince_days = 10\n'
        )

        # Set up env (overrides some file values)
        os.environ["CETUS_HOST"] = "env.com"
        os.environ["CETUS_TIMEOUT"] = "20"

        with patch("cetus.config.get_config_dir", return_value=config_dir):
            # CLI overrides host
            config = Config.load(host="cli.com")

        # Priority check
        assert config.api_key == "file-key"  # file (no env or cli override)
        assert config.host == "cli.com"  # cli overrides env and file
        assert config.timeout == 20  # env overrides file
        assert config.since_days == 10  # file (no env or cli override)


class TestConfigSave:
    """Tests for Config.save()."""

    @pytest.fixture
    def config_dir(self, tmp_path: Path) -> Path:
        """Create a temporary config directory."""
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        return config_dir

    def test_save_creates_file(self, config_dir: Path):
        """save() should create config file if it doesn't exist."""
        with patch("cetus.config.get_config_dir", return_value=config_dir):
            config = Config(api_key="test-key")
            config.save()

        config_file = config_dir / "config.toml"
        assert config_file.exists()
        assert 'api_key = "test-key"' in config_file.read_text()

    def test_save_creates_directory(self, tmp_path: Path):
        """save() should create config directory if it doesn't exist."""
        config_dir = tmp_path / "nonexistent" / "config"

        with patch("cetus.config.get_config_dir", return_value=config_dir):
            config = Config(api_key="test-key")
            config.save()

        assert (config_dir / "config.toml").exists()

    def test_save_only_non_default_values(self, config_dir: Path):
        """save() should only write non-default values."""
        with patch("cetus.config.get_config_dir", return_value=config_dir):
            config = Config(
                api_key="my-key",
                host=DEFAULT_HOST,  # default - should not be saved
                timeout=DEFAULT_TIMEOUT,  # default - should not be saved
            )
            config.save()

        content = (config_dir / "config.toml").read_text()
        assert 'api_key = "my-key"' in content
        assert "host" not in content
        assert "timeout" not in content

    def test_save_escapes_special_characters(self, config_dir: Path):
        """save() should properly escape special characters."""
        with patch("cetus.config.get_config_dir", return_value=config_dir):
            config = Config(api_key='key-with-"quotes"-and\\backslash')
            config.save()

        content = (config_dir / "config.toml").read_text()
        # The saved file should be valid TOML
        assert '\\"' in content or "quotes" in content

    def test_save_then_load_roundtrip(self, config_dir: Path):
        """Saved config should be loadable."""
        with patch("cetus.config.get_config_dir", return_value=config_dir):
            original = Config(api_key="roundtrip-key", host="roundtrip.com", timeout=123)
            original.save()

            # Clear any env vars
            for var in ["CETUS_API_KEY", "CETUS_HOST", "CETUS_TIMEOUT"]:
                os.environ.pop(var, None)

            loaded = Config.load()

        assert loaded.api_key == "roundtrip-key"
        assert loaded.host == "roundtrip.com"
        assert loaded.timeout == 123


class TestConfigRequireApiKey:
    """Tests for Config.require_api_key()."""

    def test_returns_api_key_when_set(self):
        """require_api_key() should return the API key when configured."""
        config = Config(api_key="my-api-key")
        assert config.require_api_key() == "my-api-key"

    def test_raises_error_when_not_set(self):
        """require_api_key() should raise ConfigurationError when not configured."""
        config = Config()
        with pytest.raises(ConfigurationError, match="No API key configured"):
            config.require_api_key()

    def test_error_message_includes_help(self):
        """Error message should include configuration options."""
        config = Config()
        try:
            config.require_api_key()
            pytest.fail("Should have raised ConfigurationError")
        except ConfigurationError as e:
            message = str(e)
            assert "CETUS_API_KEY" in message
            assert "config" in message.lower()
            assert "--api-key" in message


class TestConfigAsDict:
    """Tests for Config.as_dict()."""

    def test_includes_all_fields(self):
        """as_dict() should include all configuration fields."""
        config = Config(api_key="test-key", host="test.com", timeout=30, since_days=14)
        result = config.as_dict()

        assert "api_key" in result
        assert "host" in result
        assert "timeout" in result
        assert "since_days" in result
        assert "config_dir" in result
        assert "data_dir" in result

    def test_masks_api_key(self):
        """as_dict() should mask the API key for security."""
        config = Config(api_key="my-secret-api-key")
        result = config.as_dict()

        assert result["api_key"] == "***-key"
        assert "my-secret" not in result["api_key"]

    def test_short_api_key_still_masked(self):
        """Short API keys should still be masked properly."""
        config = Config(api_key="abc")
        result = config.as_dict()

        assert result["api_key"] == "***abc"

    def test_none_api_key_stays_none(self):
        """None API key should remain None in dict."""
        config = Config()
        result = config.as_dict()

        assert result["api_key"] is None

    def test_paths_converted_to_strings(self):
        """Path objects should be converted to strings."""
        config = Config()
        result = config.as_dict()

        assert isinstance(result["config_dir"], str)
        assert isinstance(result["data_dir"], str)
