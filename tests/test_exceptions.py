"""Tests for custom exceptions."""

import pytest

from cetus.exceptions import (
    APIError,
    AuthenticationError,
    CetusError,
    ConfigurationError,
    ConnectionError,
)


class TestCetusError:
    """Tests for the base CetusError exception."""

    def test_inherits_from_exception(self):
        """CetusError should inherit from Exception."""
        assert issubclass(CetusError, Exception)

    def test_can_be_raised_with_message(self):
        """CetusError can be raised with a message."""
        with pytest.raises(CetusError, match="test message"):
            raise CetusError("test message")

    def test_message_accessible_via_args(self):
        """Exception message should be accessible via args."""
        error = CetusError("test message")
        assert error.args == ("test message",)

    def test_str_representation(self):
        """Exception should have proper string representation."""
        error = CetusError("test message")
        assert str(error) == "test message"


class TestConfigurationError:
    """Tests for ConfigurationError."""

    def test_inherits_from_cetus_error(self):
        """ConfigurationError should inherit from CetusError."""
        assert issubclass(ConfigurationError, CetusError)

    def test_can_be_raised(self):
        """ConfigurationError can be raised with a message."""
        with pytest.raises(ConfigurationError, match="config problem"):
            raise ConfigurationError("config problem")

    def test_caught_by_cetus_error_handler(self):
        """ConfigurationError should be caught by CetusError handler."""
        try:
            raise ConfigurationError("test")
        except CetusError as e:
            assert isinstance(e, ConfigurationError)


class TestAuthenticationError:
    """Tests for AuthenticationError."""

    def test_inherits_from_cetus_error(self):
        """AuthenticationError should inherit from CetusError."""
        assert issubclass(AuthenticationError, CetusError)

    def test_can_be_raised(self):
        """AuthenticationError can be raised with a message."""
        with pytest.raises(AuthenticationError, match="invalid key"):
            raise AuthenticationError("invalid key")

    def test_caught_by_cetus_error_handler(self):
        """AuthenticationError should be caught by CetusError handler."""
        try:
            raise AuthenticationError("test")
        except CetusError as e:
            assert isinstance(e, AuthenticationError)


class TestAPIError:
    """Tests for APIError with status code tracking."""

    def test_inherits_from_cetus_error(self):
        """APIError should inherit from CetusError."""
        assert issubclass(APIError, CetusError)

    def test_can_be_raised_with_message_only(self):
        """APIError can be raised with just a message."""
        error = APIError("API failed")
        assert str(error) == "API failed"
        assert error.status_code is None

    def test_can_be_raised_with_status_code(self):
        """APIError can store a status code."""
        error = APIError("Not Found", status_code=404)
        assert str(error) == "Not Found"
        assert error.status_code == 404

    def test_various_status_codes(self):
        """APIError should handle various HTTP status codes."""
        for code in [400, 401, 403, 404, 500, 502, 503]:
            error = APIError(f"Error {code}", status_code=code)
            assert error.status_code == code

    def test_caught_by_cetus_error_handler(self):
        """APIError should be caught by CetusError handler."""
        try:
            raise APIError("test", status_code=500)
        except CetusError as e:
            assert isinstance(e, APIError)
            assert e.status_code == 500


class TestConnectionError:
    """Tests for ConnectionError."""

    def test_inherits_from_cetus_error(self):
        """ConnectionError should inherit from CetusError."""
        assert issubclass(ConnectionError, CetusError)

    def test_can_be_raised(self):
        """ConnectionError can be raised with a message."""
        with pytest.raises(ConnectionError, match="connection failed"):
            raise ConnectionError("connection failed")

    def test_caught_by_cetus_error_handler(self):
        """ConnectionError should be caught by CetusError handler."""
        try:
            raise ConnectionError("test")
        except CetusError as e:
            assert isinstance(e, ConnectionError)

    def test_does_not_conflict_with_builtin(self):
        """Our ConnectionError should be distinct from builtins.ConnectionError."""
        # Import to verify they're different classes
        import builtins

        assert ConnectionError is not builtins.ConnectionError
        assert issubclass(ConnectionError, CetusError)
        assert not issubclass(ConnectionError, builtins.ConnectionError)


class TestExceptionHierarchy:
    """Tests for the exception hierarchy as a whole."""

    def test_all_exceptions_inherit_from_cetus_error(self):
        """All custom exceptions should inherit from CetusError."""
        exceptions = [
            ConfigurationError,
            AuthenticationError,
            APIError,
            ConnectionError,
        ]
        for exc in exceptions:
            assert issubclass(exc, CetusError)

    def test_exceptions_can_be_caught_individually(self):
        """Each exception type can be caught individually."""
        exception_map = {
            ConfigurationError: "config",
            AuthenticationError: "auth",
            APIError: "api",
            ConnectionError: "connection",
        }

        for exc_class, msg in exception_map.items():
            try:
                if exc_class == APIError:
                    raise exc_class(msg, status_code=400)
                else:
                    raise exc_class(msg)
            except exc_class as e:
                assert msg in str(e)
