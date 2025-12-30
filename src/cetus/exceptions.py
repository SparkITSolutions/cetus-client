"""Custom exceptions for the Cetus CLI."""


class CetusError(Exception):
    """Base exception for all Cetus errors."""


class ConfigurationError(CetusError):
    """Raised when there's a configuration problem."""


class AuthenticationError(CetusError):
    """Raised when API authentication fails."""


class APIError(CetusError):
    """Raised when the API returns an error."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class ConnectionError(CetusError):
    """Raised when unable to connect to the API."""
