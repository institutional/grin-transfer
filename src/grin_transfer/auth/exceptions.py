"""
OAuth2 authentication exceptions for GRIN access
"""


class AuthError(Exception):
    """Base exception for authentication errors."""

    pass


class CredentialsMissingError(AuthError):
    """Raised when credentials are missing or invalid."""

    pass


class GRINPermissionError(AuthError):
    """Raised when GRIN denies access (403)."""

    pass
