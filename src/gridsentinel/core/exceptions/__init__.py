"""Safe public errors and explicit retry classifications."""


class GridSentinelError(Exception):
    """Base exception safe to return as an API error."""

    code = "INTERNAL_ERROR"
    status = 500
    retryable = False

    def __init__(self, message: str = "Operation failed") -> None:
        super().__init__(message)
        self.message = message


class ConfigurationError(GridSentinelError):
    """Invalid or missing runtime configuration."""

    code = "CONFIGURATION_ERROR"


class ValidationError(GridSentinelError):
    """Untrusted input does not satisfy the contract."""

    code = "VALIDATION_ERROR"
    status = 400


class AuthenticationError(GridSentinelError):
    """Authentication failed without revealing account existence."""

    code = "AUTHENTICATION_FAILED"
    status = 401


class AuthorizationError(GridSentinelError):
    """The principal lacks the required scope."""

    code = "FORBIDDEN"
    status = 403


class NotFoundError(GridSentinelError):
    """A tenant-scoped resource was not found."""

    code = "NOT_FOUND"
    status = 404


class ConflictError(GridSentinelError):
    """A state transition conflicts with durable state."""

    code = "CONFLICT"
    status = 409


class RateLimitError(GridSentinelError):
    """A bounded request budget was exhausted."""

    code = "RATE_LIMITED"
    status = 429


class InfrastructureError(GridSentinelError):
    """An external infrastructure dependency is unavailable."""

    code = "DEPENDENCY_UNAVAILABLE"
    status = 503
    retryable = True


class PoolTimeoutError(InfrastructureError):
    """No database connection became available before the deadline."""


def is_transient(error: Exception) -> bool:
    """Classify only known transport and database deadlock failures."""
    if isinstance(error, (InfrastructureError, TimeoutError, ConnectionError)):
        return True
    return bool(error.args and error.args[0] in (1205, 1213, 2006, 2013))
