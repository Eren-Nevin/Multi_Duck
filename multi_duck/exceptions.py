"""Custom exceptions for Multi_Duck."""


class MultiDuckError(Exception):
    """Base exception for Multi_Duck errors."""

    def __init__(self, message: str, error_type: str = "MultiDuckError"):
        self.message = message
        self.error_type = error_type
        super().__init__(message)


class ConnectionPoolError(MultiDuckError):
    """Error related to the connection pool."""

    def __init__(self, message: str):
        super().__init__(message, "ConnectionPoolError")


class ConnectionPoolExhaustedError(ConnectionPoolError):
    """Raised when no connections are available in the pool."""

    def __init__(self, timeout: float):
        super().__init__(f"No connections available after waiting {timeout}s")


class ConnectionPoolClosedError(ConnectionPoolError):
    """Raised when trying to use a closed connection pool."""

    def __init__(self):
        super().__init__("Connection pool is closed")


class WriteQueueError(MultiDuckError):
    """Error related to the write queue."""

    def __init__(self, message: str):
        super().__init__(message, "WriteQueueError")


class WriteQueueClosedError(WriteQueueError):
    """Raised when trying to use a closed write queue."""

    def __init__(self):
        super().__init__("Write queue is closed")


class QueryError(MultiDuckError):
    """Error executing a query."""

    def __init__(self, message: str, original_error: Exception | None = None):
        self.original_error = original_error
        error_type = type(original_error).__name__ if original_error else "QueryError"
        super().__init__(message, error_type)


class QueryParseError(MultiDuckError):
    """Error parsing a SQL query."""

    def __init__(self, message: str):
        super().__init__(message, "QueryParseError")


class CompactionError(MultiDuckError):
    """Error during database compaction."""

    def __init__(self, message: str):
        super().__init__(message, "CompactionError")
