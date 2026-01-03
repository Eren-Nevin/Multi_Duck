"""Async read connection pool for DuckDB."""

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

import duckdb

from .exceptions import (
    ConnectionPoolClosedError,
    ConnectionPoolExhaustedError,
)

logger = logging.getLogger(__name__)


class ReadConnectionPool:
    """
    Pool of DuckDB connections for parallel read operations.

    This pool manages multiple connections to a DuckDB database,
    allowing concurrent read queries without blocking each other.
    Note: We use read_write connections because DuckDB doesn't allow
    mixing read_only and read_write connections on the same database.
    Write serialization is handled by the WriteQueue.
    """

    def __init__(
        self,
        db_path: Path,
        pool_size: int = 10,
        timeout: float = 30.0,
    ):
        """
        Initialize the read connection pool.

        Args:
            db_path: Path to the DuckDB database file.
            pool_size: Number of connections to maintain in the pool.
            timeout: Maximum time to wait for a connection (seconds).
        """
        self._db_path = db_path
        self._pool_size = pool_size
        self._timeout = timeout
        self._pool: asyncio.Queue[duckdb.DuckDBPyConnection] = asyncio.Queue(maxsize=pool_size)
        self._connections: list[duckdb.DuckDBPyConnection] = []
        self._closed = False
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        """Initialize the connection pool by creating all connections."""
        async with self._lock:
            if self._closed:
                raise ConnectionPoolClosedError()

            logger.info(f"Initializing read pool with {self._pool_size} connections")

            # Create connections in a thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            for i in range(self._pool_size):
                conn = await loop.run_in_executor(None, self._create_connection)
                self._connections.append(conn)
                await self._pool.put(conn)
                logger.debug(f"Created read connection {i + 1}/{self._pool_size}")

            logger.info("Read connection pool initialized successfully")

    def _create_connection(self) -> duckdb.DuckDBPyConnection:
        """Create a new DuckDB connection for read operations."""
        # Note: We don't use read_only=True because DuckDB doesn't allow
        # mixing read_only and read_write connections on the same database.
        # Write serialization is handled by the WriteQueue.
        return duckdb.connect(str(self._db_path))

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[duckdb.DuckDBPyConnection]:
        """
        Acquire a connection from the pool.

        This is an async context manager that automatically returns
        the connection to the pool when done.

        Raises:
            ConnectionPoolClosedError: If the pool is closed.
            ConnectionPoolExhaustedError: If no connection is available within timeout.

        Yields:
            A read-only DuckDB connection.
        """
        if self._closed:
            raise ConnectionPoolClosedError()

        try:
            conn = await asyncio.wait_for(self._pool.get(), timeout=self._timeout)
        except asyncio.TimeoutError:
            raise ConnectionPoolExhaustedError(self._timeout)

        try:
            yield conn
        finally:
            if not self._closed:
                await self._pool.put(conn)

    async def close(self) -> None:
        """Close all connections in the pool."""
        async with self._lock:
            if self._closed:
                return

            self._closed = True
            logger.info("Closing read connection pool")

            # Drain the queue
            while not self._pool.empty():
                try:
                    self._pool.get_nowait()
                except asyncio.QueueEmpty:
                    break

            # Close all connections
            loop = asyncio.get_event_loop()
            for conn in self._connections:
                try:
                    await loop.run_in_executor(None, conn.close)
                except Exception as e:
                    logger.warning(f"Error closing connection: {e}")

            self._connections.clear()
            logger.info("Read connection pool closed")

    @property
    def available(self) -> int:
        """Return the number of available connections in the pool."""
        return self._pool.qsize()

    @property
    def size(self) -> int:
        """Return the total size of the pool."""
        return self._pool_size

    @property
    def is_closed(self) -> bool:
        """Return whether the pool is closed."""
        return self._closed
