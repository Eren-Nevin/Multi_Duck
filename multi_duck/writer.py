"""Write queue for serialized write operations to DuckDB."""

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from .exceptions import QueryError, WriteQueueClosedError

logger = logging.getLogger(__name__)


@dataclass
class WriteRequest:
    """A write request to be processed by the writer."""

    sql: str
    params: dict[str, Any] | None
    future: asyncio.Future[dict[str, Any]]


class WriteQueue:
    """
    Queue for serializing write operations to DuckDB.

    This class manages a single write connection and processes write
    operations sequentially from a queue. This ensures that only one
    write operation happens at a time, preventing conflicts.
    """

    def __init__(self, db_path: Path):
        """
        Initialize the write queue.

        Args:
            db_path: Path to the DuckDB database file.
        """
        self._db_path = db_path
        self._queue: asyncio.Queue[WriteRequest] = asyncio.Queue()
        self._connection: duckdb.DuckDBPyConnection | None = None
        self._processor_task: asyncio.Task | None = None
        self._closed = False
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        """Initialize the write queue and start the background processor."""
        async with self._lock:
            if self._closed:
                raise WriteQueueClosedError()

            logger.info("Initializing write queue")

            # Create the write connection
            loop = asyncio.get_event_loop()
            self._connection = await loop.run_in_executor(None, self._create_connection)

            # Start the background processor
            self._processor_task = asyncio.create_task(self._process_queue())

            logger.info("Write queue initialized successfully")

    def _create_connection(self) -> duckdb.DuckDBPyConnection:
        """Create a new read-write DuckDB connection."""
        # Ensure parent directory exists
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(str(self._db_path), read_only=False)

    async def execute(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Queue a write operation and wait for its result.

        Args:
            sql: The SQL statement to execute.
            params: Optional parameters for the SQL statement.

        Returns:
            A dictionary with the result of the operation.

        Raises:
            WriteQueueClosedError: If the queue is closed.
            QueryError: If the query fails.
        """
        if self._closed:
            raise WriteQueueClosedError()

        # Create a future for the result
        loop = asyncio.get_event_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()

        # Queue the request
        request = WriteRequest(sql=sql, params=params, future=future)
        await self._queue.put(request)

        # Wait for the result
        return await future

    async def _process_queue(self) -> None:
        """Background task that processes write requests from the queue."""
        logger.info("Write queue processor started")

        while not self._closed:
            try:
                # Wait for a request with a timeout to allow checking for shutdown
                try:
                    request = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                # Process the request
                await self._execute_request(request)

            except asyncio.CancelledError:
                logger.info("Write queue processor cancelled")
                break
            except Exception as e:
                logger.error(f"Unexpected error in write queue processor: {e}")

        # Process remaining requests before shutdown
        await self._drain_queue()
        logger.info("Write queue processor stopped")

    async def _execute_request(self, request: WriteRequest) -> None:
        """Execute a single write request."""
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                self._execute_sql,
                request.sql,
                request.params,
            )
            request.future.set_result(result)
        except Exception as e:
            if not request.future.done():
                request.future.set_exception(QueryError(str(e), e))

    def _execute_sql(
        self,
        sql: str,
        params: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Execute SQL in the write connection (runs in thread pool)."""
        if self._connection is None:
            raise WriteQueueClosedError()

        try:
            if params:
                result = self._connection.execute(sql, params)
            else:
                result = self._connection.execute(sql)

            # Get affected rows if available
            rows_affected = -1
            try:
                # For DML statements, fetchall() returns the affected rows info
                # For DDL, it may return nothing
                fetch_result = result.fetchall()
                if fetch_result and len(fetch_result) > 0:
                    # Some queries return count
                    if isinstance(fetch_result[0][0], int):
                        rows_affected = fetch_result[0][0]
            except Exception:
                # Not all statements return rows
                pass

            # Try to get rowcount from description
            if rows_affected == -1:
                try:
                    rows_affected = result.rowcount if hasattr(result, 'rowcount') else -1
                except Exception:
                    pass

            return {
                "success": True,
                "rows_affected": rows_affected,
                "message": "Query executed successfully",
            }

        except duckdb.Error as e:
            raise QueryError(str(e), e)

    async def _drain_queue(self) -> None:
        """Process remaining requests in the queue during shutdown."""
        while not self._queue.empty():
            try:
                request = self._queue.get_nowait()
                await self._execute_request(request)
            except asyncio.QueueEmpty:
                break
            except Exception as e:
                logger.error(f"Error draining queue: {e}")

    async def close(self) -> None:
        """Close the write queue and its connection."""
        async with self._lock:
            if self._closed:
                return

            self._closed = True
            logger.info("Closing write queue")

            # Cancel the processor task
            if self._processor_task:
                self._processor_task.cancel()
                try:
                    await self._processor_task
                except asyncio.CancelledError:
                    pass

            # Close the connection
            if self._connection:
                loop = asyncio.get_event_loop()
                try:
                    await loop.run_in_executor(None, self._connection.close)
                except Exception as e:
                    logger.warning(f"Error closing write connection: {e}")

            logger.info("Write queue closed")

    @property
    def pending(self) -> int:
        """Return the number of pending write requests."""
        return self._queue.qsize()

    @property
    def is_closed(self) -> bool:
        """Return whether the queue is closed."""
        return self._closed
