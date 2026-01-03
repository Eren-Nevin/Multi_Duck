"""Query routing logic - classifies SQL as read or write operations."""

import io
import logging
import re
from enum import Enum
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from .exceptions import QueryError
from .pool import ReadConnectionPool
from .writer import WriteQueue

logger = logging.getLogger(__name__)


class QueryType(Enum):
    """Type of SQL query."""

    READ = "read"
    WRITE = "write"


# SQL keywords that indicate write operations
WRITE_KEYWORDS = frozenset({
    "INSERT",
    "UPDATE",
    "DELETE",
    "CREATE",
    "DROP",
    "ALTER",
    "TRUNCATE",
    "REPLACE",
    "MERGE",
    "UPSERT",
    "COPY",  # COPY can write
    "EXPORT",
    "ATTACH",
    "DETACH",
    "VACUUM",
    "CHECKPOINT",
    "PRAGMA",  # Some pragmas modify state
    "SET",  # SET can modify session/database state
    "LOAD",
    "INSTALL",
    "FORCE",
})

# Pattern to extract the first keyword from SQL
SQL_FIRST_KEYWORD_PATTERN = re.compile(
    r"^\s*(?:--[^\n]*\n\s*)*(?:/\*.*?\*/\s*)*(\w+)",
    re.IGNORECASE | re.DOTALL,
)


def classify_query(sql: str) -> QueryType:
    """
    Classify a SQL query as read or write.

    Args:
        sql: The SQL query to classify.

    Returns:
        QueryType.READ for SELECT queries, QueryType.WRITE for others.
    """
    # Extract the first keyword
    match = SQL_FIRST_KEYWORD_PATTERN.match(sql)
    if not match:
        # If we can't parse it, assume it's a write for safety
        logger.warning(f"Could not parse SQL query, treating as write: {sql[:50]}...")
        return QueryType.WRITE

    first_keyword = match.group(1).upper()

    if first_keyword in WRITE_KEYWORDS:
        return QueryType.WRITE

    # SELECT, SHOW, DESCRIBE, EXPLAIN, WITH (CTEs) are reads
    return QueryType.READ


class QueryRouter:
    """
    Routes queries to the appropriate handler (read pool or write queue).

    This class handles the logic of determining whether a query should
    be executed via the read pool (for parallel reads) or the write
    queue (for serialized writes).
    """

    def __init__(
        self,
        read_pool: ReadConnectionPool,
        write_queue: WriteQueue,
    ):
        """
        Initialize the query router.

        Args:
            read_pool: The connection pool for read operations.
            write_queue: The queue for write operations.
        """
        self._read_pool = read_pool
        self._write_queue = write_queue

    async def execute(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> tuple[QueryType, bytes | dict[str, Any]]:
        """
        Execute a SQL query, routing to the appropriate handler.

        Args:
            sql: The SQL query to execute.
            params: Optional parameters for the query.

        Returns:
            A tuple of (query_type, result) where result is either:
            - bytes (Parquet data) for read queries
            - dict for write queries

        Raises:
            QueryError: If the query fails.
        """
        query_type = classify_query(sql)

        if query_type == QueryType.READ:
            result = await self._execute_read(sql, params)
            return (query_type, result)
        else:
            result = await self._write_queue.execute(sql, params)
            return (query_type, result)

    async def _execute_read(
        self,
        sql: str,
        params: dict[str, Any] | None,
    ) -> bytes:
        """Execute a read query and return Parquet bytes."""
        import asyncio

        async with self._read_pool.acquire() as conn:
            loop = asyncio.get_event_loop()
            parquet_bytes = await loop.run_in_executor(
                None,
                self._execute_read_sync,
                conn,
                sql,
                params,
            )
            return parquet_bytes

    def _execute_read_sync(
        self,
        conn: duckdb.DuckDBPyConnection,
        sql: str,
        params: dict[str, Any] | None,
    ) -> bytes:
        """Execute a read query synchronously (runs in thread pool)."""
        try:
            if params:
                result = conn.execute(sql, params)
            else:
                result = conn.execute(sql)

            # Convert to Arrow table
            # In newer DuckDB versions, .arrow() returns a RecordBatchReader
            # We use fetch_arrow_table() which returns a proper Table
            arrow_table = result.fetch_arrow_table()

            # Convert to Parquet bytes
            buffer = io.BytesIO()
            pq.write_table(arrow_table, buffer, compression="snappy")
            return buffer.getvalue()

        except duckdb.Error as e:
            raise QueryError(str(e), e)


def arrow_to_parquet_bytes(table: pa.Table) -> bytes:
    """Convert an Arrow table to Parquet bytes."""
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    return buffer.getvalue()
