"""FastAPI application for Multi_Duck."""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

import duckdb
import uvicorn
from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import JSONResponse

from .compaction import CompactionScheduler
from .config import settings
from .exceptions import (
    ConnectionPoolClosedError,
    ConnectionPoolExhaustedError,
    MultiDuckError,
    QueryError,
    WriteQueueClosedError,
)
from .models import (
    CompactResponse,
    ErrorResponse,
    HealthResponse,
    QueryRequest,
    QueryResponse,
    SchemaInfo,
    SchemaResponse,
)
from .pool import ReadConnectionPool
from .router import QueryRouter, QueryType
from .writer import WriteQueue

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# Global state for connection pool, write queue, and compaction scheduler
class AppState:
    """Application state container."""

    read_pool: ReadConnectionPool | None = None
    write_queue: WriteQueue | None = None
    compaction_scheduler: CompactionScheduler | None = None
    query_router: QueryRouter | None = None


state = AppState()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage application lifespan - startup and shutdown."""
    db_path = settings.get_db_path()
    logger.info(f"Starting Multi_Duck with database: {db_path}")

    # Ensure parent directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Initialize write queue first (it creates the database if needed)
    state.write_queue = WriteQueue(db_path)
    await state.write_queue.initialize()

    # Initialize read pool
    state.read_pool = ReadConnectionPool(
        db_path=db_path,
        pool_size=settings.read_pool_size,
        timeout=settings.read_pool_timeout,
    )
    await state.read_pool.initialize()

    # Initialize query router
    state.query_router = QueryRouter(
        read_pool=state.read_pool,
        write_queue=state.write_queue,
    )

    # Initialize and start compaction scheduler
    state.compaction_scheduler = CompactionScheduler(
        db_path=db_path,
        interval=settings.vacuum_interval,
        enabled=settings.vacuum_enabled,
    )
    await state.compaction_scheduler.start()

    logger.info("Multi_Duck started successfully")

    yield

    # Shutdown
    logger.info("Shutting down Multi_Duck")

    if state.compaction_scheduler:
        await state.compaction_scheduler.stop()

    if state.read_pool:
        await state.read_pool.close()

    if state.write_queue:
        await state.write_queue.close()

    logger.info("Multi_Duck shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Multi_Duck",
    description="Multi-user REST API wrapper for DuckDB with concurrent read/write support",
    version="0.1.0",
    lifespan=lifespan,
)


@app.exception_handler(MultiDuckError)
async def multi_duck_error_handler(request, exc: MultiDuckError) -> JSONResponse:
    """Handle Multi_Duck errors."""
    return JSONResponse(
        status_code=400,
        content=ErrorResponse(
            success=False,
            error=exc.message,
            error_type=exc.error_type,
        ).model_dump(),
    )


@app.exception_handler(ConnectionPoolExhaustedError)
async def pool_exhausted_handler(request, exc: ConnectionPoolExhaustedError) -> JSONResponse:
    """Handle connection pool exhausted errors."""
    return JSONResponse(
        status_code=503,
        content=ErrorResponse(
            success=False,
            error=exc.message,
            error_type=exc.error_type,
        ).model_dump(),
    )


@app.post(
    "/query",
    responses={
        200: {
            "description": "Query result",
            "content": {
                "application/octet-stream": {
                    "example": b"<parquet data>"
                },
                "application/json": {
                    "model": QueryResponse
                },
            },
        },
        400: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    },
)
async def execute_query(request: QueryRequest) -> Response:
    """
    Execute a SQL query.

    For SELECT queries, returns Parquet-encoded data.
    For INSERT/UPDATE/DELETE/DDL, returns JSON with affected rows.
    """
    if state.query_router is None:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        query_type, result = await state.query_router.execute(
            sql=request.sql,
            params=request.params,
        )

        if query_type == QueryType.READ:
            # Return Parquet data for read queries
            return Response(
                content=result,
                media_type="application/octet-stream",
                headers={"X-Multi-Duck-Format": "parquet"},
            )
        else:
            # Return JSON for write queries
            return JSONResponse(content=result)

    except QueryError as e:
        return JSONResponse(
            status_code=400,
            content=ErrorResponse(
                success=False,
                error=e.message,
                error_type=e.error_type,
            ).model_dump(),
        )
    except (ConnectionPoolClosedError, WriteQueueClosedError) as e:
        return JSONResponse(
            status_code=503,
            content=ErrorResponse(
                success=False,
                error=e.message,
                error_type=e.error_type,
            ).model_dump(),
        )


@app.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Check the health of the service."""
    read_available = 0
    read_size = 0
    write_pending = 0

    if state.read_pool:
        read_available = state.read_pool.available
        read_size = state.read_pool.size

    if state.write_queue:
        write_pending = state.write_queue.pending

    return HealthResponse(
        status="healthy",
        read_pool_available=read_available,
        read_pool_size=read_size,
        write_queue_pending=write_pending,
    )


@app.get("/schema", response_model=SchemaResponse)
async def get_schema() -> SchemaResponse:
    """Get the database schema (tables and views)."""
    if state.read_pool is None:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        async with state.read_pool.acquire() as conn:
            loop = asyncio.get_event_loop()
            schema = await loop.run_in_executor(None, _get_schema_sync, conn)
            return schema
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _get_schema_sync(conn: duckdb.DuckDBPyConnection) -> SchemaResponse:
    """Get schema information synchronously."""
    tables: list[SchemaInfo] = []
    views: list[SchemaInfo] = []

    # Get tables
    result = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
    ).fetchall()

    for (table_name,) in result:
        columns = _get_columns(conn, table_name)
        tables.append(SchemaInfo(name=table_name, type="table", columns=columns))

    # Get views
    result = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_type = 'VIEW'"
    ).fetchall()

    for (view_name,) in result:
        columns = _get_columns(conn, view_name)
        views.append(SchemaInfo(name=view_name, type="view", columns=columns))

    return SchemaResponse(tables=tables, views=views)


def _get_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> list[dict[str, str]]:
    """Get column information for a table/view."""
    result = conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = ? ORDER BY ordinal_position",
        [table_name],
    ).fetchall()

    return [{"name": name, "type": dtype} for name, dtype in result]


@app.post("/compact", response_model=CompactResponse)
async def run_compaction() -> CompactResponse:
    """
    Manually trigger database compaction.

    This runs VACUUM and CHECKPOINT to reclaim space and ensure
    data durability.
    """
    if state.compaction_scheduler is None:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        results = await state.compaction_scheduler.run_compaction()
        return CompactResponse(
            success=True,
            message="Compaction completed successfully",
            vacuum_completed=results["vacuum_completed"],
            checkpoint_completed=results["checkpoint_completed"],
        )
    except Exception as e:
        return CompactResponse(
            success=False,
            message=str(e),
            vacuum_completed=False,
            checkpoint_completed=False,
        )


def run() -> None:
    """Run the Multi_Duck server."""
    uvicorn.run(
        "multi_duck.main:app",
        host=settings.host,
        port=settings.port,
        log_level="info",
    )


if __name__ == "__main__":
    run()
