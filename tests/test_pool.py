"""Tests for the read connection pool."""

import asyncio
import tempfile
from pathlib import Path

import duckdb
import pytest

from multi_duck.exceptions import ConnectionPoolClosedError, ConnectionPoolExhaustedError
from multi_duck.pool import ReadConnectionPool


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Create a temporary database file."""
    db_file = tmp_path / "test.duckdb"
    # Initialize the database
    conn = duckdb.connect(str(db_file))
    conn.execute("CREATE TABLE test (id INTEGER, value VARCHAR)")
    conn.execute("INSERT INTO test VALUES (1, 'one'), (2, 'two'), (3, 'three')")
    conn.close()
    return db_file


@pytest.mark.asyncio
async def test_pool_initialization(db_path: Path):
    """Test pool initialization creates correct number of connections."""
    pool = ReadConnectionPool(db_path, pool_size=5)
    await pool.initialize()

    assert pool.size == 5
    assert pool.available == 5
    assert not pool.is_closed

    await pool.close()


@pytest.mark.asyncio
async def test_pool_acquire_and_release(db_path: Path):
    """Test acquiring and releasing connections."""
    pool = ReadConnectionPool(db_path, pool_size=3)
    await pool.initialize()

    assert pool.available == 3

    async with pool.acquire() as conn:
        assert pool.available == 2
        result = conn.execute("SELECT * FROM test").fetchall()
        assert len(result) == 3

    assert pool.available == 3

    await pool.close()


@pytest.mark.asyncio
async def test_pool_concurrent_reads(db_path: Path):
    """Test multiple concurrent reads."""
    pool = ReadConnectionPool(db_path, pool_size=5)
    await pool.initialize()

    async def read_data() -> list:
        async with pool.acquire() as conn:
            await asyncio.sleep(0.1)  # Simulate some work
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None, lambda: conn.execute("SELECT * FROM test").fetchall()
            )
            return result

    # Run 5 concurrent reads
    tasks = [read_data() for _ in range(5)]
    results = await asyncio.gather(*tasks)

    for result in results:
        assert len(result) == 3

    await pool.close()


@pytest.mark.asyncio
async def test_pool_exhaustion(db_path: Path):
    """Test pool exhaustion raises appropriate error."""
    pool = ReadConnectionPool(db_path, pool_size=2, timeout=0.5)
    await pool.initialize()

    async def hold_connection():
        async with pool.acquire():
            await asyncio.sleep(2)  # Hold connection longer than timeout

    # Start two tasks that hold connections
    task1 = asyncio.create_task(hold_connection())
    task2 = asyncio.create_task(hold_connection())

    # Wait a bit for tasks to acquire connections
    await asyncio.sleep(0.1)

    # Try to acquire a third connection
    with pytest.raises(ConnectionPoolExhaustedError):
        async with pool.acquire():
            pass

    task1.cancel()
    task2.cancel()

    try:
        await task1
    except asyncio.CancelledError:
        pass

    try:
        await task2
    except asyncio.CancelledError:
        pass

    await pool.close()


@pytest.mark.asyncio
async def test_pool_close(db_path: Path):
    """Test pool close prevents further acquisitions."""
    pool = ReadConnectionPool(db_path, pool_size=2)
    await pool.initialize()

    await pool.close()

    assert pool.is_closed

    with pytest.raises(ConnectionPoolClosedError):
        async with pool.acquire():
            pass


@pytest.mark.asyncio
async def test_pool_double_close(db_path: Path):
    """Test closing pool multiple times is safe."""
    pool = ReadConnectionPool(db_path, pool_size=2)
    await pool.initialize()

    await pool.close()
    await pool.close()  # Should not raise

    assert pool.is_closed
