"""Tests for the write queue."""

import asyncio
from pathlib import Path

import duckdb
import pytest

from multi_duck.exceptions import QueryError, WriteQueueClosedError
from multi_duck.writer import WriteQueue


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Return a path for a temporary database file."""
    return tmp_path / "test.duckdb"


@pytest.mark.asyncio
async def test_writer_initialization(db_path: Path):
    """Test write queue initialization."""
    writer = WriteQueue(db_path)
    await writer.initialize()

    assert not writer.is_closed
    assert writer.pending == 0

    await writer.close()


@pytest.mark.asyncio
async def test_writer_create_table(db_path: Path):
    """Test creating a table via write queue."""
    writer = WriteQueue(db_path)
    await writer.initialize()

    result = await writer.execute("CREATE TABLE test (id INTEGER, value VARCHAR)")
    assert result["success"] is True

    await writer.close()

    # Verify table was created
    conn = duckdb.connect(str(db_path), read_only=True)
    tables = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'test'"
    ).fetchall()
    conn.close()

    assert len(tables) == 1


@pytest.mark.asyncio
async def test_writer_insert(db_path: Path):
    """Test inserting data via write queue."""
    writer = WriteQueue(db_path)
    await writer.initialize()

    await writer.execute("CREATE TABLE test (id INTEGER, value VARCHAR)")
    result = await writer.execute("INSERT INTO test VALUES (1, 'one'), (2, 'two')")
    assert result["success"] is True

    await writer.close()

    # Verify data was inserted
    conn = duckdb.connect(str(db_path), read_only=True)
    rows = conn.execute("SELECT * FROM test").fetchall()
    conn.close()

    assert len(rows) == 2


@pytest.mark.asyncio
async def test_writer_update(db_path: Path):
    """Test updating data via write queue."""
    writer = WriteQueue(db_path)
    await writer.initialize()

    await writer.execute("CREATE TABLE test (id INTEGER, value VARCHAR)")
    await writer.execute("INSERT INTO test VALUES (1, 'one'), (2, 'two')")
    result = await writer.execute("UPDATE test SET value = 'updated' WHERE id = 1")
    assert result["success"] is True

    await writer.close()

    # Verify data was updated
    conn = duckdb.connect(str(db_path), read_only=True)
    row = conn.execute("SELECT value FROM test WHERE id = 1").fetchone()
    conn.close()

    assert row[0] == "updated"


@pytest.mark.asyncio
async def test_writer_delete(db_path: Path):
    """Test deleting data via write queue."""
    writer = WriteQueue(db_path)
    await writer.initialize()

    await writer.execute("CREATE TABLE test (id INTEGER, value VARCHAR)")
    await writer.execute("INSERT INTO test VALUES (1, 'one'), (2, 'two')")
    result = await writer.execute("DELETE FROM test WHERE id = 1")
    assert result["success"] is True

    await writer.close()

    # Verify data was deleted
    conn = duckdb.connect(str(db_path), read_only=True)
    rows = conn.execute("SELECT * FROM test").fetchall()
    conn.close()

    assert len(rows) == 1


@pytest.mark.asyncio
async def test_writer_concurrent_writes(db_path: Path):
    """Test multiple concurrent writes are serialized correctly."""
    writer = WriteQueue(db_path)
    await writer.initialize()

    await writer.execute("CREATE TABLE test (id INTEGER)")

    # Submit multiple writes concurrently
    tasks = [writer.execute(f"INSERT INTO test VALUES ({i})") for i in range(10)]
    results = await asyncio.gather(*tasks)

    for result in results:
        assert result["success"] is True

    await writer.close()

    # Verify all data was inserted
    conn = duckdb.connect(str(db_path), read_only=True)
    count = conn.execute("SELECT COUNT(*) FROM test").fetchone()[0]
    conn.close()

    assert count == 10


@pytest.mark.asyncio
async def test_writer_error_handling(db_path: Path):
    """Test error handling for invalid queries."""
    writer = WriteQueue(db_path)
    await writer.initialize()

    with pytest.raises(QueryError):
        await writer.execute("INSERT INTO nonexistent_table VALUES (1)")

    await writer.close()


@pytest.mark.asyncio
async def test_writer_close_prevents_new_writes(db_path: Path):
    """Test that closing the writer prevents new writes."""
    writer = WriteQueue(db_path)
    await writer.initialize()
    await writer.close()

    with pytest.raises(WriteQueueClosedError):
        await writer.execute("CREATE TABLE test (id INTEGER)")


@pytest.mark.asyncio
async def test_writer_with_params(db_path: Path):
    """Test write queue with parameterized queries."""
    writer = WriteQueue(db_path)
    await writer.initialize()

    await writer.execute("CREATE TABLE test (id INTEGER, value VARCHAR)")
    result = await writer.execute(
        "INSERT INTO test VALUES ($id, $value)",
        params={"id": 1, "value": "test"},
    )
    assert result["success"] is True

    await writer.close()

    # Verify data was inserted
    conn = duckdb.connect(str(db_path), read_only=True)
    row = conn.execute("SELECT * FROM test WHERE id = 1").fetchone()
    conn.close()

    assert row == (1, "test")
