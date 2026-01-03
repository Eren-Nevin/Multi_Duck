"""Tests for the FastAPI application."""

import io
import os
import tempfile
import uuid
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from httpx import ASGITransport, AsyncClient


@pytest.fixture
async def client(tmp_path: Path):
    """Create a test client with proper lifespan handling."""
    # Use unique database for each test to avoid conflicts
    db_path = tmp_path / f"test_{uuid.uuid4().hex}.duckdb"

    # Set environment variables before importing
    os.environ["MULTI_DUCK_DB_PATH"] = str(db_path)
    os.environ["MULTI_DUCK_READ_POOL_SIZE"] = "3"
    os.environ["MULTI_DUCK_VACUUM_ENABLED"] = "false"

    # Reimport to pick up new settings
    import importlib
    import multi_duck.config
    import multi_duck.main

    importlib.reload(multi_duck.config)
    importlib.reload(multi_duck.main)

    from multi_duck.main import app, lifespan

    async with lifespan(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            yield client


@pytest.mark.asyncio
async def test_health_check(client: AsyncClient):
    """Test the health check endpoint."""
    response = await client.get("/health")
    assert response.status_code == 200

    data = response.json()
    assert data["status"] == "healthy"
    assert "read_pool_available" in data
    assert "read_pool_size" in data
    assert "write_queue_pending" in data


@pytest.mark.asyncio
async def test_create_table(client: AsyncClient):
    """Test creating a table via /query endpoint."""
    response = await client.post(
        "/query",
        json={"sql": "CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)"},
    )
    assert response.status_code == 200

    data = response.json()
    assert data["success"] is True


@pytest.mark.asyncio
async def test_insert_data(client: AsyncClient):
    """Test inserting data via /query endpoint."""
    # Create table first
    await client.post(
        "/query",
        json={"sql": "CREATE TABLE IF NOT EXISTS products (id INTEGER, name VARCHAR)"},
    )

    response = await client.post(
        "/query",
        json={"sql": "INSERT INTO products VALUES (1, 'Widget'), (2, 'Gadget')"},
    )
    assert response.status_code == 200

    data = response.json()
    assert data["success"] is True


@pytest.mark.asyncio
async def test_select_returns_parquet(client: AsyncClient):
    """Test that SELECT queries return Parquet data."""
    # Create and populate table
    await client.post(
        "/query",
        json={"sql": "CREATE TABLE IF NOT EXISTS items (id INTEGER, value VARCHAR)"},
    )
    await client.post(
        "/query",
        json={"sql": "INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, 'c')"},
    )

    response = await client.post(
        "/query",
        json={"sql": "SELECT * FROM items ORDER BY id"},
    )
    assert response.status_code == 200
    assert response.headers.get("X-Multi-Duck-Format") == "parquet"
    assert response.headers.get("content-type") == "application/octet-stream"

    # Parse Parquet response
    buffer = io.BytesIO(response.content)
    table = pq.read_table(buffer)

    assert len(table) == 3
    assert table.column_names == ["id", "value"]


@pytest.mark.asyncio
async def test_query_with_params(client: AsyncClient):
    """Test queries with parameters."""
    # Create and populate table
    await client.post(
        "/query",
        json={"sql": "CREATE TABLE IF NOT EXISTS params_test (id INTEGER, name VARCHAR)"},
    )
    await client.post(
        "/query",
        json={"sql": "INSERT INTO params_test VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')"},
    )

    response = await client.post(
        "/query",
        json={
            "sql": "SELECT * FROM params_test WHERE id > $min_id",
            "params": {"min_id": 1},
        },
    )
    assert response.status_code == 200

    buffer = io.BytesIO(response.content)
    table = pq.read_table(buffer)

    assert len(table) == 2


@pytest.mark.asyncio
async def test_invalid_query(client: AsyncClient):
    """Test error handling for invalid queries."""
    response = await client.post(
        "/query",
        json={"sql": "SELECT * FROM nonexistent_table_xyz"},
    )
    assert response.status_code == 400

    data = response.json()
    assert data["success"] is False
    assert "error" in data


@pytest.mark.asyncio
async def test_schema_endpoint(client: AsyncClient):
    """Test the /schema endpoint."""
    # Create a table
    await client.post(
        "/query",
        json={"sql": "CREATE TABLE IF NOT EXISTS schema_test (id INTEGER, name VARCHAR)"},
    )

    response = await client.get("/schema")
    assert response.status_code == 200

    data = response.json()
    assert "tables" in data
    assert "views" in data

    # Find our table
    table_names = [t["name"] for t in data["tables"]]
    assert "schema_test" in table_names


@pytest.mark.asyncio
async def test_compact_endpoint(client: AsyncClient):
    """Test the /compact endpoint."""
    response = await client.post("/compact")
    assert response.status_code == 200

    data = response.json()
    assert "success" in data
    assert "vacuum_completed" in data
    assert "checkpoint_completed" in data


@pytest.mark.asyncio
async def test_empty_select(client: AsyncClient):
    """Test SELECT that returns no rows."""
    await client.post(
        "/query",
        json={"sql": "CREATE TABLE IF NOT EXISTS empty_test (id INTEGER)"},
    )

    response = await client.post(
        "/query",
        json={"sql": "SELECT * FROM empty_test"},
    )
    assert response.status_code == 200

    buffer = io.BytesIO(response.content)
    table = pq.read_table(buffer)

    assert len(table) == 0


@pytest.mark.asyncio
async def test_update_operation(client: AsyncClient):
    """Test UPDATE operation."""
    await client.post(
        "/query",
        json={"sql": "CREATE TABLE IF NOT EXISTS update_test (id INTEGER, value VARCHAR)"},
    )
    await client.post(
        "/query",
        json={"sql": "INSERT INTO update_test VALUES (1, 'old')"},
    )

    response = await client.post(
        "/query",
        json={"sql": "UPDATE update_test SET value = 'new' WHERE id = 1"},
    )
    assert response.status_code == 200

    data = response.json()
    assert data["success"] is True


@pytest.mark.asyncio
async def test_delete_operation(client: AsyncClient):
    """Test DELETE operation."""
    await client.post(
        "/query",
        json={"sql": "CREATE TABLE IF NOT EXISTS delete_test (id INTEGER)"},
    )
    await client.post(
        "/query",
        json={"sql": "INSERT INTO delete_test VALUES (1), (2), (3)"},
    )

    response = await client.post(
        "/query",
        json={"sql": "DELETE FROM delete_test WHERE id = 1"},
    )
    assert response.status_code == 200

    data = response.json()
    assert data["success"] is True
