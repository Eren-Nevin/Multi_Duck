# Multi_Duck

## DuckDB Multi-User REST API

A RESTful bridge for DuckDB enabling concurrent multi-process access.

* **Use Case:** A fast, multi-process replacement for SQLite/DuckDB in data-intensive microservices.
* **Performance:** Optimized for internal data pipelines rather than public-facing APIs.
* **Security:** **None.** Do not expose this to the external internet.
* **Clients:** Roll your own client or use standard HTTP libraries.

> [!WARNING]
> **Experimental software.** This project is not intended for production environments.

## Overview

Multi_Duck solves two key limitations of DuckDB:

1. **Single-user limitation**: DuckDB doesn't natively support multiple concurrent writers. Multi_Duck enables multi-user access by serializing writes through a queue while allowing parallel reads.

2. **Fragmentation**: DuckDB is append-only and doesn't automatically free space when data is deleted. Multi_Duck includes scheduled compaction (VACUUM/CHECKPOINT) to handle this. Later on we plan to add table recreation to make it even more powerful

## Features

- **Parallel reads**: Pool of read-only connections for concurrent SELECT queries
- **Serialized writes**: Write queue ensures safe concurrent access
- **Parquet responses**: Query results returned in efficient Parquet format
- **Automatic compaction**: Scheduled VACUUM/CHECKPOINT to prevent fragmentation
- **FastAPI-powered**: Modern async REST API with automatic OpenAPI documentation

## Installation

```bash
pip install multi-duck
```

Or install from source:

```bash
git clone https://github.com/yourusername/Multi_Duck.git
cd Multi_Duck
pip install -e ".[dev]"
```

## Quick Start

### Start the server

```bash
multi-duck
```

Or with Python:

```bash
python -m multi_duck.main
```

The server starts at `http://localhost:8000` by default.

### API Usage

#### Execute a query

```bash
# Create a table
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)"}'

# Insert data
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "INSERT INTO users VALUES (1, '\''Alice'\'', 30), (2, '\''Bob'\'', 25)"}'

# Query data (returns Parquet)
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM users WHERE age > 20"}' \
  -o result.parquet
```

#### Health check

```bash
curl http://localhost:8000/health
```

#### Get schema

```bash
curl http://localhost:8000/schema
```

#### Manual compaction

```bash
curl -X POST http://localhost:8000/compact
```

### Python Client Example

```python
import httpx
import pyarrow.parquet as pq
import io

# Create a table
response = httpx.post(
    "http://localhost:8000/query",
    json={"sql": "CREATE TABLE products (id INTEGER, name VARCHAR, price DECIMAL(10,2))"}
)
print(response.json())

# Insert data
response = httpx.post(
    "http://localhost:8000/query",
    json={"sql": "INSERT INTO products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 19.99)"}
)
print(response.json())

# Query and parse Parquet response
response = httpx.post(
    "http://localhost:8000/query",
    json={"sql": "SELECT * FROM products"}
)

# Parse Parquet response to DataFrame
buffer = io.BytesIO(response.content)
table = pq.read_table(buffer)
df = table.to_pandas()
print(df)
```

## Docker

### Quick Start with Docker

```bash
# Build and run
docker compose up -d

# Check logs
docker compose logs -f

# Stop
docker compose down
```

### Production Deployment

```bash
# Build the production image
docker build -t multi-duck .

# Run with custom configuration
docker run -d \
  --name multi-duck \
  -p 8000:8000 \
  -v multi_duck_data:/data \
  -e MULTI_DUCK_READ_POOL_SIZE=20 \
  multi-duck
```

### Development with Hot Reload

```bash
# Start development server with hot reload
docker compose --profile dev up multi-duck-dev

# Source code changes will automatically reload the server
```

### Running Tests in Docker

```bash
# Run the test suite
docker compose --profile test up multi-duck-test

# Or run tests interactively
docker compose --profile dev run --rm multi-duck-dev pytest -v
```

### Docker Compose Profiles

| Profile | Service | Description |
|---------|---------|-------------|
| (default) | `multi-duck` | Production server with persistent volume |
| `dev` | `multi-duck-dev` | Development server with hot reload |
| `test` | `multi-duck-test` | Run test suite and exit |

### Environment Variables

Pass environment variables to customize the container:

```bash
docker compose up -d \
  -e MULTI_DUCK_READ_POOL_SIZE=20 \
  -e MULTI_DUCK_VACUUM_INTERVAL=3600
```

## Configuration

Configure via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `MULTI_DUCK_DB_PATH` | `./data.duckdb` | Path to the database file |
| `MULTI_DUCK_READ_POOL_SIZE` | `10` | Number of read connections |
| `MULTI_DUCK_READ_POOL_TIMEOUT` | `30.0` | Timeout for acquiring a connection (seconds) |
| `MULTI_DUCK_HOST` | `0.0.0.0` | Server host |
| `MULTI_DUCK_PORT` | `8000` | Server port |
| `MULTI_DUCK_VACUUM_INTERVAL` | `86400` | Compaction interval (seconds) |
| `MULTI_DUCK_VACUUM_ENABLED` | `true` | Enable automatic compaction |

Example:

```bash
MULTI_DUCK_DB_PATH=/data/mydb.duckdb \
MULTI_DUCK_READ_POOL_SIZE=20 \
MULTI_DUCK_PORT=9000 \
multi-duck
```

## API Reference

### POST /query

Execute a SQL query.

**Request:**
```json
{
  "sql": "SELECT * FROM users WHERE age > $min_age",
  "params": {"min_age": 25}
}
```

**Response (for SELECT):**
- Content-Type: `application/octet-stream`
- Header: `X-Multi-Duck-Format: parquet`
- Body: Parquet binary data

**Response (for INSERT/UPDATE/DELETE/DDL):**
```json
{
  "success": true,
  "rows_affected": 42,
  "message": "Query executed successfully"
}
```

**Response (for errors):**
```json
{
  "success": false,
  "error": "Table 'xyz' does not exist",
  "error_type": "CatalogException"
}
```

### GET /health

Health check endpoint.

**Response:**
```json
{
  "status": "healthy",
  "read_pool_available": 10,
  "read_pool_size": 10,
  "write_queue_pending": 0
}
```

### GET /schema

Get database schema information.

**Response:**
```json
{
  "tables": [
    {
      "name": "users",
      "type": "table",
      "columns": [
        {"name": "id", "type": "INTEGER"},
        {"name": "name", "type": "VARCHAR"}
      ]
    }
  ],
  "views": []
}
```

### POST /compact

Trigger manual database compaction.

**Response:**
```json
{
  "success": true,
  "message": "Compaction completed successfully",
  "vacuum_completed": true,
  "checkpoint_completed": true
}
```

## Architecture

```
                    ┌─────────────────────┐
                    │    FastAPI App      │
                    └─────────┬───────────┘
                              │
           ┌──────────────────┼──────────────────┐
           │                  │                  │
    ┌──────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐
    │ Read Pool   │    │ Write Queue │    │  Compaction │
    │ (N conns)   │    │ (1 writer)  │    │  Scheduler  │
    └──────┬──────┘    └──────┬──────┘    └──────┬──────┘
           │                  │                  │
    ┌──────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐
    │  READ_ONLY  │    │  READ_WRITE │    │  EXCLUSIVE  │
    │ Connections │    │  Connection │    │  Connection │
    └─────────────┘    └─────────────┘    └─────────────┘
                              │
                    ┌─────────▼─────────┐
                    │   database.duckdb │
                    └───────────────────┘
```

- **Read Pool**: Multiple read-only connections for parallel SELECT queries
- **Write Queue**: Single write connection with asyncio queue for serialized writes
- **Compaction Scheduler**: Periodic VACUUM/CHECKPOINT to reclaim space

## Development

### Setup

```bash
git clone https://github.com/yourusername/Multi_Duck.git
cd Multi_Duck
pip install -e ".[dev]"
```

### Run tests

```bash
pytest
```

### Run with auto-reload

```bash
uvicorn multi_duck.main:app --reload
```

## OpenAPI Documentation

When the server is running, visit:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## License

MIT License - see LICENSE file for details.

## Roadmap

- [ ] Authentication (API keys, JWT)
- [ ] Query timeout handling
- [ ] Rate limiting
- [ ] Query result caching
- [ ] Write batching optimization
- [ ] WAL-based replication for parallel writes
- [ ] Client libraries (Python, JavaScript, Go, etc.)
