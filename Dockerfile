# Multi_Duck Dockerfile
# Multi-user REST API wrapper for DuckDB

FROM python:3.12-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Set working directory
WORKDIR /app

# Install system dependencies (if needed for pyarrow/duckdb)
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml README.md ./
COPY multi_duck/ ./multi_duck/

# Install dependencies
RUN pip install --no-cache-dir .

# Create data directory for the database
RUN mkdir -p /data

# Set default environment variables
ENV MULTI_DUCK_DB_PATH=/data/multi_duck.duckdb \
    MULTI_DUCK_HOST=0.0.0.0 \
    MULTI_DUCK_PORT=8000 \
    MULTI_DUCK_READ_POOL_SIZE=10 \
    MULTI_DUCK_VACUUM_INTERVAL=86400 \
    MULTI_DUCK_VACUUM_ENABLED=true

# Expose the API port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import httpx; httpx.get('http://localhost:8000/health').raise_for_status()" || exit 1

# Run the server
CMD ["python", "-m", "multi_duck.main"]
