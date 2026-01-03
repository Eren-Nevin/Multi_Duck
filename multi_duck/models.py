"""Pydantic models for Multi_Duck API."""

from typing import Any

from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    """Request model for executing a SQL query."""

    sql: str = Field(
        ...,
        description="The SQL query to execute",
        min_length=1,
        examples=["SELECT * FROM users WHERE age > 25"],
    )
    params: dict[str, Any] | None = Field(
        default=None,
        description="Optional parameters for prepared statements",
        examples=[{"age": 25}],
    )


class QueryResponse(BaseModel):
    """Response model for write operations."""

    success: bool = Field(
        ...,
        description="Whether the query executed successfully",
    )
    rows_affected: int = Field(
        default=-1,
        description="Number of rows affected (-1 if not applicable)",
    )
    message: str = Field(
        default="",
        description="Additional information about the query execution",
    )


class ErrorResponse(BaseModel):
    """Response model for errors."""

    success: bool = Field(
        default=False,
        description="Always false for error responses",
    )
    error: str = Field(
        ...,
        description="Error message",
    )
    error_type: str = Field(
        default="Error",
        description="Type of error that occurred",
    )


class HealthResponse(BaseModel):
    """Response model for health check."""

    status: str = Field(
        default="healthy",
        description="Health status of the service",
    )
    read_pool_available: int = Field(
        ...,
        description="Number of available read connections",
    )
    read_pool_size: int = Field(
        ...,
        description="Total size of the read connection pool",
    )
    write_queue_pending: int = Field(
        ...,
        description="Number of pending write operations",
    )


class SchemaInfo(BaseModel):
    """Information about a database schema object."""

    name: str = Field(..., description="Name of the object")
    type: str = Field(..., description="Type of the object (table, view, etc.)")
    columns: list[dict[str, str]] = Field(
        default_factory=list,
        description="List of columns with name and type",
    )


class SchemaResponse(BaseModel):
    """Response model for schema information."""

    tables: list[SchemaInfo] = Field(
        default_factory=list,
        description="List of tables in the database",
    )
    views: list[SchemaInfo] = Field(
        default_factory=list,
        description="List of views in the database",
    )


class CompactResponse(BaseModel):
    """Response model for compaction operations."""

    success: bool = Field(
        ...,
        description="Whether compaction completed successfully",
    )
    message: str = Field(
        default="",
        description="Information about the compaction",
    )
    vacuum_completed: bool = Field(
        default=False,
        description="Whether VACUUM was executed",
    )
    checkpoint_completed: bool = Field(
        default=False,
        description="Whether CHECKPOINT was executed",
    )
