"""Configuration management for Multi_Duck."""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="MULTI_DUCK_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database settings
    db_path: Path = Path("./data.duckdb")

    # Connection pool settings
    read_pool_size: int = 10
    read_pool_timeout: float = 30.0  # seconds to wait for a connection

    # Server settings
    host: str = "0.0.0.0"
    port: int = 8000

    # Compaction settings
    vacuum_interval: int = 86400  # seconds (default: 24 hours)
    vacuum_enabled: bool = True

    # Query settings
    query_timeout: float = 300.0  # seconds (default: 5 minutes)

    def get_db_path(self) -> Path:
        """Get the absolute path to the database file."""
        return self.db_path.resolve()


# Global settings instance
settings = Settings()
