"""Database compaction scheduler for handling fragmentation."""

import asyncio
import logging
from datetime import datetime
from pathlib import Path

import duckdb

from .exceptions import CompactionError

logger = logging.getLogger(__name__)


class CompactionScheduler:
    """
    Scheduler for periodic database compaction (VACUUM/CHECKPOINT).

    This class handles automatic database maintenance to prevent
    fragmentation and reclaim space from deleted rows.
    """

    def __init__(
        self,
        db_path: Path,
        interval: int = 86400,
        enabled: bool = True,
    ):
        """
        Initialize the compaction scheduler.

        Args:
            db_path: Path to the DuckDB database file.
            interval: Interval between compaction runs in seconds (default: 24 hours).
            enabled: Whether automatic compaction is enabled.
        """
        self._db_path = db_path
        self._interval = interval
        self._enabled = enabled
        self._task: asyncio.Task | None = None
        self._closed = False
        self._last_compaction: datetime | None = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        """Start the compaction scheduler."""
        if not self._enabled:
            logger.info("Compaction scheduler is disabled")
            return

        if self._task is not None:
            logger.warning("Compaction scheduler already running")
            return

        logger.info(f"Starting compaction scheduler (interval: {self._interval}s)")
        self._task = asyncio.create_task(self._scheduler_loop())

    async def _scheduler_loop(self) -> None:
        """Main scheduler loop."""
        while not self._closed:
            try:
                # Wait for the interval
                await asyncio.sleep(self._interval)

                if self._closed:
                    break

                # Run compaction
                logger.info("Running scheduled compaction")
                await self.run_compaction()

            except asyncio.CancelledError:
                logger.info("Compaction scheduler cancelled")
                break
            except Exception as e:
                logger.error(f"Error in compaction scheduler: {e}")
                # Continue running despite errors

    async def run_compaction(self) -> dict[str, bool]:
        """
        Run database compaction (VACUUM and CHECKPOINT).

        Returns:
            A dictionary with the results of each operation.

        Raises:
            CompactionError: If compaction fails.
        """
        async with self._lock:
            logger.info("Starting database compaction")

            results = {
                "vacuum_completed": False,
                "checkpoint_completed": False,
            }

            loop = asyncio.get_event_loop()

            try:
                # Run VACUUM and CHECKPOINT
                await loop.run_in_executor(None, self._run_vacuum)
                results["vacuum_completed"] = True
                logger.info("VACUUM completed successfully")

                await loop.run_in_executor(None, self._run_checkpoint)
                results["checkpoint_completed"] = True
                logger.info("CHECKPOINT completed successfully")

                self._last_compaction = datetime.now()
                logger.info("Database compaction completed successfully")

                return results

            except Exception as e:
                logger.error(f"Compaction failed: {e}")
                raise CompactionError(str(e))

    def _run_vacuum(self) -> None:
        """Run VACUUM command (executes in thread pool)."""
        # Need exclusive access for VACUUM
        conn = duckdb.connect(str(self._db_path), read_only=False)
        try:
            conn.execute("VACUUM")
        finally:
            conn.close()

    def _run_checkpoint(self) -> None:
        """Run CHECKPOINT command (executes in thread pool)."""
        conn = duckdb.connect(str(self._db_path), read_only=False)
        try:
            conn.execute("CHECKPOINT")
        finally:
            conn.close()

    async def stop(self) -> None:
        """Stop the compaction scheduler."""
        self._closed = True

        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        logger.info("Compaction scheduler stopped")

    @property
    def last_compaction(self) -> datetime | None:
        """Return the time of the last compaction."""
        return self._last_compaction

    @property
    def is_enabled(self) -> bool:
        """Return whether the scheduler is enabled."""
        return self._enabled

    @property
    def interval(self) -> int:
        """Return the compaction interval in seconds."""
        return self._interval
