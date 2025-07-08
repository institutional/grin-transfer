"""
Process summary logging infrastructure for pipeline operations.

Tracks timing, errors, retries, and other operational metrics for long-running
pipeline operations like collect, process, sync, and enrich.
"""

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiofiles

logger = logging.getLogger(__name__)


@dataclass
class ProcessSummary:
    """
    Tracks comprehensive metrics for a pipeline process.

    Follows existing codebase patterns:
    - Uses session IDs with Unix timestamps
    - Tracks timing with time.perf_counter() for precision
    - Persists state to survive interruptions
    - Includes error counts and retry tracking
    """

    # Process identification
    process_name: str
    run_name: str
    session_id: int = field(default_factory=lambda: int(datetime.now(UTC).timestamp()))

    # Timing metrics
    start_time: float = field(default_factory=time.perf_counter)
    end_time: float | None = None
    duration_seconds: float | None = None

    # Operation counters
    items_processed: int = 0
    items_successful: int = 0
    items_failed: int = 0
    items_retried: int = 0
    bytes_processed: int = 0

    # Error tracking
    error_count: int = 0
    error_types: dict[str, int] = field(default_factory=dict)
    last_error_message: str | None = None
    last_error_time: float | None = None

    # Interruption detection
    interruption_count: int = 0
    last_checkpoint_time: float | None = None

    # Progress tracking
    progress_updates: list[dict[str, Any]] = field(default_factory=list)

    # Custom metrics (for command-specific data)
    custom_metrics: dict[str, Any] = field(default_factory=dict)

    def start_process(self) -> None:
        """Start the process tracking."""
        self.start_time = time.perf_counter()
        self.last_checkpoint_time = self.start_time
        logger.info(f"Started process summary tracking for {self.process_name} (session {self.session_id})")

    def end_process(self) -> None:
        """End the process tracking and calculate final metrics."""
        if self.end_time is None:
            self.end_time = time.perf_counter()
            self.duration_seconds = self.end_time - self.start_time
            logger.info(f"Ended process summary tracking for {self.process_name} after {self.duration_seconds:.1f}s")

    def add_progress_update(self, message: str, **kwargs) -> None:
        """Add a progress update with timestamp."""
        current_time = time.perf_counter()
        elapsed = current_time - self.start_time

        update = {
            "timestamp": current_time,
            "elapsed_seconds": elapsed,
            "message": message,
            "items_processed": self.items_processed,
            "items_successful": self.items_successful,
            "items_failed": self.items_failed,
            **kwargs
        }

        self.progress_updates.append(update)
        logger.debug(f"Progress update: {message} (elapsed: {elapsed:.1f}s)")

    def increment_items(self, processed: int = 0, successful: int = 0, failed: int = 0,
                       retried: int = 0, bytes_count: int = 0) -> None:
        """Increment item counters."""
        self.items_processed += processed
        self.items_successful += successful
        self.items_failed += failed
        self.items_retried += retried
        self.bytes_processed += bytes_count

    def add_error(self, error_type: str, error_message: str) -> None:
        """Record an error occurrence."""
        self.error_count += 1
        self.error_types[error_type] = self.error_types.get(error_type, 0) + 1
        self.last_error_message = error_message
        self.last_error_time = time.perf_counter()
        logger.debug(f"Recorded error: {error_type} - {error_message}")

    def detect_interruption(self) -> bool:
        """
        Detect if the process was interrupted and restarted.

        Returns True if interruption is detected based on time gap since last checkpoint.
        """
        if self.last_checkpoint_time is None:
            return False

        current_time = time.perf_counter()
        time_gap = current_time - self.last_checkpoint_time

        # If more than 5 minutes have passed without a checkpoint, consider it an interruption
        if time_gap > 300:  # 5 minutes
            self.interruption_count += 1
            self.add_progress_update(f"Interruption detected: {time_gap:.1f}s gap since last checkpoint")
            logger.warning(f"Process interruption detected: {time_gap:.1f}s gap")
            return True

        return False

    def checkpoint(self) -> None:
        """Update checkpoint time to track for interruptions."""
        self.last_checkpoint_time = time.perf_counter()

    def set_custom_metric(self, key: str, value: Any) -> None:
        """Set a custom metric for command-specific data."""
        self.custom_metrics[key] = value

    def get_summary_dict(self) -> dict[str, Any]:
        """Get a dictionary representation of the process summary."""
        # Calculate final duration if not set
        if self.duration_seconds is None and self.end_time is not None:
            self.duration_seconds = self.end_time - self.start_time
        elif self.duration_seconds is None:
            # Process is still running
            current_time = time.perf_counter()
            current_duration = current_time - self.start_time
        else:
            current_duration = self.duration_seconds

        # Calculate success rate
        total_attempted = self.items_successful + self.items_failed
        success_rate = (self.items_successful / total_attempted * 100) if total_attempted > 0 else 0

        # Calculate processing rate
        duration_for_rate = self.duration_seconds if self.duration_seconds else current_duration
        processing_rate = self.items_processed / duration_for_rate if duration_for_rate > 0 else 0

        return {
            "process_name": self.process_name,
            "run_name": self.run_name,
            "session_id": self.session_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": self.duration_seconds,
            "is_completed": self.end_time is not None,
            "items_processed": self.items_processed,
            "items_successful": self.items_successful,
            "items_failed": self.items_failed,
            "items_retried": self.items_retried,
            "bytes_processed": self.bytes_processed,
            "success_rate_percent": round(success_rate, 2),
            "processing_rate_per_second": round(processing_rate, 2),
            "error_count": self.error_count,
            "error_types": self.error_types,
            "last_error_message": self.last_error_message,
            "last_error_time": self.last_error_time,
            "interruption_count": self.interruption_count,
            "progress_updates_count": len(self.progress_updates),
            "progress_updates": self.progress_updates,
            "custom_metrics": self.custom_metrics,
            "generated_at": datetime.now(UTC).isoformat(),
        }

    def get_progress_summary(self) -> dict[str, Any]:
        """Get a summary of recent progress updates."""
        if not self.progress_updates:
            return {}

        recent_updates = self.progress_updates[-5:]  # Last 5 updates
        return {
            "total_updates": len(self.progress_updates),
            "recent_updates": recent_updates,
            "first_update": self.progress_updates[0] if self.progress_updates else None,
            "latest_update": self.progress_updates[-1] if self.progress_updates else None,
        }


class ProcessSummaryManager:
    """
    Manages process summary persistence and recovery.

    Handles saving/loading process summaries to survive interruptions.
    """

    def __init__(self, run_name: str, process_name: str):
        self.run_name = run_name
        self.process_name = process_name
        self.summary_file = Path(f"runs/{run_name}/process_summary_{process_name}.json")
        self.summary: ProcessSummary | None = None

    async def load_or_create_summary(self) -> ProcessSummary:
        """Load existing summary or create new one."""
        if await self._summary_file_exists():
            try:
                summary = await self._load_summary()
                # Detect interruption if we're loading an existing summary
                if summary.detect_interruption():
                    summary.add_progress_update("Process resumed after interruption")
                return summary
            except Exception as e:
                logger.warning(f"Failed to load existing summary: {e}")
                # Fall back to creating new summary

        # Create new summary
        summary = ProcessSummary(
            process_name=self.process_name,
            run_name=self.run_name
        )
        summary.start_process()
        return summary

    async def save_summary(self, summary: ProcessSummary) -> None:
        """Save process summary to file."""
        try:
            # Ensure directory exists
            self.summary_file.parent.mkdir(parents=True, exist_ok=True)

            # Save summary as JSON
            summary_dict = summary.get_summary_dict()
            async with aiofiles.open(self.summary_file, "w") as f:
                await f.write(json.dumps(summary_dict, indent=2))

            logger.debug(f"Saved process summary to {self.summary_file}")
        except Exception as e:
            logger.error(f"Failed to save process summary: {e}")

    async def _summary_file_exists(self) -> bool:
        """Check if summary file exists."""
        return self.summary_file.exists()

    async def _load_summary(self) -> ProcessSummary:
        """Load summary from file."""
        async with aiofiles.open(self.summary_file) as f:
            content = await f.read()
            data = json.loads(content)

        # Reconstruct ProcessSummary from saved data
        summary = ProcessSummary(
            process_name=data["process_name"],
            run_name=data["run_name"],
            session_id=data["session_id"]
        )

        # Restore saved state
        summary.start_time = data["start_time"]
        summary.end_time = data["end_time"]
        summary.duration_seconds = data["duration_seconds"]
        summary.items_processed = data["items_processed"]
        summary.items_successful = data["items_successful"]
        summary.items_failed = data["items_failed"]
        summary.items_retried = data["items_retried"]
        summary.bytes_processed = data["bytes_processed"]
        summary.error_count = data["error_count"]
        summary.error_types = data["error_types"]
        summary.last_error_message = data["last_error_message"]
        summary.last_error_time = data["last_error_time"]
        summary.interruption_count = data["interruption_count"]
        summary.custom_metrics = data["custom_metrics"]
        summary.progress_updates = data.get("progress_updates", [])

        return summary

    async def cleanup_summary(self) -> None:
        """Remove summary file after successful completion."""
        try:
            if self.summary_file.exists():
                self.summary_file.unlink()
                logger.debug(f"Cleaned up process summary file: {self.summary_file}")
        except Exception as e:
            logger.warning(f"Failed to cleanup process summary file: {e}")


async def create_process_summary(run_name: str, process_name: str) -> ProcessSummary:
    """
    Create or load a process summary for a pipeline operation.

    Args:
        run_name: Name of the run (e.g., "harvard_2024")
        process_name: Name of the process (e.g., "collect", "sync", "enrich")

    Returns:
        ProcessSummary instance ready for tracking
    """
    manager = ProcessSummaryManager(run_name, process_name)
    return await manager.load_or_create_summary()


async def save_process_summary(summary: ProcessSummary) -> None:
    """
    Save a process summary to file.

    Args:
        summary: ProcessSummary instance to save
    """
    manager = ProcessSummaryManager(summary.run_name, summary.process_name)
    await manager.save_summary(summary)
