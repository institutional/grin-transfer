"""
Process summary logging infrastructure for pipeline operations.

Tracks timing, errors, retries, and other operational metrics for a complete
pipeline run across multiple commands (collect, process, sync, enrich).
Uses a single summary file per run that accumulates data from all stages.
"""

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiofiles

from .common import compress_file_to_temp, format_duration, get_compressed_filename

logger = logging.getLogger(__name__)


@dataclass
class ProcessStageMetrics:
    """Metrics for a single pipeline stage (collect, process, sync, enrich)."""

    stage_name: str
    start_time: float | None = None
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

    # Command-line arguments and configuration
    command_args: dict[str, Any] = field(default_factory=dict)

    # Progress tracking
    progress_updates: list[dict[str, Any]] = field(default_factory=list)

    def start_stage(self) -> None:
        """Start timing this stage."""
        self.start_time = time.perf_counter()
        logger.info(f"Started {self.stage_name} stage")

    def end_stage(self) -> None:
        """End timing this stage."""
        if self.start_time is not None and self.end_time is None:
            self.end_time = time.perf_counter()
            self.duration_seconds = self.end_time - self.start_time
            logger.info(f"Ended {self.stage_name} stage after {self.duration_seconds:.1f}s")

    def add_progress_update(self, message: str, **kwargs) -> None:
        """Add a progress update with timestamp."""
        current_time = time.perf_counter()
        elapsed = current_time - self.start_time if self.start_time else 0

        update = {
            "timestamp": current_time,
            "elapsed_seconds": elapsed,
            "message": message,
            "items_processed": self.items_processed,
            "items_successful": self.items_successful,
            "items_failed": self.items_failed,
            **kwargs,
        }

        self.progress_updates.append(update)
        logger.debug(f"{self.stage_name} progress: {message} (elapsed: {elapsed:.1f}s)")

    def increment_items(
        self, processed: int = 0, successful: int = 0, failed: int = 0, retried: int = 0, bytes_count: int = 0
    ) -> None:
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
        logger.debug(f"{self.stage_name} error: {error_type} - {error_message}")

    def set_command_arg(self, key: str, value: Any) -> None:
        """Set a command-line argument or configuration value for this stage."""
        self.command_args[key] = value


@dataclass
class RunSummary:
    """
    Tracks comprehensive metrics for an entire pipeline run.

    Accumulates data from all pipeline stages (collect, process, sync, enrich)
    in a single summary that persists across command invocations.
    """

    run_name: str
    run_start_time: float = field(default_factory=time.perf_counter)
    run_end_time: float | None = None
    total_duration_seconds: float | None = None

    # Per-stage metrics
    stages: dict[str, ProcessStageMetrics] = field(default_factory=dict)

    # Overall run tracking
    interruption_count: int = 0
    last_checkpoint_time: float | None = None

    # Run-level metadata
    session_id: int = field(default_factory=lambda: int(datetime.now(UTC).timestamp()))

    def get_or_create_stage(self, stage_name: str) -> ProcessStageMetrics:
        """Get existing stage or create new one."""
        if stage_name not in self.stages:
            self.stages[stage_name] = ProcessStageMetrics(stage_name)
        return self.stages[stage_name]

    def start_stage(self, stage_name: str) -> ProcessStageMetrics:
        """Start a new stage or resume existing one."""
        stage = self.get_or_create_stage(stage_name)
        if stage.start_time is None:
            stage.start_stage()
        self.checkpoint()
        return stage

    def end_stage(self, stage_name: str) -> None:
        """End a stage."""
        if stage_name in self.stages:
            self.stages[stage_name].end_stage()
            self.checkpoint()

    def end_run(self) -> None:
        """End the entire run."""
        if self.run_end_time is None:
            self.run_end_time = time.perf_counter()
            self.total_duration_seconds = self.run_end_time - self.run_start_time
            logger.info(f"Ended run {self.run_name} after {self.total_duration_seconds:.1f}s")

    def detect_interruption(self) -> bool:
        """Detect if the run was interrupted and restarted."""
        if self.last_checkpoint_time is None:
            return False

        current_time = time.perf_counter()
        time_gap = current_time - self.last_checkpoint_time

        # If more than 5 minutes have passed without a checkpoint, consider it an interruption
        if time_gap > 300:  # 5 minutes
            self.interruption_count += 1
            logger.warning(f"Run interruption detected: {time_gap:.1f}s gap")
            return True

        return False

    def checkpoint(self) -> None:
        """Update checkpoint time to track for interruptions."""
        self.last_checkpoint_time = time.perf_counter()

    def get_summary_dict(self) -> dict[str, Any]:
        """Get a dictionary representation of the run summary."""
        # Calculate totals across all stages
        total_items_processed = sum(stage.items_processed for stage in self.stages.values())
        total_items_successful = sum(stage.items_successful for stage in self.stages.values())
        total_items_failed = sum(stage.items_failed for stage in self.stages.values())
        total_items_retried = sum(stage.items_retried for stage in self.stages.values())
        total_bytes_processed = sum(stage.bytes_processed for stage in self.stages.values())
        total_errors = sum(stage.error_count for stage in self.stages.values())

        # Calculate success rate
        success_rate = (
            (total_items_successful / max(1, total_items_processed)) * 100 if total_items_processed > 0 else 0
        )

        # Calculate processing rate
        if self.total_duration_seconds is None and self.run_end_time is not None:
            self.total_duration_seconds = self.run_end_time - self.run_start_time
        elif self.total_duration_seconds is None:
            # Run is still active
            current_time = time.perf_counter()
            elapsed = current_time - self.run_start_time
            processing_rate = total_items_processed / elapsed if elapsed > 0 else 0
        else:
            processing_rate = (
                total_items_processed / self.total_duration_seconds if self.total_duration_seconds > 0 else 0
            )

        # Convert stages to dict
        stages_dict = {}
        for stage_name, stage in self.stages.items():
            stages_dict[stage_name] = {
                "start_time": stage.start_time,
                "end_time": stage.end_time,
                "duration_seconds": stage.duration_seconds,
                "items_processed": stage.items_processed,
                "items_successful": stage.items_successful,
                "items_failed": stage.items_failed,
                "items_retried": stage.items_retried,
                "bytes_processed": stage.bytes_processed,
                "error_count": stage.error_count,
                "error_types": stage.error_types,
                "last_error_message": stage.last_error_message,
                "last_error_time": stage.last_error_time,
                "command_args": stage.command_args,
                "progress_updates": stage.progress_updates,
                "is_completed": stage.end_time is not None,
                "success_rate_percent": (
                    (stage.items_successful / max(1, stage.items_processed)) * 100 if stage.items_processed > 0 else 0
                ),
            }

        return {
            "run_name": self.run_name,
            "session_id": self.session_id,
            "run_start_time": self.run_start_time,
            "run_end_time": self.run_end_time,
            "total_duration_seconds": self.total_duration_seconds,
            "interruption_count": self.interruption_count,
            "last_checkpoint_time": self.last_checkpoint_time,
            # Totals across all stages
            "total_items_processed": total_items_processed,
            "total_items_successful": total_items_successful,
            "total_items_failed": total_items_failed,
            "total_items_retried": total_items_retried,
            "total_bytes_processed": total_bytes_processed,
            "total_error_count": total_errors,
            "overall_success_rate_percent": success_rate,
            "overall_processing_rate_per_second": processing_rate,
            # Per-stage details
            "stages": stages_dict,
            # Run status
            "is_completed": self.run_end_time is not None,
            "has_errors": total_errors > 0,
            "active_stages": [
                name for name, stage in self.stages.items() if stage.start_time is not None and stage.end_time is None
            ],
            "completed_stages": [name for name, stage in self.stages.items() if stage.end_time is not None],
            "generated_at": datetime.now(UTC).isoformat(),
        }


class RunSummaryManager:
    """
    Manages run summary persistence and recovery.

    Handles saving/loading run summaries to survive interruptions.
    Uses a single summary file per run that tracks all pipeline stages.
    """

    def __init__(self, run_name: str):
        self.run_name = run_name
        self.summary_file = Path(f"output/{run_name}/process_summary.json")
        self.summary: RunSummary | None = None
        self._storage_upload_enabled = False
        self._book_manager = None

    async def load_or_create_summary(self) -> RunSummary:
        """Load existing summary or create new one."""
        if await self._summary_file_exists():
            try:
                summary = await self._load_summary()
                # Detect interruption if we're loading an existing summary
                if summary.detect_interruption():
                    # Add interruption note to the most recent stage
                    if summary.stages:
                        latest_stage_name = max(summary.stages.keys(), key=lambda s: summary.stages[s].start_time or 0)
                        latest_stage = summary.stages[latest_stage_name]
                        latest_stage.add_progress_update("Run resumed after interruption")
                return summary
            except Exception as e:
                logger.warning(f"Failed to load existing summary: {e}")
                # Fall back to creating new summary

        # Create new summary
        summary = RunSummary(run_name=self.run_name)
        return summary

    def enable_storage_upload(self, book_manager) -> None:
        """Enable uploading process summaries to storage."""
        self._storage_upload_enabled = True
        self._book_manager = book_manager

    async def save_summary(self, summary: RunSummary) -> None:
        """Save run summary to file and optionally upload to storage."""
        try:
            # Ensure directory exists
            self.summary_file.parent.mkdir(parents=True, exist_ok=True)

            # Save summary as JSON
            summary_dict = summary.get_summary_dict()
            async with aiofiles.open(self.summary_file, "w") as f:
                await f.write(json.dumps(summary_dict, indent=2))

            logger.debug(f"Saved run summary to {self.summary_file}")

            # Upload to storage if enabled
            if self._storage_upload_enabled and self._book_manager:
                await self._upload_to_storage()

        except Exception as e:
            logger.error(f"Failed to save run summary: {e}")

    async def _upload_to_storage(self) -> None:
        """Upload compressed process summary to metadata bucket."""
        if not self._book_manager:
            logger.warning("Storage upload requested but no book storage configured")
            return

        try:
            # Generate storage path for compressed process summary
            base_filename = f"process_summary_{self.run_name}.json"
            compressed_filename = get_compressed_filename(base_filename)
            storage_path = self._book_manager.meta_path(compressed_filename)

            # Get original file size for logging
            original_size = self.summary_file.stat().st_size

            # Upload the compressed local summary file
            async with compress_file_to_temp(self.summary_file) as compressed_path:
                compressed_size = compressed_path.stat().st_size
                compression_ratio = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0

                await self._book_manager.storage.write_file(storage_path, str(compressed_path))

                logger.info(
                    f"Uploaded compressed process summary to storage: {storage_path} "
                    f"({original_size:,} -> {compressed_size:,} bytes, {compression_ratio:.1f}% compression)"
                )

        except Exception as e:
            logger.error(f"Failed to upload process summary to storage: {e}")
            # Don't raise - storage upload failure shouldn't break local summary saving

    async def _summary_file_exists(self) -> bool:
        """Check if summary file exists."""
        return self.summary_file.exists()

    async def _load_summary(self) -> RunSummary:
        """Load summary from file."""
        async with aiofiles.open(self.summary_file) as f:
            data = json.loads(await f.read())

        # Reconstruct RunSummary from saved data
        summary = RunSummary(
            run_name=data["run_name"],
            session_id=data["session_id"],
            run_start_time=data["run_start_time"],
            run_end_time=data.get("run_end_time"),
            total_duration_seconds=data.get("total_duration_seconds"),
            interruption_count=data.get("interruption_count", 0),
            last_checkpoint_time=data.get("last_checkpoint_time"),
        )

        # Reconstruct stages
        for stage_name, stage_data in data.get("stages", {}).items():
            stage = ProcessStageMetrics(
                stage_name=stage_name,
                start_time=stage_data.get("start_time"),
                end_time=stage_data.get("end_time"),
                duration_seconds=stage_data.get("duration_seconds"),
                items_processed=stage_data.get("items_processed", 0),
                items_successful=stage_data.get("items_successful", 0),
                items_failed=stage_data.get("items_failed", 0),
                items_retried=stage_data.get("items_retried", 0),
                bytes_processed=stage_data.get("bytes_processed", 0),
                error_count=stage_data.get("error_count", 0),
                error_types=stage_data.get("error_types", {}),
                last_error_message=stage_data.get("last_error_message"),
                last_error_time=stage_data.get("last_error_time"),
                command_args=stage_data.get("command_args", {}),
                progress_updates=stage_data.get("progress_updates", []),
            )
            summary.stages[stage_name] = stage

        return summary


# Convenience functions for backward compatibility
async def create_process_summary(run_name: str, process_name: str, book_manager=None) -> RunSummary:
    """Create or load a run summary and return the specified stage."""
    manager = RunSummaryManager(run_name)

    # Enable storage upload if book_manager is provided
    if book_manager is not None:
        manager.enable_storage_upload(book_manager)

    summary = await manager.load_or_create_summary()

    # Start the requested stage
    summary.start_stage(process_name)

    return summary


async def save_process_summary(summary: RunSummary, book_manager=None) -> None:
    """Save a run summary."""
    manager = RunSummaryManager(summary.run_name)

    # Enable storage upload if book_manager is provided
    if book_manager is not None:
        manager.enable_storage_upload(book_manager)

    await manager.save_summary(summary)


# For accessing the current stage in the summary
def get_current_stage(summary: RunSummary, stage_name: str) -> ProcessStageMetrics:
    """Get the current stage from a run summary."""
    return summary.get_or_create_stage(stage_name)


async def create_book_manager_for_uploads(run_name: str):
    """Create a BookManager instance for process summary uploads."""
    from .run_config import find_run_config
    from .storage.book_manager import BookManager
    from .storage.factories import create_storage_from_config

    try:
        # Find and load run configuration
        db_path = f"output/{run_name}/books.db"
        run_config = find_run_config(db_path)

        if not run_config:
            logger.warning(f"No run configuration found for run {run_name}")
            return None

        # Create storage instance
        storage_type = run_config.storage_type

        # Local storage doesn't need process summary uploads to remote storage
        if storage_type == "local":
            logger.debug(f"Skipping process summary upload for local storage run {run_name}")
            return None

        # Use the full storage config for new API
        storage = create_storage_from_config(run_config.storage_config)

        # Validate that metadata bucket is configured
        nested_config = run_config.storage_config["config"]
        if not nested_config.get("bucket_meta"):
            logger.warning(f"No metadata bucket configured for run {run_name}")
            return None

        # Create BookStorage with run name as prefix
        book_manager = BookManager(storage, storage_config=run_config.storage_config, base_prefix=run_name)
        return book_manager

    except Exception as e:
        logger.error(f"Failed to create book storage for uploads: {e}")
        return None


def display_stage_summary(summary: RunSummary, stage_name: str) -> None:
    """Display a concise summary of the stage that just completed and overall run state.

    Args:
        summary: The run summary containing stage data
        stage_name: Name of the stage to display (e.g., 'collect', 'process', 'sync', 'enrich')
    """
    if stage_name not in summary.stages:
        return

    stage = summary.stages[stage_name]

    # Skip if stage hasn't completed
    if stage.end_time is None or stage.duration_seconds is None:
        return

    # Determine stage display name and next command
    stage_display_map = {
        "collect": ("Collected", "python grin.py sync pipeline --queue converted"),
        "process": ("Requested processing for", "python grin.py process monitor"),
        "sync": ("Synced", None),
        "enrich": ("Enriched", "python grin.py export-csv"),
    }

    action_text, next_command = stage_display_map.get(stage_name, (stage_name.title(), None))

    # Format duration and rate
    duration_str = format_duration(stage.duration_seconds)

    # Calculate rate if we have items processed
    rate_str = ""
    if stage.items_processed > 0 and stage.duration_seconds > 0:
        rate = stage.items_processed / stage.duration_seconds
        if rate >= 1:
            rate_str = f" ({rate:.1f} books/s)"
        else:
            rate_str = f" ({60 * rate:.1f} books/min)"

    # Build main summary line for current stage
    items_text = f"{stage.items_processed:,} books" if stage.items_processed > 0 else "operation"

    print(f"\n✓ {action_text} {items_text} in {duration_str}{rate_str}")

    # Show detailed results for current stage if relevant
    if stage_name in ["sync"] and stage.items_processed > 0:
        failed = stage.items_failed
        successful = stage.items_successful
        skipped = stage.items_processed - successful - failed
        if skipped > 0:
            print(f"  Success: {successful:,} | Failed: {failed:,} | Skipped: {skipped:,}")
        elif failed > 0:
            print(f"  Success: {successful:,} | Failed: {failed:,}")
    elif stage.items_failed > 0 and stage.items_processed > 0:
        print(f"  Success: {stage.items_successful:,} | Failed: {stage.items_failed:,}")

    # Show brief collection overview if we have multiple stages
    if len(summary.stages) > 1:
        stage_order = ["collect", "process", "sync", "enrich"]
        completed_stages = []

        for s in stage_order:
            if s in summary.stages and summary.stages[s].end_time is not None:
                stage_data = summary.stages[s]
                if stage_data.items_processed > 0:
                    completed_stages.append(f"{s}:{stage_data.items_processed:,}")

        if completed_stages:
            print(f"  Collection: {' | '.join(completed_stages)} books")

    # Show next command if available
    if next_command:
        run_name = summary.run_name
        print(f"  Next: {next_command} --run-name {run_name}")

    print()  # Add blank line for spacing
