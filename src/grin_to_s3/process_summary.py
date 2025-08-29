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
from typing import Any, Literal

import aiofiles

from .common import compress_file_to_temp, format_duration, get_compressed_filename

logger = logging.getLogger(__name__)

# Book processing outcome types
BookOutcome = Literal["conversion_requested", "skipped", "synced", "failed"]


@dataclass
class ProcessStageMetrics:
    """Metrics for a single pipeline stage (collect, process, sync, enrich)."""

    stage_name: str
    start_time: str | None = None  # UTC ISO timestamp
    end_time: str | None = None  # UTC ISO timestamp
    duration_seconds: float | None = None
    _start_perf_time: float | None = field(default=None, init=False, repr=False)  # Internal timing

    # Collection stage metrics
    books_collected: int = 0
    collection_failed: int = 0
    collection_rate_per_hour: float = 0.0

    # Processing (conversion request) stage metrics
    conversion_requests_made: int = 0
    conversion_requests_failed: int = 0
    conversion_request_rate_per_hour: float = 0.0

    # Sync stage metrics (cumulative)
    books_synced: int = 0
    sync_skipped: int = 0  # Already synced/etag match
    sync_failed: int = 0
    conversions_requested_during_sync: int = 0  # Sent for conversion
    sync_rate_per_hour: float = 0.0

    # Snapshot of cumulative metrics at start of current run
    metrics_snapshot_at_start: dict[str, int] = field(default_factory=dict)

    # Enrichment stage metrics
    books_enriched: int = 0
    enrichment_skipped: int = 0
    enrichment_failed: int = 0
    enrichment_rate_per_hour: float = 0.0

    # Common fields retained for all stages
    error_count: int = 0
    error_types: dict[str, int] = field(default_factory=dict)
    last_error_message: str | None = None
    last_error_time: str | None = None  # UTC ISO timestamp
    last_operation_time: str | None = None

    # Command-line arguments and configuration
    command_args: dict[str, Any] = field(default_factory=dict)

    # Progress tracking
    progress_updates: list[dict[str, Any]] = field(default_factory=list)

    # Stage-specific detailed metrics (varies by stage)
    stage_details: dict[str, Any] = field(default_factory=dict)

    # Legacy sync metrics for compatibility during transition
    queue_info: dict[str, Any] = field(default_factory=dict)  # Queue details from command args
    error_breakdown: dict[str, int] = field(default_factory=dict)  # Error counts by type

    def start_stage(self) -> None:
        """Start timing this stage."""
        self._start_perf_time = time.perf_counter()
        self.start_time = datetime.now(UTC).isoformat()
        logger.info(f"Started {self.stage_name} stage")

    def end_stage(self) -> None:
        """End timing this stage."""
        if self._start_perf_time is not None and self.end_time is None:
            end_perf_time = time.perf_counter()
            self.end_time = datetime.now(UTC).isoformat()
            self.duration_seconds = end_perf_time - self._start_perf_time
            logger.info(f"Ended {self.stage_name} stage after {self.duration_seconds:.1f}s")

    def add_progress_update(self, message: str, **kwargs) -> None:
        """Add a progress update with timestamp and stage-specific metrics."""
        current_time = time.perf_counter()
        elapsed = current_time - self._start_perf_time if self._start_perf_time else 0

        # Get stage-specific progress metrics
        stage_metrics = self.get_stage_progress_metrics()

        update = {
            "timestamp": datetime.now(UTC).isoformat(),
            "elapsed_seconds": elapsed,
            "message": message,
            **stage_metrics,
            **kwargs,
        }

        self.progress_updates.append(update)
        logger.debug(f"{self.stage_name} progress: {message} (elapsed: {elapsed:.1f}s)")

    def get_stage_totals(self) -> tuple[int, int, int]:
        """Get total processed, successful, failed for this stage."""
        if self.stage_name == "collect":
            total = self.books_collected + self.collection_failed
            successful = self.books_collected
            failed = self.collection_failed
        elif self.stage_name == "process":
            total = self.conversion_requests_made + self.conversion_requests_failed
            successful = self.conversion_requests_made
            failed = self.conversion_requests_failed
        elif self.stage_name == "sync":
            total = self.books_synced + self.sync_skipped + self.sync_failed + self.conversions_requested_during_sync
            successful = self.books_synced + self.sync_skipped + self.conversions_requested_during_sync
            failed = self.sync_failed
        elif self.stage_name == "enrich":
            total = self.books_enriched + self.enrichment_skipped + self.enrichment_failed
            successful = self.books_enriched + self.enrichment_skipped
            failed = self.enrichment_failed
        else:
            total = successful = failed = 0

        return total, successful, failed

    def get_stage_progress_metrics(self) -> dict[str, int]:
        """Get stage-specific progress metrics for progress updates."""
        if self.stage_name == "collect":
            return {
                "books_collected": self.books_collected,
                "collection_failed": self.collection_failed,
                "total_books_processed": self.books_collected + self.collection_failed,
            }
        elif self.stage_name == "process":
            return {
                "conversion_requests_made": self.conversion_requests_made,
                "conversion_requests_failed": self.conversion_requests_failed,
                "total_requests_processed": self.conversion_requests_made + self.conversion_requests_failed,
            }
        elif self.stage_name == "sync":
            return {
                "books_synced": self.books_synced,
                "sync_skipped": self.sync_skipped,
                "sync_failed": self.sync_failed,
                "conversions_requested_during_sync": self.conversions_requested_during_sync,
                "total_books_processed": self.books_synced
                + self.sync_skipped
                + self.sync_failed
                + self.conversions_requested_during_sync,
            }
        elif self.stage_name == "enrich":
            return {
                "books_enriched": self.books_enriched,
                "enrichment_skipped": self.enrichment_skipped,
                "enrichment_failed": self.enrichment_failed,
                "total_books_processed": self.books_enriched + self.enrichment_skipped + self.enrichment_failed,
            }
        else:
            return {"total_items_processed": 0, "total_successful": 0, "total_failed": 0}

    def add_error(self, error_type: str, error_message: str) -> None:
        """Record an error occurrence."""
        self.error_count += 1
        self.error_types[error_type] = self.error_types.get(error_type, 0) + 1
        self.last_error_message = error_message
        self.last_error_time = datetime.now(UTC).isoformat()
        logger.debug(f"{self.stage_name} error: {error_type} - {error_message}")

    def set_command_arg(self, key: str, value: Any) -> None:
        """Set a command-line argument or configuration value for this stage."""
        self.command_args[key] = value

    @staticmethod
    def determine_book_outcome(task_results: dict) -> BookOutcome:
        """Determine the overall outcome for a book based on its task results."""
        from grin_to_s3.sync.tasks.task_types import TaskAction, TaskType

        # Check if conversion was requested (for previous queue)
        if TaskType.REQUEST_CONVERSION in task_results:
            request_result = task_results[TaskType.REQUEST_CONVERSION]
            if request_result.action == TaskAction.SKIPPED and request_result.reason == "skip_conversion_requested":
                return "conversion_requested"

        # Check if the book was skipped early (already synced or etag match)
        if TaskType.CHECK in task_results:
            check_result = task_results[TaskType.CHECK]
            if check_result.action == TaskAction.SKIPPED:
                return "skipped"

        # Check if the full sync pipeline completed successfully
        if TaskType.UPLOAD in task_results:
            upload_result = task_results[TaskType.UPLOAD]
            if upload_result.action == TaskAction.COMPLETED:
                return "synced"

        # If we got here, something failed
        return "failed"

    def increment_by_outcome(self, outcome: BookOutcome) -> None:
        """Increment counters based on book outcome (for sync stage)."""
        if outcome == "synced":
            self.books_synced += 1
        elif outcome == "skipped":
            self.sync_skipped += 1
        elif outcome == "conversion_requested":
            self.conversions_requested_during_sync += 1
        elif outcome == "failed":
            self.sync_failed += 1

    def capture_start_snapshot(self) -> None:
        """Capture current metric values at the start of a pipeline run."""
        if self.stage_name == "sync":
            self.metrics_snapshot_at_start = {
                "books_synced": self.books_synced,
                "sync_skipped": self.sync_skipped,
                "sync_failed": self.sync_failed,
                "conversions_requested_during_sync": self.conversions_requested_during_sync,
            }


@dataclass
class RunSummary:
    """
    Tracks comprehensive metrics for an entire pipeline run.

    Accumulates data from all pipeline stages (collect, process, sync, enrich)
    in a single summary that persists across command invocations.
    """

    run_name: str
    run_start_time: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    run_end_time: str | None = None
    total_duration_seconds: float | None = None
    _run_start_perf_time: float = field(default_factory=time.perf_counter, init=False, repr=False)  # Internal timing
    _run_end_perf_time: float | None = field(default=None, init=False, repr=False)  # Internal timing

    # Per-stage metrics
    stages: dict[str, ProcessStageMetrics] = field(default_factory=dict)

    # Overall run tracking
    interruption_count: int = 0
    last_checkpoint_time: str | None = None  # UTC ISO timestamp
    _last_checkpoint_perf_time: float | None = field(default=None, init=False, repr=False)  # Internal timing

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
            self._run_end_perf_time = time.perf_counter()
            self.run_end_time = datetime.now(UTC).isoformat()
            self.total_duration_seconds = self._run_end_perf_time - self._run_start_perf_time
            logger.info(f"Ended run {self.run_name} after {self.total_duration_seconds:.1f}s")

    def detect_interruption(self) -> bool:
        """Detect if the run was interrupted and restarted."""
        if self._last_checkpoint_perf_time is None:
            return False

        current_time = time.perf_counter()
        time_gap = current_time - self._last_checkpoint_perf_time

        # If more than 5 minutes have passed without a checkpoint, consider it an interruption
        if time_gap > 300:  # 5 minutes
            self.interruption_count += 1
            logger.warning(f"Run interruption detected: {time_gap:.1f}s gap")
            return True

        return False

    def checkpoint(self) -> None:
        """Update checkpoint time to track for interruptions."""
        self._last_checkpoint_perf_time = time.perf_counter()
        self.last_checkpoint_time = datetime.now(UTC).isoformat()

    def get_summary_dict(self) -> dict[str, Any]:
        """Get a dictionary representation of the run summary."""
        # Calculate totals across all stages using new stage-specific totals
        total_items_processed = 0
        total_items_successful = 0
        total_items_failed = 0
        for stage in self.stages.values():
            stage_total, stage_successful, stage_failed = stage.get_stage_totals()
            total_items_processed += stage_total
            total_items_successful += stage_successful
            total_items_failed += stage_failed

        total_errors = sum(stage.error_count for stage in self.stages.values())
        total_errors = sum(stage.error_count for stage in self.stages.values())

        # Calculate success rate
        success_rate = (
            (total_items_successful / max(1, total_items_processed)) * 100 if total_items_processed > 0 else 0
        )

        # Calculate processing rate
        if self.total_duration_seconds is None and self._run_end_perf_time is not None:
            self.total_duration_seconds = self._run_end_perf_time - self._run_start_perf_time
        elif self.total_duration_seconds is None:
            # Run is still active
            current_time = time.perf_counter()
            elapsed = current_time - self._run_start_perf_time
            processing_rate = total_items_processed / elapsed if elapsed > 0 else 0
        else:
            processing_rate = (
                total_items_processed / self.total_duration_seconds if self.total_duration_seconds > 0 else 0
            )

        # Convert stages to dict with stage-specific counters
        stages_dict = {}
        for stage_name, stage in self.stages.items():
            # Get stage totals for success rate calculation
            stage_total, stage_successful, stage_failed = stage.get_stage_totals()

            stages_dict[stage_name] = {
                "start_time": stage.start_time,
                "end_time": stage.end_time,
                "duration_seconds": stage.duration_seconds,
                # Stage-specific counters based on stage type
                **(
                    {
                        "books_collected": stage.books_collected,
                        "collection_failed": stage.collection_failed,
                        "collection_rate_per_hour": stage.collection_rate_per_hour,
                    }
                    if stage_name == "collect"
                    else {}
                ),
                **(
                    {
                        "conversion_requests_made": stage.conversion_requests_made,
                        "conversion_requests_failed": stage.conversion_requests_failed,
                        "conversion_request_rate_per_hour": stage.conversion_request_rate_per_hour,
                    }
                    if stage_name == "process"
                    else {}
                ),
                **(
                    {
                        "books_synced": stage.books_synced,
                        "sync_skipped": stage.sync_skipped,
                        "sync_failed": stage.sync_failed,
                        "conversions_requested_during_sync": stage.conversions_requested_during_sync,
                        "sync_rate_per_hour": stage.sync_rate_per_hour,
                        "metrics_snapshot_at_start": stage.metrics_snapshot_at_start,
                    }
                    if stage_name == "sync"
                    else {}
                ),
                **(
                    {
                        "books_enriched": stage.books_enriched,
                        "enrichment_skipped": stage.enrichment_skipped,
                        "enrichment_failed": stage.enrichment_failed,
                        "enrichment_rate_per_hour": stage.enrichment_rate_per_hour,
                    }
                    if stage_name == "enrich"
                    else {}
                ),
                # Common fields
                "error_count": stage.error_count,
                "error_types": stage.error_types,
                "last_error_message": stage.last_error_message,
                "last_error_time": stage.last_error_time,
                "last_operation_time": stage.last_operation_time,
                "command_args": stage.command_args,
                "progress_updates": stage.progress_updates,
                "is_completed": stage.end_time is not None,
                "success_rate_percent": ((stage_successful / max(1, stage_total)) * 100 if stage_total > 0 else 0),
                "stage_details": stage.stage_details,
                # Legacy sync metrics for compatibility during transition
                "queue_info": stage.queue_info,
                "error_breakdown": stage.error_breakdown,
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

        # Initialize perf counters for ongoing operations
        summary._run_start_perf_time = time.perf_counter()

        if data.get("total_duration_seconds"):
            summary._run_end_perf_time = summary._run_start_perf_time + data["total_duration_seconds"]

        if data.get("last_checkpoint_time"):
            summary._last_checkpoint_perf_time = summary._run_start_perf_time

        # Reconstruct stages with new stage-specific fields
        for stage_name, stage_data in data.get("stages", {}).items():
            stage = ProcessStageMetrics(
                stage_name=stage_name,
                start_time=stage_data.get("start_time"),
                end_time=stage_data.get("end_time"),
                duration_seconds=stage_data.get("duration_seconds"),
                # Collection stage metrics
                books_collected=stage_data.get("books_collected", 0),
                collection_failed=stage_data.get("collection_failed", 0),
                collection_rate_per_hour=stage_data.get("collection_rate_per_hour", 0.0),
                # Processing stage metrics
                conversion_requests_made=stage_data.get("conversion_requests_made", 0),
                conversion_requests_failed=stage_data.get("conversion_requests_failed", 0),
                conversion_request_rate_per_hour=stage_data.get("conversion_request_rate_per_hour", 0.0),
                # Sync stage metrics (cumulative)
                books_synced=stage_data.get("books_synced", 0),
                sync_skipped=stage_data.get("sync_skipped", 0),
                sync_failed=stage_data.get("sync_failed", 0),
                conversions_requested_during_sync=stage_data.get("conversions_requested_during_sync", 0),
                sync_rate_per_hour=stage_data.get("sync_rate_per_hour", 0.0),
                # Metrics snapshot from start of current run
                metrics_snapshot_at_start=stage_data.get("metrics_snapshot_at_start", {}),
                # Enrichment stage metrics
                books_enriched=stage_data.get("books_enriched", 0),
                enrichment_skipped=stage_data.get("enrichment_skipped", 0),
                enrichment_failed=stage_data.get("enrichment_failed", 0),
                enrichment_rate_per_hour=stage_data.get("enrichment_rate_per_hour", 0.0),
                # Common fields
                error_count=stage_data.get("error_count", 0),
                error_types=stage_data.get("error_types", {}),
                last_error_message=stage_data.get("last_error_message"),
                last_error_time=stage_data.get("last_error_time"),
                last_operation_time=stage_data.get("last_operation_time"),
                command_args=stage_data.get("command_args", {}),
                progress_updates=stage_data.get("progress_updates", []),
                stage_details=stage_data.get("stage_details", {}),
                # Legacy sync metrics for compatibility
                queue_info=stage_data.get("queue_info", {}),
                error_breakdown=stage_data.get("error_breakdown", {}),
            )

            # Initialize perf counter for ongoing operations if stage not completed
            if stage_data.get("start_time") and not stage_data.get("end_time"):
                stage._start_perf_time = time.perf_counter()

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
    stage = summary.start_stage(process_name)

    # Capture starting metrics for session tracking
    stage.capture_start_snapshot()

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


def display_step_summary(summary: RunSummary, step_name: str) -> None:
    """Display stage-specific summary with appropriate terminology.

    Args:
        summary: The run summary containing step data
        step_name: Name of the step to display (e.g., 'collect', 'process', 'sync', 'enrich')
    """
    if step_name not in summary.stages:
        return

    step = summary.stages[step_name]

    # Check if step was interrupted (started but not completed)
    was_interrupted = step.start_time is not None and step.end_time is None

    # Skip display if step hasn't started
    if step.start_time is None:
        return

    # Determine duration to use
    if was_interrupted and step.duration_seconds is None:
        # Calculate duration up to now for interrupted steps
        if step._start_perf_time is not None:
            effective_duration = time.perf_counter() - step._start_perf_time
        else:
            return  # Can't calculate duration, skip display
    elif step.duration_seconds is not None:
        effective_duration = step.duration_seconds
    else:
        return  # No timing info available

    # Format duration
    duration_str = format_duration(effective_duration)

    # Display stage-specific metrics with appropriate terminology
    if step_name == "collect":
        print(f"\n✓ Collected {step.books_collected:,} books in {duration_str}")
        if step.collection_failed > 0:
            print(f"  Failed: {step.collection_failed:,}")
        next_command = "python grin.py sync pipeline --queue converted"

    elif step_name == "process":
        print(f"\n✓ Requested conversion for {step.conversion_requests_made:,} books in {duration_str}")
        if step.conversion_requests_failed > 0:
            print(f"  Failed: {step.conversion_requests_failed:,}")
        next_command = "python grin.py process monitor"

    elif step_name == "sync":
        if was_interrupted:
            print(f"\n⚠ Sync interrupted after {duration_str}")
        else:
            # Calculate session metrics from current values minus start snapshot
            session_books_synced = step.books_synced - step.metrics_snapshot_at_start.get("books_synced", 0)
            session_sync_skipped = step.sync_skipped - step.metrics_snapshot_at_start.get("sync_skipped", 0)
            session_sync_failed = step.sync_failed - step.metrics_snapshot_at_start.get("sync_failed", 0)
            session_conversions_requested = step.conversions_requested_during_sync - step.metrics_snapshot_at_start.get(
                "conversions_requested_during_sync", 0
            )

            session_total = (
                session_books_synced + session_sync_skipped + session_sync_failed + session_conversions_requested
            )

            if session_books_synced > 0:
                # Books were actually synced this session
                print(f"\n✓ Synced {session_books_synced:,} in {duration_str}")
            elif session_total == 0:
                # No books processed this session
                print(f"\n✓ Process completed in {duration_str}")
            elif session_sync_failed > 0:
                # Had errors this session
                print(f"\n⚠ Process completed with errors in {duration_str}")
            else:
                # Other outcomes (skipped, conversion requests)
                print(f"\n✓ Process completed in {duration_str}")

        # Display detailed sync metrics
        _display_sync_details(
            step,
            was_interrupted,
            session_books_synced=session_books_synced,
            session_sync_skipped=session_sync_skipped,
            session_sync_failed=session_sync_failed,
            session_conversions_requested=session_conversions_requested,
        )
        next_command = None

    elif step_name == "enrich":
        print(f"\n✓ Enriched {step.books_enriched:,} books in {duration_str}")
        if step.enrichment_skipped > 0:
            print(f"  Skipped: {step.enrichment_skipped:,}")
        if step.enrichment_failed > 0:
            print(f"  Failed: {step.enrichment_failed:,}")
        next_command = "python grin.py export-csv"

    else:
        # Fallback for unknown stage names
        stage_total, stage_successful, stage_failed = step.get_stage_totals()
        print(f"\n✓ {step_name.title()} completed {stage_total:,} items in {duration_str}")
        if stage_failed > 0:
            print(f"  Failed: {stage_failed:,}")
        next_command = None

    # Show next command if available
    if next_command:
        run_name = summary.run_name
        print(f"  Next: {next_command} --run-name {run_name}")

    print()  # Add blank line for spacing


def _display_sync_details(
    step: ProcessStageMetrics,
    was_interrupted: bool = False,
    session_books_synced: int = 0,
    session_sync_skipped: int = 0,
    session_sync_failed: int = 0,
    session_conversions_requested: int = 0,
) -> None:
    """Display detailed sync metrics for the sync stage focused on user outcomes."""
    # Display queue information
    if step.queue_info:
        queues = step.queue_info.get("queues", [])
        if queues:
            queue_str = ", ".join(queues)
            available = step.queue_info.get("available")
            if available:
                print(f"  Queue: {queue_str} ({available:,} available)")
            else:
                print(f"  Queue: {queue_str}")

        limit = step.queue_info.get("limit")
        specific_barcodes = step.queue_info.get("specific_barcodes")
        conversion_requests = step.queue_info.get("conversion_requests")

        if limit:
            print(f"  Limit: {limit:,}")
        elif specific_barcodes:
            print(f"  Specific barcodes: {specific_barcodes}")

        if conversion_requests:
            print(f"  Conversion requests: {conversion_requests:,}")

    # Display both cumulative totals and current session data
    # Show cumulative totals first
    print("  Cumulative totals:")
    if step.books_synced > 0:
        print(f"    ✓ Total synced: {step.books_synced:,}")
    if step.conversions_requested_during_sync > 0:
        print(f"    → Total sent for conversion: {step.conversions_requested_during_sync:,}")
    if step.sync_skipped > 0:
        print(f"    ⊘ Total skipped (already synced): {step.sync_skipped:,}")
    if step.sync_failed > 0:
        print(f"    ✗ Total failed: {step.sync_failed:,}")

    # Show current session data if there were any operations in this session
    session_total = session_books_synced + session_sync_skipped + session_sync_failed + session_conversions_requested

    if session_total > 0:
        print("  This session:")
        if session_books_synced > 0:
            print(f"    ✓ Successfully synced: {session_books_synced:,}")
        if session_conversions_requested > 0:
            print(f"    → Conversion requested: {session_conversions_requested:,}")
        if session_sync_skipped > 0:
            print(f"    ⊘ Skipped (already synced): {session_sync_skipped:,}")
        if session_sync_failed > 0:
            print(f"    ✗ Failed: {session_sync_failed:,}")

    # Additional metrics display could be added here if needed


def _format_bytes(bytes_count: int) -> str:
    """Format byte count as human-readable string."""
    if bytes_count < 1024:
        return f"{bytes_count} bytes"
    elif bytes_count < 1024 * 1024:
        return f"{bytes_count / 1024:.1f} KB"
    elif bytes_count < 1024 * 1024 * 1024:
        return f"{bytes_count / (1024 * 1024):.1f} MB"
    else:
        return f"{bytes_count / (1024 * 1024 * 1024):.1f} GB"
