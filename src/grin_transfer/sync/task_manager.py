#!/usr/bin/env python3
"""
Task Manager for Sync Pipeline

Task management system for handling download, decrypt, upload,
and post-processing tasks with dependency management.
"""

import asyncio
import logging
import time
from collections import defaultdict
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any, cast

from grin_transfer.common import Barcode, pluralize
from grin_transfer.process_summary import ProcessStageMetrics
from grin_transfer.sync.progress_reporter import SlidingWindowRateCalculator, show_queue_progress

from .db_updates import StatusUpdate, commit_book_record_updates, get_updates_for_task
from .tasks.task_types import (
    CheckTaskFunc,
    CleanupTaskFunc,
    DecryptTaskFunc,
    DownloadResult,
    DownloadTaskFunc,
    ExtractMarcTaskFunc,
    ExtractOcrTaskFunc,
    RequestConversionTaskFunc,
    TaskAction,
    TaskResult,
    TaskType,
    TrackConversionFailureTaskFunc,
    UnpackTaskFunc,
    UploadTaskFunc,
)

if TYPE_CHECKING:
    from grin_transfer.sync.pipeline import SyncPipeline


logger = logging.getLogger(__name__)


def _get_failure_details(task_results: dict[TaskType, TaskResult]) -> str:
    """Extract meaningful error details from task results.

    Args:
        task_results: Dict of task results for a book

    Returns:
        String describing which task(s) failed and why
    """
    failed_tasks = []

    # Collect all failed tasks
    for task_type, result in task_results.items():
        if result.action == TaskAction.FAILED:
            # Filter out CHECK 404 failures - they're just informational, not real failures
            if task_type == TaskType.CHECK and result.reason == "fail_archive_missing":
                continue

            task_name = task_type.name.lower().replace("_", " ")
            error_detail = result.error or "unknown error"
            failed_tasks.append(f"{task_name} ({error_detail})")

    # If no explicit failures, check for meaningful SKIPPED tasks that indicate permanent issues
    if not failed_tasks:
        for result in task_results.values():
            if result.action == TaskAction.SKIPPED and result.reason in [
                "skip_verified_unavailable",
                "skip_already_in_process",
            ]:
                # Check if we have the actual GRIN response in the data field
                if result.data and isinstance(result.data, dict) and "grin_response" in result.data:
                    return result.data["grin_response"]
                # Fall back to default message if no GRIN response available
                if result.reason == "skip_already_in_process":
                    return "Book is already being processed"
                return "Book not allowed to be downloaded by Google"

    if failed_tasks:
        return f"Failed tasks: {', '.join(failed_tasks)}"
    else:
        return "Book processing failed (no specific task failure found)"


class TaskManager:
    """
    Manages concurrent task execution with semaphores.
    """

    def __init__(self, limits: dict[TaskType, int]):
        """
        Initialize with concurrency limits per task type.

        Args:
            limits: Max concurrent tasks per type
        """
        self.limits = limits

        # Create semaphores for each task type
        self.semaphores = {task_type: asyncio.Semaphore(limit) for task_type, limit in self.limits.items()}

        # Track active tasks
        self.active_tasks: dict[str, set[TaskType]] = defaultdict(set)

        # Simple stats
        self.stats: dict[TaskType, dict[str, int]] = defaultdict(
            lambda: {"started": 0, "completed": 0, "skipped": 0, "failed": 0, "needs_conversion": 0}
        )

        # Track last warning time per task type for concurrency limit warnings
        self.last_warning: dict[TaskType, float] = {}

    async def run_task(
        self,
        task_type: TaskType,
        barcode: str,
        task_func: Callable[[], Coroutine[Any, Any, TaskResult]],
        pipeline: "SyncPipeline",
        previous_results: dict[TaskType, TaskResult],
    ) -> TaskResult:
        """
        Run a task with semaphore management and database updates.

        Args:
            task_type: Type of task
            barcode: Book barcode
            task_func: Async function that returns TaskResult
            pipeline: Pipeline instance for database updates
            previous_results: Results from previous tasks in the pipeline

        Returns:
            TaskResult from the task
        """
        # Check if semaphore is at capacity (before acquiring)
        # Skip warnings for tasks that don't have tunable concurrency or are intentionally rate-limited
        excluded_tasks = [
            TaskType.CHECK,
            TaskType.DOWNLOAD,  # GRIN API rate-limited
            TaskType.REQUEST_CONVERSION,  # One-off requests, not pipeline tasks
            TaskType.DATABASE_BACKUP,
            TaskType.DATABASE_UPLOAD,  # Infrastructure tasks
        ]
        if self.semaphores[task_type]._value == 0 and task_type not in excluded_tasks:
            now = time.time()
            if now - self.last_warning.get(task_type, 0) > 120:  # Warn every 2 minutes
                self.last_warning[task_type] = now
                logger.warning(f"Task concurrency limit reached: {task_type.name} (limit: {self.limits[task_type]})")
                cli_flag = f"--task-{task_type.name.lower().replace('_', '-')}-concurrency"
                print(
                    f"⚠️  {task_type.name} concurrency limit hit ({self.limits[task_type]}) — consider increasing with {cli_flag}"
                )

        async with self.semaphores[task_type]:
            self.active_tasks[barcode].add(task_type)
            self.stats[task_type]["started"] += 1

            result: TaskResult | None = None
            try:
                result = await task_func()

                # Update stats based on action
                match result.action:
                    case TaskAction.COMPLETED:
                        self.stats[task_type]["completed"] += 1
                    case TaskAction.SKIPPED:
                        self.stats[task_type]["skipped"] += 1
                    case TaskAction.FAILED:
                        # CHECK failures that route to recovery tasks count as needs_conversion
                        if task_type == TaskType.CHECK and result.reason in [
                            "fail_archive_missing",
                            "fail_known_conversion_failure",
                        ]:
                            self.stats[task_type]["needs_conversion"] += 1
                        else:
                            self.stats[task_type]["failed"] += 1

            except Exception as e:
                self.stats[task_type]["failed"] += 1
                error_msg = f"{type(e).__name__}: {e}"
                logger.error(f"[{barcode}] Task {task_type.name} failed: {error_msg}", exc_info=True)
                result = TaskResult(barcode=barcode, task_type=task_type, action=TaskAction.FAILED, error=error_msg)
            finally:
                # Always clean up active tasks tracking
                self.active_tasks[barcode].discard(task_type)
                if not self.active_tasks[barcode]:
                    del self.active_tasks[barcode]

            # Accumulate database updates
            updates = await get_updates_for_task(result, previous_results)

            # Initialize record updates for this barcode if needed
            if barcode not in pipeline.book_record_updates:
                pipeline.book_record_updates[barcode] = {"status_history": [], "books_fields": {}}

            # Accumulate status history record
            if updates.get("status"):
                status_type, status_value, metadata = updates["status"]
                status_record = StatusUpdate(barcode, status_type, status_value, metadata)
                pipeline.book_record_updates[barcode]["status_history"].append(status_record)

            # Accumulate books table field updates
            if updates.get("books"):
                pipeline.book_record_updates[barcode]["books_fields"].update(updates["books"])

            return result

    def get_active_task_count(self, task_type: TaskType | None = None) -> int:
        """Get count of active tasks."""
        if task_type is None:
            return sum(len(tasks) for tasks in self.active_tasks.values())
        return sum(1 for tasks in self.active_tasks.values() if task_type in tasks)

    def get_statistics(self) -> dict[str, Any]:
        """Get current task statistics."""
        return {
            "by_type": dict(self.stats),
            "active_total": self.get_active_task_count(),
            "active_by_type": {task_type: self.get_active_task_count(task_type) for task_type in TaskType},
        }


async def process_download_phase(
    manager: TaskManager, barcode: Barcode, pipeline: "SyncPipeline", task_funcs: dict[TaskType, Callable]
) -> dict[TaskType, TaskResult]:
    """
    Process the download phase (CHECK and DOWNLOAD tasks).

    Args:
        manager: Task manager for concurrency control
        barcode: Book barcode to process
        task_funcs: Dict mapping task types to their implementation functions

    Returns:
        Dict of download phase task results
    """
    results: dict[TaskType, TaskResult] = {}

    try:
        # Start with check (if provided)
        if TaskType.CHECK in task_funcs:
            check_func = cast(CheckTaskFunc, task_funcs[TaskType.CHECK])
            check_result = await manager.run_task(
                TaskType.CHECK,
                barcode,
                lambda: check_func(barcode, pipeline),
                pipeline,
                results,  # Pass accumulated results
            )
            results[TaskType.CHECK] = check_result

            # Check for next tasks (including recovery tasks)
            next_tasks = check_result.next_tasks()

            if TaskType.REQUEST_CONVERSION in next_tasks and TaskType.REQUEST_CONVERSION in task_funcs:
                request_func = cast(RequestConversionTaskFunc, task_funcs[TaskType.REQUEST_CONVERSION])
                request_result = await manager.run_task(
                    TaskType.REQUEST_CONVERSION,
                    barcode,
                    lambda: request_func(barcode, pipeline),
                    pipeline,
                    results,
                )
                results[TaskType.REQUEST_CONVERSION] = request_result
                return results  # End pipeline after conversion request

            if TaskType.TRACK_CONVERSION_FAILURE in next_tasks and TaskType.TRACK_CONVERSION_FAILURE in task_funcs:
                track_func = cast(TrackConversionFailureTaskFunc, task_funcs[TaskType.TRACK_CONVERSION_FAILURE])
                track_result = await manager.run_task(
                    TaskType.TRACK_CONVERSION_FAILURE,
                    barcode,
                    lambda: track_func(barcode, pipeline),
                    pipeline,
                    results,
                )
                results[TaskType.TRACK_CONVERSION_FAILURE] = track_result
                return results  # End pipeline after tracking failure

            if not check_result.should_continue_pipeline:
                return results

        # Download
        if TaskType.DOWNLOAD in task_funcs:
            download_func = cast(DownloadTaskFunc, task_funcs[TaskType.DOWNLOAD])
            download_result: DownloadResult = await manager.run_task(
                TaskType.DOWNLOAD,
                barcode,
                lambda: download_func(barcode, pipeline),
                pipeline,
                results,  # Pass accumulated results
            )
            results[TaskType.DOWNLOAD] = download_result
            if not download_result.should_continue_pipeline:
                return results
        else:
            # No download task, can't continue
            return results

    finally:
        # ALWAYS commit accumulated updates (success or failure)
        # Get persistent connection from db_tracker to avoid creating a new one
        conn = await pipeline.db_tracker.get_connection()
        await commit_book_record_updates(pipeline, barcode, conn)

    return results


async def process_processing_phase(
    manager: TaskManager,
    barcode: Barcode,
    download_results: dict[TaskType, TaskResult],
    pipeline: "SyncPipeline",
    task_funcs: dict[TaskType, Callable],
) -> dict[TaskType, TaskResult]:
    """
    Process the post-download phase (DECRYPT through CLEANUP tasks).

    Args:
        manager: Task manager for concurrency control
        barcode: Book barcode to process
        download_results: Results from the download phase
        pipeline: Pipeline instance
        task_funcs: Dict mapping task types to their implementation functions

    Returns:
        Dict of all task results for this book
    """
    # Start with download results
    results: dict[TaskType, TaskResult] = download_results.copy()

    # Get download result data for processing phase
    download_result = results.get(TaskType.DOWNLOAD)
    if not download_result or not download_result.should_continue_pipeline:
        return results

    try:
        # Decrypt
        download_data = download_result.data
        assert download_data is not None
        decrypt_func = cast(DecryptTaskFunc, task_funcs[TaskType.DECRYPT])
        decrypt_result = await manager.run_task(
            TaskType.DECRYPT,
            barcode,
            lambda: decrypt_func(barcode, download_data, pipeline),
            pipeline,
            results,  # Pass accumulated results
        )
        results[TaskType.DECRYPT] = decrypt_result

        if not decrypt_result.should_continue_pipeline:
            return results

        decrypt_data = decrypt_result.data
        assert decrypt_data is not None

        # Start upload independently (runs in parallel with everything else)
        upload_func = cast(UploadTaskFunc, task_funcs[TaskType.UPLOAD])

        upload_task = asyncio.create_task(
            manager.run_task(
                TaskType.UPLOAD,
                barcode,
                lambda: upload_func(barcode, download_data, decrypt_data, pipeline),
                pipeline,
                results,  # Pass accumulated results
            )
        )
        # Handle unpack and extractions if needed
        unpack_result = None
        if TaskType.UNPACK in task_funcs and (
            TaskType.EXTRACT_MARC in task_funcs or TaskType.EXTRACT_OCR in task_funcs
        ):
            # Await unpack directly (doesn't wait for upload)
            unpack_func = cast(UnpackTaskFunc, task_funcs[TaskType.UNPACK])

            unpack_result = await manager.run_task(
                TaskType.UNPACK,
                barcode,
                lambda: unpack_func(barcode, decrypt_data, pipeline),
                pipeline,
                results,  # Pass accumulated results
            )
            results[TaskType.UNPACK] = unpack_result

        # Run extractions if unpack succeeded
        if unpack_result and unpack_result.should_continue_pipeline:
            extraction_tasks = []

            unpack_data = unpack_result.data
            assert unpack_data is not None

            # MARC extraction
            if TaskType.EXTRACT_MARC in task_funcs:
                marc_func = cast(ExtractMarcTaskFunc, task_funcs[TaskType.EXTRACT_MARC])

                extraction_tasks.append(
                    manager.run_task(
                        TaskType.EXTRACT_MARC,
                        barcode,
                        lambda: marc_func(barcode, unpack_data, pipeline),
                        pipeline,
                        results,  # Pass accumulated results
                    )
                )

            # OCR extraction
            if TaskType.EXTRACT_OCR in task_funcs:
                ocr_func = cast(ExtractOcrTaskFunc, task_funcs[TaskType.EXTRACT_OCR])

                extraction_tasks.append(
                    manager.run_task(
                        TaskType.EXTRACT_OCR,
                        barcode,
                        lambda: ocr_func(barcode, unpack_data, pipeline),
                        pipeline,
                        results,  # Pass accumulated results
                    )
                )

            # Gather upload and extraction tasks in parallel
            parallel_tasks = [upload_task] + extraction_tasks

            parallel_results = await asyncio.gather(*parallel_tasks, return_exceptions=True)

            # Process results - first result is always upload_task
            upload_result = parallel_results[0]
            if isinstance(upload_result, TaskResult):
                results[TaskType.UPLOAD] = upload_result

            # Process extraction results
            for result in parallel_results[1:]:
                if isinstance(result, TaskResult):
                    results[result.task_type] = result
        else:
            # No extractions, just wait for upload
            upload_result = await upload_task
            results[TaskType.UPLOAD] = upload_result

        # Cleanup runs last, after everything else is done
        if TaskType.CLEANUP in task_funcs:
            cleanup_func = cast(CleanupTaskFunc, task_funcs[TaskType.CLEANUP])
            cleanup_result = await manager.run_task(
                TaskType.CLEANUP,
                barcode,
                lambda: cleanup_func(barcode, pipeline, results),
                pipeline,
                results,  # Pass accumulated results
            )
            results[TaskType.CLEANUP] = cleanup_result

    finally:
        # ALWAYS commit accumulated updates (success or failure)
        # Get persistent connection from db_tracker to avoid creating a new one
        conn = await pipeline.db_tracker.get_connection()
        await commit_book_record_updates(pipeline, barcode, conn)

    return results


async def process_books_with_queue(
    barcodes: list[str],
    pipeline: "SyncPipeline",
    task_funcs: dict[TaskType, Callable],
    task_manager: "TaskManager",
    rate_calculator: "SlidingWindowRateCalculator",
    workers: int = 20,
    progress_interval: int = 20,
) -> dict[str, dict[TaskType, TaskResult]]:
    """
    Process multiple books using two separate queues for optimal download saturation.

    Uses dedicated download workers to keep GRIN API saturated at 5 concurrent requests
    while processing workers handle post-download tasks independently.

    Args:
        barcodes: List of book barcodes to process
        pipeline: Pipeline instance for configuration
        task_funcs: Dict mapping task types to their implementation functions
        task_manager: TaskManager instance to use
        rate_calculator: Rate calculator for progress reporting
        workers: Number of concurrent worker tasks
        progress_interval: Show progress every N completed books

    Returns:
        Dict mapping barcode to its task results
    """
    manager = task_manager
    start_time = time.time()
    total_books = len(barcodes)

    # Create two separate queues for different phases
    download_queue: asyncio.Queue[str | None] = asyncio.Queue(maxsize=10)
    processing_queue: asyncio.Queue[tuple[str, dict[TaskType, TaskResult]] | None] = asyncio.Queue(maxsize=50)

    # Thread-safe counters for progress reporting
    completed_count = 0
    failure_limit_reached = False  # Flag to coordinate failure limit between workers and main loop
    results_dict = {}
    results_lock = asyncio.Lock()

    async def download_worker() -> None:
        """Worker that only handles CHECK and DOWNLOAD tasks."""
        while True:
            barcode = await download_queue.get()
            if barcode is None:  # Poison pill - time to exit
                download_queue.task_done()
                break

            try:
                # Process download phase only
                download_results = await process_download_phase(manager, barcode, pipeline, task_funcs)
                await processing_queue.put((barcode, download_results))

            except Exception as e:
                # Log error but don't crash the worker
                logger.error(f"[{barcode}] Download worker failed: {type(e).__name__}: {e}", exc_info=True)
                # Create failure result and send to processing queue
                error_msg = f"{type(e).__name__}: {e}"
                failure_result: TaskResult = TaskResult(
                    barcode=barcode, task_type=TaskType.DOWNLOAD, action=TaskAction.FAILED, error=error_msg
                )
                download_results = {TaskType.DOWNLOAD: failure_result}
                await processing_queue.put((barcode, download_results))

            finally:
                download_queue.task_done()

    async def processing_worker() -> None:
        """Worker that handles post-download processing tasks."""
        nonlocal completed_count, failure_limit_reached

        while True:
            item = await processing_queue.get()
            if item is None:  # Poison pill - time to exit
                processing_queue.task_done()
                break

            barcode, download_results = item

            try:
                # Check if download was successful
                download_result = download_results.get(TaskType.DOWNLOAD)
                if download_result and download_result.action == TaskAction.FAILED:
                    # Download failed, just store the failure result
                    final_results = download_results
                elif download_result and not download_result.should_continue_pipeline:
                    # Download succeeded but shouldn't continue (e.g., conversion requested)
                    final_results = download_results
                else:
                    # Download succeeded and should continue with processing
                    final_results = await process_processing_phase(
                        manager, barcode, download_results, pipeline, task_funcs
                    )

                # Store results thread-safely
                async with results_lock:
                    results_dict[barcode] = final_results
                    completed_count += 1
                    current_completed = completed_count

                    # Track sequential failures for pipeline exit logic
                    book_outcome = ProcessStageMetrics.determine_book_outcome(final_results)

                    if book_outcome == "failed":
                        error_details = _get_failure_details(final_results)
                        if pipeline._handle_failure(barcode, error_details):
                            # Stop feeding new books to this queue (but don't shutdown globally)
                            failure_limit_reached = True
                            logger.warning("Failure limit reached for current queue")
                    else:
                        # Reset failure counter on success (inline, no new function needed)
                        pipeline._sequential_failures = 0

                # Show progress periodically
                if current_completed % progress_interval == 0 or current_completed == total_books:
                    # Get active task counts for enhanced progress display
                    active_downloads = manager.get_active_task_count(TaskType.DOWNLOAD)
                    active_processing = manager.get_active_task_count() - active_downloads
                    download_limit = manager.limits[TaskType.DOWNLOAD]

                    show_queue_progress(
                        start_time,
                        total_books,
                        rate_calculator,
                        current_completed,
                        download_queue.qsize(),
                        processing_queue.qsize(),
                        {
                            "downloads": active_downloads,
                            "processing": active_processing,
                            "download_limit": download_limit,
                        },
                    )

            except Exception as e:
                # Log error but don't crash the worker
                logger.error(f"[{barcode}] Processing worker failed: {type(e).__name__}: {e}", exc_info=True)
                async with results_lock:
                    # Don't store failed results, just count as completed
                    completed_count += 1

            finally:
                processing_queue.task_done()

    # Create workers: use download task limit, rest to processing
    download_limit = manager.limits[TaskType.DOWNLOAD]
    download_workers = min(download_limit, workers)
    processing_workers = max(1, workers - download_workers)

    logger.info(
        f"Starting {download_workers} download workers and {processing_workers} processing workers for {total_books:,} books"
    )
    print(
        f"Starting {download_workers} download workers and {processing_workers} processing workers for {total_books:,} books...\n"
    )
    download_tasks = [asyncio.create_task(download_worker()) for _ in range(download_workers)]
    processing_tasks = [asyncio.create_task(processing_worker()) for _ in range(processing_workers)]

    try:
        # Feed download queue aggressively to keep it saturated
        barcode_iter = iter(barcodes)
        barcodes_exhausted = False

        while not barcodes_exhausted and not failure_limit_reached:
            # Try to fill the queue if there's space and we have more barcodes
            while (
                not barcodes_exhausted and not failure_limit_reached and download_queue.qsize() < download_queue.maxsize
            ):
                try:
                    barcode = next(barcode_iter)
                    if pipeline._shutdown_requested:
                        logger.info("Shutdown requested, stopping queue feeding")
                        print("\nGraceful shutdown in progress, only pending downloads will be synced...")
                        barcodes_exhausted = True
                        break
                    download_queue.put_nowait(barcode)
                except StopIteration:
                    barcodes_exhausted = True
                    break
                except asyncio.QueueFull:
                    break

            # If we still have barcodes but queue is full, wait a bit
            if not barcodes_exhausted and not failure_limit_reached:
                await asyncio.sleep(0.1)

        # Wait for all download queue items to be processed
        await download_queue.join()

        # Wait for all processing queue items to be processed
        await processing_queue.join()

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt caught during queue feeding, allowing graceful shutdown")
        raise

    finally:
        # Send poison pills to download workers
        for _ in range(download_workers):
            await download_queue.put(None)

        # Send poison pills to processing workers
        for _ in range(processing_workers):
            await processing_queue.put(None)

        # Wait for all workers to finish
        all_workers = download_tasks + processing_tasks
        await asyncio.gather(*all_workers, return_exceptions=True)

    # Log final stats with completion percentage
    completion_percentage = (len(results_dict) / total_books * 100) if total_books > 0 else 0
    logger.info(
        f"Final sync results: processed {len(results_dict)}/{total_books} books ({completion_percentage:.1f}% complete)"
    )

    # Log detailed task statistics
    for task_type in TaskType:
        stats = manager.stats[task_type]
        if stats["started"] > 0:
            task_name = task_type.name.lower().replace("_", " ")

            # Calculate success rate including completed + skipped + needs_conversion
            successful_count = stats["completed"] + stats["skipped"] + stats["needs_conversion"]
            success_rate = (successful_count / stats["started"] * 100) if stats["started"] > 0 else 0

            # Build log message with conditional needs_conversion field
            log_parts = [
                f"started={stats['started']}",
                f"completed={stats['completed']}",
                f"skipped={stats['skipped']}",
                f"failed={stats['failed']}",
            ]

            if stats["needs_conversion"] > 0:
                log_parts.insert(-1, f"needs_conversion={stats['needs_conversion']}")

            logger.info(f"Task '{task_name}': " + ", ".join(log_parts) + f" (success rate: {success_rate:.1f}%)")

    # Display formatted task statistics to console
    display_task_statistics(manager.stats)

    return results_dict


def display_task_statistics(stats: dict[TaskType, dict[str, int]]) -> None:
    """Display formatted task statistics to console with emphasis on failures."""
    if not any(task_stats["started"] > 0 for task_stats in stats.values()):
        return  # No tasks were started

    # Collect task data for display
    successful_tasks = []
    partial_failure_tasks = []
    complete_failure_tasks = []
    needs_conversion_tasks = []

    for task_type in TaskType:
        task_stats = stats[task_type]
        if task_stats["started"] == 0:
            continue

        task_name = task_type.name.lower().replace("_", " ")
        started = task_stats["started"]
        completed = task_stats["completed"]
        skipped = task_stats["skipped"]
        failed = task_stats["failed"]
        needs_conversion = task_stats["needs_conversion"]

        # Calculate success rate including completed + skipped + needs_conversion
        successful_count = completed + skipped + needs_conversion
        success_rate = (successful_count / started * 100) if started > 0 else 0

        task_info: dict[str, Any] = {
            "name": task_name,
            "completed": completed,
            "skipped": skipped,
            "started": started,
            "failed": failed,
            "needs_conversion": needs_conversion,
            "successful_count": successful_count,
            "success_rate": success_rate,
        }

        # Categorize based on true failures only (not including needs_conversion)
        if failed == 0:
            if needs_conversion > 0:
                needs_conversion_tasks.append(task_info)
            else:
                successful_tasks.append(task_info)
        elif successful_count == 0 and failed > 0:
            complete_failure_tasks.append(task_info)
        else:
            partial_failure_tasks.append(task_info)

    # Display header
    print(f"\n{'Task Statistics Summary'}")
    print("─" * 40)

    # Show critical failures first (most important)
    if complete_failure_tasks:
        for task in complete_failure_tasks:
            print(
                f"❌ CRITICAL: {task['name']:<15} {task['successful_count']}/{task['started']} ({task['success_rate']:5.1f}%) - ALL FAILED"
            )

    # Show partial failures next
    if partial_failure_tasks:
        for task in partial_failure_tasks:
            # Show detailed breakdown for partial failures
            success_parts = []
            if task["completed"] > 0:
                success_parts.append(f"{task['completed']} completed")
            if task["skipped"] > 0:
                success_parts.append(f"{task['skipped']} skipped")
            if task["needs_conversion"] > 0:
                verb = "needs" if task["needs_conversion"] == 1 else "need"
                success_parts.append(f"{task['needs_conversion']} {verb} conversion")

            success_detail = f" ({', '.join(success_parts)})" if success_parts else ""
            print(
                f"⚠️  WARNING:  {task['name']:<15} {task['successful_count']}/{task['started']} ({task['success_rate']:5.1f}%){success_detail} - {task['failed']} failed"
            )

    # Show needs conversion tasks
    if needs_conversion_tasks:
        for task in needs_conversion_tasks:
            verb = "needs" if task["needs_conversion"] == 1 else "need"
            conversion_detail = (
                f" ({task['needs_conversion']} {pluralize(task['needs_conversion'], 'book')} {verb} conversion)"
            )
            print(
                f"→ {task['name']:<20} {task['successful_count']}/{task['started']} ({task['success_rate']:5.1f}%){conversion_detail}"
            )

    # Show successful tasks last
    if successful_tasks:
        for task in successful_tasks:
            # Show breakdown of completed vs skipped for successful tasks
            if task["skipped"] > 0:
                detail = f" ({task['completed']} completed, {task['skipped']} skipped)"
            else:
                detail = ""
            print(
                f"✓ {task['name']:<20} {task['successful_count']}/{task['started']} ({task['success_rate']:5.1f}%){detail}"
            )

    # Add critical failure summary if any tasks completely failed
    if complete_failure_tasks:
        print("\n⚠️  Critical failures detected:")
        for task in complete_failure_tasks:
            print(f"   • {task['name']}: All {task['started']} attempts failed")
        print("   Consider checking task configuration and logs for details")

    print()  # Add spacing after the summary
