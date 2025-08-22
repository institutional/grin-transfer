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

from grin_to_s3.common import Barcode
from grin_to_s3.sync.progress_reporter import SlidingWindowRateCalculator, show_queue_progress

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
    UnpackTaskFunc,
    UploadTaskFunc,
)

if TYPE_CHECKING:
    from grin_to_s3.sync.pipeline import SyncPipeline


logger = logging.getLogger(__name__)


class TaskManager:
    """
    Manages concurrent task execution with semaphores.
    """

    def __init__(self, limits: dict[TaskType, int] | None = None):
        """
        Initialize with concurrency limits per task type.

        Args:
            limits: Max concurrent tasks per type
        """
        self.limits = limits or {
            # CHECK tasks use shared GRIN request semaphore (no separate limit)
            TaskType.DOWNLOAD: 5,
            TaskType.DECRYPT: 3,
            TaskType.UNPACK: 3,
            TaskType.UPLOAD: 10,
            TaskType.EXTRACT_MARC: 5,
            TaskType.EXTRACT_OCR: 5,
            TaskType.CLEANUP: 2,
        }

        # Create semaphores for each task type
        self.semaphores = {task_type: asyncio.Semaphore(limit) for task_type, limit in self.limits.items()}

        # Shared semaphore for GRIN requests (check + download tasks)
        # Both tasks hit the same endpoint so must share the 5-request limit
        self.grin_api_semaphore = asyncio.Semaphore(5)

        # CHECK and DOWNLOAD tasks use the shared GRIN API semaphore
        self.semaphores[TaskType.CHECK] = self.grin_api_semaphore
        self.semaphores[TaskType.DOWNLOAD] = self.grin_api_semaphore

        # Track active tasks
        self.active_tasks: dict[str, set[TaskType]] = defaultdict(set)

        # Simple stats
        self.stats: dict[TaskType, dict[str, int]] = defaultdict(
            lambda: {"started": 0, "completed": 0, "skipped": 0, "failed": 0}
        )

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


async def process_book_pipeline(
    manager: TaskManager, barcode: Barcode, pipeline: "SyncPipeline", task_funcs: dict[TaskType, Callable]
) -> dict[TaskType, TaskResult]:
    """
    Process a single book through the entire pipeline.

    Args:
        manager: Task manager for concurrency control
        barcode: Book barcode to process
        task_funcs: Dict mapping task types to their implementation functions

    Returns:
        Dict of all task results for this book
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
        await commit_book_record_updates(pipeline, barcode)

    # Update process summary metrics
    outcome = pipeline.process_summary_stage.determine_book_outcome(results)
    pipeline.process_summary_stage.increment_by_outcome(outcome)

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
    Process multiple books using a feed-forward queue for continuous streaming.

    This eliminates dead time by allowing new downloads to start while uploads
    from previous books are still running. Uses bounded memory via a small queue.

    Args:
        barcodes: List of book barcodes to process
        pipeline: Pipeline instance for configuration
        task_funcs: Dict mapping task types to their implementation functions
        task_manager: TaskManager instance to use
        rate_calculator: Rate calculator for progress reporting
        workers: Number of concurrent worker tasks (default 20)
        progress_interval: Show progress every N completed books (default 20)

    Returns:
        Dict mapping barcode to its task results
    """
    manager = task_manager
    start_time = time.time()
    total_books = len(barcodes)

    # Create bounded queue to prevent unbounded task creation
    queue: asyncio.Queue[str | None] = asyncio.Queue(maxsize=100)

    # Thread-safe counters for progress reporting
    completed_count = 0
    results_dict = {}
    results_lock = asyncio.Lock()

    async def worker() -> None:
        """Worker that processes books from the queue until poison pill received."""
        nonlocal completed_count

        while True:
            barcode = await queue.get()
            if barcode is None:  # Poison pill - time to exit
                queue.task_done()
                break

            try:
                # Process this book through the pipeline
                task_results = await process_book_pipeline(manager, barcode, pipeline, task_funcs)

                # Store results thread-safely
                async with results_lock:
                    results_dict[barcode] = task_results
                    completed_count += 1
                    current_completed = completed_count

                # Show progress periodically
                if current_completed % progress_interval == 0 or current_completed == total_books:
                    # Get active task counts for enhanced progress display
                    active_downloads = manager.get_active_task_count(TaskType.CHECK) + manager.get_active_task_count(TaskType.DOWNLOAD)
                    active_processing = manager.get_active_task_count() - active_downloads
                    download_limit = manager.limits.get(TaskType.DOWNLOAD, 5)

                    show_queue_progress(
                        start_time, total_books, rate_calculator, current_completed, queue.qsize(),
                        {"downloads": active_downloads, "processing": active_processing, "download_limit": download_limit}
                    )

            except Exception as e:
                # Log error but don't crash the worker
                logger.error(f"[{barcode}] Worker failed processing book: {type(e).__name__}: {e}", exc_info=True)
                async with results_lock:
                    completed_count += 1

            finally:
                queue.task_done()

    # Start fixed number of worker tasks
    logger.info(f"Starting {workers} workers for {total_books:,} books")
    worker_tasks = [asyncio.create_task(worker()) for _ in range(workers)]

    try:
        # Feed queue incrementally (blocks when full for backpressure)
        for barcode in barcodes:
            # Check for shutdown before adding each book
            if pipeline._shutdown_requested:
                logger.info("Shutdown requested, stopping queue feeding")
                print("\nGraceful shutdown in progress, no new books will be processed...")
                break

            await queue.put(barcode)

        # Wait for all queued items to be processed
        await queue.join()

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt caught during queue feeding, allowing graceful shutdown")
        raise

    finally:
        # Send poison pills to all workers
        for _ in worker_tasks:
            await queue.put(None)

        # Wait for all workers to finish
        await asyncio.gather(*worker_tasks, return_exceptions=True)

    # Log final stats
    logger.info(f"Processed {len(results_dict)}/{total_books} books")
    for task_type in TaskType:
        stats = manager.stats[task_type]
        if stats["started"] > 0:
            logger.info(
                f"{task_type.name}: "
                f"started={stats['started']}, "
                f"completed={stats['completed']}, "
                f"skipped={stats['skipped']}, "
                f"failed={stats['failed']}"
            )

    return results_dict
