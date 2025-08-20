#!/usr/bin/env python3
"""
Task Manager for Sync Pipeline

Task management system for handling download, decrypt, upload,
and post-processing tasks with dependency management.
"""

import asyncio
import logging
from collections import defaultdict
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any, NamedTuple, cast

from grin_to_s3.common import Barcode
from grin_to_s3.database.database_utils import batch_write_status_updates

from .db_updates import get_updates_for_task
from .tasks.task_types import (
    CheckTaskFunc,
    CleanupTaskFunc,
    DecryptTaskFunc,
    DownloadResult,
    DownloadTaskFunc,
    ExportCsvTaskFunc,
    ExtractMarcTaskFunc,
    ExtractOcrTaskFunc,
    RequestConversionTaskFunc,
    TaskAction,
    TaskResult,
    TaskType,
    UnpackTaskFunc,
    UploadTaskFunc,
)


class StatusUpdate(NamedTuple):
    """Status update tuple for collecting updates before writing."""

    barcode: str
    status_type: str
    status_value: str
    metadata: dict | None = None
    session_id: str | None = None


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


async def commit_book_record_updates(pipeline: "SyncPipeline", barcode: str):
    """Commit all accumulated database record updates for a book."""
    record_updates = pipeline.book_record_updates.get(barcode)
    if not record_updates:
        return

    # Write all status history records (batch_write_status_updates handles its own transaction)
    if record_updates["status_history"]:
        await batch_write_status_updates(str(pipeline.db_tracker.db_path), record_updates["status_history"])

    # Update books table fields (update_sync_data handles its own transaction)
    if record_updates["books_fields"]:
        await pipeline.db_tracker.update_sync_data(barcode, record_updates["books_fields"])

    # Clean up after commit
    del pipeline.book_record_updates[barcode]


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

            # CSV export
            if TaskType.EXPORT_CSV in task_funcs:
                csv_func = cast(ExportCsvTaskFunc, task_funcs[TaskType.EXPORT_CSV])

                extraction_tasks.append(
                    manager.run_task(
                        TaskType.EXPORT_CSV,
                        barcode,
                        lambda: csv_func(barcode, pipeline),
                        pipeline,
                        results,  # Pass accumulated results
                    )
                )

            # Wait for extractions
            if extraction_tasks:
                extraction_results = await asyncio.gather(*extraction_tasks, return_exceptions=True)
                for result in extraction_results:
                    if isinstance(result, TaskResult):
                        results[result.task_type] = result

        # Collect upload result before cleanup
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

    return results


async def process_books_batch(
    barcodes: list[str],
    pipeline: "SyncPipeline",
    task_funcs: dict[TaskType, Callable],
    task_manager: "TaskManager",
) -> dict[str, dict[TaskType, TaskResult]]:
    """
    Process multiple books with task-level concurrency control.

    Args:
        barcodes: List of book barcodes to process
        task_funcs: Dict mapping task types to their implementation functions
        task_manager: TaskManager instance to use

    Returns:
        Dict mapping barcode to its task results
    """
    manager = task_manager

    async def process_book(barcode: str):
        return barcode, await process_book_pipeline(manager, barcode, pipeline, task_funcs)

    # Process books in chunks using pipeline.batch_size to control memory usage
    results = []
    chunk_size = pipeline.batch_size

    for i in range(0, len(barcodes), chunk_size):
        # Check for shutdown between chunks for responsive cancellation
        if pipeline._shutdown_requested:
            logger.info("Shutdown requested, stopping batch processing")
            print("\nGraceful shutdown in progress, no new books will be processed...")
            break

        chunk = barcodes[i:i + chunk_size]
        chunk_num = i // chunk_size + 1
        total_chunks = (len(barcodes) + chunk_size - 1) // chunk_size
        logger.info(f"Processing chunk {chunk_num}/{total_chunks}: {len(chunk)} books")

        # Create tasks only for this chunk
        chunk_tasks = [process_book(barcode) for barcode in chunk]

        try:
            chunk_results = await asyncio.gather(*chunk_tasks, return_exceptions=True)
            results.extend(chunk_results)
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt caught during chunk processing, allowing graceful shutdown")
            raise

    # Convert to dict
    book_results = {}
    for result in results:
        if isinstance(result, tuple):
            barcode, task_results = result
            book_results[barcode] = task_results
        elif isinstance(result, KeyboardInterrupt):
            # KeyboardInterrupt from individual tasks should also propagate up
            logger.info("KeyboardInterrupt from individual task, allowing graceful shutdown")
            raise result
        elif isinstance(result, Exception):
            logger.error(f"Book processing failed: {type(result).__name__}: {result}", exc_info=result)

    # Log final stats
    logger.info(f"Processed {len(book_results)}/{len(barcodes)} books")
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

    return book_results
