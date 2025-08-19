#!/usr/bin/env python3
"""Database update handlers for sync pipeline tasks."""

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from grin_to_s3.sync.tasks.task_types import TaskAction, TaskResult, TaskType

logger = logging.getLogger(__name__)

# Registry of update handlers
UPDATE_HANDLERS: dict[tuple[TaskType, TaskAction], list] = {}


def on(task_type: TaskType, action: TaskAction, status_type: str = "sync", status_value: str | None = None) -> Callable[[Callable], Callable]:
    """Decorator to register database update handlers.

    Args:
        task_type: Task type to handle
        action: Action to handle
        status_type: Status type for the update (default: "sync")
        status_value: Status value (default: action.value)
    """
    if status_value is None:
        # Default status_value based on task and action
        if action == TaskAction.COMPLETED:
            status_value = task_type.name.lower().replace("_", "-")
        else:
            status_value = action.value

    def decorator(func):
        key = (task_type, action)
        if key in UPDATE_HANDLERS:
            existing_handler = UPDATE_HANDLERS[key][0][0]
            raise ValueError(
                f"Handler for {task_type.name}+{action.value} already exists: {existing_handler.__name__}. "
                f"Cannot register duplicate handler: {func.__name__}"
            )
        UPDATE_HANDLERS[key] = [(func, status_type, status_value)]
        return func

    return decorator


def _log_status_update(
    barcode: str, task_type: TaskType, action: TaskAction, status_type: str, status_value: str, metadata: dict | None
):
    """Generate consistent log message from status update data."""
    # Build structured log parts
    parts = [f"task={task_type.name}", f"action={action.value}", f"status={status_type}:{status_value}"]

    # Add metadata if present
    if metadata:
        for key, value in metadata.items():
            if value is not None:
                # Truncate long values for readability
                value_str = str(value)
                if len(value_str) > 100:
                    value_str = value_str[:97] + "..."
                parts.append(f"{key}={value_str}")

    logger.debug(f"[{barcode}] DB_UPDATE: {' | '.join(parts)}")


# Pure functional handlers - only return data, no side effects, no logging
@on(TaskType.CHECK, TaskAction.SKIPPED)
async def check_skipped(result: TaskResult, previous_results: dict[TaskType, TaskResult]) -> dict[str, Any]:
    return {"status": ("sync", "skipped", {"reason": result.reason} if result.reason else None), "books": {}}


@on(TaskType.CHECK, TaskAction.COMPLETED, status_value="checked")
async def check_completed(result: TaskResult, previous_results: dict[TaskType, TaskResult]):
    etag = result.data.get("etag") if result.data else None
    return {"status": ("sync", "checked", {"etag": etag} if etag else None), "books": {}}


@on(TaskType.DOWNLOAD, TaskAction.COMPLETED, status_value="downloading")
async def download_completed(result: TaskResult, previous_results: dict[TaskType, TaskResult]):
    etag = result.data.get("etag") if result.data else None
    return {
        "status": ("sync", "downloading", {"etag": etag} if etag else None),
        "books": {"encrypted_etag": etag} if etag else {},
    }


@on(TaskType.DOWNLOAD, TaskAction.FAILED, status_value="download_failed")
async def download_failed(result: TaskResult, previous_results: dict[TaskType, TaskResult]):
    return {"status": ("sync", "download_failed", {"error": result.error} if result.error else None), "books": {}}


@on(TaskType.DECRYPT, TaskAction.COMPLETED, status_value="decrypted")
async def decrypt_completed(result: TaskResult, previous_results: dict[TaskType, TaskResult]):
    return {"status": ("sync", "decrypted", None), "books": {}}


@on(TaskType.DECRYPT, TaskAction.FAILED, status_value="decrypt_failed")
async def decrypt_failed(result: TaskResult, previous_results: dict[TaskType, TaskResult]):
    return {"status": ("sync", "decrypt_failed", {"error": result.error} if result.error else None), "books": {}}


@on(TaskType.UNPACK, TaskAction.COMPLETED, status_value="unpacked")
async def unpack_completed(result: TaskResult, previous_results: dict[TaskType, TaskResult]):
    return {"status": ("sync", "unpacked", None), "books": {}}


@on(TaskType.UNPACK, TaskAction.FAILED, status_value="unpack_failed")
async def unpack_failed(result: TaskResult, previous_results: dict[TaskType, TaskResult]):
    return {"status": ("sync", "unpack_failed", {"error": result.error} if result.error else None), "books": {}}


@on(TaskType.UPLOAD, TaskAction.COMPLETED, status_value="uploaded")
async def upload_completed(result: TaskResult, previous_results: dict[TaskType, TaskResult]):
    path = str(result.data.get("upload_path")) if result.data else None

    # Get etag from download result in chain
    download_result = previous_results.get(TaskType.DOWNLOAD)
    etag = download_result.data.get("etag") if download_result and download_result.data else None

    # Get storage type from upload result data (set by upload task)
    storage_type = result.data.get("storage_type") if result.data else None

    books_updates = {
        "storage_path": path,
        "is_decrypted": True,
        "sync_timestamp": datetime.now(UTC).isoformat(),
    }
    if etag:
        books_updates["encrypted_etag"] = etag
    if storage_type:
        books_updates["storage_type"] = storage_type

    return {"status": ("sync", "uploaded", {"path": path} if path else None), "books": books_updates}


@on(TaskType.UPLOAD, TaskAction.FAILED, status_value="upload_failed")
async def upload_failed(result: TaskResult, previous_results: dict[TaskType, TaskResult]):
    # Still preserve etag from download even on upload failure
    download_result = previous_results.get(TaskType.DOWNLOAD)
    etag = download_result.data.get("etag") if download_result and download_result.data else None

    return {
        "status": ("sync", "upload_failed", {"error": result.error} if result.error else None),
        "books": {"encrypted_etag": etag} if etag else {},
    }


@on(TaskType.EXTRACT_OCR, TaskAction.COMPLETED, "text_extraction", "completed")
async def extract_ocr_completed(result: TaskResult, previous_results: dict[TaskType, TaskResult]):
    metadata = None
    if result.data:
        metadata = {
            "page_count": result.data.get("page_count"),
            "extraction_time_ms": result.data.get("extraction_time_ms"),
        }
    return {"status": ("text_extraction", "completed", metadata), "books": {}}


@on(TaskType.EXTRACT_OCR, TaskAction.FAILED, "text_extraction", "failed")
async def extract_ocr_failed(result: TaskResult, previous_results: dict[TaskType, TaskResult]):
    return {"status": ("text_extraction", "failed", {"error": result.error} if result.error else None), "books": {}}


@on(TaskType.EXTRACT_MARC, TaskAction.COMPLETED, "marc_extraction", "completed")
async def extract_marc_completed(result: TaskResult, previous_results: dict[TaskType, TaskResult]):
    metadata = None
    if result.data:
        metadata = {"field_count": result.data.get("field_count")}
    return {"status": ("marc_extraction", "completed", metadata), "books": {}}


@on(TaskType.EXTRACT_MARC, TaskAction.FAILED, "marc_extraction", "failed")
async def extract_marc_failed(result: TaskResult, previous_results: dict[TaskType, TaskResult]):
    return {"status": ("marc_extraction", "failed", {"error": result.error} if result.error else None), "books": {}}


@on(TaskType.EXPORT_CSV, TaskAction.COMPLETED, "export", "csv_updated")
async def export_csv_completed(result: TaskResult, previous_results: dict[TaskType, TaskResult]):
    return {"status": ("export", "csv_updated", None), "books": {}}


@on(TaskType.CLEANUP, TaskAction.COMPLETED, status_value="completed")
async def cleanup_completed(result: TaskResult, previous_results: dict[TaskType, TaskResult]):
    return {"status": ("sync", "completed", None), "books": {}}


async def get_updates_for_task(result: TaskResult, previous_results: dict[TaskType, TaskResult]) -> dict[str, Any]:
    """Get database updates from registered handlers."""
    handlers = UPDATE_HANDLERS.get((result.task_type, result.action))

    if not handlers:
        return {"status": None, "books": {}}

    # Get the single handler (exactly one per task/action combo)
    handler_func, status_type, status_value = handlers[0]

    # Call handler to get standardized updates
    updates = await handler_func(result, previous_results)

    # Log the status update
    if updates.get("status"):
        status_type_val, status_value_val, metadata = updates["status"]
        _log_status_update(result.barcode, result.task_type, result.action, status_type_val, status_value_val, metadata)

    return updates
