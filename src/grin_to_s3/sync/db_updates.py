#!/usr/bin/env python3
"""Database update handlers for sync pipeline tasks."""

import logging
from datetime import UTC, datetime
from typing import Any

from grin_to_s3.database_utils import batch_write_status_updates
from grin_to_s3.extract.tracking import collect_status
from grin_to_s3.sync.tasks.task_types import TaskAction, TaskResult, TaskType

logger = logging.getLogger(__name__)

# Registry of update handlers
UPDATE_HANDLERS: dict[tuple[TaskType, TaskAction], list] = {}


def on(task_type: TaskType, action: TaskAction, status_type: str = "sync", status_value: str = None):
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
        if key not in UPDATE_HANDLERS:
            UPDATE_HANDLERS[key] = []
        UPDATE_HANDLERS[key].append((func, status_type, status_value))
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
async def check_skipped(result: TaskResult, pipeline_data: dict[str, Any]):
    return {"metadata": {"reason": result.reason} if result.reason else None}


@on(TaskType.CHECK, TaskAction.COMPLETED, status_value="checked")
async def check_completed(result: TaskResult, pipeline_data: dict[str, Any]):
    etag = result.data.get("etag") if result.data else None
    return {"metadata": {"etag": etag} if etag else None, "etag_to_store": etag}


@on(TaskType.DOWNLOAD, TaskAction.COMPLETED, status_value="downloading")
async def download_completed(result: TaskResult, pipeline_data: dict[str, Any]):
    return {"metadata": None}


@on(TaskType.DOWNLOAD, TaskAction.FAILED, status_value="download_failed")
async def download_failed(result: TaskResult, pipeline_data: dict[str, Any]):
    return {"metadata": {"error": result.error} if result.error else None}


@on(TaskType.DECRYPT, TaskAction.COMPLETED, status_value="decrypted")
async def decrypt_completed(result: TaskResult, pipeline_data: dict[str, Any]):
    return {"metadata": None}


@on(TaskType.DECRYPT, TaskAction.FAILED, status_value="decrypt_failed")
async def decrypt_failed(result: TaskResult, pipeline_data: dict[str, Any]):
    return {"metadata": {"error": result.error} if result.error else None}


@on(TaskType.UNPACK, TaskAction.COMPLETED, status_value="unpacked")
async def unpack_completed(result: TaskResult, pipeline_data: dict[str, Any]):
    return {"metadata": None}


@on(TaskType.UNPACK, TaskAction.FAILED, status_value="unpack_failed")
async def unpack_failed(result: TaskResult, pipeline_data: dict[str, Any]):
    return {"metadata": {"error": result.error} if result.error else None}


@on(TaskType.UPLOAD, TaskAction.COMPLETED, status_value="uploaded")
async def upload_completed(result: TaskResult, pipeline_data: dict[str, Any]):
    path = str(result.data.get("upload_path")) if result.data else None

    # Build sync_data for books table update
    sync_data = None
    if result.data:
        sync_data = {
            "storage_type": pipeline_data.get("storage_protocol"),
            "storage_path": path,
            "is_decrypted": True,
            "sync_timestamp": datetime.now(UTC).isoformat(),
        }
        # Include stored etag if available
        stored_etag = pipeline_data.get("etags", {}).get(result.barcode)
        if stored_etag:
            sync_data["encrypted_etag"] = stored_etag

    return {"metadata": {"path": path} if path else None, "sync_data": sync_data}


@on(TaskType.UPLOAD, TaskAction.FAILED, status_value="upload_failed")
async def upload_failed(result: TaskResult, pipeline_data: dict[str, Any]):
    return {"metadata": {"error": result.error} if result.error else None}


@on(TaskType.EXTRACT_OCR, TaskAction.COMPLETED, "text_extraction", "completed")
async def extract_ocr_completed(result: TaskResult, pipeline_data: dict[str, Any]):
    if result.data:
        return {
            "metadata": {
                "page_count": result.data.get("page_count"),
                "extraction_time_ms": result.data.get("extraction_time_ms"),
            }
        }
    return {"metadata": None}


@on(TaskType.EXTRACT_OCR, TaskAction.FAILED, "text_extraction", "failed")
async def extract_ocr_failed(result: TaskResult, pipeline_data: dict[str, Any]):
    return {"metadata": {"error": result.error} if result.error else None}


@on(TaskType.EXTRACT_MARC, TaskAction.COMPLETED, "marc_extraction", "completed")
async def extract_marc_completed(result: TaskResult, pipeline_data: dict[str, Any]):
    if result.data:
        return {"metadata": {"field_count": result.data.get("field_count")}}
    return {"metadata": None}


@on(TaskType.EXTRACT_MARC, TaskAction.FAILED, "marc_extraction", "failed")
async def extract_marc_failed(result: TaskResult, pipeline_data: dict[str, Any]):
    return {"metadata": {"error": result.error} if result.error else None}


@on(TaskType.EXPORT_CSV, TaskAction.COMPLETED, "export", "csv_updated")
async def export_csv_completed(result: TaskResult, pipeline_data: dict[str, Any]):
    return {"metadata": None}


@on(TaskType.CLEANUP, TaskAction.COMPLETED, status_value="completed")
async def cleanup_completed(result: TaskResult, pipeline_data: dict[str, Any]):
    return {"metadata": None}


async def update_database_for_task(result: TaskResult, pipeline):
    """Update database using registered handlers."""
    handlers = UPDATE_HANDLERS.get((result.task_type, result.action), [])

    # Prepare read-only pipeline data for handlers
    pipeline_data = {
        "storage_protocol": pipeline.config.storage_config["protocol"],
        "etags": getattr(pipeline, "current_etags", {}),
    }

    all_updates = []
    for handler_func, status_type, status_value in handlers:
        # Call handler to get data
        handler_result = await handler_func(result, pipeline_data)

        # Create status update
        metadata = handler_result.get("metadata")
        all_updates.append(collect_status(result.barcode, status_type, status_value, metadata))

        # Generate log from the data
        _log_status_update(result.barcode, result.task_type, result.action, status_type, status_value, metadata)

        if "sync_data" in handler_result and handler_result["sync_data"]:
            await pipeline.db_tracker.update_sync_data(result.barcode, handler_result["sync_data"])
            logger.debug(f"[{result.barcode}] Updated sync_data in books table")

    if all_updates:
        await batch_write_status_updates(str(pipeline.db_tracker.db_path), all_updates)
