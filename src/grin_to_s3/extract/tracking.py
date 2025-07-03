"""
Simple database tracking for OCR text extraction operations.

Just writes status rows to book_status_history table when extraction events occur.
"""

import json
import logging
from datetime import UTC, datetime
from enum import Enum

from ..database import connect_async

logger = logging.getLogger(__name__)

# Status type for text extraction operations
TEXT_EXTRACTION_STATUS_TYPE = "text_extraction"


# Status values for text extraction
class ExtractionStatus(Enum):
    STARTING = "starting"
    EXTRACTING = "extracting"
    COMPLETED = "completed"
    FAILED = "failed"


# Extraction method types
class ExtractionMethod(Enum):
    DISK = "disk"
    MEMORY = "memory"
    STREAMING = "streaming"


async def write_status(
    db_path: str, barcode: str, status: ExtractionStatus, metadata: dict | None = None, session_id: str | None = None
) -> None:
    """Write a status row to the database."""
    try:
        async with connect_async(db_path) as conn:
            await conn.execute(
                """INSERT INTO book_status_history
                   (barcode, status_type, status_value, timestamp, session_id, metadata)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    barcode,
                    TEXT_EXTRACTION_STATUS_TYPE,
                    status.value,
                    datetime.now(UTC).isoformat(),
                    session_id,
                    json.dumps(metadata) if metadata else None,
                ),
            )
            await conn.commit()
    except Exception as e:
        logger.warning(f"⚠️ Failed to write extraction status for {barcode}: {e}")


async def track_start(db_path: str, barcode: str, session_id: str | None = None) -> None:
    """Track extraction start."""
    metadata: dict[str, str] = {
        "started_at": datetime.now(UTC).isoformat(),
        "extraction_stage": "initialization",
    }
    await write_status(db_path, barcode, ExtractionStatus.STARTING, metadata, session_id)


async def track_progress(db_path: str, barcode: str, page_count: int, session_id: str | None = None) -> None:
    """Track extraction progress."""
    metadata: dict[str, str | int] = {
        "page_count": page_count,
        "extraction_stage": "processing_pages",
        "progress_at": datetime.now(UTC).isoformat(),
    }
    await write_status(db_path, barcode, ExtractionStatus.EXTRACTING, metadata, session_id)


async def track_completion(
    db_path: str,
    barcode: str,
    page_count: int,
    extraction_time_ms: int,
    method: ExtractionMethod,
    session_id: str | None = None,
    file_size: int = 0,
    output_path: str = "",
) -> None:
    """Track successful extraction completion."""
    metadata: dict[str, str | int] = {
        "page_count": page_count,
        "extraction_time_ms": extraction_time_ms,
        "extraction_method": method.value,
        "completed_at": datetime.now(UTC).isoformat(),
    }
    if file_size:
        metadata["jsonl_file_size"] = file_size
    if output_path:
        metadata["output_path"] = output_path

    await write_status(db_path, barcode, ExtractionStatus.COMPLETED, metadata, session_id)


async def track_failure(
    db_path: str,
    barcode: str,
    error: Exception,
    method: ExtractionMethod,
    session_id: str | None = None,
    partial_page_count: int = 0,
) -> None:
    """Track extraction failure."""
    metadata: dict[str, str | int] = {
        "error_type": type(error).__name__,
        "error_message": str(error),
        "extraction_method": method.value,
        "failed_at": datetime.now(UTC).isoformat(),
        "partial_page_count": partial_page_count,
    }

    await write_status(db_path, barcode, ExtractionStatus.FAILED, metadata, session_id)


# Query functions for monitoring
async def get_status_summary(db_path: str) -> dict[str, int]:
    """Get summary of extraction statuses."""
    try:
        async with connect_async(db_path) as conn:
            cursor = await conn.execute(
                """SELECT status_value, COUNT(*) as count
                   FROM book_status_history
                   WHERE status_type = ?
                   GROUP BY status_value""",
                (TEXT_EXTRACTION_STATUS_TYPE,),
            )
            results = await cursor.fetchall()

            summary = {
                ExtractionStatus.STARTING.value: 0,
                ExtractionStatus.EXTRACTING.value: 0,
                ExtractionStatus.COMPLETED.value: 0,
                ExtractionStatus.FAILED.value: 0,
            }

            for status_value, count in results:
                if status_value in summary:
                    summary[status_value] = count

            summary["total"] = sum(summary.values())
            return summary
    except Exception as e:
        logger.error(f"⚠️ Failed to get status summary: {e}")
        return {"total": 0}


async def get_failed_extractions(db_path: str, limit: int = 100) -> list[dict]:
    """Get details of recent extraction failures."""
    try:
        async with connect_async(db_path) as conn:
            cursor = await conn.execute(
                """SELECT barcode, timestamp, metadata
                   FROM book_status_history
                   WHERE status_type = ? AND status_value = ?
                   ORDER BY timestamp DESC
                   LIMIT ?""",
                (TEXT_EXTRACTION_STATUS_TYPE, ExtractionStatus.FAILED.value, limit),
            )
            results = await cursor.fetchall()

            failures = []
            for barcode, timestamp, metadata_json in results:
                metadata = json.loads(metadata_json) if metadata_json else {}
                failures.append(
                    {
                        "barcode": barcode,
                        "timestamp": timestamp,
                        "error_type": metadata.get("error_type"),
                        "error_message": metadata.get("error_message"),
                        "extraction_method": metadata.get("extraction_method"),
                        "partial_page_count": metadata.get("partial_page_count", 0),
                    }
                )
            return failures
    except Exception as e:
        logger.error(f"⚠️ Failed to get failed extractions: {e}")
        return []


async def get_extraction_progress(db_path: str) -> dict:
    """Get overall extraction progress statistics."""
    try:
        status_summary = await get_status_summary(db_path)
        recent_failures = await get_failed_extractions(db_path, limit=10)

        # Get aggregated statistics for completed extractions
        async with connect_async(db_path) as conn:
            cursor = await conn.execute(
                """SELECT metadata
                   FROM book_status_history
                   WHERE status_type = ? AND status_value = ?""",
                (TEXT_EXTRACTION_STATUS_TYPE, ExtractionStatus.COMPLETED.value),
            )
            results = await cursor.fetchall()

            total_pages = 0
            total_time_ms = 0
            completed_count = 0

            for (metadata_json,) in results:
                if metadata_json:
                    metadata = json.loads(metadata_json)
                    total_pages += metadata.get("page_count", 0)
                    total_time_ms += metadata.get("extraction_time_ms", 0)
                    completed_count += 1

            # Calculate averages
            avg_pages = total_pages / completed_count if completed_count > 0 else 0
            avg_time_ms = total_time_ms / completed_count if completed_count > 0 else 0

            return {
                "status_summary": status_summary,
                "total_pages_extracted": total_pages,
                "avg_pages_per_book": round(avg_pages, 1),
                "avg_extraction_time_ms": round(avg_time_ms, 1),
                "recent_failures": recent_failures,
            }
    except Exception as e:
        logger.error(f"⚠️ Failed to get extraction progress: {e}")
        return {"status_summary": {"total": 0}, "total_pages_extracted": 0, "recent_failures": []}
