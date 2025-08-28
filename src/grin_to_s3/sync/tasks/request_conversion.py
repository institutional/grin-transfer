#!/usr/bin/env python3
"""
Request Conversion Task

Handles conversion requests for missing archives during sync operations.
"""

import logging
from typing import TYPE_CHECKING

from grin_to_s3.processing import ProcessingRequestError, request_conversion

if TYPE_CHECKING:
    from grin_to_s3.sync.pipeline import SyncPipeline

from .task_types import RequestConversionResult, TaskAction, TaskType

logger = logging.getLogger(__name__)


async def main(barcode: str, pipeline: "SyncPipeline") -> RequestConversionResult:
    """Request conversion for a missing archive."""

    logger.info(f"[{barcode}] Requesting conversion for missing archive")
    try:
        result = await request_conversion(barcode, pipeline.library_directory, pipeline.secrets_dir)
        pipeline.conversion_requests_made += 1

        # Process result string
        result_lower = result.lower()

        if result_lower == "success":
            logger.info(f"[{barcode}] Conversion requested successfully")
            return RequestConversionResult(
                barcode=barcode,
                task_type=TaskType.REQUEST_CONVERSION,
                action=TaskAction.SKIPPED,
                data={"conversion_status": "requested", "request_count": pipeline.conversion_requests_made},
                reason="skip_conversion_requested",
            )
        elif "already" in result_lower and ("process" in result_lower or "available for download" in result_lower):
            if "available for download" in result_lower:
                logger.info(f"[{barcode}] Book already converted and available for download")
                return RequestConversionResult(
                    barcode=barcode,
                    task_type=TaskType.REQUEST_CONVERSION,
                    action=TaskAction.SKIPPED,
                    data={"conversion_status": "already_available", "request_count": pipeline.conversion_requests_made},
                    reason="skip_already_available",
                )
            else:
                logger.warning(f"[{barcode}] GRIN reports title is already being processed: {result}")
                return RequestConversionResult(
                    barcode=barcode,
                    task_type=TaskType.REQUEST_CONVERSION,
                    action=TaskAction.SKIPPED,
                    data={"conversion_status": "in_process", "request_count": pipeline.conversion_requests_made},
                    reason="skip_already_in_process",
                )
        else:
            # Any other response from GRIN means the book can't be converted
            logger.error(f"[{barcode}] Conversion request failed: {result}")
            return RequestConversionResult(
                barcode=barcode,
                task_type=TaskType.REQUEST_CONVERSION,
                action=TaskAction.SKIPPED,
                data={"conversion_status": "unavailable", "request_count": pipeline.conversion_requests_made},
                reason="skip_verified_unavailable",
            )
    except ProcessingRequestError as e:
        # Check if this is a 429 "Too Many Requests" error
        if "429" in str(e) and "Too Many Requests" in str(e):
            logger.warning(f"[{barcode}] GRIN queue limit reached (429 Too Many Requests)")
            return RequestConversionResult(
                barcode=barcode,
                task_type=TaskType.REQUEST_CONVERSION,
                action=TaskAction.FAILED,  # FAILED triggers sequential failure counter
                data={"conversion_status": "queue_limit_reached", "request_count": pipeline.conversion_requests_made},
                reason="fail_queue_limit_reached",
            )
        else:
            # Re-raise other ProcessingRequestError for task failure
            raise
