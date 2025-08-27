#!/usr/bin/env python3
"""
Request Conversion Task

Handles conversion requests for missing archives during sync operations.
"""

import logging
from typing import TYPE_CHECKING

from grin_to_s3.processing import request_conversion

if TYPE_CHECKING:
    from grin_to_s3.sync.pipeline import SyncPipeline

from .task_types import RequestConversionResult, TaskAction, TaskType

logger = logging.getLogger(__name__)


async def main(barcode: str, pipeline: "SyncPipeline") -> RequestConversionResult:
    """Request conversion for a missing archive."""

    # Check conversion request limit
    if pipeline.conversion_requests_made >= pipeline.conversion_request_limit:
        logger.warning(f"[{barcode}] Conversion request limit reached ({pipeline.conversion_request_limit})")
        return RequestConversionResult(
            barcode=barcode,
            task_type=TaskType.REQUEST_CONVERSION,
            action=TaskAction.SKIPPED,
            data={"conversion_status": "limit_reached", "request_count": pipeline.conversion_requests_made},
            reason="skip_conversion_limit_reached",
        )

    logger.info(f"[{barcode}] Requesting conversion for missing archive")
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
    elif "already" in result_lower and "process" in result_lower:
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
