#!/usr/bin/env python3
"""
Request Conversion Task

Handles conversion requests for missing archives during sync operations.
"""

import logging
from typing import TYPE_CHECKING

import aiohttp
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_exponential

from grin_to_s3.processing import request_conversion

if TYPE_CHECKING:
    from grin_to_s3.sync.pipeline import SyncPipeline

from .task_types import RequestConversionResult, TaskAction, TaskType

logger = logging.getLogger(__name__)


# Retry conversion requests with exponential backoff for transient failures
# Retry schedule: immediate, 2s, 4s (total ~6s across 3 attempts)
# Note: 429 errors are excluded from retry as they indicate queue is full
@retry(
    stop=stop_after_attempt(3),
    retry=lambda retry_state: bool(
        retry_state.outcome
        and retry_state.outcome.failed
        and not (
            isinstance(retry_state.outcome.exception(), aiohttp.ClientResponseError)
            and getattr(retry_state.outcome.exception(), "status", None) == 429
        )
    ),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
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
                action=TaskAction.COMPLETED,
                data={
                    "conversion_status": "requested",
                    "request_count": pipeline.conversion_requests_made,
                    "grin_response": result,
                },
                reason="success_conversion_requested",
            )
        elif "already" in result_lower and ("process" in result_lower or "available for download" in result_lower):
            if "available for download" in result_lower:
                logger.info(f"[{barcode}] Book already converted and available for download")
                return RequestConversionResult(
                    barcode=barcode,
                    task_type=TaskType.REQUEST_CONVERSION,
                    action=TaskAction.SKIPPED,
                    data={
                        "conversion_status": "already_available",
                        "request_count": pipeline.conversion_requests_made,
                        "grin_response": result,
                    },
                    reason="skip_already_available",
                )
            else:
                logger.warning(f"[{barcode}] GRIN reports title is already being processed: {result}")
                return RequestConversionResult(
                    barcode=barcode,
                    task_type=TaskType.REQUEST_CONVERSION,
                    action=TaskAction.SKIPPED,
                    data={
                        "conversion_status": "in_process",
                        "request_count": pipeline.conversion_requests_made,
                        "grin_response": result,
                    },
                    reason="skip_already_in_process",
                )
        else:
            # Any other response from GRIN means the book can't be converted
            logger.error(f"[{barcode}] Conversion request failed: {result}")
            return RequestConversionResult(
                barcode=barcode,
                task_type=TaskType.REQUEST_CONVERSION,
                action=TaskAction.SKIPPED,
                data={
                    "conversion_status": "unavailable",
                    "request_count": pipeline.conversion_requests_made,
                    "grin_response": result,
                },
                reason="skip_verified_unavailable",
            )

    except aiohttp.ClientResponseError as e:
        # Handle 429 errors after retries are exhausted
        if e.status == 429:
            logger.warning(f"[{barcode}] GRIN queue limit reached (429 Too Many Requests)")
            return RequestConversionResult(
                barcode=barcode,
                task_type=TaskType.REQUEST_CONVERSION,
                action=TaskAction.FAILED,  # FAILED triggers sequential failure counter
                error="Queue limit reached in GRIN",
                data={
                    "conversion_status": "queue_limit_reached",
                    "request_count": pipeline.conversion_requests_made,
                    "grin_response": "Queue limit reached in GRIN",
                },
                reason="fail_queue_limit_reached",
            )
        else:
            # Re-raise other HTTP errors for task failure
            raise

    # All other exceptions bubble up naturally
