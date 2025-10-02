#!/usr/bin/env python3
"""
Track Conversion Failure Task

Records conversion failure metadata for books that failed conversion in GRIN.
"""

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from grin_to_s3.sync.pipeline import SyncPipeline

from .task_types import TaskAction, TaskType, TrackConversionFailureResult

logger = logging.getLogger(__name__)


async def main(barcode: str, pipeline: "SyncPipeline") -> TrackConversionFailureResult:
    """Track conversion failure metadata for a book.

    Retrieves failure metadata stored in the pipeline during queue processing
    and returns it for database recording.

    Args:
        barcode: Book barcode
        pipeline: Pipeline instance containing failure metadata

    Returns:
        TrackConversionFailureResult with failure metadata
    """
    logger.info(f"[{barcode}] Recording conversion failure metadata")

    # Retrieve failure metadata from pipeline
    failure_metadata = pipeline.conversion_failure_metadata.get(barcode)

    if not failure_metadata:
        logger.error(f"[{barcode}] No conversion failure metadata found in pipeline")
        return TrackConversionFailureResult(
            barcode=barcode,
            task_type=TaskType.TRACK_CONVERSION_FAILURE,
            action=TaskAction.FAILED,
            error="No failure metadata found",
        )

    logger.debug(
        f"[{barcode}] Conversion failed on {failure_metadata.get('grin_convert_failed_date')}: "
        f"{failure_metadata.get('grin_convert_failed_info')}"
    )

    return TrackConversionFailureResult(
        barcode=barcode,
        task_type=TaskType.TRACK_CONVERSION_FAILURE,
        action=TaskAction.COMPLETED,
        data=failure_metadata,
    )
