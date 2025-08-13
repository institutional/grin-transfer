#!/usr/bin/env python3
"""
Sync Teardown Operations

Standalone functions for handling post-sync operations and status updates
without mutating external state.
"""

import logging
from copy import deepcopy
from typing import Any

from grin_to_s3.database_utils import batch_write_status_updates
from grin_to_s3.extract.tracking import StatusUpdate

from .models import SyncStats

logger = logging.getLogger(__name__)


async def process_skip_result_teardown(
    barcode: str,
    sync_status_updates: list[StatusUpdate],
    db_path: str,
    current_stats: SyncStats,
) -> tuple[dict[str, Any], SyncStats]:
    """
    Process skip result and return status dictionary and updated stats copy.

    Does not mutate the input stats dictionary - returns a copy with updates.
    Handles database writes for status updates.

    Args:
        barcode: Book barcode that was skipped
        sync_status_updates: List of status updates from etag check
        db_path: Database path for writing status updates
        current_stats: Current statistics dictionary (not mutated)

    Returns:
        Tuple of (status_dict, updated_stats_copy)
    """
    # Create a copy of stats to avoid mutation
    updated_stats = deepcopy(current_stats)

    # Write sync status updates for books that don't need download
    if sync_status_updates:
        try:
            await batch_write_status_updates(db_path, sync_status_updates)
        except Exception as e:
            logger.warning(f"[{barcode}] Failed to write status updates: {e}")

    # Check the first status update to determine the result type
    status_value = sync_status_updates[0].status_value if sync_status_updates else "unknown"
    conversion_status = (
        sync_status_updates[0].metadata.get("conversion_status")
        if sync_status_updates and sync_status_updates[0].metadata
        else None
    )

    if status_value == "completed" and conversion_status == "requested":
        updated_stats["conversion_requested"] += 1
        return {
            "barcode": barcode,
            "download_success": False,
            "completed": True,
            "conversion_requested": True,
        }, updated_stats
    elif status_value == "completed" and conversion_status == "in_process":
        updated_stats["conversion_requested"] += 1  # Count in_process as conversion_requested in stats
        return {
            "barcode": barcode,
            "download_success": False,
            "completed": True,
            "already_in_process": True,
        }, updated_stats
    elif status_value == "marked_unavailable":
        updated_stats["marked_unavailable"] += 1
        return {
            "barcode": barcode,
            "download_success": False,
            "marked_unavailable": True,
        }, updated_stats
    elif (
        status_value == "skipped"
        and sync_status_updates[0].metadata
        and sync_status_updates[0].metadata.get("skip_reason") == "conversion_limit_reached"
    ):
        updated_stats["skipped_conversion_limit"] += 1
        updated_stats["skipped"] += 1
        return {
            "barcode": barcode,
            "download_success": False,
            "skipped": True,
            "conversion_limit_reached": True,
        }, updated_stats
    else:
        # Default ETag match or other skip
        updated_stats["skipped_etag_match"] += 1
        updated_stats["skipped"] += 1
        return {
            "barcode": barcode,
            "download_success": False,
            "skipped": True,
            "skip_result": True,  # Simplified since we don't have access to the original skip_result
        }, updated_stats
