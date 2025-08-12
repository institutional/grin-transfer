#!/usr/bin/env python3
"""
Conversion Request Handler

Handles conversion requests for missing archives during sync operations.
"""

import logging
from typing import Any

from grin_to_s3.client import GRINClient
from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.processing import ProcessingRequestError, request_conversion

logger = logging.getLogger(__name__)


class ConversionRequestHandler:
    """Handler for conversion requests during sync operations."""

    def __init__(self, library_directory: str, db_tracker: SQLiteProgressTracker, secrets_dir: str | None = None):
        self.library_directory = library_directory
        self.db_tracker = db_tracker
        self.secrets_dir = secrets_dir
        self.requests_made = 0

    async def handle_missing_archive(self, barcode: str, request_limit: int) -> str:
        """Handle archive that returned 404.

        Args:
            barcode: Book barcode that returned 404
            request_limit: Maximum number of conversion requests allowed

        Returns:
            Status string:
            - "requested": Conversion requested successfully
            - "in_process": Already being processed
            - "unavailable": Cannot be converted
            - "limit_reached": At request limit
        """
        # Check if under request limit
        if self.requests_made >= request_limit:
            logger.info(f"[{barcode}] Conversion request limit reached ({request_limit})")
            return "limit_reached"

        try:
            logger.info(f"[{barcode}] Requesting conversion for missing archive")
            result = await request_conversion(barcode, self.library_directory, self.secrets_dir)

            self.requests_made += 1

            if result == "Success":
                logger.info(f"[{barcode}] Conversion requested successfully")
                return "requested"
            elif "already in process" in result.lower() or "already available" in result.lower():
                logger.warning(f"[{barcode}] Already in process (shouldn't happen with filtering): {result}")
                return "in_process"
            else:
                # Mark as verified_unavailable
                logger.warning(f"[{barcode}] Conversion request failed: {result}")
                await self._mark_verified_unavailable(barcode, result)
                return "unavailable"

        except ProcessingRequestError as e:
            logger.error(f"[{barcode}] Conversion request failed: {e}")
            await self._mark_verified_unavailable(barcode, str(e))
            return "unavailable"
        except Exception as e:
            logger.error(f"[{barcode}] Unexpected error during conversion request: {e}")
            await self._mark_verified_unavailable(barcode, str(e))
            return "unavailable"

    async def _mark_verified_unavailable(self, barcode: str, reason: str) -> None:
        """Mark a book as verified unavailable in the database."""
        try:
            await self.db_tracker.add_status(
                barcode=barcode, status_type="sync", status_value="verified_unavailable", details={"reason": reason}
            )
            logger.info(f"[{barcode}] Marked as verified_unavailable: {reason}")
        except Exception as e:
            logger.error(f"[{barcode}] Failed to mark as verified_unavailable: {e}")


async def handle_missing_archive_for_previous_queue(
    barcode: str,
    grin_client: GRINClient,
    library_directory: str,
    db_tracker: SQLiteProgressTracker,
    request_limit: int,
    requests_made: int,
    secrets_dir: str | None = None,
) -> dict[str, Any]:
    """Handle missing archive for previous queue processing.

    This is a convenience function that can be used directly without instantiating
    the ConversionRequestHandler class.

    Args:
        barcode: Book barcode that returned 404
        grin_client: GRIN client instance (unused but kept for consistency)
        library_directory: Library directory name
        db_tracker: Database tracker instance
        request_limit: Maximum number of conversion requests allowed
        requests_made: Number of requests made so far
        secrets_dir: Directory containing GRIN secrets files

    Returns:
        dict with keys:
        - status: str (same as ConversionRequestHandler.handle_missing_archive)
        - requests_made: int (updated count)
    """
    # Create handler instance
    handler = ConversionRequestHandler(library_directory, db_tracker, secrets_dir)
    handler.requests_made = requests_made

    # Handle the missing archive
    status = await handler.handle_missing_archive(barcode, request_limit)

    return {"status": status, "requests_made": handler.requests_made}
