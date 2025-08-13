#!/usr/bin/env python3
"""
Conversion Request Handler

Handles conversion requests for missing archives during sync operations.
"""

import logging

from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.database_utils import mark_verified_unavailable
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
            elif (
                "already in process" in result.lower()
                or "already available" in result.lower()
                or "already being converted" in result.lower()
            ):
                logger.info(f"[{barcode}] Already being processed: {result}")
                return "in_process"
            else:
                # Mark as verified_unavailable
                logger.warning(f"[{barcode}] Conversion request failed: {result}")
                await self._mark_verified_unavailable(barcode, result)
                return "unavailable"

        except ProcessingRequestError as e:
            error_msg = str(e).lower()
            if (
                "already being converted" in error_msg
                or "already in process" in error_msg
                or "already available" in error_msg
            ):
                logger.info(f"[{barcode}] Already being processed: {e}")
                return "in_process"
            else:
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
            await mark_verified_unavailable(self.db_tracker.db_path, barcode, reason)
        except Exception as e:
            logger.error(f"[{barcode}] Failed to mark as verified_unavailable in database: {e}")
            # Don't re-raise - database failures shouldn't prevent conversion logic from continuing
