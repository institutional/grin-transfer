#!/usr/bin/env python3
"""
Conversion Request Handler

Handles conversion requests for missing archives during sync operations.
"""

import logging

from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.database.database_utils import mark_verified_unavailable

logger = logging.getLogger(__name__)


class ConversionRequestHandler:
    """Handler for conversion requests during sync operations."""

    def __init__(self, library_directory: str, db_tracker: SQLiteProgressTracker, secrets_dir: str | None = None):
        self.library_directory = library_directory
        self.db_tracker = db_tracker
        self.secrets_dir = secrets_dir
        self.requests_made = 0

    async def handle_missing_archive(self, barcode: str) -> str:
        """Handle archive that returned 404.

        Args:
            barcode: Book barcode that returned 404

        Returns:
            Status string:
            - "requested": Conversion requested successfully
            - "in_process": Already being processed
            - "unavailable": Cannot be converted
        """
        try:
            from grin_to_s3.processing import request_conversion

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
                await mark_verified_unavailable(str(self.db_tracker.db_path), barcode, result)
                return "unavailable"

        except Exception as e:
            logger.error(f"[{barcode}] Conversion request failed: {e}")
            await mark_verified_unavailable(str(self.db_tracker.db_path), barcode, str(e))
            return "unavailable"
