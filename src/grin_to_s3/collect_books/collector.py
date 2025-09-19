#!/usr/bin/env python3
"""
Book Collection main orchestrator.

Contains the BookCollector class responsible for coordinating the entire book collection process.
"""

import asyncio
import logging
import signal
import time
from collections.abc import AsyncGenerator

from grin_to_s3.client import GRINClient, GRINRow
from grin_to_s3.common import (
    RateLimiter,
    pluralize,
    print_oauth_setup_instructions,
)
from grin_to_s3.constants import BOOKS_EXPORT_CSV_FILENAME, GRIN_RATE_LIMIT_QPS
from grin_to_s3.export import upload_csv_to_storage, write_books_to_csv
from grin_to_s3.run_config import RunConfig, StorageConfig
from grin_to_s3.storage import BookManager, create_storage_from_config
from grin_to_s3.sync.progress_reporter import SlidingWindowRateCalculator, show_progress

from .grin_parser import parse_grin_row
from .models import BookRecord, SQLiteProgressTracker

# Set up module logger
logger = logging.getLogger(__name__)

# Progress display frequency (books per progress update)
PROGRESS_UPDATE_FREQUENCY = 2000

GRIN_PAGE_SIZE = 10_000


class BookCollector:
    """Main book collection orchestrator."""

    def __init__(
        self,
        process_summary_stage,
        storage_config: StorageConfig,
        run_config: RunConfig,
        secrets_dir: str | None = None,
        refresh_mode: bool = False,
    ):
        # Load or use provided configuration
        self.run_config = run_config
        self.refresh_mode = refresh_mode

        self.directory = self.run_config.library_directory
        self.process_summary_stage = process_summary_stage

        # Initialize client
        self.grin_client = GRINClient(secrets_dir=secrets_dir)
        self.rate_limiter = RateLimiter(GRIN_RATE_LIMIT_QPS)
        self.storage_config = storage_config

        # Progress tracking
        self.rate_calculator = SlidingWindowRateCalculator(window_size=5)
        self.start_time: float | None = None
        self.sqlite_tracker = SQLiteProgressTracker(str(self.run_config.sqlite_db_path))
        storage = create_storage_from_config(storage_config)
        self.book_manager: BookManager = BookManager(
            storage, storage_config=storage_config, base_prefix=run_config.run_name
        )

    async def get_all_books(self, limit: int | None = None) -> AsyncGenerator[GRINRow, None]:
        """Stream all books from GRIN using HTML pagination with large page sizes.

        Always starts from page 1 for idempotent collection.

        Args:
            limit: Maximum number of books to yield

        Yields:
            GRINRow: book_row
        """
        print("Collecting all books from GRIN...")
        logger.info("Streaming all books from GRIN...")

        total_yielded = 0

        async for (
            book_row,
            _,
        ) in self.grin_client.stream_book_list_html_prefetch(
            directory=self.directory,
            list_type="_all_books",
            page_size=GRIN_PAGE_SIZE,
            start_page=1,
            sqlite_tracker=self.sqlite_tracker,
        ):
            if limit and total_yielded >= limit:
                break

            yield book_row
            total_yielded += 1

            if total_yielded % 5000 == 0:
                logger.info(f"Streamed {total_yielded:,} {pluralize(total_yielded, 'book')}...")

        print(f"Collection complete: {total_yielded:,} total books collected")

    async def collect_books(self, limit: int | None = None) -> bool:
        """
        Book collection with pagination.

        Processes books one at a time with reliable pagination and resume capability.
        Exports CSV to storage at the end from the SQLite database.

        Returns:
            True if collection completed successfully, False if interrupted or incomplete
        """
        if self.refresh_mode:
            print("Starting book collection in refresh mode (updates existing, adds new)")
        else:
            print("Starting book collection")
        if limit:
            print(f"Limit: {limit:,} {pluralize(limit, 'book')}")
            print("\n⚠️  WARNING: Using --limit will result in an incomplete collection only suitable for quick tests.")
            print("   For production sync, a full collect (without --limit) is recommended.\n")

        # Validate credentials before starting export
        logger.debug("Validating GRIN credentials...")
        try:
            await self.grin_client.auth.validate_credentials(self.directory)
        except Exception as e:
            print(f"Credential validation failed: {e}")
            print("Collection cannot continue without valid credentials.")
            print_oauth_setup_instructions()
            return False

        # Set up async-friendly signal handling
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()
        completed_successfully = True

        def handle_interrupt():
            print("\nInterrupt received - saving progress and exiting gracefully...")
            stop_event.set()

        # Install signal handler
        loop.add_signal_handler(signal.SIGINT, handle_interrupt)

        try:
            # Initialize SQLite tracker
            await self.sqlite_tracker.init_db()

            # Start progress tracking
            self.start_time = time.time()
            processed_count = 0

            # Process books one by one using configured data mode
            async for grin_row in self.get_all_books(limit=limit):
                # Check for interrupt
                if stop_event.is_set():
                    print("Collection interrupted - saving progress...")
                    completed_successfully = False
                    break

                # Extract barcode for checking
                barcode = grin_row.get("barcode", "")
                if not barcode:
                    continue

                # Process all books - let database UPSERT handle duplicates safely

                # Process the book
                try:
                    record = await self.process_book(grin_row)
                    if record:
                        processed_count += 1

                        # Show progress every N items
                        if processed_count % PROGRESS_UPDATE_FREQUENCY == 0:
                            extra_info = {"current": record.barcode} if record else None
                            show_progress(
                                start_time=self.start_time,
                                total_items=None,
                                rate_calculator=self.rate_calculator,
                                completed_count=processed_count,
                                operation_name="barcode records",
                                extra_info=extra_info,
                            )

                        # Track in process summary
                        self.process_summary_stage.books_collected += 1

                    else:
                        # Book was processed but no record created (already exists, etc.)
                        self.process_summary_stage.books_collected += 1

                except Exception as e:
                    print(f"Error processing {barcode}: {e}")
                    await self.sqlite_tracker.mark_failed(barcode, str(e))

                    # Track failed item in process summary
                    self.process_summary_stage.collection_failed += 1
                    continue

            print(f"Processed {processed_count} new books")

            # Export CSV from database if collection completed successfully
            if completed_successfully or processed_count > 0:
                print("\nExporting collection to CSV...")

                # First generate the CSV file in the output directory
                csv_path, record_count = await write_books_to_csv(
                    self.sqlite_tracker, self.run_config.output_directory / BOOKS_EXPORT_CSV_FILENAME
                )

                # Then upload it to storage
                storage_path = await upload_csv_to_storage(
                    csv_path=csv_path,
                    book_manager=self.book_manager,
                    compression_enabled=False,  # No compression for CLI usage
                )
                print(f"CSV export completed: {storage_path} ({record_count:,} records)")
                if self.storage_config["protocol"] != "file":
                    print(f"Local CSV available at: {csv_path}")

        finally:
            # Remove signal handler
            loop.remove_signal_handler(signal.SIGINT)

            # Show final completion message
            print(f"✓ Completed Book Collection: {processed_count:,} barcode records")

        # Return completion status
        return completed_successfully

    async def process_book(self, grin_row: GRINRow) -> BookRecord | None:
        """Process a single GRINRow from GRIN and return its record.
        Args:
            grin_row: GRINRow dict from client with parsed book data
        """

        # Process GRIN data directly from the row
        parsed_data = parse_grin_row(grin_row)
        if not parsed_data or not parsed_data.get("barcode"):
            return None

        barcode = parsed_data["barcode"]

        try:
            # Create record
            record = BookRecord(**parsed_data)

            # Warn if GRIN returned empty title but metadata suggests the book should be downloadable
            if not record.title or record.title.strip() == "":
                grin_state = record.grin_state or ""
                if grin_state != "NOT_AVAILABLE_FOR_DOWNLOAD":
                    logger.warning(
                        f"[{barcode}] GRIN returned empty title field; grin_state={grin_state or 'None'}; expected only when download is unavailable"
                    )

            # Let the database handle everything via UPSERT
            await self.sqlite_tracker.save_book(record, refresh_mode=self.refresh_mode)

            # Mark as processed for progress tracking
            await self.sqlite_tracker.mark_processed(barcode)
            return record

        except Exception as e:
            print(f"Error processing {barcode}: {e}")
            await self.sqlite_tracker.mark_failed(barcode, str(e))
            return None

    async def cleanup(self):
        """Clean up resources."""
        if hasattr(self, "grin_client"):
            await self.grin_client.close()
        if hasattr(self, "sqlite_tracker"):
            await self.sqlite_tracker.close()
