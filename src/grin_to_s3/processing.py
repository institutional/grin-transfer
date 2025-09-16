#!/usr/bin/env python3
"""
GRIN Book Processing Management

Request book processing via GRIN.
"""

import argparse
import asyncio
import logging
import sys
import time
from datetime import UTC, datetime
from typing import NamedTuple

from grin_to_s3.client import GRINClient
from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.common import (
    BarcodeSet,
    format_duration,
    parse_barcode_arguments,
    pluralize,
    print_oauth_setup_instructions,
)
from grin_to_s3.database.database_utils import validate_database_file
from grin_to_s3.logging_config import setup_logging
from grin_to_s3.process_summary import (
    create_book_manager_for_uploads,
    create_process_summary,
    display_step_summary,
    get_current_stage,
    save_process_summary,
)
from grin_to_s3.run_config import apply_run_config_to_args, load_run_config

from .constants import OUTPUT_DIR
from .database import connect_async
from .queue_utils import get_in_process_set


class StatusUpdate(NamedTuple):
    """Status update tuple for collecting updates before writing."""

    barcode: str
    status_type: str
    status_value: str
    metadata: dict | None = None
    session_id: str | None = None


logger = logging.getLogger(__name__)


async def parse_failed_books_response(grin_client, library_directory: str) -> BarcodeSet:
    """Parse GRIN _failed endpoint response into a set of barcodes."""
    response_text = await grin_client.fetch_resource(library_directory, "_failed?format=text")
    lines = response_text.strip().split("\n")
    return {line.strip() for line in lines if line.strip()}


class ProcessingClient:
    """Client for requesting book processing via GRIN _process endpoint."""

    def __init__(
        self,
        directory: str,
        secrets_dir: str | None = None,
        timeout: int = 60,
    ):
        self.directory = directory
        self.grin_client = GRINClient(timeout=timeout, secrets_dir=secrets_dir)

        # In-memory tracking for batch database updates
        self.requested_books: set[str] = set()  # Track books we've requested processing for

    async def cleanup(self) -> None:
        """Clean up resources and close connections safely."""
        try:
            if hasattr(self, "grin_client"):
                await self.grin_client.close()
                logger.debug("Closed GRIN client session")
        except Exception as e:
            logger.warning(f"Error closing GRIN client session: {e}")

    async def request_processing_batch(self, barcodes: list[str]) -> dict[str, str]:
        """
        Request processing for a batch of books.

        Args:
            barcodes: List of book barcodes to request processing for

        Returns:
            Dict mapping barcode to status message

        Raises:
            ProcessingRequestError: If the request fails
        """
        if not barcodes:
            return {}

        # Use GRIN's _process endpoint with comma-separated barcodes
        barcodes_param = ",".join(barcodes)
        process_url = f"_process?barcodes={barcodes_param}"
        response_text = await self.grin_client.fetch_resource(self.directory, process_url)

        # Log full GRIN response for debugging
        logger.debug(f"Full GRIN response for {len(barcodes)} barcodes:\n{response_text}")

        # Parse TSV response ("Barcode\tStatus" format)
        lines = response_text.strip().split("\n")
        result_lines = lines[1:]  # Skip header

        if len(result_lines) != len(barcodes):
            logger.warning(f"Expected {len(barcodes)} result lines, got {len(result_lines)}")

        results = {}
        for line in result_lines:
            if not line.strip():
                continue

            parts = line.split("\t")
            returned_barcode, status = parts  # Let IndexError bubble up for malformed lines
            # Strip whitespace from barcode - barcodes should be clean identifiers
            returned_barcode = returned_barcode.strip()
            results[returned_barcode] = status

            if status == "Success":
                logger.debug(f"Successfully requested processing for {returned_barcode}")
            else:
                logger.warning(f"Processing request failed for {returned_barcode}: {status}")

        return results

    async def get_in_process_books(self) -> BarcodeSet:
        """Get list of books currently in processing queue."""
        return await get_in_process_set(self.grin_client, self.directory)

    async def get_failed_books(self) -> BarcodeSet:
        """Get list of books that failed processing."""
        return await parse_failed_books_response(self.grin_client, self.directory)


class ProcessingPipeline:
    """Pipeline for requesting book processing with database tracking."""

    def __init__(
        self,
        db_path: str,
        directory: str,
        process_summary_stage,
        batch_size: int = 200,  # Increased from 100 based on testing
        max_in_process: int = 50000,  # GRIN's max queue limit
        secrets_dir: str | None = None,
    ):
        self.db_path = db_path
        self.directory = directory
        self.batch_size = batch_size
        self.max_in_process = max_in_process
        self.process_summary_stage = process_summary_stage

        # Initialize components
        self.processing_client = ProcessingClient(
            directory=directory,
            secrets_dir=secrets_dir,
        )
        self.db_tracker = SQLiteProgressTracker(db_path)

        # Concurrency control - limit concurrent batch requests
        self.max_concurrent_batches = 5  # Maximum parallel batch requests
        self._batch_semaphore = asyncio.Semaphore(self.max_concurrent_batches)

        # Queue monitoring
        self.queue_report_interval = 30.0  # Default to 30 seconds, can be overridden

        # Statistics
        self.stats = {
            "requested": 0,
            "successful": 0,
            "failed": 0,
            "skipped": 0,
        }

    async def cleanup(self) -> None:
        """Clean up resources and close connections safely."""
        await self.processing_client.cleanup()
        try:
            if hasattr(self, "db_tracker"):
                await self.db_tracker.close()
        except Exception as e:
            logger.warning(f"Error closing database connection: {e}")

    async def get_processing_status(self) -> dict:
        """Get current processing status from GRIN."""
        in_process = await self.processing_client.get_in_process_books()

        return {
            "in_process": len(in_process),
            "queue_space": max(0, self.max_in_process - len(in_process)),
        }

    async def _get_candidate_barcodes(
        self, limit: int, barcodes: list[str] | None = None, in_process_books: set[str] | None = None
    ) -> list[str]:
        """Get candidate barcodes for processing requests.

        Args:
            limit: Maximum number of barcodes to return
            barcodes: If provided, return these barcodes directly (overrides database search)
            in_process_books: Books currently in GRIN processing queue to filter out

        Returns:
            List of candidate barcodes for processing
        """
        # If specific barcodes provided, return them directly (no filtering)
        if barcodes:
            print(f"Using {len(barcodes)} explicitly specified barcodes")
            return barcodes

        # Otherwise, use database query logic
        in_process_books = in_process_books or set()
        total_books = await self.db_tracker.get_book_count()

        candidate_barcodes: list[str] = []
        fetch_offset = 0
        fetch_batch_size = min(limit * 100, 50000)  # Start with reasonable batch

        print("Searching for books that haven't been requested for processing...")

        while len(candidate_barcodes) < limit and fetch_offset < total_books:
            # Fetch next batch of books from database that haven't been requested
            # Skip books marked as NOT_AVAILABLE_FOR_DOWNLOAD or CHECKED_IN
            async with connect_async(self.db_path) as db:
                cursor = await db.execute(
                    """
                    SELECT barcode FROM books
                    WHERE NULLIF(processing_request_timestamp, '') IS NULL
                    AND NULLIF(converted_date, '') IS NULL
                    AND (NULLIF(grin_state, '') IS NULL OR grin_state NOT IN ('NOT_AVAILABLE_FOR_DOWNLOAD', 'CHECKED_IN'))
                    ORDER BY created_at LIMIT ? OFFSET ?
                    """,
                    (fetch_batch_size, fetch_offset),
                )
                rows = await cursor.fetchall()
                batch_barcodes = [row[0] for row in rows]

            if not batch_barcodes:
                # No more unrequested books in database
                break

            # Filter out books currently in GRIN's processing queue
            new_candidates = [barcode for barcode in batch_barcodes if barcode not in in_process_books]

            candidate_barcodes.extend(new_candidates)
            fetch_offset += len(batch_barcodes)

            print(
                f"  Searched {fetch_offset:,}/{total_books:,} books, found {len(candidate_barcodes):,} new candidates"
            )

            # If we found enough candidates, we can stop
            if len(candidate_barcodes) >= limit:
                break

            # If we didn't find any new candidates in this batch, try a larger batch
            if not new_candidates and len(batch_barcodes) == fetch_batch_size:
                fetch_batch_size = min(fetch_batch_size * 2, 100000)
                print(f"  No new candidates found, increasing batch size to {fetch_batch_size:,}")

        # Limit to the requested number
        candidate_barcodes = candidate_barcodes[:limit]

        return candidate_barcodes

    async def run_processing_requests(self, limit: int | None = None, barcodes: list[str] | None = None) -> None:
        """Run the complete processing request pipeline."""
        print("Starting GRIN book processing request pipeline")
        print(f"Database: {self.db_path}")
        print(f"Directory: {self.directory}")
        print(f"Max concurrent batches: {self.max_concurrent_batches}")
        print(f"Max in process: {self.max_in_process:,}")
        if limit:
            print(f"Limit: {limit:,} {pluralize(limit, 'request')}")
        print()

        logger.info("Starting processing request pipeline")
        logger.info(f"Database: {self.db_path}")
        logger.info(f"Directory: {self.directory}")

        start_time = time.time()

        try:
            # Validate credentials
            logger.debug("Validating GRIN credentials...")
            try:
                await asyncio.wait_for(
                    self.processing_client.grin_client.auth.validate_credentials(self.directory), timeout=30.0
                )
            except TimeoutError:
                print("Credential validation timed out after 30 seconds")
                return
            except Exception as e:
                print(f"Credential validation failed: {e}")
                print_oauth_setup_instructions()
                return
            print()

            # Get initial status
            print("Checking current GRIN processing status...")
            try:
                status = await asyncio.wait_for(self.get_processing_status(), timeout=60.0)
                print(f"GRIN status: {status['in_process']:,} in process")
                print(f"Queue space available: {status['queue_space']:,} slots")
            except TimeoutError:
                print("❌ Getting GRIN status timed out after 60 seconds")
                return
            except Exception as e:
                print(f"❌ Failed to get GRIN status: {e}")
                return
            print()

            if status["queue_space"] <= 0:
                print("GRIN processing queue is full. Cannot submit new requests.")
                return

            # Determine how many books to request
            if barcodes:
                books_to_request = len(barcodes)
                print(f"Will request processing for {books_to_request} explicitly specified books")
            else:
                # Get books from database that could be processed
                total_books = await self.db_tracker.get_book_count()
                print(f"Database contains {total_books:,} books")

                books_to_request = min(limit or status["queue_space"], status["queue_space"])
                if books_to_request <= 0:
                    print("No requests to make")
                    return

                print(f"Will attempt to request processing for up to {books_to_request:,} new books")
            print()

            # Get current GRIN in-process books to avoid duplicate requests (unless using explicit barcodes)
            in_process_books = set()
            if not barcodes:
                print("Getting current GRIN in-process books to avoid duplicates...")
                try:
                    in_process_books = await asyncio.wait_for(
                        self.processing_client.get_in_process_books(), timeout=60.0
                    )
                    print(f"Found {len(in_process_books):,} books currently in GRIN processing queue")
                except TimeoutError:
                    print("❌ Getting in-process books timed out after 60 seconds")
                    print("Proceeding without filtering duplicates...")
                    in_process_books = set()
                except Exception as e:
                    print(f"❌ Failed to get in-process books: {e}")
                    print("Proceeding without filtering duplicates...")
                    in_process_books = set()
                print()

            # Get candidate barcodes using helper method
            candidate_barcodes = await self._get_candidate_barcodes(books_to_request, barcodes, in_process_books)

            print(f"Final result: {len(candidate_barcodes):,} books ready for processing requests")

            if not candidate_barcodes:
                if barcodes:
                    print("No barcodes provided for processing")
                else:
                    print("No new books to request processing for - all searched books are already in GRIN system")
                return

            # Process requests with async concurrency
            await self._process_batches_concurrently(candidate_barcodes, limit, start_time)

        except KeyboardInterrupt:
            print("\nProcessing requests interrupted by user")
            logger.info("Processing requests interrupted by user")

        except Exception as e:
            print(f"\nProcessing requests failed: {e}")
            logger.error(f"Processing requests failed: {e}", exc_info=True)

        finally:
            # Clean up resources
            await self.cleanup()

            # Final statistics
            total_elapsed = time.time() - start_time
            final_status = await self.get_processing_status()

            print("\nProcessing requests completed:")
            print(f"  Total runtime: {format_duration(total_elapsed)}")
            print(f"  Requests attempted: {self.stats['requested']:,}")
            print(f"  Successful requests: {self.stats['successful']:,}")
            print(f"  Failed requests: {self.stats['failed']:,}")
            if total_elapsed > 0:
                print(f"  Average rate: {self.stats['requested'] / total_elapsed:.1f} requests/second")

            print("\nFinal GRIN status:")
            print(f"  In process: {final_status['in_process']:,}")
            print(f"  Queue space: {final_status['queue_space']:,}")

            logger.info("Processing request pipeline completed")

    async def _process_single_batch(self, batch: list[str], batch_num: int) -> dict:
        """Process a single batch of books with rate limiting."""
        async with self._batch_semaphore:
            batch_start = time.time()
            batch_size = len(batch)

            print(f"Starting batch {batch_num}: {batch_size} books")

            try:
                # Make the batch processing request to GRIN
                batch_results = await self.processing_client.request_processing_batch(batch)

                # Track requested books for bulk database update
                successfully_requested_books = []

                # Process results
                successful = 0
                failed = 0

                for barcode in batch:
                    if barcode in batch_results:
                        status = batch_results[barcode]
                        if status == "Success":
                            successful += 1
                            successfully_requested_books.append(barcode)
                            logger.info(f"Successfully requested processing for {barcode}")
                            self.process_summary_stage.conversion_requests_made += 1
                        elif status == "Already available for download":
                            successful += 1  # Count as successful since book is ready
                            successfully_requested_books.append(barcode)  # Still track as requested
                            logger.warning(f"Book {barcode} already processed: {status}")
                            self.process_summary_stage.conversion_requests_made += 1
                        else:
                            failed += 1
                            error_msg = f"Failed to request processing for {barcode}: {status}"
                            logger.error(error_msg)
                            print(f"❌ {error_msg}")
                            self.process_summary_stage.conversion_requests_failed += 1
                    else:
                        failed += 1
                        error_msg = f"No result returned for {barcode}"
                        logger.error(error_msg)
                        print(f"❌ {error_msg}")
                        self.process_summary_stage.conversion_requests_failed += 1

                batch_elapsed = time.time() - batch_start
                rate = batch_size / batch_elapsed if batch_elapsed > 0 else 0

                print(
                    f"Completed batch {batch_num}: {successful}/{batch_size} successful "
                    f"in {batch_elapsed:.1f}s ({rate:.1f} req/s)"
                )

                # Bulk update database for all successfully requested books
                if successfully_requested_books:
                    await self._bulk_update_requested_books(successfully_requested_books)

                return {
                    "batch_num": batch_num,
                    "total": batch_size,
                    "successful": successful,
                    "failed": failed,
                    "elapsed": batch_elapsed,
                }

            except Exception as e:
                batch_elapsed = time.time() - batch_start
                logger.error(f"Batch {batch_num} processing request failed: {e}")
                print(f"❌ Batch {batch_num} failed: {e}")

                return {
                    "batch_num": batch_num,
                    "total": batch_size,
                    "successful": 0,
                    "failed": batch_size,
                    "elapsed": batch_elapsed,
                    "error": str(e),
                }

    async def _bulk_update_requested_books(self, barcodes: list[str]) -> None:
        """Update database with processing request timestamps for a batch of books."""
        if not barcodes:
            return

        try:
            current_timestamp = datetime.now(UTC).isoformat()
            placeholders = ",".join(["?"] * len(barcodes))

            async with connect_async(self.db_path) as db:
                await db.execute(
                    f"UPDATE books SET processing_request_timestamp = ? WHERE barcode IN ({placeholders})",
                    [current_timestamp] + barcodes,
                )
                await db.commit()

            logger.debug(f"Bulk updated {len(barcodes)} books with processing request timestamp")
        except Exception as e:
            logger.warning(f"Failed to bulk update {len(barcodes)} books in database: {e}")
            # Don't raise - database updates shouldn't block the processing pipeline

    async def _process_batches_concurrently(
        self, candidate_barcodes: list[str], limit: int | None, start_time: float
    ) -> None:
        """Process multiple batches concurrently with rate limiting and periodic queue reporting."""
        # Split into batches
        batches: list[list[str]] = []
        for i in range(0, len(candidate_barcodes), self.batch_size):
            batch = candidate_barcodes[i : i + self.batch_size]
            if limit:
                # Calculate how many books we still need
                books_processed = sum(len(b) for b in batches)
                remaining_needed = limit - books_processed
                if remaining_needed <= 0:
                    break
                if len(batch) > remaining_needed:
                    batch = batch[:remaining_needed]
            batches.append(batch)

        print(
            f"\nProcessing {len(candidate_barcodes)} books in {len(batches)} batches with up to "
            f"{self.max_concurrent_batches} concurrent batches"
        )
        print()

        # Create tasks for all batches
        tasks = [self._process_single_batch(batch, i + 1) for i, batch in enumerate(batches)]

        # Start queue size monitoring task
        queue_monitor_task = asyncio.create_task(self._monitor_queue_size_periodically())

        try:
            # Process all batches concurrently
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            # Cancel queue monitoring
            queue_monitor_task.cancel()
            try:
                await queue_monitor_task
            except asyncio.CancelledError:
                pass

        # Aggregate results
        total_processed = 0
        total_successful = 0
        total_failed = 0

        for i, result in enumerate(batch_results):
            if isinstance(result, Exception):
                print(f"❌ Batch {i + 1} failed with exception: {result}")
                batch_size = len(batches[i])
                total_processed += batch_size
                total_failed += batch_size
                self.stats["failed"] += batch_size
                self.stats["requested"] += batch_size
            elif isinstance(result, dict):
                total_processed += result["total"]
                total_successful += result["successful"]
                total_failed += result["failed"]
                self.stats["successful"] += result["successful"]
                self.stats["failed"] += result["failed"]
                self.stats["requested"] += result["total"]

        total_elapsed = time.time() - start_time
        overall_rate = total_processed / total_elapsed if total_elapsed > 0 else 0

        print("\n✅ All batches completed:")
        print(f"  Total processed: {total_processed}")
        print(f"  Successful: {total_successful}")
        print(f"  Failed: {total_failed}")
        print(f"  Total time: {total_elapsed:.1f}s")
        print(f"  Overall rate: {overall_rate:.1f} requests/sec")

    async def _monitor_queue_size_periodically(self) -> None:
        """Monitor GRIN queue size periodically during batch processing."""
        report_interval = float(self.queue_report_interval)  # Use configured interval
        last_report_time = time.time()

        try:
            while True:
                await asyncio.sleep(5.0)  # Check every 5 seconds
                current_time = time.time()

                # Report queue status every 30 seconds
                if current_time - last_report_time >= report_interval:
                    try:
                        status = await asyncio.wait_for(self.get_processing_status(), timeout=10.0)
                        timestamp = datetime.now().strftime("%H:%M:%S")

                        # Calculate change in queue size since start
                        current_in_process = status["in_process"]
                        queue_space = status["queue_space"]

                        # Show processing progress if we've made requests
                        progress_info = ""
                        if self.stats["successful"] > 0 or self.stats["failed"] > 0:
                            total_requests = self.stats["successful"] + self.stats["failed"]
                            progress_info = f" | Progress: {total_requests:,} requests completed"

                        print(
                            f"[{timestamp}] GRIN Queue: {current_in_process:,} in process, "
                            f"{queue_space:,} space available{progress_info}"
                        )
                        last_report_time = current_time

                    except TimeoutError:
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] Queue status check timed out")
                        last_report_time = current_time
                    except Exception as e:
                        logger.debug(f"Queue status check failed: {e}")
                        # Don't print errors to avoid spam, just update timing
                        last_report_time = current_time

        except asyncio.CancelledError:
            # Task was cancelled, exit cleanly
            pass


async def cmd_request(args) -> None:
    """Handle the 'request' command."""
    run_config = load_run_config(OUTPUT_DIR / args.run_name / "run_config.json")

    # Apply run configuration defaults
    apply_run_config_to_args(args, run_config)

    # Validate that we have a library directory
    if not getattr(args, "grin_library_directory", None):
        print("❌ Error: No GRIN library directory specified. This should be set in the run configuration.")
        print("Make sure you collected books with --library-directory argument.")
        sys.exit(1)

    # Validate database
    validate_database_file(OUTPUT_DIR / args.run_name / "books.db", check_books_count=True)
    setup_logging(args.log_level, run_config.log_file)

    # Log processing pipeline startup
    logger = logging.getLogger(__name__)
    logger.info(
        f"PROCESSING PIPELINE STARTED - {args.command} directory={args.grin_library_directory} "
        f"batch_size={args.batch_size}"
    )
    logger.info(f"Command: {' '.join(sys.argv)}")

    # Get run name from run config
    run_name = run_config.run_name

    # Create book storage for process summary uploads
    book_manager = await create_book_manager_for_uploads(run_name)

    # Create or load process summary
    run_summary = await create_process_summary(run_name, "process", book_manager)
    process_stage = get_current_stage(run_summary, "process")
    process_stage.set_command_arg("grin_library_directory", args.grin_library_directory)
    process_stage.set_command_arg("batch_size", args.batch_size)
    process_stage.set_command_arg("max_in_process", args.max_in_process)
    if args.limit:
        process_stage.set_command_arg("limit", args.limit)
    if args.barcodes:
        process_stage.set_command_arg("barcodes", args.barcodes)
    if hasattr(args, "barcodes_file") and args.barcodes_file:
        process_stage.set_command_arg("barcodes_file", args.barcodes_file)

    # Parse and validate barcodes if provided
    barcodes_str = getattr(args, "barcodes", None)
    barcodes_file = getattr(args, "barcodes_file", None)
    parsed_barcodes = parse_barcode_arguments(barcodes_str, barcodes_file)

    if parsed_barcodes:
        source_desc = "command line" if barcodes_str else f"file '{barcodes_file}'"
        print(f"Parsed {len(parsed_barcodes)} barcodes for processing from {source_desc}")

    # Create and run pipeline
    try:
        try:
            pipeline = ProcessingPipeline(
                db_path=str(OUTPUT_DIR / args.run_name / "books.db"),
                directory=args.grin_library_directory,
                process_summary_stage=process_stage,
                batch_size=args.batch_size,
                max_in_process=args.max_in_process,
                secrets_dir=args.secrets_dir,
            )

            # Set queue report interval
            pipeline.queue_report_interval = args.queue_report_interval

            # Run processing requests
            process_stage.add_progress_update("Starting processing requests")
            await pipeline.run_processing_requests(limit=args.limit, barcodes=parsed_barcodes)
            process_stage.add_progress_update("Processing requests completed")

        except KeyboardInterrupt:
            process_stage.add_progress_update("Operation cancelled by user")
            process_stage.add_error("KeyboardInterrupt", "User cancelled operation")
            print("\nOperation cancelled by user")
        except Exception as e:
            error_type = type(e).__name__
            process_stage.add_error(error_type, str(e))
            process_stage.add_progress_update(f"Pipeline failed: {error_type}")
            print(f"Pipeline failed: {e}")
            sys.exit(1)

    finally:
        # Always end the stage and save summary
        run_summary.end_stage("process")
        await save_process_summary(run_summary, book_manager)

        # Display completion summary
        display_step_summary(run_summary, "process")


# Exported functions for use by other modules


def _create_processing_client(library_directory: str, secrets_dir: str | None = None) -> ProcessingClient:
    """Create a ProcessingClient instance with consistent configuration.

    Args:
        library_directory: GRIN library directory
        secrets_dir: Directory containing GRIN secrets files

    Returns:
        Configured ProcessingClient instance
    """
    return ProcessingClient(
        directory=library_directory,
        secrets_dir=secrets_dir,
    )


async def request_conversion(barcode: str, library_directory: str, secrets_dir: str | None = None) -> str:
    """Request conversion for a single book.

    Args:
        barcode: Book barcode to request conversion for
        library_directory: GRIN library directory
        secrets_dir: Directory containing GRIN secrets files

    Returns:
        Status message from GRIN

    Raises:
        KeyError: If barcode not found in results
        aiohttp.ClientResponseError: For HTTP errors (including 429 Too Many Requests)
        IndexError: For malformed response format
    """
    processing_client = _create_processing_client(library_directory, secrets_dir)

    try:
        results = await processing_client.request_processing_batch([barcode])
        return results[barcode]  # Let KeyError bubble up if barcode missing
    finally:
        await processing_client.cleanup()


async def request_conversions_batch(
    barcodes: list[str], library_directory: str, secrets_dir: str | None = None
) -> dict[str, str]:
    """Request conversion for a batch of books.

    Args:
        barcodes: List of book barcodes to request conversion for
        library_directory: GRIN library directory
        secrets_dir: Directory containing GRIN secrets files

    Returns:
        Dict mapping barcode to status message

    Raises:
        aiohttp.ClientResponseError: For HTTP errors (including 429 Too Many Requests)
        IndexError: For malformed response format
    """
    processing_client = _create_processing_client(library_directory, secrets_dir)

    try:
        return await processing_client.request_processing_batch(barcodes)
    finally:
        await processing_client.cleanup()


async def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="GRIN book processing management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python grin.py process request --run-name harvard_2024
  python grin.py process request --run-name harvard_2024 --limit 100
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Request command
    request_parser = subparsers.add_parser(
        "request",
        help="Request processing for books",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Request processing for books from run
  python grin.py process request --run-name harvard_2024

  # Request processing with limits
  python grin.py process request --run-name harvard_2024 --limit 100

  # Process specific barcodes
  python grin.py process request --run-name harvard_2024 --barcodes "YL1BTJ,ABC123,XYZ789"
        """,
    )

    request_parser.add_argument("--run-name", required=True, help="Run name (e.g., harvard_2024)")

    # Processing options
    request_parser.add_argument("--batch-size", type=int, default=200, help="Batch size for processing (default: 200)")
    request_parser.add_argument(
        "--max-in-process", type=int, default=50000, help="Maximum books in GRIN queue (default: 50000)"
    )
    request_parser.add_argument("--limit", type=int, help="Limit number of processing requests to make")
    request_parser.add_argument(
        "--barcodes", help="Comma-separated list of specific barcodes to process (e.g., '12345,67890,abcde')"
    )
    request_parser.add_argument(
        "--barcodes-file",
        help="Path to a text file containing barcodes to process (one per line, supports comments with #)",
    )
    request_parser.add_argument(
        "--queue-report-interval", type=int, default=30, help="Queue status reporting interval in seconds (default: 30)"
    )

    # GRIN options
    request_parser.add_argument(
        "--secrets-dir", help="Directory containing GRIN secrets files (auto-detected from run config if not specified)"
    )

    # Logging
    request_parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "request":
        await cmd_request(args)


if __name__ == "__main__":
    asyncio.run(main())
