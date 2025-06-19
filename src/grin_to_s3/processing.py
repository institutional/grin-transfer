#!/usr/bin/env python3
"""
GRIN Book Processing Management

Unified script to request and monitor book processing via GRIN.
Based on v1 conversion system with modern async architecture.
"""

import argparse
import asyncio
import logging
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from grin_to_s3.client import GRINClient
from grin_to_s3.collect_books.models import SQLiteProgressTracker
from grin_to_s3.common import format_duration, pluralize
from grin_to_s3.run_config import apply_run_config_to_args, setup_run_database_path

logger = logging.getLogger(__name__)


class ProcessingRequestError(Exception):
    """Raised when book processing request fails."""


class ProcessingClient:
    """Client for requesting book processing via GRIN _process endpoint."""

    def __init__(
        self,
        directory: str = "Harvard",
        rate_limit_delay: float = 0.2,  # 5 QPS
        secrets_dir: str | None = None,
        timeout: int = 60,
    ):
        self.directory = directory
        self.rate_limit_delay = rate_limit_delay
        self.grin_client = GRINClient(timeout=timeout, secrets_dir=secrets_dir)
        self._request_timestamps: list[float] = []

    async def cleanup(self) -> None:
        """Clean up resources and close connections safely."""
        try:
            if hasattr(self.grin_client, "session") and self.grin_client.session:
                await self.grin_client.session.close()
                logger.debug("Closed GRIN client session")
        except Exception as e:
            logger.warning(f"Error closing GRIN client session: {e}")

    async def _rate_limit(self) -> None:
        """Apply rate limiting for GRIN requests."""
        if self.rate_limit_delay <= 0:
            return

        current_time = time.time()

        # Clean up old timestamps (keep only last second for QPS calculation)
        cutoff_time = current_time - 1.0
        self._request_timestamps = [t for t in self._request_timestamps if t > cutoff_time]

        # Calculate how many requests we can make per second
        max_requests_per_second = 1.0 / self.rate_limit_delay

        # If we're at the rate limit, wait until we can make another request
        if len(self._request_timestamps) >= max_requests_per_second:
            # Wait until the oldest timestamp is more than 1 second old
            oldest_timestamp = min(self._request_timestamps)
            wait_time = oldest_timestamp + 1.0 - current_time
            if wait_time > 0:
                await asyncio.sleep(wait_time)
                current_time = time.time()

        # Record this request timestamp
        self._request_timestamps.append(current_time)

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

        await self._rate_limit()

        try:
            # Use GRIN's _process endpoint with comma-separated barcodes
            barcodes_param = ",".join(barcodes)
            process_url = f"_process?barcodes={barcodes_param}"
            response_text = await self.grin_client.fetch_resource(self.directory, process_url)

            # Parse TSV response (should be "Barcode\tStatus" format)
            lines = response_text.strip().split("\n")
            if len(lines) < 2:
                raise ProcessingRequestError(f"Invalid response format: got {len(lines)} lines, expected at least 2")

            # Check header
            header = lines[0]
            expected_header = "Barcode\tStatus"
            if header != expected_header:
                raise ProcessingRequestError(f"Unexpected response header: '{header}', expected '{expected_header}'")

            # Parse results for all barcodes
            results = {}
            result_lines = lines[1:]

            if len(result_lines) != len(barcodes):
                logger.warning(f"Expected {len(barcodes)} result lines, got {len(result_lines)}")

            for line in result_lines:
                if not line.strip():
                    continue

                parts = line.split("\t")
                if len(parts) != 2:
                    raise ProcessingRequestError(f"Invalid result format: '{line}'")

                returned_barcode, status = parts
                results[returned_barcode] = status

                if status == "Success":
                    logger.info(f"Successfully requested processing for {returned_barcode}")
                else:
                    logger.warning(f"Processing request failed for {returned_barcode}: {status}")

            return results

        except Exception as e:
            if isinstance(e, ProcessingRequestError):
                raise
            else:
                raise ProcessingRequestError(f"Batch request failed for {len(barcodes)} books: {e}") from e

    async def request_processing(self, barcode: str) -> str:
        """
        Request processing for a single book (wrapper around batch method).

        Args:
            barcode: Book barcode to request processing for

        Returns:
            Success status message

        Raises:
            ProcessingRequestError: If the request fails
        """
        results = await self.request_processing_batch([barcode])
        if barcode not in results:
            raise ProcessingRequestError(f"No result returned for {barcode}")

        status = results[barcode]
        if status != "Success":
            raise ProcessingRequestError(f"Processing request failed for {barcode}: {status}")

        return status

    async def get_in_process_books(self) -> set[str]:
        """Get list of books currently in processing queue."""
        try:
            response_text = await self.grin_client.fetch_resource(self.directory, "_in_process?format=text")
            lines = response_text.strip().split("\n")
            return {line.strip() for line in lines if line.strip()}
        except Exception as e:
            logger.warning(f"Failed to get in-process books: {e}")
            return set()

    async def get_failed_books(self) -> set[str]:
        """Get list of books that failed processing."""
        try:
            response_text = await self.grin_client.fetch_resource(self.directory, "_failed?format=text")
            lines = response_text.strip().split("\n")
            return {line.strip() for line in lines if line.strip()}
        except Exception as e:
            logger.warning(f"Failed to get failed books: {e}")
            return set()

    async def get_converted_books(self) -> set[str]:
        """Get list of books that have been converted (available for download)."""
        try:
            response_text = await self.grin_client.fetch_resource(self.directory, "_converted?format=text")
            lines = response_text.strip().split("\n")
            # Remove .tar.gz.gpg suffix to get barcodes
            converted_barcodes = set()
            for line in lines:
                if line.strip() and ".tar.gz.gpg" in line:
                    barcode = line.strip().replace(".tar.gz.gpg", "")
                    converted_barcodes.add(barcode)
            return converted_barcodes
        except Exception as e:
            logger.warning(f"Failed to get converted books: {e}")
            return set()


class ProcessingPipeline:
    """Pipeline for requesting book processing with database tracking."""

    def __init__(
        self,
        db_path: str,
        directory: str = "Harvard",
        rate_limit_delay: float = 0.2,  # 5 QPS
        batch_size: int = 200,  # Increased from 100 based on testing
        max_in_process: int = 50000,  # GRIN's max queue limit
        secrets_dir: str | None = None,
    ):
        self.db_path = db_path
        self.directory = directory
        self.rate_limit_delay = rate_limit_delay
        self.batch_size = batch_size
        self.max_in_process = max_in_process

        # Initialize components
        self.processing_client = ProcessingClient(
            directory=directory,
            rate_limit_delay=rate_limit_delay,
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
            if hasattr(self.db_tracker, "_db") and self.db_tracker._db:
                await self.db_tracker._db.close()
        except Exception as e:
            logger.warning(f"Error closing database connection: {e}")

    async def get_processing_status(self) -> dict:
        """Get current processing status from GRIN."""
        in_process = await self.processing_client.get_in_process_books()

        return {
            "in_process": len(in_process),
            "queue_space": max(0, self.max_in_process - len(in_process)),
        }

    async def run_processing_requests(self, limit: int | None = None) -> None:
        """Run the complete processing request pipeline."""
        print("Starting GRIN book processing request pipeline")
        print(f"Database: {self.db_path}")
        print(f"Directory: {self.directory}")
        print(f"Rate limit: {1 / self.rate_limit_delay:.1f} requests/second")
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
            print("Validating GRIN credentials...")
            try:
                await asyncio.wait_for(
                    self.processing_client.grin_client.auth.validate_credentials(self.directory),
                    timeout=30.0
                )
            except TimeoutError:
                print("❌ Credential validation timed out after 30 seconds")
                return
            except Exception as e:
                print(f"❌ Credential validation failed: {e}")
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

            # Get books from database that could be processed
            total_books = await self.db_tracker.get_book_count()
            print(f"Database contains {total_books:,} books")

            # Get current GRIN in-process books to avoid duplicate requests
            print("Getting current GRIN in-process books to avoid duplicates...")
            try:
                in_process_books = await asyncio.wait_for(
                    self.processing_client.get_in_process_books(),
                    timeout=60.0
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

            books_to_request = min(limit or status["queue_space"], status["queue_space"])
            if books_to_request <= 0:
                print("No requests to make")
                return

            print(f"Will attempt to request processing for up to {books_to_request:,} new books")
            print()

            # Get books from database, filtering out those already in GRIN
            import aiosqlite

            candidate_barcodes: list[str] = []
            fetch_offset = 0
            fetch_batch_size = min(books_to_request * 100, 50000)  # Start with reasonable batch

            print("Searching for books that haven't been requested for processing...")

            while len(candidate_barcodes) < books_to_request and fetch_offset < total_books:
                # Fetch next batch of books from database that haven't been requested
                async with aiosqlite.connect(self.db_path) as db:
                    cursor = await db.execute(
                        """
                        SELECT barcode FROM books
                        WHERE processing_request_timestamp IS NULL
                        ORDER BY created_at LIMIT ? OFFSET ?
                        """,
                        (fetch_batch_size, fetch_offset)
                    )
                    rows = await cursor.fetchall()
                    batch_barcodes = [row[0] for row in rows]

                if not batch_barcodes:
                    # No more unrequested books in database
                    break

                # Filter out books currently in GRIN's processing queue
                new_candidates = [
                    barcode for barcode in batch_barcodes
                    if barcode not in in_process_books
                ]

                candidate_barcodes.extend(new_candidates)
                fetch_offset += len(batch_barcodes)

                print(
                    f"  Searched {fetch_offset:,}/{total_books:,} books, "
                    f"found {len(candidate_barcodes):,} new candidates"
                )

                # If we found enough candidates, we can stop
                if len(candidate_barcodes) >= books_to_request:
                    break

                # If we didn't find any new candidates in this batch, try a larger batch
                if not new_candidates and len(batch_barcodes) == fetch_batch_size:
                    fetch_batch_size = min(fetch_batch_size * 2, 100000)
                    print(f"  No new candidates found, increasing batch size to {fetch_batch_size:,}")

            # Limit to the requested number
            candidate_barcodes = candidate_barcodes[:books_to_request]

            print(f"Final result: {len(candidate_barcodes):,} books ready for processing requests")

            if not candidate_barcodes:
                print("No new books to request processing for - all searched books are already in GRIN system")
                print(f"Searched {fetch_offset:,}/{total_books:,} books in database")
                if fetch_offset < total_books:
                    print("You may want to try again with a higher limit to search more books")
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

    async def _mark_book_as_requested(self, barcode: str) -> None:
        """Mark a book as requested in the database with current timestamp."""
        try:
            # Get the book record from database
            book = await self.db_tracker.get_book(barcode)
            if book:
                # Update the processing request fields using atomic status change
                await self.db_tracker.add_status_change(barcode, "processing_request", "requested")

                # Also update the timestamp field for backwards compatibility
                book.processing_request_timestamp = datetime.now(UTC).isoformat()
                await self.db_tracker.save_book(book)
                logger.debug(f"Marked book {barcode} as requested in database")
            else:
                logger.warning(f"Book {barcode} not found in database when trying to mark as requested")
        except Exception as e:
            logger.error(f"Failed to mark book {barcode} as requested in database: {e}")

    async def _process_single_batch(self, batch: list[str], batch_num: int) -> dict:
        """Process a single batch of books with rate limiting."""
        async with self._batch_semaphore:
            batch_start = time.time()
            batch_size = len(batch)

            print(f"Starting batch {batch_num}: {batch_size} books")

            try:
                # Mark all books in batch as requested in database before making the request
                for barcode in batch:
                    await self._mark_book_as_requested(barcode)

                # Make the batch processing request to GRIN
                batch_results = await self.processing_client.request_processing_batch(batch)

                # Process results
                successful = 0
                failed = 0

                for barcode in batch:
                    if barcode in batch_results:
                        status = batch_results[barcode]
                        if status == "Success":
                            successful += 1
                            logger.info(f"Successfully requested processing for {barcode}")
                        elif status == "Already available for download":
                            successful += 1  # Count as successful since book is ready
                            logger.warning(f"Book {barcode} already processed: {status}")
                            # Update status in database to track this
                            try:
                                await self.db_tracker.add_status_change(
                                    barcode, "processing_request", "already_processed"
                                )
                            except Exception as e:
                                logger.debug(f"Failed to update status for {barcode}: {e}")
                        else:
                            failed += 1
                            logger.error(f"Failed to request processing for {barcode}: {status}")
                    else:
                        failed += 1
                        logger.error(f"No result returned for {barcode}")

                batch_elapsed = time.time() - batch_start
                rate = batch_size / batch_elapsed if batch_elapsed > 0 else 0

                print(
                    f"Completed batch {batch_num}: {successful}/{batch_size} successful "
                    f"in {batch_elapsed:.1f}s ({rate:.1f} req/s)"
                )

                return {
                    "batch_num": batch_num,
                    "total": batch_size,
                    "successful": successful,
                    "failed": failed,
                    "elapsed": batch_elapsed,
                }

            except ProcessingRequestError as e:
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

    async def _process_batches_concurrently(
        self, candidate_barcodes: list[str], limit: int | None, start_time: float
    ) -> None:
        """Process multiple batches concurrently with rate limiting and periodic queue reporting."""
        # Split into batches
        batches: list[list[str]] = []
        for i in range(0, len(candidate_barcodes), self.batch_size):
            batch = candidate_barcodes[i:i + self.batch_size]
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
        tasks = [
            self._process_single_batch(batch, i + 1)
            for i, batch in enumerate(batches)
        ]

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
                        status = await asyncio.wait_for(
                            self.get_processing_status(),
                            timeout=10.0
                        )
                        timestamp = datetime.now().strftime("%H:%M:%S")

                        # Calculate change in queue size since start
                        current_in_process = status['in_process']
                        queue_space = status['queue_space']

                        # Show processing progress if we've made requests
                        progress_info = ""
                        if self.stats['successful'] > 0 or self.stats['failed'] > 0:
                            total_requests = self.stats['successful'] + self.stats['failed']
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


class ProcessingMonitor:
    """Monitor for GRIN book processing status."""

    def __init__(self, directory: str = "Harvard", secrets_dir: str | None = None):
        self.directory = directory
        self.grin_client = GRINClient(secrets_dir=secrets_dir)
        self.db_path: str | None = None

    async def cleanup(self) -> None:
        """Clean up resources and close connections safely."""
        try:
            if hasattr(self.grin_client, "session") and self.grin_client.session:
                await self.grin_client.session.close()
        except Exception as e:
            print(f"Warning: Error closing GRIN client session: {e}")

    async def get_converted_books(self) -> list[str]:
        """Get list of books that have been converted (ready for download)."""
        try:
            response_text = await self.grin_client.fetch_resource(self.directory, "_converted?format=text")
            lines = response_text.strip().split("\n")
            converted_barcodes = []
            for line in lines:
                if line.strip() and ".tar.gz.gpg" in line:
                    barcode = line.strip().replace(".tar.gz.gpg", "")
                    converted_barcodes.append(barcode)
            return converted_barcodes
        except Exception as e:
            print(f"Error getting converted books: {e}")
            return []

    async def get_in_process_books(self) -> list[str]:
        """Get list of books currently in processing queue."""
        try:
            response_text = await self.grin_client.fetch_resource(self.directory, "_in_process?format=text")
            lines = response_text.strip().split("\n")
            return [line.strip() for line in lines if line.strip()]
        except Exception as e:
            print(f"Error getting in-process books: {e}")
            return []

    async def get_failed_books(self) -> list[str]:
        """Get list of books that failed processing."""
        try:
            response_text = await self.grin_client.fetch_resource(self.directory, "_failed?format=text")
            lines = response_text.strip().split("\n")
            return [line.strip() for line in lines if line.strip()]
        except Exception as e:
            print(f"Error getting failed books: {e}")
            return []

    async def get_requested_books(self) -> set[str]:
        """Get list of books that were requested for processing by this run."""
        if not self.db_path:
            return set()

        # Check if database file exists before attempting to connect
        from pathlib import Path
        if not Path(self.db_path).exists():
            return set()

        try:
            import sqlite3
            # Use a shorter timeout to avoid hanging in CI
            with sqlite3.connect(self.db_path, timeout=5.0) as conn:
                cursor = conn.cursor()
                # Check if the table exists first
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='books'")
                if not cursor.fetchone():
                    return set()

                # Get all books that were requested by this run, regardless of current status
                cursor.execute("SELECT barcode FROM books WHERE processing_request_timestamp IS NOT NULL")
                rows = cursor.fetchall()
                return {row[0] for row in rows}
        except Exception as e:
            logger.warning(f"Failed to get requested books from database: {e}")
            return set()

    async def show_status_summary(self) -> None:
        """Show overall processing status summary."""
        # Update book statuses in database first
        if self.db_path:
            try:
                status_updates = await self.update_book_statuses()
                total_updates = sum(status_updates.values())
                if total_updates > 0:
                    logger.debug(f"Updated {total_updates} book statuses: {status_updates}")
            except Exception as e:
                logger.debug(f"Status update failed: {e}")

        print("GRIN Processing Status Summary")
        print("=" * 50)
        print(f"Directory: {self.directory}")
        print(f"Checked at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()

        # Get all GRIN status
        all_converted = await self.get_converted_books()
        all_in_process = await self.get_in_process_books()
        all_failed = await self.get_failed_books()

        # Get books requested by this run
        requested_books = await self.get_requested_books()

        # Filter GRIN status to only our requested books
        our_converted = requested_books.intersection(set(all_converted))
        our_in_process = requested_books.intersection(set(all_in_process))
        our_failed = requested_books.intersection(set(all_failed))

        # Calculate totals
        our_total_processed = len(our_converted) + len(our_in_process) + len(our_failed)
        all_total_processed = len(all_converted) + len(all_in_process) + len(all_failed)
        queue_space = 50000 - len(all_in_process)

        print("Books from this run:")
        print(f"  Converted (ready for download): {len(our_converted):>8,}")
        print(f"  In process (being converted):   {len(our_in_process):>8,}")
        print(f"  Failed (conversion failed):     {len(our_failed):>8,}")
        print(f"  Total processed:                {our_total_processed:>8,}")
        print()
        print("All books in GRIN (including other runs):")
        print(f"  Converted (ready for download): {len(all_converted):>8,}")
        print(f"  In process (being converted):   {len(all_in_process):>8,}")
        print(f"  Failed (conversion failed):     {len(all_failed):>8,}")
        print(f"  Total processed:                {all_total_processed:>8,}")
        print(f"  Queue space available:          {queue_space:>8,}")

    async def show_converted_books(self, limit: int = 50) -> None:
        """Show list of converted books ready for download."""
        converted = await self.get_converted_books()

        print(f"\nConverted Books Ready for Download ({len(converted):,} total)")
        print("=" * 60)

        if not converted:
            print("No converted books found.")
            return

        for i, barcode in enumerate(converted[:limit], 1):
            print(f"{i:4}. {barcode}")

        if len(converted) > limit:
            print(f"... and {len(converted) - limit:,} more")
            print(f"\nUse --limit={len(converted)} to see all converted books")

    async def show_in_process_books(self, limit: int = 50) -> None:
        """Show list of books currently being processed."""
        in_process = await self.get_in_process_books()

        print(f"\nBooks Currently Being Processed ({len(in_process):,} total)")
        print("=" * 60)

        if not in_process:
            print("No books currently in process.")
            return

        for i, barcode in enumerate(in_process[:limit], 1):
            print(f"{i:4}. {barcode}")

        if len(in_process) > limit:
            print(f"... and {len(in_process) - limit:,} more")

    async def show_failed_books(self, limit: int = 50) -> None:
        """Show list of books that failed processing."""
        failed = await self.get_failed_books()

        print(f"\nBooks That Failed Processing ({len(failed):,} total)")
        print("=" * 60)

        if not failed:
            print("No failed books.")
            return

        for i, barcode in enumerate(failed[:limit], 1):
            print(f"{i:4}. {barcode}")

        if len(failed) > limit:
            print(f"... and {len(failed) - limit:,} more")

    async def search_barcode(self, barcode: str) -> None:
        """Search for a specific barcode across all GRIN states."""
        print(f"Searching for barcode: {barcode}")
        print("=" * 40)

        converted = await self.get_converted_books()
        in_process = await self.get_in_process_books()
        failed = await self.get_failed_books()

        found = False

        if barcode in converted:
            print("Found in CONVERTED - ready for download!")
            found = True

        if barcode in in_process:
            print("Found in IN_PROCESS - currently being converted")
            found = True

        if barcode in failed:
            print("Found in FAILED - conversion failed")
            found = True

        if not found:
            print("Not found in GRIN system - may not have been requested for processing")

    async def export_converted_list(self, output_file: str) -> None:
        """Export list of converted books to a file."""
        converted = await self.get_converted_books()

        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w') as f:
            f.write("# Converted books ready for download\n")
            f.write(f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# Total: {len(converted):,} books\n")
            f.write("#\n")
            for barcode in converted:
                f.write(f"{barcode}\n")

        print(f"Exported {len(converted):,} converted books to: {output_file}")

    async def get_grin_status_summary(self) -> dict:
        """Get GRIN processing status as a dictionary for change tracking."""
        try:
            # Get all GRIN status
            all_converted = await self.get_converted_books()
            all_in_process = await self.get_in_process_books()
            all_failed = await self.get_failed_books()

            # Get books requested by this run
            requested_books = await self.get_requested_books()

            # Filter GRIN status to only our requested books
            our_converted = requested_books.intersection(set(all_converted))
            our_in_process = requested_books.intersection(set(all_in_process))
            our_failed = requested_books.intersection(set(all_failed))

            # Calculate totals
            our_total_processed = len(our_converted) + len(our_in_process) + len(our_failed)
            all_total_processed = len(all_converted) + len(all_in_process) + len(all_failed)
            queue_space_available = 50000 - len(all_in_process)

            return {
                # Our run stats
                "our_converted": len(our_converted),
                "our_in_process": len(our_in_process),
                "our_failed": len(our_failed),
                "our_total_processed": our_total_processed,
                # All GRIN stats
                "all_converted": len(all_converted),
                "all_in_process": len(all_in_process),
                "all_failed": len(all_failed),
                "all_total_processed": all_total_processed,
                "queue_space_available": queue_space_available,
                "timestamp": datetime.now(UTC).isoformat(),
            }
        except Exception as e:
            logger.warning(f"Failed to get GRIN status summary: {e}")
            return {
                "our_converted": 0,
                "our_in_process": 0,
                "our_failed": 0,
                "our_total_processed": 0,
                "all_converted": 0,
                "all_in_process": 0,
                "all_failed": 0,
                "all_total_processed": 0,
                "queue_space_available": 50000,
                "timestamp": datetime.now(UTC).isoformat(),
                "error": str(e),
            }

    async def update_book_statuses(self) -> dict[str, int]:
        """Update database book statuses based on current GRIN state.

        Returns:
            Dictionary with counts of status updates made.
        """
        if not self.db_path:
            return {}

        try:
            from pathlib import Path

            if not Path(self.db_path).exists():
                return {}

            # Get current GRIN state
            converted_books = set(await self.get_converted_books())
            in_process_books = set(await self.get_in_process_books())
            failed_books = set(await self.get_failed_books())

            # Get books requested by this run
            requested_books = await self.get_requested_books()

            updates = {"converted": 0, "in_process": 0, "failed": 0}

            # Use the database tracker to make atomic status changes
            from grin_to_s3.collect_books.models import SQLiteProgressTracker
            db_tracker = SQLiteProgressTracker(self.db_path)

            # Update books that are now converted
            our_converted = requested_books.intersection(converted_books)
            for barcode in our_converted:
                current_status = await db_tracker.get_latest_status(barcode, "processing_request")
                if current_status != "converted":
                    await db_tracker.add_status_change(barcode, "processing_request", "converted")
                    updates["converted"] += 1

            # Update books that are now in process
            our_in_process = requested_books.intersection(in_process_books)
            for barcode in our_in_process:
                current_status = await db_tracker.get_latest_status(barcode, "processing_request")
                if current_status not in ("converted", "in_process"):
                    await db_tracker.add_status_change(barcode, "processing_request", "in_process")
                    updates["in_process"] += 1

            # Update books that have failed
            our_failed = requested_books.intersection(failed_books)
            for barcode in our_failed:
                current_status = await db_tracker.get_latest_status(barcode, "processing_request")
                if current_status != "failed":
                    await db_tracker.add_status_change(barcode, "processing_request", "failed")
                    updates["failed"] += 1

            return updates

        except Exception as e:
            logger.warning(f"Failed to update book statuses: {e}")
            return {}


def setup_logging(level: str = "INFO", log_file: str | None = None) -> None:
    """Setup logging configuration."""
    log_level = getattr(logging, level.upper())

    # Create formatters
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Set up root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Clear any existing handlers
    root_logger.handlers.clear()

    # Add file handler if log_file specified
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # Add console handler for warnings and above
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Suppress debug logs from dependencies
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("aiosqlite").setLevel(logging.WARNING)


def validate_database_file(db_path: str) -> None:
    """Validate that the database file exists and contains books."""
    import sqlite3

    db_file = Path(db_path)

    if not db_file.exists():
        print(f"Error: Database file does not exist: {db_path}")
        print("\nRun a book collection first:")
        print("python collect_books.py --run-name <your_run_name>")
        sys.exit(1)

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM books")
            count = cursor.fetchone()[0]

            if count == 0:
                print(f"Error: Database contains no books: {db_path}")
                sys.exit(1)

            print(f"Using database: {db_path} ({count:,} books)")

    except sqlite3.Error as e:
        print(f"Error: Cannot read SQLite database: {e}")
        sys.exit(1)


async def cmd_request(args) -> None:
    """Handle the 'request' command."""
    # Apply run configuration defaults
    apply_run_config_to_args(args, args.db_path)

    # Validate database
    validate_database_file(args.db_path)

    # Set up logging
    db_name = Path(args.db_path).stem
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/processing_request_{db_name}_{timestamp}.log"
    setup_logging(args.log_level, log_file)
    print(f"Logging to file: {log_file}")

    # Create and run pipeline
    try:
        pipeline = ProcessingPipeline(
            db_path=args.db_path,
            directory="Harvard",  # Use default if not set
            rate_limit_delay=args.rate_limit,
            batch_size=args.batch_size,
            max_in_process=args.max_in_process,
            secrets_dir=args.secrets_dir,
        )

        # Set queue report interval
        pipeline.queue_report_interval = args.queue_report_interval

        if args.status_only:
            # Just show status
            status = await pipeline.get_processing_status()
            print("GRIN Processing Status:")
            print(f"  In process: {status['in_process']:,}")
            print(f"  Queue space: {status['queue_space']:,}")

            # Show database status
            total_books = await pipeline.db_tracker.get_book_count()
            print("\nDatabase Status:")
            print(f"  Total books: {total_books:,}")

            # Count books by processing request status
            import aiosqlite
            async with aiosqlite.connect(pipeline.db_path) as db:
                cursor = await db.execute(
                    """
                    SELECT COALESCE(h1.status_value, 'no_status') as status, COUNT(*) as count
                    FROM books b
                    LEFT JOIN (
                        SELECT DISTINCT h1.barcode, h1.status_value
                        FROM book_status_history h1
                        INNER JOIN (
                            SELECT barcode, MAX(timestamp) as max_timestamp, MAX(id) as max_id
                            FROM book_status_history
                            WHERE status_type = 'processing_request'
                            GROUP BY barcode
                        ) h2 ON h1.barcode = h2.barcode
                            AND h1.timestamp = h2.max_timestamp
                            AND h1.id = h2.max_id
                        WHERE h1.status_type = 'processing_request'
                    ) h1 ON b.barcode = h1.barcode
                    GROUP BY COALESCE(h1.status_value, 'no_status')
                    """
                )
                status_counts = await cursor.fetchall()
                for status, count in status_counts:
                    status_name = status if status else "not requested"
                    print(f"  {status_name}: {count:,}")
        else:
            # Run processing requests
            await pipeline.run_processing_requests(limit=args.limit)

    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        print(f"Pipeline failed: {e}")
        sys.exit(1)


async def cmd_monitor(args) -> None:
    """Handle the 'monitor' command."""
    try:
        monitor = ProcessingMonitor(
            directory="Harvard",  # Use default if not set
            secrets_dir=args.secrets_dir,
        )

        # Set database path from run argument
        monitor.db_path = args.db_path

        # Validate credentials
        try:
            await monitor.grin_client.auth.validate_credentials("Harvard")
        except Exception as e:
            print(f"Error: Credential validation failed: {e}")
            sys.exit(1)

        if args.watch:
            # Watch mode: continuously poll GRIN every 5 minutes
            await run_watch_mode(monitor, args)
        else:
            # Single execution mode
            await run_single_monitor(monitor, args)

        await monitor.cleanup()

    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


async def run_single_monitor(monitor: "ProcessingMonitor", args) -> None:
    """Run monitor in single execution mode."""
    # Show status summary unless specific action requested
    if not any([args.converted, args.in_process, args.failed, args.search, args.export]):
        await monitor.show_status_summary()

    # Handle specific requests
    if args.converted:
        await monitor.show_converted_books(limit=args.limit)

    if args.in_process:
        await monitor.show_in_process_books(limit=args.limit)

    if args.failed:
        await monitor.show_failed_books(limit=args.limit)

    if args.search:
        await monitor.search_barcode(args.search)

    if args.export:
        await monitor.export_converted_list(args.export)


async def run_watch_mode(monitor: "ProcessingMonitor", args) -> None:
    """Run monitor in watch mode with periodic polling."""
    import signal
    from datetime import UTC, datetime

    print("Starting watch mode - polling GRIN every 5 minutes")
    print("Press Ctrl+C to stop\n")

    # Set up graceful shutdown
    shutdown_requested = False

    def signal_handler(signum: int, frame) -> None:
        nonlocal shutdown_requested
        shutdown_requested = True
        print("\nShutdown requested, stopping watch mode...")

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Track previous state for change detection
    previous_status = None
    poll_count = 0

    while not shutdown_requested:
        poll_count += 1
        timestamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")

        try:
            print(f"[{timestamp}] Poll #{poll_count} - Checking GRIN status...")

            # Get current status
            current_status = await monitor.get_grin_status_summary()

            # Show status summary
            await monitor.show_status_summary()

            # Detect and report changes
            if previous_status is not None:
                changes = detect_status_changes(previous_status, current_status)
                if changes:
                    print("\nChanges detected since last poll:")
                    for change in changes:
                        print(f"  {change}")
                else:
                    print("No changes detected since last poll")

            previous_status = current_status

            if shutdown_requested:
                break

            # Wait 5 minutes (300 seconds) between polls
            print("\nNext poll in 5 minutes...")
            print("-" * 50)

            # Sleep with interruption checking
            for _ in range(300):  # 300 seconds = 5 minutes
                if shutdown_requested:
                    break
                await asyncio.sleep(1)

        except Exception as e:
            print(f"Error during polling: {e}")
            print("Retrying in 5 minutes...")

            # Sleep with interruption checking on error
            for _ in range(300):
                if shutdown_requested:
                    break
                await asyncio.sleep(1)

    print("Watch mode stopped")


def detect_status_changes(previous: dict, current: dict) -> list[str]:
    """Detect changes between status snapshots."""
    changes = []

    # Check for changes in our run metrics (most important)
    our_metrics = [
        ('our_converted', 'Our converted'),
        ('our_in_process', 'Our in process'),
        ('our_failed', 'Our failed'),
        ('our_total_processed', 'Our total processed'),
    ]

    # Check for changes in overall GRIN metrics
    all_metrics = [
        ('all_converted', 'All converted'),
        ('all_in_process', 'All in process'),
        ('all_failed', 'All failed'),
        ('queue_space_available', 'Queue space available'),
    ]

    for metric, label in our_metrics + all_metrics:
        prev_val = previous.get(metric, 0)
        curr_val = current.get(metric, 0)

        if prev_val != curr_val:
            diff = curr_val - prev_val
            if diff > 0:
                changes.append(f"{label}: {prev_val:,} → {curr_val:,} (+{diff:,})")
            else:
                changes.append(f"{label}: {prev_val:,} → {curr_val:,} ({diff:,})")

    return changes


async def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="GRIN book processing management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python grin.py process request --run-name harvard_2024
  python grin.py process request --run-name harvard_2024 --limit 100
  python grin.py process monitor --run-name harvard_2024
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

  # Check current status only
  python grin.py process request --run-name harvard_2024 --status-only

  # Custom rate limiting
  python grin.py process request --run-name harvard_2024 --rate-limit 0.1
        """,
    )

    request_parser.add_argument("--run-name", required=True, help="Run name (e.g., harvard_2024)")

    # Processing options
    request_parser.add_argument(
        "--rate-limit", type=float, default=0.2, help="Delay between requests (default: 0.2s for 5 QPS)"
    )
    request_parser.add_argument("--batch-size", type=int, default=200, help="Batch size for processing (default: 200)")
    request_parser.add_argument(
        "--max-in-process", type=int, default=50000, help="Maximum books in GRIN queue (default: 50000)"
    )
    request_parser.add_argument("--limit", type=int, help="Limit number of processing requests to make")
    request_parser.add_argument(
        "--status-only", action="store_true", help="Only check current status, don't make requests"
    )
    request_parser.add_argument(
        "--queue-report-interval",
        type=int,
        default=30,
        help="Queue status reporting interval in seconds (default: 30)"
    )

    # GRIN options
    request_parser.add_argument(
        "--secrets-dir",
        help="Directory containing GRIN secrets files (auto-detected from run config if not specified)"
    )

    # Logging
    request_parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")

    # Monitor command
    monitor_parser = subparsers.add_parser(
        "monitor",
        help="Monitor processing status",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Show overall status summary
  python grin.py process monitor --run-name harvard_2024

  # Show converted books ready for download
  python grin.py process monitor --run-name harvard_2024 --converted

  # Show books currently being processed
  python grin.py process monitor --run-name harvard_2024 --in-process

  # Show books that failed processing
  python grin.py process monitor --run-name harvard_2024 --failed

  # Search for a specific barcode
  python grin.py process monitor --run-name harvard_2024 --search TZ1XH8

  # Export converted books to file
  python grin.py process monitor --run-name harvard_2024 --export converted_books.txt

  # Show more results
  python grin.py process monitor --run-name harvard_2024 --converted --limit 100

  # Watch mode: continuously monitor with 5-minute polling
  python grin.py process monitor --run-name harvard_2024 --watch
        """,
    )

    monitor_parser.add_argument("--run-name", required=True, help="Run name (e.g., harvard_2024)")
    monitor_parser.add_argument(
        "--secrets-dir",
        help="Directory containing GRIN secrets files (auto-detected from run config if not specified)"
    )

    # What to show
    monitor_parser.add_argument("--converted", action="store_true", help="Show converted books ready for download")
    monitor_parser.add_argument("--in-process", action="store_true", help="Show books currently being processed")
    monitor_parser.add_argument("--failed", action="store_true", help="Show books that failed processing")
    monitor_parser.add_argument("--search", metavar="BARCODE", help="Search for a specific barcode")
    monitor_parser.add_argument("--export", metavar="FILE", help="Export converted books to file")

    # Options
    monitor_parser.add_argument("--limit", type=int, default=50, help="Limit number of results to show (default: 50)")
    monitor_parser.add_argument(
        "--watch",
        action="store_true",
        help="Watch mode: poll GRIN every 5 minutes for status updates"
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Set up database path and apply run configuration
    db_path = setup_run_database_path(args, args.run_name)
    print(f"Using run: {args.run_name}")
    print(f"Database: {db_path}")

    if args.command == "request":
        await cmd_request(args)
    elif args.command == "monitor":
        await cmd_monitor(args)


if __name__ == "__main__":
    asyncio.run(main())
