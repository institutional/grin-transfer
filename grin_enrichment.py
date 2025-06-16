#!/usr/bin/env python3
"""
GRIN Metadata Enrichment Pipeline

Reads books from SQLite database and enriches them with detailed GRIN metadata.
This is a separate pipeline step that runs after CSV export.
"""

import argparse
import asyncio
import logging
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# Check Python version requirement
from client import GRINClient
from collect_books.models import BookRecord, SQLiteProgressTracker
from common import format_duration, pluralize

logger = logging.getLogger(__name__)


class GRINEnrichmentPipeline:
    """Pipeline for enriching book records with detailed GRIN metadata."""

    def __init__(
        self,
        directory: str = "Harvard",
        db_path: str = "output/default/books.db",
        rate_limit_delay: float = 0.2,  # 5 QPS
        batch_size: int = 1000,
        grin_batch_size: int = 200,  # Barcodes per GRIN request
        max_concurrent_requests: int = 5,  # Concurrent GRIN requests
        secrets_dir: str | None = None,  # Directory containing secrets files
        timeout: int = 60,
    ):
        self.directory = directory
        self.db_path = db_path
        self.rate_limit_delay = rate_limit_delay
        self.batch_size = batch_size
        self.grin_batch_size = grin_batch_size
        self.max_concurrent_requests = max_concurrent_requests
        self.timeout = timeout

        # Initialize components
        self.grin_client = GRINClient(timeout=timeout, secrets_dir=secrets_dir)
        self.sqlite_tracker = SQLiteProgressTracker(db_path)

        # Rate limiting semaphore for concurrent requests
        self._rate_limit_semaphore = asyncio.Semaphore(max_concurrent_requests)
        self._request_timestamps: list[float] = []  # Track request timestamps for rate limiting


        # Track if pipeline is shutting down for cleanup
        self._shutdown_requested = False

    async def cleanup(self) -> None:
        """Clean up resources and close connections safely."""
        if self._shutdown_requested:
            return  # Already cleaning up

        self._shutdown_requested = True
        logger.info("Shutting down enrichment pipeline...")

        try:
            # Close GRIN client session
            if hasattr(self.grin_client, "session") and self.grin_client.session:
                await self.grin_client.session.close()
                logger.debug("Closed GRIN client session")
        except Exception as e:
            logger.warning(f"Error closing GRIN client session: {e}")

        try:
            # Close SQLite tracker connections
            if hasattr(self.sqlite_tracker, "_db") and self.sqlite_tracker._db:
                await self.sqlite_tracker._db.close()
                logger.debug("Closed SQLite database connection")
        except Exception as e:
            logger.warning(f"Error closing database connection: {e}")

        logger.info("Cleanup completed")

    async def _rate_limit(self) -> None:
        """Apply rate limiting for concurrent GRIN requests."""
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

    def _calculate_max_batch_size(self, barcodes: list[str]) -> int:
        """Calculate maximum batch size that fits in URL length limit."""
        if not barcodes:
            return self.grin_batch_size

        # Base URL components
        base_url = f"https://books.google.com/libraries/{self.directory}/_barcode_search?execute_query=true&format=text&mode=full&barcodes="
        max_url_length = 7500  # Conservative limit for HTTP headers
        available_length = max_url_length - len(base_url)

        # Build barcode string incrementally until we hit the limit
        barcode_string = ""
        max_batch_size = 0
        
        for i, barcode in enumerate(barcodes):
            # Add space separator if not first barcode
            test_string = barcode_string + (" " if barcode_string else "") + barcode
            
            if len(test_string) <= available_length:
                barcode_string = test_string
                max_batch_size = i + 1
            else:
                break  # Would exceed URL limit
        
        # Ensure we have at least 1 barcode per batch
        result = max(1, max_batch_size)
        logger.debug(f"Calculated max batch size: {result} (total URL length: {len(base_url + barcode_string)})")
        return result

    async def fetch_grin_metadata_batch(self, barcodes: list[str]) -> dict[str, dict | None]:
        """Fetch detailed GRIN metadata for a batch of barcodes."""
        try:
            # Use GRIN's _barcode_search endpoint with space-delimited barcodes
            barcode_list = " ".join(barcodes)
            search_url = f"_barcode_search?execute_query=true&format=text&mode=full&barcodes={barcode_list}"

            # Check URL length and split if too long (HTTP headers have ~8KB limit)
            base_url = f"https://books.google.com/libraries/{self.directory}/"
            full_url = base_url + search_url
            max_url_length = 7500  # Conservative limit to stay under 8KB header limit

            if len(full_url) > max_url_length:
                logger.warning(f"URL too long ({len(full_url)} chars) for {len(barcodes)} barcodes, splitting batch")
                # Split the batch in half and process recursively
                mid = len(barcodes) // 2
                if mid == 0:
                    # Single barcode causing issues - skip it
                    logger.error(f"Single barcode {barcodes[0]} causes URL too long, skipping")
                    return {barcodes[0]: None}

                first_half = await self.fetch_grin_metadata_batch(barcodes[:mid])
                second_half = await self.fetch_grin_metadata_batch(barcodes[mid:])

                # Merge results
                result = {}
                result.update(first_half)
                result.update(second_half)
                return result

            response_text = await self.grin_client.fetch_resource(self.directory, search_url)

            # Parse TSV response
            lines = response_text.strip().split("\n")
            if len(lines) < 2:
                logger.debug(f"Insufficient TSV data for batch of {len(barcodes)} barcodes")
                return dict.fromkeys(barcodes)

            headers = lines[0].split("\t")
            results: dict[str, dict[str, str] | None] = {}

            # Process each data line (skip header)
            for i, line in enumerate(lines[1:], 1):
                values = line.split("\t")

                # Get barcode from first column
                if not values or not values[0]:
                    logger.debug(f"Empty barcode in line {i}")
                    continue

                barcode = values[0]

                # Pad values with empty strings if there are fewer values than headers
                if len(values) < len(headers):
                    missing_count = len(headers) - len(values)
                    logger.debug(
                        f"Padding {barcode}: {len(headers)} headers, {len(values)} values - "
                        f"adding {missing_count} empty values"
                    )
                    values.extend([""] * (len(headers) - len(values)))
                elif len(values) > len(headers):
                    # This shouldn't happen, but handle it gracefully
                    logger.warning(
                        f"More values than headers for {barcode}: {len(headers)} headers, "
                        f"{len(values)} values - truncating values"
                    )
                    values = values[: len(headers)]

                # Create mapping and extract enrichment fields
                data_map = dict(zip(headers, values, strict=False))

                enrichment_data = {
                    "grin_state": data_map.get("State", ""),
                    "viewability": data_map.get("Viewability", ""),
                    "opted_out": data_map.get("Opted-Out (post-scan)", ""),
                    "conditions": data_map.get("Conditions", ""),
                    "scannable": data_map.get("Scannable", ""),
                    "tagging": data_map.get("Tagging", ""),
                    "audit": data_map.get("Audit", ""),
                    "material_error_percent": data_map.get("Material Error%", ""),
                    "overall_error_percent": data_map.get("Overall Error%", ""),
                    "claimed": data_map.get("Claimed", ""),
                    "ocr_analysis_score": data_map.get("OCR Analysis Score", ""),
                    "ocr_gtd_score": data_map.get("OCR GTD Score", ""),
                    "digitization_method": data_map.get("Digitization Method", ""),
                }

                results[barcode] = enrichment_data
                logger.debug(
                    f"Enriched {barcode}: State={enrichment_data.get('grin_state')}, "
                    f"Viewability={enrichment_data.get('viewability')}"
                )

            # Ensure all requested barcodes have entries (even if None)
            for barcode in barcodes:
                if barcode not in results:
                    logger.debug(f"No data returned for {barcode}")
                    results[barcode] = None

            return results

        except Exception as e:
            logger.error(f"Failed to fetch GRIN metadata for batch of {len(barcodes)} barcodes: {e}")
            return dict.fromkeys(barcodes)

    async def _fetch_batch_with_rate_limiting(self, barcodes: list[str]) -> dict[str, dict | None]:
        """Fetch a batch with concurrent rate limiting using semaphore."""
        async with self._rate_limit_semaphore:
            # Apply rate limiting
            await self._rate_limit()

            # Fetch the batch
            try:
                return await self.fetch_grin_metadata_batch(barcodes)
            except Exception as e:
                logger.error(f"Error in concurrent batch fetch: {e}")
                return dict.fromkeys(barcodes)

    async def enrich_books_batch(self, barcodes: list[str]) -> int:
        """Enrich a batch of books with GRIN metadata using concurrent requests."""
        enriched_count = 0

        # Split barcodes into GRIN batches, calculating max size for each batch
        grin_batches = []
        remaining_barcodes = barcodes[:]
        
        while remaining_barcodes:
            # Calculate maximum batch size that fits in URL for remaining barcodes
            max_url_batch_size = self._calculate_max_batch_size(remaining_barcodes)
            
            # Use the smaller of user's setting or URL limit
            effective_batch_size = min(self.grin_batch_size, max_url_batch_size, len(remaining_barcodes))
            
            # Take the calculated number of barcodes for this batch
            grin_batch = remaining_barcodes[:effective_batch_size]
            grin_batches.append(grin_batch)
            
            # Remove processed barcodes
            remaining_barcodes = remaining_barcodes[effective_batch_size:]

        batch_sizes = [len(batch) for batch in grin_batches]
        avg_batch_size = sum(batch_sizes) / len(batch_sizes) if batch_sizes else 0
        print(
            f"  → Split into {len(grin_batches)} GRIN API calls (sizes: {batch_sizes}, avg: {avg_batch_size:.1f}) "
            f"with up to {self.max_concurrent_requests} concurrent requests"
        )

        # Process batches concurrently with rate limiting
        batch_tasks = []
        for grin_batch in grin_batches:
            task = self._fetch_batch_with_rate_limiting(grin_batch)
            batch_tasks.append((grin_batch, task))

        # Execute all batch requests concurrently
        try:
            batch_results_list = await asyncio.gather(*[task for _, task in batch_tasks], return_exceptions=True)

            # Process results from each batch
            for (grin_batch, _), batch_results in zip(batch_tasks, batch_results_list, strict=False):
                try:
                    # Handle exceptions from gather
                    if isinstance(batch_results, Exception):
                        logger.error(f"Batch fetch failed for {len(grin_batch)} barcodes: {batch_results}")
                        batch_results = dict.fromkeys(grin_batch)

                    # Process each barcode result
                    for barcode in grin_batch:
                        try:
                            enrichment_data = batch_results.get(barcode) if isinstance(batch_results, dict) else None

                            if enrichment_data:
                                # Update book record in database
                                success = await self.sqlite_tracker.update_book_enrichment(barcode, enrichment_data)
                                if success:
                                    enriched_count += 1
                                    logger.debug(f"Successfully enriched {barcode}")
                                else:
                                    logger.warning(f"Failed to update database for {barcode}")
                            else:
                                # Still mark as processed with empty enrichment timestamp
                                await self.sqlite_tracker.update_book_enrichment(barcode, {})
                                logger.debug(f"No enrichment data found for {barcode}")

                        except Exception as e:
                            logger.error(f"Error processing {barcode}: {e}")
                            # Mark as processed even if failed to avoid reprocessing
                            try:
                                await self.sqlite_tracker.update_book_enrichment(barcode, {})
                            except Exception:
                                pass

                except Exception as e:
                    logger.error(f"Error processing batch results: {e}")
                    # Mark all barcodes in this batch as processed (empty) to avoid reprocessing
                    for barcode in grin_batch:
                        try:
                            await self.sqlite_tracker.update_book_enrichment(barcode, {})
                        except Exception:
                            pass

        except Exception as e:
            logger.error(f"Critical error in concurrent batch processing: {e}")
            # Fallback: mark all barcodes as processed to avoid infinite loops
            for barcode in barcodes:
                try:
                    await self.sqlite_tracker.update_book_enrichment(barcode, {})
                except Exception:
                    pass

        return enriched_count

    async def reset_enrichment_data(self) -> int:
        """Reset enrichment data for all books in the database."""
        import aiosqlite

        async with aiosqlite.connect(self.db_path) as conn:
            # Reset enrichment fields to NULL
            cursor = await conn.execute(
                """
                UPDATE books SET
                    grin_state = NULL,
                    viewability = NULL,
                    opted_out = NULL,
                    conditions = NULL,
                    scannable = NULL,
                    tagging = NULL,
                    audit = NULL,
                    material_error_percent = NULL,
                    overall_error_percent = NULL,
                    claimed = NULL,
                    ocr_analysis_score = NULL,
                    ocr_gtd_score = NULL,
                    digitization_method = NULL,
                    enrichment_timestamp = NULL,
                    updated_at = ?
                WHERE enrichment_timestamp IS NOT NULL
            """,
                (datetime.now(UTC).isoformat(),),
            )

            reset_count = cursor.rowcount
            await conn.commit()
            return reset_count

    async def run_enrichment(self, limit: int | None = None, resume: bool = True, reset: bool = False) -> None:
        """Run the complete enrichment pipeline."""
        print("Starting GRIN metadata enrichment pipeline")
        print(f"Database: {self.db_path}")
        print(f"Directory: {self.directory}")
        print(f"Rate limit: {1 / self.rate_limit_delay:.1f} requests/second")
        print(f"Concurrent requests: {self.max_concurrent_requests}")
        print(f"GRIN batch size: {self.grin_batch_size} barcodes per request")
        if limit:
            print(f"Limit: {limit:,} {pluralize(limit, 'book')}")
        if reset:
            print("Reset mode: Will clear existing enrichment data")
        print()

        # Reset enrichment data if requested
        if reset:
            print("Resetting enrichment data...")
            reset_count = await self.reset_enrichment_data()
            print(f"Reset enrichment data for {reset_count:,} books")
            print()

        # Validate credentials
        print("Validating GRIN credentials...")
        try:
            await self.grin_client.auth.validate_credentials(self.directory)
        except Exception as e:
            print(f"❌ Credential validation failed: {e}")
            return
        print()

        # Get initial counts
        total_books = await self.sqlite_tracker.get_book_count()
        enriched_books = await self.sqlite_tracker.get_enriched_book_count()
        remaining_books = total_books - enriched_books

        print("Database status:")
        print(f"  Total books: {total_books:,}")
        print(f"  Already enriched: {enriched_books:,}")
        print(f"  Remaining: {remaining_books:,}")
        print()

        if remaining_books == 0:
            print("✅ All books are already enriched!")
            return

        # Start enrichment
        start_time = time.time()
        processed_count = 0
        total_enriched = 0

        print(f"Starting enrichment of {remaining_books:,} books...")

        try:
            while True:
                # Check for shutdown request
                if self._shutdown_requested:
                    print("Shutdown requested, stopping enrichment...")
                    break

                # Get batch of books that need enrichment
                if limit:
                    batch_limit = min(self.batch_size, limit - processed_count)
                    if batch_limit <= 0:
                        break
                else:
                    batch_limit = self.batch_size

                barcodes = await self.sqlite_tracker.get_books_for_enrichment(batch_limit)
                if not barcodes:
                    print("✅ No more books to enrich")
                    break

                batch_start = time.time()
                print(f"Processing batch of {len(barcodes)} books...")

                # Enrich the batch
                enriched_in_batch = await self.enrich_books_batch(barcodes)

                batch_elapsed = time.time() - batch_start
                processed_count += len(barcodes)
                total_enriched += enriched_in_batch

                # Progress report
                overall_elapsed = time.time() - start_time
                rate = processed_count / max(1, overall_elapsed)

                print(f"  Batch completed: {enriched_in_batch}/{len(barcodes)} enriched in {batch_elapsed:.1f}s")
                print(f"  Overall progress: {processed_count:,}/{remaining_books:,} processed ({rate:.1f} books/sec)")

                if limit and processed_count >= limit:
                    print(f"Reached limit of {limit} books")
                    break

                # Small delay between batches
                await asyncio.sleep(0.1)

        except KeyboardInterrupt:
            print("\nEnrichment interrupted by user")
            print("Cleaning up resources...")

        except Exception as e:
            print(f"\nEnrichment failed: {e}")
            import traceback

            traceback.print_exc()

        finally:
            # Clean up resources
            await self.cleanup()

            # Final statistics
            total_elapsed = time.time() - start_time
            final_enriched = await self.sqlite_tracker.get_enriched_book_count()

            print("\nEnrichment pipeline completed:")
            print(f"  Total runtime: {format_duration(total_elapsed)}")
            print(f"  Books processed: {processed_count:,}")
            print(f"  Books enriched: {total_enriched:,}")
            print(f"  Total enriched in database: {final_enriched:,}")
            print(f"  Average rate: {processed_count / max(1, total_elapsed):.1f} books/second")


async def export_enriched_csv(db_path: str, output_file: str) -> None:
    """Export all books (including enriched data) to CSV."""
    print(f"Exporting enriched data from {db_path} to {output_file}")

    sqlite_tracker = SQLiteProgressTracker(db_path)

    try:
        books = await sqlite_tracker.get_all_books_csv_data()

        print(f"Found {len(books):,} books in database")

        # Write CSV
        import csv

        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(BookRecord.csv_headers())

            for book in books:
                writer.writerow(book.to_csv_row())

        print(f"✅ Exported {len(books):,} books to {output_file}")

    finally:
        # Clean up database connections
        try:
            if hasattr(sqlite_tracker, "_db") and sqlite_tracker._db:
                await sqlite_tracker._db.close()
        except Exception:
            pass


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
        from pathlib import Path

        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # Add console handler for INFO and above
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Suppress debug logs from dependencies
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("aiosqlite").setLevel(logging.WARNING)

    # Also suppress other noisy loggers
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("concurrent.futures").setLevel(logging.WARNING)


def validate_database_file(db_path: str) -> None:
    """
    Validate that the database file exists and is a valid SQLite database.

    Args:
        db_path: Path to the SQLite database file

    Raises:
        SystemExit: If the database file is invalid or inaccessible
    """
    import sqlite3

    db_file = Path(db_path)

    # Check if file exists
    if not db_file.exists():
        print(f"❌ Error: Database file does not exist: {db_path}")
        print("\nMake sure you've run a book collection first:")
        print("python collect_books.py --run-name <your_run_name>")
        print("\nOr check available databases:")

        # Try to show available databases
        output_dir = Path("output")
        if output_dir.exists():
            print("\nAvailable run directories:")
            for run_dir in output_dir.iterdir():
                if run_dir.is_dir():
                    db_file_path = run_dir / "books.db"
                    if db_file_path.exists():
                        print(f"  {db_file_path}")

        sys.exit(1)

    # Check if it's a valid SQLite database
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()

            # Check for expected tables
            table_names = [table[0] for table in tables]
            required_tables = ["books", "processed", "failed"]
            missing_tables = [table for table in required_tables if table not in table_names]

            if missing_tables:
                print(f"❌ Error: Database is missing required tables: {missing_tables}")
                print(f"Database file: {db_path}")
                print("This doesn't appear to be a valid book collection database.")
                sys.exit(1)

    except sqlite3.Error as e:
        print(f"❌ Error: Cannot read SQLite database: {e}")
        print(f"Database file: {db_path}")
        print("The file may be corrupted or not a valid SQLite database.")
        sys.exit(1)

    # If we get here, the database is valid
    print(f"✅ Using database: {db_path}")


async def main() -> None:
    """Main CLI entry point."""
    import signal

    # Set up signal handlers for graceful shutdown
    def signal_handler(signum: int, frame: Any) -> None:
        print(f"\nReceived signal {signum}, shutting down gracefully...")
        raise KeyboardInterrupt()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    parser = argparse.ArgumentParser(
        description="GRIN metadata enrichment pipeline", formatter_class=argparse.RawDescriptionHelpFormatter
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Enrichment command
    enrich_parser = subparsers.add_parser("enrich", help="Enrich books with GRIN metadata")
    enrich_parser.add_argument("db_path", help="SQLite database path (e.g., output/harvard_2024/books.db)")
    enrich_parser.add_argument("--directory", default="Harvard", help="GRIN directory")
    enrich_parser.add_argument(
        "--rate-limit", type=float, default=0.2, help="Delay between requests (default: 0.2s for 5 QPS)"
    )
    enrich_parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for processing")
    enrich_parser.add_argument(
        "--grin-batch-size", type=int, default=200, help="Number of barcodes per GRIN request (default: 200)"
    )
    enrich_parser.add_argument(
        "--max-concurrent", type=int, default=5, help="Maximum concurrent GRIN requests (default: 5)"
    )
    enrich_parser.add_argument("--limit", type=int, help="Limit number of books to process")
    enrich_parser.add_argument(
        "--reset", action="store_true", help="Reset enrichment data for all books before enriching"
    )
    enrich_parser.add_argument(
        "--secrets-dir", help="Directory containing GRIN secrets files (searches home directory if not specified)"
    )
    enrich_parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")

    # Export command
    export_parser = subparsers.add_parser("export-csv", help="Export enriched data to CSV")
    export_parser.add_argument("db_path", help="SQLite database path (e.g., output/harvard_2024/books.db)")
    export_parser.add_argument("--output", default="books_enriched.csv", help="Output CSV file")
    export_parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")

    # Status command
    status_parser = subparsers.add_parser("status", help="Show enrichment status")
    status_parser.add_argument("db_path", help="SQLite database path (e.g., output/harvard_2024/books.db)")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Validate database file exists and is accessible
    validate_database_file(args.db_path)

    # Generate log file name based on command and database
    if args.command in ["enrich", "export-csv"]:
        from datetime import datetime
        from pathlib import Path

        db_name = Path(args.db_path).stem
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"logs/grin_enrichment_{args.command}_{db_name}_{timestamp}.log"

        setup_logging(args.log_level, log_file)
        print(f"Logging to file: {log_file}")

    try:
        match args.command:
            case "enrich":
                pipeline = GRINEnrichmentPipeline(
                    directory=args.directory,
                    db_path=args.db_path,
                    rate_limit_delay=args.rate_limit,
                    batch_size=args.batch_size,
                    grin_batch_size=args.grin_batch_size,
                    max_concurrent_requests=args.max_concurrent,
                    secrets_dir=args.secrets_dir,
                )
                await pipeline.run_enrichment(limit=args.limit, reset=args.reset)

            case "export-csv":
                await export_enriched_csv(args.db_path, args.output)

            case "status":
                sqlite_tracker = SQLiteProgressTracker(args.db_path)
                try:
                    total_books = await sqlite_tracker.get_book_count()
                    enriched_books = await sqlite_tracker.get_enriched_book_count()

                    print("Enrichment Status:")
                    print(f"  Database: {args.db_path}")
                    print(f"  Total books: {total_books:,}")
                    print(f"  Enriched books: {enriched_books:,}")
                    print(f"  Remaining: {total_books - enriched_books:,}")
                    print(f"  Progress: {enriched_books / max(1, total_books) * 100:.1f}%")
                finally:
                    # Clean up database connections
                    try:
                        if hasattr(sqlite_tracker, "_db") and sqlite_tracker._db:
                            await sqlite_tracker._db.close()
                    except Exception:
                        pass

    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        print(f"Operation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
