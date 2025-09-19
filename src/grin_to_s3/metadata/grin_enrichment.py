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

from tenacity import retry, stop_after_attempt, wait_exponential

from grin_to_s3.client import GRINClient
from grin_to_s3.collect_books.models import BookRecord, SQLiteProgressTracker
from grin_to_s3.common import (
    format_duration,
    pluralize,
)
from grin_to_s3.constants import OUTPUT_DIR
from grin_to_s3.database.connections import connect_async
from grin_to_s3.database.database_backup import upload_database_to_storage
from grin_to_s3.database.database_utils import validate_database_file
from grin_to_s3.logging_config import setup_logging
from grin_to_s3.metadata.tsv_parser import parse_grin_tsv
from grin_to_s3.process_summary import (
    create_book_manager_for_uploads,
    create_process_summary,
    display_step_summary,
    get_current_stage,
    save_process_summary,
)
from grin_to_s3.run_config import RunConfig, apply_run_config_to_args, load_run_config
from grin_to_s3.sync.progress_reporter import SlidingWindowRateCalculator

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 6_000  # Will be dynamically split up to optimize concurrency without exceeding URL limits


class GRINEnrichmentPipeline:
    """Pipeline for enriching book records with detailed GRIN metadata."""

    @classmethod
    def from_run_config(
        cls,
        config: RunConfig,
        process_summary_stage,
        rate_limit_delay: float = 0.2,  # 5 QPS
        batch_size: int = DEFAULT_BATCH_SIZE,
        max_concurrent_requests: int = 5,  # Concurrent GRIN requests
        timeout: int = 60,
    ) -> "GRINEnrichmentPipeline":
        """Create GRINEnrichmentPipeline from RunConfig."""
        return cls(
            directory=config.library_directory,
            process_summary_stage=process_summary_stage,
            db_path=config.sqlite_db_path,
            rate_limit_delay=rate_limit_delay,
            batch_size=batch_size,
            max_concurrent_requests=max_concurrent_requests,
            secrets_dir=config.secrets_dir,
            timeout=timeout,
        )

    def __init__(
        self,
        directory: str,
        process_summary_stage,
        db_path: Path,
        rate_limit_delay: float = 0.2,  # 5 QPS
        batch_size: int = DEFAULT_BATCH_SIZE,
        max_concurrent_requests: int = 5,  # Concurrent GRIN requests
        secrets_dir: Path | str | None = None,  # Directory containing secrets files
        timeout: int = 60,
    ):
        self.directory = directory
        self.db_path = db_path
        self.batch_size = batch_size
        self.max_concurrent_requests = max_concurrent_requests
        self.timeout = timeout
        self.process_summary_stage = process_summary_stage

        # Initialize components
        self.grin_client = GRINClient(timeout=timeout, secrets_dir=secrets_dir)
        self.sqlite_tracker = SQLiteProgressTracker(db_path)

        # Concurrency limiting
        self._rate_limit_semaphore = asyncio.Semaphore(max_concurrent_requests)

        # Track if pipeline is shutting down for cleanup
        self._shutdown_requested = False

    async def initialize_resources(self):
        """Initialize async resources that require await."""
        await self.sqlite_tracker.initialize()

    async def cleanup(self) -> None:
        """Clean up resources and close connections safely."""
        if self._shutdown_requested:
            return  # Already cleaning up

        self._shutdown_requested = True
        logger.info("Shutting down enrichment pipeline...")

        if hasattr(self, "grin_client"):
            await self.grin_client.close()
        if hasattr(self, "sqlite_tracker"):
            await self.sqlite_tracker.close()

    def _create_url_batches(self, barcodes: list[str]) -> list[list[str]]:
        """Split barcodes into URL-sized batches.

        Dynamically sizes batches based on actual barcode lengths
        to maximize throughput while staying under HTTP header limits.
        """
        base_url = f"https://books.google.com/libraries/{self.directory}/_barcode_search?execute_query=true&format=text&mode=full&barcodes="
        max_url_length = 7500  # Conservative HTTP header limit
        available = max_url_length - len(base_url)

        batches: list[list[str]] = []
        current_batch: list[str] = []
        current_length = 0

        for barcode in barcodes:
            # +1 for space separator (except first item)
            additional = len(barcode) + (1 if current_batch else 0)

            if current_length + additional > available:
                # Current batch is full, start new one
                if current_batch:
                    batches.append(current_batch)
                current_batch = [barcode]
                current_length = len(barcode)
            else:
                # Add to current batch
                current_batch.append(barcode)
                current_length += additional

        # Add final batch
        if current_batch:
            batches.append(current_batch)

        # Return all batches (will be processed in chunks)
        return batches

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=8), reraise=True)
    async def _fetch_grin_batch(self, barcodes: list[str]) -> str:
        """Fetch TSV data from GRIN with automatic retry."""
        barcode_param = " ".join(barcodes)
        url = f"_barcode_search?execute_query=true&format=text&mode=full&barcodes={barcode_param}"
        return await self.grin_client.fetch_resource(self.directory, url)

    async def _fetch_and_update(self, barcodes: list[str]) -> int:
        """Fetch metadata and update database for a batch.

        Returns:
            Number of successfully enriched books
        """
        async with self._rate_limit_semaphore:
            try:
                # Fetch with automatic retry
                tsv_data = await self._fetch_grin_batch(barcodes)

                # Parse response
                metadata = parse_grin_tsv(tsv_data)

                # Update database
                enriched_count = 0
                for barcode, data in metadata.items():
                    if data and await self.sqlite_tracker.update_book_enrichment(barcode, data):
                        enriched_count += 1
                        self.process_summary_stage.books_enriched += 1
                    else:
                        self.process_summary_stage.enrichment_skipped += 1

                # Mark missing barcodes as attempted
                missing = set(barcodes) - set(metadata.keys())
                for barcode in missing:
                    await self.sqlite_tracker.update_book_enrichment(barcode, {})
                    self.process_summary_stage.enrichment_skipped += 1

                return enriched_count

            except Exception as e:
                logger.error(f"Batch of {len(barcodes)} failed: {e}")
                # Mark all as attempted to prevent infinite retries
                for barcode in barcodes:
                    await self.sqlite_tracker.update_book_enrichment(barcode, {})
                    self.process_summary_stage.enrichment_failed += 1
                return 0

    async def reset_enrichment_data(self) -> int:
        """Reset enrichment data for all books in the database."""

        async with connect_async(self.db_path) as conn:
            # Reset enrichment fields to NULL using generated SQL
            cursor = await conn.execute(
                BookRecord.build_reset_enrichment_sql(),
                (datetime.now(UTC).isoformat(),),
            )

            reset_count = cursor.rowcount
            await conn.commit()
            return reset_count

    async def run_enrichment(self, limit: int | None = None, reset: bool = False) -> None:
        """Run the complete enrichment pipeline."""
        print("Starting GRIN metadata enrichment pipeline")
        logger.info("Starting GRIN metadata enrichment pipeline")
        logger.info(f"Database: {self.db_path}")
        logger.info(f"Directory: {self.directory}")
        logger.info(f"Concurrent requests: {self.max_concurrent_requests}")

        logger.info(f"Database batch size: {self.batch_size:,} books")
        logger.info("GRIN URL batches: Dynamic sizing optimized for concurrency")

        if limit:
            logger.info(f"Limit: {limit:,} {pluralize(limit, 'book')}")
        if reset:
            logger.info("Reset mode: Will clear existing enrichment data")

        # Reset enrichment data if requested
        if reset:
            print("Resetting enrichment data...")
            reset_count = await self.reset_enrichment_data()
            print(f"Reset enrichment data for {reset_count:,} books")
            print()

        # Initialize persistent database connection for performance optimization
        await self.initialize_resources()

        # Validate credentials
        logger.debug("Validating GRIN credentials...")
        try:
            await self.grin_client.auth.validate_credentials(self.directory)
        except Exception as e:
            print(f"Credential validation failed: {e}")
            return

        # Get initial counts
        total_books = await self.sqlite_tracker.get_book_count()
        enriched_books = await self.sqlite_tracker.get_enriched_book_count()
        remaining_books = total_books - enriched_books

        print(f"Database: {total_books:,} total, {enriched_books:,} enriched, {remaining_books:,} remaining")
        logger.info("Database status:")
        logger.info(f"  Total books: {total_books:,}")
        logger.info(f"  Already enriched: {enriched_books:,}")
        logger.info(f"  Remaining: {remaining_books:,}")

        if remaining_books == 0:
            print("✅ All books are already enriched!")

            # Suggest next steps since enrichment is complete
            run_name = Path(self.db_path).parent.name
            print("\nNext steps:")
            print(f"  Download converted books: python grin.py sync pipeline --run-name {run_name} --queue converted")

            # Clean up resources before returning
            await self.cleanup()
            return

        # Start enrichment
        start_time = time.time()
        processed_count = 0
        total_enriched = 0

        # Initialize sliding window rate calculator
        rate_calculator = SlidingWindowRateCalculator(window_size=20)

        logger.info(f"Starting enrichment of {remaining_books:,} books...")

        try:
            while limit is None or processed_count < limit:
                # Check for shutdown request
                if self._shutdown_requested:
                    print("Shutdown requested, stopping enrichment...")
                    break

                # Get batch from database
                batch_size = min(self.batch_size, limit - processed_count) if limit else self.batch_size
                barcodes = await self.sqlite_tracker.get_books_for_enrichment(batch_size)

                if not barcodes:
                    print("✅ No more books to enrich")
                    break

                batch_start = time.time()
                logger.info(f"Processing batch of {len(barcodes)} books...")

                # Create URL-sized batches dynamically for this set
                url_batches = self._create_url_batches(barcodes)

                # Process all URL batches concurrently (semaphore controls actual concurrency)
                tasks = [self._fetch_and_update(batch) for batch in url_batches]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Update progress
                enriched = sum(r for r in results if isinstance(r, int))
                processed_count += len(barcodes)
                total_enriched += enriched

                batch_elapsed = time.time() - batch_start

                # Track batch completion for sliding window rate calculation
                current_time = time.time()
                rate_calculator.add_batch(current_time, processed_count)

                # Calculate rate using sliding window
                rate = rate_calculator.get_rate(start_time, processed_count)

                # Calculate ETA if we have enough data (after 5+ batches for stable estimate)
                eta_text = ""
                if len(rate_calculator.batch_times) >= 5 and rate > 0:
                    books_remaining = max(0, remaining_books - processed_count)
                    eta_seconds = books_remaining / rate if books_remaining > 0 else 0
                    eta_text = f" (ETA: {format_duration(eta_seconds)})" if books_remaining > 0 else " (Complete)"

                logger.info(f"  Batch completed: {enriched}/{len(barcodes)} enriched in {batch_elapsed:.1f}s")
                # Show progress against remaining books for intuitive progress tracking
                print(f"Progress: {processed_count:,}/{remaining_books:,} processed ({rate:.1f} books/sec){eta_text}")
                progress_msg = f"  Overall progress: {processed_count:,}/{remaining_books:,} processed"
                rate_msg = f" ({rate:.1f} books/sec){eta_text}"
                logger.info(progress_msg + rate_msg)

                # Small delay between batches
                await asyncio.sleep(0.1)

        except KeyboardInterrupt:
            print("\nEnrichment interrupted by user")
            logger.info("Enrichment interrupted by user")
            logger.info("Cleaning up resources...")

        except Exception as e:
            print(f"\nEnrichment failed: {e}")
            logger.error(f"Enrichment failed: {e}")
            import traceback

            traceback.print_exc()
            # Also log the full traceback
            logger.error("Full traceback:", exc_info=True)

        finally:
            # Final statistics (get database count before cleanup)
            total_elapsed = time.time() - start_time
            final_enriched = await self.sqlite_tracker.get_enriched_book_count()

            # Calculate and set enrichment rate
            if total_elapsed > 0:
                rate_per_second = processed_count / total_elapsed
                self.process_summary_stage.enrichment_rate_per_hour = rate_per_second * 3600

            # Clean up resources
            await self.cleanup()

            completion_msg = f"\nCompleted: {processed_count:,} processed, {total_enriched:,} enriched"
            rate = processed_count / max(1, total_elapsed)
            timing_msg = f" in {format_duration(total_elapsed)} ({rate:.1f} books/sec)"
            print(completion_msg + timing_msg)
            logger.info("Enrichment pipeline completed:")
            logger.info(f"  Total runtime: {format_duration(total_elapsed)}")
            logger.info(f"  Books processed: {processed_count:,}")
            logger.info(f"  Books enriched: {total_enriched:,}")
            logger.info(f"  Total enriched in database: {final_enriched:,}")
            logger.info(f"  Average rate: {processed_count / max(1, total_elapsed):.1f} books/second")

            # Suggest next steps if enrichment was successful
            if total_enriched > 0:
                run_name = Path(self.db_path).parent.name
                print("\nNext steps:")
                print(
                    f"  Download converted books: python grin.py sync pipeline --run-name {run_name} --queue converted"
                )


async def main() -> None:
    """Main CLI entry point."""
    import signal

    # Set up signal handlers for graceful shutdown
    def signal_handler(signum: int, _frame: Any) -> None:
        print(f"\nReceived signal {signum}, shutting down gracefully...")
        raise KeyboardInterrupt()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    parser = argparse.ArgumentParser(
        description="GRIN metadata enrichment pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python grin.py enrich --run-name harvard_2024
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Enrichment command
    enrich_parser = subparsers.add_parser("enrich", help="Enrich books with GRIN metadata")
    enrich_parser.add_argument("--run-name", required=True, help="Run name (e.g., harvard_2024)")
    enrich_parser.add_argument(
        "--rate-limit", type=float, default=0.2, help="Delay between requests (default: 0.2s for 5 QPS)"
    )
    enrich_parser.add_argument(
        "--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Database batch size for processing books"
    )
    enrich_parser.add_argument(
        "--max-concurrent", type=int, default=5, help="Maximum concurrent GRIN requests (default: 5)"
    )
    enrich_parser.add_argument("--limit", type=int, help="Limit number of books to process")
    enrich_parser.add_argument(
        "--reset", action="store_true", help="Reset enrichment data for all books before enriching"
    )
    enrich_parser.add_argument(
        "--secrets-dir", help="Directory containing GRIN secrets files (auto-detected from run config if not specified)"
    )
    enrich_parser.add_argument(
        "--library-directory",
        dest="grin_library_directory",
        help="GRIN library directory name (auto-detected from run config if not specified)",
    )
    enrich_parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    run_config = load_run_config(OUTPUT_DIR / args.run_name / "run_config.json")
    apply_run_config_to_args(args, run_config)

    # Validate database file exists and is accessible
    validate_database_file(run_config.sqlite_db_path, check_tables=True)

    # Set up logging - use unified log file from run config
    if args.command == "enrich":
        setup_logging(args.log_level, run_config.log_file)

        logger = logging.getLogger(__name__)
        logger.info(f"ENRICHMENT PIPELINE STARTED - {args.command}")
        logger.info(f"Command: {' '.join(sys.argv)}")

    # Create process summary for enrich command
    run_summary = None
    enrich_stage = None
    book_manager = None
    pipeline = None
    if args.command == "enrich":
        # Create book storage for process summary uploads
        book_manager = await create_book_manager_for_uploads(args.run_name)

        run_summary = await create_process_summary(args.run_name, "enrich", book_manager)
        enrich_stage = get_current_stage(run_summary, "enrich")

    try:
        try:
            match args.command:
                case "enrich":
                    assert enrich_stage is not None  # Should be set for enrich command
                    pipeline = GRINEnrichmentPipeline.from_run_config(
                        config=run_config,
                        process_summary_stage=enrich_stage,
                        rate_limit_delay=args.rate_limit,
                        batch_size=args.batch_size,
                        max_concurrent_requests=args.max_concurrent,
                    )

                    enrich_stage.add_progress_update("Starting enrichment pipeline")
                    await pipeline.run_enrichment(limit=args.limit, reset=args.reset)
                    enrich_stage.add_progress_update("Enrichment pipeline completed successfully")

        except KeyboardInterrupt:
            if enrich_stage:
                enrich_stage.add_progress_update("Operation cancelled by user")
                enrich_stage.add_error("KeyboardInterrupt", "User cancelled operation")
            print("\nOperation cancelled by user")
        except Exception as e:
            if enrich_stage:
                error_type = type(e).__name__
                enrich_stage.add_error(error_type, str(e))
                enrich_stage.add_progress_update(f"Operation failed: {error_type}")
            print(f"Operation failed: {e}")
            sys.exit(1)

    finally:
        # Clean up book manager storage resources if created
        if book_manager:
            try:
                await book_manager.storage.close()
            except Exception as e:
                logger.warning(f"Error during book manager storage cleanup: {e}")

        # Always end the stage and save summary for enrich command
        if run_summary:
            run_summary.end_stage("enrich")
            await save_process_summary(run_summary, book_manager)

            # Upload books database to storage
            if book_manager:
                await upload_database_to_storage(run_config.sqlite_db_path, book_manager)

            # Display completion summary
            display_step_summary(run_summary, "enrich")


async def enrich_main():
    """Entry point for 'grin enrich' command."""
    # Insert 'enrich' as the subcommand
    sys.argv.insert(1, "enrich")
    await main()


if __name__ == "__main__":
    asyncio.run(main())
