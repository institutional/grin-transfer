#!/usr/bin/env python3
"""
GRIN Sync Management

Unified script to sync books from GRIN to storage and check sync status.
"""

import argparse
import asyncio
import json
import logging
import signal
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiosqlite
from collect_books.models import SQLiteProgressTracker
from common import ProgressReporter, SlidingWindowRateCalculator, create_storage_from_config, format_duration, pluralize
from download import download_book, reset_bucket_cache
from client import GRINClient
from run_config import find_run_config, apply_run_config_to_args

logger = logging.getLogger(__name__)


class SyncPipeline:
    """Pipeline for syncing converted books from GRIN to storage with database tracking."""

    def __init__(
        self,
        db_path: str,
        storage_type: str,
        storage_config: dict,
        concurrent_downloads: int = 3,
        batch_size: int = 100,
        directory: str = "Harvard",
        secrets_dir: str | None = None,
        gpg_key_file: str | None = None,
        force: bool = False,
    ):
        self.db_path = db_path
        self.storage_type = storage_type
        self.storage_config = storage_config
        self.concurrent_downloads = concurrent_downloads
        self.batch_size = batch_size
        self.directory = directory
        self.secrets_dir = secrets_dir
        self.gpg_key_file = gpg_key_file
        self.force = force

        # Initialize components
        self.db_tracker = SQLiteProgressTracker(db_path)
        self.progress_reporter = ProgressReporter("sync", None)
        self.grin_client = GRINClient(secrets_dir=secrets_dir)

        # Concurrency control
        self._download_semaphore = asyncio.Semaphore(concurrent_downloads)
        self._shutdown_requested = False

        # Statistics
        self.stats = {
            "processed": 0,
            "completed": 0,
            "failed": 0,
            "skipped": 0,
            "total_bytes": 0,
        }

    async def cleanup(self) -> None:
        """Clean up resources and close connections safely."""
        if self._shutdown_requested:
            return

        self._shutdown_requested = True
        logger.info("Shutting down sync pipeline...")

        try:
            if hasattr(self.db_tracker, "_db") and self.db_tracker._db:
                await self.db_tracker._db.close()
                logger.debug("Closed database connection")
        except Exception as e:
            logger.warning(f"Error closing database connection: {e}")

        try:
            if hasattr(self.grin_client, "session") and self.grin_client.session:
                await self.grin_client.session.close()
                logger.debug("Closed GRIN client session")
        except Exception as e:
            logger.warning(f"Error closing GRIN client session: {e}")

        logger.info("Cleanup completed")

    async def _mark_book_as_converted(self, barcode: str) -> None:
        """Mark a book as converted in our database after successful download."""
        try:
            from datetime import UTC
            book = await self.db_tracker.get_book(barcode)
            if book:
                book.processing_request_status = "converted"
                await self.db_tracker.save_book(book)
                logger.debug(f"Marked {barcode} as converted in database")
        except Exception as e:
            logger.warning(f"Failed to mark {barcode} as converted: {e}")

    async def get_converted_books(self) -> set[str]:
        """Get set of books that are converted and ready for download."""
        try:
            response_text = await self.grin_client.fetch_resource(self.directory, "_converted?format=text")
            lines = response_text.strip().split("\n")
            converted_barcodes = set()
            for line in lines:
                if line.strip() and ".tar.gz.gpg" in line:
                    barcode = line.strip().replace(".tar.gz.gpg", "")
                    converted_barcodes.add(barcode)
            return converted_barcodes
        except Exception as e:
            logger.warning(f"Failed to get converted books: {e}")
            return set()

    async def _download_with_semaphore(self, barcode: str) -> dict:
        """Download a single book with concurrency control."""
        async with self._download_semaphore:
            try:
                result = await download_book(
                    barcode=barcode,
                    storage_type=self.storage_type,
                    storage_config=self.storage_config,
                    directory=self.directory,
                    secrets_dir=self.secrets_dir,
                    force=self.force,
                    gpg_key_file=self.gpg_key_file,
                    db_tracker=self.db_tracker,
                    verbose=True,  # Enable verbose output for debug logging
                    show_progress=False,  # Disable percentage progress in pipeline
                )
                
                self.stats["completed"] += 1
                self.stats["total_bytes"] += result.get("file_size", 0)
                
                # Mark book as converted since we successfully downloaded it
                await self._mark_book_as_converted(barcode)
                
                return {
                    "barcode": barcode,
                    "status": "completed",
                    "size": result.get("file_size", 0),
                    "duration": result.get("total_time", 0),
                    "decrypted": result.get("is_decrypted", False),
                }
                
            except Exception as e:
                self.stats["failed"] += 1
                logger.error(f"Failed to download {barcode}: {e}")
                
                return {
                    "barcode": barcode,
                    "status": "failed",
                    "error": str(e),
                }

    async def process_batch(self, barcodes: list[str]) -> list[dict]:
        """Process a batch of downloads concurrently."""
        if not barcodes:
            return []

        # Create download tasks
        tasks = [self._download_with_semaphore(barcode) for barcode in barcodes]
        
        # Execute all downloads concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        processed_results: list[dict[str, Any]] = []
        for i, result in enumerate(results):
            barcode = barcodes[i]
            self.stats["processed"] += 1
            
            if isinstance(result, Exception):
                self.stats["failed"] += 1
                logger.error(f"Download task failed for {barcode}: {result}")
                processed_results.append({
                    "barcode": barcode,
                    "status": "failed",
                    "error": str(result),
                })
            elif isinstance(result, dict):
                processed_results.append(result)
            else:
                # Unexpected result type, treat as error
                self.stats["failed"] += 1
                logger.error(f"Unexpected result type for {barcode}: {type(result)}")
                processed_results.append({
                    "barcode": barcode,
                    "status": "failed",
                    "error": f"Unexpected result type: {type(result)}",
                })
                
        return processed_results

    async def get_sync_status(self) -> dict:
        """Get current sync status and statistics."""
        stats = await self.db_tracker.get_sync_stats(self.storage_type)
        return {
            **stats,
            "session_stats": self.stats,
        }

    async def run_sync(self, limit: int | None = None, status_filter: str | None = None) -> None:
        """Run the complete sync pipeline."""
        print(f"Starting GRIN-to-Storage sync pipeline")
        print(f"Database: {self.db_path}")
        print(f"Storage: {self.storage_type}")
        print(f"Concurrent downloads: {self.concurrent_downloads}")
        print(f"Batch size: {self.batch_size}")
        if limit:
            print(f"Limit: {limit:,} {pluralize(limit, 'book')}")
        if status_filter:
            print(f"Status filter: {status_filter}")
        print()

        logger.info("Starting sync pipeline")
        logger.info(f"Database: {self.db_path}")
        logger.info(f"Storage type: {self.storage_type}")
        logger.info(f"Concurrent downloads: {self.concurrent_downloads}")
        
        # Reset bucket cache at start of sync
        reset_bucket_cache()
        
        start_time = time.time()

        try:
            # Get list of converted books from GRIN
            print("Fetching list of converted books from GRIN...")
            converted_barcodes = await self.get_converted_books()
            if len(converted_barcodes) == 0:
                print("Warning: GRIN reports no converted books available (this could indicate an API issue)")
            else:
                print(f"GRIN reports {len(converted_barcodes):,} converted books available for download")
            
            # Get initial status
            initial_status = await self.get_sync_status()
            total_converted = initial_status["total_converted"]
            already_synced = initial_status["synced"]
            failed_count = initial_status["failed"]
            pending_count = initial_status["pending"]

            print(f"Database sync status: {total_converted:,} total, {already_synced:,} synced, "
                  f"{failed_count:,} failed, {pending_count:,} pending")

            # Check how many requested books need syncing (only those actually converted by GRIN)
            available_to_sync = await self.db_tracker.get_books_for_sync(
                storage_type=self.storage_type,
                limit=999999,  # Get all available
                status_filter=status_filter,
                converted_barcodes=converted_barcodes  # Only sync books that GRIN reports as converted
            )
            
            print(f"Found {len(available_to_sync):,} converted books that need syncing")
            
            if not available_to_sync:
                if len(converted_barcodes) == 0:
                    print("No converted books available from GRIN")
                else:
                    print("No converted books found that need syncing (all may already be synced)")
                
                # Report on pending books
                pending_books = await self.db_tracker.get_books_for_sync(
                    storage_type=self.storage_type,
                    limit=999999,
                    status_filter=status_filter,
                    converted_barcodes=None  # Get all requested books regardless of conversion
                )
                
                if pending_books:
                    print(f"📋 Status summary:")
                    print(f"  - {len(pending_books):,} books requested for processing but not yet converted")
                    print(f"  - {len(converted_barcodes):,} books available from GRIN (from other requests)")
                    print(f"  - 0 books ready to sync (no overlap between requested and converted)")
                    print(f"\n💡 Tip: Use 'python processing.py monitor --run-name {Path(self.db_path).parent.name}' to check processing progress")
                
                return

            # Set up progress tracking
            books_to_process = min(limit or len(available_to_sync), len(available_to_sync))
            self.progress_reporter = ProgressReporter("sync", books_to_process)
            self.progress_reporter.start()

            processed_count = 0
            
            # Initialize sliding window rate calculator
            rate_calculator = SlidingWindowRateCalculator(window_size=5)

            while True:
                # Check for shutdown request
                if self._shutdown_requested:
                    print("\nShutdown requested, stopping sync...")
                    break

                # Get batch of books to sync
                remaining_limit = None
                if limit:
                    remaining_limit = limit - processed_count
                    if remaining_limit <= 0:
                        break

                batch_limit = min(self.batch_size, remaining_limit or self.batch_size)
                barcodes = await self.db_tracker.get_books_for_sync(
                    storage_type=self.storage_type,
                    limit=batch_limit,
                    status_filter=status_filter,
                    converted_barcodes=converted_barcodes  # Only sync books that GRIN reports as converted
                )

                if not barcodes:
                    print("✅ No more books to sync")
                    break

                logger.info(f"Processing batch of {len(barcodes)} books...")

                # Process the batch
                batch_start = time.time()
                batch_results = await self.process_batch(barcodes)
                batch_elapsed = time.time() - batch_start

                # Update progress
                processed_count += len(barcodes)
                self.progress_reporter.update(
                    items=len(barcodes),
                    bytes_count=sum(r.get("size", 0) for r in batch_results),
                    force=True
                )
                
                # Track batch completion for sliding window rate calculation
                current_time = time.time()
                rate_calculator.add_batch(current_time, processed_count)

                # Log batch completion
                completed_in_batch = sum(1 for r in batch_results if r["status"] == "completed")
                failed_in_batch = sum(1 for r in batch_results if r["status"] == "failed")
                
                logger.info(f"Batch completed: {completed_in_batch}/{len(barcodes)} successful "
                           f"in {batch_elapsed:.1f}s")

                # Calculate rate using sliding window
                rate = rate_calculator.get_rate(start_time, processed_count)
                
                if books_to_process > 0:
                    percentage = (processed_count / books_to_process) * 100
                    remaining = books_to_process - processed_count
                    eta_seconds = remaining / rate if rate > 0 else 0
                    eta_text = f" (ETA: {format_duration(eta_seconds)})" if eta_seconds > 0 else ""
                    
                    print(f"Progress: {processed_count:,}/{books_to_process:,} "
                          f"({percentage:.1f}%) - {rate:.1f} books/sec{eta_text}")

                if limit and processed_count >= limit:
                    print(f"Reached limit of {limit:,} books")
                    break

                # Small delay between batches
                await asyncio.sleep(0.1)

        except KeyboardInterrupt:
            print("\nSync interrupted by user")
            logger.info("Sync interrupted by user")

        except Exception as e:
            print(f"\nSync failed: {e}")
            logger.error(f"Sync failed: {e}", exc_info=True)

        finally:
            # Clean up resources
            await self.cleanup()

            # Final statistics
            self.progress_reporter.finish()
            
            total_elapsed = time.time() - start_time
            final_status = await self.get_sync_status()

            print(f"\nSync completed:")
            print(f"  Total runtime: {format_duration(total_elapsed)}")
            print(f"  Books processed: {self.stats['processed']:,}")
            print(f"  Successful downloads: {self.stats['completed']:,}")
            print(f"  Failed downloads: {self.stats['failed']:,}")
            print(f"  Total data: {self.stats['total_bytes'] / 1024 / 1024:.1f} MB")
            if total_elapsed > 0:
                print(f"  Average rate: {self.stats['processed'] / total_elapsed:.1f} books/second")
            
            print(f"\nFinal sync status:")
            print(f"  Total converted books: {final_status['total_converted']:,}")
            print(f"  Successfully synced: {final_status['synced']:,}")
            print(f"  Failed syncs: {final_status['failed']:,}")
            print(f"  Pending syncs: {final_status['pending']:,}")
            print(f"  Decrypted books: {final_status['decrypted']:,}")

            logger.info("Sync pipeline completed")


async def show_sync_status(db_path: str, storage_type: str | None = None) -> None:
    """Show sync status for books in the database."""
    
    # Validate database file
    db_file = Path(db_path)
    if not db_file.exists():
        print(f"❌ Error: Database file does not exist: {db_path}")
        return

    print(f"Sync Status Report")
    print(f"Database: {db_path}")
    if storage_type:
        print(f"Storage Type: {storage_type}")
    print("=" * 50)

    tracker = SQLiteProgressTracker(db_path)
    
    try:
        # Get overall book counts
        total_books = await tracker.get_book_count()
        enriched_books = await tracker.get_enriched_book_count()
        converted_books = await tracker.get_converted_books_count()
        
        print(f"Overall Book Counts:")
        print(f"  Total books in database: {total_books:,}")
        print(f"  Books with enrichment data: {enriched_books:,}")
        print(f"  Books in converted state: {converted_books:,}")
        print()
        
        # Get sync statistics
        sync_stats = await tracker.get_sync_stats(storage_type)
        
        print(f"Sync Status:")
        print(f"  Total converted books: {sync_stats['total_converted']:,}")
        print(f"  Successfully synced: {sync_stats['synced']:,}")
        print(f"  Failed syncs: {sync_stats['failed']:,}")
        print(f"  Pending syncs: {sync_stats['pending']:,}")
        print(f"  Currently syncing: {sync_stats['syncing']:,}")
        print(f"  Books with decrypted archives: {sync_stats['decrypted']:,}")
        
        if sync_stats['total_converted'] > 0:
            sync_percentage = (sync_stats['synced'] / sync_stats['total_converted']) * 100
            print(f"  Sync completion: {sync_percentage:.1f}%")
        
        print()
        
        # Show breakdown by storage type if not filtered
        if not storage_type:
            print("Storage Type Breakdown:")
            
            # Get books by storage type and extract bucket from storage_path
            async with aiosqlite.connect(db_path) as db:
                cursor = await db.execute("""
                    SELECT storage_type, storage_path, COUNT(*) as count
                    FROM books 
                    WHERE storage_type IS NOT NULL AND storage_path IS NOT NULL
                    GROUP BY storage_type, storage_path
                    ORDER BY storage_type, count DESC
                """)
                storage_breakdown = await cursor.fetchall()
                
                if storage_breakdown:
                    # Group by storage type and extract bucket from path
                    storage_buckets = {}
                    for storage, path, count in storage_breakdown:
                        # Extract bucket from path (first part after removing prefix)
                        bucket = "unknown"
                        if path:
                            # For paths like "bucket/BARCODE/..." extract the bucket
                            parts = path.split("/")
                            if parts:
                                bucket = parts[0]
                        
                        key = f"{storage}/{bucket}"
                        storage_buckets[key] = storage_buckets.get(key, 0) + count
                    
                    for storage_bucket, count in sorted(storage_buckets.items()):
                        print(f"  {storage_bucket}: {count:,} books")
                else:
                    print("  No books have been synced to any storage yet")
                    
                print()
        
        # Show recent sync activity
        print("Recent Sync Activity (last 10):")
        async with aiosqlite.connect(db_path) as db:
            query = """
                SELECT barcode, sync_status, sync_timestamp, sync_error, storage_type
                FROM books 
                WHERE sync_timestamp IS NOT NULL
            """
            params = []
            
            if storage_type:
                query += " AND storage_type = ?"
                params.append(storage_type)
                
            query += " ORDER BY sync_timestamp DESC LIMIT 10"
            
            cursor = await db.execute(query, params)
            recent_syncs = await cursor.fetchall()
            
            if recent_syncs:
                for barcode, status, timestamp, error, st_type in recent_syncs:
                    status_icon = "✅" if status == "completed" else "❌" if status == "failed" else "🔄"
                    print(f"  {status_icon} {barcode} ({st_type}) - {status} at {timestamp}")
                    if error:
                        print(f"      Error: {error}")
            else:
                print("  No recent sync activity found")
                
    except Exception as e:
        print(f"❌ Error reading database: {e}")
        return
        
    finally:
        # Clean up database connections
        try:
            if hasattr(tracker, "_db") and tracker._db:
                await tracker._db.close()
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
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # Add console handler for ERROR and above (minimal console output)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.ERROR)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Suppress debug logs from dependencies
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("aiosqlite").setLevel(logging.WARNING)


def validate_database_file(db_path: str) -> None:
    """Validate that the database file exists and contains the required tables."""
    import sqlite3

    db_file = Path(db_path)

    if not db_file.exists():
        print(f"❌ Error: Database file does not exist: {db_path}")
        print("\nRun a book collection first:")
        print("python collect_books.py --run-name <your_run_name>")
        sys.exit(1)

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()

            table_names = [table[0] for table in tables]
            required_tables = ["books", "processed", "failed"]
            missing_tables = [table for table in required_tables if table not in table_names]

            if missing_tables:
                print(f"❌ Error: Database is missing required tables: {missing_tables}")
                sys.exit(1)

    except sqlite3.Error as e:
        print(f"❌ Error: Cannot read SQLite database: {e}")
        sys.exit(1)

    print(f"✅ Using database: {db_path}")


async def cmd_pipeline(args) -> None:
    """Handle the 'pipeline' command."""
    # Determine database path from run name
    args.db_path = f"output/{args.run_name}/books.db"
    print(f"Using run: {args.run_name}")
    print(f"Database: {args.db_path}")
    
    # Apply run configuration defaults
    apply_run_config_to_args(args, args.db_path)
    
    # Set up signal handlers for graceful shutdown
    def signal_handler(signum: int, frame: Any) -> None:
        print(f"\nReceived signal {signum}, shutting down gracefully...")
        raise KeyboardInterrupt()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Validate database
    validate_database_file(args.db_path)

    # Validate that we have required storage arguments
    if not args.storage:
        print("❌ Error: --storage argument is required (or must be in run config)")
        sys.exit(1)
    
    missing_buckets = []
    if not args.bucket_raw:
        missing_buckets.append("--bucket-raw")
    if not args.bucket_meta:
        missing_buckets.append("--bucket-meta")  
    if not args.bucket_full:
        missing_buckets.append("--bucket-full")
    
    if missing_buckets:
        print(f"❌ Error: The following bucket arguments are required (or must be in run config): {', '.join(missing_buckets)}")
        sys.exit(1)

    # Build storage configuration
    storage_config = {
        "bucket_raw": args.bucket_raw,
        "bucket_meta": args.bucket_meta,
        "bucket_full": args.bucket_full,
    }
    if args.prefix:
        storage_config["prefix"] = args.prefix
    if args.endpoint_url:
        storage_config["endpoint_url"] = args.endpoint_url
    if args.access_key:
        storage_config["access_key"] = args.access_key
    if args.secret_key:
        storage_config["secret_key"] = args.secret_key
    if args.account_id:
        storage_config["account_id"] = args.account_id
    if args.credentials_file:
        storage_config["credentials_file"] = args.credentials_file

    # Update run configuration with storage parameters if they were provided
    config_path = Path(args.db_path).parent / "run_config.json"
    if config_path.exists():
        try:
            # Read existing config
            with open(config_path) as f:
                run_config = json.load(f)
            
            # Update storage configuration
            storage_config_dict = {
                "type": args.storage,
                "config": {k: v for k, v in storage_config.items() if v is not None},
                "prefix": storage_config.get("prefix", "grin-books")
            }
            
            run_config["storage_config"] = storage_config_dict
            
            # Write back to config file
            with open(config_path, "w") as f:
                json.dump(run_config, f, indent=2)
            
            print(f"Updated storage configuration in {config_path}")
            
        except (json.JSONDecodeError, OSError) as e:
            print(f"Warning: Could not update run config: {e}")
    else:
        print(f"Note: No run config found at {config_path} to update")

    # Auto-configure MinIO from docker-compose file if needed
    if args.storage == "minio" and not (args.endpoint_url and args.access_key and args.secret_key):
        try:
            import yaml
            
            compose_file = Path("docker-compose.minio.yml")
            if compose_file.exists():
                with open(compose_file) as f:
                    compose_config = yaml.safe_load(f)

                minio_service = compose_config.get("services", {}).get("minio", {})
                env = minio_service.get("environment", {})
                ports = minio_service.get("ports", [])

                if not args.endpoint_url:
                    api_port = "9000"
                    for port_mapping in ports:
                        if isinstance(port_mapping, str) and ":9000" in port_mapping:
                            api_port = port_mapping.split(":")[0]
                            break
                    storage_config["endpoint_url"] = f"http://localhost:{api_port}"

                if not args.access_key:
                    storage_config["access_key"] = env.get("MINIO_ROOT_USER", "minioadmin")

                if not args.secret_key:
                    storage_config["secret_key"] = env.get("MINIO_ROOT_PASSWORD", "minioadmin123")

                print("Auto-configured MinIO from docker-compose.minio.yml")

        except ImportError:
            print("Warning: PyYAML not available, cannot auto-configure MinIO")
        except Exception as e:
            print(f"Warning: Failed to read docker-compose.minio.yml: {e}")

    # Set up logging
    db_name = Path(args.db_path).stem
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/sync_pipeline_{args.storage}_{db_name}_{timestamp}.log"
    setup_logging(args.log_level, log_file)
    print(f"Logging to file: {log_file}")

    # Create and run pipeline
    try:
        pipeline = SyncPipeline(
            db_path=args.db_path,
            storage_type=args.storage,
            storage_config=storage_config,
            concurrent_downloads=args.concurrent,
            batch_size=args.batch_size,
            directory=args.directory,
            secrets_dir=args.secrets_dir,
            gpg_key_file=args.gpg_key_file,
            force=args.force,
        )
        
        await pipeline.run_sync(limit=args.limit, status_filter=args.status)

    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        print(f"Pipeline failed: {e}")
        sys.exit(1)


async def cmd_status(args) -> None:
    """Handle the 'status' command."""
    # Determine database path from run name
    args.db_path = f"output/{args.run_name}/books.db"
    print(f"Using run: {args.run_name}")
    print(f"Database: {args.db_path}")
    
    try:
        await show_sync_status(args.db_path, args.storage_type)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


async def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="GRIN sync management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python sync.py pipeline --run-name harvard_2024
  python sync.py status --run-name harvard_2024
        """,
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # Pipeline command
    pipeline_parser = subparsers.add_parser(
        "pipeline",
        help="Sync converted books to storage",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic sync (auto-detects storage config from run)
  python sync.py pipeline --run-name harvard_2024

  # Sync with explicit storage configuration
  python sync.py pipeline --run-name harvard_2024 --storage r2 --bucket-raw grin-raw --bucket-meta grin-meta --bucket-full grin-full

  # Sync with custom concurrency
  python sync.py pipeline --run-name harvard_2024 --concurrent 5

  # Retry failed syncs only
  python sync.py pipeline --run-name harvard_2024 --status failed

  # Sync with limit and force overwrite
  python sync.py pipeline --run-name harvard_2024 --limit 100 --force
        """,
    )

    pipeline_parser.add_argument("--run-name", required=True, help="Run name (e.g., harvard_2024)")

    # Storage configuration
    pipeline_parser.add_argument("--storage", choices=["minio", "r2", "s3"], help="Storage backend (auto-detected from run config if not specified)")
    pipeline_parser.add_argument("--bucket-raw", help="Raw data bucket (auto-detected from run config if not specified)")
    pipeline_parser.add_argument("--bucket-meta", help="Metadata bucket (auto-detected from run config if not specified)")
    pipeline_parser.add_argument("--bucket-full", help="Full-text bucket (auto-detected from run config if not specified)")
    pipeline_parser.add_argument("--prefix", help="Storage prefix/path")

    # Storage credentials
    pipeline_parser.add_argument("--endpoint-url", help="Custom endpoint URL (MinIO)")
    pipeline_parser.add_argument("--access-key", help="Access key")
    pipeline_parser.add_argument("--secret-key", help="Secret key")
    pipeline_parser.add_argument("--account-id", help="Account ID (R2)")
    pipeline_parser.add_argument("--credentials-file", help="Custom credentials file path")

    # Pipeline options
    pipeline_parser.add_argument("--concurrent", type=int, default=3, help="Concurrent downloads (default: 3)")
    pipeline_parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing (default: 100)")
    pipeline_parser.add_argument("--limit", type=int, help="Limit number of books to sync")
    pipeline_parser.add_argument("--status", choices=["pending", "failed"], help="Only sync books with this status")
    pipeline_parser.add_argument("--force", action="store_true", help="Force download and overwrite existing files")

    # GRIN options
    pipeline_parser.add_argument("--directory", help="GRIN directory (auto-detected from run config if not specified)")
    pipeline_parser.add_argument("--secrets-dir", help="Directory containing GRIN secrets (auto-detected from run config if not specified)")
    pipeline_parser.add_argument("--gpg-key-file", help="Custom GPG key file path")

    # Logging
    pipeline_parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")

    # Status command
    status_parser = subparsers.add_parser(
        "status",
        help="Check sync status",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check overall sync status
  python sync.py status --run-name harvard_2024

  # Check sync status for specific storage type
  python sync.py status --run-name harvard_2024 --storage-type r2
        """,
    )

    status_parser.add_argument("--run-name", required=True, help="Run name (e.g., harvard_2024)")
    status_parser.add_argument("--storage-type", choices=["local", "minio", "r2", "s3"], 
                              help="Filter by storage type")

    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    if args.command == "pipeline":
        await cmd_pipeline(args)
    elif args.command == "status":
        await cmd_status(args)


if __name__ == "__main__":
    asyncio.run(main())