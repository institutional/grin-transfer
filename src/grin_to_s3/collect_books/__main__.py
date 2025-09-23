#!/usr/bin/env python3
"""
CLI interface for book collection pipeline.

This module provides the command-line interface for the book collection system.
Run with: python grin.py collect
"""

import argparse
import asyncio
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import cast, get_args

from grin_to_s3.constants import GRIN_RATE_LIMIT_QPS, OUTPUT_DIR, STORAGE_PROTOCOLS, STORAGE_TYPES
from grin_to_s3.storage.factories import create_local_storage_directories

from .collector import BookCollector

sys.path.append("..")
from grin_to_s3.database.database_backup import upload_database_to_storage
from grin_to_s3.logging_config import (
    setup_logging,
)
from grin_to_s3.process_summary import (
    create_book_manager_for_uploads,
    create_process_summary,
    display_step_summary,
    get_current_stage,
    save_process_summary,
)
from grin_to_s3.run_config import (
    DEFAULT_SYNC_DISK_SPACE_THRESHOLD,
    DEFAULT_SYNC_TASK_CHECK_CONCURRENCY,
    DEFAULT_SYNC_TASK_CLEANUP_CONCURRENCY,
    DEFAULT_SYNC_TASK_DECRYPT_CONCURRENCY,
    DEFAULT_SYNC_TASK_DOWNLOAD_CONCURRENCY,
    DEFAULT_SYNC_TASK_EXPORT_CSV_CONCURRENCY,
    DEFAULT_SYNC_TASK_EXTRACT_MARC_CONCURRENCY,
    DEFAULT_SYNC_TASK_EXTRACT_OCR_CONCURRENCY,
    DEFAULT_SYNC_TASK_UNPACK_CONCURRENCY,
    DEFAULT_SYNC_TASK_UPLOAD_CONCURRENCY,
    RunConfig,
    StorageConfig,
    StorageConfigDict,
    build_storage_config_dict,
    build_sync_config_from_args,
    save_run_config,
)
from grin_to_s3.storage import get_storage_protocol

# Check Python version requirement

# Force unbuffered output for immediate logging
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)  # type: ignore[union-attr]
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(line_buffering=True)  # type: ignore[union-attr]

# Set up module logger
logger = logging.getLogger(__name__)


def valid_library_directory(value: str) -> str:
    """Validate library directory is a simple string, not a URL or path."""
    # Check for empty string
    if not value or not value.strip():
        raise argparse.ArgumentTypeError("Library directory cannot be empty")

    # Check for URL
    if "://" in value or value.startswith(("http:", "https:")):
        raise argparse.ArgumentTypeError(
            f"Library directory should be the library name from your GRIN URL path.\n\n"
            f"For example, given 'https://books.google.com/libraries/Harvard/_all_books', "
            f"use 'Harvard' (case sensitive).\n\n"
            f"You provided: '{value}'"
        )

    # Check for path separators - user may have included path components
    if "/" in value or "\\" in value:
        raise argparse.ArgumentTypeError(
            f"Library directory should be just the directory name (e.g., 'Harvard'), not a path.\nYou provided: {value}"
        )

    # Check for valid characters
    if not re.match(r"^[a-zA-Z0-9_-]+$", value):
        raise argparse.ArgumentTypeError(
            f"Library directory can only contain letters, numbers, hyphens, and underscores.\n"
            f"Examples: Harvard, MIT, Yale\n"
            f"You provided: {value}"
        )

    return value


async def main():
    """CLI interface for book collection pipeline."""
    parser = argparse.ArgumentParser(
        description="Collect library book metadata in local db with progress tracking",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic collection with timestamp-based run name
  python grin.py collect --storage r2 --bucket-raw grin-raw --bucket-meta grin-meta --bucket-full grin-full

  # Named collection run
  python grin.py collect --run-name "harvard_fall_2024" --storage r2 --bucket-raw grin-raw --bucket-meta grin-meta --bucket-full grin-full

  # With rate limiting and storage checking
  python grin.py collect --rate-limit 0.5 --storage s3 --bucket-raw my-raw --bucket-meta my-meta --bucket-full my-full

  # Local storage (requires base_path, no buckets needed)
  python grin.py collect --storage local --run-name "local_test" --storage-config base_path=/path/to/storage

  # Resume interrupted collection (uses run-specific progress files and saved config)
  python grin.py collect --run-name "harvard_fall_2024" --storage r2 --bucket-raw grin-raw --bucket-meta grin-meta --bucket-full grin-full
        """,
    )

    # Run identification
    parser.add_argument(
        "--run-name", help="Name for this collection run (defaults to timestamp). Used for all output files."
    )

    # Export options
    parser.add_argument("--limit", type=int, help="Limit number of books to process (for testing)")
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Update metadata for existing books and add new ones (default: skip existing books)",
    )

    # Logging options
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )
    parser.add_argument(
        "--log-dir",
        type=str,
        default=os.environ.get("GRIN_LOG_DIR", "logs"),
        help="Directory for log files (default: logs)",
    )

    # Storage options
    parser.add_argument(
        "--storage",
        choices=list(get_args(STORAGE_TYPES)),
        required=True,
        help="Storage backend for run configuration",
    )
    parser.add_argument(
        "--bucket-raw",
        help="Raw data bucket (for sync archives, required for MinIO, GCS, or S3, optional for R2 if in config file)",
    )
    parser.add_argument(
        "--bucket-meta",
        help="Metadata bucket (for CSV/database outputs, required for MinIO, GCS, or S3, optional for R2 if in config file)",
    )
    parser.add_argument(
        "--bucket-full",
        help="Full-text bucket (for OCR outputs, required for MinIO, GCS, or S3, optional for R2 if in config file)",
    )
    parser.add_argument("--storage-config", action="append", help="Additional storage config key=value")

    parser.add_argument(
        "--secrets-dir",
        type=str,
        help="Directory containing GRIN secrets files (searches home directory if not specified)",
    )
    parser.add_argument(
        "--library-directory",
        type=valid_library_directory,
        required=True,
        help="Library directory name from your GRIN URL (e.g., Harvard, MIT, Yale). "
        "This is case-sensitive and should match the directory in "
        "https://books.google.com/libraries/YOUR_DIRECTORY/",
    )

    # Sync configuration options (stored in run config for later use)
    parser.add_argument(
        "--sync-task-check-concurrency",
        type=int,
        default=DEFAULT_SYNC_TASK_CHECK_CONCURRENCY,
        help=f"Check/head task concurrency for sync operations (default: {DEFAULT_SYNC_TASK_CHECK_CONCURRENCY})",
    )
    parser.add_argument(
        "--sync-task-download-concurrency",
        type=int,
        default=DEFAULT_SYNC_TASK_DOWNLOAD_CONCURRENCY,
        help=f"Download task concurrency for sync operations (default: {DEFAULT_SYNC_TASK_DOWNLOAD_CONCURRENCY})",
    )
    parser.add_argument(
        "--sync-task-upload-concurrency",
        type=int,
        default=DEFAULT_SYNC_TASK_UPLOAD_CONCURRENCY,
        help=f"Upload task concurrency for sync operations (default: {DEFAULT_SYNC_TASK_UPLOAD_CONCURRENCY})",
    )
    parser.add_argument(
        "--sync-task-decrypt-concurrency",
        type=int,
        default=DEFAULT_SYNC_TASK_DECRYPT_CONCURRENCY,
        help=f"Decrypt task concurrency for sync operations (default: {DEFAULT_SYNC_TASK_DECRYPT_CONCURRENCY})",
    )
    parser.add_argument(
        "--sync-task-unpack-concurrency",
        type=int,
        default=DEFAULT_SYNC_TASK_UNPACK_CONCURRENCY,
        help=f"Unpack task concurrency for sync operations (default: {DEFAULT_SYNC_TASK_UNPACK_CONCURRENCY})",
    )
    parser.add_argument(
        "--sync-task-extract-marc-concurrency",
        type=int,
        default=DEFAULT_SYNC_TASK_EXTRACT_MARC_CONCURRENCY,
        help=f"Extract MARC task concurrency for sync operations (default: {DEFAULT_SYNC_TASK_EXTRACT_MARC_CONCURRENCY})",
    )
    parser.add_argument(
        "--sync-task-extract-ocr-concurrency",
        type=int,
        default=DEFAULT_SYNC_TASK_EXTRACT_OCR_CONCURRENCY,
        help=f"Extract OCR task concurrency for sync operations (default: {DEFAULT_SYNC_TASK_EXTRACT_OCR_CONCURRENCY})",
    )
    parser.add_argument(
        "--sync-task-export-csv-concurrency",
        type=int,
        default=DEFAULT_SYNC_TASK_EXPORT_CSV_CONCURRENCY,
        help=f"Export CSV task concurrency for sync operations (default: {DEFAULT_SYNC_TASK_EXPORT_CSV_CONCURRENCY})",
    )
    parser.add_argument(
        "--sync-task-cleanup-concurrency",
        type=int,
        default=DEFAULT_SYNC_TASK_CLEANUP_CONCURRENCY,
        help=f"Cleanup task concurrency for sync operations (default: {DEFAULT_SYNC_TASK_CLEANUP_CONCURRENCY})",
    )
    parser.add_argument("--sync-staging-dir", help="Custom staging directory path for sync operations (default: auto)")
    parser.add_argument(
        "--sync-disk-space-threshold",
        type=float,
        default=DEFAULT_SYNC_DISK_SPACE_THRESHOLD,
        help=f"Disk usage threshold to pause downloads (0.0-1.0, default: {DEFAULT_SYNC_DISK_SPACE_THRESHOLD})",
    )

    args = parser.parse_args()

    # Generate run name and output file paths

    if args.run_name:
        # Use provided run name (make it filename-safe)
        run_name = "".join(c for c in args.run_name if c.isalnum() or c in ("-", "_")).strip()
        if not run_name:
            parser.error("Run name must contain valid filename characters")
    else:
        # Generate timestamp-based run name
        run_name = datetime.now().strftime("run_%Y%m%d_%H%M%S")

    # Generate timestamp for output files (not resume files)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    log_file = Path(f"{args.log_dir}/grin_pipeline_{run_name}_{timestamp}.log")
    sqlite_db_path = Path(f"{OUTPUT_DIR}/{run_name}/books.db")

    # Create directories
    log_file.parent.mkdir(parents=True, exist_ok=True)
    sqlite_db_path.parent.mkdir(parents=True, exist_ok=True)

    # Build storage configuration
    final_storage_dict = build_storage_config_dict(args)

    # Auto-configure MinIO with standard bucket names (only used in Docker)
    if args.storage == "minio":
        from ..common import auto_configure_minio

        auto_configure_minio(final_storage_dict)

    # Determine storage protocol for operational logic
    storage_protocol = get_storage_protocol(args.storage)
    storage_config: StorageConfig = {
        "type": cast(STORAGE_TYPES, args.storage),
        "protocol": cast(STORAGE_PROTOCOLS, storage_protocol),
        "config": cast(StorageConfigDict, final_storage_dict),
        "prefix": "",
    }

    if args.storage == "local":
        await create_local_storage_directories(final_storage_dict)
    else:
        print(f"Configured with {args.storage} cloud storage")
    # Build sync configuration from CLI arguments
    sync_config = build_sync_config_from_args(args)
    run_config = RunConfig(
        run_name=run_name,
        library_directory=args.library_directory,
        output_directory=Path(OUTPUT_DIR / run_name),
        sqlite_db_path=sqlite_db_path,
        log_file=log_file,
        sync_config=sync_config,
        storage_config=storage_config,
        secrets_dir=args.secrets_dir,
    )

    # Write config to run directory
    config_path = Path(f"{OUTPUT_DIR}/{run_name}/run_config.json")
    config_path.parent.mkdir(parents=True, exist_ok=True)

    save_run_config(run_config)

    print(f"Configuration written to {config_path}")
    print(f"Run directory: output/{run_name}/")
    print(f"Database: {sqlite_db_path}")

    # Initialize logging
    setup_logging(level=args.log_level, log_file=log_file, append=False)

    logger.info(f"COLLECTION PIPELINE STARTED - run={run_name} storage={args.storage} rate_limit={GRIN_RATE_LIMIT_QPS}")
    logger.info(f"Command: {' '.join(sys.argv)}")

    try:
        book_manager = await create_book_manager_for_uploads(run_name)

        # Create or load process summary
        run_summary = await create_process_summary(run_name, "collect", book_manager)
        collect_stage = get_current_stage(run_summary, "collect")
        collect_stage.set_command_arg("library_directory", args.library_directory)
        collect_stage.set_command_arg("storage_type", args.storage)
        if args.limit:
            collect_stage.set_command_arg("limit", args.limit)

        try:
            # Upload config to storage if available
            if book_manager:
                storage_path = book_manager.meta_path("run_config.json")
                await book_manager.storage.write_file(storage_path, str(config_path))
                logger.info(f"Configuration uploaded to storage: {storage_path}")

            collect_stage.add_progress_update("Configuration written, starting collection")

            # Validate required storage configuration
            if not storage_config:
                raise ValueError("Storage configuration is required. Provide --storage option.")
            # Create book collector with configuration
            collector = BookCollector(
                process_summary_stage=collect_stage,
                storage_config=storage_config,
                run_config=run_config,
                secrets_dir=args.secrets_dir,
                refresh_mode=args.refresh,
            )

            # Run book collection with pagination
            collect_stage.add_progress_update("Starting book collection")
            completed = await collector.collect_books(args.limit)

            # Track completion status
            if completed:
                collect_stage.add_progress_update("Collection completed successfully")
            else:
                collect_stage.add_progress_update("Collection incomplete - interrupted or limited")

            return 0

        except Exception as e:
            # Record error in summary
            error_type = type(e).__name__
            collect_stage.add_error(error_type, str(e))

            if isinstance(e, KeyboardInterrupt):
                collect_stage.add_progress_update("Collection interrupted by user")
            else:
                collect_stage.add_progress_update(f"Collection failed: {e}")
            raise

        finally:
            # Clean up collector resources
            if "collector" in locals():
                await collector.cleanup()
            # Always end the stage and save summary
            run_summary.end_stage("collect")
            await save_process_summary(run_summary, book_manager)

            # Upload books database to storage
            await upload_database_to_storage(sqlite_db_path, book_manager)

            # Clean up book manager storage resources
            if "book_manager" in locals() and book_manager and hasattr(book_manager, "storage"):
                await book_manager.storage.close()

            # Display completion summary
            display_step_summary(run_summary, "collect")

    except Exception as e:
        import traceback

        if isinstance(e, KeyboardInterrupt):
            print("\n🚫 Collection interrupted by user")
        else:
            print("❌ Collection failed")
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    with asyncio.Runner() as runner:
        exit(runner.run(main()))
