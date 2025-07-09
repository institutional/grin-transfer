#!/usr/bin/env python3
"""
Sync CLI Interface

Command-line interface for sync operations.
"""

import argparse
import asyncio
import json
import logging
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from grin_to_s3.common import setup_logging, setup_storage_with_checks
from grin_to_s3.process_summary import create_process_summary, get_current_stage, save_process_summary
from grin_to_s3.run_config import (
    apply_run_config_to_args,
    build_storage_config_dict,
    setup_run_database_path,
)
from grin_to_s3.sync.catchup import (
    confirm_catchup_sync,
    find_catchup_books,
    get_books_for_catchup_sync,
    mark_books_for_catchup_processing,
    run_catchup_validation,
    show_catchup_dry_run,
)
from grin_to_s3.sync.models import validate_and_parse_barcodes
from grin_to_s3.sync.status import show_sync_status, validate_database_file

logger = logging.getLogger(__name__)


async def cmd_pipeline(args) -> None:
    """Handle the 'pipeline' command."""
    # Set up database path and apply run configuration
    db_path = setup_run_database_path(args, args.run_name)
    apply_run_config_to_args(args, db_path)

    print(f"Database: {db_path}")

    # Validate database
    validate_database_file(args.db_path)

    # Build storage configuration from run config
    config_path = Path(args.db_path).parent / "run_config.json"
    storage_config = {}

    if config_path.exists():
        try:
            with open(config_path) as f:
                run_config = json.load(f)

            # Get storage config from run config
            existing_storage_config = run_config.get("storage_config", {})
            storage_type = existing_storage_config.get("type")
            storage_config = existing_storage_config.get("config", {})

            # Check if any storage-related arguments were explicitly provided that should override
            explicit_storage_args = any(
                [
                    getattr(args, "storage", None) and args.storage,
                    getattr(args, "bucket_raw", None),
                    getattr(args, "bucket_meta", None),
                    getattr(args, "bucket_full", None),
                    getattr(args, "storage_config", None),
                ]
            )

            if explicit_storage_args:
                print("Explicit storage arguments provided, merging with run config...")
                # Build args-based config and merge with existing
                args_storage_config = build_storage_config_dict(args)

                # Merge: start with existing and override with args
                merged_config = storage_config.copy()
                for k, v in args_storage_config.items():
                    if v is not None:
                        merged_config[k] = v

                storage_config_dict = {
                    "type": args.storage or storage_type,
                    "config": merged_config,
                    "prefix": args_storage_config.get("prefix", existing_storage_config.get("prefix", "")),
                }

                run_config["storage_config"] = storage_config_dict
                storage_config = merged_config

                # Write back to config file
                with open(config_path, "w") as f:
                    json.dump(run_config, f, indent=2)

                print(f"Updated storage configuration in {config_path}")
            else:
                print(f"Using existing storage configuration from {config_path}")
                # Use storage type from run config if not explicitly provided
                if not args.storage:
                    args.storage = storage_type

        except (json.JSONDecodeError, OSError) as e:
            print(f"Warning: Could not read run config: {e}")
            # Fall back to building from args
            storage_config = build_storage_config_dict(args)
    else:
        print(f"Note: No run config found at {config_path}, building from args")
        storage_config = build_storage_config_dict(args)

    # Set up storage with auto-configuration and connectivity checks
    await setup_storage_with_checks(args.storage, storage_config)

    # Set up logging - use unified log file from run config
    from grin_to_s3.run_config import find_run_config

    run_config = find_run_config(args.db_path)
    if run_config is None:
        print(f"Error: No run configuration found. Expected run_config.json in {Path(args.db_path).parent}")
        print("Run 'python grin.py collect' first to generate the run configuration.")
        sys.exit(1)
    setup_logging(args.log_level, run_config.log_file)

    # Log sync pipeline startup
    logger = logging.getLogger(__name__)
    barcodes_info = f" barcodes={','.join(args.barcodes)}" if hasattr(args, "barcodes") and args.barcodes else ""
    limit_info = f" limit={args.limit}" if hasattr(args, "limit") and args.limit else ""
    logger.info(
        f"SYNC PIPELINE STARTED - storage={args.storage} concurrent={args.concurrent}/{args.concurrent_uploads}"
        f" force={args.force}{barcodes_info}{limit_info}"
    )
    logger.info(f"Command: {' '.join(sys.argv)}")

    # Import and create pipeline
    from grin_to_s3.sync.pipeline import SyncPipeline

    # Create or load process summary
    run_summary = await create_process_summary(args.run_name, "sync")
    sync_stage = get_current_stage(run_summary, "sync")
    sync_stage.set_command_arg("storage_type", args.storage)
    sync_stage.set_command_arg("concurrent_downloads", args.concurrent)
    sync_stage.set_command_arg("concurrent_uploads", args.concurrent_uploads)
    sync_stage.set_command_arg("batch_size", args.batch_size)
    sync_stage.set_command_arg("force_mode", args.force)
    if args.limit:
        sync_stage.set_command_arg("limit", args.limit)

    try:
        try:
            pipeline = SyncPipeline(
                db_path=args.db_path,
                storage_type=args.storage,
                storage_config=storage_config,
                library_directory=args.grin_library_directory,
                process_summary_stage=sync_stage,
                concurrent_downloads=args.concurrent,
                concurrent_uploads=args.concurrent_uploads,
                batch_size=args.batch_size,
                secrets_dir=args.secrets_dir,
                gpg_key_file=args.gpg_key_file,
                force=args.force,
                staging_dir=args.staging_dir,
                disk_space_threshold=args.disk_space_threshold,
                skip_extract_ocr=args.skip_extract_ocr,
                enrichment_enabled=not args.skip_enrichment,
                enrichment_workers=args.enrichment_workers,
                skip_csv_export=args.skip_csv_export,
            )

            # Set up signal handlers for graceful shutdown
            def signal_handler(signum: int, frame: Any) -> None:
                if pipeline._shutdown_requested:
                    # Second interrupt - hard exit
                    print(f"\nReceived second signal {signum}, forcing immediate exit...")
                    sync_stage.add_progress_update("Force exit requested")
                    sys.exit(1)
                print(f"\nReceived signal {signum}, shutting down gracefully...")
                print("Press Control-C again to force immediate exit")
                sync_stage.add_progress_update("Graceful shutdown requested")
                pipeline._shutdown_requested = True

            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)

            # Parse barcodes if provided
            specific_barcodes = None
            if hasattr(args, "barcodes") and args.barcodes:
                try:
                    specific_barcodes = validate_and_parse_barcodes(args.barcodes)
                    print(f"Filtering to specific barcodes: {', '.join(specific_barcodes)}")
                    sync_stage.set_command_arg("specific_barcodes", len(specific_barcodes))
                    sync_stage.add_progress_update(f"Filtering to {len(specific_barcodes)} specific barcodes")
                except ValueError as e:
                    sync_stage.add_error("BarcodeValidationError", str(e))
                    print(f"Error: Invalid barcodes: {e}")
                    sys.exit(1)

            # Auto-optimization for single barcode processing
            if specific_barcodes and len(specific_barcodes) == 1:
                print(f"Single barcode detected: {specific_barcodes[0]}")
                print("Auto-optimizing settings for single book processing...")

                # Create optimized pipeline for single book
                pipeline = SyncPipeline(
                    db_path=args.db_path,
                    storage_type=args.storage,
                    storage_config=storage_config,
                    library_directory=args.grin_library_directory,
                    process_summary_stage=sync_stage,
                    concurrent_downloads=1,  # Optimal for single book
                    concurrent_uploads=1,  # Optimal for single book
                    batch_size=1,  # Single book batch
                    secrets_dir=args.secrets_dir,
                    gpg_key_file=args.gpg_key_file,
                    force=args.force,
                    staging_dir=args.staging_dir,
                    disk_space_threshold=args.disk_space_threshold,
                    skip_extract_ocr=args.skip_extract_ocr,
                    enrichment_enabled=not args.skip_enrichment,
                    enrichment_workers=args.enrichment_workers,
                    skip_csv_export=args.skip_csv_export,
                )
                print("  - Concurrent downloads: 1")
                print("  - Concurrent uploads: 1")
                print("  - Batch size: 1")
                print()
                sync_stage.add_progress_update("Single book mode optimization applied")

            sync_stage.add_progress_update("Starting sync pipeline")
            await pipeline.run_sync(limit=args.limit, specific_barcodes=specific_barcodes)
            sync_stage.add_progress_update("Sync pipeline completed successfully")

        except KeyboardInterrupt:
            sync_stage.add_progress_update("Operation cancelled by user")
            sync_stage.add_error("KeyboardInterrupt", "User cancelled operation")
            print("\nOperation cancelled by user")
        except Exception as e:
            error_type = type(e).__name__
            sync_stage.add_error(error_type, str(e))
            sync_stage.add_progress_update(f"Pipeline failed: {error_type}")
            print(f"Pipeline failed: {e}")
            sys.exit(1)

    finally:
        # Always end the stage and save summary
        run_summary.end_stage("sync")
        await save_process_summary(run_summary)


async def cmd_status(args) -> None:
    """Handle the 'status' command."""
    # Set up database path and apply run configuration
    db_path = setup_run_database_path(args, args.run_name)
    logger.debug(f"Using run: {args.run_name}")
    print(f"Database: {db_path}")

    try:
        await show_sync_status(args.db_path, args.storage_type)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


async def cmd_catchup(args) -> None:
    """Handle the 'catchup' command."""
    # Set up database path and apply run configuration
    db_path = setup_run_database_path(args, args.run_name)
    logger.debug(f"Using run: {args.run_name}")
    print(f"Database: {db_path}")

    # Set up signal handlers for graceful shutdown
    def signal_handler(signum: int, frame: Any) -> None:
        print(f"\nReceived signal {signum}, shutting down gracefully...")
        raise KeyboardInterrupt()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Validate database
    validate_database_file(args.db_path)

    # Build storage configuration from run config
    config_path = Path(args.db_path).parent / "run_config.json"
    if not config_path.exists():
        print(f"❌ Error: No run config found at {config_path}")
        print("Run collect_books.py first to create a run configuration")
        sys.exit(1)

    try:
        with open(config_path) as f:
            run_config = json.load(f)

        storage_config_dict = run_config.get("storage_config")
        if not storage_config_dict:
            print("❌ Error: No storage configuration found in run config")
            print("Run collect_books.py with storage configuration first")
            sys.exit(1)

        storage_type = storage_config_dict.get("type")
        storage_config = storage_config_dict.get("config", {})

        # Validate configuration
        storage_type, storage_config = await run_catchup_validation(run_config, storage_type, storage_config)

    except (json.JSONDecodeError, OSError) as e:
        print(f"❌ Error reading run config: {e}")
        sys.exit(1)

    # Get current timestamp for session tracking
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    try:
        # Find books available for catchup
        converted_barcodes, all_books, catchup_candidates = await find_catchup_books(
            args.db_path, run_config.get("library_directory"), run_config.get("secrets_dir")
        )

        if not catchup_candidates:
            print("No books available for catchup")
            return

        # Get books that need sync
        books_to_sync = await get_books_for_catchup_sync(args.db_path, storage_type, catchup_candidates, args.limit)

        if not books_to_sync:
            return

        # Handle dry-run mode
        if args.dry_run:
            show_catchup_dry_run(books_to_sync)
            return

        # Confirm with user
        if not confirm_catchup_sync(books_to_sync, args.yes):
            return

        print(f"Catchup sync: {len(books_to_sync):,} books")

        # Mark books for processing
        await mark_books_for_catchup_processing(args.db_path, books_to_sync, timestamp)

        # Import and create pipeline for catchup
        # Create a minimal process stage for catchup (not part of main pipeline tracking)
        from grin_to_s3.process_summary import ProcessStageMetrics
        from grin_to_s3.sync.pipeline import SyncPipeline
        catchup_stage = ProcessStageMetrics("catchup")

        pipeline = SyncPipeline(
            db_path=args.db_path,
            storage_type=storage_type,
            storage_config=storage_config,
            library_directory=run_config.get("library_directory"),
            process_summary_stage=catchup_stage,
            concurrent_downloads=args.concurrent,
            concurrent_uploads=args.concurrent_uploads,
            batch_size=args.batch_size,
            secrets_dir=run_config.get("secrets_dir"),
            gpg_key_file=args.gpg_key_file,
            force=args.force,
        )

        # Use a custom method to sync these specific books
        await pipeline._run_catchup_sync(books_to_sync, args.limit)

    except KeyboardInterrupt:
        print("\nCatchup cancelled by user")
    except Exception as e:
        print(f"Catchup failed: {e}")
        sys.exit(1)


async def main() -> None:
    """Main CLI entry point for sync commands."""
    parser = argparse.ArgumentParser(
        description="GRIN sync management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Sync all converted books in collection
  python grin.py sync pipeline --run-name harvard_2024

  # Sync specific books only
  python grin.py sync pipeline --run-name harvard_2024 --barcodes "12345,67890,abcde"

  # Sync a single book (auto-optimized settings)
  python grin.py sync pipeline --run-name harvard_2024 --barcodes "39015123456789"

  # Check sync status
  python grin.py sync status --run-name harvard_2024

  # Download already-converted books
  python grin.py sync catchup --run-name harvard_2024
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
  python grin.py sync pipeline --run-name harvard_2024

  # Sync with explicit storage configuration
  python grin.py sync pipeline --run-name harvard_2024 --storage r2
      --bucket-raw grin-raw --bucket-meta grin-meta --bucket-full grin-full

  # Sync specific books only
  python grin.py sync pipeline --run-name harvard_2024 --barcodes "12345,67890,abcde"

  # Sync with custom concurrency
  python grin.py sync pipeline --run-name harvard_2024 --concurrent 5

  # Retry failed syncs only
  python grin.py sync pipeline --run-name harvard_2024 --status failed

  # Sync with limit and force overwrite
  python grin.py sync pipeline --run-name harvard_2024 --limit 100 --force
        """,
    )

    pipeline_parser.add_argument("--run-name", required=True, help="Run name (e.g., harvard_2024)")

    # Storage configuration
    pipeline_parser.add_argument(
        "--storage",
        choices=["minio", "r2", "s3"],
        help="Storage backend (auto-detected from run config if not specified)",
    )
    pipeline_parser.add_argument(
        "--bucket-raw", help="Raw data bucket (auto-detected from run config if not specified)"
    )
    pipeline_parser.add_argument(
        "--bucket-meta", help="Metadata bucket (auto-detected from run config if not specified)"
    )
    pipeline_parser.add_argument(
        "--bucket-full", help="Full-text bucket (auto-detected from run config if not specified)"
    )
    pipeline_parser.add_argument("--credentials-file", help="Custom credentials file path")

    # Pipeline options
    pipeline_parser.add_argument("--concurrent", type=int, default=5, help="Concurrent downloads (default: 5)")
    pipeline_parser.add_argument("--concurrent-uploads", type=int, default=10, help="Concurrent uploads (default: 10)")
    pipeline_parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing (default: 100)")
    pipeline_parser.add_argument("--limit", type=int, help="Limit number of books to sync")
    pipeline_parser.add_argument(
        "--barcodes", help="Comma-separated list of specific barcodes to sync (e.g., '12345,67890,abcde')"
    )
    pipeline_parser.add_argument("--force", action="store_true", help="Force download and overwrite existing files")
    pipeline_parser.add_argument(
        "--grin-library-directory", help="GRIN library directory name (required, from run config)"
    )

    # Staging directory options
    pipeline_parser.add_argument(
        "--staging-dir", help="Custom staging directory path (default: output/run-name/staging)"
    )
    pipeline_parser.add_argument(
        "--disk-space-threshold",
        type=float,
        default=0.9,
        help="Disk usage threshold to pause downloads (0.0-1.0, default: 0.9)",
    )

    # OCR extraction options
    pipeline_parser.add_argument(
        "--skip-extract-ocr", action="store_true", help="Skip OCR text extraction (default: extract OCR)"
    )

    # Enrichment options
    pipeline_parser.add_argument(
        "--skip-enrichment", action="store_true", help="Skip automatic enrichment (default: enrichment enabled)"
    )
    pipeline_parser.add_argument(
        "--enrichment-workers", type=int, default=1, help="Number of enrichment workers (default: 1)"
    )
    pipeline_parser.add_argument(
        "--skip-csv-export", action="store_true", help="Skip automatic CSV export (default: export CSV)"
    )

    # GRIN options
    pipeline_parser.add_argument(
        "--secrets-dir", help="Directory containing GRIN secrets (auto-detected from run config if not specified)"
    )
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
  python grin.py sync status --run-name harvard_2024

  # Check sync status for specific storage type
  python grin.py sync status --run-name harvard_2024 --storage-type r2
        """,
    )

    status_parser.add_argument("--run-name", required=True, help="Run name (e.g., harvard_2024)")
    status_parser.add_argument("--storage-type", choices=["local", "minio", "r2", "s3"], help="Filter by storage type")

    # Catchup command
    catchup_parser = subparsers.add_parser(
        "catchup",
        help="Download already-converted books from GRIN",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check what books would be synced
  python grin.py sync catchup --run-name harvard_2024 --dry-run

  # Basic catchup (uses storage config from run)
  python grin.py sync catchup --run-name harvard_2024

  # Catchup with limit and auto-confirm
  python grin.py sync catchup --run-name harvard_2024 --limit 100 --yes

  # Catchup with custom concurrency
  python grin.py sync catchup --run-name harvard_2024 --concurrent 5
        """,
    )

    catchup_parser.add_argument("--run-name", required=True, help="Run name (e.g., harvard_2024)")
    catchup_parser.add_argument("--limit", type=int, help="Limit number of books to download")
    catchup_parser.add_argument("--concurrent", type=int, default=5, help="Concurrent downloads (default: 5)")
    catchup_parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing (default: 100)")
    catchup_parser.add_argument("--force", action="store_true", help="Force download and overwrite existing files")
    catchup_parser.add_argument("--yes", "-y", action="store_true", help="Auto-confirm without prompting")
    catchup_parser.add_argument(
        "--dry-run", action="store_true", help="Show what books would be synced without downloading"
    )
    catchup_parser.add_argument("--gpg-key-file", help="Custom GPG key file path")
    catchup_parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "pipeline":
        await cmd_pipeline(args)
    elif args.command == "status":
        await cmd_status(args)
    elif args.command == "catchup":
        await cmd_catchup(args)


if __name__ == "__main__":
    asyncio.run(main())
