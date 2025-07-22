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
from pathlib import Path
from typing import Any

from grin_to_s3.common import setup_logging, setup_storage_with_checks
from grin_to_s3.process_summary import create_process_summary, get_current_stage, save_process_summary
from grin_to_s3.run_config import (
    RunConfig,
    apply_run_config_to_args,
    build_storage_config_dict,
    load_run_config,
    setup_run_database_path,
)
from grin_to_s3.sync.models import validate_and_parse_barcodes
from grin_to_s3.sync.status import show_sync_status, validate_database_file

logger = logging.getLogger(__name__)


async def cmd_pipeline(args) -> None:
    """Handle the 'pipeline' command."""
    # Set up database path
    db_path = setup_run_database_path(args, args.run_name)

    # Check if any storage-related arguments were explicitly provided on command line
    # (before applying run config defaults)
    storage_checks = [
        ("storage", getattr(args, "storage", None) is not None),
        ("bucket_raw", getattr(args, "bucket_raw", None) is not None),
        ("bucket_meta", getattr(args, "bucket_meta", None) is not None),
        ("bucket_full", getattr(args, "bucket_full", None) is not None),
        ("storage_config", getattr(args, "storage_config", None) is not None),
    ]
    explicit_storage_args = any(check[1] for check in storage_checks)


    # Apply run configuration defaults
    apply_run_config_to_args(args, db_path)

    print(f"Database: {db_path}")

    # Validate database
    validate_database_file(args.db_path)

    # Build storage configuration from run config
    config_path = Path(args.db_path).parent / "run_config.json"
    storage_config = {}
    existing_storage_config = {}

    if config_path.exists():
        try:
            with open(config_path) as f:
                run_config = json.load(f)

            # Get storage config from run config
            existing_storage_config = run_config.get("storage_config", {})
            storage_type = existing_storage_config.get("type")
            storage_config = existing_storage_config.get("config", {})

            if explicit_storage_args:
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
    logger.info(f"SYNC PIPELINE STARTED - storage={args.storage} force={args.force}{barcodes_info}{limit_info}")
    logger.info(f"Command: {' '.join(sys.argv)}")

    # Import and create pipeline
    from grin_to_s3.process_summary import create_book_manager_for_uploads
    from grin_to_s3.sync.pipeline import SyncPipeline

    # Create book storage for process summary uploads
    book_storage = await create_book_manager_for_uploads(args.run_name)

    # Create or load process summary
    run_summary = await create_process_summary(args.run_name, "sync", book_storage)
    sync_stage = get_current_stage(run_summary, "sync")
    sync_stage.set_command_arg("storage_type", args.storage)
    sync_stage.set_command_arg("force_mode", args.force)
    if args.limit:
        sync_stage.set_command_arg("limit", args.limit)

    try:
        try:
            # Load run config for factory method
            config_path = Path(args.db_path).parent / "run_config.json"
            run_config = load_run_config(str(config_path))

            # Update storage config in run_config if it was modified above
            if existing_storage_config != run_config.config_dict.get("storage_config", {}):
                run_config.config_dict["storage_config"] = {
                    "type": args.storage,
                    "config": storage_config,
                    "prefix": "",
                }

            pipeline = SyncPipeline.from_run_config(
                config=run_config,
                process_summary_stage=sync_stage,
                force=args.force,
                skip_extract_ocr=args.skip_extract_ocr,
                skip_extract_marc=args.skip_extract_marc,
                skip_enrichment=args.skip_enrichment,
                skip_csv_export=args.skip_csv_export,
                skip_staging_cleanup=args.skip_staging_cleanup,
                skip_database_backup=args.skip_database_backup,
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
                # Clone run config and modify sync settings for single book
                single_book_config = RunConfig(run_config.config_dict.copy())
                single_book_config.config_dict["sync_config"] = {
                    **single_book_config.config_dict.get("sync_config", {}),
                    "concurrent_downloads": 1,  # Optimal for single book
                    "concurrent_uploads": 1,  # Optimal for single book
                    "batch_size": 1,  # Single book batch
                }

                pipeline = SyncPipeline.from_run_config(
                    config=single_book_config,
                    process_summary_stage=sync_stage,
                    force=args.force,
                    skip_extract_ocr=args.skip_extract_ocr,
                    skip_extract_marc=args.skip_extract_marc,
                    skip_enrichment=args.skip_enrichment,
                    skip_csv_export=args.skip_csv_export,
                    skip_staging_cleanup=args.skip_staging_cleanup,
                    skip_database_backup=args.skip_database_backup,
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
        await save_process_summary(run_summary, book_storage)


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

    # Runtime options (configuration options are stored in run config)
    pipeline_parser.add_argument("--limit", type=int, help="Limit number of books to sync")
    pipeline_parser.add_argument(
        "--barcodes", help="Comma-separated list of specific barcodes to sync (e.g., '12345,67890,abcde')"
    )
    pipeline_parser.add_argument("--status", help="Filter books by sync status (e.g., 'failed', 'pending')")
    pipeline_parser.add_argument("--force", action="store_true", help="Force download and overwrite existing files")
    pipeline_parser.add_argument(
        "--grin-library-directory", help="GRIN library directory name (auto-detected from run config if not specified)"
    )

    # OCR extraction options
    pipeline_parser.add_argument(
        "--skip-extract-ocr", action="store_true", help="Skip OCR text extraction (default: extract OCR)"
    )

    # MARC extraction options
    pipeline_parser.add_argument(
        "--skip-extract-marc", action="store_true", help="Skip MARC metadata extraction (default: extract MARC)"
    )

    # Enrichment options
    pipeline_parser.add_argument(
        "--skip-enrichment", action="store_true", help="Skip automatic enrichment (default: enrichment enabled)"
    )
    pipeline_parser.add_argument(
        "--skip-csv-export", action="store_true", help="Skip automatic CSV export (default: export CSV)"
    )

    # GRIN options
    pipeline_parser.add_argument(
        "--secrets-dir", help="Directory containing GRIN secrets (auto-detected from run config if not specified)"
    )

    # Staging cleanup
    pipeline_parser.add_argument(
        "--skip-staging-cleanup", action="store_true", help="Skip deletion of files in staging directory after sync"
    )

    # Database backup options
    pipeline_parser.add_argument(
        "--skip-database-backup", action="store_true",
        help="Skip automatic database backup and upload (default: backup enabled)"
    )

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
    status_parser.add_argument("--storage-type", choices=["local", "minio", "r2", "s3", "gcs"], help="Filter by storage type")

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
