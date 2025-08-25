#!/usr/bin/env python3
"""
Sync CLI Interface

Command-line interface for sync operations.
"""

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Any

from grin_to_s3.common import (
    DEFAULT_DOWNLOAD_RETRIES,
    DEFAULT_DOWNLOAD_TIMEOUT,
    DEFAULT_MAX_SEQUENTIAL_FAILURES,
    parse_barcode_arguments,
)
from grin_to_s3.logging_config import setup_logging
from grin_to_s3.process_summary import (
    create_book_manager_for_uploads,
    create_process_summary,
    display_step_summary,
    get_current_stage,
    save_process_summary,
)
from grin_to_s3.run_config import (
    DEFAULT_WORKER_CONCURRENCY,
    RunConfig,
    apply_run_config_to_args,
    build_storage_config_dict,
    find_run_config,
    load_run_config,
    setup_run_database_path,
)
from grin_to_s3.sync.pipeline import SyncPipeline
from grin_to_s3.sync.status import show_sync_status, validate_database_file

logger = logging.getLogger(__name__)


def _collect_task_concurrency_overrides(args) -> dict[str, int]:
    """Collect task concurrency overrides from CLI arguments."""
    overrides = {}

    task_concurrency_args = [
        ("task_check_concurrency", getattr(args, "task_check_concurrency", None)),
        ("task_download_concurrency", getattr(args, "task_download_concurrency", None)),
        ("task_decrypt_concurrency", getattr(args, "task_decrypt_concurrency", None)),
        ("task_upload_concurrency", getattr(args, "task_upload_concurrency", None)),
        ("task_unpack_concurrency", getattr(args, "task_unpack_concurrency", None)),
        ("task_extract_marc_concurrency", getattr(args, "task_extract_marc_concurrency", None)),
        ("task_extract_ocr_concurrency", getattr(args, "task_extract_ocr_concurrency", None)),
        ("task_export_csv_concurrency", getattr(args, "task_export_csv_concurrency", None)),
        ("task_cleanup_concurrency", getattr(args, "task_cleanup_concurrency", None)),
    ]

    for config_key, value in task_concurrency_args:
        if value is not None:
            overrides[config_key] = value

    return overrides


def _parse_and_validate_barcodes(args, sync_stage) -> list[str] | None:
    """Parse and validate barcode arguments from either --barcodes or --barcodes-file."""
    try:
        barcodes_str = getattr(args, "barcodes", None)
        barcodes_file = getattr(args, "barcodes_file", None)

        specific_barcodes = parse_barcode_arguments(barcodes_str, barcodes_file)

        if specific_barcodes is None:
            return None

        source_desc = "command line" if barcodes_str else f"file '{barcodes_file}'"
        print(f"Filtering to specific barcodes from {source_desc}: {', '.join(specific_barcodes)}")
        sync_stage.set_command_arg("specific_barcodes", len(specific_barcodes))
        sync_stage.add_progress_update(f"Filtering to {len(specific_barcodes)} specific barcodes from {source_desc}")
        return specific_barcodes
    except (ValueError, FileNotFoundError) as e:
        sync_stage.add_error("BarcodeValidationError", str(e))
        print(f"Error: {e}")
        sys.exit(1)


def _apply_single_book_optimization(
    run_config: RunConfig, specific_barcodes: list[str] | None, sync_stage
) -> RunConfig:
    """Apply single-book optimization if only one barcode is specified."""
    if not (specific_barcodes and len(specific_barcodes) == 1):
        return run_config

    # Create optimized config for single book
    optimized_config = RunConfig(run_config.config_dict.copy())
    optimized_config.config_dict["sync_config"] = {
        **optimized_config.config_dict.get("sync_config", {}),
        "concurrent_downloads": 1,  # Optimal for single book
        "concurrent_uploads": 1,  # Optimal for single book
    }
    print("  - Concurrent downloads: 1")
    print("  - Concurrent uploads: 1")
    print()
    sync_stage.add_progress_update("Single book mode optimization applied")
    return optimized_config


def _setup_signal_handlers(pipeline, sync_stage) -> None:
    """Set up signal handlers for graceful shutdown."""

    def signal_handler(signum: int, frame: Any) -> None:
        if pipeline._shutdown_requested:
            # Second interrupt - hard exit
            print(f"\nReceived second signal {signum}, forcing immediate exit...")
            sync_stage.add_progress_update("Force exit requested")
            # Use os._exit() instead of sys.exit() to avoid asyncio shutdown issues
            os._exit(1)
        print(f"\nReceived signal {signum}, finishing sync for books in flight...")
        print("Press Control-C again to force immediate exit")
        sync_stage.add_progress_update("Graceful shutdown requested")
        pipeline._shutdown_requested = True

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


async def _run_sync_pipeline(args, run_config: RunConfig, sync_stage) -> None:
    """Execute the main sync pipeline logic."""
    # Parse and validate barcodes
    specific_barcodes = _parse_and_validate_barcodes(args, sync_stage)

    # Apply single-book optimization if needed
    run_config = _apply_single_book_optimization(run_config, specific_barcodes, sync_stage)

    # Create pipeline with final configuration
    pipeline = SyncPipeline.from_run_config(
        config=run_config,
        process_summary_stage=sync_stage,
        force=args.force,
        dry_run=args.dry_run,
        skip_extract_ocr=args.skip_extract_ocr,
        skip_extract_marc=args.skip_extract_marc,
        skip_csv_export=args.skip_csv_export,
        skip_staging_cleanup=args.skip_staging_cleanup,
        skip_database_backup=args.skip_database_backup,
        download_timeout=args.download_timeout,
        download_retries=args.download_retries,
        max_sequential_failures=args.max_sequential_failures,
        task_concurrency_overrides=_collect_task_concurrency_overrides(args),
        worker_count=args.workers,
    )

    # Set up signal handlers for graceful shutdown
    _setup_signal_handlers(pipeline, sync_stage)

    try:
        # Execute the sync pipeline
        sync_stage.add_progress_update("Starting sync pipeline")
        await pipeline.setup_sync_loop(queues=args.queue, limit=args.limit, specific_barcodes=specific_barcodes)
        sync_stage.add_progress_update("Sync pipeline completed successfully")
    finally:
        # Clean up pipeline resources
        await pipeline.cleanup()


def _handle_pipeline_error(e: Exception, sync_stage) -> None:
    """Handle pipeline execution errors."""
    if isinstance(e, KeyboardInterrupt):
        sync_stage.add_progress_update("Operation cancelled by user")
        sync_stage.add_error("KeyboardInterrupt", "User cancelled operation")
        print("\nOperation cancelled by user")
    else:
        error_type = type(e).__name__
        sync_stage.add_error(error_type, str(e))
        sync_stage.add_progress_update(f"Pipeline failed: {error_type}")
        print(f"Pipeline failed: {e}")
        sys.exit(1)


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

    # Collect task concurrency overrides
    task_concurrency_overrides = _collect_task_concurrency_overrides(args)

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

                # Apply task concurrency overrides and compression settings to run config
                if task_concurrency_overrides or args.skip_compression_meta or args.skip_compression_full:
                    if "sync_config" not in run_config:
                        run_config["sync_config"] = {}
                    if task_concurrency_overrides:
                        run_config["sync_config"].update(task_concurrency_overrides)
                    # Set compression based on CLI flags (default True, disabled if respective --skip-compression-* flag)
                    run_config["sync_config"]["compression_meta_enabled"] = not args.skip_compression_meta
                    run_config["sync_config"]["compression_full_enabled"] = not args.skip_compression_full

                # Write back to config file
                with open(config_path, "w") as f:
                    json.dump(run_config, f, indent=2)

                print(f"Config in {config_path}")
                if task_concurrency_overrides:
                    print(f"Applied task concurrency overrides: {task_concurrency_overrides}")
            else:
                print(f"Using existing storage configuration from {config_path}")
                # Use storage type from run config if not explicitly provided
                if not args.storage:
                    args.storage = storage_type

                # Still apply task concurrency overrides and compression settings even if no storage args were provided
                if task_concurrency_overrides or args.skip_compression_meta or args.skip_compression_full:
                    if "sync_config" not in run_config:
                        run_config["sync_config"] = {}
                    if task_concurrency_overrides:
                        run_config["sync_config"].update(task_concurrency_overrides)
                    # Set compression based on CLI flags (default True, disabled if respective --skip-compression-* flag)
                    run_config["sync_config"]["compression_meta_enabled"] = not args.skip_compression_meta
                    run_config["sync_config"]["compression_full_enabled"] = not args.skip_compression_full

                    # Write back to config file
                    with open(config_path, "w") as f:
                        json.dump(run_config, f, indent=2)

                    messages = []
                    if task_concurrency_overrides:
                        messages.append(f"task concurrency overrides: {task_concurrency_overrides}")
                    if args.skip_compression_meta:
                        messages.append("meta compression disabled")
                    if args.skip_compression_full:
                        messages.append("full compression disabled")
                    print(f"Applied {', '.join(messages)}")

        except (json.JSONDecodeError, OSError) as e:
            print(f"Warning: Could not read run config: {e}")
            # Fall back to building from args
            storage_config = build_storage_config_dict(args)
    else:
        print(f"Note: No run config found at {config_path}, building from args")
        storage_config = build_storage_config_dict(args)

        # Create basic run config with task concurrency overrides and compression settings if provided
        if task_concurrency_overrides or args.skip_compression_meta or args.skip_compression_full:
            basic_run_config: dict[str, Any] = {"sync_config": {}}
            if task_concurrency_overrides:
                basic_run_config["sync_config"].update(task_concurrency_overrides)
            # Set compression based on CLI flags (default True, disabled if respective --skip-compression-* flag)
            basic_run_config["sync_config"]["compression_meta_enabled"] = not args.skip_compression_meta
            basic_run_config["sync_config"]["compression_full_enabled"] = not args.skip_compression_full

            with open(config_path, "w") as f:
                json.dump(basic_run_config, f, indent=2)

            messages = []
            if task_concurrency_overrides:
                messages.append(f"task concurrency overrides: {task_concurrency_overrides}")
            if args.skip_compression_meta:
                messages.append("meta compression disabled")
            if args.skip_compression_full:
                messages.append("full compression disabled")
            print(f"Created basic run config with {', '.join(messages)}")

    # Set up logging - use unified log file from run config
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

    # Create book storage for process summary uploads (skip in dry-run)
    book_manager = None if args.dry_run else await create_book_manager_for_uploads(args.run_name)

    # Create or load process summary
    run_summary = await create_process_summary(args.run_name, "sync", book_manager)
    sync_stage = get_current_stage(run_summary, "sync")
    sync_stage.set_command_arg("storage_type", args.storage)
    sync_stage.set_command_arg("force_mode", args.force)
    if args.limit:
        sync_stage.set_command_arg("limit", args.limit)
    if hasattr(args, "queue") and args.queue:
        sync_stage.set_command_arg("queues", args.queue)

    try:
        # Load run config and update storage configuration
        config_path = Path(args.db_path).parent / "run_config.json"
        run_config = load_run_config(str(config_path))

        # Update storage config if it was modified above
        if existing_storage_config != run_config.config_dict.get("storage_config", {}):
            run_config.config_dict["storage_config"] = {
                "type": args.storage,
                "config": storage_config,
                "prefix": "",
            }

        # Execute the main sync pipeline
        await _run_sync_pipeline(args, run_config, sync_stage)

    except Exception as e:
        # Handle all pipeline errors with appropriate error recording
        _handle_pipeline_error(e, sync_stage)

    finally:
        # Always finalize process summary regardless of success/failure/interruption
        run_summary.end_stage("sync")
        if not args.dry_run:
            await save_process_summary(run_summary, book_manager)

        # Display completion summary
        display_step_summary(run_summary, "sync")


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
  python grin.py sync pipeline --run-name harvard_2024 --queue converted

  # Sync books from multiple queues in order
  python grin.py sync pipeline --run-name harvard_2024 --queue converted --queue previous

  # Sync specific books only (no --queue needed)
  python grin.py sync pipeline --run-name harvard_2024 --barcodes "12345,67890,abcde"

  # Sync books from a file (no --queue needed)
  python grin.py sync pipeline --run-name harvard_2024 --barcodes-file barcodes.txt

  # Sync a single book (no --queue needed)
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
  python grin.py sync pipeline --run-name harvard_2024 --queue converted

  # Sync with explicit storage configuration
  python grin.py sync pipeline --run-name harvard_2024 --queue converted --storage r2
      --bucket-raw grin-raw --bucket-meta grin-meta --bucket-full grin-full

  # Sync specific books only (no --queue needed with --barcodes)
  python grin.py sync pipeline --run-name harvard_2024 --barcodes "12345,67890,abcde"

  # Sync books from a text file (no --queue needed with --barcodes-file)
  python grin.py sync pipeline --run-name harvard_2024 --barcodes-file my_books.txt

  # Retry failed syncs only
  python grin.py sync pipeline --run-name harvard_2024 --queue converted --status failed

  # Sync with limit and force overwrite
  python grin.py sync pipeline --run-name harvard_2024 --queue converted --limit 100 --force

  # Preview what would be processed without actually doing it
  python grin.py sync pipeline --run-name harvard_2024 --queue converted --dry-run --limit 10
        """,
    )

    pipeline_parser.add_argument("--run-name", required=True, help="Run name (e.g., harvard_2024)")
    pipeline_parser.add_argument(
        "--queue",
        choices=["converted", "previous", "changed", "all"],
        action="append",
        help="Queue type to process. Multiple options allowed (e.g., --queue converted --queue previous). Processed in order specified. Required unless --barcodes is provided.",
    )

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
    pipeline_parser.add_argument(
        "--barcodes-file", help="Path to a text file containing barcodes to sync (one per line, supports comments with #)"
    )
    pipeline_parser.add_argument("--status", help="Filter books by sync status (e.g., 'failed', 'pending')")
    pipeline_parser.add_argument("--force", action="store_true", help="Force download and overwrite existing files")
    pipeline_parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be processed without downloading or uploading files"
    )
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

    pipeline_parser.add_argument(
        "--skip-csv-export", action="store_true", help="Skip automatic CSV export (default: export CSV)"
    )

    # Compression options
    pipeline_parser.add_argument(
        "--skip-compression-meta", action="store_true", help="Skip compression for CSV files in meta bucket (default: compression enabled)"
    )
    pipeline_parser.add_argument(
        "--skip-compression-full", action="store_true", help="Skip compression for JSONL files in full bucket (default: compression enabled)"
    )

    # GRIN options
    pipeline_parser.add_argument(
        "--secrets-dir", help="Directory containing GRIN secrets (auto-detected from run config if not specified)"
    )

    # Task concurrency options
    pipeline_parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKER_CONCURRENCY,
        help=f"Total number of concurrent workers for processing. Workers are split between download and processing phases (default: {DEFAULT_WORKER_CONCURRENCY})",
    )
    pipeline_parser.add_argument(
        "--task-check-concurrency",
        type=int,
        help="Maximum concurrent check tasks",
    )
    pipeline_parser.add_argument(
        "--task-download-concurrency",
        type=int,
        help="Maximum concurrent download tasks",
    )
    pipeline_parser.add_argument(
        "--task-decrypt-concurrency",
        type=int,
        help="Maximum concurrent decrypt tasks",
    )
    pipeline_parser.add_argument(
        "--task-upload-concurrency",
        type=int,
        help="Maximum concurrent upload tasks",
    )
    pipeline_parser.add_argument(
        "--task-unpack-concurrency",
        type=int,
        help="Maximum concurrent unpack tasks",
    )
    pipeline_parser.add_argument(
        "--task-extract-marc-concurrency",
        type=int,
        help="Maximum concurrent MARC extraction tasks",
    )
    pipeline_parser.add_argument(
        "--task-extract-ocr-concurrency",
        type=int,
        help="Maximum concurrent OCR extraction tasks",
    )
    pipeline_parser.add_argument(
        "--task-export-csv-concurrency",
        type=int,
        help="Maximum concurrent CSV export tasks",
    )
    pipeline_parser.add_argument(
        "--task-cleanup-concurrency",
        type=int,
        help="Maximum concurrent cleanup tasks",
    )

    # Download options
    pipeline_parser.add_argument(
        "--download-timeout",
        type=int,
        default=DEFAULT_DOWNLOAD_TIMEOUT,
        help=f"Timeout for book downloads in seconds (default: {DEFAULT_DOWNLOAD_TIMEOUT}, separate from HTML requests)",
    )
    pipeline_parser.add_argument(
        "--download-retries",
        type=int,
        default=DEFAULT_DOWNLOAD_RETRIES,
        help=f"Number of retry attempts for failed downloads (default: {DEFAULT_DOWNLOAD_RETRIES})",
    )
    pipeline_parser.add_argument(
        "--max-sequential-failures",
        type=int,
        default=DEFAULT_MAX_SEQUENTIAL_FAILURES,
        help=f"Exit pipeline after this many consecutive failures (default: {DEFAULT_MAX_SEQUENTIAL_FAILURES})",
    )

    # Staging cleanup
    pipeline_parser.add_argument(
        "--skip-staging-cleanup", action="store_true", help="Skip deletion of files in staging directory after sync"
    )

    # Database backup options
    pipeline_parser.add_argument(
        "--skip-database-backup",
        action="store_true",
        help="Skip automatic database backup and upload (default: backup enabled)",
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
    status_parser.add_argument(
        "--storage-type", choices=["local", "minio", "r2", "s3", "gcs"], help="Filter by storage type"
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Validate --queue and barcode options mutual exclusivity for pipeline command
    if args.command == "pipeline":
        has_queue = hasattr(args, "queue") and args.queue
        has_barcodes = hasattr(args, "barcodes") and args.barcodes
        has_barcodes_file = hasattr(args, "barcodes_file") and getattr(args, "barcodes_file", None)

        barcode_count = int(bool(has_barcodes)) + int(bool(has_barcodes_file))

        # Check mutual exclusivity between queue and barcode options
        if has_queue and barcode_count > 0:
            print(
                "Error: --queue and barcode options (--barcodes, --barcodes-file) are mutually exclusive. "
                "Use either --queue to process from queues or barcode options to process specific books."
            )
            sys.exit(1)

        # Ensure at least one option is provided
        elif not has_queue and barcode_count == 0:
            print(
                "Error: Either --queue or a barcode option (--barcodes or --barcodes-file) is required. "
                "Use --queue to process from queues or barcode options to process specific books."
            )
            sys.exit(1)

    if args.command == "pipeline":
        await cmd_pipeline(args)
    elif args.command == "status":
        await cmd_status(args)


if __name__ == "__main__":
    asyncio.run(main())
