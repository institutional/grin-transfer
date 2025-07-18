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
import sys

from .collector import BookCollector
from .config import ConfigManager

sys.path.append("..")
from grin_to_s3.common import (
    create_storage_buckets_or_directories,
    setup_logging,
    setup_storage_with_checks,
)
from grin_to_s3.process_summary import create_process_summary, get_current_stage, save_process_summary
from grin_to_s3.run_config import (
    DEFAULT_SYNC_BATCH_SIZE,
    DEFAULT_SYNC_CONCURRENT_DOWNLOADS,
    DEFAULT_SYNC_CONCURRENT_UPLOADS,
    DEFAULT_SYNC_DISK_SPACE_THRESHOLD,
    DEFAULT_SYNC_ENRICHMENT_WORKERS,
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


def print_resume_command(args, run_name: str) -> None:
    """
    Print the command to resume an interrupted collection.

    Args:
        args: Parsed command line arguments
        run_name: The run name for this collection
    """
    print("\n" + "=" * 60)
    print("TO RESUME THIS COLLECTION:")
    print("=" * 60)

    # Build the resume command
    cmd_parts = ["python grin.py collect"]

    # Add run name (most important for resume)
    cmd_parts.append(f'--run-name "{run_name}"')

    # Add other important arguments that should be preserved
    if args.limit:
        cmd_parts.append(f"--limit {args.limit}")

    # Directory is passed via --library-directory argument
    cmd_parts.append(f'--library-directory "{args.library_directory}"')

    if args.rate_limit != 5.0:  # Only if not default
        cmd_parts.append(f"--rate-limit {args.rate_limit}")

    if args.test_mode:
        cmd_parts.append("--test-mode")

    if args.storage:
        cmd_parts.append(f"--storage {args.storage}")
        if args.bucket_raw:
            cmd_parts.append(f"--bucket-raw {args.bucket_raw}")
        if args.bucket_meta:
            cmd_parts.append(f"--bucket-meta {args.bucket_meta}")
        if args.bucket_full:
            cmd_parts.append(f"--bucket-full {args.bucket_full}")
        if args.storage_config:
            for config_item in args.storage_config:
                cmd_parts.append(f"--storage-config {config_item}")

    if args.config_file:
        cmd_parts.append(f'--config-file "{args.config_file}"')

    # Custom output file path (if specified)
    if args.output_file:
        cmd_parts.insert(1, f'"{args.output_file}"')  # Add after script name

    resume_command = " ".join(cmd_parts)
    print(f"\n{resume_command}")
    print(f"\nRun directory: output/{run_name}/")
    print("\nTo enrich metadata:")
    print(f"python grin.py enrich --run-name {run_name}")
    print("\nTo check status:")
    print(f"python grin.py status --run-name {run_name}")
    print("=" * 60)


async def main():
    """CLI interface for book collection pipeline."""
    parser = argparse.ArgumentParser(
        description="Collect library book metadata in local db with progress tracking",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic collection with timestamp-based run name
  python grin.py collect --storage r2 --bucket-raw grin-raw \
                         --bucket-meta grin-meta --bucket-full grin-full

  # Named collection run
  python grin.py collect --run-name "harvard_fall_2024" --storage r2 \
                         --bucket-raw grin-raw --bucket-meta grin-meta \
                         --bucket-full grin-full

  # Specific output file with custom run
  python grin.py collect books.csv --run-name "test_run" --limit 100 \
                         --storage r2 --bucket-raw grin-raw \
                         --bucket-meta grin-meta --bucket-full grin-full

  # With rate limiting and storage checking
  python grin.py collect --rate-limit 0.5 --storage s3 \
                         --bucket-raw my-raw --bucket-meta my-meta \
                         --bucket-full my-full

  # With additional storage configuration
  python grin.py collect --storage minio --bucket-raw grin-raw \
                         --bucket-meta grin-meta --bucket-full grin-full \
                         --storage-config endpoint_url=localhost:9000

  # Local storage (requires base_path, no buckets needed)
  python grin.py collect --storage local --run-name "local_test" \
                         --storage-config base_path=/path/to/storage

  # Resume interrupted collection (uses run-specific progress files and saved config)
  python grin.py collect --run-name "harvard_fall_2024" --storage r2 \
                         --bucket-raw grin-raw --bucket-meta grin-meta \
                         --bucket-full grin-full
        """,
    )

    parser.add_argument("output_file", nargs="?", help="Output CSV file path (optional if using --run-name)")

    # Run identification
    parser.add_argument(
        "--run-name", help="Name for this collection run (defaults to timestamp). Used for all output files."
    )

    # Export options
    parser.add_argument("--limit", type=int, help="Limit number of books to process (for testing)")
    parser.add_argument("--rate-limit", type=float, default=5.0, help="API requests per second (default: 5.0)")
    parser.add_argument("--test-mode", action="store_true", help="Use mock data for testing (no network calls)")

    # Logging options
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )
    parser.add_argument("--log-dir", type=str, default=os.environ.get("GRIN_LOG_DIR", "logs"), help="Directory for log files (default: logs)")

    # Storage options
    parser.add_argument(
        "--storage", choices=["local", "minio", "r2", "s3"], required=True, help="Storage backend for run configuration"
    )
    parser.add_argument(
        "--bucket-raw",
        help="Raw data bucket (for sync archives, required for MinIO, optional for R2/S3 if in config file)",
    )
    parser.add_argument(
        "--bucket-meta",
        help="Metadata bucket (for CSV/database outputs, required for MinIO, optional for R2/S3 if in config file)",
    )
    parser.add_argument(
        "--bucket-full",
        help="Full-text bucket (for OCR outputs, required for MinIO, optional for R2/S3 if in config file)",
    )
    parser.add_argument("--storage-config", action="append", help="Additional storage config key=value")

    # Resume/progress options
    parser.add_argument(
        "--resume-file", default="output/default/progress.json", help="Progress tracking file for resume capability"
    )

    # Configuration options
    parser.add_argument("--config-file", type=str, help="Configuration file path (JSON format)")
    parser.add_argument("--create-config", type=str, help="Create default config file at specified path and exit")
    parser.add_argument(
        "--write-config",
        action="store_true",
        help="Write configuration to run directory and exit (requires storage options)",
    )
    parser.add_argument(
        "--secrets-dir",
        type=str,
        help="Directory containing GRIN secrets files (searches home directory if not specified)",
    )
    parser.add_argument(
        "--library-directory",
        type=str,
        required=True,
        help="Library directory name for GRIN API requests (e.g., Harvard, MIT, Yale)",
    )

    # Data source options (removed - only HTML mode supported)

    # Pagination options
    parser.add_argument("--page-size", type=int, help="Records per page for API requests (default: 10000)")
    parser.add_argument("--max-pages", type=int, help="Maximum pages to fetch (default: 1000)")
    parser.add_argument("--start-page", type=int, help="Starting page number (default: 1)")

    # Performance options
    parser.add_argument("--disable-prefetch", action="store_true", help="Disable HTTP prefetching for next page")

    # Sync configuration options (stored in run config for later use)
    parser.add_argument(
        "--sync-concurrent-downloads",
        type=int,
        default=DEFAULT_SYNC_CONCURRENT_DOWNLOADS,
        help=f"Concurrent downloads for sync operations (default: {DEFAULT_SYNC_CONCURRENT_DOWNLOADS})",
    )
    parser.add_argument(
        "--sync-concurrent-uploads",
        type=int,
        default=DEFAULT_SYNC_CONCURRENT_UPLOADS,
        help=f"Concurrent uploads for sync operations (default: {DEFAULT_SYNC_CONCURRENT_UPLOADS})",
    )
    parser.add_argument(
        "--sync-batch-size",
        type=int,
        default=DEFAULT_SYNC_BATCH_SIZE,
        help=f"Batch size for sync operations (default: {DEFAULT_SYNC_BATCH_SIZE})",
    )
    parser.add_argument("--sync-staging-dir", help="Custom staging directory path for sync operations (default: auto)")
    parser.add_argument(
        "--sync-disk-space-threshold",
        type=float,
        default=DEFAULT_SYNC_DISK_SPACE_THRESHOLD,
        help=f"Disk usage threshold to pause downloads (0.0-1.0, default: {DEFAULT_SYNC_DISK_SPACE_THRESHOLD})",
    )
    parser.add_argument(
        "--sync-enrichment-workers",
        type=int,
        default=DEFAULT_SYNC_ENRICHMENT_WORKERS,
        help=f"Number of enrichment workers for sync operations (default: {DEFAULT_SYNC_ENRICHMENT_WORKERS})",
    )
    parser.add_argument("--sync-gpg-key-file", help="Custom GPG key file path for sync operations")

    args = parser.parse_args()

    # Validate storage arguments
    # For R2 and S3, bucket names are optional (can be in config files)
    # For MinIO, bucket names are auto-configured from docker-compose
    # For local, no buckets needed but base_path is required

    # Validate local storage has base_path
    if args.storage == "local":
        # Check if base_path is provided in storage_config
        has_base_path = False
        if args.storage_config:
            for item in args.storage_config:
                if "=" in item and item.split("=", 1)[0] == "base_path":
                    has_base_path = True
                    break

        if not has_base_path:
            parser.error(
                "Local storage requires explicit base_path. "
                "Use: --storage local --storage-config base_path=/path/to/storage"
            )

    # Handle config creation
    if args.create_config:
        from pathlib import Path

        config = ConfigManager.create_default_config(Path(args.create_config))
        print(f"Created default configuration at {args.create_config}")
        return 0

    # Generate run name and output file paths
    from datetime import datetime
    from pathlib import Path

    if args.run_name:
        # Use provided run name (make it filename-safe)
        run_name = "".join(c for c in args.run_name if c.isalnum() or c in ("-", "_")).strip()
        if not run_name:
            parser.error("Run name must contain valid filename characters")
    else:
        # Generate timestamp-based run name
        run_name = datetime.now().strftime("run_%Y%m%d_%H%M%S")

    # Extract the actual identifier from run_name (remove "run_" prefix if present)
    run_identifier = run_name.removeprefix("run_") if run_name.startswith("run_") else run_name

    # Generate timestamp for output files (not resume files)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Determine output file path
    if args.output_file:
        output_file = args.output_file
    else:
        output_file = f"output/{run_name}/books_{timestamp}.csv"

    # Generate file paths - resume files stay consistent, outputs get timestamped
    log_file = f"{args.log_dir}/grin_pipeline_{run_name}_{timestamp}.log"
    progress_file = f"output/{run_name}/progress.json"  # No timestamp for resume
    sqlite_db = f"output/{run_name}/books.db"  # No timestamp for resume

    # Create directories
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    Path(progress_file).parent.mkdir(parents=True, exist_ok=True)

    # Handle write-config option
    if args.write_config:
        # Build storage configuration
        storage_config = None
        if args.storage:
            storage_dict: dict[str, str] = {}

            # Add bucket names if provided
            if args.bucket_raw:
                storage_dict["bucket_raw"] = args.bucket_raw
            if args.bucket_meta:
                storage_dict["bucket_meta"] = args.bucket_meta
            if args.bucket_full:
                storage_dict["bucket_full"] = args.bucket_full

            # Add additional storage config
            if args.storage_config:
                for item in args.storage_config:
                    if "=" in item:
                        key, value = item.split("=", 1)
                        storage_dict[key] = value

            storage_config = {"type": args.storage, "config": storage_dict, "prefix": ""}

        # Load configuration with CLI overrides
        config = ConfigManager.load_config(
            config_file=args.config_file,
            library_directory=args.library_directory,
            rate_limit=args.rate_limit,
            resume_file=progress_file,
            pagination_page_size=args.page_size,
            pagination_max_pages=args.max_pages,
            pagination_start_page=args.start_page,
            sqlite_db_path=sqlite_db,
        )

        # Build sync configuration from CLI arguments
        sync_config = {
            "concurrent_downloads": args.sync_concurrent_downloads,
            "concurrent_uploads": args.sync_concurrent_uploads,
            "batch_size": args.sync_batch_size,
            "staging_dir": args.sync_staging_dir,
            "disk_space_threshold": args.sync_disk_space_threshold,
            "enrichment_workers": args.sync_enrichment_workers,
            "gpg_key_file": args.sync_gpg_key_file,
        }

        # Create enhanced config dict with storage and runtime info
        config_dict = config.to_dict()
        config_dict.update(
            {
                "run_name": run_name,
                "run_identifier": run_identifier,
                "output_directory": f"output/{run_name}",
                "sqlite_db_path": sqlite_db,
                "progress_file": progress_file,
                "log_file": log_file,
                "storage_config": storage_config,
                "sync_config": sync_config,
                "secrets_dir": args.secrets_dir,
                "limit": args.limit,
            }
        )

        # Write config to run directory
        config_path = Path(f"output/{run_name}/run_config.json")
        config_path.parent.mkdir(parents=True, exist_ok=True)

        with open(config_path, "w") as f:
            import json

            json.dump(config_dict, f, indent=2)

        print(f"Configuration written to {config_path}")
        print(f"Run directory: output/{run_name}/")
        print(f"Database: {sqlite_db}")
        return 0

    # Initialize logging
    setup_logging(level=args.log_level, log_file=log_file, append=False)

    logger = logging.getLogger(__name__)
    limit_info = f" limit={args.limit}" if args.limit else ""
    logger.info(
        f"COLLECTION PIPELINE STARTED - run={run_name} storage={args.storage} rate_limit={args.rate_limit}{limit_info}"
    )
    logger.info(f"Command: {' '.join(sys.argv)}")

    # Build storage configuration
    storage_config = None
    if args.storage:
        final_storage_dict: dict[str, str] = {}

        # Add bucket names if provided
        if args.bucket_raw:
            final_storage_dict["bucket_raw"] = args.bucket_raw
        if args.bucket_meta:
            final_storage_dict["bucket_meta"] = args.bucket_meta
        if args.bucket_full:
            final_storage_dict["bucket_full"] = args.bucket_full

        # Add additional storage config
        if args.storage_config:
            for item in args.storage_config:
                if "=" in item:
                    key, value = item.split("=", 1)
                    final_storage_dict[key] = value

        # Determine storage protocol for operational logic
        storage_protocol = get_storage_protocol(args.storage)
        storage_config = {
            "type": args.storage,
            "protocol": storage_protocol,
            "config": final_storage_dict,
            "prefix": "",
        }

        # Set up storage with auto-configuration and connectivity checks
        await setup_storage_with_checks(args.storage, final_storage_dict)

        # Create all required buckets/directories early to fail fast
        await create_storage_buckets_or_directories(args.storage, final_storage_dict)

    try:
        # Create book storage for process summary uploads
        from grin_to_s3.process_summary import create_book_manager_for_uploads

        book_storage = await create_book_manager_for_uploads(run_name)

        # Create or load process summary
        run_summary = await create_process_summary(run_name, "collect", book_storage)
        collect_stage = get_current_stage(run_summary, "collect")
        collect_stage.set_command_arg("library_directory", args.library_directory)
        collect_stage.set_command_arg("storage_type", args.storage)
        collect_stage.set_command_arg("test_mode", args.test_mode)
        if args.limit:
            collect_stage.set_command_arg("limit", args.limit)

        try:
            # Load configuration with CLI overrides
            config = ConfigManager.load_config(
                config_file=args.config_file,
                library_directory=args.library_directory,
                rate_limit=args.rate_limit,
                resume_file=progress_file,  # Use generated progress file
                pagination_page_size=args.page_size,
                pagination_max_pages=args.max_pages,
                pagination_start_page=args.start_page,
                sqlite_db_path=sqlite_db,  # Use generated SQLite path
            )


            # Build sync configuration from CLI arguments
            sync_config = {
                "concurrent_downloads": args.sync_concurrent_downloads,
                "concurrent_uploads": args.sync_concurrent_uploads,
                "batch_size": args.sync_batch_size,
                "staging_dir": args.sync_staging_dir,
                "disk_space_threshold": args.sync_disk_space_threshold,
                "enrichment_workers": args.sync_enrichment_workers,
                "gpg_key_file": args.sync_gpg_key_file,
            }

            # Write run configuration to run directory
            config_dict = config.to_dict()
            config_dict.update(
                {
                    "run_name": run_name,
                    "run_identifier": run_identifier,
                    "output_directory": f"output/{run_name}",
                    "sqlite_db_path": sqlite_db,
                    "progress_file": progress_file,
                    "log_file": log_file,
                    "storage_config": storage_config,
                    "sync_config": sync_config,
                    "secrets_dir": args.secrets_dir,
                    "limit": args.limit,
                }
            )

            config_path = Path(f"output/{run_name}/run_config.json")
            config_path.parent.mkdir(parents=True, exist_ok=True)

            with open(config_path, "w") as f:
                import json

                json.dump(config_dict, f, indent=2)

            logger.info(f"Configuration written to {config_path}")
            collect_stage.add_progress_update("Configuration written, starting collection")

            # Create book collector with configuration
            collector = BookCollector(
                directory=args.library_directory,
                process_summary_stage=collect_stage,
                storage_config=storage_config,
                test_mode=args.test_mode,
                config=config,
                secrets_dir=args.secrets_dir,
            )

            # Run book collection with pagination
            collect_stage.add_progress_update("Starting book collection")
            completed = await collector.collect_books(output_file, args.limit)

            # Track completion status
            if completed:
                collect_stage.add_progress_update("Collection completed successfully")
            else:
                collect_stage.add_progress_update("Collection incomplete - interrupted or limited")

            # Only show resume command if collection was not completed
            if not completed:
                print_resume_command(args, run_name)

            return 0

        except Exception as e:
            # Record error in summary
            error_type = type(e).__name__
            collect_stage.add_error(error_type, str(e))

            if isinstance(e, KeyboardInterrupt):
                collect_stage.add_progress_update("Collection interrupted by user")
            else:
                collect_stage.add_progress_update(f"Collection failed: {error_type}")
            raise

        finally:
            # Always end the stage and save summary
            run_summary.end_stage("collect")
            await save_process_summary(run_summary, book_storage)

        return 0

    except Exception as e:
        if isinstance(e, KeyboardInterrupt):
            print("\n🚫 Collection interrupted by user")
        else:
            print(f"❌ Collection failed: {e}")

        # Show resume command for any failure/interruption
        print_resume_command(args, run_name)
        return 1


if __name__ == "__main__":
    exit(asyncio.run(main()))
