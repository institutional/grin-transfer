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

from .config import ConfigManager
from .exporter import BookCollector

sys.path.append('..')
from grin_to_s3.common import setup_storage_with_checks

# Check Python version requirement

# Force unbuffered output for immediate logging
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(line_buffering=True)  # type: ignore[union-attr]
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(line_buffering=True)  # type: ignore[union-attr]

# Set up module logger
logger = logging.getLogger(__name__)


def setup_logging(level: str = "INFO", log_file: str | None = None) -> None:
    """
    Configure logging for book collection operations.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_file: Optional log file path (defaults to timestamped file in logs/)
    """
    from datetime import datetime
    from pathlib import Path

    # Create root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))

    # Clear any existing handlers
    root_logger.handlers.clear()

    # Suppress debug logging from dependency modules
    if level.upper() == "DEBUG":
        # Set dependency modules to INFO level to reduce noise
        logging.getLogger("aiosqlite").setLevel(logging.INFO)
        logging.getLogger("urllib3").setLevel(logging.INFO)
        logging.getLogger("requests").setLevel(logging.INFO)
        logging.getLogger("google").setLevel(logging.INFO)
        logging.getLogger("google.auth").setLevel(logging.INFO)
        logging.getLogger("google.oauth2").setLevel(logging.INFO)
        logging.getLogger("asyncio").setLevel(logging.INFO)

    # Create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    # Console handler (always present) with immediate flushing
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Force immediate flushing for all handlers
    for handler in root_logger.handlers:

        def make_flush_func(h):
            return lambda: h.stream.flush() if hasattr(h, "stream") else None

        # Replace flush method - type ignore for dynamic assignment
        handler.flush = make_flush_func(handler)  # type: ignore[method-assign]

    # File handler (auto-generate timestamped filename if not provided)
    if log_file is None:
        # Create logs directory
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)

        # Generate timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = str(logs_dir / f"collect_books_{timestamp}.log")
    else:
        # Ensure parent directory exists for custom log file
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(str(log_file))
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    logger.info(f"Logging to file: {log_file}")

    logger.info(f"Logging initialized at {level} level")


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
    cmd_parts = ["python collect_books.py"]

    # Add run name (most important for resume)
    cmd_parts.append(f'--run-name "{run_name}"')

    # Add other important arguments that should be preserved
    if args.limit:
        cmd_parts.append(f"--limit {args.limit}")

    # Directory is now always "Harvard" (default)

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
    print(f"python grin_enrichment.py enrich --run-name {run_name}")
    print("\nTo check status:")
    print(f"python grin_enrichment.py status --run-name {run_name}")
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

  # Local storage (no buckets required)
  python grin.py collect --storage local --run-name "local_test"

  # Resume interrupted collection (uses run-specific progress files and saved config)
  python grin.py collect --run-name "harvard_fall_2024" --storage r2 \
                         --bucket-raw grin-raw --bucket-meta grin-meta \
                         --bucket-full grin-full
        """,
    )

    parser.add_argument("output_file", nargs="?", help="Output CSV file path (optional if using --run-name)")

    # Run identification
    parser.add_argument(
        "--run-name",
        help="Name for this collection run (defaults to timestamp). Used for all output files."
    )

    # Export options
    parser.add_argument("--limit", type=int, help="Limit number of books to process (for testing)")
    parser.add_argument(
        "--rate-limit", type=float, default=5.0, help="API requests per second (default: 5.0)"
    )
    parser.add_argument(
        "--test-mode", action="store_true", help="Use mock data for testing (no network calls)"
    )

    # Logging options
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )
    parser.add_argument(
        "--log-file",
        help="Log file path (default: auto-generated timestamped file in logs/ directory)"
    )

    # Storage options
    parser.add_argument(
        "--storage",
        choices=["local", "minio", "r2", "s3"],
        required=True,
        help="Storage backend for run configuration"
    )
    parser.add_argument(
        "--bucket-raw",
        help="Raw data bucket (for sync archives, required unless storage=local)"
    )
    parser.add_argument(
        "--bucket-meta",
        help="Metadata bucket (for CSV/database outputs, required unless storage=local)"
    )
    parser.add_argument(
        "--bucket-full",
        help="Full-text bucket (for OCR outputs, required unless storage=local)"
    )
    parser.add_argument(
        "--storage-config", action="append", help="Additional storage config key=value"
    )

    # Resume/progress options
    parser.add_argument(
        "--resume-file",
        default="output/default/progress.json",
        help="Progress tracking file for resume capability"
    )

    # Configuration options
    parser.add_argument(
        "--config-file", type=str, help="Configuration file path (JSON format)"
    )
    parser.add_argument(
        "--create-config",
        type=str,
        help="Create default config file at specified path and exit"
    )
    parser.add_argument(
        "--write-config",
        action="store_true",
        help="Write configuration to run directory and exit (requires storage options)"
    )
    parser.add_argument(
        "--secrets-dir",
        type=str,
        help="Directory containing GRIN secrets files (searches home directory if not specified)",
    )

    # Data source options (removed - only HTML mode supported)

    # Pagination options
    parser.add_argument(
        "--page-size", type=int, help="Records per page for API requests (default: 10000)"
    )
    parser.add_argument(
        "--max-pages", type=int, help="Maximum pages to fetch (default: 1000)"
    )
    parser.add_argument(
        "--start-page", type=int, help="Starting page number (default: 1)"
    )

    # Performance options
    parser.add_argument(
        "--disable-prefetch",
        action="store_true",
        help="Disable HTTP prefetching for next page"
    )

    args = parser.parse_args()

    # Validate storage arguments
    if args.storage != "local":
        missing_buckets = []
        if not args.bucket_raw:
            missing_buckets.append("--bucket-raw")
        if not args.bucket_meta:
            missing_buckets.append("--bucket-meta")
        if not args.bucket_full:
            missing_buckets.append("--bucket-full")

        if missing_buckets:
            parser.error(
                f"The following bucket parameters are required when using "
                f"--storage={args.storage}: {', '.join(missing_buckets)}"
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
        run_name = "".join(
            c for c in args.run_name if c.isalnum() or c in ("-", "_")
        ).strip()
        if not run_name:
            parser.error("Run name must contain valid filename characters")
    else:
        # Generate timestamp-based run name
        run_name = datetime.now().strftime("run_%Y%m%d_%H%M%S")

    # Extract the actual identifier from run_name (remove "run_" prefix if present)
    run_identifier = (
        run_name.removeprefix("run_") if run_name.startswith("run_") else run_name
    )

    # Generate timestamp for output files (not resume files)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Determine output file path
    if args.output_file:
        output_file = args.output_file
    else:
        output_file = f"output/{run_name}/books_{timestamp}.csv"

    # Generate file paths - resume files stay consistent, outputs get timestamped
    log_file = args.log_file or f"logs/collect_books_{run_identifier}_{timestamp}.log"
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

            storage_config = {
                "type": args.storage,
                "config": storage_dict,
                "prefix": "grin-books"
            }

        # Load configuration with CLI overrides
        config = ConfigManager.load_config(
            config_file=args.config_file,
            directory="Harvard",
            rate_limit=args.rate_limit,
            resume_file=progress_file,
            pagination_page_size=args.page_size,
            pagination_max_pages=args.max_pages,
            pagination_start_page=args.start_page,
            sqlite_db_path=sqlite_db,
        )

        # Apply performance overrides
        if args.disable_prefetch:
            config.enable_prefetch = False

        # Create enhanced config dict with storage and runtime info
        config_dict = config.to_dict()
        config_dict.update({
            "run_name": run_name,
            "run_identifier": run_identifier,
            "output_directory": f"output/{run_name}",
            "sqlite_db_path": sqlite_db,
            "progress_file": progress_file,
            "storage_config": storage_config,
            "secrets_dir": args.secrets_dir,
        })

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
    setup_logging(level=args.log_level, log_file=log_file)

    logger = logging.getLogger(__name__)
    logger.info("Book Collection Pipeline started")
    logger.info(f"Run name: {run_name}")
    logger.info(f"Output file: {output_file}")
    logger.info(f"Progress file: {progress_file}")
    logger.info(f"SQLite database: {sqlite_db}")

    # Log full command for debugging
    import sys

    logger.info(f"Full command: {' '.join(sys.argv)}")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Working directory: {os.getcwd()}")

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

        storage_config = {
            "type": args.storage,
            "config": final_storage_dict,
            "prefix": "grin-books"
        }

        # Set up storage with auto-configuration and connectivity checks
        await setup_storage_with_checks(args.storage, final_storage_dict)

    try:
        # Load configuration with CLI overrides
        config = ConfigManager.load_config(
            config_file=args.config_file,
            directory="Harvard",
            rate_limit=args.rate_limit,
            resume_file=progress_file,  # Use generated progress file
            pagination_page_size=args.page_size,
            pagination_max_pages=args.max_pages,
            pagination_start_page=args.start_page,
            sqlite_db_path=sqlite_db,  # Use generated SQLite path
        )

        # Apply performance overrides
        if args.disable_prefetch:
            config.enable_prefetch = False

        # Write run configuration to run directory
        config_dict = config.to_dict()
        config_dict.update({
            "run_name": run_name,
            "run_identifier": run_identifier,
            "output_directory": f"output/{run_name}",
            "sqlite_db_path": sqlite_db,
            "progress_file": progress_file,
            "storage_config": storage_config,
            "secrets_dir": args.secrets_dir,
        })

        config_path = Path(f"output/{run_name}/run_config.json")
        config_path.parent.mkdir(parents=True, exist_ok=True)

        with open(config_path, "w") as f:
            import json
            json.dump(config_dict, f, indent=2)

        logger.info(f"Configuration written to {config_path}")

        # Create book collector with configuration
        collector = BookCollector(
            storage_config=storage_config,
            test_mode=args.test_mode,
            config=config,
            secrets_dir=args.secrets_dir
        )

        # Run book collection with pagination
        await collector.collect_books(output_file, args.limit)

        # Always show resume command at the end
        print_resume_command(args, run_name)
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
