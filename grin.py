#!/usr/bin/env python3
"""
GRIN-to-S3 Command Line Interface

A unified tool for archiving Google Books data from GRIN to S3-compatible storage.

Commands:
  auth           Set up OAuth2 authentication with GRIN
  collect        Collect library book metadata from GRIN with progress tracking
  process        Request book processing from GRIN
  sync           Sync converted books from GRIN to storage
  storage        Manage storage buckets (ls, rm)
  extract        Extract OCR text from decrypted book archives
  enrich         Enrich books with GRIN metadata
  export         Export ALL books in collection to CSV with available metadata
  reports        View process summaries and reports
"""

import argparse
import asyncio
import sys
from pathlib import Path

# Add src directory to Python path before any local imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Import all subcommand modules at startup to catch import errors early
from grin_to_s3.auth import main as auth_main
from grin_to_s3.collect_books.__main__ import main as collect_main
from grin_to_s3.common import SessionLock
from grin_to_s3.export import main as export_main
from grin_to_s3.extract import main as extract_main
from grin_to_s3.metadata.grin_enrichment import enrich_main
from grin_to_s3.processing import main as process_main
from grin_to_s3.reports import main as reports_main
from grin_to_s3.storage import main as storage_main
from grin_to_s3.sync import main as sync_main


def create_parser():
    """Create the main argument parser with subcommands."""
    parser = argparse.ArgumentParser(
        prog="grin",
        description="GRIN-to-S3: Archive Google Books data from GRIN to S3-compatible storage",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Set up OAuth2 authentication
  python grin.py auth setup

  # Collect books metadata
  python grin.py collect --run-name harvard_2024 --storage r2 \\
                         --bucket-raw grin-raw --bucket-meta grin-meta --bucket-full grin-full

  # Request processing for collected books
  python grin.py process request --run-name harvard_2024 --limit 1000


  # Sync converted books to storage
  python grin.py sync pipeline --run-name harvard_2024 --queue converted




  # Enrich with detailed metadata
  python grin.py enrich --run-name harvard_2024

  # Export ALL books to CSV (works at any pipeline stage)
  python grin.py export --run-name harvard_2024 --output books.csv

  # View process summary (human-readable)
  python grin.py reports view --run-name harvard_2024

  # View raw JSON (can be piped to file)
  python grin.py reports view --run-name harvard_2024 --raw > summary.json

For more help on each command, use: python grin.py <command> --help
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Add subcommand parsers (will be populated by individual modules)
    auth_parser = subparsers.add_parser("auth", help="Set up OAuth2 authentication")
    collect_parser = subparsers.add_parser("collect", help="Collect book metadata from GRIN")
    process_parser = subparsers.add_parser("process", help="Request book processing")
    sync_parser = subparsers.add_parser("sync", help="Sync converted books from GRIN to storage")
    storage_parser = subparsers.add_parser("storage", help="Manage storage buckets and data (ls, rm, cp)")
    extract_parser = subparsers.add_parser("extract", help="Extract OCR text from decrypted book archives")
    enrich_parser = subparsers.add_parser("enrich", help="Enrich books with GRIN metadata")
    export_parser = subparsers.add_parser("export", help="Export ALL books in collection to CSV")
    reports_parser = subparsers.add_parser("reports", help="View process summaries and reports")

    return parser, {
        "auth": auth_parser,
        "collect": collect_parser,
        "process": process_parser,
        "sync": sync_parser,
        "storage": storage_parser,
        "extract": extract_parser,
        "enrich": enrich_parser,
        "export": export_parser,
        "reports": reports_parser,
    }


def get_run_name_from_args() -> str | None:
    """Extract run name from command line arguments."""
    for i, arg in enumerate(sys.argv):
        if arg == "--run-name" and i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return None


def needs_database_lock(command: str) -> bool:
    """Determine if a command needs database locking."""
    # Skip lock for dry-run operations since they don't modify the database
    if "--dry-run" in sys.argv:
        return False
    return command in ["collect", "process", "sync", "enrich"]


async def main():
    """Main entry point for the grin CLI."""
    parser, _ = create_parser()

    # Parse args to get the command
    if len(sys.argv) < 2:
        parser.print_help()
        return 1

    command = sys.argv[1]

    # Apply session lock for database-modifying commands
    session_lock = None
    if needs_database_lock(command):
        run_name = get_run_name_from_args()
        if run_name:
            lock_path = Path(f"output/{run_name}/session.lock")
            session_lock = SessionLock(lock_path)
            if not session_lock.acquire():
                print(f"Another session is already running for run-name '{run_name}'")
                print("Wait for the other session to complete or stop it before starting a new one.")
                return 1

    try:
        # Route to appropriate module based on command
        if command == "auth":
            # Remove 'auth' from args and pass the rest
            sys.argv = [sys.argv[0]] + sys.argv[2:]
            auth_main()
            return 0

        elif command == "collect":
            # Remove 'collect' from args and pass the rest
            sys.argv = [sys.argv[0]] + sys.argv[2:]
            return await collect_main()

        elif command == "process":
            # Remove 'process' from args and pass the rest
            sys.argv = [sys.argv[0]] + sys.argv[2:]
            return await process_main()

        elif command == "sync":
            # Remove 'sync' from args and pass the rest
            sys.argv = [sys.argv[0]] + sys.argv[2:]
            return await sync_main()

        elif command == "storage":
            # Remove 'storage' from args and pass the rest
            sys.argv = [sys.argv[0]] + sys.argv[2:]
            return await storage_main()

        elif command == "extract":
            # Remove 'extract' from args and pass the rest
            sys.argv = [sys.argv[0]] + sys.argv[2:]
            return await extract_main()

        elif command == "enrich":
            # Remove 'enrich' from args and pass the rest
            sys.argv = [sys.argv[0]] + sys.argv[2:]
            return await enrich_main()

        elif command == "export":
            # Remove 'export' from args and pass the rest
            sys.argv = [sys.argv[0]] + sys.argv[2:]
            return await export_main()

        elif command == "reports":
            # Remove 'reports' from args and pass the rest
            sys.argv = [sys.argv[0]] + sys.argv[2:]
            return await reports_main()

        elif command in ["-h", "--help"]:
            parser.print_help()
            return 0

        else:
            print(f"Unknown command: {command}")
            parser.print_help()
            return 1

    finally:
        # Release session lock if acquired
        if session_lock:
            session_lock.release()


if __name__ == "__main__":
    exit(asyncio.run(main()))
