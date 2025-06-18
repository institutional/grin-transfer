#!/usr/bin/env python3
"""
GRIN-to-S3 Command Line Interface

A unified tool for archiving Google Books data from GRIN to S3-compatible storage.

Commands:
  auth           Set up OAuth2 authentication with GRIN
  collect        Collect library book metadata from GRIN with progress tracking
  process        Request book processing and monitor conversion status
  sync-pipeline  Sync converted books from GRIN to storage
  sync-status    Check sync status
  sync-catchup   Download already-converted books from GRIN
  enrich         Enrich books with GRIN metadata
  export-csv     Export enriched data to CSV
  status         Show enrichment status
"""

import argparse
import asyncio
import sys
from pathlib import Path

# Add src directory to Python path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))


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

  # Monitor processing status
  python grin.py process monitor --run-name harvard_2024

  # Sync converted books to storage
  python grin.py sync-pipeline --run-name harvard_2024

  # Check sync status
  python grin.py sync-status --run-name harvard_2024

  # Catchup sync for already-converted books
  python grin.py sync-catchup --run-name harvard_2024

  # Enrich with detailed metadata
  python grin.py enrich --run-name harvard_2024

  # Show enrichment status
  python grin.py status --run-name harvard_2024

  # Export to CSV
  python grin.py export-csv --run-name harvard_2024 --output books.csv

For more help on each command, use: python grin.py <command> --help
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Add subcommand parsers (will be populated by individual modules)
    auth_parser = subparsers.add_parser("auth", help="Set up OAuth2 authentication")
    collect_parser = subparsers.add_parser("collect", help="Collect book metadata from GRIN")
    process_parser = subparsers.add_parser("process", help="Request and monitor book processing")
    sync_pipeline_parser = subparsers.add_parser("sync-pipeline", help="Sync converted books to storage")
    sync_status_parser = subparsers.add_parser("sync-status", help="Check sync status")
    sync_catchup_parser = subparsers.add_parser("sync-catchup", help="Download already-converted books")
    enrich_parser = subparsers.add_parser("enrich", help="Enrich books with GRIN metadata")
    export_parser = subparsers.add_parser("export-csv", help="Export enriched data to CSV")
    status_parser = subparsers.add_parser("status", help="Show enrichment status")

    return parser, {
        "auth": auth_parser,
        "collect": collect_parser,
        "process": process_parser,
        "sync-pipeline": sync_pipeline_parser,
        "sync-status": sync_status_parser,
        "sync-catchup": sync_catchup_parser,
        "enrich": enrich_parser,
        "export-csv": export_parser,
        "status": status_parser,
    }


async def main():
    """Main entry point for the grin CLI."""
    parser, subparsers = create_parser()

    # Parse args to get the command
    if len(sys.argv) < 2:
        parser.print_help()
        return 1

    command = sys.argv[1]

    # Route to appropriate module based on command
    if command == "auth":
        from grin_to_s3.auth import main as auth_main
        # Remove 'auth' from args and pass the rest
        sys.argv = [sys.argv[0]] + sys.argv[2:]
        auth_main()
        return 0

    elif command == "collect":
        from grin_to_s3.collect_books.__main__ import main as collect_main
        # Remove 'collect' from args and pass the rest
        sys.argv = [sys.argv[0]] + sys.argv[2:]
        return await collect_main()

    elif command == "process":
        from grin_to_s3.processing import main as process_main
        # Remove 'process' from args and pass the rest
        sys.argv = [sys.argv[0]] + sys.argv[2:]
        return await process_main()

    elif command == "sync-pipeline":
        from grin_to_s3.sync import sync_pipeline_main
        # Remove 'sync-pipeline' from args and pass the rest
        sys.argv = [sys.argv[0]] + sys.argv[2:]
        return await sync_pipeline_main()

    elif command == "sync-status":
        from grin_to_s3.sync import sync_status_main
        # Remove 'sync-status' from args and pass the rest
        sys.argv = [sys.argv[0]] + sys.argv[2:]
        return await sync_status_main()

    elif command == "sync-catchup":
        from grin_to_s3.sync import sync_catchup_main
        # Remove 'sync-catchup' from args and pass the rest
        sys.argv = [sys.argv[0]] + sys.argv[2:]
        return await sync_catchup_main()

    elif command == "enrich":
        from grin_to_s3.grin_enrichment import enrich_main
        # Remove 'enrich' from args and pass the rest
        sys.argv = [sys.argv[0]] + sys.argv[2:]
        return await enrich_main()

    elif command == "export-csv":
        from grin_to_s3.grin_enrichment import export_csv_main
        # Remove 'export-csv' from args and pass the rest
        sys.argv = [sys.argv[0]] + sys.argv[2:]
        return await export_csv_main()

    elif command == "status":
        from grin_to_s3.grin_enrichment import status_main
        # Remove 'status' from args and pass the rest
        sys.argv = [sys.argv[0]] + sys.argv[2:]
        return await status_main()

    elif command in ["-h", "--help"]:
        parser.print_help()
        return 0

    else:
        print(f"Unknown command: {command}")
        parser.print_help()
        return 1


if __name__ == "__main__":
    exit(asyncio.run(main()))
