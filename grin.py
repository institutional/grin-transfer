#!/usr/bin/env python3
"""
GRIN-to-S3 Command Line Interface

A unified tool for archiving Google Books data from GRIN to S3-compatible storage.

Commands:
  auth           Set up OAuth2 authentication with GRIN
  collect        Collect library book metadata from GRIN with progress tracking
  process        Request book processing and monitor conversion status
  sync           Sync converted books from GRIN to storage (pipeline, status)
  storage        Manage storage buckets (ls, rm)
  extract        Extract OCR text from decrypted book archives
  enrich         Enrich books with GRIN metadata
  export         Export ALL books in collection to CSV with available metadata
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
  python grin.py sync pipeline --run-name harvard_2024

  # Check sync status
  python grin.py sync status --run-name harvard_2024


  # Extract OCR text from decrypted archives
  python grin.py extract /path/to/book.tar.gz --output book.json

  # Enrich with detailed metadata
  python grin.py enrich --run-name harvard_2024

  # Show enrichment status
  python grin.py status --run-name harvard_2024

  # Export ALL books to CSV (works at any pipeline stage)
  python grin.py export --run-name harvard_2024 --output books.csv

For more help on each command, use: python grin.py <command> --help
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Add subcommand parsers (will be populated by individual modules)
    auth_parser = subparsers.add_parser("auth", help="Set up OAuth2 authentication")
    collect_parser = subparsers.add_parser("collect", help="Collect book metadata from GRIN")
    process_parser = subparsers.add_parser("process", help="Request and monitor book processing")
    sync_parser = subparsers.add_parser("sync", help="Sync converted books from GRIN to storage")
    storage_parser = subparsers.add_parser("storage", help="Manage storage buckets and data (ls, rm)")
    extract_parser = subparsers.add_parser("extract", help="Extract OCR text from decrypted book archives")
    enrich_parser = subparsers.add_parser("enrich", help="Enrich books with GRIN metadata")
    export_parser = subparsers.add_parser("export", help="Export ALL books in collection to CSV")
    status_parser = subparsers.add_parser("status", help="Show enrichment status")

    return parser, {
        "auth": auth_parser,
        "collect": collect_parser,
        "process": process_parser,
        "sync": sync_parser,
        "storage": storage_parser,
        "extract": extract_parser,
        "enrich": enrich_parser,
        "export": export_parser,
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

    elif command == "sync":
        from grin_to_s3.sync import main as sync_main

        # Remove 'sync' from args and pass the rest
        sys.argv = [sys.argv[0]] + sys.argv[2:]
        return await sync_main()

    elif command == "storage":
        from grin_to_s3.storage import main as storage_main

        # Remove 'storage' from args and pass the rest
        sys.argv = [sys.argv[0]] + sys.argv[2:]
        return await storage_main()

    elif command == "extract":
        from grin_to_s3.extract import main as extract_main

        # Remove 'extract' from args and pass the rest
        sys.argv = [sys.argv[0]] + sys.argv[2:]
        return await extract_main()

    elif command == "enrich":
        from grin_to_s3.grin_enrichment import enrich_main

        # Remove 'enrich' from args and pass the rest
        sys.argv = [sys.argv[0]] + sys.argv[2:]
        return await enrich_main()

    elif command == "export":
        from grin_to_s3.export import main as export_main

        # Remove 'export' from args and pass the rest
        sys.argv = [sys.argv[0]] + sys.argv[2:]
        return await export_main()

    # TODO make this an overall status command that can be used for all steps
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
