#!/usr/bin/env python3
"""
Logs and Process Summary Management CLI

Commands for viewing and managing process summaries and logs.
"""

import argparse
import asyncio
import json
import sys

from grin_to_s3.process_summary import RunSummaryManager, create_book_storage_for_uploads


def create_parser() -> argparse.ArgumentParser:
    """Create argument parser for logs commands."""
    parser = argparse.ArgumentParser(
        prog="grin logs",
        description="View and manage process summaries and logs",
    )

    subparsers = parser.add_subparsers(dest="subcommand", help="Available log operations")

    # Summary subcommand
    summary_parser = subparsers.add_parser("summary", help="View or download process summaries")
    summary_parser.add_argument("--run-name", required=True, help="Name of the run")
    summary_parser.add_argument("--download", help="Download summary to local file", metavar="PATH")
    summary_parser.add_argument("--pretty", action="store_true", help="Pretty print the summary")

    return parser


async def handle_summary(args) -> int:
    """Handle summary subcommand."""
    try:
        if args.download:
            # Download summary from storage
            book_storage = await create_book_storage_for_uploads(args.run_name)
            if not book_storage:
                print(f"No storage configuration found for run '{args.run_name}'")
                return 1
            
            # Generate storage path
            filename = f"process_summary_{args.run_name}.json"
            storage_path = book_storage._meta_path(filename)
            
            # Download from storage
            try:
                summary_data = await book_storage.storage.read_bytes(storage_path)
                
                # Save to output file
                with open(args.download, "wb") as f:
                    f.write(summary_data)
                print(f"Downloaded process summary to: {args.download}")
                return 0
                
            except Exception as e:
                print(f"Error downloading process summary: {e}")
                return 1
        else:
            # View local summary
            manager = RunSummaryManager(args.run_name)
            
            if not await manager._summary_file_exists():
                print(f"No local process summary found for run '{args.run_name}'")
                print(f"Expected location: {manager.summary_file}")
                return 1
            
            try:
                with open(manager.summary_file) as f:
                    summary = json.load(f)
                
                if args.pretty:
                    print(json.dumps(summary, indent=2))
                else:
                    # Show key metrics in a readable format
                    print(f"Run: {summary['run_name']}")
                    print(f"Status: {'Completed' if summary['is_completed'] else 'In Progress'}")
                    print(f"Total Items Processed: {summary['total_items_processed']}")
                    print(f"Success Rate: {summary['overall_success_rate_percent']:.1f}%")
                    print(f"Total Duration: {summary.get('total_duration_seconds', 'N/A')} seconds")
                    print(f"Errors: {summary['total_error_count']}")

                    if summary["stages"]:
                        print("\nStages:")
                        for stage_name, stage_data in summary["stages"].items():
                            status = "✓" if stage_data["is_completed"] else "⚠"
                            print(f"  {status} {stage_name}: {stage_data['items_processed']} items")

                return 0
                
            except Exception as e:
                print(f"Error reading process summary: {e}")
                return 1

    except Exception as e:
        print(f"Error accessing process summary: {e}")
        return 1


async def main() -> int:
    """Main entry point for logs commands."""
    parser = create_parser()
    args = parser.parse_args()

    if not args.subcommand:
        parser.print_help()
        return 1

    if args.subcommand == "summary":
        return await handle_summary(args)
    else:
        print(f"Unknown subcommand: {args.subcommand}")
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
