#!/usr/bin/env python3
"""
Logs and Process Summary Management CLI

Commands for viewing and managing process summaries and logs.
"""

import argparse
import asyncio
import json
import sys

from grin_to_s3.process_summary import RunSummaryManager


def create_parser() -> argparse.ArgumentParser:
    """Create argument parser for logs commands."""
    parser = argparse.ArgumentParser(
        prog="grin logs",
        description="View and manage process summaries and logs",
    )

    subparsers = parser.add_subparsers(dest="subcommand", help="Available log operations")

    # View subcommand
    view_parser = subparsers.add_parser("view", help="View process summaries")
    view_parser.add_argument("--run-name", required=True, help="Name of the run")
    view_parser.add_argument("--raw", action="store_true", help="Output raw JSON (can be piped to file)")

    return parser


async def handle_view(args) -> int:
    """Handle view subcommand."""
    try:
        # View local summary
        manager = RunSummaryManager(args.run_name)

        if not await manager._summary_file_exists():
            print(f"No local process summary found for run '{args.run_name}'")
            print(f"Expected location: {manager.summary_file}")
            return 1

        try:
            with open(manager.summary_file) as f:
                summary = json.load(f)

            if args.raw:
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
    args = create_parser().parse_args()

    if not args.subcommand:
        create_parser().print_help()
        return 1

    if args.subcommand == "view":
        return await handle_view(args)
    else:
        print(f"Unknown subcommand: {args.subcommand}")
        create_parser().print_help()
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
