#!/usr/bin/env python3
"""
Logs and Process Summary Management CLI

Commands for viewing and managing process summaries and logs.
"""

import argparse
import asyncio
import json
import sys
from datetime import UTC, datetime

from grin_to_s3.process_summary import RunSummaryManager


def format_duration(seconds: float | None) -> str:
    """Format duration in a human-readable way."""
    if seconds is None:
        return "N/A"

    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"


def calculate_effective_duration(summary: dict) -> float | None:
    """Calculate effective duration for display, showing elapsed time since run start."""
    # Calculate elapsed time from run start to now
    run_start_time = summary.get("run_start_time")
    if run_start_time:
        try:
            # Handle both string (ISO format) and float (perf_counter) timestamps
            if isinstance(run_start_time, str):
                start_dt = datetime.fromisoformat(run_start_time.replace("Z", "+00:00"))
                now_dt = datetime.now(UTC)
                return (now_dt - start_dt).total_seconds()
            else:
                # Skip float values - they're from perf_counter and not useful for elapsed time
                pass
        except (ValueError, TypeError):
            pass

    # Fallback: sum completed stage durations
    total_stage_duration = 0
    for stage_data in summary.get("stages", {}).values():
        stage_duration = stage_data.get("duration_seconds")
        if stage_duration is not None:
            total_stage_duration += stage_duration

    return total_stage_duration if total_stage_duration > 0 else None


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
                effective_duration = calculate_effective_duration(summary)

                # Build a more meaningful status description
                completed_stages = summary.get("completed_stages", [])
                active_stages = summary.get("active_stages", [])

                if active_stages:
                    status = f"Active: {', '.join(active_stages)}"
                elif completed_stages:
                    status = f"Last stage: {completed_stages[-1]}"
                else:
                    status = "No stages run yet"

                print(f"Run: {summary['run_name']}")
                print(f"Status: {status}")
                print(f"Total Items Processed: {summary['total_items_processed']}")
                print(f"Success Rate: {summary['overall_success_rate_percent']:.1f}%")
                print(f"Total Duration: {format_duration(effective_duration)}")
                print(f"Errors: {summary['total_error_count']}")

                if summary["stages"]:
                    print("\nStages:")
                    for stage_name, stage_data in summary["stages"].items():
                        status = "✓" if stage_data["is_completed"] else "⚠"

                        # Calculate stage totals based on stage-specific fields
                        if stage_name == "collect":
                            total = stage_data.get("books_collected", 0) + stage_data.get("collection_failed", 0)
                        elif stage_name == "process":
                            total = stage_data.get("conversion_requests_made", 0) + stage_data.get(
                                "conversion_requests_failed", 0
                            )
                        elif stage_name == "sync":
                            total = (
                                stage_data.get("books_synced", 0)
                                + stage_data.get("sync_skipped", 0)
                                + stage_data.get("sync_failed", 0)
                                + stage_data.get("conversions_requested_during_sync", 0)
                            )
                        elif stage_name == "enrich":
                            total = (
                                stage_data.get("books_enriched", 0)
                                + stage_data.get("enrichment_skipped", 0)
                                + stage_data.get("enrichment_failed", 0)
                            )
                        else:
                            total = 0

                        print(f"  {status} {stage_name}: {total} items")

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
