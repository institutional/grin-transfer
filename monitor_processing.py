#!/usr/bin/env python3
"""
GRIN Processing Monitor

Monitor the status of book processing requests and show which books are complete.
"""

import argparse
import asyncio
import sys
from datetime import datetime
from pathlib import Path

from client import GRINClient


class ProcessingMonitor:
    """Monitor for GRIN book processing status."""

    def __init__(self, directory: str = "Harvard", secrets_dir: str | None = None):
        self.directory = directory
        self.grin_client = GRINClient(secrets_dir=secrets_dir)

    async def cleanup(self) -> None:
        """Clean up resources and close connections safely."""
        try:
            if hasattr(self.grin_client, "session") and self.grin_client.session:
                await self.grin_client.session.close()
        except Exception as e:
            print(f"Warning: Error closing GRIN client session: {e}")

    async def get_converted_books(self) -> list[str]:
        """Get list of books that have been converted (ready for download)."""
        try:
            response_text = await self.grin_client.fetch_resource(self.directory, "_converted?format=text")
            lines = response_text.strip().split("\n")
            converted_barcodes = []
            for line in lines:
                if line.strip() and ".tar.gz.gpg" in line:
                    barcode = line.strip().replace(".tar.gz.gpg", "")
                    converted_barcodes.append(barcode)
            return converted_barcodes
        except Exception as e:
            print(f"Error getting converted books: {e}")
            return []

    async def get_in_process_books(self) -> list[str]:
        """Get list of books currently in processing queue."""
        try:
            response_text = await self.grin_client.fetch_resource(self.directory, "_in_process?format=text")
            lines = response_text.strip().split("\n")
            return [line.strip() for line in lines if line.strip()]
        except Exception as e:
            print(f"Error getting in-process books: {e}")
            return []

    async def get_failed_books(self) -> list[str]:
        """Get list of books that failed processing."""
        try:
            response_text = await self.grin_client.fetch_resource(self.directory, "_failed?format=text")
            lines = response_text.strip().split("\n")
            return [line.strip() for line in lines if line.strip()]
        except Exception as e:
            print(f"Error getting failed books: {e}")
            return []

    async def show_status_summary(self) -> None:
        """Show overall processing status summary."""
        print("GRIN Processing Status Summary")
        print("=" * 50)
        print(f"Directory: {self.directory}")
        print(f"Checked at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()

        converted = await self.get_converted_books()
        in_process = await self.get_in_process_books()
        failed = await self.get_failed_books()

        print(f"📗 Converted (ready for download): {len(converted):,}")
        print(f"⏳ In process (being converted):    {len(in_process):,}")
        print(f"❌ Failed (conversion failed):     {len(failed):,}")
        print(f"📊 Total processed:                {len(converted) + len(in_process) + len(failed):,}")
        print(f"🎯 Queue space available:          {50000 - len(in_process):,}")

    async def show_converted_books(self, limit: int = 50) -> None:
        """Show list of converted books ready for download."""
        converted = await self.get_converted_books()
        
        print(f"\nConverted Books Ready for Download ({len(converted):,} total)")
        print("=" * 60)
        
        if not converted:
            print("No converted books found.")
            return

        for i, barcode in enumerate(converted[:limit], 1):
            print(f"{i:4}. {barcode}")
            
        if len(converted) > limit:
            print(f"... and {len(converted) - limit:,} more")
            print(f"\nUse --limit={len(converted)} to see all converted books")

    async def show_in_process_books(self, limit: int = 50) -> None:
        """Show list of books currently being processed."""
        in_process = await self.get_in_process_books()
        
        print(f"\nBooks Currently Being Processed ({len(in_process):,} total)")
        print("=" * 60)
        
        if not in_process:
            print("No books currently in process.")
            return

        for i, barcode in enumerate(in_process[:limit], 1):
            print(f"{i:4}. {barcode}")
            
        if len(in_process) > limit:
            print(f"... and {len(in_process) - limit:,} more")

    async def show_failed_books(self, limit: int = 50) -> None:
        """Show list of books that failed processing."""
        failed = await self.get_failed_books()
        
        print(f"\nBooks That Failed Processing ({len(failed):,} total)")
        print("=" * 60)
        
        if not failed:
            print("No failed books.")
            return

        for i, barcode in enumerate(failed[:limit], 1):
            print(f"{i:4}. {barcode}")
            
        if len(failed) > limit:
            print(f"... and {len(failed) - limit:,} more")

    async def search_barcode(self, barcode: str) -> None:
        """Search for a specific barcode across all GRIN states."""
        print(f"Searching for barcode: {barcode}")
        print("=" * 40)

        converted = await self.get_converted_books()
        in_process = await self.get_in_process_books()
        failed = await self.get_failed_books()

        found = False

        if barcode in converted:
            print(f"✅ Found in CONVERTED - ready for download!")
            found = True

        if barcode in in_process:
            print(f"⏳ Found in IN_PROCESS - currently being converted")
            found = True

        if barcode in failed:
            print(f"❌ Found in FAILED - conversion failed")
            found = True

        if not found:
            print(f"❓ Not found in GRIN system - may not have been requested for processing")

    async def export_converted_list(self, output_file: str) -> None:
        """Export list of converted books to a file."""
        converted = await self.get_converted_books()
        
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            f.write(f"# Converted books ready for download\n")
            f.write(f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# Total: {len(converted):,} books\n")
            f.write(f"#\n")
            for barcode in converted:
                f.write(f"{barcode}\n")
        
        print(f"Exported {len(converted):,} converted books to: {output_file}")


async def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Monitor GRIN book processing status",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Show overall status summary
  python monitor_processing.py

  # Show converted books ready for download
  python monitor_processing.py --converted

  # Show books currently being processed  
  python monitor_processing.py --in-process

  # Show books that failed processing
  python monitor_processing.py --failed

  # Search for a specific barcode
  python monitor_processing.py --search TZ1XH8

  # Export converted books to file
  python monitor_processing.py --export converted_books.txt

  # Show more results
  python monitor_processing.py --converted --limit 100
        """,
    )

    parser.add_argument("--directory", default="Harvard", help="GRIN directory (default: Harvard)")
    parser.add_argument("--secrets-dir", help="Directory containing GRIN secrets files")

    # What to show
    parser.add_argument("--converted", action="store_true", help="Show converted books ready for download")
    parser.add_argument("--in-process", action="store_true", help="Show books currently being processed")
    parser.add_argument("--failed", action="store_true", help="Show books that failed processing")
    parser.add_argument("--search", metavar="BARCODE", help="Search for a specific barcode")
    parser.add_argument("--export", metavar="FILE", help="Export converted books to file")

    # Options
    parser.add_argument("--limit", type=int, default=50, help="Limit number of results to show (default: 50)")

    args = parser.parse_args()

    try:
        monitor = ProcessingMonitor(
            directory=args.directory,
            secrets_dir=args.secrets_dir,
        )

        # Validate credentials
        try:
            await monitor.grin_client.auth.validate_credentials(args.directory)
        except Exception as e:
            print(f"Error: Credential validation failed: {e}")
            sys.exit(1)

        # Show status summary unless specific action requested
        if not any([args.converted, args.in_process, args.failed, args.search, args.export]):
            await monitor.show_status_summary()
        
        # Handle specific requests
        if args.converted:
            await monitor.show_converted_books(limit=args.limit)
        
        if args.in_process:
            await monitor.show_in_process_books(limit=args.limit)
        
        if args.failed:
            await monitor.show_failed_books(limit=args.limit)
        
        if args.search:
            await monitor.search_barcode(args.search)
        
        if args.export:
            await monitor.export_converted_list(args.export)

        await monitor.cleanup()

    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())