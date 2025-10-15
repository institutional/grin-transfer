#!/usr/bin/env python3
"""
Text Extraction CLI Interface

Simple command-line interface for OCR text extraction from extracted book archives.
Run with: python grin.py extract
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from .text_extraction import extract_ocr_pages

logger = logging.getLogger(__name__)


async def main():
    """Main entry point for the extract command."""
    parser = argparse.ArgumentParser(
        prog="grin extract",
        description="Extract OCR text from extracted book archive directories",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract text from extracted directory and save to JSONL file
  python grin.py extract /path/to/book_extracted --output /path/to/output.jsonl

  # Extract from multiple extracted directories to output directory
  python grin.py extract /path/to/books/*_extracted --output-dir /path/to/jsonl_files/
        """,
    )

    parser.add_argument(
        "archives",
        nargs="+",
        help="Path to extracted archive directory (or multiple directories)",
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--output",
        "-o",
        help="Output JSONL file path (for single archive)",
    )
    group.add_argument(
        "--output-dir",
        "-d",
        help="Output directory for JSONL files (for multiple archives)",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose output",
    )

    args = parser.parse_args()

    # Set up logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    try:
        if args.output:
            # Single file output
            if len(args.archives) > 1:
                print("Error: --output can only be used with a single archive")
                return 1

            archive_path = Path(args.archives[0])
            output_path = Path(args.output)

            if not archive_path.exists():
                print(f"Error: Archive directory not found: {archive_path}")
                return 1

            if not archive_path.is_dir():
                print(f"Error: Expected directory, got file: {archive_path}")
                return 1

            print(f"Extracting OCR text from: {archive_path}")

            unpack_data = {"unpacked_path": archive_path}
            page_count = await extract_ocr_pages(unpack_data, output_path)

            print(f"  ✓ Extracted {page_count} pages")
            print(f"  ✓ Saved to: {output_path}")

        else:
            # Multiple files to output directory
            output_dir = Path(args.output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)

            total_pages = 0
            processed = 0

            for archive_str in args.archives:
                archive_path = Path(archive_str)

                if not archive_path.exists():
                    print(f"Warning: Skipping non-existent archive: {archive_path}")
                    continue

                if not archive_path.is_dir():
                    print(f"Warning: Skipping non-directory: {archive_path}")
                    continue

                # Generate output filename based on archive directory name
                barcode = archive_path.name.replace("_extracted", "")
                output_path = output_dir / f"{barcode}.jsonl"

                print(f"Processing: {archive_path}")

                try:
                    unpack_data = {"unpacked_path": archive_path}
                    page_count = await extract_ocr_pages(unpack_data, output_path)

                    total_pages += page_count
                    processed += 1

                    print(f"  ✓ Extracted {page_count} pages → {output_path}")

                except Exception as e:
                    print(f"  ✗ Failed: {e}")
                    if args.verbose:
                        import traceback

                        traceback.print_exc()

            print(f"\nSummary: Processed {processed} archives, extracted {total_pages} total pages")

        return 0

    except KeyboardInterrupt:
        print("\nExtraction cancelled by user")
        return 1
    except Exception as e:
        print(f"Error: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
