#!/usr/bin/env python3
"""
Text Extraction CLI Interface

Command-line interface for OCR text extraction from decrypted book archives.
Run with: python grin.py extract
"""

import argparse
import json
import logging
import sys
from pathlib import Path

from .text_extraction import (
    CorruptedArchiveError,
    InvalidPageFormatError,
    TextExtractionError,
    extract_text_from_archive,
    extract_text_to_jsonl_file,
    get_barcode_from_path,
)

logger = logging.getLogger(__name__)


def create_parser() -> argparse.ArgumentParser:
    """Create argument parser for text extraction commands."""
    parser = argparse.ArgumentParser(
        description="Extract OCR text from decrypted book archives",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract text and print to stdout (JSONL format)
  python grin.py extract /path/to/book.tar.gz

  # Extract text and save to JSONL file
  python grin.py extract /path/to/book.tar.gz --output /path/to/output.jsonl

  # Extract multiple archives
  python grin.py extract /path/to/books/*.tar.gz --output-dir /path/to/jsonl_files/
        """,
    )

    parser.add_argument(
        "archives",
        nargs="+",
        help="Path(s) to decrypted .tar.gz archive file(s)",
    )

    parser.add_argument(
        "-o", "--output",
        type=str,
        help="Output JSONL file path (default: print to stdout)",
    )

    parser.add_argument(
        "--output-dir",
        type=str,
        help="Output directory for multiple files (creates BARCODE.jsonl for each archive)",
    )


    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output",
    )

    parser.add_argument(
        "--summary",
        action="store_true",
        help="Show extraction summary statistics",
    )

    parser.add_argument(
        "--extraction-dir",
        type=str,
        help="Directory to extract archives to (creates BARCODE/ subdirectories, only used with --use-disk)",
    )

    parser.add_argument(
        "--keep-extracted",
        action="store_true",
        help="Keep extracted files after processing (only used with --use-disk)",
    )

    parser.add_argument(
        "--use-disk",
        action="store_true",
        help="Extract to disk instead of memory (better for parallel processing)",
    )

    parser.add_argument(
        "--no-streaming",
        action="store_true",
        help="Disable streaming mode for JSONL output (loads all pages into memory)",
    )

    return parser


def extract_single_archive(
    archive_path: str,
    output_path: str | None = None,
    verbose: bool = False,
    extraction_dir: str | None = None,
    keep_extracted: bool = False,
    use_memory: bool = True,
    streaming: bool = False
) -> dict:
    """Extract text from single archive and return stats."""
    if verbose:
        print(f"Processing: {archive_path}")

    try:
        if output_path:
            if streaming:
                # Use memory-efficient streaming mode
                extract_text_to_jsonl_file(archive_path, output_path, streaming=True)
                if verbose:
                    print(f"  ✓ Saved to: {output_path} (streaming mode)")
                # For stats, we need to count pages (streaming doesn't return them)
                pages = []  # Empty for stats calculation
            else:
                # Extract text first
                pages = extract_text_from_archive(
                    archive_path,
                    extraction_dir=extraction_dir,
                    keep_extracted=keep_extracted,
                    use_memory=use_memory
                )

                # Save to JSONL file
                output_path_obj = Path(output_path)
                output_path_obj.parent.mkdir(parents=True, exist_ok=True)

                with open(output_path_obj, "w", encoding="utf-8") as f:
                    for page in pages:
                        f.write(json.dumps(page, ensure_ascii=False) + "\n")

                if verbose:
                    print(f"  ✓ Saved to: {output_path}")

        else:
            # Extract text (memory or disk based)
            pages = extract_text_from_archive(
                archive_path,
                extraction_dir=extraction_dir,
                keep_extracted=keep_extracted,
                use_memory=use_memory
            )

            # Print to stdout in JSONL format
            for page in pages:
                print(json.dumps(page, ensure_ascii=False))

        # Calculate stats
        total_chars = sum(len(page) for page in pages)
        non_empty_pages = sum(1 for page in pages if page.strip())

        return {
            "success": True,
            "archive": archive_path,
            "output": output_path,
            "pages": len(pages),
            "non_empty_pages": non_empty_pages,
            "total_chars": total_chars,
        }

    except CorruptedArchiveError as e:
        error_msg = f"Corrupted archive: {e}"
        print(f"✗ {error_msg}", file=sys.stderr)
        return {
            "success": False,
            "archive": archive_path,
            "error": error_msg,
        }

    except InvalidPageFormatError as e:
        error_msg = f"Invalid page format: {e}"
        print(f"✗ {error_msg}", file=sys.stderr)
        return {
            "success": False,
            "archive": archive_path,
            "error": error_msg,
        }

    except TextExtractionError as e:
        error_msg = f"Extraction failed: {e}"
        print(f"✗ {error_msg}", file=sys.stderr)
        return {
            "success": False,
            "archive": archive_path,
            "error": error_msg,
        }

    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        print(f"✗ {error_msg}", file=sys.stderr)
        return {
            "success": False,
            "archive": archive_path,
            "error": error_msg,
        }


def main() -> int:
    """Main entry point for text extraction CLI."""
    parser = create_parser()
    args = parser.parse_args()

    # Validate arguments
    if args.output and args.output_dir:
        print("Error: Cannot specify both --output and --output-dir", file=sys.stderr)
        return 1

    if len(args.archives) > 1 and args.output:
        print("Error: Cannot use --output with multiple archives. Use --output-dir instead.", file=sys.stderr)
        return 1

    # Process archives
    results = []

    for archive_path in args.archives:
        # Determine output path
        output_path: str | None = None
        if args.output:
            output_path = args.output
        elif args.output_dir:
            output_dir = Path(args.output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            barcode = get_barcode_from_path(archive_path)
            output_path = str(output_dir / f"{barcode}.jsonl")

        # Extract from archive
        result = extract_single_archive(
            archive_path=archive_path,
            output_path=output_path,
            verbose=args.verbose,
            extraction_dir=args.extraction_dir,
            keep_extracted=args.keep_extracted,
            use_memory=not args.use_disk,
            streaming=not args.no_streaming  # Default to streaming
        )
        results.append(result)

    # Show summary if requested or if processing multiple files
    if args.summary or len(args.archives) > 1:
        successful = [r for r in results if r["success"]]
        failed = [r for r in results if not r["success"]]

        print("\nExtraction Summary:", file=sys.stderr)
        print(f"  Total archives: {len(results)}", file=sys.stderr)
        print(f"  Successful: {len(successful)}", file=sys.stderr)
        print(f"  Failed: {len(failed)}", file=sys.stderr)

        if successful:
            total_pages = sum(r["pages"] for r in successful)
            total_chars = sum(r["total_chars"] for r in successful)
            print(f"  Total pages extracted: {total_pages}", file=sys.stderr)
            print(f"  Total characters: {total_chars:,}", file=sys.stderr)

        if failed and args.verbose:
            print("\nFailed extractions:", file=sys.stderr)
            for result in failed:
                print(f"  {result['archive']}: {result['error']}", file=sys.stderr)

    # Return appropriate exit code
    failed_count = sum(1 for r in results if not r["success"])
    return min(failed_count, 1)  # Return 1 if any failed, 0 if all succeeded


if __name__ == "__main__":
    sys.exit(main())
