#!/usr/bin/env python3
"""
Text Extraction CLI Interface

Command-line interface for OCR text extraction from decrypted book archives.
Run with: python grin.py extract
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from ..run_config import (
    apply_run_config_to_args,
    setup_run_database_path,
)
from ..storage.book_manager import BookManager
from .text_extraction import (
    CorruptedArchiveError,
    InvalidPageFormatError,
    TextExtractionError,
    extract_ocr_pages,
    get_barcode_from_path,
)

logger = logging.getLogger(__name__)


async def write_to_bucket(book_storage: BookManager, barcode: str, jsonl_file_path: str, verbose: bool = False) -> None:
    """Upload JSONL file to full-text bucket."""
    try:
        path = await book_storage.save_ocr_text_jsonl_from_file(barcode, jsonl_file_path)
        print(f"  ✓ Uploaded to bucket: {path}")

    except Exception as e:
        print(f"  ✗ Upload failed: {e}", file=sys.stderr)
        raise


def create_parser() -> argparse.ArgumentParser:
    """Create argument parser for text extraction commands."""
    parser = argparse.ArgumentParser(
        description="Extract OCR text from decrypted book archives",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract text and save to JSONL file
  python grin.py extract /path/to/book.tar.gz --output /path/to/output.jsonl

  # Extract multiple archives to directory
  python grin.py extract /path/to/books/*.tar.gz --output-dir /path/to/jsonl_files/

  # Extract and upload to buckets using run configuration
  python grin.py extract /path/to/book.tar.gz --run-name harvard_2024

  # Extract multiple files using run configuration
  python grin.py extract /path/to/books/*.tar.gz --run-name harvard_2024
        """,
    )

    parser.add_argument(
        "archives",
        nargs="+",
        help="Path(s) to decrypted .tar.gz archive file(s)",
    )

    parser.add_argument(
        "-o",
        "--output",
        type=str,
        help="Output JSONL file path (default: print to stdout)",
    )

    parser.add_argument(
        "--output-dir",
        type=str,
        help="Output directory for multiple files (creates BARCODE.jsonl for each archive)",
    )

    parser.add_argument(
        "--verbose",
        "-v",
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
        help="Directory to extract archives to (creates BARCODE/ subdirectories)",
    )

    parser.add_argument(
        "--keep-extracted",
        action="store_true",
        help="Keep extracted files after processing",
    )

    parser.add_argument(
        "--use-memory",
        action="store_true",
        help="Extract in memory instead of to disk (memory efficient for smaller archives)",
    )

    # Run configuration
    parser.add_argument(
        "--run-name",
        type=str,
        help="Name of the collection run (uses run configuration for bucket storage)",
    )

    return parser


async def extract_single_archive(
    archive_path: str,
    db_path: str,
    session_id: str,
    output_path: str | None = None,
    book_storage: BookManager | None = None,
    verbose: bool = False,
) -> dict:
    """Extract text from single archive and return stats."""
    print(f"Processing: {archive_path}")

    try:
        if output_path:
            # File output: write directly to specified path
            page_count = await extract_ocr_pages(archive_path, db_path, session_id, output_file=output_path)
            print(f"  ✓ Saved to: {output_path}")
        else:
            # Bucket output: write to staging file, upload, then cleanup
            import tempfile
            from pathlib import Path

            if book_storage is None:
                raise ValueError("Book storage is required for bucket output mode")

            barcode = get_barcode_from_path(archive_path)
            with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as temp_file:
                temp_path = temp_file.name

            try:
                page_count = await extract_ocr_pages(archive_path, db_path, session_id, output_file=temp_path)
                await write_to_bucket(book_storage, barcode, temp_path, verbose)
            finally:
                # Clean up temp file
                Path(temp_path).unlink(missing_ok=True)

        return {
            "success": True,
            "archive": archive_path,
            "output": output_path,
            "pages": page_count,
        }

    except (CorruptedArchiveError, InvalidPageFormatError, TextExtractionError, Exception) as e:
        error_msg = str(e)
        print(f"✗ {error_msg}", file=sys.stderr)
        return {
            "success": False,
            "archive": archive_path,
            "error": error_msg,
        }


async def main() -> int:
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

    # --run-name is now required for database tracking
    if not args.run_name:
        print("Error: --run-name is required for database tracking", file=sys.stderr)
        return 1

    # Set up database path and session ID - required for tracking
    try:
        # Set up database path and apply run configuration
        db_path = setup_run_database_path(args, args.run_name)
        apply_run_config_to_args(args, db_path)
    except Exception as e:
        print(f"Error: Failed to set up run configuration: {e}", file=sys.stderr)
        return 1

    # Generate session ID for this extraction run
    import uuid

    session_id = f"extract_{uuid.uuid4().hex[:8]}"
    # Create book storage from run configuration if bucket output is needed
    book_storage = None
    if not (args.output or args.output_dir):
        try:
            # Build storage configuration from run config
            config_path = Path(db_path).parent / "run_config.json"

            if config_path.exists():
                import json

                with open(config_path) as f:
                    run_config = json.load(f)

                # Get storage config from run config
                existing_storage_config = run_config.get("storage_config", {})
                storage_type = existing_storage_config.get("type")
                storage_prefix = existing_storage_config.get("prefix", "")

                # Build full storage config with credentials (e.g., from R2 file)
                from argparse import Namespace

                from ..run_config import build_storage_config_dict

                temp_args = Namespace(
                    storage=storage_type,
                    secrets_dir=run_config.get("secrets_dir"),
                    bucket_raw=None,
                    bucket_meta=None,
                    bucket_full=None,
                    storage_config=None,
                )

                # Get credentials and merge with run config
                credentials_config = build_storage_config_dict(temp_args)
                storage_config = credentials_config.copy()
                storage_config.update(existing_storage_config.get("config", {}))

                from ..storage.factories import create_book_manager_with_full_text

                book_storage = create_book_manager_with_full_text(storage_type, storage_config, storage_prefix)
            else:
                raise FileNotFoundError(f"Run configuration not found: {config_path}")
            if args.verbose:
                print(f"Using run configuration: {args.run_name}")
        except Exception as e:
            print(f"Error: Failed to load run configuration: {e}", file=sys.stderr)
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
        result = await extract_single_archive(
            archive_path=archive_path,
            db_path=db_path,
            session_id=session_id,
            output_path=output_path,
            book_storage=book_storage,
            verbose=args.verbose,
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
            print(f"  Total pages extracted: {total_pages}", file=sys.stderr)

        if failed and args.verbose:
            print("\nFailed extractions:", file=sys.stderr)
            for result in failed:
                print(f"  {result['archive']}: {result['error']}", file=sys.stderr)

    # Return appropriate exit code
    failed_count = sum(1 for r in results if not r["success"])
    return min(failed_count, 1)  # Return 1 if any failed, 0 if all succeeded


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
