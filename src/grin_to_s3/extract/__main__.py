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

from ..storage.book_storage import BookStorage
from .text_extraction import (
    CorruptedArchiveError,
    InvalidPageFormatError,
    TextExtractionError,
    extract_text_to_jsonl_file,
    get_barcode_from_path,
)

logger = logging.getLogger(__name__)


def validate_storage_config(args: argparse.Namespace) -> dict | None:
    """
    Validate and build storage configuration from CLI arguments.

    Returns:
        Storage config dict if storage is configured, None otherwise

    Raises:
        ValueError: If storage configuration is invalid
    """
    if not args.storage:
        return None

    if not args.bucket_full:
        raise ValueError("--bucket-full is required when using storage")
    if not args.bucket_raw:
        raise ValueError("--bucket-raw is required when using storage")
    if not args.bucket_meta:
        raise ValueError("--bucket-meta is required when using storage")

    # Build config with all three buckets
    config = {
        "bucket_full": args.bucket_full,
        "bucket_raw": args.bucket_raw,
        "bucket_meta": args.bucket_meta
    }

    # Add storage-specific configuration
    if args.storage == "r2":
        if args.credentials_file:
            config["credentials_file"] = args.credentials_file
    elif args.storage == "minio":
        if args.endpoint_url:
            config["endpoint_url"] = args.endpoint_url
        if args.access_key:
            config["access_key"] = args.access_key
        if args.secret_key:
            config["secret_key"] = args.secret_key

    return config


async def write_to_bucket(
    book_storage: BookStorage,
    barcode: str,
    jsonl_file_path: str,
    verbose: bool = False
) -> None:
    """Upload JSONL file to full-text bucket."""
    try:
        path = await book_storage.save_ocr_text_jsonl_from_file(barcode, jsonl_file_path)
        if verbose:
            print(f"  ✓ Uploaded to bucket: {path}")
    except Exception as e:
        raise RuntimeError(f"Failed to upload to bucket: {e}") from e


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

  # Extract multiple archives to directory
  python grin.py extract /path/to/books/*.tar.gz --output-dir /path/to/jsonl_files/

  # Extract and save to R2 buckets
  python grin.py extract /path/to/book.tar.gz --storage r2 \\
    --bucket-raw my-raw --bucket-meta my-meta --bucket-full my-full

  # Extract multiple files to R2 buckets
  python grin.py extract /path/to/books/*.tar.gz --storage r2 \\
    --bucket-raw my-raw --bucket-meta my-meta --bucket-full my-full

  # Extract to MinIO with custom endpoint
  python grin.py extract /path/to/book.tar.gz --storage minio \\
    --bucket-raw my-raw --bucket-meta my-meta --bucket-full my-full \\
    --endpoint-url http://localhost:9000
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


    # Bucket storage options
    storage_group = parser.add_argument_group("bucket storage options")

    storage_group.add_argument(
        "--storage",
        choices=["s3", "r2", "minio"],
        help="Storage backend type",
    )

    storage_group.add_argument(
        "--bucket-full",
        type=str,
        help="Full-text bucket name for storage",
    )

    storage_group.add_argument(
        "--bucket-raw",
        type=str,
        help="Raw data bucket name (required when using storage)",
    )

    storage_group.add_argument(
        "--bucket-meta",
        type=str,
        help="Metadata bucket name (required when using storage)",
    )

    storage_group.add_argument(
        "--storage-prefix",
        type=str,
        default="",
        help="Storage path prefix (default: none)",
    )

    storage_group.add_argument(
        "--credentials-file",
        type=str,
        help="Path to credentials file (for R2 storage)",
    )

    storage_group.add_argument(
        "--endpoint-url",
        type=str,
        help="Custom endpoint URL (for MinIO storage)",
    )

    storage_group.add_argument(
        "--access-key",
        type=str,
        help="Access key for storage backend",
    )

    storage_group.add_argument(
        "--secret-key",
        type=str,
        help="Secret key for storage backend",
    )

    return parser


async def extract_single_archive(
    archive_path: str,
    output_path: str | None = None,
    book_storage: BookStorage | None = None,
    verbose: bool = False,
    extraction_dir: str | None = None,
    keep_extracted: bool = False,
    use_memory: bool = True,
) -> dict:
    """Extract text from single archive and return stats."""
    if verbose:
        print(f"Processing: {archive_path}")

    try:
        if output_path:
            # File output: write directly to specified path
            page_count = extract_text_to_jsonl_file(archive_path, output_path)
            if verbose:
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
                page_count = extract_text_to_jsonl_file(archive_path, temp_path)
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

    # Validate and set up storage configuration
    try:
        storage_config = validate_storage_config(args)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    # Create book storage if bucket configuration provided
    book_storage = None
    if storage_config:
        try:
            from ..storage.factories import create_book_storage_with_full_text
            book_storage = create_book_storage_with_full_text(
                args.storage, storage_config, args.storage_prefix
            )
            if args.verbose:
                print(f"Configured {args.storage} storage with bucket: {args.bucket_full}")
        except Exception as e:
            print(f"Error: Failed to configure storage: {e}", file=sys.stderr)
            return 1

    # Validate that we have exactly one output method
    has_file_output = bool(args.output or args.output_dir)
    has_bucket_output = bool(book_storage)

    if not has_file_output and not has_bucket_output:
        print(
            "Error: Must specify either file output (--output/--output-dir) or bucket storage configuration",
            file=sys.stderr
        )
        return 1

    if has_file_output and has_bucket_output:
        print("Error: Cannot specify both file output and bucket storage - choose one", file=sys.stderr)
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
            output_path=output_path,
            book_storage=book_storage,
            verbose=args.verbose,
            extraction_dir=args.extraction_dir,
            keep_extracted=args.keep_extracted,
            use_memory=not args.use_disk,
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
