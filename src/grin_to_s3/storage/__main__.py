#!/usr/bin/env python3
"""
Storage CLI Interface

Command-line interface for storage bucket management operations.
Run with: python grin.py storage
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

import boto3

from grin_to_s3.constants import OUTPUT_DIR
from grin_to_s3.storage.factories import create_storage_from_config

from ..run_config import (
    apply_run_config_to_args,
    load_run_config,
)

logger = logging.getLogger(__name__)


def list_bucket_files(
    storage, bucket: str, prefix: str = "", fetch_all: bool = False
) -> tuple[list[tuple[str, int]], bool]:  # type: ignore
    """List files in bucket with sizes.

    Args:
        storage: Storage instance
        bucket: Bucket name
        prefix: Optional prefix filter
        fetch_all: If False, only fetch first page (1000 items). If True, fetch all.

    Returns:
        Tuple of (files, has_more) where has_more indicates truncated results
    """

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=storage.config.options.get("key"),
        aws_secret_access_key=storage.config.options.get("secret"),
        endpoint_url=storage.config.endpoint_url,
    )

    list_kwargs = {"Bucket": bucket}
    if prefix:
        list_kwargs["Prefix"] = prefix

    paginator = s3_client.get_paginator("list_objects_v2")
    files = []
    has_more = False

    for page in paginator.paginate(**list_kwargs):  # type: ignore
        contents = page.get("Contents", [])
        for obj in contents:
            key = obj.get("Key")
            size = obj.get("Size")
            if key is not None and size is not None:
                files.append((key, size))

        # If not fetching all, stop after first page
        if not fetch_all:
            # Check if there are more results (page was full or IsTruncated flag)
            has_more = page.get("IsTruncated", False)
            break

    return files, has_more


def get_bucket_stats(storage, bucket: str, prefix: str = "", fetch_all: bool = False) -> tuple[int, int, bool]:
    """Get bucket file count and total size.

    Args:
        storage: Storage instance
        bucket: Bucket name
        prefix: Optional prefix filter
        fetch_all: If False, only fetch first page (1000 items). If True, fetch all.

    Returns:
        Tuple of (count, size, has_more) where has_more indicates truncated results
    """
    files, has_more = list_bucket_files(storage, bucket, prefix, fetch_all)
    total_size = sum(size for _, size in files)
    return len(files), total_size, has_more


def delete_bucket_contents(
    storage, bucket: str, prefix: str = "", files_to_delete: list[tuple[str, int]] | None = None
) -> tuple[int, int]:
    """Delete all contents from bucket with prefix."""

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=storage.config.options.get("key"),
        aws_secret_access_key=storage.config.options.get("secret"),
        endpoint_url=storage.config.endpoint_url,
    )

    # Use provided file list or fetch from bucket
    if files_to_delete is not None:
        objects_to_delete = [filename for filename, _ in files_to_delete]
    else:
        list_kwargs = {"Bucket": bucket}
        if prefix:
            list_kwargs["Prefix"] = prefix

        paginator = s3_client.get_paginator("list_objects_v2")
        objects_to_delete = []

        page_count = 0
        for page in paginator.paginate(**list_kwargs):  # type: ignore
            contents = page.get("Contents", [])
            for obj in contents:
                key = obj.get("Key")
                if key is not None:
                    objects_to_delete.append(key)

            page_count += 1
            if page_count > 1:  # Show progress after first page
                print(f"  Scanned {len(objects_to_delete):,} objects...")

    if not objects_to_delete:
        return 0, 0

    # Delete in batches of 1000 (S3 API limit)
    deleted_count = 0
    failed_count = 0
    batch_size = 1000
    batch_num = 0

    for i in range(0, len(objects_to_delete), batch_size):
        batch = objects_to_delete[i : i + batch_size]
        delete_objects = [{"Key": filename} for filename in batch]  # type: ignore

        try:
            response = s3_client.delete_objects(Bucket=bucket, Delete={"Objects": delete_objects})  # type: ignore

            # Count successful deletions
            deleted_count += len(response.get("Deleted", []))

            # Count failures
            failed_count += len(response.get("Errors", []))

            # Show progress for large deletions
            batch_num += 1
            if batch_num > 1 or len(objects_to_delete) > batch_size:
                print(f"  Deleted {deleted_count:,} objects...")

        except Exception:
            failed_count += len(batch)

    return deleted_count, failed_count


def format_size(size_bytes: int) -> str:
    """Format size in human-readable format."""
    size_float = float(size_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_float < 1024.0:
            return f"{size_float:.1f} {unit}"
        size_float = size_float / 1024.0
    return f"{size_float:.1f} PB"


async def cmd_ls(args) -> None:
    """List contents of all storage buckets."""

    run_config = load_run_config(OUTPUT_DIR / args.run_name / "run_config.json")
    apply_run_config_to_args(args, run_config)

    storage_config_dict = run_config.storage_config
    storage_type = storage_config_dict["type"]
    storage = create_storage_from_config(run_config.storage_config)
    prefix = str(storage_config_dict.get("prefix", "") or "")
    buckets = {
        "raw": run_config.storage_bucket_raw,
        "meta": run_config.storage_bucket_meta,
        "full": run_config.storage_bucket_full,
    }

    print(f"\nStorage Listing for '{args.run_name}'")
    print("=" * 60)
    print(f"Storage Type: {storage_type}")

    print()

    total_files = 0
    total_size = 0

    for bucket_name, bucket in buckets.items():
        if args.bucket_name and bucket_name != args.bucket_name:
            continue
        if not bucket:
            print(f"{bucket_name} Bucket: Not configured")
            continue

        print(f"{bucket_name} Bucket: {bucket}")
        try:
            if args.long:
                # Long listing format with files and sizes
                if storage_type in ("r2", "s3", "minio"):
                    files, has_more = list_bucket_files(storage, bucket, prefix, fetch_all=args.all)

                    if not files:
                        print("  (empty)")
                    else:
                        # Sort files by path
                        files.sort(key=lambda x: x[0])

                        for file_path, size in files:
                            if size > 0:
                                size_str = format_size(size)
                                print(f"  {file_path:60} {size_str:>10}")
                            else:
                                print(f"  {file_path:60}        0 B")

                        # Calculate stats from files we already have instead of calling get_bucket_stats
                        bucket_files = len(files)
                        bucket_size = sum(size for _, size in files)

                        print(f"  \nTotal: {bucket_files:,} files")
                        if has_more:
                            print("  (More files available - use --all to fetch all)")

                    total_files += bucket_files
                    total_size += bucket_size
                else:
                    print("  ❌ Long listing not supported for this storage type")
                    # Fallback to simple count
                    objects = await storage.list_objects(f"{bucket}/{prefix}" if prefix else bucket)
                    bucket_files = len(objects)
                    total_files += bucket_files
                    print(f"  Total files: {bucket_files:,}")
            else:
                # Regular summary format
                if storage_type in ("r2", "s3", "minio"):
                    bucket_files, bucket_size, has_more = get_bucket_stats(storage, bucket, prefix, fetch_all=args.all)
                    total_files += bucket_files
                    total_size += bucket_size
                    print(f"  Total files: {bucket_files:,}")
                    if has_more:
                        print("  (More files available - use --all to fetch all)")
                else:
                    objects = await storage.list_objects(f"{bucket}/{prefix}" if prefix else bucket)
                    bucket_files = len(objects)
                    total_files += bucket_files
                    print(f"  Total files: {bucket_files:,}")

        except Exception as e:
            print(f"  ❌ Error accessing bucket: {e}")

        print()

    print("Overall Summary:")
    print(f"  Total files across all buckets: {total_files:,}")
    if args.long:
        print(f"  Total storage used: {format_size(total_size)}")


async def cmd_cp(args) -> None:
    """Copy a file from named storage bucket to local directory."""

    run_config = load_run_config(OUTPUT_DIR / args.run_name / "run_config.json")
    apply_run_config_to_args(args, run_config)

    storage_config_dict = run_config.storage_config
    storage_type = storage_config_dict["type"]
    storage = create_storage_from_config(run_config.storage_config)
    prefix = str(storage_config_dict.get("prefix", "") or "")
    buckets = {
        "raw": run_config.storage_bucket_raw,
        "meta": run_config.storage_bucket_meta,
        "full": run_config.storage_bucket_full,
    }

    cp_bucket = buckets[args.bucket_name]

    prefix = str(storage_config_dict.get("prefix", "") or "")

    print(f"\nStorage Copy for {args.run_name}")
    print("=" * 60)
    print(f"Storage Type: {storage_type}")
    print(f"Source Bucket: {cp_bucket}")
    print(f"Source File: {args.filename}")
    print(f"Destination: {args.local_dir}")
    print()

    # Construct full source path with prefix if needed
    source_path = f"{prefix}/{args.filename}" if prefix else args.filename

    # Ensure local directory exists
    local_dir = Path(args.local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)

    # Extract just the filename for local storage
    filename_only = Path(args.filename).name
    local_file_path = local_dir / filename_only

    print(f"Downloading {source_path} from {cp_bucket}...")

    # Download the file - include bucket in path like BookStorage does
    file_data = await storage.read_bytes(f"{cp_bucket}/{source_path}")

    # Write to local file
    with open(local_file_path, "wb") as f:
        f.write(file_data)

    file_size = len(file_data)
    print(f"✅ Successfully downloaded {args.filename}")
    print(f"   Size: {format_size(file_size)}")
    print(f"   Saved to: {local_file_path}")


async def cmd_rm(args) -> None:
    """Remove all contents from a storage bucket."""

    run_config = load_run_config(OUTPUT_DIR / args.run_name / "run_config.json")
    apply_run_config_to_args(args, run_config)

    storage_config_dict = run_config.storage_config
    storage_type = storage_config_dict["type"]
    storage = create_storage_from_config(run_config.storage_config)
    prefix = str(storage_config_dict.get("prefix", "") or "")
    buckets = {
        "raw": run_config.storage_bucket_raw,
        "meta": run_config.storage_bucket_meta,
        "full": run_config.storage_bucket_full,
    }

    bucket_name = args.bucket_name
    if bucket_name not in buckets:
        print(f"❌ Error: Unknown bucket '{bucket_name}'")
        print(f"Available buckets: {', '.join(buckets.keys())}")
        sys.exit(1)

    bucket = buckets[bucket_name]
    assert bucket

    print(f"\nStorage Removal for {args.run_name}")
    print("=" * 60)
    print(f"Storage Type: {storage_type}")
    print(f"Target Bucket: {bucket} ({bucket_name})")
    if prefix:
        logger.debug(f"Using prefix: {prefix}")
    print()

    # Check bucket contents
    print("Checking bucket contents...")
    if storage_type in ("r2", "s3", "minio"):
        # Fetch first page to show samples and estimate count
        files, has_more = list_bucket_files(storage, bucket, prefix, fetch_all=False)
        object_count = len(files)

        if object_count == 0:
            print("Bucket is already empty")
            return

        # Display count (with + if more pages exist)
        if has_more:
            print(f"Found {object_count:,}+ objects to delete (scanning...)")
        else:
            print(f"Found {object_count:,} objects to delete")

        # Show sample of files to be deleted
        if object_count <= 10:
            print("\nObjects to be deleted:")
            for i, (filename, _) in enumerate(files, 1):
                print(f"  {i:2d}. {filename}")
        else:
            print("\nFirst 5 objects to be deleted:")
            for i, (filename, _) in enumerate(files[:5], 1):
                print(f"  {i:2d}. {filename}")
            if has_more:
                print("  ... and more (use Ctrl+C to cancel)")
            else:
                print(f"  ... and {object_count - 5:,} more objects")
    else:
        print("❌ Error: Bucket removal only supported for S3-compatible storage")
        sys.exit(1)

    # Handle dry-run mode
    if args.dry_run:
        count_msg = f"{object_count:,}+" if has_more else f"{object_count:,}"
        print(f"\n📋 DRY RUN: Would delete {count_msg} objects from:")
        print(f"   Bucket: {bucket}")
        if prefix:
            print(f"   Prefix: {prefix}")
        print("\nTo actually delete these objects, run without --dry-run")
        return

    # Confirm deletion
    if not args.yes:
        count_msg = f"{object_count:,}+" if has_more else f"{object_count:,}"
        print(f"\n⚠️  WARNING: This will permanently delete {count_msg} objects from:")
        print(f"   Bucket: {bucket}")
        if prefix:
            print(f"   Prefix: {prefix}")
        print("\nThis action CANNOT be undone!")

        response = input(f"\nType 'DELETE' to confirm removal of {count_msg} objects: ").strip()
        if response != "DELETE":
            print("Removal cancelled")
            return

    # Perform deletion - let delete_bucket_contents do its own pagination with progress
    print("\nDeleting objects...")

    deleted_count, failed_count = delete_bucket_contents(storage, bucket, prefix, files_to_delete=None)

    print("\nDeletion complete:")
    print(f"  Successfully deleted: {deleted_count:,}")
    if failed_count > 0:
        print(f"  Failed deletions: {failed_count:,}")


async def main() -> None:
    """Main CLI entry point for storage management."""
    parser = argparse.ArgumentParser(
        description="GRIN storage management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List contents of all buckets (summary)
  python grin.py storage ls --run-name harvard_2024

  # Long format listing with recursive file listing
  python grin.py storage ls --run-name harvard_2024 -l

  # Copy a file from raw bucket to local directory
  python grin.py storage cp BARCODE123/BARCODE123.tar.gz ./downloads --run-name harvard_2024

  # Remove all files from raw bucket
  python grin.py storage rm raw --run-name harvard_2024

  # Remove files with auto-confirm
  python grin.py storage rm meta --run-name harvard_2024 --yes
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Storage commands")

    # List command
    ls_parser = subparsers.add_parser(
        "ls",
        help="List contents of all storage buckets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ls_parser.add_argument("--run-name", required=True, help="Run name (e.g., harvard_2024)")
    ls_parser.add_argument(
        "-l", "--long", action="store_true", help="Use long listing format with recursive file listing"
    )
    ls_parser.add_argument("--bucket-name", choices=["raw", "meta", "full"], help="Bucket to list (raw, meta, or full)")
    ls_parser.add_argument("--all", action="store_true", help="Fetch all files (default: first 1000 only)")

    # Copy command
    cp_parser = subparsers.add_parser(
        "cp",
        help="Copy a file from a storage bucket to local directory",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    cp_parser.add_argument(
        "bucket_name", choices=["raw", "meta", "full"], help="Bucket to copy from (raw, meta, or full)"
    )
    cp_parser.add_argument("filename", help="Name of file to download from bucket")
    cp_parser.add_argument("local_dir", help="Local directory to save the file to")
    cp_parser.add_argument("--run-name", required=True, help="Run name (e.g., harvard_2024)")

    # Remove command
    rm_parser = subparsers.add_parser(
        "rm",
        help="Remove all contents from a storage bucket",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    rm_parser.add_argument("bucket_name", choices=["raw", "meta", "full"], help="Bucket to clear (raw, meta, or full)")
    rm_parser.add_argument("--run-name", required=True, help="Run name (e.g., harvard_2024)")
    rm_parser.add_argument("--yes", "-y", action="store_true", help="Auto-confirm without prompting (dangerous!)")
    rm_parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be deleted without actually deleting"
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "ls":
        await cmd_ls(args)
    elif args.command == "cp":
        await cmd_cp(args)
    elif args.command == "rm":
        await cmd_rm(args)


if __name__ == "__main__":
    asyncio.run(main())
