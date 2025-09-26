#!/usr/bin/env python3
"""Function for filtering barcodes through the sync pipeline."""

from grin_to_s3.common import pluralize


def filter_and_print_barcodes(
    specific_barcodes: list[str] | None,
    queue_books: set[str] | None,
    books_already_synced: set[str],
    limit: int | None,
    queue_name: str | None = None,
) -> list[str]:
    """Filter barcodes and print summary. Returns list of books to process."""

    # Get source books
    if specific_barcodes:
        books_from_source = set(specific_barcodes)
        source_description = f"{len(books_from_source):,} specific {pluralize(len(books_from_source), 'barcode')}"
        using_specific = True
    elif queue_books:
        books_from_source = queue_books
        if queue_name == "previous":
            source_description = f"{len(books_from_source):,} {pluralize(len(books_from_source), 'book')} from queues (already-requested and unavailable excluded)"
        else:
            source_description = f"{len(books_from_source):,} {pluralize(len(books_from_source), 'book')} from queues"
        using_specific = False
    else:
        books_from_source = set()
        source_description = "no books available"
        using_specific = False

    # Filter by sync status (skip for specific barcodes)
    if using_specific:
        books_needing_sync = list(books_from_source)
        already_synced_count = 0
    else:
        books_needing_sync = list(books_from_source - books_already_synced)
        already_synced_count = len(books_from_source) - len(books_needing_sync)

    # Apply limit
    if limit and limit < len(books_needing_sync):
        books_to_process = books_needing_sync[:limit]
        limit_applied = limit
    else:
        books_to_process = books_needing_sync
        limit_applied = None

    # Print summary
    print("\nBarcode filtering:")
    print(f"  1. Source: {source_description}")
    print(f"     -> {len(books_from_source):,} {pluralize(len(books_from_source), 'book')} available")

    if using_specific:
        print("  2. Sync filter: Skipped (using specific barcodes)")
        print(f"     -> {len(books_needing_sync):,} {pluralize(len(books_needing_sync), 'book')} to process")
    elif already_synced_count > 0:
        print(f"  2. Sync filter: Removed {already_synced_count:,} already synced")
        print(
            f"     -> {len(books_needing_sync):,} {pluralize(len(books_needing_sync), 'book')} will be processed by this queue"
        )
    else:
        print("  2. Sync filter: No books already synced")
        print(
            f"     -> {len(books_needing_sync):,} {pluralize(len(books_needing_sync), 'book')} will be processed by this queue"
        )

    if limit_applied:
        print(f"  3. Limit: Applied limit of {limit_applied:,}")
        print(f"     -> {len(books_to_process):,} {pluralize(len(books_to_process), 'book')} will be processed")

    # Handle edge cases with appropriate messages
    if not books_to_process:
        if not books_from_source:
            queue_msg = f" from '{queue_name}' queue" if queue_name else ""
            print(f"No books available{queue_msg}")
        else:
            queue_msg = f" from '{queue_name}' queue" if queue_name else ""
            if queue_name == "previous":
                print(
                    f"No books{queue_msg} will be processed (remaining books have already been requested for processing or are synced)"
                )
            else:
                print(f"No books{queue_msg} will be processed (all may already be synced)")

    return books_to_process
