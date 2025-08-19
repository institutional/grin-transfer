#!/usr/bin/env python3
"""Pure functions for filtering barcodes through the sync pipeline."""

import itertools
from typing import NamedTuple

from grin_to_s3.common import pluralize


class BarcodeFilteringResult(NamedTuple):
    """Result of the barcode filtering pipeline."""

    source_books: set[str]  # Books from GRIN or user input
    books_needing_sync: list[str]  # After filtering by sync status
    books_after_limit: list[str]  # Final list after applying limit

    # Metadata for reporting
    source_description: str
    already_synced_count: int
    limit_applied: int | None


def get_source_books(specific_barcodes: list[str] | None, queue_books: set[str] | None) -> tuple[set[str], str]:
    """Get the initial set of books from either specific barcodes or queues."""
    if specific_barcodes:
        count = len(specific_barcodes)
        return set(specific_barcodes), f"{count:,} specific {pluralize(count, 'barcode')}"
    elif queue_books:
        count = len(queue_books)
        return queue_books, f"{count:,} {pluralize(count, 'book')} from queues"
    else:
        return set(), "no books available"


def filter_books_needing_sync(
    source_books: set[str], books_already_synced: set[str], skip_filtering: bool = False
) -> tuple[list[str], int]:
    """Filter source books to only those needing sync."""
    if skip_filtering:
        return list(source_books), 0

    books_needing_sync = source_books - books_already_synced
    already_synced_count = len(source_books) - len(books_needing_sync)
    return list(books_needing_sync), already_synced_count


def apply_limit_to_books(books: list[str], limit: int | None) -> tuple[list[str], int | None]:
    """Apply a limit to the list of books if specified."""
    if limit is None or limit >= len(books):
        return books, None

    return list(itertools.islice(books, limit)), limit


def create_filtering_summary(result: BarcodeFilteringResult) -> list[str]:
    """Create a human-readable summary of the filtering process."""
    source_count = len(result.source_books)
    needing_sync_count = len(result.books_needing_sync)
    final_count = len(result.books_after_limit)

    lines = [
        "\nBook Selection Pipeline:",
        f"  1. Source: {result.source_description}",
        f"     -> {source_count:,} {pluralize(source_count, 'book')} available",
    ]

    if result.already_synced_count > 0:
        lines.append(f"  2. Sync filter: Removed {result.already_synced_count:,} already synced")
        lines.append(f"     -> {needing_sync_count:,} {pluralize(needing_sync_count, 'book')} need syncing")
    else:
        lines.append("  2. Sync filter: Skipped (using specific barcodes)")
        lines.append(f"     -> {needing_sync_count:,} {pluralize(needing_sync_count, 'book')} to process")

    if result.limit_applied:
        lines.append(f"  3. Limit: Applied limit of {result.limit_applied:,}")
        lines.append(f"     -> {final_count:,} {pluralize(final_count, 'book')} will be processed")
    else:
        lines.append("  3. Limit: None specified")
        lines.append(f"     -> {final_count:,} {pluralize(final_count, 'book')} will be processed")

    lines.append(f"\nFinal count: {final_count:,} {pluralize(final_count, 'book')}")

    return lines


def filter_barcodes_pipeline(
    specific_barcodes: list[str] | None, queue_books: set[str] | None, books_already_synced: set[str], limit: int | None
) -> BarcodeFilteringResult:
    """Main pipeline function that composes all filtering steps."""
    # Step 1: Get source books
    source_books, source_description = get_source_books(specific_barcodes, queue_books)

    # Step 2: Filter by sync status
    books_needing_sync, already_synced_count = filter_books_needing_sync(
        source_books, books_already_synced, skip_filtering=bool(specific_barcodes)
    )

    # Step 3: Apply limit
    books_after_limit, limit_applied = apply_limit_to_books(books_needing_sync, limit)

    return BarcodeFilteringResult(
        source_books=source_books,
        books_needing_sync=books_needing_sync,
        books_after_limit=books_after_limit,
        source_description=source_description,
        already_synced_count=already_synced_count,
        limit_applied=limit_applied,
    )
