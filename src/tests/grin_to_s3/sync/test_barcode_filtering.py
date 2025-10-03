#!/usr/bin/env python3
"""Tests for barcode filtering logic."""

import pytest

from grin_to_s3.sync.barcode_filtering import filter_and_print_barcodes


def test_specific_barcodes_no_filtering():
    """filter_and_print_barcodes should process all specific barcodes without filtering."""
    specific_barcodes = ["BC001", "BC002", "BC003"]
    books_already_synced = {"BC002"}  # Should not filter when using specific barcodes

    result = filter_and_print_barcodes(
        specific_barcodes=specific_barcodes,
        queue_books=None,
        books_already_synced=books_already_synced,
        limit=None,
    )

    assert set(result) == {"BC001", "BC002", "BC003"}


def test_specific_barcodes_with_limit():
    """filter_and_print_barcodes should apply limit to specific barcodes."""
    specific_barcodes = ["BC001", "BC002", "BC003", "BC004"]

    result = filter_and_print_barcodes(
        specific_barcodes=specific_barcodes,
        queue_books=None,
        books_already_synced=set(),
        limit=2,
    )

    assert len(result) == 2
    assert set(result).issubset({"BC001", "BC002", "BC003", "BC004"})


def test_queue_books_no_filtering():
    """filter_and_print_barcodes should process queue books when none already synced."""
    queue_books = {"QB001", "QB002", "QB003"}

    result = filter_and_print_barcodes(
        specific_barcodes=None,
        queue_books=queue_books,
        books_already_synced=set(),
        limit=None,
    )

    assert set(result) == queue_books


def test_queue_books_with_filtering():
    """filter_and_print_barcodes should filter out already synced books from queue."""
    queue_books = {"QB001", "QB002", "QB003", "QB004"}
    books_already_synced = {"QB002", "QB004"}

    result = filter_and_print_barcodes(
        specific_barcodes=None,
        queue_books=queue_books,
        books_already_synced=books_already_synced,
        limit=None,
    )

    assert set(result) == {"QB001", "QB003"}


def test_queue_books_with_limit():
    """filter_and_print_barcodes should apply limit after filtering."""
    queue_books = {"QB001", "QB002", "QB003", "QB004"}
    books_already_synced = {"QB002"}

    result = filter_and_print_barcodes(
        specific_barcodes=None,
        queue_books=queue_books,
        books_already_synced=books_already_synced,
        limit=2,
    )

    assert len(result) == 2
    assert set(result).issubset({"QB001", "QB003", "QB004"})


@pytest.mark.parametrize(
    "specific_barcodes,queue_books,queue_name",
    [
        (None, None, None),
        (None, set(), "unconverted"),
        ([], None, None),
    ],
)
def test_empty_source(specific_barcodes, queue_books, queue_name):
    """filter_and_print_barcodes should return empty list for empty sources."""
    result = filter_and_print_barcodes(
        specific_barcodes=specific_barcodes,
        queue_books=queue_books,
        books_already_synced=set(),
        limit=None,
        queue_name=queue_name,
    )

    assert result == []


def test_specific_barcodes_override_queue_books():
    """filter_and_print_barcodes should use specific_barcodes when both provided."""
    specific_barcodes = ["BC001", "BC002"]
    queue_books = {"QB001", "QB002", "QB003"}

    result = filter_and_print_barcodes(
        specific_barcodes=specific_barcodes,
        queue_books=queue_books,
        books_already_synced=set(),
        limit=None,
    )

    assert set(result) == {"BC001", "BC002"}


@pytest.mark.parametrize(
    "queue_books,already_synced,expected_remaining,queue_name",
    [
        ({"A", "B", "C"}, set(), 3, None),
        ({"A", "B", "C"}, {"A"}, 2, "unconverted"),
        ({"A", "B", "C"}, {"A", "B"}, 1, "previous"),
        ({"A", "B", "C"}, {"A", "B", "C"}, 0, "previous"),
    ],
)
def test_filtering_combinations(queue_books, already_synced, expected_remaining, queue_name):
    """filter_and_print_barcodes should correctly filter various combinations."""
    result = filter_and_print_barcodes(
        specific_barcodes=None,
        queue_books=queue_books,
        books_already_synced=already_synced,
        limit=None,
        queue_name=queue_name,
    )

    assert len(result) == expected_remaining
    assert set(result) == queue_books - already_synced


@pytest.mark.parametrize(
    "count,limit,expected_count",
    [
        (10, 3, 3),
        (10, 10, 10),
        (10, 15, 10),
        (5, 1, 1),
    ],
)
def test_limit_scenarios(count, limit, expected_count):
    """filter_and_print_barcodes should handle various limit scenarios."""
    queue_books = {f"BC{i:03d}" for i in range(count)}

    result = filter_and_print_barcodes(
        specific_barcodes=None,
        queue_books=queue_books,
        books_already_synced=set(),
        limit=limit,
    )

    assert len(result) == expected_count
