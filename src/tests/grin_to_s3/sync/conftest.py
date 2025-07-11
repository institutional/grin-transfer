#!/usr/bin/env python3
"""
Shared test fixtures for sync module testing.
"""


import pytest

from grin_to_s3.sync.models import create_sync_stats


@pytest.fixture
def sync_stats():
    """Create sync statistics for testing."""
    return create_sync_stats()


@pytest.fixture
def test_barcodes():
    """Sample barcodes for testing."""
    return ["TEST123", "TEST456", "TEST789"]


@pytest.fixture
def invalid_barcodes():
    """Invalid barcodes for testing validation."""
    return [
        "",  # Empty
        "ab",  # Too short
        "a" * 51,  # Too long
        "test@book",  # Invalid characters
        "test book",  # Space not allowed
    ]
