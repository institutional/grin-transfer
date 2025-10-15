#!/usr/bin/env python3
"""
Tests for storage calculation utilities.

Tests pure calculation functions for part sizing and timeout calculation
that are used in multipart upload operations.
"""

import pytest

from grin_transfer.storage.base import BackendConfig, Storage

# Constants for readability
MB = 1024 * 1024
GB = 1024 * MB


class TestPartSizeCalculation:
    """Test multipart upload part size calculation."""

    @pytest.mark.parametrize(
        "file_size,expected_part_size",
        [
            # Small files (< 100MB) -> 10MB parts
            (10 * MB, 10 * MB),
            (50 * MB, 10 * MB),
            (99 * MB, 10 * MB),
            # Medium files (100MB - 1GB) -> 16MB parts
            (100 * MB, 16 * MB),
            (500 * MB, 16 * MB),
            (999 * MB, 16 * MB),
            # Large files (1GB - <5GB) -> 32MB parts
            (1 * GB, 32 * MB),
            (3 * GB, 32 * MB),
            ((5 * GB) - 1, 32 * MB),  # Just under 5GB still gets 32MB
            # Very large files (>=5GB) -> target ~100 parts, capped at 100MB
            (5 * GB, 53687091),  # 5GB // 100 = 53687091 bytes
            (6 * GB, 64424509),  # 6GB // 100 = 64424509 bytes
            (10 * GB, 100 * MB),  # 10GB // 100 = ~107MB, but capped at 100MB
            (50 * GB, 100 * MB),  # 50GB // 100 = ~536MB, but capped at 100MB
        ],
    )
    def test_calculate_part_size(self, file_size, expected_part_size):
        """Part size should scale appropriately with file size."""
        storage = Storage(BackendConfig(protocol="s3"))
        assert storage._calculate_part_size(file_size) == expected_part_size


class TestUploadTimeout:
    """Test upload timeout calculation."""

    @pytest.mark.parametrize(
        "part_size,expected_timeout",
        [
            # Base timeout: 480s + (part_size_mb * 2s/MB)
            (10 * MB, 500),  # 480 + 10MB * 2s/MB = 500s
            (16 * MB, 512),  # 480 + 16MB * 2s/MB = 512s
            (32 * MB, 544),  # 480 + 32MB * 2s/MB = 544s
            (100 * MB, 680),  # 480 + 100MB * 2s/MB = 680s
            # Edge case: very small part (rounds up to 1MB minimum)
            (100 * 1024, 482),  # 480 + 1MB * 2s/MB = 482s
        ],
    )
    def test_calculate_upload_timeout(self, part_size, expected_timeout):
        """Upload timeout should scale with part size (480s base + 2s per MB)."""
        storage = Storage(BackendConfig(protocol="s3"))
        assert storage._calculate_upload_timeout(part_size) == expected_timeout
