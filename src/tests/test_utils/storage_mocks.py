"""Storage mocking utilities for tests."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock


def create_mock_storage(s3_compatible: bool = False, should_fail: bool = False) -> MagicMock:
    """Create a fully configured mock storage."""
    mock_storage = MagicMock()
    mock_storage.is_s3_compatible = MagicMock(return_value=s3_compatible)

    if should_fail:
        mock_storage.write_file = AsyncMock(side_effect=Exception("Storage write failed"))
        mock_storage.write_bytes = AsyncMock(side_effect=Exception("Storage write failed"))
        mock_storage.write_text = AsyncMock(side_effect=Exception("Storage write failed"))
    else:
        mock_storage.write_file = AsyncMock(return_value=None)
        mock_storage.write_bytes = AsyncMock(return_value=None)
        mock_storage.write_text = AsyncMock(return_value=None)
        mock_storage.write_bytes_with_metadata = AsyncMock(return_value="test-path")

    mock_storage.read_bytes = AsyncMock(return_value=b"test data")
    mock_storage.read_text = AsyncMock(return_value="test content")
    mock_storage.exists = AsyncMock(return_value=True)
    mock_storage.delete = AsyncMock(return_value=None)

    return mock_storage


def create_mock_book_storage(bucket_config: dict = None, should_fail: bool = False, base_prefix: str = "") -> MagicMock:
    """Create a fully configured mock BookStorage."""
    if bucket_config is None:
        bucket_config = {
            "bucket_raw": "test-raw",
            "bucket_meta": "test-meta",
            "bucket_full": "test-full"
        }

    mock_book_storage = MagicMock()
    mock_book_storage.bucket_raw = bucket_config["bucket_raw"]
    mock_book_storage.bucket_meta = bucket_config["bucket_meta"]
    mock_book_storage.bucket_full = bucket_config["bucket_full"]
    mock_book_storage.base_prefix = base_prefix

    # Add path generation methods
    def meta_path(filename: str) -> str:
        if base_prefix:
            return f"{bucket_config['bucket_meta']}/{base_prefix}/{filename}"
        return f"{bucket_config['bucket_meta']}/{filename}"

    mock_book_storage._meta_path = meta_path

    if should_fail:
        mock_book_storage.save_decrypted_archive_from_file = AsyncMock(
            side_effect=Exception("Storage upload failed")
        )
        mock_book_storage.save_ocr_text_jsonl_from_file = AsyncMock(
            side_effect=Exception("Storage upload failed")
        )
    else:
        mock_book_storage.save_decrypted_archive_from_file = AsyncMock(
            return_value="test-raw/TEST123/TEST123.tar.gz"
        )
        mock_book_storage.save_ocr_text_jsonl_from_file = AsyncMock(
            return_value="test-full/TEST123.jsonl"
        )

    mock_book_storage.save_timestamp = AsyncMock(return_value="test-raw/TEST123/TEST123.tar.gz.gpg.retrieval")
    mock_book_storage.archive_exists = AsyncMock(return_value=False)
    mock_book_storage.upload_csv_file = AsyncMock(return_value=("latest.csv", "timestamped.csv"))

    return mock_book_storage


def create_mock_staging_manager(staging_dir: str = "/tmp/staging") -> MagicMock:
    """Create a fully configured mock staging manager."""
    mock_staging = MagicMock()
    staging_path = Path(staging_dir)
    mock_staging.staging_path = staging_path
    mock_staging.staging_dir = staging_path
    mock_staging.get_staging_path = MagicMock(return_value=staging_path / "test_file")
    mock_staging.get_decrypted_file_path = lambda barcode: staging_path / f"{barcode}.tar.gz"
    mock_staging.cleanup_file = AsyncMock(return_value=1024)
    mock_staging.cleanup_files = MagicMock(return_value=1024 * 1024)
    mock_staging.available_space = MagicMock(return_value=10 * 1024 * 1024 * 1024)  # 10GB
    return mock_staging


def standard_bucket_config() -> dict:
    """Standard bucket configuration for tests."""
    return {
        "bucket_raw": "test-raw",
        "bucket_meta": "test-meta",
        "bucket_full": "test-full"
    }
