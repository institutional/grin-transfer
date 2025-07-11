#!/usr/bin/env python3
"""
Test storage abstraction layer
"""

import asyncio
import json
import os
import sys
import tempfile

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from grin_to_s3.storage import BookManager, StorageConfig, StorageNotFoundError, create_storage_from_config


async def test_local_storage():
    """Test local filesystem storage."""
    print("Testing Local Storage")
    print("=" * 20)

    with tempfile.TemporaryDirectory() as temp_dir:
        storage = create_storage_from_config("local", {"base_path": temp_dir})

        # Test basic operations
        test_path = "test/hello.txt"
        test_content = "Hello, Storage!"

        # Write and read text
        await storage.write_text(test_path, test_content)
        assert await storage.exists(test_path), "File should exist"

        read_content = await storage.read_text(test_path)
        assert read_content == test_content, "Content should match"

        # Write and read bytes
        test_bytes = b"Hello, bytes!"
        bytes_path = "test/data.bin"
        await storage.write_bytes(bytes_path, test_bytes)

        read_bytes = await storage.read_bytes(bytes_path)
        assert read_bytes == test_bytes, "Bytes should match"

        # Test detailed listing for metadata
        detailed_objects = await storage.list_objects_detailed("test/")
        assert len(detailed_objects) >= 2, "Should list files with metadata"

        # Test listing
        objects = await storage.list_objects("test/")
        assert len(objects) >= 2, "Should list files"

        print("✅ Basic operations working")

        # Test error handling
        try:
            await storage.read_text("nonexistent.txt")
            raise AssertionError("Should raise exception")
        except StorageNotFoundError:
            print("✅ Error handling working")

        # Test deletion
        await storage.delete(test_path)
        assert not await storage.exists(test_path), "File should be deleted"

        print("✅ Local storage tests passed\n")


async def test_book_storage():
    """Test book-specific storage operations."""
    print("Testing Book Storage")
    print("=" * 20)

    with tempfile.TemporaryDirectory() as temp_dir:
        storage = create_storage_from_config("local", {"base_path": temp_dir})
        book_storage = BookManager(storage, base_prefix="books")

        barcode = "TEST123"

        # Test archive operations
        archive_data = b"Fake encrypted archive data"
        archive_path = await book_storage.save_archive(barcode, archive_data)
        print(f"✅ Saved archive: {archive_path}")

        assert await book_storage.archive_exists(barcode), "Archive should exist"

        retrieved_data = await book_storage.get_archive(barcode)
        assert retrieved_data == archive_data, "Archive data should match"

        # Test JSON text storage
        pages = ["Page 1 text", "Page 2 text", "Page 3 text"]
        json_path = await book_storage.save_text_jsonl(barcode, pages)
        print(f"✅ Saved JSON: {json_path}")

        # Verify JSON content
        json_content = await storage.read_text(json_path)
        loaded_pages = json.loads(json_content)
        assert loaded_pages == pages, "Pages should match"

        # Test timestamp
        timestamp_path = await book_storage.save_timestamp(barcode)
        print(f"✅ Saved timestamp: {timestamp_path}")

        # Test streaming download
        chunks = []
        async for chunk in book_storage.stream_archive_download(barcode):
            chunks.append(chunk)

        streamed_data = b"".join(chunks)
        assert streamed_data == archive_data, "Streamed data should match"

        print("✅ Book storage tests passed\n")


async def test_storage_configs():
    """Test different storage configurations."""
    print("Testing Storage Configurations")
    print("=" * 30)

    # Test S3 config
    s3_config = StorageConfig.s3(bucket="test-bucket", key="test-key", secret="test-secret")
    assert s3_config.protocol == "s3", "S3 protocol should be correct"
    assert "key" in s3_config.options, "Should have key option"
    print("✅ S3 config")

    # Test R2 config
    r2_config = StorageConfig.r2(account_id="test", access_key="key", secret_key="secret")
    assert r2_config.protocol == "s3", "R2 should use S3 protocol"
    assert "r2.cloudflarestorage.com" in r2_config.endpoint_url, "Should have R2 endpoint"
    print("✅ R2 config")

    # Test GCS config
    gcs_config = StorageConfig.gcs(project="test-project")
    assert gcs_config.protocol == "gcs", "GCS protocol should be correct"
    assert "project" in gcs_config.options, "Should have project"
    print("✅ GCS config")

    # Test MinIO config
    minio_config = StorageConfig.minio(
        endpoint_url="http://localhost:9000", access_key="minioadmin", secret_key="minioadmin"
    )
    assert minio_config.protocol == "s3", "MinIO should use S3 protocol"
    assert minio_config.endpoint_url == "http://localhost:9000", "Should have endpoint"
    print("✅ MinIO config")

    print("✅ Configuration tests passed\n")


async def test_integration_example():
    """Test integration with download_single.py pattern."""
    print("Testing Integration Example")
    print("=" * 26)

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create storage for downloaded files
        storage = create_storage_from_config("local", {"base_path": temp_dir})
        book_storage = BookManager(storage, base_prefix="downloads")

        # Simulate download_single.py workflow
        barcode = "EXAMPLE456"

        # Step 1: Download archive (simulated)
        fake_archive = b"This would be real GRIN archive data from download_single.py"

        # Step 2: Save to storage
        archive_path = await book_storage.save_archive(barcode, fake_archive)
        print(f"✅ Archive saved: {archive_path}")

        # Step 3: Save timestamp
        timestamp_path = await book_storage.save_timestamp(barcode)
        print(f"✅ Timestamp saved: {timestamp_path}")

        # Step 4: Verify storage
        assert await book_storage.archive_exists(barcode), "Archive should exist"
        print("✅ Storage verified")

        # This shows how download_single.py could be modified to use storage abstraction
        print("✅ Integration example complete\n")


async def main():
    """Run all storage tests."""
    print("Storage Abstraction Tests")
    print("=" * 40)

    try:
        await test_storage_configs()
        await test_local_storage()
        await test_book_storage()
        await test_integration_example()

        print("🎉 All storage tests passed!")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
