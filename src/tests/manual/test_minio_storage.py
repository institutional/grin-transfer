#!/usr/bin/env python3
"""
Test storage with MinIO backend
"""

import asyncio
import os
import sys
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from grin_to_s3.common import create_storage_from_config
from grin_to_s3.storage import BookStorage

# MinIO configuration (adjust if needed)
MINIO_CONFIG = {
    "endpoint_url": "http://localhost:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin123",  # Docker setup uses minioadmin123
}

BUCKET_NAME = "v2-test-bucket"


async def check_minio_connection():
    """Check if MinIO is running and accessible."""
    print("Checking MinIO connection...")

    try:
        storage = create_storage_from_config("minio", MINIO_CONFIG)

        # Try to list objects (this will fail if MinIO is down)
        await storage.list_objects("")
        print("✅ MinIO connection successful")
        return True

    except Exception as e:
        print(f"❌ MinIO connection failed: {e}")
        print("\nTroubleshooting:")
        print("1. Make sure MinIO is running:")
        print("   - Docker: docker-compose -f docker-compose.minio.yml up -d")
        print("   - Homebrew: minio server ~/minio-data --console-address :9001")
        print("2. Check MinIO is accessible at http://localhost:9000")
        print("3. Verify credentials (minioadmin/minioadmin or minioadmin/minioadmin123)")
        return False


async def test_minio_basic_operations():
    """Test basic MinIO operations."""
    print("\nTesting MinIO Basic Operations")
    print("=" * 32)

    storage = create_storage_from_config("minio", MINIO_CONFIG)

    # Test write and read
    test_path = f"{BUCKET_NAME}/test/hello.txt"
    test_content = "Hello from MinIO storage!"

    await storage.write_text(test_path, test_content)
    print("✅ File written to MinIO")

    # Check if exists
    exists = await storage.exists(test_path)
    assert exists, "File should exist"
    print("✅ File existence check")

    # Read back
    read_content = await storage.read_text(test_path)
    assert read_content == test_content, "Content should match"
    print("✅ File read from MinIO")

    # Test binary data
    binary_data = b"Binary data test: \x00\x01\x02\xff"
    binary_path = f"{BUCKET_NAME}/test/binary.dat"

    await storage.write_bytes(binary_path, binary_data)
    read_binary = await storage.read_bytes(binary_path)
    assert read_binary == binary_data, "Binary data should match"
    print("✅ Binary data operations")

    # Test metadata
    info = await storage.get_info(test_path)
    print(f"✅ File metadata: {info.get('size', 'unknown')} bytes")

    # Test listing
    objects = await storage.list_objects(f"{BUCKET_NAME}/test/")
    assert len(objects) >= 2, "Should list uploaded files"
    print(f"✅ Object listing: {len(objects)} objects")

    # Cleanup
    await storage.delete(test_path)
    await storage.delete(binary_path)
    print("✅ Cleanup completed")


async def test_minio_book_storage():
    """Test book storage operations with MinIO."""
    print("\nTesting MinIO Book Storage")
    print("=" * 26)

    storage = create_storage_from_config("minio", MINIO_CONFIG)
    book_storage = BookStorage(storage, base_prefix=f"{BUCKET_NAME}/books")

    barcode = "MINIO_TEST_456"

    # Test archive storage
    fake_archive = b"This is fake encrypted archive data for MinIO testing " * 100
    archive_path = await book_storage.save_archive(barcode, fake_archive)
    print(f"✅ Archive saved: {archive_path}")

    # Check archive exists
    exists = await book_storage.archive_exists(barcode)
    assert exists, "Archive should exist"
    print("✅ Archive existence verified")

    # Retrieve archive
    retrieved_archive = await book_storage.get_archive(barcode)
    assert retrieved_archive == fake_archive, "Archive data should match"
    print("✅ Archive retrieval successful")

    # Test JSON text storage
    pages = ["This is page 1 of the test book", "Page 2 contains more sample text", "Final page 3 with unicode: 📚📖✨"]

    json_path = await book_storage.save_text_json(barcode, pages)
    print(f"✅ JSON text saved: {json_path}")

    # Test timestamp
    timestamp_path = await book_storage.save_timestamp(barcode)
    print(f"✅ Timestamp saved: {timestamp_path}")

    # Test streaming download
    print("Testing streaming download...")
    chunks = []
    chunk_count = 0

    async for chunk in book_storage.stream_archive_download(barcode):
        chunks.append(chunk)
        chunk_count += 1

    streamed_data = b"".join(chunks)
    assert streamed_data == fake_archive, "Streamed data should match original"
    print(f"✅ Streaming download: {chunk_count} chunks, {len(streamed_data)} bytes")

    print("✅ MinIO book storage tests passed")


async def test_minio_performance():
    """Test MinIO performance with larger files."""
    print("\nTesting MinIO Performance")
    print("=" * 24)

    storage = create_storage_from_config("minio", MINIO_CONFIG)

    # Create larger test file (1MB)
    large_data = b"Performance test data " * 50000  # ~1MB
    large_path = f"{BUCKET_NAME}/performance/large_file.dat"

    # Time the upload
    start_time = time.time()
    await storage.write_bytes(large_path, large_data)
    upload_time = time.time() - start_time

    print(f"✅ Upload: {len(large_data):,} bytes in {upload_time:.2f}s")
    print(f"   Upload speed: {len(large_data) / upload_time / 1024 / 1024:.1f} MB/s")

    # Time the download
    start_time = time.time()
    downloaded_data = await storage.read_bytes(large_path)
    download_time = time.time() - start_time

    assert downloaded_data == large_data, "Downloaded data should match"
    print(f"✅ Download: {len(downloaded_data):,} bytes in {download_time:.2f}s")
    print(f"   Download speed: {len(downloaded_data) / download_time / 1024 / 1024:.1f} MB/s")

    # Cleanup
    await storage.delete(large_path)
    print("✅ Performance test completed")


async def demo_integration_with_download():
    """Demo how download_single.py could integrate with MinIO."""
    print("\nDemonstrating Download Integration")
    print("=" * 34)

    storage = create_storage_from_config("minio", MINIO_CONFIG)
    book_storage = BookStorage(storage, base_prefix=f"{BUCKET_NAME}/downloads")

    # Simulate download_single.py workflow
    barcode = "INTEGRATION_TEST"

    print("Simulating download_single.py workflow:")

    # Step 1: Download from GRIN (simulated)
    print("1. Downloading from GRIN... (simulated)")
    simulated_grin_data = b"Fake GRIN archive data " * 1000

    # Step 2: Save to MinIO instead of local filesystem
    print("2. Saving to MinIO storage...")
    archive_path = await book_storage.save_archive(barcode, simulated_grin_data)

    # Step 3: Add metadata
    print("3. Saving metadata...")
    await book_storage.save_timestamp(barcode)

    # Step 4: Verify
    print("4. Verifying storage...")
    assert await book_storage.archive_exists(barcode), "Archive should exist in MinIO"

    file_size = len(simulated_grin_data)
    print("✅ Integration demo complete:")
    print(f"   File: {archive_path}")
    print(f"   Size: {file_size:,} bytes")
    print("   Storage: MinIO (S3-compatible)")

    print("\nThis shows how the system can easily switch between:")
    print("- Local filesystem (development)")
    print("- MinIO (local S3-compatible)")
    print("- AWS S3 (production)")
    print("- Cloudflare R2 (cost-effective)")


async def main():
    """Run all MinIO tests."""
    print("MinIO Storage Testing")
    print("=" * 24)

    # Check connection first
    if not await check_minio_connection():
        return

    try:
        await test_minio_basic_operations()
        await test_minio_book_storage()
        await test_minio_performance()
        await demo_integration_with_download()

        print("\n🎉 All MinIO tests passed!")
        print("\nMinIO Console: http://localhost:9001")
        print("Username: minioadmin")
        print("Password: minioadmin (or minioadmin123)")

    except Exception as e:
        print(f"\n❌ MinIO test failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
