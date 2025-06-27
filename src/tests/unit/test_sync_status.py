#!/usr/bin/env python3
"""
Tests for sync status operations.
"""

import sqlite3
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.sync.status import (
    export_sync_status_csv,
    get_sync_statistics,
    show_sync_status,
    validate_database_file,
)


class TestValidateDatabaseFile:
    """Test database file validation."""

    def test_validate_nonexistent_database(self, tmp_path):
        """Test validation of non-existent database file."""
        db_path = str(tmp_path / "nonexistent.db")

        with pytest.raises(SystemExit):
            validate_database_file(db_path)

    def test_validate_missing_tables(self, tmp_path):
        """Test validation of database with missing tables."""
        db_path = str(tmp_path / "incomplete.db")

        # Create database with wrong tables
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE wrong_table (id INTEGER)")
        conn.commit()
        conn.close()

        with pytest.raises(SystemExit):
            validate_database_file(db_path)

    def test_validate_correct_database(self, temp_db_path):
        """Test validation of correct database."""
        # The temp_db_path fixture creates a database with required tables
        # This should pass without raising an exception
        validate_database_file(temp_db_path)

    def test_validate_corrupted_database(self, tmp_path):
        """Test validation of corrupted database file."""
        db_path = str(tmp_path / "corrupted.db")

        # Create a file that's not a valid SQLite database
        with open(db_path, 'w') as f:
            f.write("This is not a database")

        with pytest.raises(SystemExit):
            validate_database_file(db_path)


class TestSyncStatistics:
    """Test sync statistics functionality."""

    @pytest.mark.asyncio
    async def test_get_sync_statistics(self, temp_db_path):
        """Test getting sync statistics."""
        with patch('grin_to_s3.sync.status.SQLiteProgressTracker') as mock_tracker_class:
            mock_tracker = MagicMock()
            mock_tracker_class.return_value = mock_tracker

            # Mock the various count methods
            mock_tracker.get_book_count = AsyncMock(return_value=100)
            mock_tracker.get_enriched_book_count = AsyncMock(return_value=80)
            mock_tracker.get_converted_books_count = AsyncMock(return_value=50)
            mock_tracker.get_sync_stats = AsyncMock(return_value={
                "total_converted": 50,
                "synced": 30,
                "failed": 5,
                "pending": 15,
                "syncing": 0,
                "decrypted": 25,
            })
            mock_tracker._db = MagicMock()
            mock_tracker._db.close = AsyncMock()

            stats = await get_sync_statistics(temp_db_path)

            assert stats["total_books"] == 100
            assert stats["enriched_books"] == 80
            assert stats["converted_books"] == 50
            assert stats["total_converted"] == 50
            assert stats["synced"] == 30
            assert stats["failed"] == 5
            assert stats["pending"] == 15

    @pytest.mark.asyncio
    async def test_get_sync_statistics_with_storage_filter(self, temp_db_path):
        """Test getting sync statistics with storage type filter."""
        with patch('grin_to_s3.sync.status.SQLiteProgressTracker') as mock_tracker_class:
            mock_tracker = MagicMock()
            mock_tracker_class.return_value = mock_tracker

            mock_tracker.get_book_count = AsyncMock(return_value=100)
            mock_tracker.get_enriched_book_count = AsyncMock(return_value=80)
            mock_tracker.get_converted_books_count = AsyncMock(return_value=50)
            mock_tracker.get_sync_stats = AsyncMock(return_value={
                "total_converted": 25,
                "synced": 20,
                "failed": 2,
                "pending": 3,
                "syncing": 0,
                "decrypted": 18,
            })
            mock_tracker._db = MagicMock()
            mock_tracker._db.close = AsyncMock()

            stats = await get_sync_statistics(temp_db_path, "minio")

            # Should call get_sync_stats with storage type filter
            mock_tracker.get_sync_stats.assert_called_once_with("minio")
            assert stats["synced"] == 20


class TestShowSyncStatus:
    """Test sync status display functionality."""

    @pytest.mark.asyncio
    async def test_show_sync_status_nonexistent_db(self, capsys):
        """Test show_sync_status with non-existent database."""
        await show_sync_status("/nonexistent/path.db")

        captured = capsys.readouterr()
        assert "❌ Error: Database file does not exist" in captured.out

    @pytest.mark.asyncio
    async def test_show_sync_status_success(self, temp_db_path, capsys):
        """Test successful sync status display."""
        with patch('grin_to_s3.sync.status.SQLiteProgressTracker') as mock_tracker_class, \
             patch('aiosqlite.connect') as mock_connect:

            mock_tracker = MagicMock()
            mock_tracker_class.return_value = mock_tracker

            # Mock tracker methods
            mock_tracker.get_book_count = AsyncMock(return_value=100)
            mock_tracker.get_enriched_book_count = AsyncMock(return_value=80)
            mock_tracker.get_converted_books_count = AsyncMock(return_value=50)
            mock_tracker.get_sync_stats = AsyncMock(return_value={
                "total_converted": 50,
                "synced": 30,
                "failed": 5,
                "pending": 15,
                "syncing": 0,
                "decrypted": 25,
            })
            mock_tracker._db = MagicMock()
            mock_tracker._db.close = AsyncMock()

            # Mock database connection for storage breakdown and recent activity
            mock_db = MagicMock()
            mock_connect.return_value.__aenter__.return_value = mock_db

            # Mock storage breakdown query
            mock_cursor1 = MagicMock()
            mock_cursor1.fetchall = AsyncMock(return_value=[
                ("minio", "test-bucket/book1", 10),
                ("r2", "prod-bucket/book2", 20),
            ])

            # Mock recent activity query
            mock_cursor2 = MagicMock()
            mock_cursor2.fetchall = AsyncMock(return_value=[
                ("TEST123", "completed", "2024-01-01T10:00:00", None, "minio"),
                ("TEST456", "failed", "2024-01-01T09:00:00", "Network error", "r2"),
            ])

            mock_db.execute = AsyncMock(side_effect=[mock_cursor1, mock_cursor2])

            await show_sync_status(temp_db_path)

            captured = capsys.readouterr()
            assert "Sync Status Report" in captured.out
            assert "Total books in database: 100" in captured.out
            assert "Successfully synced: 30" in captured.out
            assert "Storage Type Breakdown:" in captured.out
            assert "Recent Sync Activity" in captured.out


class TestExportSyncStatusCsv:
    """Test CSV export functionality."""

    @pytest.mark.asyncio
    async def test_export_sync_status_csv(self, temp_db_path, tmp_path, capsys):
        """Test exporting sync status to CSV."""
        output_path = str(tmp_path / "sync_status.csv")

        with patch('aiosqlite.connect') as mock_connect:
            mock_db = MagicMock()
            mock_connect.return_value.__aenter__.return_value = mock_db

            mock_cursor = MagicMock()
            mock_cursor.fetchall = AsyncMock(return_value=[
                ("TEST123", "minio", "bucket/TEST123.tar.gz", "bucket/TEST123.tar.gz",
                 True, "2024-01-01T10:00:00", None, "completed"),
                ("TEST456", "r2", "bucket/TEST456.tar.gz", "bucket/TEST456.tar.gz",
                 False, "2024-01-01T09:00:00", "Upload failed", "failed"),
            ])
            mock_db.execute = AsyncMock(return_value=mock_cursor)

            await export_sync_status_csv(temp_db_path, output_path)

            # Check that CSV file was created
            assert Path(output_path).exists()

            # Check console output
            captured = capsys.readouterr()
            assert f"Sync status exported to: {output_path}" in captured.out

            # Read and verify CSV content
            with open(output_path) as f:
                content = f.read()
                assert "barcode,storage_type,storage_path" in content
                assert "TEST123,minio" in content
                assert "TEST456,r2" in content

    @pytest.mark.asyncio
    async def test_export_sync_status_csv_with_filter(self, temp_db_path, tmp_path):
        """Test exporting sync status to CSV with storage type filter."""
        output_path = str(tmp_path / "sync_status_filtered.csv")

        with patch('aiosqlite.connect') as mock_connect:
            mock_db = MagicMock()
            mock_connect.return_value.__aenter__.return_value = mock_db

            mock_cursor = MagicMock()
            mock_cursor.fetchall = AsyncMock(return_value=[
                ("TEST123", "minio", "bucket/TEST123.tar.gz", "bucket/TEST123.tar.gz",
                 True, "2024-01-01T10:00:00", None, "completed"),
            ])
            mock_db.execute = AsyncMock(return_value=mock_cursor)

            await export_sync_status_csv(temp_db_path, output_path, "minio")

            # Verify that the query was called with storage type filter
            call_args = mock_db.execute.call_args
            query = call_args[0][0]
            params = call_args[0][1]

            assert "AND b.storage_type = ?" in query
            assert "minio" in params
