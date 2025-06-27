#!/usr/bin/env python3
"""
Tests for sync catchup operations.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.sync.catchup import (
    confirm_catchup_sync,
    find_catchup_books,
    get_books_for_catchup_sync,
    mark_books_for_catchup_processing,
    run_catchup_validation,
    show_catchup_dry_run,
)


class TestConfirmCatchupSync:
    """Test user confirmation for catchup sync."""

    def test_confirm_catchup_sync_auto_confirm(self):
        """Test auto-confirmation mode."""
        result = confirm_catchup_sync(["TEST123", "TEST456"], auto_confirm=True)
        assert result is True

    def test_confirm_catchup_sync_user_yes(self, monkeypatch):
        """Test user confirmation with 'yes' response."""
        monkeypatch.setattr('builtins.input', lambda _: 'y')
        result = confirm_catchup_sync(["TEST123", "TEST456"], auto_confirm=False)
        assert result is True

    def test_confirm_catchup_sync_user_no(self, monkeypatch, capsys):
        """Test user confirmation with 'no' response."""
        monkeypatch.setattr('builtins.input', lambda _: 'n')
        result = confirm_catchup_sync(["TEST123", "TEST456"], auto_confirm=False)
        assert result is False

        captured = capsys.readouterr()
        assert "Catchup cancelled" in captured.out

    def test_confirm_catchup_sync_user_full_yes(self, monkeypatch):
        """Test user confirmation with 'yes' full word response."""
        monkeypatch.setattr('builtins.input', lambda _: 'yes')
        result = confirm_catchup_sync(["TEST123"], auto_confirm=False)
        assert result is True


class TestShowCatchupDryRun:
    """Test dry run display functionality."""

    def test_show_catchup_dry_run_small_list(self, capsys):
        """Test dry run display for small list of books."""
        books = ["TEST123", "TEST456", "TEST789"]
        show_catchup_dry_run(books)

        captured = capsys.readouterr()
        assert "📋 DRY RUN: Would sync 3 books" in captured.out
        assert "Books that would be synced:" in captured.out
        assert "1. TEST123" in captured.out
        assert "2. TEST456" in captured.out
        assert "3. TEST789" in captured.out

    def test_show_catchup_dry_run_large_list(self, capsys):
        """Test dry run display for large list of books."""
        books = [f"TEST{i:03d}" for i in range(25)]  # 25 books
        show_catchup_dry_run(books)

        captured = capsys.readouterr()
        assert "📋 DRY RUN: Would sync 25 books" in captured.out
        assert "First 10 books that would be synced:" in captured.out
        assert "Last 10 books that would be synced:" in captured.out
        assert "... and 15 more books" in captured.out
        assert "1. TEST000" in captured.out
        assert "16. TEST015" in captured.out  # Should show as position 16 in last 10

    def test_show_catchup_dry_run_empty_list(self, capsys):
        """Test dry run display for empty list."""
        show_catchup_dry_run([])

        captured = capsys.readouterr()
        assert "📋 DRY RUN: Would sync 0 books" in captured.out


class TestFindCatchupBooks:
    """Test finding books available for catchup."""

    @pytest.mark.asyncio
    async def test_find_catchup_books_success(self, temp_db_path, capsys):
        """Test successful finding of catchup books."""
        with patch('grin_to_s3.sync.catchup.get_converted_books') as mock_get_converted, \
             patch('grin_to_s3.sync.catchup.GRINClient') as mock_client_class, \
             patch('grin_to_s3.sync.catchup.SQLiteProgressTracker') as mock_tracker_class, \
             patch('aiosqlite.connect') as mock_connect:

            # Mock GRIN client
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.session = MagicMock()
            mock_client.session.close = AsyncMock()

            # Mock converted books from GRIN
            mock_get_converted.return_value = {"TEST123", "TEST456", "TEST789"}

            # Mock database tracker
            mock_tracker = MagicMock()
            mock_tracker_class.return_value = mock_tracker
            mock_tracker.init_db = AsyncMock()

            # Mock database connection
            mock_db = MagicMock()
            mock_connect.return_value.__aenter__.return_value = mock_db
            mock_cursor = MagicMock()
            mock_cursor.fetchall = AsyncMock(return_value=[
                ("TEST123",), ("TEST456",), ("TEST999",)  # Third book not converted
            ])
            mock_db.execute = AsyncMock(return_value=mock_cursor)

            converted, all_books, candidates = await find_catchup_books(
                temp_db_path, "Harvard", "/secrets"
            )

            assert converted == {"TEST123", "TEST456", "TEST789"}
            assert all_books == {"TEST123", "TEST456", "TEST999"}
            assert candidates == {"TEST123", "TEST456"}  # Intersection

            captured = capsys.readouterr()
            assert "GRIN reports 3 total converted books available" in captured.out
            assert "Database contains 3 books" in captured.out
            assert "Found 2 books that are converted and in our database" in captured.out

    @pytest.mark.asyncio
    async def test_find_catchup_books_grin_error(self, temp_db_path):
        """Test handling of GRIN API error."""
        with patch('grin_to_s3.sync.catchup.get_converted_books') as mock_get_converted, \
             patch('grin_to_s3.sync.catchup.GRINClient') as mock_client_class, \
             pytest.raises(SystemExit):

            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.session = MagicMock()
            mock_client.session.close = AsyncMock()

            # Mock GRIN error
            mock_get_converted.side_effect = Exception("Network error")

            await find_catchup_books(temp_db_path, "Harvard", "/secrets")


class TestGetBooksForCatchupSync:
    """Test getting books that need catchup sync."""

    @pytest.mark.asyncio
    async def test_get_books_for_catchup_sync(self, temp_db_path, capsys):
        """Test getting books for catchup sync."""
        with patch('grin_to_s3.sync.catchup.SQLiteProgressTracker') as mock_tracker_class:
            mock_tracker = MagicMock()
            mock_tracker_class.return_value = mock_tracker

            # Mock books that need sync
            mock_tracker.get_books_for_sync = AsyncMock(return_value=[
                "TEST123", "TEST456", "TEST789"
            ])

            candidates = {"TEST123", "TEST456", "TEST789", "TEST999"}
            books = await get_books_for_catchup_sync(
                temp_db_path, "minio", candidates, limit=None
            )

            assert books == ["TEST123", "TEST456", "TEST789"]

            captured = capsys.readouterr()
            assert "Found 3 books ready for catchup sync" in captured.out

    @pytest.mark.asyncio
    async def test_get_books_for_catchup_sync_with_limit(self, temp_db_path, capsys):
        """Test getting books for catchup sync with limit."""
        with patch('grin_to_s3.sync.catchup.SQLiteProgressTracker') as mock_tracker_class:
            mock_tracker = MagicMock()
            mock_tracker_class.return_value = mock_tracker

            mock_tracker.get_books_for_sync = AsyncMock(return_value=[
                "TEST123", "TEST456", "TEST789", "TEST999", "TEST000"
            ])

            candidates = {"TEST123", "TEST456", "TEST789", "TEST999", "TEST000"}
            books = await get_books_for_catchup_sync(
                temp_db_path, "minio", candidates, limit=3
            )

            assert books == ["TEST123", "TEST456", "TEST789"]

            captured = capsys.readouterr()
            assert "Found 5 books ready for catchup sync" in captured.out
            assert "Limited to 3 books for this catchup run" in captured.out

    @pytest.mark.asyncio
    async def test_get_books_for_catchup_sync_none_available(self, temp_db_path, capsys):
        """Test getting books when none are available for sync."""
        with patch('grin_to_s3.sync.catchup.SQLiteProgressTracker') as mock_tracker_class:
            mock_tracker = MagicMock()
            mock_tracker_class.return_value = mock_tracker

            mock_tracker.get_books_for_sync = AsyncMock(return_value=[])

            candidates = {"TEST123", "TEST456"}
            books = await get_books_for_catchup_sync(
                temp_db_path, "minio", candidates, limit=None
            )

            assert books == []

            captured = capsys.readouterr()
            assert "Found 0 books ready for catchup sync" in captured.out
            assert "All converted books are already synced" in captured.out


class TestMarkBooksForCatchupProcessing:
    """Test marking books for catchup processing."""

    @pytest.mark.asyncio
    async def test_mark_books_for_catchup_processing(self, temp_db_path):
        """Test marking books for catchup processing."""
        with patch('grin_to_s3.sync.catchup.SQLiteProgressTracker') as mock_tracker_class:
            mock_tracker = MagicMock()
            mock_tracker_class.return_value = mock_tracker
            mock_tracker.add_status_change = AsyncMock()

            books = ["TEST123", "TEST456"]
            timestamp = "20240101_120000"

            await mark_books_for_catchup_processing(temp_db_path, books, timestamp)

            # Verify that add_status_change was called for each book
            assert mock_tracker.add_status_change.call_count == 2

            # Check the calls
            calls = mock_tracker.add_status_change.call_args_list
            assert calls[0][0] == ("TEST123", "processing_request", "converted")
            assert calls[1][0] == ("TEST456", "processing_request", "converted")

            # Check session_id and metadata
            assert calls[0][1]["session_id"] == "catchup_20240101_120000"
            assert calls[0][1]["metadata"]["source"] == "catchup"


class TestRunCatchupValidation:
    """Test catchup configuration validation."""

    @pytest.mark.asyncio
    async def test_run_catchup_validation_success(self, capsys):
        """Test successful catchup validation."""
        run_config = {
            "library_directory": "Harvard",
            "secrets_dir": "/secrets"
        }
        storage_type = "minio"
        storage_config = {
            "bucket_raw": "test-bucket",
            "endpoint_url": "localhost:9000"
        }

        result_type, result_config = await run_catchup_validation(
            run_config, storage_type, storage_config
        )

        assert result_type == "minio"
        assert result_config == storage_config

        captured = capsys.readouterr()
        assert "Using storage: minio" in captured.out
        assert "Target bucket: test-bucket" in captured.out

    @pytest.mark.asyncio
    async def test_run_catchup_validation_local_storage(self, capsys):
        """Test catchup validation for local storage."""
        run_config = {
            "library_directory": "Harvard",
            "secrets_dir": "/secrets"
        }
        storage_type = "local"
        storage_config = {
            "base_path": "/tmp/storage"
        }

        result_type, result_config = await run_catchup_validation(
            run_config, storage_type, storage_config
        )

        assert result_type == "local"
        assert result_config == storage_config

        captured = capsys.readouterr()
        assert "Using storage: local" in captured.out
        assert "Local storage path: /tmp/storage" in captured.out

    @pytest.mark.asyncio
    async def test_run_catchup_validation_missing_library_directory(self):
        """Test catchup validation with missing library directory."""
        run_config = {}  # Missing library_directory
        storage_type = "minio"
        storage_config = {"bucket_raw": "test-bucket"}

        with pytest.raises(SystemExit):
            await run_catchup_validation(run_config, storage_type, storage_config)

    @pytest.mark.asyncio
    async def test_run_catchup_validation_missing_storage_config(self):
        """Test catchup validation with missing storage config."""
        run_config = {"library_directory": "Harvard"}
        storage_type = None
        storage_config = None

        with pytest.raises(SystemExit):
            await run_catchup_validation(run_config, storage_type, storage_config)
