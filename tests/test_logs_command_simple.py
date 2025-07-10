"""
Simple tests for the logs command functionality.
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.logs.__main__ import handle_summary


class TestLogsCommand:
    """Test logs command functionality."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    @pytest.fixture
    def mock_run_name(self):
        """Provide a test run name."""
        return "test_logs_run"

    @pytest.fixture
    def sample_summary_data(self):
        """Create sample summary data for testing."""
        return {
            "run_name": "test_logs_run",
            "session_id": 1234567890,
            "total_items_processed": 100,
            "total_items_successful": 95,
            "total_items_failed": 5,
            "total_error_count": 3,
            "overall_success_rate_percent": 95.0,
            "is_completed": True,
            "stages": {
                "collect": {
                    "items_processed": 60,
                    "is_completed": True,
                },
                "sync": {
                    "items_processed": 40,
                    "is_completed": True,
                },
            },
        }

    @pytest.mark.asyncio
    async def test_view_local_summary_pretty(self, temp_dir, mock_run_name, sample_summary_data, capsys):
        """Test viewing local summary in pretty format."""
        # Create mock args
        args = MagicMock()
        args.run_name = mock_run_name
        args.download = None
        args.pretty = True

        # Create summary file
        with patch("grin_to_s3.logs.__main__.RunSummaryManager") as mock_manager_class:
            mock_manager = MagicMock()
            mock_manager.summary_file = temp_dir / "process_summary.json"
            mock_manager._summary_file_exists = AsyncMock(return_value=True)
            mock_manager_class.return_value = mock_manager

            # Write sample data to file
            with open(mock_manager.summary_file, "w") as f:
                json.dump(sample_summary_data, f)

            # Test viewing in pretty format
            result = await handle_summary(args)

            # Capture output
            captured = capsys.readouterr()

            # Verify success
            assert result == 0
            # Verify JSON output can be parsed
            output_data = json.loads(captured.out)
            assert output_data["run_name"] == "test_logs_run"
            assert output_data["total_items_processed"] == 100

    @pytest.mark.asyncio
    async def test_view_local_summary_not_found(self, mock_run_name, capsys):
        """Test viewing local summary when file doesn't exist."""
        # Create mock args
        args = MagicMock()
        args.run_name = mock_run_name
        args.download = None
        args.pretty = False

        with patch("grin_to_s3.logs.__main__.RunSummaryManager") as mock_manager_class:
            mock_manager = MagicMock()
            mock_manager.summary_file = Path("nonexistent/process_summary.json")
            mock_manager._summary_file_exists = AsyncMock(return_value=False)
            mock_manager_class.return_value = mock_manager

            # Test viewing non-existent file
            result = await handle_summary(args)

            # Capture output
            captured = capsys.readouterr()

            # Verify error
            assert result == 1
            assert "No local process summary found" in captured.out

    @pytest.mark.asyncio
    async def test_download_summary_from_storage(self, temp_dir, mock_run_name, sample_summary_data):
        """Test downloading summary from storage."""
        # Create mock args
        args = MagicMock()
        args.run_name = mock_run_name
        args.download = str(temp_dir / "downloaded_summary.json")

        # Mock book storage
        mock_book_storage = MagicMock()
        mock_book_storage._meta_path = MagicMock(return_value="test-meta/test_run/process_summary_test_logs_run.json")
        mock_book_storage.storage.read_bytes = AsyncMock(return_value=json.dumps(sample_summary_data).encode())

        with patch("grin_to_s3.logs.__main__.create_book_storage_for_uploads", return_value=mock_book_storage):
            # Test downloading
            result = await handle_summary(args)

            # Verify success
            assert result == 0

            # Verify file was created
            downloaded_file = Path(args.download)
            assert downloaded_file.exists()

            # Verify file content
            with open(downloaded_file) as f:
                file_data = json.load(f)
            assert file_data["run_name"] == "test_logs_run"

    @pytest.mark.asyncio
    async def test_download_summary_no_storage_config(self, mock_run_name, capsys):
        """Test downloading summary when no storage config is found."""
        # Create mock args
        args = MagicMock()
        args.run_name = mock_run_name
        args.download = "output.json"

        with patch("grin_to_s3.logs.__main__.create_book_storage_for_uploads", return_value=None):
            # Test downloading with no storage config
            result = await handle_summary(args)

            # Capture output
            captured = capsys.readouterr()

            # Verify error
            assert result == 1
            assert "No storage configuration found" in captured.out

    @pytest.mark.asyncio
    async def test_view_local_summary_readable_format(self, temp_dir, mock_run_name, sample_summary_data, capsys):
        """Test viewing local summary in human-readable format."""
        # Create mock args
        args = MagicMock()
        args.run_name = mock_run_name
        args.download = None
        args.pretty = False

        # Create summary file
        with patch("grin_to_s3.logs.__main__.RunSummaryManager") as mock_manager_class:
            mock_manager = MagicMock()
            mock_manager.summary_file = temp_dir / "process_summary.json"
            mock_manager._summary_file_exists = AsyncMock(return_value=True)
            mock_manager_class.return_value = mock_manager

            # Write sample data to file
            with open(mock_manager.summary_file, "w") as f:
                json.dump(sample_summary_data, f)

            # Test viewing in readable format
            result = await handle_summary(args)

            # Capture output
            captured = capsys.readouterr()

            # Verify success
            assert result == 0
            # Verify human-readable output
            assert "Run: test_logs_run" in captured.out
            assert "Total Items Processed: 100" in captured.out
            assert "Success Rate: 95.0%" in captured.out
            assert "collect: 60 items" in captured.out
            assert "sync: 40 items" in captured.out
