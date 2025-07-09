"""
Integration tests for process summary tracking in pipeline commands.
"""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from grin_to_s3.process_summary import create_process_summary


class TestProcessSummaryIntegration:
    """Test process summary integration with pipeline commands."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    @pytest.fixture
    def mock_run_name(self):
        """Provide a test run name."""
        return "test_run_2024"

    @pytest.mark.asyncio
    async def test_collect_command_integration(self, temp_dir, mock_run_name):
        """Test process summary tracking in collect command."""
        # Mock the runs directory for this test
        runs_dir = temp_dir / "runs"
        runs_dir.mkdir()

        with patch("grin_to_s3.process_summary.Path") as mock_path:
            mock_path.return_value = runs_dir / mock_run_name

            # Test creating process summary for collect command
            summary = await create_process_summary(mock_run_name, "collect")

            assert summary.process_name == "collect"
            assert summary.run_name == mock_run_name
            assert summary.start_time > 0
            assert summary.last_checkpoint_time > 0

            # Test setting custom metrics (like collect command does)
            summary.set_custom_metric("library_directory", "Harvard")
            summary.set_custom_metric("storage_type", "minio")
            summary.set_custom_metric("test_mode", False)

            # Test adding progress updates
            summary.add_progress_update("Configuration written, starting collection")
            summary.add_progress_update("Starting book collection")

            # Test incrementing counters
            summary.increment_items(processed=100, successful=95, failed=5)

            # Test error handling
            summary.add_error("NetworkError", "Connection timeout")

            # Test completion
            summary.end_process()

            # Verify metrics
            summary_dict = summary.get_summary_dict()
            assert summary_dict["custom_metrics"]["library_directory"] == "Harvard"
            assert summary_dict["custom_metrics"]["storage_type"] == "minio"
            assert summary_dict["items_processed"] == 100
            assert summary_dict["items_successful"] == 95
            assert summary_dict["items_failed"] == 5
            assert summary_dict["error_count"] == 1
            assert summary_dict["error_types"]["NetworkError"] == 1
            assert summary_dict["is_completed"]
            assert len(summary_dict["progress_updates"]) == 2

    @pytest.mark.asyncio
    async def test_sync_command_integration(self, temp_dir, mock_run_name):
        """Test process summary tracking in sync command."""
        runs_dir = temp_dir / "runs"
        runs_dir.mkdir()

        with patch("grin_to_s3.process_summary.Path") as mock_path:
            mock_path.return_value = runs_dir / mock_run_name

            # Test creating process summary for sync command
            summary = await create_process_summary(mock_run_name, "sync")

            assert summary.process_name == "sync"
            assert summary.run_name == mock_run_name

            # Test setting custom metrics (like sync command does)
            summary.set_custom_metric("storage_type", "r2")
            summary.set_custom_metric("concurrent_downloads", 5)
            summary.set_custom_metric("concurrent_uploads", 10)
            summary.set_custom_metric("batch_size", 100)
            summary.set_custom_metric("force_mode", False)

            # Test progress updates
            summary.add_progress_update("Starting sync pipeline")
            summary.add_progress_update("Single book mode optimization applied")

            # Test metrics for sync operations
            summary.increment_items(processed=50, successful=48, failed=2, bytes_count=1048576)

            # Test completion
            summary.add_progress_update("Sync pipeline completed successfully")
            summary.end_process()

            # Verify metrics
            summary_dict = summary.get_summary_dict()
            assert summary_dict["custom_metrics"]["storage_type"] == "r2"
            assert summary_dict["custom_metrics"]["concurrent_downloads"] == 5
            assert summary_dict["bytes_processed"] == 1048576
            assert summary_dict["success_rate_percent"] == 96.0
            assert summary_dict["is_completed"]

    @pytest.mark.asyncio
    async def test_process_command_integration(self, temp_dir, mock_run_name):
        """Test process summary tracking in process command."""
        runs_dir = temp_dir / "runs"
        runs_dir.mkdir()

        with patch("grin_to_s3.process_summary.Path") as mock_path:
            mock_path.return_value = runs_dir / mock_run_name

            # Test creating process summary for process command
            summary = await create_process_summary(mock_run_name, "process")

            assert summary.process_name == "process"
            assert summary.run_name == mock_run_name

            # Test setting custom metrics (like process command does)
            summary.set_custom_metric("grin_library_directory", "MIT")
            summary.set_custom_metric("rate_limit", 0.2)
            summary.set_custom_metric("batch_size", 1000)
            summary.set_custom_metric("max_in_process", 50000)
            summary.set_custom_metric("status_only", False)

            # Test progress updates
            summary.add_progress_update("Starting processing requests")

            # Test metrics for processing operations
            summary.increment_items(processed=500, successful=495, failed=5, retried=3)

            # Test completion
            summary.add_progress_update("Processing requests completed")
            summary.end_process()

            # Verify metrics
            summary_dict = summary.get_summary_dict()
            assert summary_dict["custom_metrics"]["grin_library_directory"] == "MIT"
            assert summary_dict["custom_metrics"]["rate_limit"] == 0.2
            assert summary_dict["items_processed"] == 500
            assert summary_dict["items_retried"] == 3
            assert summary_dict["success_rate_percent"] == 99.0

    @pytest.mark.asyncio
    async def test_enrich_command_integration(self, temp_dir, mock_run_name):
        """Test process summary tracking in enrich command."""
        runs_dir = temp_dir / "runs"
        runs_dir.mkdir()

        with patch("grin_to_s3.process_summary.Path") as mock_path:
            mock_path.return_value = runs_dir / mock_run_name

            # Test creating process summary for enrich command
            summary = await create_process_summary(mock_run_name, "enrich")

            assert summary.process_name == "enrich"
            assert summary.run_name == mock_run_name

            # Test setting custom metrics (like enrich command does)
            summary.set_custom_metric("grin_library_directory", "Yale")
            summary.set_custom_metric("rate_limit", 0.2)
            summary.set_custom_metric("batch_size", 2000)
            summary.set_custom_metric("max_concurrent", 5)
            summary.set_custom_metric("reset", False)

            # Test progress updates
            summary.add_progress_update("Starting enrichment pipeline")

            # Test metrics for enrichment operations
            summary.increment_items(processed=1000, successful=980, failed=20)

            # Test completion
            summary.add_progress_update("Enrichment pipeline completed successfully")
            summary.end_process()

            # Verify metrics
            summary_dict = summary.get_summary_dict()
            assert summary_dict["custom_metrics"]["grin_library_directory"] == "Yale"
            assert summary_dict["custom_metrics"]["batch_size"] == 2000
            assert summary_dict["items_processed"] == 1000
            assert summary_dict["success_rate_percent"] == 98.0

    @pytest.mark.asyncio
    async def test_interruption_handling(self, temp_dir, mock_run_name):
        """Test process summary handling of interruptions."""
        runs_dir = temp_dir / "runs"
        runs_dir.mkdir()

        with patch("grin_to_s3.process_summary.Path") as mock_path:
            mock_path.return_value = runs_dir / mock_run_name

            # Create initial summary
            summary = await create_process_summary(mock_run_name, "collect")
            summary.increment_items(processed=50, successful=45, failed=5)
            summary.add_progress_update("Collection in progress")

            # Simulate interruption (KeyboardInterrupt)
            summary.add_error("KeyboardInterrupt", "User cancelled operation")
            summary.add_progress_update("Collection interrupted by user")
            summary.end_process()

            # Verify error tracking
            summary_dict = summary.get_summary_dict()
            assert summary_dict["error_count"] == 1
            assert summary_dict["error_types"]["KeyboardInterrupt"] == 1
            assert summary_dict["last_error_message"] == "User cancelled operation"
            assert any("interrupted" in update["message"] for update in summary_dict["progress_updates"])

    @pytest.mark.asyncio
    async def test_command_specific_metrics(self, temp_dir, mock_run_name):
        """Test that each command tracks appropriate metrics."""
        runs_dir = temp_dir / "runs"
        runs_dir.mkdir()

        with patch("grin_to_s3.process_summary.Path") as mock_path:
            mock_path.return_value = runs_dir / mock_run_name

            # Test collect command metrics
            collect_summary = await create_process_summary(mock_run_name, "collect")
            collect_summary.set_custom_metric("library_directory", "Harvard")
            collect_summary.set_custom_metric("storage_type", "minio")
            collect_summary.set_custom_metric("test_mode", False)

            # Test sync command metrics
            sync_summary = await create_process_summary(mock_run_name, "sync")
            sync_summary.set_custom_metric("storage_type", "r2")
            sync_summary.set_custom_metric("concurrent_downloads", 5)
            sync_summary.set_custom_metric("force_mode", False)

            # Test process command metrics
            process_summary = await create_process_summary(mock_run_name, "process")
            process_summary.set_custom_metric("grin_library_directory", "MIT")
            process_summary.set_custom_metric("rate_limit", 0.2)
            process_summary.set_custom_metric("status_only", False)

            # Test enrich command metrics
            enrich_summary = await create_process_summary(mock_run_name, "enrich")
            enrich_summary.set_custom_metric("grin_library_directory", "Yale")
            enrich_summary.set_custom_metric("max_concurrent", 5)
            enrich_summary.set_custom_metric("reset", False)

            # Verify each command has appropriate metrics
            collect_dict = collect_summary.get_summary_dict()
            assert "library_directory" in collect_dict["custom_metrics"]
            assert "storage_type" in collect_dict["custom_metrics"]

            sync_dict = sync_summary.get_summary_dict()
            assert "concurrent_downloads" in sync_dict["custom_metrics"]
            assert "force_mode" in sync_dict["custom_metrics"]

            process_dict = process_summary.get_summary_dict()
            assert "rate_limit" in process_dict["custom_metrics"]
            assert "status_only" in process_dict["custom_metrics"]

            enrich_dict = enrich_summary.get_summary_dict()
            assert "max_concurrent" in enrich_dict["custom_metrics"]
            assert "reset" in enrich_dict["custom_metrics"]

    @pytest.mark.asyncio
    async def test_progress_updates_content(self, temp_dir, mock_run_name):
        """Test that progress updates contain meaningful information."""
        runs_dir = temp_dir / "runs"
        runs_dir.mkdir()

        with patch("grin_to_s3.process_summary.Path") as mock_path:
            mock_path.return_value = runs_dir / mock_run_name

            summary = await create_process_summary(mock_run_name, "sync")

            # Add various progress updates with different contexts
            summary.add_progress_update("Starting sync pipeline")
            summary.add_progress_update("Filtering to 5 specific barcodes", barcode_count=5)
            summary.add_progress_update("Single book mode optimization applied", optimization="single_book")
            summary.add_progress_update("Sync pipeline completed successfully", completion_status="success")

            # Verify progress updates have proper structure
            progress_dict = summary.get_progress_summary()
            assert progress_dict["total_updates"] == 4
            assert progress_dict["first_update"]["message"] == "Starting sync pipeline"
            assert progress_dict["latest_update"]["message"] == "Sync pipeline completed successfully"

            # Check that custom fields are preserved
            updates = summary.get_summary_dict()["progress_updates"]
            barcode_update = next(u for u in updates if "barcode_count" in u)
            assert barcode_update["barcode_count"] == 5

            optimization_update = next(u for u in updates if "optimization" in u)
            assert optimization_update["optimization"] == "single_book"
