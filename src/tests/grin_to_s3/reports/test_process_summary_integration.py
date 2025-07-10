"""
Integration tests for process summary tracking functionality.
"""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from grin_to_s3.process_summary import create_process_summary, save_process_summary


class TestProcessSummaryIntegration:
    """Integration tests for process summary functionality."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    @pytest.fixture
    def mock_run_name(self):
        """Provide a test run name."""
        return "test_integration_run"

    @pytest.mark.asyncio
    async def test_interruption_handling(self, temp_dir, mock_run_name):
        """Test that the process summary correctly handles interruptions."""
        runs_dir = temp_dir / "runs"
        runs_dir.mkdir()

        with patch("grin_to_s3.process_summary.Path") as mock_path:
            mock_path.return_value = runs_dir / mock_run_name

            # Create first summary
            summary1 = await create_process_summary(mock_run_name, "collect")
            collect_stage = summary1.stages["collect"]
            collect_stage.increment_items(processed=100, successful=95, failed=5)
            await save_process_summary(summary1)

            # Simulate interruption by creating a new summary with the same name
            summary2 = await create_process_summary(mock_run_name, "sync")

            # Should detect interruption when creating new stage
            summary2.detect_interruption()

            # The specific behavior depends on implementation, but should handle gracefully
            assert summary2.run_name == mock_run_name
            assert "collect" in summary2.stages  # Previous stage should be preserved
            assert "sync" in summary2.stages  # New stage should be added

    @pytest.mark.asyncio
    async def test_command_specific_metrics(self, temp_dir, mock_run_name):
        """Test that different commands can track their specific metrics."""
        runs_dir = temp_dir / "runs"
        runs_dir.mkdir()

        with patch("grin_to_s3.process_summary.Path") as mock_path:
            mock_path.return_value = runs_dir / mock_run_name

            # Test sync command metrics
            summary = await create_process_summary(mock_run_name, "sync")
            sync_stage = summary.stages["sync"]

            # Set command-specific arguments
            sync_stage.command_args.update(
                {
                    "storage_type": "r2",
                    "concurrent_downloads": 5,
                    "concurrent_uploads": 10,
                    "batch_size": 100,
                    "force_mode": False,
                }
            )

            sync_stage.increment_items(processed=50, successful=48, failed=2)
            sync_stage.add_progress_update("Sync pipeline completed")
            summary.end_stage("sync")

            # Verify the stage was recorded correctly
            summary_dict = summary.get_summary_dict()
            assert summary_dict["total_items_processed"] == 50
            assert summary_dict["total_items_successful"] == 48
            assert summary_dict["total_items_failed"] == 2

    @pytest.mark.asyncio
    async def test_progress_updates_content(self, temp_dir, mock_run_name):
        """Test that progress updates are stored correctly."""
        runs_dir = temp_dir / "runs"
        runs_dir.mkdir()

        with patch("grin_to_s3.process_summary.Path") as mock_path:
            mock_path.return_value = runs_dir / mock_run_name

            summary = await create_process_summary(mock_run_name, "collect")
            collect_stage = summary.stages["collect"]

            collect_stage.add_progress_update("Starting collection")
            collect_stage.add_progress_update("Processing batch 1")
            collect_stage.add_progress_update("Collection completed")

            # Check that progress updates were recorded
            assert len(collect_stage.progress_updates) == 3
            assert collect_stage.progress_updates[0]["message"] == "Starting collection"
            assert collect_stage.progress_updates[2]["message"] == "Collection completed"

    @pytest.mark.asyncio
    async def test_collect_command_integration(self, temp_dir, mock_run_name):
        """Test process summary tracking in collect command."""
        runs_dir = temp_dir / "runs"
        runs_dir.mkdir()

        with patch("grin_to_s3.process_summary.Path") as mock_path:
            mock_path.return_value = runs_dir / mock_run_name

            summary = await create_process_summary(mock_run_name, "collect")
            assert "collect" in summary.stages
            assert summary.run_name == mock_run_name

            collect_stage = summary.stages["collect"]
            collect_stage.increment_items(processed=200, successful=195, failed=5)
            collect_stage.add_progress_update("Collection completed successfully")
            summary.end_stage("collect")

            # Verify totals
            summary_dict = summary.get_summary_dict()
            assert summary_dict["total_items_processed"] == 200
            assert summary_dict["total_items_successful"] == 195
            assert summary_dict["total_items_failed"] == 5

    @pytest.mark.asyncio
    async def test_sync_command_integration(self, temp_dir, mock_run_name):
        """Test process summary tracking in sync command."""
        runs_dir = temp_dir / "runs"
        runs_dir.mkdir()

        with patch("grin_to_s3.process_summary.Path") as mock_path:
            mock_path.return_value = runs_dir / mock_run_name

            summary = await create_process_summary(mock_run_name, "sync")
            assert "sync" in summary.stages
            assert summary.run_name == mock_run_name

            sync_stage = summary.stages["sync"]
            sync_stage.command_args.update({"storage_type": "r2", "concurrent_downloads": 5, "force_mode": False})

            sync_stage.add_progress_update("Starting sync pipeline")
            sync_stage.increment_items(processed=50, successful=48, failed=2, bytes_count=1048576)
            sync_stage.add_progress_update("Sync pipeline completed successfully")
            summary.end_stage("sync")

            # Verify metrics
            summary_dict = summary.get_summary_dict()
            assert summary_dict["total_items_processed"] == 50
            assert summary_dict["total_bytes_processed"] == 1048576

    @pytest.mark.asyncio
    async def test_process_command_integration(self, temp_dir, mock_run_name):
        """Test process summary tracking in process command."""
        runs_dir = temp_dir / "runs"
        runs_dir.mkdir()

        with patch("grin_to_s3.process_summary.Path") as mock_path:
            mock_path.return_value = runs_dir / mock_run_name

            summary = await create_process_summary(mock_run_name, "process")
            assert "process" in summary.stages
            assert summary.run_name == mock_run_name

            process_stage = summary.stages["process"]
            process_stage.command_args.update(
                {"grin_library_directory": "MIT", "rate_limit": 0.2, "batch_size": 1000, "status_only": False}
            )

            process_stage.add_progress_update("Starting processing requests")
            process_stage.increment_items(processed=500, successful=495, failed=5, retried=3)
            process_stage.add_progress_update("Processing requests completed")
            summary.end_stage("process")

            # Verify metrics
            summary_dict = summary.get_summary_dict()
            assert summary_dict["total_items_processed"] == 500
            assert summary_dict["total_items_retried"] == 3
            assert summary_dict["overall_success_rate_percent"] == 99.0

    @pytest.mark.asyncio
    async def test_enrich_command_integration(self, temp_dir, mock_run_name):
        """Test process summary tracking in enrich command."""
        runs_dir = temp_dir / "runs"
        runs_dir.mkdir()

        with patch("grin_to_s3.process_summary.Path") as mock_path:
            mock_path.return_value = runs_dir / mock_run_name

            summary = await create_process_summary(mock_run_name, "enrich")
            assert "enrich" in summary.stages
            assert summary.run_name == mock_run_name

            enrich_stage = summary.stages["enrich"]
            enrich_stage.command_args.update(
                {"grin_library_directory": "Columbia", "rate_limit": 5.0, "batch_size": 500, "force_mode": True}
            )

            enrich_stage.add_progress_update("Starting enrichment")
            enrich_stage.increment_items(processed=300, successful=298, failed=2)
            enrich_stage.add_progress_update("Enrichment completed")
            summary.end_stage("enrich")

            # Verify metrics
            summary_dict = summary.get_summary_dict()
            assert summary_dict["total_items_processed"] == 300
            assert summary_dict["total_items_successful"] == 298
            assert summary_dict["total_items_failed"] == 2
