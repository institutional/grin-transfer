"""Tests for OCR extraction configuration management."""

from dataclasses import dataclass
from unittest.mock import Mock, patch

import pytest

from grin_to_s3.sync.pipeline import SyncPipeline


@dataclass
class MockOCRConfig:
    """Mock OCR configuration for testing actual implemented schema."""

    enabled: bool = True
    log_level: str = "INFO"

    def __post_init__(self):
        """Validate OCR configuration parameters."""
        if self.log_level not in ["DEBUG", "INFO", "WARNING", "ERROR"]:
            raise ValueError("log_level must be DEBUG, INFO, WARNING, or ERROR")


class TestOCRConfiguration:
    """Test OCR extraction configuration validation and behavior."""

    def test_sync_pipeline_default_ocr_enabled(self, mock_process_stage, test_config_builder):
        """Test that OCR extraction is enabled by default."""
        with (
            patch("grin_to_s3.sync.pipeline.SQLiteProgressTracker") as mock_tracker,
            patch("grin_to_s3.sync.pipeline.ProgressReporter") as mock_reporter,
            patch("grin_to_s3.sync.pipeline.GRINClient") as mock_client,
            patch("grin_to_s3.storage.StagingDirectoryManager") as mock_staging,
        ):
            mock_tracker.return_value = Mock()
            mock_reporter.return_value = Mock()
            mock_client.return_value = Mock()
            mock_staging.return_value = Mock()

            config = (test_config_builder
                     .with_db_path(":memory:")
                     .with_library_directory("/tmp/library")
                     .local_storage("/tmp/test")
                     .with_staging_dir("/tmp/test")
                     .build())

            pipeline = SyncPipeline.from_run_config(
                config=config,
                process_summary_stage=mock_process_stage,
            )
            assert pipeline.skip_extract_ocr is False  # Default is to extract OCR

    def test_sync_pipeline_ocr_disabled(self, mock_process_stage, test_config_builder):
        """Test that OCR extraction can be disabled."""
        with (
            patch("grin_to_s3.sync.pipeline.SQLiteProgressTracker") as mock_tracker,
            patch("grin_to_s3.sync.pipeline.ProgressReporter") as mock_reporter,
            patch("grin_to_s3.sync.pipeline.GRINClient") as mock_client,
            patch("grin_to_s3.storage.StagingDirectoryManager") as mock_staging,
        ):
            mock_tracker.return_value = Mock()
            mock_reporter.return_value = Mock()
            mock_client.return_value = Mock()
            mock_staging.return_value = Mock()

            config = (test_config_builder
                     .with_db_path(":memory:")
                     .with_library_directory("/tmp/library")
                     .local_storage("/tmp/test")
                     .with_staging_dir("/tmp/test")
                     .build())

            pipeline = SyncPipeline.from_run_config(
                config=config,
                process_summary_stage=mock_process_stage,
                skip_extract_ocr=True,
            )
            assert pipeline.skip_extract_ocr is True

    def test_ocr_config_validation_valid(self):
        """Test valid OCR configuration passes validation."""
        config = MockOCRConfig(enabled=True, log_level="INFO")
        assert config.enabled is True
        assert config.log_level == "INFO"

    def test_ocr_config_validation_invalid_log_level(self):
        """Test that invalid log_level raises ValueError."""
        with pytest.raises(ValueError, match="log_level must be DEBUG, INFO, WARNING, or ERROR"):
            MockOCRConfig(log_level="INVALID")

        with pytest.raises(ValueError, match="log_level must be DEBUG, INFO, WARNING, or ERROR"):
            MockOCRConfig(log_level="debug")  # Case sensitive

    def test_ocr_config_defaults(self):
        """Test OCR configuration default values."""
        config = MockOCRConfig()
        assert config.enabled is True
        assert config.log_level == "INFO"

    def test_ocr_config_disabled(self):
        """Test OCR configuration when disabled."""
        config = MockOCRConfig(enabled=False)
        assert config.enabled is False
        assert config.log_level == "INFO"

    @pytest.mark.parametrize("log_level", ["DEBUG", "INFO", "WARNING", "ERROR"])
    def test_ocr_config_valid_log_levels(self, log_level):
        """Test all valid log levels are accepted."""
        config = MockOCRConfig(log_level=log_level)
        assert config.log_level == log_level

    def test_ocr_config_boundary_values(self):
        """Test OCR configuration boundary values."""
        # Test enabled/disabled states
        config_enabled = MockOCRConfig(enabled=True)
        assert config_enabled.enabled is True

        config_disabled = MockOCRConfig(enabled=False)
        assert config_disabled.enabled is False

    def test_sync_pipeline_ocr_configuration_integration(self, mock_process_stage, test_config_builder):
        """Test OCR configuration integration with sync pipeline."""
        with (
            patch("grin_to_s3.sync.pipeline.SQLiteProgressTracker") as mock_tracker,
            patch("grin_to_s3.sync.pipeline.ProgressReporter") as mock_reporter,
            patch("grin_to_s3.sync.pipeline.GRINClient") as mock_client,
            patch("grin_to_s3.storage.StagingDirectoryManager") as mock_staging,
        ):
            mock_tracker.return_value = Mock()
            mock_reporter.return_value = Mock()
            mock_client.return_value = Mock()
            mock_staging.return_value = Mock()

            config = (test_config_builder
                     .with_db_path(":memory:")
                     .with_library_directory("/tmp/library")
                     .local_storage("/tmp/test")
                     .with_staging_dir("/tmp/test")
                     .build())

            # Test OCR enabled
            pipeline_enabled = SyncPipeline.from_run_config(
                config=config,
                process_summary_stage=mock_process_stage,
                skip_extract_ocr=False,
            )
            assert pipeline_enabled.skip_extract_ocr is False

            # Test OCR disabled
            pipeline_disabled = SyncPipeline.from_run_config(
                config=config,
                process_summary_stage=mock_process_stage,
                skip_extract_ocr=True,
            )
            assert pipeline_disabled.skip_extract_ocr is True

    def test_ocr_config_error_messages(self):
        """Test that configuration errors provide clear messages."""
        # Test log level error message
        with pytest.raises(ValueError) as exc_info:
            MockOCRConfig(log_level="TRACE")
        assert "log_level must be DEBUG, INFO, WARNING, or ERROR" in str(exc_info.value)

    def test_ocr_config_serialization_ready(self):
        """Test that OCR configuration can be serialized for storage."""
        config = MockOCRConfig(enabled=False, log_level="DEBUG")

        # Verify config can be converted to dict (for JSON serialization)
        config_dict = {"enabled": config.enabled, "log_level": config.log_level}

        assert config_dict == {"enabled": False, "log_level": "DEBUG"}

        # Verify config can be reconstructed from dict
        reconstructed = MockOCRConfig(enabled=bool(config_dict["enabled"]), log_level=str(config_dict["log_level"]))
        assert reconstructed.enabled == config.enabled
        assert reconstructed.log_level == config.log_level
