#!/usr/bin/env python3
"""
Unit tests for unified logging functionality
"""

import json
import logging
import tempfile
from pathlib import Path

import pytest

from grin_to_s3.common import setup_logging
from grin_to_s3.run_config import RunConfig


class TestSetupLogging:
    """Test setup_logging function behavior."""

    def test_setup_logging_creates_log_file(self):
        """Test that setup_logging creates a log file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file = Path(temp_dir) / "test.log"

            setup_logging("INFO", str(log_file))

            # Log something to ensure file is created
            logger = logging.getLogger("test")
            logger.info("Test message")

            assert log_file.exists()
            assert "Test message" in log_file.read_text()

    def test_setup_logging_append_mode(self):
        """Test that append mode appends to existing log file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file = Path(temp_dir) / "test.log"

            # First logging session (create new file)
            setup_logging("INFO", str(log_file), append=False)
            logger1 = logging.getLogger("test1")
            logger1.info("First message")

            # Second logging session with append (default)
            setup_logging("INFO", str(log_file))
            logger2 = logging.getLogger("test2")
            logger2.info("Second message")

            content = log_file.read_text()
            assert "First message" in content
            assert "Second message" in content

    def test_setup_logging_overwrite_mode(self):
        """Test that overwrite mode overwrites existing log file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file = Path(temp_dir) / "test.log"

            # First logging session
            setup_logging("INFO", str(log_file))
            logger1 = logging.getLogger("test1")
            logger1.info("First message")

            # Second logging session without append (overwrite)
            setup_logging("INFO", str(log_file), append=False)
            logger2 = logging.getLogger("test2")
            logger2.info("Second message")

            content = log_file.read_text()
            assert "First message" not in content
            assert "Second message" in content

    def test_setup_logging_creates_parent_directories(self):
        """Test that setup_logging creates parent directories."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file = Path(temp_dir) / "subdir" / "nested" / "test.log"

            setup_logging("INFO", str(log_file))

            # Log something to ensure file is created
            logger = logging.getLogger("test")
            logger.info("Test message")

            assert log_file.exists()
            assert log_file.parent.exists()


class TestRunConfigLogFile:
    """Test RunConfig log_file property."""

    def test_run_config_log_file_property(self, test_config_builder):
        """Test that RunConfig.log_file returns the correct path."""
        run_config = (
            test_config_builder.local_storage().with_run_name("test_run").with_log_file("/path/to/logfile.log").build()
        )

        assert run_config.log_file == "/path/to/logfile.log"

    def test_run_config_log_file_with_run_name(self, test_config_builder):
        """Test log_file property with realistic run name format."""
        run_config = (
            test_config_builder.local_storage()
            .with_run_name("my_test_run")
            .with_log_file("logs/grin_pipeline_my_test_run_20250626_105045.log")
            .build()
        )

        assert "grin_pipeline_my_test_run" in run_config.log_file
        assert run_config.log_file.endswith(".log")


class TestUnifiedLoggingIntegration:
    """Test unified logging integration with collect command."""

    def test_collect_creates_log_file_in_config(self, test_config_builder):
        """Test that collect command creates log_file in run_config.json."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Simulate what collect command does
            run_name = "test_run"
            timestamp = "20250626_105045"
            log_dir = "logs"
            log_file = f"{log_dir}/grin_pipeline_{run_name}_{timestamp}.log"

            # Create config dict as collect would
            config_dict = (
                test_config_builder.local_storage().with_run_name(run_name).with_log_file(log_file).build().config_dict
            )

            # Write config file
            config_path = Path(temp_dir) / "run_config.json"
            with open(config_path, "w") as f:
                json.dump(config_dict, f, indent=2)

            # Load and verify
            run_config = RunConfig(config_dict)
            assert run_config.log_file == log_file
            assert "grin_pipeline_test_run" in run_config.log_file

    def test_custom_log_dir_in_config(self, test_config_builder):
        """Test that custom log directory is preserved in config."""
        with tempfile.TemporaryDirectory():
            # Simulate collect with custom log dir
            run_name = "test_run"
            timestamp = "20250626_105045"
            custom_log_dir = "custom_logs"
            log_file = f"{custom_log_dir}/grin_pipeline_{run_name}_{timestamp}.log"

            config_dict = (
                test_config_builder.local_storage().with_run_name(run_name).with_log_file(log_file).build().config_dict
            )

            run_config = RunConfig(config_dict)
            assert run_config.log_file.startswith("custom_logs/")
            assert "grin_pipeline_test_run" in run_config.log_file

    def test_subsequent_commands_use_same_log_file(self, test_config_builder):
        """Test that subsequent commands can access the same log file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file = str(Path(temp_dir) / "grin_pipeline_test_20250626_105045.log")

            config_dict = (
                test_config_builder.local_storage().with_run_name("test").with_log_file(log_file).build().config_dict
            )

            # First command (collect) creates log
            setup_logging("INFO", log_file)
            logger1 = logging.getLogger("collect")
            logger1.info("Collection started")

            # Second command (sync) appends to same log
            run_config = RunConfig(config_dict)
            setup_logging("INFO", run_config.log_file, append=True)
            logger2 = logging.getLogger("sync")
            logger2.info("Sync started")

            # Verify both messages are in the log
            content = Path(log_file).read_text()
            assert "Collection started" in content
            assert "Sync started" in content


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
