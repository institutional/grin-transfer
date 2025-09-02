#!/usr/bin/env python3
"""
Unit tests for CSV export configuration functionality
"""

import json
import os
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from grin_to_s3.collect_books.config import ConfigManager, ExportConfig


class TestExportConfig:
    """Test main export configuration."""

    def test_export_config_defaults(self):
        """Test default export configuration."""
        config = ExportConfig(library_directory="TestLibrary")

        assert config.library_directory == "TestLibrary"
        assert config.rate_limit == 5.0
        assert config.burst_limit == 10
        assert config.processing_chunk_size == 1000

    def test_export_config_from_dict(self):
        """Test creating config from dictionary."""
        config_dict = {
            "library_directory": "TestLibrary",
            "rate_limit": 3.0,
            "burst_limit": 15,
        }

        config = ExportConfig.from_dict(config_dict)

        assert config.library_directory == "TestLibrary"
        assert config.rate_limit == 3.0
        assert config.burst_limit == 15

    def test_export_config_to_dict(self):
        """Test converting config to dictionary."""
        config = ExportConfig(library_directory="TestLibrary", rate_limit=1.5)

        config_dict = config.to_dict()

        assert config_dict["library_directory"] == "TestLibrary"
        assert config_dict["rate_limit"] == 1.5
        assert "burst_limit" in config_dict

    def test_export_config_file_operations(self):
        """Test saving and loading config from file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "test_config.json"

            # Create and save config
            original_config = ExportConfig(library_directory="TestLibrary", rate_limit=4.0, burst_limit=25)
            original_config.save_to_file(config_path)

            # Verify file exists and has content
            assert config_path.exists()

            # Load config and verify
            loaded_config = ExportConfig.load_from_file(config_path)

            assert loaded_config.library_directory == "TestLibrary"
            assert loaded_config.rate_limit == 4.0
            assert loaded_config.burst_limit == 25

    def test_export_config_load_missing_file(self):
        """Test loading config from missing file returns defaults."""
        non_existent_path = Path("/tmp/non_existent_config.json")

        config = ExportConfig.load_from_file(non_existent_path)

        # Should return default config
        assert config.library_directory == "REQUIRED"
        assert config.rate_limit == 5.0

    def test_export_config_update_from_args(self):
        """Test updating config from CLI arguments."""
        config = ExportConfig(library_directory="TestLibrary")

        # Update basic settings
        config.update_from_args(library_directory="TestLibrary2", rate_limit=2.0, burst_limit=30)

        assert config.library_directory == "TestLibrary2"
        assert config.rate_limit == 2.0
        assert config.burst_limit == 30


class TestConfigManager:
    """Test configuration manager."""

    def test_config_manager_load_default(self):
        """Test loading default configuration."""
        config = ConfigManager.load_config()

        # Should return default config with placeholder
        assert config.library_directory == "REQUIRED"
        assert config.rate_limit == 5.0

    def test_config_manager_load_with_overrides(self):
        """Test loading config with CLI overrides."""
        config = ConfigManager.load_config(
            library_directory="TestLibrary", rate_limit=1.0, pagination_page_size=2000, pagination_max_pages=500
        )

        assert config.library_directory == "TestLibrary"
        assert config.rate_limit == 1.0

    def test_config_manager_load_from_file(self):
        """Test loading config from specific file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "custom_config.json"

            # Create config file
            config_data = {
                "library_directory": "TestLibrary",
                "rate_limit": 0.5,
                "pagination": {"page_size": 500, "max_pages": 2000},
            }

            with open(config_path, "w") as f:
                json.dump(config_data, f)

            # Load with overrides
            config = ConfigManager.load_config(
                config_file=str(config_path),
                rate_limit=3.0,  # Override file setting
            )

            assert config.library_directory == "REQUIRED"  # From file
            assert config.rate_limit == 3.0  # From override

    def test_config_manager_create_default(self):
        """Test creating default configuration file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "created_config.json"

            ConfigManager.create_default_config(config_path)

            # Verify file was created
            assert config_path.exists()

            # Verify content
            with open(config_path) as f:
                saved_data = json.load(f)

            assert saved_data["library_directory"] == "CHANGE_ME"
            assert saved_data["rate_limit"] == 5.0

    def test_export_config_update_data_mode_from_args(self):
        """Test updating config from CLI arguments."""
        config = ExportConfig(library_directory="TestLibrary")

        # Test updating other parameters
        config.update_from_args(rate_limit=10.0)
        assert config.rate_limit == 10.0


if __name__ == "__main__":
    # Run tests with: python -m pytest tests/unit/test_collect_books_config.py -v
    pytest.main([__file__, "-v"])
