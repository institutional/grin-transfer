"""
Tests for storage CLI functionality.

Ensures the storage command interface remains available and functional.
"""

import pytest


def test_storage_module_has_main_function():
    """Test that storage module exports main function for CLI."""
    from grin_to_s3.storage import main

    assert callable(main)


def test_storage_main_import_from_grin_py():
    """Test that grin.py can import storage main function."""
    # This simulates the import that happens in grin.py
    from grin_to_s3.storage import main as storage_main

    assert callable(storage_main)


def test_storage_cli_module_exists():
    """Test that storage.__main__ module exists and is importable."""
    import grin_to_s3.storage.__main__ as storage_main_module

    assert hasattr(storage_main_module, "main")
    assert callable(storage_main_module.main)


def test_storage_cli_parser_creation():
    """Test that storage CLI can create argument parser without errors."""
    import argparse
    import sys
    from unittest.mock import patch

    # Mock sys.argv to prevent argparse from reading actual command line
    with patch.object(sys, "argv", ["storage", "ls", "--run-name", "test"]):
        # Mock argparse.ArgumentParser.parse_args to prevent actual parsing
        with patch.object(argparse.ArgumentParser, "parse_args") as mock_parse:
            # Mock the parsed args
            mock_args = argparse.Namespace(command="ls", run_name="test", long=False)
            mock_parse.return_value = mock_args

            # This should not raise an ImportError or other exception
            # We're just testing the module structure, not the full functionality
            try:
                # Import the functions to ensure they're available
                from grin_to_s3.storage.__main__ import cmd_ls, cmd_rm, format_size

                assert callable(cmd_ls)
                assert callable(cmd_rm)
                assert callable(format_size)
            except ImportError as e:
                pytest.fail(f"Storage CLI module missing required functions: {e}")


def test_format_size_function():
    """Test the format_size utility function."""
    from grin_to_s3.storage.__main__ import format_size

    assert format_size(0) == "0.0 B"
    assert format_size(1024) == "1.0 KB"
    assert format_size(1024 * 1024) == "1.0 MB"
    assert format_size(1536) == "1.5 KB"  # 1.5 KB
