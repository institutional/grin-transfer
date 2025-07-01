"""
Unit tests for extract CLI bucket functionality.

Tests the new bucket configuration and automatic writing features.
"""

import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.extract.__main__ import write_to_bucket

# Storage configuration validation is now handled by run_config utilities


class TestWriteToBucket:
    """Test bucket writing functionality."""

    @pytest.mark.asyncio
    async def test_write_to_bucket_success(self):
        """Test successful uploading to bucket."""
        mock_storage = AsyncMock()
        mock_storage.save_ocr_text_jsonl_from_file.return_value = "test-prefix/12345.jsonl"

        jsonl_file_path = "/path/to/12345.jsonl"
        barcode = "12345"

        await write_to_bucket(mock_storage, barcode, jsonl_file_path, verbose=True)

        mock_storage.save_ocr_text_jsonl_from_file.assert_called_once_with(barcode, jsonl_file_path)

    @pytest.mark.asyncio
    async def test_write_to_bucket_failure(self):
        """Test error handling when bucket uploading fails."""
        mock_storage = AsyncMock()
        mock_storage.save_ocr_text_jsonl_from_file.side_effect = Exception("Network error")

        jsonl_file_path = "/path/to/12345.jsonl"
        barcode = "12345"

        with pytest.raises(Exception, match="Network error"):
            await write_to_bucket(mock_storage, barcode, jsonl_file_path)


class TestExtractCLIIntegration:
    """Test CLI integration with bucket functionality."""

    @patch("grin_to_s3.storage.factories.create_book_storage_with_full_text")
    @patch("grin_to_s3.extract.__main__.extract_text_to_jsonl_file")
    @pytest.mark.asyncio
    async def test_extract_single_archive_with_bucket(self, mock_extract_to_file, mock_create_storage):
        """Test extract_single_archive with bucket storage only."""
        from grin_to_s3.extract.__main__ import extract_single_archive

        mock_book_storage = AsyncMock()
        mock_book_storage.save_ocr_text_jsonl_from_file.return_value = "book12345.jsonl"

        # Test extraction with bucket storage only (no file output)
        result = await extract_single_archive(
            archive_path="/path/to/book12345.tar.gz",
            book_storage=mock_book_storage,
            verbose=True
        )

        # Verify file extraction was called to temp file
        assert mock_extract_to_file.call_count == 1
        call_args = mock_extract_to_file.call_args
        assert call_args[0][0] == "/path/to/book12345.tar.gz"
        # Second arg should be a temp file path
        assert call_args[0][1].endswith(".jsonl")

        # Verify bucket upload was called
        mock_book_storage.save_ocr_text_jsonl_from_file.assert_called_once()
        upload_call_args = mock_book_storage.save_ocr_text_jsonl_from_file.call_args
        assert upload_call_args[0][0] == "book12345"  # barcode
        assert upload_call_args[0][1].endswith(".jsonl")  # temp file path

        # Verify result
        assert result["success"] is True
        assert result["archive"] == "/path/to/book12345.tar.gz"

    @patch("grin_to_s3.extract.__main__.extract_text_to_jsonl_file")
    @pytest.mark.asyncio
    async def test_extract_single_archive_bucket_and_file(self, mock_extract_to_file):
        """Test that file output and bucket storage are mutually exclusive."""
        from unittest.mock import MagicMock

        from grin_to_s3.extract.__main__ import main

        # Mock arguments for both file and run configuration output
        with patch("argparse.ArgumentParser.parse_args") as mock_parse_args:
            mock_args = MagicMock()
            mock_args.run_name = "test_run"  # Run configuration
            mock_args.output = "/path/to/output.jsonl"  # File output
            mock_args.output_dir = None
            mock_args.archives = ["/path/to/test.tar.gz"]
            mock_args.verbose = False
            mock_args.summary = False

            mock_parse_args.return_value = mock_args

            # Mock the run config functions to simulate successful config loading
            with patch("grin_to_s3.extract.__main__.setup_run_database_path") as mock_setup_db, \
                 patch("grin_to_s3.extract.__main__.apply_run_config_to_args") as mock_apply_config, \
                 patch("pathlib.Path.exists") as mock_path_exists, \
                 patch("json.load") as mock_json_load, \
                 patch("grin_to_s3.storage.factories.create_book_storage_with_full_text") as mock_create_storage, \
                 patch("builtins.print") as mock_print:

                mock_setup_db.return_value = "/path/to/db"
                mock_path_exists.return_value = True
                mock_json_load.return_value = {
                    "storage_config": {
                        "type": "r2",
                        "config": {"bucket_full": "test-bucket"},
                        "prefix": ""
                    }
                }
                mock_create_storage.return_value = MagicMock()

                result = await main()

                # Should exit with error code 1 due to conflicting options
                assert result == 1

                # Should print error about conflicting options
                mock_print.assert_called_with(
                    "Error: Cannot specify both file output and run configuration - choose one",
                    file=sys.stderr
                )

    @patch("grin_to_s3.extract.__main__.extract_text_to_jsonl_file")
    @pytest.mark.asyncio
    async def test_extract_single_archive_bucket_only_no_stdout(self, mock_extract_to_file):
        """Test that stdout output is suppressed when using bucket storage."""
        from grin_to_s3.extract.__main__ import extract_single_archive

        mock_extract_to_file.return_value = 1  # Return page count
        mock_book_storage = AsyncMock()

        with patch("builtins.print") as mock_print:
            result = await extract_single_archive(
                archive_path="/path/to/test.tar.gz",
                book_storage=mock_book_storage
            )

            # Verify no stdout printing of JSONL content when using bucket storage
            # Since we removed stdout support completely, there shouldn't be any JSON content printed
            print_calls = [call for call in mock_print.call_args_list
                          if '"Test content"' in str(call)]
            assert len(print_calls) == 0

    @patch("argparse.ArgumentParser.parse_args")
    @patch("grin_to_s3.extract.__main__.setup_run_database_path")
    @patch("grin_to_s3.extract.__main__.apply_run_config_to_args")
    @patch("pathlib.Path.exists")
    @patch("builtins.open", create=True)
    @patch("json.load")
    @patch("grin_to_s3.storage.factories.create_book_storage_with_full_text")
    @patch("grin_to_s3.extract.__main__.extract_single_archive")
    @pytest.mark.asyncio
    async def test_main_with_run_config(self, mock_extract_single, mock_create_storage, mock_json_load,
                                       mock_open, mock_path_exists, mock_apply_config, mock_setup_db, mock_parse_args):
        """Test main function with run configuration."""
        from grin_to_s3.extract.__main__ import main

        # Mock arguments
        mock_args = MagicMock()
        mock_args.run_name = "test_run"
        mock_args.output = None
        mock_args.output_dir = None
        mock_args.verbose = True
        mock_args.archives = ["/path/to/test.tar.gz"]
        mock_args.summary = False
        mock_args.extraction_dir = None
        mock_args.keep_extracted = False
        mock_args.use_disk = False

        mock_parse_args.return_value = mock_args

        # Mock run config functions
        mock_setup_db.return_value = "/path/to/test_run/books.db"
        mock_path_exists.return_value = True
        mock_json_load.return_value = {
            "storage_config": {
                "type": "r2",
                "config": {"bucket_full": "test-full-bucket"},
                "prefix": ""
            }
        }

        # Mock storage creation
        mock_book_storage = AsyncMock()
        mock_create_storage.return_value = mock_book_storage

        # Mock extraction result
        mock_extract_single.return_value = {
            "success": True,
            "archive": "/path/to/test.tar.gz",
            "pages": 1
        }

        result = await main()

        # Verify run config functions were called
        mock_setup_db.assert_called_once_with(mock_args, "test_run")
        mock_apply_config.assert_called_once()
        mock_path_exists.assert_called()
        mock_json_load.assert_called()

        # Verify storage was created
        mock_create_storage.assert_called_once_with("r2", {"bucket_full": "test-full-bucket"}, "")

        # Verify extraction was called with bucket storage
        mock_extract_single.assert_called_once()
        call_kwargs = mock_extract_single.call_args[1]
        assert call_kwargs["book_storage"] == mock_book_storage

        # Verify success exit code
        assert result == 0

    @patch("argparse.ArgumentParser.parse_args")
    @pytest.mark.asyncio
    async def test_main_missing_output_method(self, mock_parse_args):
        """Test main function fails when no output method is specified."""
        from grin_to_s3.extract.__main__ import main

        mock_args = MagicMock()
        mock_args.run_name = None
        mock_args.output = None
        mock_args.output_dir = None
        mock_args.archives = ["/path/to/test.tar.gz"]

        mock_parse_args.return_value = mock_args

        with patch("builtins.print") as mock_print:
            result = await main()

            # Verify error message and exit code
            mock_print.assert_any_call(
                "Error: Must specify either file output (--output/--output-dir) or run configuration (--run-name)",
                file=sys.stderr
            )
            assert result == 1
