"""Tests for atomic archive extraction functionality."""

import tarfile

import pytest

from grin_to_s3.storage.staging import StagingDirectoryManager


class TestStagingDirectoryManager:
    """Test staging directory manager with extracted directory support."""

    @pytest.fixture
    def staging_manager(self, tmp_path):
        """Create a staging manager for testing."""
        return StagingDirectoryManager(tmp_path)

    def test_get_extracted_directory_path(self, staging_manager):
        """Test extracted directory path generation."""
        barcode = "test123"
        expected_path = staging_manager.staging_path / f"{barcode}_extracted"

        result = staging_manager.get_extracted_directory_path(barcode)

        assert result == expected_path
        assert str(result).endswith("test123_extracted")


class TestAtomicArchiveExtraction:
    """Test atomic archive extraction functionality."""

    @pytest.fixture
    def sample_archive(self, tmp_path):
        """Create a sample tar.gz archive for testing."""
        archive_dir = tmp_path / "archive_content"
        archive_dir.mkdir()

        # Create sample files
        (archive_dir / "page001.txt").write_text("Page 1 content")
        (archive_dir / "page002.txt").write_text("Page 2 content")
        (archive_dir / "metadata.xml").write_text("<xml>metadata</xml>")

        # Create subdirectory
        sub_dir = archive_dir / "subdir"
        sub_dir.mkdir()
        (sub_dir / "page003.txt").write_text("Page 3 content")

        # Create tar.gz archive
        archive_path = tmp_path / "test_archive.tar.gz"
        with tarfile.open(archive_path, "w:gz") as tar:
            tar.add(archive_dir, arcname=".")

        return archive_path


class TestExtractionIntegration:
    """Integration tests for the complete extraction workflow."""

    def test_staging_extraction_workflow(self, tmp_path):
        """Test complete workflow: staging -> extraction -> filesystem access."""
        # Setup staging manager
        staging_manager = StagingDirectoryManager(tmp_path)
        barcode = "integration_test"

        # Create sample archive
        archive_content_dir = tmp_path / "archive_content"
        archive_content_dir.mkdir()
        (archive_content_dir / "page001.txt").write_text("Integration test content")
        (archive_content_dir / "metadata.xml").write_text("<metadata>test</metadata>")

        archive_path = tmp_path / "test.tar.gz"
        with tarfile.open(archive_path, "w:gz") as tar:
            tar.add(archive_content_dir, arcname=".")

        # Extract to staging directory
        extracted_dir = staging_manager.get_extracted_directory_path(barcode)
        extracted_dir.mkdir(parents=True, exist_ok=True)

        with tarfile.open(str(archive_path), "r:gz") as tar:
            tar.extractall(path=extracted_dir)

        # Verify filesystem access works
        txt_files = list(extracted_dir.glob("**/*.txt"))
        xml_files = list(extracted_dir.glob("**/*.xml"))

        assert len(txt_files) == 1
        assert len(xml_files) == 1
        assert txt_files[0].read_text() == "Integration test content"

        # Test cleanup paths identification
        cleanup_paths = staging_manager.get_paths_for_cleanup(barcode)
        assert len(cleanup_paths) == 3  # encrypted, decrypted, extracted_dir
        assert extracted_dir in cleanup_paths
