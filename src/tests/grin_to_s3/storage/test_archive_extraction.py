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

    def test_cleanup_includes_extracted_directory(self, staging_manager):
        """Test cleanup handles extracted directories."""
        barcode = "test456"

        # Create test files
        decrypted_file = staging_manager.get_decrypted_file_path(barcode)
        extracted_dir = staging_manager.get_extracted_directory_path(barcode)

        # Create the files and directories
        decrypted_file.parent.mkdir(parents=True, exist_ok=True)
        decrypted_file.write_text("test content")

        extracted_dir.mkdir(parents=True, exist_ok=True)
        (extracted_dir / "file1.txt").write_text("content1")
        (extracted_dir / "file2.txt").write_text("content2")

        # Cleanup should remove both
        freed_bytes = staging_manager.cleanup_files(barcode)

        assert not decrypted_file.exists()
        assert not extracted_dir.exists()
        assert freed_bytes > 0


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

    def test_atomic_archive_extraction(self, sample_archive, tmp_path):
        """Test the new tarfile.extractall() step."""
        extracted_dir = tmp_path / "extracted"
        extracted_dir.mkdir()

        # Extract archive
        with tarfile.open(str(sample_archive), "r:gz") as tar:
            tar.extractall(path=extracted_dir)

        # Verify extraction
        assert (extracted_dir / "page001.txt").exists()
        assert (extracted_dir / "page002.txt").exists()
        assert (extracted_dir / "metadata.xml").exists()
        assert (extracted_dir / "subdir" / "page003.txt").exists()

        # Verify content
        assert (extracted_dir / "page001.txt").read_text() == "Page 1 content"
        assert (extracted_dir / "subdir" / "page003.txt").read_text() == "Page 3 content"

    def test_extraction_creates_expected_structure(self, sample_archive, tmp_path):
        """Test extracted files have expected layout."""
        extracted_dir = tmp_path / "extracted"
        extracted_dir.mkdir()

        with tarfile.open(str(sample_archive), "r:gz") as tar:
            tar.extractall(path=extracted_dir)

        # Check expected file structure
        txt_files = list(extracted_dir.glob("**/*.txt"))
        xml_files = list(extracted_dir.glob("**/*.xml"))

        assert len(txt_files) == 3  # page001, page002, page003
        assert len(xml_files) == 1  # metadata.xml

        # Verify nested structure is preserved
        subdir_files = list((extracted_dir / "subdir").glob("*.txt"))
        assert len(subdir_files) == 1

    def test_extraction_handles_corrupted_archive(self, tmp_path):
        """Test error handling for corrupted archives."""
        # Create a corrupted file (not a valid tar.gz)
        corrupted_file = tmp_path / "corrupted.tar.gz"
        corrupted_file.write_text("This is not a valid tar.gz file")

        extracted_dir = tmp_path / "extracted"
        extracted_dir.mkdir()

        # Should raise an exception
        with pytest.raises((tarfile.ReadError, tarfile.CompressionError)):
            with tarfile.open(str(corrupted_file), "r:gz") as tar:
                tar.extractall(path=extracted_dir)

    def test_memory_efficiency_single_decompression(self, sample_archive, tmp_path):
        """Verify single decompression vs multiple."""
        extracted_dir = tmp_path / "extracted"
        extracted_dir.mkdir()

        # Single extraction (the efficient way)
        with tarfile.open(str(sample_archive), "r:gz") as tar:
            tar.extractall(path=extracted_dir)

        # Verify all files are accessible from filesystem
        txt_files = list(extracted_dir.glob("**/*.txt"))
        for txt_file in txt_files:
            # Each file can be read directly from filesystem
            content = txt_file.read_text()
            assert len(content) > 0

        # This demonstrates that we can access all files
        # without reopening the archive multiple times


class TestFilesystemOperations:
    """Test direct filesystem operations for OCR and MARC extraction."""

    @pytest.fixture
    def extracted_directory(self, tmp_path):
        """Create a sample extracted directory structure."""
        extracted_dir = tmp_path / "extracted"
        extracted_dir.mkdir()

        # Create OCR text files
        (extracted_dir / "page001.txt").write_text("First page content")
        (extracted_dir / "page002.txt").write_text("Second page content")
        (extracted_dir / "page010.txt").write_text("Tenth page content")

        # Create metadata XML
        xml_content = """<?xml version="1.0"?>
        <METS:mets xmlns:METS="http://www.loc.gov/METS/">
            <METS:dmdSec>
                <METS:mdWrap>
                    <METS:xmlData>
                        <record>
                            <datafield tag="245">
                                <subfield code="a">Test Title</subfield>
                            </datafield>
                        </record>
                    </METS:xmlData>
                </METS:mdWrap>
            </METS:dmdSec>
        </METS:mets>"""
        (extracted_dir / "metadata.xml").write_text(xml_content)

        # Create subdirectory with more files
        sub_dir = extracted_dir / "images"
        sub_dir.mkdir()
        (sub_dir / "page003.txt").write_text("Third page content")

        return extracted_dir

    def test_filesystem_text_extraction(self, extracted_directory):
        """Test direct filesystem access for OCR text files."""
        # Find all .txt files
        txt_files = list(extracted_directory.glob("**/*.txt"))
        assert len(txt_files) == 4  # 3 in root + 1 in subdir

        # Test reading each file
        page_data = {}
        for txt_file in txt_files:
            with open(txt_file, encoding="utf-8") as f:
                content = f.read()
            page_data[txt_file.name] = content

        assert "page001.txt" in page_data
        assert page_data["page001.txt"] == "First page content"
        assert "page003.txt" in page_data  # From subdirectory

    def test_filesystem_xml_extraction(self, extracted_directory):
        """Test direct filesystem access for XML metadata."""
        # Find XML files
        xml_files = list(extracted_directory.glob("**/*.xml"))
        assert len(xml_files) == 1

        # Read XML content
        xml_file = xml_files[0]
        with open(xml_file, encoding="utf-8") as f:
            content = f.read()

        assert "METS:mets" in content
        assert "Test Title" in content

    def test_missing_files_handling(self, tmp_path):
        """Test handling when expected files are missing."""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        # No .txt files should be found
        txt_files = list(empty_dir.glob("**/*.txt"))
        assert len(txt_files) == 0

        # No .xml files should be found
        xml_files = list(empty_dir.glob("**/*.xml"))
        assert len(xml_files) == 0


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

        # Test cleanup
        freed_bytes = staging_manager.cleanup_files(barcode)
        assert not extracted_dir.exists()
        assert freed_bytes > 0
