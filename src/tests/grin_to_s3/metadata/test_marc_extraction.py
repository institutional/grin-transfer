"""Tests for MARC metadata extraction from METS XML files."""

import io
import tarfile
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

from grin_to_s3.metadata.marc_extraction import (
    _extract_date1_from_008,
    _extract_date2_from_008,
    _extract_date_type_from_008,
    _extract_language_from_008,
    _extract_marc_fields,
    _extract_mets_from_archive,
    _get_controlfield,
    _get_datafield_subfield,
    _get_datafield_subfield_list,
    _parse_marc_from_mets,
    extract_marc_metadata,
)

# Sample METS XML content with embedded MARC record (simplified for testing)
SAMPLE_METS_XML = """<?xml version="1.0" encoding="UTF-8"?>
<mets:mets xmlns:mets="http://www.loc.gov/METS/" xmlns:slim="http://www.loc.gov/MARC21/slim">
    <mets:dmdSec ID="DMDMARC">
        <mets:mdWrap MIMETYPE="text/xml" MDTYPE="MARC">
            <mets:xmlData>
                <slim:record>
                    <slim:leader>03209cas a2200541I  4500</slim:leader>
                    <slim:controlfield tag="001">000128102</slim:controlfield>
                    <slim:controlfield tag="008">750727c17909999gw br p       0   b1gerdd</slim:controlfield>
                    <slim:datafield tag="010" ind1=" " ind2=" ">
                        <slim:subfield code="a">  76647114</slim:subfield>
                    </slim:datafield>
                    <slim:datafield tag="020" ind1=" " ind2=" ">
                        <slim:subfield code="a">9780123456789</slim:subfield>
                    </slim:datafield>
                    <slim:datafield tag="035" ind1=" " ind2=" ">
                        <slim:subfield code="a">(OCoLC)12345678</slim:subfield>
                    </slim:datafield>
                    <slim:datafield tag="035" ind1=" " ind2=" ">
                        <slim:subfield code="a">987654321</slim:subfield>
                    </slim:datafield>
                    <slim:datafield tag="050" ind1="0" ind2="0">
                        <slim:subfield code="a">QC1</slim:subfield>
                        <slim:subfield code="b">.A6</slim:subfield>
                    </slim:datafield>
                    <slim:datafield tag="100" ind1="1" ind2=" ">
                        <slim:subfield code="a">Einstein, Albert,</slim:subfield>
                        <slim:subfield code="d">1879-1955.</slim:subfield>
                    </slim:datafield>
                    <slim:datafield tag="245" ind1="0" ind2="0">
                        <slim:subfield code="a">Annalen der Physik.</slim:subfield>
                        <slim:subfield code="b">A comprehensive physics journal</slim:subfield>
                    </slim:datafield>
                    <slim:datafield tag="500" ind1=" " ind2=" ">
                        <slim:subfield code="a">General note about the publication.</slim:subfield>
                    </slim:datafield>
                    <slim:datafield tag="500" ind1=" " ind2=" ">
                        <slim:subfield code="a">Another note about the work.</slim:subfield>
                    </slim:datafield>
                    <slim:datafield tag="650" ind1=" " ind2="0">
                        <slim:subfield code="a">Physics.</slim:subfield>
                    </slim:datafield>
                    <slim:datafield tag="650" ind1=" " ind2="0">
                        <slim:subfield code="a">Science</slim:subfield>
                    </slim:datafield>
                    <slim:datafield tag="655" ind1=" " ind2="7">
                        <slim:subfield code="a">Academic journals.</slim:subfield>
                    </slim:datafield>
                </slim:record>
            </mets:xmlData>
        </mets:mdWrap>
    </mets:dmdSec>
</mets:mets>"""

# Malformed XML for error testing
MALFORMED_XML = """<?xml version="1.0" encoding="UTF-8"?>
<mets:mets xmlns:mets="http://www.loc.gov/METS/">
    <unclosed_tag>
</mets:mets>"""

# METS without MARC record
METS_NO_MARC = """<?xml version="1.0" encoding="UTF-8"?>
<mets:mets xmlns:mets="http://www.loc.gov/METS/">
    <mets:dmdSec ID="DMDOTHER">
        <mets:mdWrap MIMETYPE="text/xml" MDTYPE="OTHER">
            <mets:xmlData>
                <other_metadata>Not MARC</other_metadata>
            </mets:xmlData>
        </mets:mdWrap>
    </mets:dmdSec>
</mets:mets>"""


class TestMARCFieldExtraction:
    """Test individual MARC field extraction functions."""

    def test_extract_dates_from_008_field(self):
        """Test date extraction from MARC 008 field."""
        # Format: positions 0-5 (entry date), 6 (date type), 7-10 (date1), 11-14 (date2), etc.
        field_008 = "750727c17909999gw br p       0   b1gerdd"

        assert _extract_date1_from_008(field_008) == "1790"
        assert _extract_date2_from_008(field_008) is None  # 9999 is filtered out
        assert _extract_date_type_from_008(field_008) == "c"
        assert _extract_language_from_008(field_008) == "ger"

    def test_extract_dates_with_placeholder_values(self):
        """Test that placeholder values like 9999 are filtered out."""
        field_008 = "750727s19999999en            000 0 eng d"

        assert _extract_date1_from_008(field_008) is None  # 9999 filtered
        assert _extract_date2_from_008(field_008) is None  # 9999 filtered
        assert _extract_date_type_from_008(field_008) == "s"
        assert _extract_language_from_008(field_008) == "eng"

    def test_extract_dates_short_field(self):
        """Test date extraction with short 008 field."""
        field_008_short = "750727"

        assert _extract_date1_from_008(field_008_short) is None
        assert _extract_date2_from_008(field_008_short) is None
        assert _extract_date_type_from_008(field_008_short) is None
        assert _extract_language_from_008(field_008_short) is None

    def test_extract_language_with_spaces(self):
        """Test language extraction when field contains spaces."""
        field_008 = "750727s1990    en            000 0    d"
        assert _extract_language_from_008(field_008) is None  # Spaces filtered


class TestMETSParsing:
    """Test METS XML parsing and MARC extraction."""

    def test_parse_valid_mets_xml(self):
        """Test parsing valid METS XML with MARC record."""
        result = _parse_marc_from_mets(SAMPLE_METS_XML)

        assert result["control_number"] == "000128102"
        assert result["date1"] == "1790"
        assert result["date2"] is None  # 9999 filtered out
        assert result["date_type"] == "c"
        assert result["language"] == "ger"
        assert result["loc_control_number"] == "76647114"
        assert result["isbn"] == "9780123456789"
        assert result["title"] == "Annalen der Physik."
        assert result["title_remainder"] == "A comprehensive physics journal"
        assert result["author100"] == "Einstein, Albert,"
        assert result["loc_call_number"] == "QC1"
        assert result["subject"] == "Physics, Science"
        assert result["genre"] == "Academic journals"
        assert result["note"] == "General note about the publication., Another note about the work."
        assert result["oclc"] == "(OCoLC)12345678"  # Only OCoLC prefixed values

    def test_parse_mets_without_marc(self):
        """Test parsing METS XML without MARC record."""
        result = _parse_marc_from_mets(METS_NO_MARC)
        assert result == {}

    def test_parse_malformed_xml(self):
        """Test parsing malformed XML returns empty dict."""
        result = _parse_marc_from_mets(MALFORMED_XML)
        assert result == {}

    def test_parse_empty_xml(self):
        """Test parsing empty string returns empty dict."""
        result = _parse_marc_from_mets("")
        assert result == {}


class TestArchiveExtraction:
    """Test extraction of METS XML from tar.gz archives."""

    def test_extract_mets_from_valid_archive(self):
        """Test extracting METS XML from a valid tar.gz archive."""
        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as temp_archive:
            # Create a test tar.gz archive with XML content
            with tarfile.open(temp_archive.name, "w:gz") as tar:
                # Add the XML content as a file in the archive
                xml_info = tarfile.TarInfo(name="test_book.xml")
                xml_info.size = len(SAMPLE_METS_XML.encode("utf-8"))
                tar.addfile(xml_info, fileobj=io.BytesIO(SAMPLE_METS_XML.encode("utf-8")))

            temp_path = Path(temp_archive.name)
            try:
                result = _extract_mets_from_archive(temp_path)
                assert result == SAMPLE_METS_XML
            finally:
                temp_path.unlink()

    def test_extract_mets_from_archive_no_xml(self):
        """Test extracting from archive with no XML files."""
        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as temp_archive:
            # Create archive with non-XML file
            with tarfile.open(temp_archive.name, "w:gz") as tar:
                txt_info = tarfile.TarInfo(name="test.txt")
                txt_content = b"Not XML content"
                txt_info.size = len(txt_content)
                tar.addfile(txt_info, fileobj=io.BytesIO(txt_content))

            temp_path = Path(temp_archive.name)
            try:
                result = _extract_mets_from_archive(temp_path)
                assert result is None
            finally:
                temp_path.unlink()

    def test_extract_mets_nonexistent_archive(self):
        """Test extracting from non-existent archive."""
        result = _extract_mets_from_archive("/nonexistent/archive.tar.gz")
        assert result is None

    def test_extract_mets_corrupted_archive(self):
        """Test extracting from corrupted archive."""
        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as temp_file:
            # Write non-tar content
            temp_file.write(b"This is not a tar.gz file")
            temp_file.flush()

            temp_path = Path(temp_file.name)
            try:
                result = _extract_mets_from_archive(temp_path)
                assert result is None
            finally:
                temp_path.unlink()


class TestEndToEndExtraction:
    """Test complete MARC metadata extraction workflow."""

    def test_extract_marc_metadata_success(self):
        """Test successful end-to-end MARC metadata extraction."""
        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as temp_archive:
            # Create archive with METS XML
            with tarfile.open(temp_archive.name, "w:gz") as tar:
                xml_info = tarfile.TarInfo(name="book_metadata.xml")
                xml_info.size = len(SAMPLE_METS_XML.encode("utf-8"))
                tar.addfile(xml_info, fileobj=io.BytesIO(SAMPLE_METS_XML.encode("utf-8")))

            temp_path = Path(temp_archive.name)
            try:
                result = extract_marc_metadata(temp_path)

                # Verify key fields are extracted
                assert result["control_number"] == "000128102"
                assert result["title"] == "Annalen der Physik."
                assert result["author100"] == "Einstein, Albert,"
                assert result["date1"] == "1790"
                assert result["language"] == "ger"
                assert len(result) > 10  # Should have many fields

            finally:
                temp_path.unlink()

    def test_extract_marc_metadata_no_mets(self):
        """Test extraction when archive has no METS XML."""
        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as temp_archive:
            # Create archive without XML
            with tarfile.open(temp_archive.name, "w:gz") as tar:
                txt_info = tarfile.TarInfo(name="readme.txt")
                txt_content = b"No XML here"
                txt_info.size = len(txt_content)
                tar.addfile(txt_info, fileobj=io.BytesIO(txt_content))

            temp_path = Path(temp_archive.name)
            try:
                result = extract_marc_metadata(temp_path)
                assert result == {}
            finally:
                temp_path.unlink()

    def test_extract_marc_metadata_invalid_archive(self):
        """Test extraction with invalid archive path."""
        result = extract_marc_metadata("/invalid/path/archive.tar.gz")
        assert result == {}

    def test_extract_marc_metadata_malformed_mets(self):
        """Test extraction with malformed METS XML."""
        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as temp_archive:
            # Create archive with malformed XML
            with tarfile.open(temp_archive.name, "w:gz") as tar:
                xml_info = tarfile.TarInfo(name="bad_metadata.xml")
                xml_info.size = len(MALFORMED_XML.encode("utf-8"))
                tar.addfile(xml_info, fileobj=io.BytesIO(MALFORMED_XML.encode("utf-8")))

            temp_path = Path(temp_archive.name)
            try:
                result = extract_marc_metadata(temp_path)
                assert result == {}
            finally:
                temp_path.unlink()


class TestHelperFunctions:
    """Test helper functions for XML parsing."""

    def test_get_controlfield_existing(self):
        """Test getting existing controlfield."""
        xml_content = """<slim:record xmlns:slim="http://www.loc.gov/MARC21/slim">
            <slim:controlfield tag="001">123456</slim:controlfield>
        </slim:record>"""

        root = ET.fromstring(xml_content)
        result = _get_controlfield(root, "001")
        assert result == "123456"

    def test_get_controlfield_missing(self):
        """Test getting non-existent controlfield."""
        xml_content = """<slim:record xmlns:slim="http://www.loc.gov/MARC21/slim">
            <slim:controlfield tag="001">123456</slim:controlfield>
        </slim:record>"""

        root = ET.fromstring(xml_content)
        result = _get_controlfield(root, "999")
        assert result is None

    def test_get_datafield_subfield_existing(self):
        """Test getting existing datafield subfield."""
        xml_content = """<slim:record xmlns:slim="http://www.loc.gov/MARC21/slim">
            <slim:datafield tag="245" ind1="0" ind2="0">
                <slim:subfield code="a">Test Title</slim:subfield>
            </slim:datafield>
        </slim:record>"""

        root = ET.fromstring(xml_content)
        result = _get_datafield_subfield(root, "245", "a")
        assert result == "Test Title"

    def test_get_datafield_subfield_missing(self):
        """Test getting non-existent datafield subfield."""
        xml_content = """<slim:record xmlns:slim="http://www.loc.gov/MARC21/slim">
            <slim:datafield tag="245" ind1="0" ind2="0">
                <slim:subfield code="a">Test Title</slim:subfield>
            </slim:datafield>
        </slim:record>"""

        root = ET.fromstring(xml_content)
        result = _get_datafield_subfield(root, "245", "b")
        assert result is None

    def test_get_datafield_subfield_list_multiple(self):
        """Test getting multiple datafield subfields."""
        xml_content = """<slim:record xmlns:slim="http://www.loc.gov/MARC21/slim">
            <slim:datafield tag="650" ind1=" " ind2="0">
                <slim:subfield code="a">Physics</slim:subfield>
            </slim:datafield>
            <slim:datafield tag="650" ind1=" " ind2="0">
                <slim:subfield code="a">Science</slim:subfield>
            </slim:datafield>
        </slim:record>"""

        root = ET.fromstring(xml_content)
        result = _get_datafield_subfield_list(root, "650", "a")
        assert result == "Physics, Science"

    def test_get_datafield_subfield_list_empty(self):
        """Test getting empty datafield subfield list."""
        xml_content = """<slim:record xmlns:slim="http://www.loc.gov/MARC21/slim">
            <slim:controlfield tag="001">123456</slim:controlfield>
        </slim:record>"""

        root = ET.fromstring(xml_content)
        result = _get_datafield_subfield_list(root, "650", "a")
        assert result is None

    def test_get_datafield_subfield_list_with_period_stripping(self):
        """Test getting multiple datafield subfields with period stripping."""
        xml_content = """<slim:record xmlns:slim="http://www.loc.gov/MARC21/slim">
            <slim:datafield tag="650" ind1=" " ind2="0">
                <slim:subfield code="a">Physics.</slim:subfield>
            </slim:datafield>
            <slim:datafield tag="650" ind1=" " ind2="0">
                <slim:subfield code="a">Science.</slim:subfield>
            </slim:datafield>
        </slim:record>"""

        root = ET.fromstring(xml_content)
        result = _get_datafield_subfield_list(root, "650", "a", strip_periods=True)
        assert result == "Physics, Science"

    def test_get_datafield_subfield_list_without_period_stripping(self):
        """Test getting multiple datafield subfields without period stripping."""
        xml_content = """<slim:record xmlns:slim="http://www.loc.gov/MARC21/slim">
            <slim:datafield tag="500" ind1=" " ind2=" ">
                <slim:subfield code="a">First note.</slim:subfield>
            </slim:datafield>
            <slim:datafield tag="500" ind1=" " ind2=" ">
                <slim:subfield code="a">Second note.</slim:subfield>
            </slim:datafield>
        </slim:record>"""

        root = ET.fromstring(xml_content)
        result = _get_datafield_subfield_list(root, "500", "a", strip_periods=False)
        assert result == "First note., Second note."


class TestOCLCFiltering:
    """Test OCLC number filtering functionality."""

    def test_oclc_filtering_with_mixed_values(self):
        """Test that OCLC filtering only includes (OCoLC) prefixed values."""
        xml_content = """<slim:record xmlns:slim="http://www.loc.gov/MARC21/slim">
            <slim:controlfield tag="001">123456</slim:controlfield>
            <slim:controlfield tag="008">750727c17909999gw br p       0   b1gerdd</slim:controlfield>
            <slim:datafield tag="035" ind1=" " ind2=" ">
                <slim:subfield code="a">(OCoLC)12345678</slim:subfield>
            </slim:datafield>
            <slim:datafield tag="035" ind1=" " ind2=" ">
                <slim:subfield code="a">987654321</slim:subfield>
            </slim:datafield>
            <slim:datafield tag="035" ind1=" " ind2=" ">
                <slim:subfield code="a">(OCoLC)87654321</slim:subfield>
            </slim:datafield>
            <slim:datafield tag="035" ind1=" " ind2=" ">
                <slim:subfield code="a">(DLC)somevalue</slim:subfield>
            </slim:datafield>
        </slim:record>"""

        root = ET.fromstring(xml_content)
        result = _extract_marc_fields(root)
        assert result["oclc"] == "(OCoLC)12345678, (OCoLC)87654321"

    def test_oclc_filtering_no_oclc_values(self):
        """Test OCLC filtering when no (OCoLC) values present."""
        xml_content = """<slim:record xmlns:slim="http://www.loc.gov/MARC21/slim">
            <slim:controlfield tag="001">123456</slim:controlfield>
            <slim:controlfield tag="008">750727c17909999gw br p       0   b1gerdd</slim:controlfield>
            <slim:datafield tag="035" ind1=" " ind2=" ">
                <slim:subfield code="a">987654321</slim:subfield>
            </slim:datafield>
            <slim:datafield tag="035" ind1=" " ind2=" ">
                <slim:subfield code="a">(DLC)somevalue</slim:subfield>
            </slim:datafield>
        </slim:record>"""

        root = ET.fromstring(xml_content)
        result = _extract_marc_fields(root)
        assert result["oclc"] is None
