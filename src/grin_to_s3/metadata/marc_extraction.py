"""MARC metadata extraction from METS XML files in Google Books archives."""

import logging
import xml.etree.ElementTree as ET
from pathlib import Path

logger = logging.getLogger(__name__)

# METS and MARC XML namespaces as found in Google Books archives
METS_NAMESPACE = "http://www.loc.gov/METS/"
MARC_NAMESPACE = "http://www.loc.gov/MARC21/slim"

# Register namespaces for XPath queries
ET.register_namespace("mets", METS_NAMESPACE)
ET.register_namespace("slim", MARC_NAMESPACE)

NAMESPACES = {
    "mets": METS_NAMESPACE,
    "slim": MARC_NAMESPACE,
}


def extract_marc_metadata(extracted_dir_path: str | Path) -> dict[str, str | None]:
    """
    Extract MARC metadata from METS XML in extracted archive directory.

    Args:
        extracted_dir_path: Path to extracted archive directory (not .tar.gz file)

    Returns:
        Dictionary of MARC fields extracted from the METS XML.
        Returns empty dict if no METS XML found or extraction fails.

    Fields extracted match the v1 implementation:
        - control_number: MARC 001 field
        - date_type: Date type from MARC 008 field
        - date1: First date from MARC 008 field
        - date2: Second date from MARC 008 field
        - language: Language from MARC 008 field
        - loc_control_number: MARC 010 field
        - loc_call_number: MARC 050 field
        - isbn: MARC 020 field
        - subject: MARC 650 field (subject headings)
        - genre: MARC 655 field (genre/form terms)
        - note: MARC 500 field (general notes)
        - title: MARC 245 subfield a (title proper)
        - title_remainder: MARC 245 subfield b (remainder of title)
        - author100: MARC 100 field (personal name)
        - author110: MARC 110 field (corporate name)
        - author111: MARC 111 field (meeting name)
        - oclc: MARC 035 field (OCLC numbers)
    """
    try:
        mets_xml_content = _extract_mets_from_directory(extracted_dir_path)
        if not mets_xml_content:
            logger.warning(f"No METS XML found in directory: {extracted_dir_path}")
            return {}

        return _parse_marc_from_mets(mets_xml_content)

    except Exception as e:
        logger.error(f"Failed to extract MARC metadata from directory {extracted_dir_path}: {e}")
        return {}


def _extract_mets_from_directory(extracted_dir_path: str | Path) -> str | None:
    """
    Extract the METS XML content from extracted archive directory.

    Args:
        extracted_dir_path: Path to extracted directory

    Returns:
        XML content as string, or None if not found
    """
    try:
        extracted_dir = Path(extracted_dir_path)

        # Find XML files in extracted directory
        xml_files = list(extracted_dir.glob("**/*.xml"))

        if not xml_files:
            logger.debug(f"No XML files found in directory: {extracted_dir_path}")
            return None

        # Try each XML file until we find one that works
        for xml_file in xml_files:
            try:
                with open(xml_file, encoding="utf-8") as f:
                    content = f.read()
                logger.debug(f"Extracted XML content from {xml_file}")
                return content
            except Exception as e:
                logger.warning(f"Failed to read {xml_file}: {e}")
                continue

    except Exception as e:
        logger.error(f"Failed to access directory {extracted_dir_path}: {e}")

    return None


def _parse_marc_from_mets(mets_content: str) -> dict[str, str | None]:
    """
    Parse MARC metadata from METS XML content.

    Args:
        mets_content: XML content as string

    Returns:
        Dictionary of extracted MARC fields
    """
    try:
        root = ET.fromstring(mets_content)

        # Find the MARC record within the METS structure
        # Path: .//mets:dmdSec/mets:mdWrap/mets:xmlData/slim:record
        marc_record = root.find(".//mets:dmdSec/mets:mdWrap/mets:xmlData/slim:record", NAMESPACES)

        if marc_record is None:
            logger.debug("No MARC record found in METS XML")
            return {}

        return _extract_marc_fields(marc_record)

    except ET.ParseError as e:
        logger.error(f"Failed to parse METS XML: {e}")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error parsing METS XML: {e}")
        return {}


def _extract_marc_fields(marc_record: ET.Element) -> dict[str, str | None]:
    """
    Extract specific MARC fields from a MARC record element.

    Args:
        marc_record: The MARC record XML element

    Returns:
        Dictionary of extracted field values
    """
    fields = {}

    # Control fields (001, 008, etc.)
    fields["control_number"] = _get_controlfield(marc_record, "001")

    # Extract components from 008 field
    field_008 = _get_controlfield(marc_record, "008")
    if field_008 and len(field_008) >= 40:
        # MARC 008 positions for date and language info
        fields["date1"] = _extract_date1_from_008(field_008)
        fields["date2"] = _extract_date2_from_008(field_008)
        fields["date_type"] = _extract_date_type_from_008(field_008)
        fields["language"] = _extract_language_from_008(field_008)
    else:
        fields["date1"] = None
        fields["date2"] = None
        fields["date_type"] = None
        fields["language"] = None

    # Data fields with subfields - single values
    fields["loc_control_number"] = _get_datafield_subfield(marc_record, "010", "a")
    fields["title"] = _get_datafield_subfield(marc_record, "245", "a")
    fields["title_remainder"] = _get_datafield_subfield(marc_record, "245", "b")
    fields["author100"] = _get_datafield_subfield(marc_record, "100", "a")
    fields["author110"] = _get_datafield_subfield(marc_record, "110", "a")
    fields["author111"] = _get_datafield_subfield(marc_record, "111", "a")

    # Data fields with subfields - repeating values (joined with ", ")
    fields["loc_call_number"] = _get_datafield_subfield_list(marc_record, "050", "a")
    fields["isbn"] = _get_datafield_subfield_list(marc_record, "020", "a")
    fields["subject"] = _get_datafield_subfield_list(marc_record, "650", "a", strip_periods=True)
    fields["genre"] = _get_datafield_subfield_list(marc_record, "655", "a", strip_periods=True)
    fields["note"] = _get_datafield_subfield_list(marc_record, "500", "a")

    # OCLC numbers from 035 field (filter for OCoLC prefix)
    oclc_string = _get_datafield_subfield_list(marc_record, "035", "a")
    if oclc_string:
        oclc_numbers = oclc_string.split(", ")
        oclc_filtered = [num.strip() for num in oclc_numbers if num.strip() and "(OCoLC)" in num]
        fields["oclc"] = ", ".join(oclc_filtered) if oclc_filtered else None
    else:
        fields["oclc"] = None

    return fields


def _get_controlfield(record: ET.Element, tag: str) -> str | None:
    """Get value from a MARC controlfield."""
    field = record.find(f".//slim:controlfield[@tag='{tag}']", NAMESPACES)
    return field.text.strip() if field is not None and field.text else None


def _get_datafield_subfield(record: ET.Element, tag: str, subfield: str) -> str | None:
    """Get value from a MARC datafield subfield (first occurrence)."""
    field = record.find(f".//slim:datafield[@tag='{tag}']/slim:subfield[@code='{subfield}']", NAMESPACES)
    return field.text.strip() if field is not None and field.text else None


def _get_datafield_subfield_list(record: ET.Element, tag: str, subfield: str, strip_periods: bool = False) -> str | None:
    """Get all values from MARC datafield subfields, joined with commas."""
    fields = record.findall(f".//slim:datafield[@tag='{tag}']/slim:subfield[@code='{subfield}']", NAMESPACES)
    if fields:
        values = []
        for f in fields:
            if f.text:
                text = f.text.strip()
                if strip_periods:
                    text = text.rstrip(".")
                values.append(text)
        return ", ".join(values) if values else None
    return None


def _extract_date1_from_008(field_008: str) -> str | None:
    """Extract date1 from positions 7-10 of MARC 008 field."""
    if len(field_008) >= 11:
        date_str = field_008[7:11]
        # Filter out placeholder values (9999 or containing 9s indicating unknown)
        if date_str and date_str.isdigit() and "999" not in date_str:
            return date_str
    return None


def _extract_date2_from_008(field_008: str) -> str | None:
    """Extract date2 from positions 11-14 of MARC 008 field."""
    if len(field_008) >= 15:
        date_str = field_008[11:15]
        # Filter out placeholder values (9999 or containing 9s indicating unknown)
        if date_str and date_str.isdigit() and "999" not in date_str:
            return date_str
    return None


def _extract_date_type_from_008(field_008: str) -> str | None:
    """Extract date type from position 6 of MARC 008 field."""
    if len(field_008) >= 7:
        return field_008[6] if field_008[6] != " " else None
    return None


def _extract_language_from_008(field_008: str) -> str | None:
    """Extract language from positions 35-37 of MARC 008 field."""
    if len(field_008) >= 38:
        lang = field_008[35:38]
        return lang if lang and lang != "   " else None
    return None

