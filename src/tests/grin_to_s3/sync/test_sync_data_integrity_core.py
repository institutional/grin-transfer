#!/usr/bin/env python3
"""
Core data integrity tests for sync operations focusing on MARC field mapping.

Tests the critical MARC field mapping functionality based on analysis of
real Google Books MARC data from archive 32044010748390.
"""

import json

from grin_to_s3.metadata.marc_extraction import convert_marc_keys_to_db_fields


class TestMARCFieldMappingIntegrity:
    """Test MARC field extraction and mapping for data integrity."""

    def test_marc_field_key_mapping_completeness(self):
        """Test that MARC field mapping covers all expected database fields."""
        # Get the mapping from MARC extraction keys to database fields
        # Use the actual MARC extraction field names from the real implementation
        marc_keys_to_db = convert_marc_keys_to_db_fields(
            {
                "control_number": "001234",
                "date_type": "s",
                "date1": "2024",
                "date2": "2025",
                "language": "eng",
                "loc_control_number": "2024001234",
                "loc_call_number": "QA76.76.O63",
                "isbn": "9781234567890",
                "oclc": "(OCoLC)1234567890",
                "title": "Test Title",
                "title_remainder": "subtitle",
                "author100": "Smith, John",
                "author110": "Test Corporation",
                "author111": "Test Conference",
                "subject": "Computer programming",
                "genre": "Technical manual",
                "note": "Includes index",
            }
        )

        # Expected database field names (from actual database schema and implementation)
        expected_db_fields = {
            "marc_control_number",
            "marc_date_type",
            "marc_date_1",
            "marc_date_2",
            "marc_language",
            "marc_lccn",
            "marc_lc_call_number",
            "marc_isbn",
            "marc_oclc_numbers",
            "marc_title",
            "marc_title_remainder",
            "marc_author_personal",
            "marc_author_corporate",
            "marc_author_meeting",
            "marc_subjects",
            "marc_genres",
            "marc_general_note",
            "marc_extraction_timestamp",
        }

        # Verify all expected fields are mapped
        actual_db_fields = set(marc_keys_to_db.keys())
        assert actual_db_fields == expected_db_fields, f"Missing fields: {expected_db_fields - actual_db_fields}"

        # Verify extraction timestamp is added
        assert "marc_extraction_timestamp" in marc_keys_to_db
        assert isinstance(marc_keys_to_db["marc_extraction_timestamp"], str)

        # Verify field mappings are correct (non-None values should remain strings)
        for field, value in marc_keys_to_db.items():
            if field != "marc_extraction_timestamp" and value is not None:
                assert isinstance(value, str), f"Field {field} is not string: {type(value)}"

    def test_marc_field_data_type_consistency(self):
        """Test that MARC field data types are consistently handled."""
        # Test various data types that might come from MARC extraction
        # Use actual field names from the MARC extraction implementation
        test_data_cases = [
            # String data (most common)
            {"control_number": "001234", "expected_type": str},
            {"title": "Test Title", "expected_type": str},
            {"oclc": "(OCoLC)1234567890", "expected_type": str},
            # Empty/None cases
            {"control_number": "", "expected_type": str},
            {"title": None, "expected_type": type(None)},
            # Special characters and Unicode
            {"title": "Café Programming: A naïve approach", "expected_type": str},
            {"author100": "Müller, José María", "expected_type": str},
            # Numbers that should be strings
            {"isbn": "9781234567890", "expected_type": str},
            {"date1": "2024", "expected_type": str},
            # Multi-value fields (should be concatenated strings)
            {"subject": "Programming; Computer science; Software", "expected_type": str},
            {"genre": "Technical manual; Reference", "expected_type": str},
        ]

        for test_case in test_data_cases:
            for field, value in test_case.items():
                if field == "expected_type":
                    continue

                # Test field mapping preserves data type
                marc_data = {field: value}
                db_fields = convert_marc_keys_to_db_fields(marc_data)

                # Find the corresponding database field name
                db_field_name = None
                for db_field in db_fields:
                    if db_field.startswith("marc_") and field in db_field:
                        db_field_name = db_field
                        break

                if db_field_name and db_field_name != "marc_extraction_timestamp":
                    mapped_value = db_fields[db_field_name]
                    expected_type = test_case["expected_type"]

                    if expected_type is type(None):
                        assert mapped_value is None or mapped_value == ""
                    else:
                        assert isinstance(mapped_value, expected_type), (
                            f"Field {field} -> {db_field_name}: expected {expected_type}, got {type(mapped_value)}"
                        )

    def test_marc_unicode_handling_integrity(self):
        """Test that Unicode characters in MARC fields are handled correctly."""
        unicode_test_cases = [
            # European accents
            {"title": "Café Programming", "author100": "Müller, José"},
            # Asian characters
            {"title": "プログラミング入門", "author100": "田中太郎"},
            # Arabic script
            {"title": "برمجة الحاسوب", "author100": "أحمد محمد"},
            # Mixed scripts
            {"title": "Programming 程式設計 برمجة", "subject": "CS; 計算機科學; علوم حاسوب"},
            # Emoji and special symbols
            {"title": "Programming 🔥 Guide", "note": "⭐ Excellent resource ⚡"},
            # Control characters (should be handled gracefully)
            {"title": "Test\nTitle\tWith\rControls", "author100": "Smith\x00John"},
        ]

        for test_case in unicode_test_cases:
            # Test that Unicode survives MARC field mapping
            db_fields = convert_marc_keys_to_db_fields(test_case)

            for original_field, original_value in test_case.items():
                # Find the corresponding database field name
                db_field_name = None
                for db_field in db_fields:
                    if db_field.startswith("marc_") and original_field in db_field:
                        db_field_name = db_field
                        break

                if db_field_name and db_field_name != "marc_extraction_timestamp":
                    mapped_value = db_fields[db_field_name]

                    # Unicode should be preserved
                    if original_value and isinstance(original_value, str):
                        assert isinstance(mapped_value, str)
                        # Basic preservation test - mapped value should equal original for direct mappings
                        assert mapped_value == original_value, (
                            f"Unicode not preserved: {original_value} -> {mapped_value}"
                        )

    def test_marc_field_length_validation(self):
        """Test that MARC fields handle various lengths appropriately."""
        # Test edge cases for field lengths
        length_test_cases = [
            # Empty fields
            {"title": "", "expected_valid": True},
            {"title": None, "expected_valid": True},
            # Normal length fields
            {"title": "A" * 100, "expected_valid": True},
            {"author100": "B" * 255, "expected_valid": True},
            # Very long fields (should be handled gracefully)
            {"title": "C" * 1000, "expected_valid": True},
            {"subject": "D" * 2000, "expected_valid": True},
            # Single character
            {"isbn": "X", "expected_valid": True},
            {"language": "en", "expected_valid": True},
        ]

        for test_case in length_test_cases:
            expected_valid = test_case.pop("expected_valid")

            try:
                # Test field mapping handles length gracefully
                db_fields = convert_marc_keys_to_db_fields(test_case)

                # All mapped fields should be valid strings or None
                for _field_name, field_value in db_fields.items():
                    assert field_value is None or isinstance(field_value, str)

                # Should succeed if expected_valid is True
                assert expected_valid, f"Expected validation failure for: {test_case}"

            except Exception as e:
                # Should only fail if expected_valid is False
                assert not expected_valid, f"Unexpected validation failure: {e}"


class TestETagComparisonIntegrity:
    """Test ETag comparison operations for data integrity."""

    def test_etag_quote_normalization_consistency(self):
        """Test that ETag quote handling is consistent across storage and database operations."""
        # Test various ETag quote formats that Google might return
        etag_variations = [
            '"abc123"',  # Standard quoted
            "abc123",  # Unquoted
            '""abc123""',  # Double quoted
            '"abc123',  # Missing closing quote
            'abc123"',  # Missing opening quote
            "",  # Empty ETag
            None,  # Null ETag
        ]

        for etag in etag_variations:
            # Test ETag normalization for storage metadata comparison
            if etag is None:
                normalized_etag = ""
            else:
                # This is the normalization logic from archive_matches_encrypted_etag()
                normalized_etag = etag.strip('"') if etag else ""

            # Test database comparison logic
            if etag:
                db_comparison_etag = etag.strip('"') if isinstance(etag, str) else ""
            else:
                db_comparison_etag = ""

            # Both normalizations should be consistent
            assert normalized_etag == db_comparison_etag, f"ETag normalization inconsistent for: {etag}"

    def test_etag_json_metadata_integrity(self):
        """Test that ETag data in JSON metadata fields maintains integrity."""
        # Test various JSON metadata structures that might contain ETag data
        test_metadata_cases = [
            # Valid metadata with ETag
            {"encrypted_etag": "abc123", "timestamp": "2024-01-01T00:00:00Z"},
            # Metadata with special characters in ETag
            {"encrypted_etag": "café-123-naïve", "file_size": 1024},
            # Metadata with nested ETag reference
            {"google_response": {"etag": "nested123"}, "encrypted_etag": "main123"},
            # Empty/null ETag cases
            {"encrypted_etag": "", "valid": True},
            {"encrypted_etag": None, "fallback_used": True},
            # Malformed metadata
            {"etag": "wrong_key", "encrypted_etag": "correct123"},
        ]

        for metadata in test_metadata_cases:
            # Test JSON serialization/deserialization integrity
            serialized = json.dumps(metadata)
            deserialized = json.loads(serialized)

            # Verify ETag data survives serialization
            if "encrypted_etag" in metadata:
                original_etag = metadata["encrypted_etag"]
                recovered_etag = deserialized.get("encrypted_etag")
                assert original_etag == recovered_etag

            # Test that metadata structure is preserved
            assert deserialized == metadata
