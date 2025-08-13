"""Parametrized test helpers for common test scenarios."""

import pytest

# Common test parameters
MEANINGFUL_STORAGE_TYPES = ["s3", "local"]  # Representative cloud + local for meaningful testing

EXTRACTION_SCENARIOS = [
    pytest.param(False, False, id="both_enabled"),
    pytest.param(True, False, id="skip_ocr"),
    pytest.param(False, True, id="skip_marc"),
    pytest.param(True, True, id="skip_both"),
]


def meaningful_storage_parametrize():
    """Decorator for testing meaningful storage differences (local vs cloud)."""
    return pytest.mark.parametrize("storage_type", MEANINGFUL_STORAGE_TYPES)


def extraction_scenarios_parametrize():
    """Decorator for testing extraction scenarios."""
    return pytest.mark.parametrize("skip_ocr,skip_marc", EXTRACTION_SCENARIOS)


def combined_scenarios_parametrize():
    """Decorator for testing combinations of meaningful storage types and extraction scenarios."""
    return pytest.mark.parametrize(
        "storage_type,skip_ocr,skip_marc",
        [
            pytest.param(storage, ocr, marc, id=f"{storage}_{extraction_id}")
            for storage in MEANINGFUL_STORAGE_TYPES  # Use meaningful storage types only
            for (ocr, marc), extraction_id in zip(
                [(False, False), (True, False), (False, True), (True, True)],
                ["both_enabled", "skip_ocr", "skip_marc", "skip_both"],
                strict=False,
            )
        ],
    )
