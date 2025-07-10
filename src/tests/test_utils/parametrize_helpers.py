"""Parametrized test helpers for common test scenarios."""

import pytest

# Common test parameters
STORAGE_TYPES = ["s3", "r2", "minio", "local"]

EXTRACTION_SCENARIOS = [
    pytest.param(False, False, id="both_enabled"),
    pytest.param(True, False, id="skip_ocr"),
    pytest.param(False, True, id="skip_marc"),
    pytest.param(True, True, id="skip_both"),
]

STORAGE_CONFIGS = [
    pytest.param(
        {"bucket_raw": "test-raw", "bucket_full": "test-full"},
        id="standard_buckets"
    ),
    pytest.param(
        {"bucket_raw": "custom-raw", "bucket_full": "custom-full", "prefix": "custom-prefix"},
        id="custom_prefix"
    ),
    pytest.param(
        {"base_path": "/tmp/storage"},
        id="local_storage"
    ),
]


def storage_type_parametrize():
    """Decorator for testing across storage types."""
    return pytest.mark.parametrize("storage_type", STORAGE_TYPES)


def extraction_scenarios_parametrize():
    """Decorator for testing extraction scenarios."""
    return pytest.mark.parametrize("skip_ocr,skip_marc", EXTRACTION_SCENARIOS)


def storage_configs_parametrize():
    """Decorator for testing different storage configurations."""
    return pytest.mark.parametrize("storage_config", STORAGE_CONFIGS)


def combined_scenarios_parametrize():
    """Decorator for testing combinations of storage types and extraction scenarios."""
    return pytest.mark.parametrize(
        "storage_type,skip_ocr,skip_marc",
        [
            pytest.param(storage, ocr, marc, id=f"{storage}_{extraction_id}")
            for storage in STORAGE_TYPES
            for (ocr, marc), extraction_id in zip(
                [(False, False), (True, False), (False, True), (True, True)],
                ["both_enabled", "skip_ocr", "skip_marc", "skip_both"], strict=False
            )
        ]
    )


# Helper functions for creating test IDs
def make_test_id(storage_type: str, skip_ocr: bool, skip_marc: bool) -> str:
    """Create a descriptive test ID from parameters."""
    extraction_part = []
    if skip_ocr:
        extraction_part.append("no_ocr")
    if skip_marc:
        extraction_part.append("no_marc")

    extraction_suffix = "_".join(extraction_part) if extraction_part else "full_extraction"
    return f"{storage_type}_{extraction_suffix}"


def make_config_test_id(config: dict) -> str:
    """Create a descriptive test ID from storage config."""
    if "base_path" in config:
        return "local_storage"
    elif "prefix" in config:
        return f"prefixed_{config.get('bucket_raw', 'default')}"
    else:
        return f"standard_{config.get('bucket_raw', 'default')}"
