"""Mock configuration classes for test scenarios."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class StorageConfig:
    """Configuration for mock storage objects."""

    archive_path: str = "path/to/archive"
    ocr_path: str = "bucket_full/TEST123/TEST123.jsonl"
    upload_should_fail: bool = False
    base_path: str | None = None
    bucket_config: dict[str, str] = field(default_factory=lambda: {"bucket_raw": "test-raw", "bucket_full": "test-full"})


@dataclass
class ExtractionConfig:
    """Configuration for extraction mocking."""

    ocr_enabled: bool = True
    marc_enabled: bool = True
    ocr_should_fail: bool = False
    marc_should_fail: bool = False
    extraction_delay: float = 0.0


@dataclass
class TestScenario:
    """Complete test scenario configuration."""

    storage: StorageConfig = field(default_factory=StorageConfig)
    extraction: ExtractionConfig = field(default_factory=ExtractionConfig)
    expected_status: str = "completed"
    should_raise: Exception | None = None
    barcode: str = "TEST123"
    staging_file: str = "/staging/TEST123.tar.gz.gpg"
    storage_type: str = "local"


# Pre-defined common scenarios
SUCCESS_SCENARIO = TestScenario()

OCR_DISABLED_SCENARIO = TestScenario(
    extraction=ExtractionConfig(ocr_enabled=False)
)

MARC_DISABLED_SCENARIO = TestScenario(
    extraction=ExtractionConfig(marc_enabled=False)
)

BOTH_DISABLED_SCENARIO = TestScenario(
    extraction=ExtractionConfig(ocr_enabled=False, marc_enabled=False)
)

UPLOAD_FAILURE_SCENARIO = TestScenario(
    storage=StorageConfig(upload_should_fail=True),
    expected_status="failed",
    should_raise=Exception("Storage upload failed")
)

OCR_FAILURE_SCENARIO = TestScenario(
    extraction=ExtractionConfig(ocr_should_fail=True),
    expected_status="completed"  # OCR failure should not block sync
)

MARC_FAILURE_SCENARIO = TestScenario(
    extraction=ExtractionConfig(marc_should_fail=True),
    expected_status="completed"  # MARC failure should not block sync
)


def get_storage_config_dict(scenario: TestScenario) -> dict[str, Any]:
    """Convert TestScenario to storage config dictionary."""
    return {
        "archive_path": scenario.storage.archive_path,
        "ocr_path": scenario.storage.ocr_path,
        "base_path": scenario.storage.base_path,
        **scenario.storage.bucket_config,
    }
