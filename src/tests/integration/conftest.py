#!/usr/bin/env python3
"""
Shared test fixtures for integration tests.
"""

import pytest

from grin_to_s3.process_summary import ProcessStageMetrics


@pytest.fixture(autouse=True)
def mock_process_stage():
    """Create a mock process summary stage for testing. Auto-used in all tests."""
    return ProcessStageMetrics("test")
