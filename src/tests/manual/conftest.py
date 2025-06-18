"""
Pytest configuration for manual tests directory

This directory contains manual tests that require real credentials
and user interaction. They should not be run by pytest autodiscovery.
"""

import pytest


def pytest_collection_modifyitems(config, items):
    """Skip all tests in the manual directory during autodiscovery"""
    skip_manual = pytest.mark.skip(reason="Manual test - requires credentials and user interaction")
    for item in items:
        if "manual" in str(item.fspath):
            item.add_marker(skip_manual)
