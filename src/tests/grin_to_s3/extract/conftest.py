"""Shared fixtures for extraction tests."""

import json
import tempfile
from pathlib import Path

import pytest

from grin_to_s3.collect_books.models import SQLiteProgressTracker


@pytest.fixture
def temp_jsonl_file():
    """Create a temporary JSONL file for extraction output."""
    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
        output_path = f.name

    yield output_path

    # Cleanup
    Path(output_path).unlink(missing_ok=True)


@pytest.fixture
async def temp_db():
    """Create a temporary database with proper schema initialized."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    # Initialize tracker which will create the schema
    tracker = SQLiteProgressTracker(db_path=db_path)
    await tracker.init_db()

    yield db_path

    # Cleanup
    await tracker.close()
    Path(db_path).unlink(missing_ok=True)


def read_jsonl_file(file_path):
    """Helper function to read JSONL file and return list of page contents."""
    with open(file_path, encoding="utf-8") as f:
        return [json.loads(line.strip()) for line in f if line.strip()]
