"""Test Path serialization in RunConfig."""

import json
import tempfile
from dataclasses import asdict
from pathlib import Path

import pytest

from grin_to_s3.run_config import (
    RunConfig,
    StorageConfig,
    SyncConfig,
    build_storage_config_dict,
    load_run_config,
    save_run_config,
    serialize_paths,
)


@pytest.mark.parametrize(
    "field_path,expected",
    [
        (["path_field"], "/tmp/test"),
        (["string_field"], "hello"),
        (["nested", "another_path"], "/home/user"),
        (["nested", "number"], 42),
    ],
)
def test_serialize_paths_handles_path_objects(field_path, expected):
    """serialize_paths should convert Path objects to strings."""
    data = {
        "path_field": Path("/tmp/test"),
        "string_field": "hello",
        "nested": {"another_path": Path("/home/user"), "number": 42},
    }

    result = serialize_paths(data)

    # Navigate to the field using the path
    value = result
    for key in field_path:
        value = value[key]

    assert value == expected


def test_serialization_converts_paths_to_strings():
    """Serialization should produce JSON-compatible strings from Path objects."""
    storage_config: StorageConfig = {
        "type": "local",
        "protocol": "file",
        "config": {"base_path": "/tmp/storage"},
    }

    sync_config: SyncConfig = {
        "task_check_concurrency": 1,
        "task_download_concurrency": 2,
        "task_decrypt_concurrency": 3,
        "task_upload_concurrency": 4,
        "task_unpack_concurrency": 5,
        "task_extract_marc_concurrency": 6,
        "task_extract_ocr_concurrency": 7,
        "task_export_csv_concurrency": 8,
        "task_cleanup_concurrency": 9,
        "staging_dir": Path("/tmp/staging"),
        "disk_space_threshold": 0.8,
    }

    config = RunConfig(
        run_name="test_run",
        library_directory="test_lib",
        output_directory=Path("/tmp/output"),
        sqlite_db_path=Path("/tmp/db.sqlite"),
        storage_config=storage_config,
        sync_config=sync_config,
        log_file=Path("/tmp/log.txt"),
        secrets_dir=Path("/tmp/secrets"),
    )

    # Test serialization produces JSON-serializable data
    serialized = serialize_paths(asdict(config))
    json_str = json.dumps(serialized)

    # Verify it's a valid JSON string
    assert isinstance(json_str, str)

    # Verify paths were converted to strings
    data = json.loads(json_str)
    assert data["output_directory"] == "/tmp/output"
    assert data["sqlite_db_path"] == "/tmp/db.sqlite"
    assert data["log_file"] == "/tmp/log.txt"
    assert data["secrets_dir"] == "/tmp/secrets"
    assert data["sync_config"]["staging_dir"] == "/tmp/staging"


def test_save_config_creates_valid_json():
    """save_run_config should create valid JSON files with Path objects converted to strings."""
    with tempfile.TemporaryDirectory() as temp_dir:
        output_dir = Path(temp_dir)

        storage_config: StorageConfig = {
            "type": "local",
            "protocol": "file",
            "config": {"base_path": "/tmp/storage"},
        }

        sync_config: SyncConfig = {
            "task_check_concurrency": 1,
            "task_download_concurrency": 2,
            "task_decrypt_concurrency": 3,
            "task_upload_concurrency": 4,
            "task_unpack_concurrency": 5,
            "task_extract_marc_concurrency": 6,
            "task_extract_ocr_concurrency": 7,
            "task_export_csv_concurrency": 8,
            "task_cleanup_concurrency": 9,
            "staging_dir": Path("/tmp/staging"),
            "disk_space_threshold": 0.8,
        }

        config = RunConfig(
            run_name="test_run",
            library_directory="test_lib",
            output_directory=output_dir,
            sqlite_db_path=Path("/tmp/db.sqlite"),
            storage_config=storage_config,
            sync_config=sync_config,
            log_file=Path("/tmp/log.txt"),
            secrets_dir=Path("/tmp/secrets"),
        )

        # Save config (should create run_config.json in output_directory)
        save_run_config(config)

        # Verify the file was created in the correct location
        config_file = output_dir / "run_config.json"
        assert config_file.exists()

        # Verify it contains valid JSON with Path objects serialized as strings
        with open(config_file) as f:
            data = json.load(f)

        assert data["run_name"] == "test_run"
        assert data["output_directory"] == str(output_dir)
        assert data["sqlite_db_path"] == "/tmp/db.sqlite"
        assert data["log_file"] == "/tmp/log.txt"
        assert data["secrets_dir"] == "/tmp/secrets"
        assert data["sync_config"]["staging_dir"] == "/tmp/staging"


def test_load_run_config_returns_dataclass():
    """load_run_config should return a proper RunConfig dataclass instance."""
    with tempfile.TemporaryDirectory() as temp_dir:
        output_dir = Path(temp_dir)

        storage_config: StorageConfig = {
            "type": "local",
            "protocol": "file",
            "config": {"base_path": "/tmp/storage"},
        }

        sync_config: SyncConfig = {
            "task_check_concurrency": 1,
            "task_download_concurrency": 2,
            "task_decrypt_concurrency": 3,
            "task_upload_concurrency": 4,
            "task_unpack_concurrency": 5,
            "task_extract_marc_concurrency": 6,
            "task_extract_ocr_concurrency": 7,
            "task_export_csv_concurrency": 8,
            "task_cleanup_concurrency": 9,
            "staging_dir": Path("/tmp/staging"),
            "disk_space_threshold": 0.8,
        }

        original_config = RunConfig(
            run_name="test_run",
            library_directory="test_lib",
            output_directory=output_dir,
            sqlite_db_path=Path("/tmp/db.sqlite"),
            storage_config=storage_config,
            sync_config=sync_config,
            log_file=Path("/tmp/log.txt"),
            secrets_dir=Path("/tmp/secrets"),
        )

        # Save config
        save_run_config(original_config)
        config_file = output_dir / "run_config.json"

        # Load config and verify it's a proper dataclass instance
        loaded_config = load_run_config(str(config_file))

        # Verify it's the correct type
        assert isinstance(loaded_config, RunConfig)

        # Verify Path fields are actually Path objects, not strings
        assert isinstance(loaded_config.output_directory, Path)
        assert isinstance(loaded_config.sqlite_db_path, Path)
        assert isinstance(loaded_config.log_file, Path)
        assert isinstance(loaded_config.secrets_dir, Path)
        assert isinstance(loaded_config.sync_config["staging_dir"], Path)

        # Verify values are correct
        assert loaded_config.run_name == "test_run"
        assert loaded_config.output_directory == output_dir
        assert loaded_config.sqlite_db_path == Path("/tmp/db.sqlite")
        assert loaded_config.log_file == Path("/tmp/log.txt")
        assert loaded_config.secrets_dir == Path("/tmp/secrets")
        assert loaded_config.sync_config["staging_dir"] == Path("/tmp/staging")


class MockArgs:
    """Mock arguments object for testing build_storage_config_dict."""

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


def test_build_storage_config_dict_with_storage_path():
    """build_storage_config_dict should handle --storage-path argument."""
    args = MockArgs(
        storage_path="/tmp/test-storage",
        storage_config=None,
        bucket_raw=None,
        bucket_meta=None,
        bucket_full=None,
        storage="local",
    )

    result = build_storage_config_dict(args)

    assert result["base_path"] == "/tmp/test-storage"


def test_build_storage_config_dict_with_storage_config_base_path():
    """build_storage_config_dict should handle --storage-config base_path for backward compatibility."""
    args = MockArgs(
        storage_path=None,
        storage_config=["base_path=/tmp/legacy-storage"],
        bucket_raw=None,
        bucket_meta=None,
        bucket_full=None,
        storage="local",
    )

    result = build_storage_config_dict(args)

    assert result["base_path"] == "/tmp/legacy-storage"


def test_build_storage_config_dict_conflicts_storage_path_and_base_path():
    """build_storage_config_dict should raise error when both --storage-path and --storage-config base_path are specified."""
    args = MockArgs(
        storage_path="/tmp/new-storage",
        storage_config=["base_path=/tmp/legacy-storage"],
        bucket_raw=None,
        bucket_meta=None,
        bucket_full=None,
        storage="local",
    )

    try:
        build_storage_config_dict(args)
        raise AssertionError("Expected ValueError to be raised")
    except ValueError as e:
        assert "Cannot specify both --storage-path and --storage-config base_path" in str(e)
