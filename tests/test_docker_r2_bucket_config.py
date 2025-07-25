#!/usr/bin/env python3
"""
Docker-based test for R2 storage bucket configuration issues.

This test reproduces the bug where R2 storage fails with "Bucket name cannot be empty"
when bucket names are not provided via CLI arguments or R2 credentials file.
"""

import json
import subprocess
import tempfile
from pathlib import Path


class TestDockerR2BucketConfig:
    """Test R2 bucket configuration in Docker environment."""

    def test_r2_storage_without_buckets_now_works_after_fix(self):
        """Test that R2 storage without explicit bucket CLI args now works with credentials file.

        This test validates that the fix allows Docker to correctly find bucket names
        from the r2_credentials.json file when no bucket names are provided via CLI.

        Previously this would fail with "Bucket name cannot be empty: bucket_raw"
        because build_storage_config_dict used hardcoded paths instead of find_credential_file.
        """

        # Run the docker command that previously failed but should now work
        cmd = [
            "./grin-docker", "collect",
            "--library-directory", "Harvard",
            "--limit", "5",
            "--storage", "r2",
            "--test-mode"  # Use test mode to avoid real API calls
            # Note: No bucket names provided, relies on r2_credentials.json in container
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd="/Users/liza/work/harvard-idi/grin-to-s3-wip"
        )

        # Should now succeed
        assert result.returncode == 0

        # Should not contain the bucket error
        error_output = result.stderr + result.stdout
        assert "Bucket name cannot be empty: bucket_raw" not in error_output

        # Should show successful completion
        assert "✓ Completed Book Collection" in error_output or "Book Collection Summary" in error_output

    def test_r2_storage_with_cli_buckets_succeeds_past_config_stage(self):
        """Test that R2 storage with CLI bucket names gets past the configuration stage."""

        cmd = [
            "./grin-docker", "collect",
            "--library-directory", "Harvard",
            "--limit", "1",
            "--storage", "r2",
            "--bucket-raw", "test-raw",
            "--bucket-meta", "test-meta",
            "--bucket-full", "test-full",
            "--test-mode"  # Use test mode to avoid actual API calls
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd="/Users/liza/work/harvard-idi/grin-to-s3-wip"
        )

        # Should not fail with bucket configuration error
        assert "Bucket name cannot be empty" not in result.stderr
        assert "Bucket name cannot be empty" not in result.stdout

        # May fail later with auth or other issues, but bucket config should be OK

    def test_r2_storage_with_credentials_file_succeeds_past_config_stage(self):
        """Test that R2 storage with credentials file containing buckets works."""

        # Create temporary R2 credentials file with bucket names
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            r2_config = {
                "endpoint_url": "https://test-account.r2.cloudflarestorage.com",
                "access_key": "test-key",
                "secret_key": "test-secret",
                "bucket_raw": "creds-raw",
                "bucket_meta": "creds-meta",
                "bucket_full": "creds-full"
            }
            json.dump(r2_config, f, indent=2)
            temp_creds_file = f.name

        try:
            cmd = [
                "./grin-docker", "collect",
                "--library-directory", "Harvard",
                "--limit", "1",
                "--storage", "r2",
                "--storage-config", f"credentials_file={temp_creds_file}",
                "--test-mode"
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd="/Users/liza/work/harvard-idi/grin-to-s3-wip"
            )

            # Should not fail with bucket configuration error
            assert "Bucket name cannot be empty" not in result.stderr
            assert "Bucket name cannot be empty" not in result.stdout

        finally:
            # Clean up temp file
            Path(temp_creds_file).unlink(missing_ok=True)

    def test_write_config_with_r2_buckets_creates_valid_config(self):
        """Test that --write-config with R2 buckets creates a valid configuration."""

        cmd = [
            "./grin-docker", "collect",
            "--library-directory", "Harvard",
            "--storage", "r2",
            "--bucket-raw", "config-raw",
            "--bucket-meta", "config-meta",
            "--bucket-full", "config-full",
            "--run-name", "test-config",
            "--write-config"
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd="/Users/liza/work/harvard-idi/grin-to-s3-wip"
        )

        # Should succeed
        assert result.returncode == 0

        # Check that config file was created
        config_path = Path("/Users/liza/work/harvard-idi/grin-to-s3-wip/output/test-config/run_config.json")
        assert config_path.exists()

        # Load and validate config
        with open(config_path) as f:
            config = json.load(f)

        # Check storage config has bucket names
        storage_config = config["storage_config"]
        assert storage_config["type"] == "r2"
        assert storage_config["config"]["bucket_raw"] == "config-raw"
        assert storage_config["config"]["bucket_meta"] == "config-meta"
        assert storage_config["config"]["bucket_full"] == "config-full"

        # Clean up
        config_path.unlink(missing_ok=True)
        config_path.parent.rmdir()

    def test_docker_credentials_directory_accessible(self):
        """Test that Docker container can access the credentials directory."""

        cmd = [
            "./grin-docker", "bash", "-c",
            "ls -la /app/.config/grin-to-s3-read/ && echo '--- WRITABLE DIR ---' && ls -la /app/.config/grin-to-s3-write/"
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd="/Users/liza/work/harvard-idi/grin-to-s3-wip"
        )

        # Should succeed
        assert result.returncode == 0

        # Should show directory contents
        output = result.stdout
        assert "total" in output  # ls -la output

        print(f"Docker credentials directory contents:\n{output}")

    def test_docker_r2_credentials_file_detection(self):
        """Test how Docker container detects R2 credentials file."""

        cmd = [
            "./grin-docker", "bash", "-c",
            "python -c \"from grin_to_s3.storage.factories import find_credential_file; print('R2 creds file:', find_credential_file('r2_credentials.json'))\""
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd="/Users/liza/work/harvard-idi/grin-to-s3-wip"
        )

        # Should succeed
        assert result.returncode == 0

        print(f"R2 credentials file detection result:\n{result.stdout}")

        # Check if it found a file or returned None
        if "None" in result.stdout:
            print("❌ Docker container cannot find r2_credentials.json file")
        else:
            print("✅ Docker container found r2_credentials.json file")


if __name__ == "__main__":
    # Run the test to validate the fix works
    test = TestDockerR2BucketConfig()
    test.test_r2_storage_without_buckets_now_works_after_fix()
    print("✅ Successfully validated the R2 bucket configuration fix")

    # Also test credentials directory access
    test.test_docker_credentials_directory_accessible()
    test.test_docker_r2_credentials_file_detection()
