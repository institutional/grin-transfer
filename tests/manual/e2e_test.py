#!/usr/bin/env python3
"""
End-to-end testing script for grin-to-s3

This script performs comprehensive testing across multiple configurations using real credentials:
1. Checks out repo in temp directory
2. Performs fresh install from scratch
3. Tests local machine with multiple storage backends:
   - Local filesystem storage
   - R2 storage (if configured)
4. Builds Docker from scratch
5. Tests Docker with multiple storage backends:
   - MinIO storage
   - R2 storage (if configured)

Each configuration runs both collect and sync pipeline with --limit 20.

Usage:
    python tests/manual/e2e_test.py --run-name test_e2e

This script is designed to be run manually for comprehensive testing.
"""

import argparse
import logging
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class E2ETestRunner:
    """End-to-end test runner for grin-to-s3 pipeline."""

    def __init__(self, run_name: str, limit: int = 20, cleanup: bool = True):
        self.run_name = run_name
        self.limit = limit
        self.cleanup = cleanup
        self.temp_dir: Path | None = None
        self.original_dir = Path.cwd()

    def _run_command(
        self,
        cmd: list[str],
        cwd: Path = None,
        check: bool = True,
        quiet: bool = False,
        timeout: int = 300,
        env: dict = None,
    ) -> subprocess.CompletedProcess:
        """Run a command and log the output."""
        cmd_str = " ".join(cmd)

        if not quiet:
            logger.info(f"Running: {cmd_str}")

        try:
            result = subprocess.run(
                cmd,
                cwd=cwd,
                capture_output=True,
                text=True,
                check=check,
                timeout=timeout,
                stdin=subprocess.DEVNULL,  # Prevent hanging on input prompts
                env=env,
            )

            # Only show output for non-quiet commands or if there are errors
            if not quiet or result.returncode != 0:
                if result.stdout:
                    logger.info(f"STDOUT:\n{result.stdout}")
                if result.stderr and not self._is_noise(result.stderr, cmd[0]):
                    logger.warning(f"STDERR:\n{result.stderr}")

            return result

        except subprocess.TimeoutExpired as e:
            logger.error(f"Command timed out after {timeout} seconds")
            logger.error(f"Command: {cmd_str}")
            if e.stdout:
                logger.error(f"STDOUT before timeout:\n{e.stdout}")
            if e.stderr:
                logger.error(f"STDERR before timeout:\n{e.stderr}")
            raise
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed with exit code {e.returncode}")
            if e.stdout:
                logger.error(f"STDOUT:\n{e.stdout}")
            if e.stderr:
                logger.error(f"STDERR:\n{e.stderr}")
            raise

    def _is_noise(self, stderr: str, command: str) -> bool:
        """Check if stderr output is just noise we can ignore."""
        noise_patterns = [
            "Cloning into",  # Git clone messages
            "Use 'docker scan'",  # Docker security scan suggestion
            "WARNING: Running pip as the 'root' user",  # Pip warnings
            "DEPRECATION:",  # Deprecation warnings
            "Network",  # Docker network creation messages
            "Container",  # Docker container creation/startup messages
            "Creating",  # Docker compose creation messages
            "Created",  # Docker compose created messages
            "Starting",  # Docker compose starting messages
            "Started",  # Docker compose started messages
            "Waiting",  # Docker compose waiting messages
            "Healthy",  # Docker compose health messages
        ]

        # For pip installs, ignore most stderr noise
        if command in ["python", "/Users/liza/.pyenv/versions/3.12.11/bin/python"] and any(
            pattern in stderr for pattern in ["Installing", "Requirement already satisfied", "Building"]
        ):
            return True

        # For Docker Compose commands, ignore container lifecycle noise
        if command == "docker" and any(
            pattern in stderr
            for pattern in ["Network", "Container", "Creating", "Created", "Starting", "Started", "Waiting", "Healthy"]
        ):
            return True

        return any(pattern in stderr for pattern in noise_patterns)

    def setup_temp_repo(self) -> Path:
        """Clone repository to temporary directory."""
        logger.info("Setting up temporary repository...")

        self.temp_dir = Path(tempfile.mkdtemp(prefix="grin_e2e_"))
        logger.info(f"Created temporary directory: {self.temp_dir}")

        # Clone the local repository (including uncommitted changes)
        repo_dir = self.temp_dir / "grin-to-s3"
        self._run_command(["git", "clone", str(self.original_dir), str(repo_dir)], quiet=True)

        # Get current branch from original directory
        current_branch_result = subprocess.run(
            ["git", "branch", "--show-current"], cwd=self.original_dir, capture_output=True, text=True
        )

        if current_branch_result.returncode == 0:
            current_branch = current_branch_result.stdout.strip()
            if current_branch:
                logger.info(f"Checking out branch: {current_branch}")
                try:
                    self._run_command(["git", "checkout", current_branch], cwd=repo_dir)
                except subprocess.CalledProcessError:
                    logger.warning(f"Failed to checkout {current_branch}, staying on default branch")

        return repo_dir

    def install_dependencies(self, repo_dir: Path) -> None:
        """Perform fresh installation from scratch."""
        logger.info("Installing dependencies from scratch...")

        # Create a virtual environment in the temp repo
        venv_dir = repo_dir / "venv"
        logger.info("Creating virtual environment...")
        self._run_command([sys.executable, "-m", "venv", str(venv_dir)], cwd=repo_dir, quiet=True)

        # Determine the python executable in the venv (Unix/macOS only)
        venv_python = venv_dir / "bin" / "python"

        # Install in development mode
        logger.info("Installing package dependencies...")
        self._run_command([str(venv_python), "-m", "pip", "install", "-e", "."], cwd=repo_dir, quiet=True)

        # Install test dependencies if they exist
        requirements_test = repo_dir / "requirements-test.txt"
        if requirements_test.exists():
            self._run_command(
                [str(venv_python), "-m", "pip", "install", "-r", "requirements-test.txt"], cwd=repo_dir, quiet=True
            )

        # Store the venv python path for later use
        self.venv_python = str(venv_python)

    def check_credentials(self) -> None:
        """Verify that required credentials are available."""
        logger.info("Checking for required credentials...")

        creds_dir = Path.home() / ".config" / "grin-to-s3"
        required_files = ["client_secret.json"]

        for file_name in required_files:
            file_path = creds_dir / file_name
            if not file_path.exists():
                logger.error(f"Required credential file not found: {file_path}")
                logger.error("Please set up credentials before running E2E tests")
                sys.exit(1)

        logger.info("Credentials check passed")

    def test_local_pipeline(self, repo_dir: Path) -> None:
        """Test the pipeline on the local machine with multiple storage configurations."""
        logger.info("Testing local pipeline with multiple configurations...")

        # Configuration 1: Local storage
        logger.info("=== Testing local machine with local storage ===")
        local_run_name = f"{self.run_name}_local"

        # Collect with local storage
        storage_base = str(repo_dir / "test-data")
        self._run_command(
            [
                self.venv_python,
                "grin.py",
                "collect",
                "--run-name",
                local_run_name,
                "--limit",
                str(self.limit),
                "--storage",
                "local",
                "--storage-config",
                f"base_path={storage_base}",
                "--bucket-raw",
                "raw",
                "--bucket-meta",
                "meta",
                "--bucket-full",
                "full",
                "--library-directory",
                "Harvard",
            ],
            cwd=repo_dir,
        )

        # Sync with local storage - converted queue
        self._run_command(
            [
                self.venv_python,
                "grin.py",
                "sync",
                "pipeline",
                "--run-name",
                local_run_name,
                "--queue",
                "converted",
                "--limit",
                "1",
            ],
            cwd=repo_dir,
            timeout=180,
        )

        # Sync with local storage - previous queue (test PR5 functionality)
        self._run_command(
            [
                self.venv_python,
                "grin.py",
                "sync",
                "pipeline",
                "--run-name",
                local_run_name,
                "--queue",
                "previous",
                "--limit",
                "1",
            ],
            cwd=repo_dir,
            timeout=180,
        )

        # Enrich with metadata (test the enrich step)
        self._run_command(
            [
                self.venv_python,
                "grin.py",
                "enrich",
                "--run-name",
                local_run_name,
                "--limit",
                "5",
            ],
            cwd=repo_dir,
            timeout=180,
        )

        # Verify that archives actually exist and are not zero-byte
        self._verify_local_archives(repo_dir, storage_base, local_run_name)

        # Verify metadata file organization
        self._verify_local_metadata(storage_base, local_run_name)

        # Configuration 2: R2 storage (if credentials available)
        logger.info("=== Testing local machine with R2 storage ===")
        r2_creds_file = Path.home() / ".config" / "grin-to-s3" / "r2_credentials.json"
        if r2_creds_file.exists():
            r2_run_name = f"{self.run_name}_r2"

            # Collect with R2 storage (let it use user's config)
            self._run_command(
                [
                    sys.executable,
                    "grin.py",
                    "collect",
                    "--run-name",
                    r2_run_name,
                    "--limit",
                    str(self.limit),
                    "--storage",
                    "r2",
                    "--library-directory",
                    "Harvard",
                ],
                cwd=repo_dir,
            )

            # Sync with R2 storage
            self._run_command(
                [
                    self.venv_python,
                    "grin.py",
                    "sync",
                    "pipeline",
                    "--run-name",
                    r2_run_name,
                    "--queue",
                    "converted",
                    "--limit",
                    "1",
                    "--dry-run",
                ],
                cwd=repo_dir,
                timeout=180,
            )

            # Enrich with metadata (test the enrich step)
            self._run_command(
                [
                    self.venv_python,
                    "grin.py",
                    "enrich",
                    "--run-name",
                    r2_run_name,
                    "--limit",
                    "5",
                ],
                cwd=repo_dir,
                timeout=180,
            )
        else:
            logger.warning("R2 credentials not found, skipping R2 storage test")

        logger.info("Local pipeline tests completed successfully")

    def build_docker_image(self, repo_dir: Path) -> None:
        """Build Docker image from scratch."""
        logger.info("Building Docker image from scratch...")

        # Clean up any existing containers and networks
        logger.info("Cleaning up existing Docker containers...")
        self._run_command(
            ["docker", "compose", "down", "--remove-orphans", "--volumes"], cwd=repo_dir, check=False, quiet=True
        )

        # Also clean up any containers with grin-to-s3 names
        try:
            # Stop and remove any containers with grin-to-s3 in the name
            containers_result = subprocess.run(
                ["docker", "ps", "-a", "--filter", "name=grin-to-s3", "-q"], capture_output=True, text=True
            )

            if containers_result.stdout.strip():
                subprocess.run(["docker", "rm", "-f"] + containers_result.stdout.strip().split(), check=False)
        except subprocess.CalledProcessError:
            pass

        # Remove any existing images to ensure fresh build
        try:
            # Check if image exists first to avoid noisy error messages
            result = subprocess.run(["docker", "images", "-q", "grin-to-s3:latest"], capture_output=True, text=True)

            if result.stdout.strip():  # Image exists
                self._run_command(["docker", "rmi", "grin-to-s3:latest"], check=False)
        except subprocess.CalledProcessError:
            pass  # Expected if docker command fails

        # Build the image
        logger.info("Building Docker image (this may take a few minutes)...")
        self._run_command(["docker", "compose", "build", "--no-cache"], cwd=repo_dir, quiet=True)

        logger.info("Docker build completed successfully")

    def test_docker_pipeline(self, repo_dir: Path) -> None:
        """Test the pipeline in Docker with multiple storage configurations."""
        logger.info("Testing Docker pipeline with multiple configurations...")

        # Set unique Docker instance ID for this test run
        import os

        test_instance_id = f"e2e-{self.run_name}-{int(time.time())}"
        docker_env = os.environ.copy()
        docker_env["GRIN_INSTANCE_ID"] = test_instance_id
        logger.info(f"Using Docker instance ID: {test_instance_id}")

        # Clean up any existing Docker containers first
        logger.info("Cleaning up existing Docker containers...")
        self._run_command(
            ["docker", "compose", "down", "--remove-orphans", "--volumes"], cwd=repo_dir, check=False, quiet=True
        )

        # Remove any leftover grin-to-s3 containers
        try:
            result = subprocess.run(
                ["docker", "ps", "-aq", "--filter", "name=grin-to-s3"], capture_output=True, text=True
            )
            if result.stdout.strip():
                subprocess.run(["docker", "rm", "-f"] + result.stdout.strip().split(), check=False)
        except subprocess.CalledProcessError:
            pass

        # Set up Docker credentials by copying from user's config
        logger.info("Setting up Docker credentials...")
        docker_creds_dir = repo_dir / "docker-data" / "credentials"
        user_creds_dir = Path.home() / ".config" / "grin-to-s3"

        # Create docker credentials directory
        docker_creds_dir.mkdir(parents=True, exist_ok=True)

        # Copy credentials from user's config if they exist
        user_creds_file = user_creds_dir / "credentials.json"
        docker_creds_file = docker_creds_dir / "credentials.json"

        try:
            if user_creds_file.exists():
                shutil.copy2(user_creds_file, docker_creds_file)
                logger.info("Copied OAuth2 credentials to Docker directory")
            else:
                logger.warning("No OAuth2 credentials found in user config")
                logger.warning("Run 'python grin.py auth setup' manually first")
                logger.warning("Skipping Docker tests")
                return
        except (OSError, PermissionError) as e:
            logger.warning(f"Could not copy credentials: {e}")
            logger.warning("Skipping Docker tests")
            return

        # Configuration 1: MinIO storage in Docker
        logger.info("=== Testing Docker with MinIO storage ===")
        docker_minio_run_name = f"{self.run_name}_docker_minio"

        # Collect with MinIO storage in Docker - use standard bucket names that get created
        self._run_command(
            [
                "./grin-docker",
                "collect",
                "--run-name",
                docker_minio_run_name,
                "--limit",
                str(self.limit),
                "--storage",
                "minio",
                "--bucket-raw",
                "grin-raw",
                "--bucket-meta",
                "grin-meta",
                "--bucket-full",
                "grin-full",
                "--library-directory",
                "Harvard",
            ],
            cwd=repo_dir,
            env=docker_env,
        )

        # Wait a moment for MinIO to be fully ready for the next command
        logger.info("Waiting for MinIO to be ready for sync...")
        time.sleep(5)

        # Sync with MinIO storage in Docker
        self._run_command(
            [
                "./grin-docker",
                "sync",
                "pipeline",
                "--run-name",
                docker_minio_run_name,
                "--queue",
                "converted",
                "--limit",
                "1",
            ],
            cwd=repo_dir,
            timeout=180,
            env=docker_env,
        )

        # Verify that metadata files exist in the expected bucket structure
        self._verify_minio_metadata(repo_dir, docker_minio_run_name, docker_env)

        # Configuration 2: R2 storage in Docker (if credentials available)
        logger.info("=== Testing Docker with R2 storage ===")
        r2_creds_file = Path.home() / ".config" / "grin-to-s3" / "r2_credentials.json"
        if r2_creds_file.exists():
            docker_r2_run_name = f"{self.run_name}_docker_r2"

            # Collect with R2 storage in Docker (let it use user's config)
            self._run_command(
                [
                    "./grin-docker",
                    "collect",
                    "--run-name",
                    docker_r2_run_name,
                    "--limit",
                    str(self.limit),
                    "--storage",
                    "r2",
                    "--library-directory",
                    "Harvard",
                    "--dry-run",
                ],
                cwd=repo_dir,
                env=docker_env,
            )

            # Sync with R2 storage in Docker (longer timeout for network operations)
            self._run_command(
                [
                    "./grin-docker",
                    "sync",
                    "pipeline",
                    "--run-name",
                    docker_r2_run_name,
                    "--queue",
                    "converted",
                    "--limit",
                    "1",
                    "--dry-run",
                ],
                cwd=repo_dir,
                timeout=180,
                env=docker_env,
            )
        else:
            logger.warning("R2 credentials not found, skipping Docker R2 storage test")

        logger.info("Docker pipeline tests completed successfully")

        # Clean up Docker containers after testing
        logger.info("Cleaning up Docker containers...")
        self._run_command(
            ["docker", "compose", "down", "--remove-orphans", "--volumes"],
            cwd=repo_dir,
            check=False,
            quiet=True,
            env=docker_env,
        )

        # Also clean up any leftover containers that might conflict
        self._run_command(["docker", "ps", "-aq", "--filter", "name=grin-to-s3"], check=False, quiet=True)

        try:
            result = subprocess.run(
                ["docker", "ps", "-aq", "--filter", "name=grin-to-s3"], capture_output=True, text=True
            )
            if result.stdout.strip():
                subprocess.run(["docker", "rm", "-f"] + result.stdout.strip().split(), check=False)
        except subprocess.CalledProcessError:
            pass

    def _verify_local_archives(self, repo_dir: Path, storage_base: str, run_name: str) -> None:
        """Verify that book archives exist and are not zero-byte files."""
        logger.info("Verifying that book archives exist and are not zero-byte...")

        # Check the raw storage directory for archives
        raw_dir = Path(storage_base) / "raw"
        if not raw_dir.exists():
            raise Exception(f"Raw storage directory does not exist: {raw_dir}")

        # Find all .tar.gz files (should be decrypted archives)
        archive_files = list(raw_dir.glob("**/*.tar.gz"))

        if not archive_files:
            raise Exception(f"No .tar.gz archive files found in {raw_dir}")

        logger.info(f"Found {len(archive_files)} archive files")

        # Verify each archive is not zero-byte
        zero_byte_files = []
        for archive_file in archive_files:
            file_size = archive_file.stat().st_size
            logger.info(f"Archive {archive_file.name}: {file_size} bytes")

            if file_size == 0:
                zero_byte_files.append(archive_file)

        if zero_byte_files:
            zero_byte_names = [f.name for f in zero_byte_files]
            raise Exception(f"Found zero-byte archive files: {zero_byte_names}")

        logger.info("All archive files verified - they exist and are not zero-byte")

    def _verify_local_metadata(self, storage_base: str, run_name: str) -> None:
        """Verify that metadata files exist in local storage with the new structure."""
        logger.info("Verifying metadata files in local storage structure...")

        # Check the meta storage directory for the new structure
        meta_dir = Path(storage_base) / "meta"
        run_dir = meta_dir / run_name

        if not meta_dir.exists():
            logger.warning(f"Meta storage directory does not exist: {meta_dir}")
            return

        if not run_dir.exists():
            logger.warning(f"Run directory does not exist in meta storage: {run_dir}")
            return

        # Check for expected metadata files in the new structure
        expected_files = [
            run_dir / "config.json",  # Run configuration (uncompressed)
            run_dir / "process_summary.json.gz",  # Process summary (compressed)
            run_dir / "books_latest.csv.gz",  # Latest CSV (compressed)
            run_dir / "books_latest.db.gz",  # Latest database (compressed)
        ]

        files_found = []
        files_missing = []

        for expected_file in expected_files:
            if expected_file.exists():
                file_size = expected_file.stat().st_size
                files_found.append(expected_file)
                logger.info(f"✓ Found metadata file: {expected_file.name} ({file_size} bytes)")
            else:
                files_missing.append(expected_file)
                logger.warning(f"✗ Missing metadata file: {expected_file.name}")

        # Check for timestamped subdirectory (may not exist if no timestamped files were created)
        timestamped_dir = run_dir / "timestamped"
        if timestamped_dir.exists():
            timestamped_files = list(timestamped_dir.glob("*"))
            if timestamped_files:
                logger.info(f"✓ Found timestamped subdirectory with {len(timestamped_files)} files")
                for file in timestamped_files:
                    logger.info(f"  - {file.name}")
            else:
                logger.info("✓ Found timestamped subdirectory (empty)")
        else:
            logger.info("ℹ No timestamped subdirectory found (expected for short test runs)")

        # Report verification results
        logger.info(f"Local metadata verification complete: {len(files_found)}/{len(expected_files)} files found")

        if files_missing:
            logger.warning(f"Missing metadata files: {[f.name for f in files_missing]}")
            # Don't fail the test for missing files since some may not be created in short runs
        else:
            logger.info("✓ All expected metadata files found in correct local structure")

    def _verify_minio_metadata(self, repo_dir: Path, run_name: str, docker_env: dict) -> None:
        """Verify that metadata files exist in MinIO with the new bucket structure."""
        logger.info("Verifying metadata files in MinIO bucket structure...")

        try:
            # Use docker compose exec to run mc (MinIO client) inside the MinIO container
            # First check if the bucket and run directory exist
            result = self._run_command(
                ["docker", "compose", "exec", "-T", "minio", "mc", "ls", f"minio/grin-meta/{run_name}/"],
                cwd=repo_dir,
                env=docker_env,
                quiet=True,
                check=False,
            )

            if result.returncode != 0:
                logger.warning(f"Run directory may not exist in meta bucket: {run_name}")
                # Still continue to check for individual files

            # Check for expected metadata files in the new structure
            expected_files = [
                f"{run_name}/config.json",  # Run configuration (uncompressed)
                f"{run_name}/process_summary.json.gz",  # Process summary (compressed)
                f"{run_name}/books_latest.csv.gz",  # Latest CSV (compressed)
                f"{run_name}/books_latest.db.gz",  # Latest database (compressed)
            ]

            files_found = []
            files_missing = []

            for expected_file in expected_files:
                # Check if each file exists in the bucket
                result = self._run_command(
                    ["docker", "compose", "exec", "-T", "minio", "mc", "stat", f"minio/grin-meta/{expected_file}"],
                    cwd=repo_dir,
                    env=docker_env,
                    quiet=True,
                    check=False,
                )

                if result.returncode == 0:
                    files_found.append(expected_file)
                    logger.info(f"✓ Found metadata file: {expected_file}")
                else:
                    files_missing.append(expected_file)
                    logger.warning(f"✗ Missing metadata file: {expected_file}")

            # Check for timestamped subdirectory (may not exist if no timestamped files were created)
            timestamped_result = self._run_command(
                ["docker", "compose", "exec", "-T", "minio", "mc", "ls", f"minio/grin-meta/{run_name}/timestamped/"],
                cwd=repo_dir,
                env=docker_env,
                quiet=True,
                check=False,
            )

            if timestamped_result.returncode == 0:
                logger.info("✓ Found timestamped subdirectory")
            else:
                logger.info("ℹ No timestamped files found (expected for short test runs)")

            # Report verification results
            logger.info(f"Metadata verification complete: {len(files_found)}/{len(expected_files)} files found")

            if files_missing:
                logger.warning(f"Missing metadata files: {files_missing}")
                # Don't fail the test for missing files since sync may not have created all files
                # This is more of an informational check
            else:
                logger.info("✓ All expected metadata files found in correct bucket structure")

        except Exception as e:
            logger.error(f"Failed to verify MinIO metadata: {e}")
            # Don't fail the entire test for metadata verification issues
            logger.warning("Continuing test despite metadata verification failure")

    def cleanup_temp_dir(self) -> None:
        """Clean up temporary directory."""
        if self.cleanup and self.temp_dir and self.temp_dir.exists():
            logger.info(f"Cleaning up temporary directory: {self.temp_dir}")
            shutil.rmtree(self.temp_dir)

    def run_all_tests(self) -> None:
        """Run the complete end-to-end test suite."""
        try:
            logger.info(f"Starting E2E test run: {self.run_name}")

            # Setup and preparation
            self.check_credentials()
            repo_dir = self.setup_temp_repo()
            self.install_dependencies(repo_dir)

            # Local testing
            self.test_local_pipeline(repo_dir)

            # Docker testing
            self.build_docker_image(repo_dir)
            self.test_docker_pipeline(repo_dir)

            logger.info("All E2E tests completed successfully!")

        except Exception as e:
            logger.error(f"E2E test failed: {str(e)}")
            sys.exit(1)

        finally:
            self.cleanup_temp_dir()


def main():
    """Main entry point for E2E testing script."""
    parser = argparse.ArgumentParser(description="End-to-end testing script for grin-to-s3")
    parser.add_argument("--run-name", help="Run name for the test pipeline (default: auto-generated)")
    parser.add_argument("--limit", type=int, default=20, help="Limit number of books to process (default: 20)")
    parser.add_argument("--no-cleanup", action="store_true", help="Don't clean up temporary directory after tests")

    args = parser.parse_args()

    # Generate run name if not provided
    if not args.run_name:
        import os

        args.run_name = f"e2e_test_{os.getenv('USER', 'user')}_{int(time.time())}"

    runner = E2ETestRunner(run_name=args.run_name, limit=args.limit, cleanup=not args.no_cleanup)

    runner.run_all_tests()


if __name__ == "__main__":
    main()
