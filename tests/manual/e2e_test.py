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

        # Collect with local storage
        storage_base = str(repo_dir / "test-data")
        self._run_command(
            [
                self.venv_python,
                "grin.py",
                "collect",
                "--run-name",
                self.run_name,
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
                self.run_name,
                "--queue",
                "converted",
                "--limit",
                "1",
            ],
            cwd=repo_dir,
            timeout=180,
        )

        # Sync with local storage - previous queue
        self._run_command(
            [
                self.venv_python,
                "grin.py",
                "sync",
                "pipeline",
                "--run-name",
                self.run_name,
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
                self.run_name,
                "--limit",
                "5",
            ],
            cwd=repo_dir,
            timeout=180,
        )

        # Export to CSV (test the export functionality)
        csv_output = f"{self.run_name}_books.csv"
        self._run_command(
            [
                self.venv_python,
                "grin.py",
                "export",
                "--run-name",
                self.run_name,
                "--output",
                csv_output,
            ],
            cwd=repo_dir,
            timeout=60,
        )

        # Configuration 2: R2 storage (if credentials available)
        logger.info("=== Testing local machine with R2 storage ===")
        r2_creds_file = Path.home() / ".config" / "grin-to-s3" / "r2_credentials.json"
        if r2_creds_file.exists():
            # Collect with R2 storage (let it use user's config)
            self._run_command(
                [
                    sys.executable,
                    "grin.py",
                    "collect",
                    "--run-name",
                    self.run_name,
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
                    self.run_name,
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
                    self.run_name,
                    "--limit",
                    "5",
                ],
                cwd=repo_dir,
                timeout=180,
            )

            # Export to CSV (test the export functionality)
            csv_output = f"{self.run_name}_books.csv"
            self._run_command(
                [
                    self.venv_python,
                    "grin.py",
                    "export",
                    "--run-name",
                    self.run_name,
                    "--output",
                    csv_output,
                ],
                cwd=repo_dir,
                timeout=60,
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

        # Collect with MinIO storage in Docker - use standard bucket names that get created
        self._run_command(
            [
                "./grin-docker",
                "collect",
                "--run-name",
                self.run_name,
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
                self.run_name,
                "--queue",
                "converted",
                "--limit",
                "1",
            ],
            cwd=repo_dir,
            timeout=180,
            env=docker_env,
        )

        # Configuration 2: R2 storage in Docker (if credentials available)
        logger.info("=== Testing Docker with R2 storage ===")
        r2_creds_file = Path.home() / ".config" / "grin-to-s3" / "r2_credentials.json"
        if r2_creds_file.exists():
            # Collect with R2 storage in Docker (let it use user's config)
            self._run_command(
                [
                    "./grin-docker",
                    "collect",
                    "--run-name",
                    self.run_name,
                    "--limit",
                    str(self.limit),
                    "--storage",
                    "r2",
                    "--library-directory",
                    "Harvard",
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
                    self.run_name,
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
    parser.add_argument("--run-name", default="e2e_test", help="Run name for the test pipeline (default: test)")
    parser.add_argument("--limit", type=int, default=20, help="Limit number of books to process (default: 20)")
    parser.add_argument("--no-cleanup", action="store_true", help="Don't clean up temporary directory after tests")

    args = parser.parse_args()

    runner = E2ETestRunner(run_name=args.run_name, limit=args.limit, cleanup=not args.no_cleanup)

    runner.run_all_tests()


if __name__ == "__main__":
    main()
