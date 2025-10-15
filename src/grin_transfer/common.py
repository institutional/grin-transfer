"""
Common utilities for V2 architecture

Shared functions and patterns to eliminate code duplication across V2 modules.
"""

import asyncio
import fcntl
import gzip
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import TextIO

import aiohttp

from grin_transfer.docker import is_docker_environment
from grin_transfer.run_config import StorageConfigDict

from .auth.grin_auth import find_credential_file

logger = logging.getLogger(__name__)


# Common type aliases
type Barcode = str
type BarcodeSet = set[str]


# HTTP Client Configuration
DEFAULT_TIMEOUT = 60

DEFAULT_COMPRESSION_LEVEL = 1  # Fastest


class SessionLock:
    """File-based session lock using native fcntl locking.

    Prevents concurrent access to shared resources within the same run.
    Lock is automatically released when the process exits.
    """

    def __init__(self, lock_file_path: Path):
        self.lock_file_path = lock_file_path
        self.lock_file: TextIO | None = None

    def acquire(self) -> bool:
        """Try to acquire exclusive lock.

        Returns:
            bool: True if lock acquired, False if already locked by another process
        """
        self.lock_file_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock_file = open(self.lock_file_path, "w")
        try:
            fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except BlockingIOError:
            self.lock_file.close()
            self.lock_file = None
            return False

    def release(self) -> None:
        """Release the lock."""
        if self.lock_file:
            fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_UN)
            self.lock_file.close()
            self.lock_file = None

    def __enter__(self):
        """Context manager entry."""
        if not self.acquire():
            raise RuntimeError("Could not acquire session lock")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.release()


def format_bytes(size_bytes: int) -> str:
    """
    Format byte count as human-readable string.

    Args:
        size_bytes: Size in bytes

    Returns:
        str: Formatted size (e.g., "1.5 MB", "2.3 GB")
    """
    if size_bytes == 0:
        return "0 B"

    size_names = ["B", "KB", "MB", "GB", "TB"]
    size_float = float(size_bytes)

    for i, unit in enumerate(size_names):
        if size_float < 1024.0 or i == len(size_names) - 1:
            if i == 0:  # Bytes - no decimal
                return f"{int(size_float)} {unit}"
            else:  # Larger units - 1 decimal place
                return f"{size_float:.1f} {unit}"
        size_float /= 1024.0

    return f"{size_float:.1f} TB"


def pluralize(count: int, word: str) -> str:
    """
    Return correct singular/plural form of a word.

    Args:
        count: Number of items
        word: Base word (singular form)

    Returns:
        str: Correctly pluralized word
    """
    return word if count == 1 else f"{word}s"


def format_duration(seconds: float) -> str:
    """
    Format duration as human-readable string.

    Args:
        seconds: Duration in seconds

    Returns:
        str: Formatted duration (e.g., "1.5s", "2m 30s", "1h 15m")
    """
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    elif seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        remaining_seconds = seconds % 60
        return f"{minutes}m {remaining_seconds:.0f}s"
    else:
        hours = int(seconds // 3600)
        remaining_minutes = int((seconds % 3600) // 60)
        return f"{hours}h {remaining_minutes}m"


def _validate_barcode(barcode: str) -> None:
    """Validate a single barcode.

    Args:
        barcode: Barcode to validate

    Raises:
        ValueError: If the barcode is invalid
    """
    if not barcode:
        raise ValueError("Empty barcode found")
    if len(barcode) < 3 or len(barcode) > 50:
        raise ValueError(f"Barcode '{barcode}' has invalid length (must be 3-50 characters)")
    # Check for reasonable characters (alphanumeric, dash, underscore)
    if not all(c.isalnum() or c in "-_" for c in barcode):
        raise ValueError(
            f"Barcode '{barcode}' contains invalid characters (only alphanumeric, dash, underscore allowed)"
        )


def validate_and_parse_barcodes(barcodes_str: str) -> list[str]:
    """Validate and parse comma-separated barcode string.

    Args:
        barcodes_str: Comma-separated string of barcodes

    Returns:
        List of validated barcodes

    Raises:
        ValueError: If any barcode is invalid
    """
    if not barcodes_str.strip():
        raise ValueError("Barcodes string cannot be empty")

    # Split by comma and clean up whitespace
    barcodes = [barcode.strip() for barcode in barcodes_str.split(",")]

    # Remove empty entries
    barcodes = [barcode for barcode in barcodes if barcode]

    if not barcodes:
        raise ValueError("No valid barcodes found")

    # Validate each barcode
    for barcode in barcodes:
        _validate_barcode(barcode)

    return barcodes


def read_barcodes_from_file(file_path: str) -> list[str]:
    """Read and validate barcodes from a text file.

    Args:
        file_path: Path to the text file containing barcodes

    Returns:
        List of validated barcodes

    Raises:
        FileNotFoundError: If the file doesn't exist
        ValueError: If any barcode is invalid or no valid barcodes are found
    """
    from pathlib import Path

    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Barcodes file not found: {file_path}")

    if not path.is_file():
        raise ValueError(f"Path is not a file: {file_path}")

    try:
        with open(path, encoding="utf-8") as f:
            lines = f.readlines()
    except OSError as e:
        raise ValueError(f"Failed to read barcodes file '{file_path}': {e}") from e

    barcodes = []
    for line_num, line in enumerate(lines, 1):
        # Strip whitespace and skip empty lines and comments
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        try:
            _validate_barcode(line)
            barcodes.append(line)
        except ValueError as e:
            raise ValueError(f"Invalid barcode on line {line_num}: {e}") from e

    if not barcodes:
        raise ValueError(f"No valid barcodes found in file: {file_path}")

    return barcodes


def parse_barcode_arguments(barcodes_str: str | None, barcodes_file: str | None) -> list[str] | None:
    """Parse and validate barcodes from either string or file input.

    Args:
        barcodes_str: Comma-separated string of barcodes (or None)
        barcodes_file: Path to file containing barcodes (or None)

    Returns:
        List of validated barcodes, or None if no input provided

    Raises:
        ValueError: If both inputs provided, or if any barcode is invalid
        FileNotFoundError: If file doesn't exist
    """
    has_barcodes = barcodes_str is not None and barcodes_str.strip()
    has_barcodes_file = barcodes_file is not None and barcodes_file.strip()

    # Check mutual exclusivity
    if has_barcodes and has_barcodes_file:
        raise ValueError("Cannot specify both --barcodes and --barcodes-file. Use one or the other.")

    # Return None if neither is provided
    if not has_barcodes and not has_barcodes_file:
        return None

    if has_barcodes:
        assert barcodes_str is not None  # Type checker hint - already checked above
        return validate_and_parse_barcodes(barcodes_str)
    else:
        assert barcodes_file is not None  # Type checker hint - already checked above
        return read_barcodes_from_file(barcodes_file)


def get_gpg_passphrase_file_path(secrets_dir: str | None = None) -> str | None:
    """
    Get path to GPG passphrase file in secrets directory.

    Args:
        secrets_dir: Directory containing secrets files (searches home directory if not specified)

    Returns:
        Path to passphrase file or None if not found
    """
    # First check credentials directory (configurable, used by Docker)
    creds_passphrase_file = find_credential_file("gpg_passphrase.asc")
    if creds_passphrase_file:
        return str(creds_passphrase_file)

    # Search for passphrase file in the same way as get_gpg_passphrase_from_secrets
    search_paths = []

    if secrets_dir:
        search_paths.append(Path(secrets_dir).expanduser())
    else:
        # Search common locations in home directory
        home = Path.home()
        search_paths.extend([home / ".config" / "grin-to-s3", home, home / ".grin", home / ".config"])

    # Look for gpg_passphrase.asc file
    for search_path in search_paths:
        passphrase_file = search_path / "gpg_passphrase.asc"
        if passphrase_file.exists():
            return str(passphrase_file)

    return None


async def decrypt_gpg_file(encrypted_file_path: str, decrypted_file_path: str, secrets_dir: str | None = None) -> None:
    """
    Decrypt GPG-encrypted file to another file using the system's gpg command.

    Args:
        encrypted_file_path: Path to the GPG-encrypted file
        decrypted_file_path: Path where decrypted file should be saved
        secrets_dir: Directory containing secrets files (searches home directory if not specified)

    Raises:
        subprocess.CalledProcessError: If GPG decryption fails
        RuntimeError: If GPG is not available or other issues occur
    """
    # Get passphrase file path if available
    passphrase_file_path = get_gpg_passphrase_file_path(secrets_dir)

    # Keys should already be available in GPG keyring

    loop = asyncio.get_event_loop()

    def _decrypt_file_with_gpg():
        try:
            # Require passphrase file for decryption
            if not passphrase_file_path:
                raise RuntimeError("GPG passphrase file required for decryption but not found")

            # Use direct passphrase file with performance optimization flags
            env = {**os.environ, "GPG_TTY": ""}
            subprocess.run(
                [
                    "gpg",
                    "--batch",
                    "--yes",
                    "--no-use-agent",
                    "--trust-model",
                    "always",
                    "--no-auto-check-trustdb",
                    "--pinentry-mode",
                    "loopback",
                    "--output",
                    decrypted_file_path,
                    "--passphrase-file",
                    passphrase_file_path,
                    "--quiet",
                    "--decrypt",
                    encrypted_file_path,
                ],
                capture_output=True,
                check=True,
                timeout=600,  # 10 minute timeout for decryption
                env=env,
            )

            return True
        except FileNotFoundError:
            raise RuntimeError("GPG command not found. Please install GPG on your system.") from None
        except subprocess.TimeoutExpired:
            raise RuntimeError("GPG decryption timed out after 10 minutes.") from None

    await loop.run_in_executor(None, _decrypt_file_with_gpg)


def auto_configure_minio(storage_config: StorageConfigDict) -> None:
    """Auto-configure MinIO credentials for Docker container environment."""
    is_docker = is_docker_environment()

    if "endpoint_url" not in storage_config:
        if is_docker:
            # Use Docker network address when running inside container
            storage_config["endpoint_url"] = "http://minio:9000"
        else:
            # Use localhost when running outside container
            storage_config["endpoint_url"] = "http://localhost:9000"

    # Note: MinIO credentials should be read from secrets directories, not stored in config
    # Default credentials are: access_key=minioadmin, secret_key=minioadmin123

    # Auto-configure bucket names for MinIO if not provided
    if "bucket_raw" not in storage_config:
        storage_config["bucket_raw"] = "grin-raw"
    if "bucket_meta" not in storage_config:
        storage_config["bucket_meta"] = "grin-meta"
    if "bucket_full" not in storage_config:
        storage_config["bucket_full"] = "grin-full"

    endpoint_type = "Docker network" if is_docker else "localhost"
    print(f"Auto-configured MinIO for {endpoint_type} at {storage_config['endpoint_url']}")


def get_command_prefix() -> str:
    """Get the appropriate command prefix based on environment."""
    if is_docker_environment():
        return "./grin-docker"
    else:
        # Use the same Python executable that was used to run this script
        python_exe = os.path.basename(sys.executable)
        return f"{python_exe} grin.py"


def print_oauth_setup_instructions() -> None:
    """Print appropriate OAuth setup instructions based on environment."""
    command_prefix = get_command_prefix()
    if is_docker_environment():
        print("\nTo set up OAuth credentials in Docker:")
        print(f"{command_prefix} auth setup")
    else:
        print("\nTo set up OAuth credentials:")
        print(f"{command_prefix} auth setup")


async def check_minio_connectivity(storage_config: dict) -> None:
    """Check if MinIO is accessible and fail fast if not.

    Args:
        storage_config: Storage configuration dict containing endpoint_url

    Raises:
        SystemExit: If MinIO is not accessible
    """
    endpoint_url = storage_config.get("endpoint_url")
    if not endpoint_url:
        print("❌ Error: MinIO endpoint_url not configured")
        print("   Either start MinIO with: docker-compose -f docker-compose.minio.yml up -d")
        print("   Or use local storage with: --storage local")
        exit(1)

    # Extract base URL for health check
    if endpoint_url.endswith("/"):
        health_url = f"{endpoint_url}minio/health/live"
    else:
        health_url = f"{endpoint_url}/minio/health/live"

    try:
        timeout_config = aiohttp.ClientTimeout(total=5, connect=10)

        async with aiohttp.ClientSession(timeout=timeout_config) as session:
            async with session.get(health_url) as response:
                if response.status == 200:
                    print(f"✅ MinIO connectivity verified: {endpoint_url}")
                    return
                else:
                    print(f"❌ MinIO health check failed with status {response.status}")
    except TimeoutError:
        print(f"❌ MinIO connection timeout: {endpoint_url}")
        print("   MinIO is not responding within 5 seconds")
    except aiohttp.ClientConnectorError as e:
        print(f"❌ Cannot connect to MinIO: {endpoint_url}")
        print(f"   Connection error: {e}")
    except Exception as e:
        print(f"❌ MinIO connectivity check failed: {e}")

    print("\n💡 To fix this:")
    print("   Start MinIO: docker-compose -f docker-compose.minio.yml up -d")
    print("   Or use local storage: --storage local")
    print("   Or provide different MinIO credentials with --endpoint-url")
    exit(1)


class RateLimiter:
    """Simple rate limiter for API requests."""

    def __init__(self, requests_per_second: float = 1.0):
        """Initialize rate limiter.

        Args:
            requests_per_second: Maximum request rate
        """
        self.requests_per_second = requests_per_second
        self.last_request_time = 0.0

    async def acquire(self):
        """Wait until next request is allowed."""
        if self.requests_per_second <= 0:
            return

        now = time.time()
        time_since_last = now - self.last_request_time
        min_interval = 1.0 / self.requests_per_second

        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            await asyncio.sleep(sleep_time)

        self.last_request_time = time.time()


def get_compressed_filename(original_filename: str) -> str:
    """Get compressed filename by adding .gz extension."""
    return f"{original_filename}.gz"


def compress_file_to_temp(source_path: Path, compression_level: int = DEFAULT_COMPRESSION_LEVEL):
    """Async context manager that compresses a file to a temporary location with automatic cleanup.

    Args:
        source_path: Path to source file to compress
        compression_level: Compression level 1-9 (9 = maximum compression)

    Returns:
        Async context manager yielding Path to the temporary compressed file

    Raises:
        CompressionError: If compression fails
        FileNotFoundError: If source file doesn't exist
    """
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    class AsyncCompressedTempFile:
        def __init__(self, source: Path):
            self.source_path = source

        async def __aenter__(self):
            self.temp_file = tempfile.NamedTemporaryFile(suffix=".gz", delete=True)
            temp_path = Path(self.temp_file.name)

            try:
                # Get original file size for logging
                original_size = self.source_path.stat().st_size

                # Perform compression in executor to avoid blocking
                def _compress():
                    with open(self.source_path, "rb") as f_in:
                        with gzip.open(temp_path, "wb", compresslevel=compression_level) as f_out:
                            shutil.copyfileobj(f_in, f_out)

                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, _compress)

                # Get compression stats
                compressed_size = temp_path.stat().st_size
                compression_ratio = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0

                logger.debug(
                    f"Compression completed: {self.source_path.name} "
                    f"({original_size:,} bytes -> {compressed_size:,} bytes, "
                    f"{compression_ratio:.1f}% reduction)"
                )

                return temp_path
            except Exception as e:
                self.temp_file.close()  # Cleanup on error
                raise e

        async def __aexit__(self, exc_type, exc_val, exc_tb):  # noqa: ARG002
            self.temp_file.close()  # Automatic cleanup

    return AsyncCompressedTempFile(source_path)
