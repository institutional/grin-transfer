"""
Common utilities for V2 architecture

Shared functions and patterns to eliminate code duplication across V2 modules.
"""

import asyncio
import logging
import os
import shutil
import subprocess
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

import aiofiles
import aiohttp

from grin_to_s3.storage.book_manager import BucketConfig

logger = logging.getLogger(__name__)

# Default directory names for local storage
LOCAL_STORAGE_DEFAULTS = {
    "bucket_raw": "raw",
    "bucket_meta": "meta",
    "bucket_full": "full"
}


def extract_bucket_config(storage_type: str, config_dict: dict) -> BucketConfig:
    """
    Extract bucket configuration with appropriate defaults based on storage type.

    Args:
        storage_type: Type of storage ("local", "s3", "r2", etc.)
        config_dict: Configuration dictionary containing bucket names

    Returns:
        BucketConfig with bucket_raw, bucket_meta, bucket_full keys
    """
    if storage_type == "local":
        # For local storage, use standard directory names as defaults
        local_config: BucketConfig = {
            "bucket_raw": config_dict.get("bucket_raw", LOCAL_STORAGE_DEFAULTS["bucket_raw"]),
            "bucket_meta": config_dict.get("bucket_meta", LOCAL_STORAGE_DEFAULTS["bucket_meta"]),
            "bucket_full": config_dict.get("bucket_full", LOCAL_STORAGE_DEFAULTS["bucket_full"]),
        }
        return local_config
    else:
        # For cloud storage, bucket names are required
        cloud_config: BucketConfig = {
            "bucket_raw": config_dict.get("bucket_raw", ""),
            "bucket_meta": config_dict.get("bucket_meta", ""),
            "bucket_full": config_dict.get("bucket_full", ""),
        }
        return cloud_config

# HTTP Client Configuration
DEFAULT_TIMEOUT = 60
DEFAULT_CONNECTOR_LIMITS = {"limit": 10, "limit_per_host": 5}


@asynccontextmanager
async def create_http_session(timeout: int | None = None):
    """
    Create properly configured aiohttp session with consistent settings.

    Args:
        timeout: Request timeout in seconds (default: 60)

    Yields:
        aiohttp.ClientSession: Configured session
    """
    timeout_config = aiohttp.ClientTimeout(total=timeout or DEFAULT_TIMEOUT, connect=10)
    connector = aiohttp.TCPConnector(
        limit=DEFAULT_CONNECTOR_LIMITS["limit"], limit_per_host=DEFAULT_CONNECTOR_LIMITS["limit_per_host"]
    )

    async with aiohttp.ClientSession(timeout=timeout_config, connector=connector) as session:
        yield session


def expand_path(path: str) -> str:
    """
    Expand user home directory in file paths.

    Args:
        path: File path that may contain ~

    Returns:
        str: Expanded absolute path
    """
    return os.path.expanduser(path)


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


def calculate_transfer_speed(bytes_transferred: int, duration_seconds: float) -> str:
    """
    Calculate and format transfer speed.

    Args:
        bytes_transferred: Number of bytes transferred
        duration_seconds: Time taken in seconds

    Returns:
        str: Formatted speed (e.g., "15.2 MB/s", "1.3 GB/s")
    """
    if duration_seconds <= 0:
        return "0 B/s"

    bytes_per_second = bytes_transferred / duration_seconds
    return f"{format_bytes(int(bytes_per_second))}/s"


class SlidingWindowRateCalculator:
    """
    Calculate processing rates using a sliding window for more accurate ETAs.

    This prevents ETAs from being skewed by startup overhead or early slow batches
    by using only the most recent batch completions for rate calculation.
    """

    def __init__(self, window_size: int = 5):
        """
        Initialize the rate calculator.

        Args:
            window_size: Number of recent batches to consider for rate calculation
        """
        self.window_size = window_size
        self.batch_times: list[tuple[float, int]] = []  # (timestamp, processed_count)

    def add_batch(self, timestamp: float, processed_count: int) -> None:
        """
        Add a batch completion record.

        Args:
            timestamp: Time when batch was completed
            processed_count: Cumulative number of items processed
        """
        self.batch_times.append((timestamp, processed_count))

        # Keep only recent batches for rate calculation
        if len(self.batch_times) > self.window_size:
            self.batch_times.pop(0)

    def get_rate(self, fallback_start_time: float, fallback_processed_count: int) -> float:
        """
        Calculate current processing rate based on sliding window.

        Args:
            fallback_start_time: Start time for fallback rate calculation
            fallback_processed_count: Total processed count for fallback

        Returns:
            Processing rate in items per second
        """
        if len(self.batch_times) >= 2:
            # Use time and count span from oldest to newest batch in window
            oldest_time, oldest_count = self.batch_times[0]
            newest_time, newest_count = self.batch_times[-1]

            time_span = newest_time - oldest_time
            count_span = newest_count - oldest_count

            return count_span / max(1, time_span)
        else:
            # Fallback to overall rate for first batch
            current_time = time.time()
            overall_elapsed = current_time - fallback_start_time
            return fallback_processed_count / max(1, overall_elapsed)


class ProgressReporter:
    """
    Consistent progress reporting for long-running operations.
    """

    def __init__(self, operation_name: str, total_items: int | None = None):
        self.operation_name = operation_name
        self.total_items = total_items
        self.start_time: float | None = None
        self.last_report_time: float | None = None
        self.processed_items = 0
        self.processed_bytes = 0
        self.last_record_id: str | None = None
        self.first_record_id: str | None = None

    def start(self) -> None:
        """Start progress tracking."""
        self.start_time = time.perf_counter()
        self.last_report_time = self.start_time

    def increment(
        self, items: int = 1, bytes_count: int = 0, force: bool = False, record_id: str | None = None
    ) -> None:
        """
        Increment progress by the specified amount.

        Args:
            items: Number of items processed in this increment
            bytes_count: Number of bytes processed in this increment
            force: Force progress report even if time threshold not met
            record_id: ID of the record being processed (e.g., barcode)
        """
        self.processed_items += items
        self.processed_bytes += bytes_count

        # Track record IDs
        if record_id:
            if self.first_record_id is None:
                self.first_record_id = record_id
            self.last_record_id = record_id

        current_time = time.perf_counter()

        # Report every 5 seconds or when forced
        if force or (self.last_report_time is not None and current_time - self.last_report_time >= 5.0):
            elapsed = current_time - (self.start_time or 0)

            # Build progress message
            parts = []

            if self.total_items:
                percentage = (self.processed_items / self.total_items) * 100
                parts.append(f"{self.processed_items}/{self.total_items} ({percentage:.1f}%)")
            else:
                parts.append(f"{self.processed_items:,} {pluralize(self.processed_items, 'item')}")

            if self.processed_bytes > 0:
                parts.append(f"{format_bytes(self.processed_bytes)}")
                parts.append(f"{calculate_transfer_speed(self.processed_bytes, elapsed)}")

            parts.append(f"elapsed: {format_duration(elapsed)}")

            # Add record range if available
            if self.first_record_id and self.last_record_id:
                if self.first_record_id == self.last_record_id:
                    parts.append(f"current: {self.last_record_id}")
                else:
                    parts.append(f"range: {self.first_record_id}...{self.last_record_id}")

            print(f"Progress: {' | '.join(parts)}", flush=True)
            self.last_report_time = current_time

    def finish(self) -> None:
        """Complete progress tracking and show final summary."""
        if self.start_time:
            total_time = time.perf_counter() - self.start_time

            parts = [f"Completed {self.operation_name}"]
            parts.append(f"{self.processed_items:,} {pluralize(self.processed_items, 'item')}")

            if self.processed_bytes > 0:
                parts.append(f"{format_bytes(self.processed_bytes)}")
                parts.append(f"{calculate_transfer_speed(self.processed_bytes, total_time)}")

            parts.append(f"total time: {format_duration(total_time)}")

            print(f"✓ {' | '.join(parts)}", flush=True)


class BackupManager:
    """Utility class for creating and managing timestamped backups of files."""

    def __init__(self, backup_dir):
        """Initialize backup manager with target backup directory."""
        self.backup_dir = Path(backup_dir)

    async def backup_file(self, source_file, file_type: str = "file") -> bool:
        """Create a timestamped backup of a file.

        Args:
            source_file: Path to the source file to backup
            file_type: Type description for user feedback (e.g., "database", "progress file")

        Returns:
            True if backup was successful or not needed, False if failed.
        """
        source_file = Path(source_file)

        if not source_file.exists():
            logger.debug(f"No existing {file_type} to backup")
            return True

        try:
            # Create backups directory
            self.backup_dir.mkdir(exist_ok=True)

            # Create timestamped backup filename
            now = datetime.now(UTC)
            timestamp = now.strftime("%Y%m%d_%H%M%S")
            backup_filename = f"{source_file.stem}_backup_{timestamp}{source_file.suffix}"
            backup_path = self.backup_dir / backup_filename

            # Copy the file
            if source_file.suffix == ".json":
                # For JSON files, use async file operations
                async with aiofiles.open(source_file) as src:
                    content = await src.read()
                async with aiofiles.open(backup_path, "w") as dst:
                    await dst.write(content)
            else:
                # For other files (like SQLite), use synchronous copy
                shutil.copy2(source_file, backup_path)

            logger.debug(f"{file_type.title()} backed up: {backup_filename}")

            # Keep only the last 10 backups to prevent disk space issues
            await self._cleanup_old_backups(source_file.stem, source_file.suffix)

            return True

        except Exception as e:
            logger.warning(f"Failed to backup {file_type}: {e}")
            logger.warning(f"Proceeding with execution, but {file_type} corruption risk exists")
            return False

    async def _cleanup_old_backups(self, file_stem: str, file_suffix: str) -> None:
        """Keep only the most recent 10 backups for a specific file."""
        try:
            backup_pattern = f"{file_stem}_backup_*{file_suffix}"
            backup_files = list(self.backup_dir.glob(backup_pattern))

            if len(backup_files) > 10:
                # Sort by modification time (newest first)
                backup_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)

                # Remove old backups beyond the 10 most recent
                for old_backup in backup_files[10:]:
                    old_backup.unlink()
                    logger.debug(f"Removed old backup: {old_backup.name}")

        except Exception as e:
            logger.warning(f"Failed to cleanup old backups: {e}")


def get_gpg_key_path(custom_path: str | None = None) -> Path:
    """
    Get path to GPG key file, checking custom path first then default location.

    Args:
        custom_path: Custom path to GPG key file

    Returns:
        Path to GPG key file
    """
    if custom_path:
        return Path(custom_path).expanduser()

    # Default location in config directory
    home = Path.home()
    return home / ".config" / "grin-to-s3" / "gpg_key.asc"


def get_gpg_passphrase_from_secrets(secrets_dir: str | None = None) -> str:
    """
    Get GPG passphrase from gpg_passphrase.asc file in secrets directory.

    Args:
        secrets_dir: Directory containing secrets files (searches home directory if not specified)

    Returns:
        GPG passphrase

    Raises:
        FileNotFoundError: If passphrase file is not found
        ValueError: If passphrase file is empty
    """
    # Search for passphrase file in the same way as GRIN credentials
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
            try:
                passphrase = passphrase_file.read_text(encoding="utf-8").strip()
                if not passphrase:
                    raise ValueError(f"GPG passphrase file is empty: {passphrase_file}")
                return passphrase
            except UnicodeDecodeError as e:
                raise ValueError(f"GPG passphrase file has invalid encoding: {passphrase_file}") from e

    # If we get here, no passphrase file was found
    default_path = Path.home() / ".config" / "grin-to-s3" / "gpg_passphrase.asc"
    raise FileNotFoundError(f"GPG passphrase file not found. Expected at: {default_path}")


def check_gpg_keys_available() -> bool:
    """
    Check if GPG has any secret keys available for decryption.

    Returns:
        True if secret keys are available, False otherwise
    """
    try:
        result = subprocess.run(
            ["gpg", "--list-secret-keys", "--batch", "--no-tty"], capture_output=True, check=True, timeout=10
        )
        # If we have any output, we have secret keys
        return len(result.stdout.strip()) > 0
    except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return False


async def import_gpg_key_if_available(gpg_key_file: str | None = None) -> bool:
    """
    Import GPG key from file if available.

    Args:
        gpg_key_file: Custom path to GPG key file

    Returns:
        True if key was imported or already available, False if no key found
    """
    gpg_key_path = get_gpg_key_path(gpg_key_file)

    if not gpg_key_path.exists():
        return False

    try:
        # Import the key
        subprocess.run(
            ["gpg", "--quiet", "--batch", "--no-tty", "--import", str(gpg_key_path)],
            capture_output=True,
            check=True,
            timeout=30,
        )
        return True
    except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return False


async def decrypt_gpg_data(
    encrypted_data: bytes, gpg_key_file: str | None = None, secrets_dir: str | None = None
) -> bytes:
    """
    Decrypt GPG-encrypted data using the system's gpg command.

    Args:
        encrypted_data: The GPG-encrypted bytes
        gpg_key_file: Optional path to GPG key file to import
        secrets_dir: Directory containing secrets files (searches home directory if not specified)

    Returns:
        The decrypted bytes

    Raises:
        subprocess.CalledProcessError: If GPG decryption fails
        RuntimeError: If GPG is not available or other issues occur
    """
    # Get passphrase from secrets directory if needed
    passphrase = None
    try:
        passphrase = get_gpg_passphrase_from_secrets(secrets_dir)
    except (FileNotFoundError, ValueError):
        # No passphrase file found or empty - will try without passphrase
        pass

    # Try to import key from file if specified or available in default location
    if gpg_key_file or get_gpg_key_path().exists():
        await import_gpg_key_if_available(gpg_key_file)

    loop = asyncio.get_event_loop()

    def _decrypt_with_gpg():
        try:
            if passphrase:
                # Use gpg command with passphrase via stdin
                # --quiet: suppress output
                # --batch: non-interactive mode
                # --no-tty: don't use TTY for passphrase prompts
                # --pinentry-mode loopback: read passphrase from stdin
                # --passphrase-fd 0: read passphrase from stdin (fd 0)
                # --decrypt: decrypt mode
                gpg_input = passphrase.encode("utf-8") + b"\n" + encrypted_data
                result = subprocess.run(
                    [
                        "gpg",
                        "--quiet",
                        "--batch",
                        "--no-tty",
                        "--pinentry-mode",
                        "loopback",
                        "--passphrase-fd",
                        "0",
                        "--decrypt",
                    ],
                    input=gpg_input,
                    capture_output=True,
                    check=True,
                    timeout=600,  # 10 minute timeout for decryption
                    env={**os.environ, "GPG_TTY": ""},  # Disable TTY usage
                )
            else:
                # Use gpg command without passphrase (key has no passphrase)
                # --quiet: suppress output
                # --batch: non-interactive mode
                # --no-tty: don't use TTY for passphrase prompts
                # --yes: assume yes for questions
                # --decrypt: decrypt mode
                result = subprocess.run(
                    ["gpg", "--quiet", "--batch", "--no-tty", "--yes", "--decrypt"],
                    input=encrypted_data,
                    capture_output=True,
                    check=True,
                    timeout=600,  # 10 minute timeout for decryption
                    env={**os.environ, "GPG_TTY": ""},  # Disable TTY usage
                )
            return result.stdout
        except FileNotFoundError:
            raise RuntimeError("GPG command not found. Please install GPG on your system.") from None
        except subprocess.TimeoutExpired:
            raise RuntimeError("GPG decryption timed out after 10 minutes.") from None
        except subprocess.CalledProcessError as e:
            stderr_msg = e.stderr.decode("utf-8", errors="replace") if e.stderr else "Unknown error"

            # Check for specific GPG key-related error messages
            if "no secret key" in stderr_msg.lower() or "secret key not available" in stderr_msg.lower():
                gpg_key_path = get_gpg_key_path(gpg_key_file)
                if gpg_key_path.exists():
                    raise RuntimeError(
                        f"GPG private key found at {gpg_key_path} but import failed. "
                        f"Please import manually: gpg --import {gpg_key_path}"
                    ) from e
                else:
                    raise RuntimeError(
                        f"GPG private key not found. Place your GPG key at {gpg_key_path} "
                        f"or import manually with: gpg --import <key_file>"
                    ) from e
            elif "public key not found" in stderr_msg.lower():
                raise RuntimeError("GPG public key not found. Please import the appropriate GPG keys.") from e
            elif "bad session key" in stderr_msg.lower() or "inappropriate ioctl" in stderr_msg.lower():
                raise RuntimeError(
                    "GPG passphrase required but not available in non-interactive mode. "
                    "Please decrypt the key manually or use a key without passphrase for automated processing."
                ) from e
            elif "problem with the agent" in stderr_msg.lower():
                raise RuntimeError(
                    "GPG agent error. Try running 'gpg-connect-agent reloadagent /bye' or use a key without passphrase."
                ) from e
            else:
                raise RuntimeError(f"GPG decryption failed: {stderr_msg}") from e

    return await loop.run_in_executor(None, _decrypt_with_gpg)


def get_gpg_passphrase_file_path(secrets_dir: str | None = None) -> str | None:
    """
    Get path to GPG passphrase file in secrets directory.

    Args:
        secrets_dir: Directory containing secrets files (searches home directory if not specified)

    Returns:
        Path to passphrase file or None if not found
    """
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


async def decrypt_gpg_file(
    encrypted_file_path: str, decrypted_file_path: str, gpg_key_file: str | None = None, secrets_dir: str | None = None
) -> None:
    """
    Decrypt GPG-encrypted file to another file using the system's gpg command.

    Args:
        encrypted_file_path: Path to the GPG-encrypted file
        decrypted_file_path: Path where decrypted file should be saved
        gpg_key_file: Optional path to GPG key file to import
        secrets_dir: Directory containing secrets files (searches home directory if not specified)

    Raises:
        subprocess.CalledProcessError: If GPG decryption fails
        RuntimeError: If GPG is not available or other issues occur
    """
    # Get passphrase file path if available
    passphrase_file_path = get_gpg_passphrase_file_path(secrets_dir)

    # Try to import key from file if specified or available in default location
    if gpg_key_file or get_gpg_key_path().exists():
        await import_gpg_key_if_available(gpg_key_file)

    loop = asyncio.get_event_loop()

    def _decrypt_file_with_gpg():
        try:
            # Use direct passphrase file (no temp file needed since it already exists)
            env = {**os.environ, "GPG_TTY": ""}
            if passphrase_file_path:
                subprocess.run(
                    [
                        "gpg",
                        "--batch",
                        "--yes",
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
            else:
                # No passphrase file available - try without passphrase
                subprocess.run(
                    [
                        "gpg",
                        "--batch",
                        "--yes",
                        "--output",
                        decrypted_file_path,
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
        except subprocess.CalledProcessError as e:
            stderr_msg = e.stderr.decode("utf-8", errors="replace") if e.stderr else "Unknown error"
            # Same error handling as decrypt_gpg_data
            if "no secret key" in stderr_msg.lower() or "secret key not available" in stderr_msg.lower():
                gpg_key_path = get_gpg_key_path(gpg_key_file)
                raise RuntimeError(
                    f"GPG secret key not found. Please import your private key:\n"
                    f"  gpg --import {gpg_key_path}\n"
                    f"Or copy your key to: {gpg_key_path}"
                ) from e
            elif "public key not found" in stderr_msg.lower():
                raise RuntimeError("GPG public key not found. Please import the appropriate GPG keys.") from e
            elif "bad session key" in stderr_msg.lower() or "inappropriate ioctl" in stderr_msg.lower():
                raise RuntimeError(
                    "GPG passphrase required but not available in non-interactive mode. "
                    "Please decrypt the key manually or use a key without passphrase for automated processing."
                ) from e
            elif "problem with the agent" in stderr_msg.lower():
                raise RuntimeError(
                    "GPG agent error. Try running 'gpg-connect-agent reloadagent /bye' or use a key without passphrase."
                ) from e
            else:
                raise RuntimeError(f"GPG decryption failed: {stderr_msg}") from e

    await loop.run_in_executor(None, _decrypt_file_with_gpg)





def auto_configure_minio(storage_config: dict) -> None:
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


def is_docker_environment() -> bool:
    """Check if we're running inside a Docker container."""
    return os.path.exists("/.dockerenv") or os.environ.get("DOCKER_ENV") == "true"


def print_oauth_setup_instructions() -> None:
    """Print appropriate OAuth setup instructions based on environment."""
    if is_docker_environment():
        print("\nTo set up OAuth credentials in Docker:")
        print("./grin-docker python grin.py auth setup")
    else:
        print("\nTo set up OAuth credentials:")
        print("python grin.py auth setup")


async def setup_storage_with_checks(storage_type: str, storage_config: dict,
                                   required_credentials: list[str] | None = None) -> None:
    """Set up storage with auto-configuration and connectivity checks.

    Args:
        storage_type: Type of storage ("minio", "r2", "s3", "local")
        storage_config: Storage configuration dictionary to modify
        required_credentials: List of required credential keys for auto-config check
    """
    if storage_type == "minio":
        # Auto-configure MinIO if credentials are missing
        if required_credentials is None:
            required_credentials = ["endpoint_url", "access_key", "secret_key"]

        if not all(k in storage_config for k in required_credentials):
            auto_configure_minio(storage_config)

        # Check connectivity
        await check_minio_connectivity(storage_config)


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
        async with create_http_session(timeout=5) as session:
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


async def create_storage_buckets_or_directories(storage_type: str, storage_config: dict) -> None:
    """Create all required buckets or directories for the storage type.

    Args:
        storage_type: Type of storage ("minio", "r2", "s3", "local")
        storage_config: Storage configuration dictionary
    """
    if storage_type == "local":
        # Create directories for local storage
        base_path = storage_config.get("base_path")
        if not base_path:
            raise ValueError("Local storage requires base_path in configuration")

        base_path = Path(base_path)
        # Create the main directories using default names
        (base_path / LOCAL_STORAGE_DEFAULTS["bucket_raw"]).mkdir(parents=True, exist_ok=True)
        (base_path / LOCAL_STORAGE_DEFAULTS["bucket_meta"]).mkdir(parents=True, exist_ok=True)
        (base_path / LOCAL_STORAGE_DEFAULTS["bucket_full"]).mkdir(parents=True, exist_ok=True)
        print(f"Created local storage directories at {base_path}")

    else:
        print(f"Configured with {storage_type} cloud storage")



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


def setup_logging(level: str = "INFO", log_file: str | None = None, append: bool = True) -> None:
    """
    Configure logging for all pipeline operations.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_file: Optional log file path (defaults to timestamped file in logs/)
        append: Whether to append to existing log file (default: True)
    """
    # Create root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))

    # Clear any existing handlers
    root_logger.handlers.clear()

    # Suppress debug logging from dependency modules
    # Set dependency modules to INFO level to reduce noise
    logging.getLogger("aiosqlite").setLevel(logging.INFO)
    logging.getLogger("urllib3").setLevel(logging.INFO)
    logging.getLogger("requests").setLevel(logging.INFO)
    logging.getLogger("google").setLevel(logging.INFO)
    logging.getLogger("google.auth").setLevel(logging.INFO)
    logging.getLogger("google.oauth2").setLevel(logging.INFO)
    logging.getLogger("asyncio").setLevel(logging.INFO)
    # Suppress boto3/botocore verbose logging
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("s3transfer").setLevel(logging.WARNING)
    logging.getLogger("aioboto3").setLevel(logging.WARNING)
    logging.getLogger("aiobotocore").setLevel(logging.WARNING)
    logging.getLogger("s3fs").setLevel(logging.WARNING)
    # Suppress specific botocore sub-modules
    logging.getLogger("botocore.hooks").setLevel(logging.WARNING)
    logging.getLogger("botocore.endpoint").setLevel(logging.WARNING)
    logging.getLogger("botocore.credentials").setLevel(logging.WARNING)
    logging.getLogger("botocore.awsrequest").setLevel(logging.WARNING)
    logging.getLogger("botocore.regions").setLevel(logging.WARNING)

    # Create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    # File handler (auto-generate timestamped filename if not provided)
    if log_file is None:
        # Create logs directory using environment variable or default
        logs_dir = Path(os.environ.get("GRIN_LOG_DIR", "logs"))
        logs_dir.mkdir(exist_ok=True)

        # Generate timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = str(logs_dir / f"grin_pipeline_{timestamp}.log")
    else:
        # Ensure parent directory exists for custom log file
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(str(log_file), mode="a" if append else "w")
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # Force immediate flushing for file handler
    def make_flush_func(h):
        return lambda: h.stream.flush() if hasattr(h, "stream") else None

    # Replace flush method - type ignore for dynamic assignment
    file_handler.flush = make_flush_func(file_handler)  # type: ignore[method-assign]

    # Show user-friendly path (host-relative for Docker)
    display_path = log_file
    if is_docker_environment() and log_file.startswith("/app/logs/"):
        # Convert container path to host path for Docker users
        display_path = log_file.replace("/app/logs/", "docker-data/logs/")

    print(f"Logging to file: {display_path}\n")
    logger = logging.getLogger(__name__)
    logger.info(f"Logging to file: {log_file}")
    logger.info(f"Logging initialized at {level} level")
