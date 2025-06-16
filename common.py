"""
Common utilities for V2 architecture

Shared functions and patterns to eliminate code duplication across V2 modules.
"""

import asyncio
import json
import os
import subprocess
from contextlib import asynccontextmanager
from pathlib import Path

import aiohttp

from storage import V2Storage, create_local_storage, create_minio_storage, create_r2_storage, create_s3_storage

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


def load_json_credentials(credentials_file: str) -> dict:
    """
    Load JSON credentials from file with proper error handling.

    Args:
        credentials_file: Path to JSON credentials file

    Returns:
        dict: Loaded credentials

    Raises:
        FileNotFoundError: If credentials file doesn't exist
        ValueError: If credentials file is invalid JSON
    """
    credentials_path = expand_path(credentials_file)

    if not os.path.exists(credentials_path):
        raise FileNotFoundError(f"Credentials file not found: {credentials_path}")

    try:
        with open(credentials_path) as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in credentials file {credentials_path}: {e}") from e


def validate_required_keys(data: dict, required_keys: list, context: str = "configuration") -> None:
    """
    Validate that required keys exist in configuration dictionary.

    Args:
        data: Dictionary to validate
        required_keys: List of required key names
        context: Description for error messages

    Raises:
        ValueError: If any required keys are missing
    """
    missing_keys = [key for key in required_keys if key not in data]
    if missing_keys:
        raise ValueError(f"Missing required {context} keys: {missing_keys}")


def create_storage_from_config(storage_type: str, config: dict) -> V2Storage:
    """
    Create storage instance based on type and configuration.

    Centralized storage factory to eliminate duplication between modules.

    Args:
        storage_type: Storage backend type (local, minio, r2, s3)
        config: Configuration dictionary for the storage type

    Returns:
        V2Storage: Configured storage instance

    Raises:
        ValueError: If storage type is unknown or configuration is invalid
    """
    match storage_type:
        case "local":
            base_path = config.get("base_path", ".")
            return create_local_storage(base_path)

        case "minio":
            return create_minio_storage(
                endpoint_url=config.get("endpoint_url", "http://localhost:9000"),
                access_key=config.get("access_key", "minioadmin"),
                secret_key=config.get("secret_key", "minioadmin123"),
            )

        case "r2":
            # Check for credentials file (custom path or default)
            credentials_file = config.get("credentials_file")
            if not credentials_file:
                # Use default path in config directory
                home = Path.home()
                credentials_file = home / ".config" / "grin-to-s3" / "r2_credentials.json"

            try:
                creds = load_json_credentials(str(credentials_file))
                validate_required_keys(creds, ["account_id", "access_key", "secret_key"], "R2 credentials")
                return create_r2_storage(
                    account_id=creds["account_id"],
                    access_key=creds["access_key"],
                    secret_key=creds["secret_key"]
                )
            except FileNotFoundError as e:
                if config.get("credentials_file"):
                    # Custom path was specified but file doesn't exist
                    raise ValueError(f"R2 credentials file not found: {credentials_file}") from e
                else:
                    # Default path doesn't exist, provide helpful error
                    raise ValueError(
                        f"R2 credentials file not found at {credentials_file}. "
                        f"Create this file with your R2 credentials or specify a custom path with --credentials-file"
                    ) from e
            except (ValueError, KeyError) as e:
                raise ValueError(f"Invalid R2 credentials file {credentials_file}: {e}") from e

        case "s3":
            bucket = config.get("bucket")
            if not bucket:
                raise ValueError("S3 storage requires bucket name")

            # AWS credentials from environment or ~/.aws/credentials
            return create_s3_storage(bucket=bucket)

        case _:
            raise ValueError(f"Unknown storage type: {storage_type}")


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
        import time

        self.start_time = time.perf_counter()
        self.last_report_time = self.start_time
        print(f"Starting {self.operation_name}...", flush=True)

    def update(self, items: int = 1, bytes_count: int = 0, force: bool = False, record_id: str | None = None) -> None:
        """
        Update progress.

        Args:
            items: Number of items processed in this update
            bytes_count: Number of bytes processed in this update
            force: Force progress report even if time threshold not met
            record_id: ID of the record being processed (e.g., barcode)
        """
        import time

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
        import time

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
        from pathlib import Path

        self.backup_dir = Path(backup_dir)

    async def backup_file(self, source_file, file_type: str = "file") -> bool:
        """Create a timestamped backup of a file.

        Args:
            source_file: Path to the source file to backup
            file_type: Type description for user feedback (e.g., "database", "progress file")

        Returns:
            True if backup was successful or not needed, False if failed.
        """
        import shutil
        from datetime import UTC, datetime
        from pathlib import Path

        source_file = Path(source_file)

        if not source_file.exists():
            print(f"📁 No existing {file_type} to backup")
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
                import aiofiles

                async with aiofiles.open(source_file) as src:
                    content = await src.read()
                async with aiofiles.open(backup_path, "w") as dst:
                    await dst.write(content)
            else:
                # For other files (like SQLite), use synchronous copy
                shutil.copy2(source_file, backup_path)

            print(f"📁 {file_type.title()} backed up: {backup_filename}")

            # Keep only the last 10 backups to prevent disk space issues
            await self._cleanup_old_backups(source_file.stem, source_file.suffix)

            return True

        except Exception as e:
            print(f"⚠️  Failed to backup {file_type}: {e}")
            print(f"   Proceeding with execution, but {file_type} corruption risk exists")
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
                    print(f"🗑️  Removed old backup: {old_backup.name}")

        except Exception as e:
            print(f"⚠️  Failed to cleanup old backups: {e}")


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
        search_paths.extend([
            home / ".config" / "grin-to-s3",
            home,
            home / ".grin",
            home / ".config"
        ])

    # Look for gpg_passphrase.asc file
    for search_path in search_paths:
        passphrase_file = search_path / "gpg_passphrase.asc"
        if passphrase_file.exists():
            try:
                passphrase = passphrase_file.read_text(encoding='utf-8').strip()
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
            ["gpg", "--list-secret-keys", "--batch", "--no-tty"],
            capture_output=True,
            check=True,
            timeout=10
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
            timeout=30
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
                gpg_input = passphrase.encode('utf-8') + b'\n' + encrypted_data
                result = subprocess.run(
                    [
                        "gpg", "--quiet", "--batch", "--no-tty",
                        "--pinentry-mode", "loopback", "--passphrase-fd", "0", "--decrypt"
                    ],
                    input=gpg_input,
                    capture_output=True,
                    check=True,
                    timeout=60,  # 60 second timeout for decryption
                    env={**os.environ, "GPG_TTY": ""}  # Disable TTY usage
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
                    timeout=60,  # 60 second timeout for decryption
                    env={**os.environ, "GPG_TTY": ""}  # Disable TTY usage
                )
            return result.stdout
        except FileNotFoundError:
            raise RuntimeError("GPG command not found. Please install GPG on your system.") from None
        except subprocess.TimeoutExpired:
            raise RuntimeError("GPG decryption timed out after 60 seconds.") from None
        except subprocess.CalledProcessError as e:
            stderr_msg = e.stderr.decode('utf-8', errors='replace') if e.stderr else "Unknown error"

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
