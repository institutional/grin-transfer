"""
Staging Directory Management

Handles disk space monitoring, file management, and cleanup for the staging directory
where downloaded files are temporarily stored before upload.
"""

import asyncio
import logging
import shutil
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class StagingDirectoryError(Exception):
    """Raised when staging directory operations fail."""

    pass


class DiskSpaceError(StagingDirectoryError):
    """Raised when disk space is insufficient."""

    pass


class DirectoryManager:
    """Manages directory for file storage during sync pipeline."""

    def __init__(self, staging_path: str | Path, capacity_threshold: float = 0.9):
        """
        Initialize staging directory manager.

        Args:
            staging_path: Path to staging directory
            capacity_threshold: Disk usage threshold (0.0-1.0) to pause downloads
        """
        self.staging_path = Path(staging_path)
        self.capacity_threshold = capacity_threshold

        self.staging_path.mkdir(parents=True, exist_ok=True)

    def get_encrypted_file_path(self, barcode: str) -> Path:
        """Get path for encrypted archive file."""
        return self.staging_path / f"{barcode}.tar.gz.gpg"

    def get_decrypted_file_path(self, barcode: str) -> Path:
        """Get path for decrypted archive file."""
        return self.staging_path / f"{barcode}.tar.gz"

    def get_extracted_directory_path(self, barcode: str) -> Path:
        """Get path for extracted archive directory."""
        return self.staging_path / f"{barcode}_extracted"

    def get_staging_files(self) -> list[tuple[str, Path, Path]]:
        """
        Get list of files currently in staging directory.

        Returns:
            List of (barcode, encrypted_path, decrypted_path) tuples.
            Paths may not exist if files are partially downloaded.
        """
        files = []

        # Find all encrypted files
        for encrypted_file in self.staging_path.glob("*.tar.gz.gpg"):
            if encrypted_file.name.endswith(".tar.gz.gpg"):
                barcode = encrypted_file.name[:-12]  # Remove .tar.gz.gpg
                decrypted_file = self.get_decrypted_file_path(barcode)
                files.append((barcode, encrypted_file, decrypted_file))

        return files

    async def wait_for_disk_space(self, *wargs, **kwargs):
        # No-op for local directory
        pass

    def get_files_for_cleanup(self, barcode: str) -> list[Path]:
        return []

    def check_disk_space(self):
        return True


class LocalDirectoryManager(DirectoryManager):
    def get_encrypted_file_path(self, barcode: str) -> Path:
        """Get path for encrypted archive file including staging."""
        return self.staging_path / "staging" / f"{barcode}.tar.gz.gpg"

    def get_decrypted_file_path(self, barcode: str) -> Path:
        """Get path for decrypted archive file including "raw" pseudo-bucket."""
        return self.staging_path / "raw" / barcode / f"{barcode}.tar.gz"

    def get_extracted_directory_path(self, barcode: str) -> Path:
        """Get path for extracted archive directory."""
        return self.staging_path / "staging" / f"{barcode}_extracted"

    def get_paths_for_cleanup(self, barcode: str) -> list[Path]:
        encrypted_path = self.get_encrypted_file_path(barcode)
        extracted_dir = self.get_extracted_directory_path(barcode)
        return [encrypted_path, extracted_dir]


class StagingDirectoryManager(DirectoryManager):
    """Assumes the staging directory is an ephemeral resource used during sync operations, and may be safely destroyed"""

    def get_disk_usage(self) -> tuple[int, int, float]:
        """
        Get disk usage information for staging directory.

        Returns:
            Tuple of (used_bytes, total_bytes, usage_ratio)
        """
        usage = shutil.disk_usage(self.staging_path)
        used_bytes = usage.total - usage.free
        total_bytes = usage.total
        usage_ratio = used_bytes / total_bytes if total_bytes > 0 else 0.0

        return used_bytes, total_bytes, usage_ratio

    def check_disk_space(self, required_bytes: int = 0) -> bool:
        """
        Check if there's sufficient disk space for downloads.

        Args:
            required_bytes: Additional bytes needed (for upcoming download)

        Returns:
            True if space is available, False if at capacity
        """
        # Always check disk space for critical operations

        used_bytes, total_bytes, usage_ratio = self.get_disk_usage()

        # Calculate projected usage with required bytes
        projected_used = used_bytes + required_bytes
        projected_ratio = projected_used / total_bytes if total_bytes > 0 else 0.0

        if projected_ratio >= self.capacity_threshold:
            logger.warning(
                f"Disk space limit reached: {usage_ratio:.1%} used "
                f"(threshold: {self.capacity_threshold:.1%}). "
                f"Used: {used_bytes / (1024**3):.1f}GB / {total_bytes / (1024**3):.1f}GB"
            )
            print("⚠️ Disk space limit reached ({usage_ratio:.1%} used) — new downloads paused until space freed")
            return False

        # Log disk usage occasionally
        if usage_ratio > 0.7:  # Log when >70% full
            logger.debug(
                f"Staging disk usage: {usage_ratio:.1%} "
                f"({used_bytes / (1024**3):.1f}GB / {total_bytes / (1024**3):.1f}GB)"
            )

        return True

    async def wait_for_disk_space(self, required_bytes: int = 0, check_interval: int = 30, timeout: int = 600) -> None:
        """
        Wait for disk space to become available before proceeding.

        This is a reusable utility that blocks until sufficient disk space is available,
        respecting the configured capacity threshold.

        Args:
            required_bytes: Additional bytes needed (for upcoming operations)
            check_interval: How often to check disk space in seconds (default: 30)
            timeout: Maximum time to wait in seconds (default: 600 = 10 minutes)

        Raises:
            DiskSpaceError: If timeout is reached without sufficient space
        """
        space_warned = False
        start_time = time.time()

        while not self.check_disk_space(required_bytes):
            if not space_warned:
                used_bytes, total_bytes, usage_ratio = self.get_disk_usage()
                logger.info(
                    f"Waiting for disk space ({usage_ratio:.1%} full, "
                    f"{(total_bytes - used_bytes) / (1024 * 1024 * 1024):.1f} GB available), pausing operations... "
                    f"(threshold: {self.capacity_threshold:.1%})"
                )
                space_warned = True

            # Check timeout
            elapsed = time.time() - start_time
            if elapsed >= timeout:
                used_bytes, total_bytes, usage_ratio = self.get_disk_usage()
                raise DiskSpaceError(
                    f"Timed out waiting for disk space after {timeout} seconds. "
                    f"Current usage: {usage_ratio:.1%} full "
                    f"(threshold: {self.capacity_threshold:.1%})"
                )

            await asyncio.sleep(check_interval)

        if space_warned:
            used_bytes, total_bytes, usage_ratio = self.get_disk_usage()
            logger.info(
                f"Disk space available, resuming operations "
                f"({usage_ratio:.1%} full, {(total_bytes - used_bytes) / (1024 * 1024 * 1024):.1f} GB available)"
            )

    def get_orphaned_files(self) -> list[tuple[str, list[Path]]]:
        """
        Get list of potentially orphaned files in staging directory.

        Returns:
            List of (barcode, file_paths) for files that may be stale
        """
        orphaned = []
        current_time = time.time()

        for barcode, encrypted_path, decrypted_path in self.get_staging_files():
            file_paths = []

            # Check if files are old (>1 hour without modification)
            for path in [encrypted_path, decrypted_path]:
                if path.exists():
                    age = current_time - path.stat().st_mtime
                    if age > 3600:  # 1 hour
                        file_paths.append(path)

            if file_paths:
                orphaned.append((barcode, file_paths))

        return orphaned

    def get_staging_summary(self) -> dict[str, Any]:
        """
        Get summary information about staging directory.

        Returns:
            Dictionary with staging directory statistics
        """
        used_bytes, total_bytes, usage_ratio = self.get_disk_usage()
        staging_files = self.get_staging_files()
        orphaned_files = self.get_orphaned_files()

        # Calculate total staging file size
        staging_size = 0
        for _, encrypted_path, decrypted_path in staging_files:
            for path in [encrypted_path, decrypted_path]:
                if path.exists():
                    staging_size += path.stat().st_size

        return {
            "staging_path": str(self.staging_path),
            "disk_usage_ratio": usage_ratio,
            "disk_used_gb": used_bytes / (1024**3),
            "disk_total_gb": total_bytes / (1024**3),
            "staging_files_count": len(staging_files),
            "staging_size_mb": staging_size / (1024**2),
            "orphaned_files_count": len(orphaned_files),
            "capacity_threshold": self.capacity_threshold,
            "space_available": usage_ratio < self.capacity_threshold,
        }

    def get_paths_for_cleanup(self, barcode: str) -> list[Path]:
        encrypted_path = self.get_encrypted_file_path(barcode)
        decrypted_path = self.get_decrypted_file_path(barcode)
        extracted_dir = self.get_extracted_directory_path(barcode)
        return [encrypted_path, decrypted_path, extracted_dir]
