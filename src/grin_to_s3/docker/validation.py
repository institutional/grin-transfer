"""
Docker environment validation for storage paths.

This module provides validation logic specific to Docker environments,
ensuring storage paths are writable and persist to the host.
"""

import os
from pathlib import Path


def is_docker_environment() -> bool:
    """Check if we're running inside a Docker container."""
    return os.path.exists("/.dockerenv") or os.environ.get("DOCKER_ENV") == "true"


def translate_docker_data_path_for_local_storage(original_path: str) -> str:
    """Translate docker-data relative paths to container paths for local storage.

    Args:
        original_path: The original path string from user

    Returns:
        Translated path for container use, or original path if no translation needed
    """
    # Handle docker-data relative paths from host perspective
    if original_path.startswith("docker-data/"):
        # Map docker-data subdirectories to their /app equivalents
        docker_data_mappings = {
            "docker-data/data/": "/app/data/",
            "docker-data/output/": "/app/output/",
            "docker-data/logs/": "/app/logs/",
            "docker-data/staging/": "/app/staging/",
        }

        # Check for specific subdirectory mappings first (longest prefix first)
        for host_prefix, container_prefix in sorted(docker_data_mappings.items(), key=len, reverse=True):
            if original_path.startswith(host_prefix):
                # Replace the host path with container path
                return original_path.replace(host_prefix, container_prefix, 1)

        # For general docker-data/foo paths, map to /app/docker-data/foo
        # This requires mounting ./docker-data:/app/docker-data in docker-compose.yml
        return original_path.replace("docker-data/", "/app/docker-data/", 1)

    return original_path


def process_local_storage_path(base_path_str: str) -> Path:
    """Process and validate a local storage path for Docker environments.

    Args:
        base_path_str: The original path string from user

    Returns:
        Fully processed and validated Path object

    Raises:
        ValueError: If path is not accessible in Docker environment
    """
    if is_docker_environment():
        translated_path = translate_docker_data_path_for_local_storage(base_path_str)
        # Always expand paths (handles ~, relative paths, etc.)
        base_path = Path(translated_path).expanduser().resolve()
        # Validate path accessibility for Docker local storage
        validate_docker_local_storage_path(base_path)
        return base_path
    else:
        # Always expand paths (handles ~, relative paths, etc.)
        return Path(base_path_str).expanduser().resolve()


def validate_docker_local_storage_path(expanded_path: Path) -> None:
    """Validate that local storage path is mounted and will persist to host.

    Args:
        expanded_path: The fully expanded and resolved path

    Raises:
        ValueError: If path is not mounted and won't persist to host
    """
    expanded_str = str(expanded_path)

    # Only allow paths that are specifically mounted volumes (persist to host)
    mounted_prefixes = [
        "/app/data/",  # Mounted to ./docker-data/data
        "/app/output/",  # Mounted to ./docker-data/output
        "/app/logs/",  # Mounted to ./docker-data/logs
        "/app/staging/",  # Mounted to ./docker-data/staging
        "/app/docker-data/",  # Mounted to ./docker-data (full access)
        "/app/custom/",  # Custom mount (see docker-compose.yml)
    ]

    is_mounted = any(expanded_str.startswith(prefix) for prefix in mounted_prefixes)

    if not is_mounted:
        print("❌ ERROR: This base path is not mounted in Docker:")
        print(f"   Path: {expanded_path}")
        print()
        print("💡 Use a docker-data directory (recommended):")
        print("   --storage-path docker-data/storage")
        print()
        print("💡 Or add a custom mount in docker-compose.yml:")
        print("   volumes:")
        print("     - /your/host/path:/app/custom")
        print("   Then use: --storage-path /app/custom")
        print()
        raise ValueError(f"Storage path not mounted: {expanded_path}")

    # Test that the mounted path is actually writable
    try:
        expanded_path.mkdir(parents=True, exist_ok=True)
        test_file = expanded_path / ".write_test"
        test_file.write_text("test")
        test_file.unlink()  # Clean up
    except (PermissionError, OSError) as e:
        print("❌ ERROR: Cannot write to mounted storage directory")
        print(f"   Path: {expanded_path}")
        print(f"   Error: {e}")
        print()
        raise ValueError(f"Storage path not writable: {expanded_path}") from e
