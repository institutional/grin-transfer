"""
Docker-specific utilities for grin-to-s3.

This module contains Docker environment detection and validation logic.
"""

from .validation import translate_docker_data_path, validate_docker_storage_path

__all__ = ["translate_docker_data_path", "validate_docker_storage_path"]
