#!/usr/bin/env python3
"""
Configuration management for book collection functionality.

Provides centralized configuration for pagination, rate limiting, and other
collection parameters that can be overridden via CLI arguments or config files.
"""

import logging
from dataclasses import asdict, dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class ExportConfig:
    """Main configuration for book collection operations."""

    # Core settings
    library_directory: str
    rate_limit: float = 5.0  # API requests per second

    # SQLite database settings
    sqlite_db_path: str = "output/default/books.db"

    @classmethod
    def from_dict(cls, config_dict: dict[str, Any]) -> "ExportConfig":
        """Create config from dictionary."""
        return cls(**config_dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert config to dictionary."""
        result = asdict(self)
        return result
