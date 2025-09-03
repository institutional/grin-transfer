#!/usr/bin/env python3
"""
Configuration management for book collection functionality.

Provides centralized configuration for pagination, rate limiting, and other
collection parameters that can be overridden via CLI arguments or config files.
"""

import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path
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
    sqlite_cleanup_sessions: int = 5  # Keep only N most recent sessions

    # Performance settings
    burst_limit: int = 10  # Rate limiter burst capacity
    processing_chunk_size: int = 1000  # Records to process before progress save

    # Memory management (now minimal with SQLite)
    recent_cache_size: int = 1000  # Small cache for recent items (performance optimization)
    recent_failed_cache_size: int = 100  # Small cache for recent failures

    # Estimation settings
    estimation_threshold: int = 100000  # When to start estimating total
    estimation_extrapolation: int = 100000  # Extra books to estimate

    @classmethod
    def from_dict(cls, config_dict: dict[str, Any]) -> "ExportConfig":
        """Create config from dictionary."""
        return cls(**config_dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert config to dictionary."""
        result = asdict(self)
        return result

    def save_to_file(self, config_path: Path) -> None:
        """Save configuration to JSON file."""
        config_path.parent.mkdir(parents=True, exist_ok=True)

        with open(config_path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

        logger.info(f"Saved configuration to {config_path}")

    @classmethod
    def load_from_file(cls, config_path: Path) -> "ExportConfig":
        """Load configuration from JSON file."""
        if not config_path.exists():
            logger.warning(f"Config file {config_path} not found, using defaults")
            return cls(library_directory="REQUIRED")

        try:
            with open(config_path) as f:
                config_dict = json.load(f)

            logger.info(f"Loaded configuration from {config_path}")
            return cls.from_dict(config_dict)

        except (json.JSONDecodeError, TypeError, ValueError) as e:
            logger.error(f"Failed to load config from {config_path}: {e}")
            logger.warning("Using default configuration")
            return cls(library_directory="REQUIRED")

    def update_from_args(self, **kwargs) -> None:
        """Update configuration from CLI arguments."""
        for key, value in kwargs.items():
            if value is not None:
                if hasattr(self, key):
                    setattr(self, key, value)
                    logger.debug(f"Updated {key} = {value}")


class ConfigManager:
    """Manages configuration loading and CLI argument integration."""

    DEFAULT_CONFIG_PATHS = [
        Path("collect_books_config.json"),
        Path("config/collect_books.json"),
        Path.home() / ".grin-to-s3" / "collect_books.json",
    ]

    @classmethod
    def load_config(cls, config_file: str | None = None, **cli_overrides) -> ExportConfig:
        """
        Load configuration with CLI argument overrides.

        Args:
            config_file: Explicit config file path
            **cli_overrides: CLI arguments to override config values

        Returns:
            Merged configuration
        """
        # Determine config file to use
        if config_file:
            config_path = Path(config_file)
            config = ExportConfig.load_from_file(config_path)
        else:
            # Try default locations
            config = None
            for path in cls.DEFAULT_CONFIG_PATHS:
                if path.exists():
                    config = ExportConfig.load_from_file(path)
                    break

            if config is None:
                logger.info("No config file found, using defaults")
                # NOTE: library_directory must be provided via CLI args (required parameter)
                config = ExportConfig(library_directory="REQUIRED")

        # Apply CLI overrides
        if config is not None:
            config.update_from_args(**cli_overrides)

        return config or ExportConfig(library_directory="REQUIRED")

    @classmethod
    def create_default_config(cls, output_path: Path) -> ExportConfig:
        """Create and save a default configuration file."""
        if output_path is None:
            output_path = cls.DEFAULT_CONFIG_PATHS[0]

        # NOTE: When creating default config, library_directory should be updated by user
        config = ExportConfig(library_directory="CHANGE_ME")
        config.save_to_file(output_path)

        return config


def get_default_config() -> ExportConfig:
    """Get default configuration for book collection."""
    # NOTE: library_directory must be provided via CLI args (required parameter)
    return ExportConfig(library_directory="REQUIRED")


if __name__ == "__main__":
    # CLI tool to create default config
    import argparse

    parser = argparse.ArgumentParser(description="Manage book collection configuration")
    parser.add_argument("--create-default", type=str, help="Create default config file at specified path")
    parser.add_argument("--show-default", action="store_true", help="Show default configuration")

    args = parser.parse_args()

    if args.create_default:
        config = ConfigManager.create_default_config(Path(args.create_default))
        print(f"Created default configuration at {args.create_default}")
    elif args.show_default:
        config = get_default_config()
        print(json.dumps(config.to_dict(), indent=2))
    else:
        parser.print_help()
