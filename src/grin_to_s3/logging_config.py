import logging
import os
from datetime import datetime
from pathlib import Path

from grin_to_s3.docker import is_docker_environment


def setup_logging(level: str = "INFO", log_file: Path | None = None, append: bool = True) -> None:
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
        log_file = logs_dir / f"grin_pipeline_{timestamp}.log"
    else:
        # Ensure parent directory exists for custom log file
        log_file.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(str(log_file), mode="a" if append else "w")
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # Force immediate flushing for file handler
    def make_flush_func(h):
        return lambda: h.stream.flush() if hasattr(h, "stream") else None

    # Replace flush method - type ignore for dynamic assignment
    file_handler.flush = make_flush_func(file_handler)  # type: ignore[method-assign]

    # Show user-friendly path (host-relative for Docker)
    display_path = str(log_file)
    if is_docker_environment() and str(log_file).startswith("/app/logs/"):
        # Convert container path to host path for Docker users
        display_path = str(log_file).replace("/app/logs/", "docker-data/logs/")

    print(f"Logging to file: {display_path}\n")
    logger = logging.getLogger(__name__)
    logger.info(f"Logging to file: {log_file}")
    logger.info(f"Logging initialized at {level} level")
