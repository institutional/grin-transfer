from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, Literal, Protocol, TypedDict, TypeVar

if TYPE_CHECKING:
    from grin_to_s3.sync.pipeline import SyncPipeline

class TaskType(Enum):
    """Types of tasks in the sync pipeline."""

    # Preflight operations
    DATABASE_BACKUP = auto()
    DATABASE_UPLOAD = auto()

    # Per-barcode tasks
    CHECK = auto()
    REQUEST_CONVERSION = auto()
    DOWNLOAD = auto()
    DECRYPT = auto()
    UNPACK = auto()
    UPLOAD = auto()
    EXTRACT_MARC = auto()
    EXTRACT_OCR = auto()
    EXPORT_CSV = auto()
    CLEANUP = auto()

    # Teardown operations
    FINAL_DATABASE_UPLOAD = auto()
    STAGING_CLEANUP = auto()


class TaskAction(Enum):
    """What action was taken by a task."""

    COMPLETED = "completed"
    SKIPPED = "skipped"
    FAILED = "failed"


# Type-specific data structures for each task
class CheckData(TypedDict):
    """Data from CHECK task."""

    etag: str | None
    file_size_bytes: int | None
    http_status_code: int | None


class ETagMatchResult(TypedDict):
    """Whether an Etag matched stored etag metadata, and why the determination was made"""

    matched: bool
    reason: Literal["etag_match", "etag_mismatch", "no_archive", "no_etag"]


class DownloadData(TypedDict):
    """Data from DOWNLOAD task."""

    file_path: Path
    etag: str | None
    file_size_bytes: int | None
    http_status_code: int


class DecryptData(TypedDict):
    """Data from DECRYPT task."""

    decrypted_path: Path
    original_path: Path


class UnpackData(TypedDict):
    """Data from UNPACK task."""

    unpacked_path: Path


class UploadData(TypedDict):
    """Data from UPLOAD task."""

    upload_path: Path


class ExtractMarcData(TypedDict):
    """Data from EXTRACT_MARC task."""

    marc_metadata: dict


class ExtractOcrData(TypedDict):
    """Data from EXTRACT_OCR task."""

    json_file_path: Path
    page_count: int


class ExportCsvData(TypedDict):
    """Data from EXPORT_CSV task."""

    csv_file_path: Path
    record_count: int


class RequestConversionData(TypedDict):
    """Data from REQUEST_CONVERSION task."""

    conversion_status: Literal["requested", "in_process", "unavailable", "limit_reached"]
    request_count: int


class CleanupData(TypedDict):
    """Data from CLEANUP task."""

    pass


class ArchiveMetadata(TypedDict):
    barcode: str
    acquisition_date: str
    encrypted_etag: str
    original_filename: str


class ArchiveOcrMetadata(TypedDict):
    barcode: str
    acquisition_date: str
    page_count: str


# Preflight task data types
class DatabaseBackupData(TypedDict):
    """Data from DATABASE_BACKUP task."""

    backup_filename: str | None
    file_size: int
    backup_time: float


class DatabaseUploadData(TypedDict):
    """Data from DATABASE_UPLOAD task."""

    backup_filename: str | None
    file_size: int
    compressed_size: int
    backup_time: float


# Teardown task data types
class FinalDatabaseUploadData(TypedDict):
    """Data from FINAL_DATABASE_UPLOAD task."""

    backup_filename: str | None
    file_size: int
    compressed_size: int
    backup_time: float


class StagingCleanupData(TypedDict):
    """Data from STAGING_CLEANUP task."""

    staging_path: str
    cleanup_time: float


# Generic type for task result data
TData = TypeVar("TData")

SKIP_REASONS = Literal[
    "completed_match_with_force",
    "fail_archive_missing",
    "fail_no_marc_metadata",
    "fail_unexpected_http_status_code",
    "skip_already_in_process",
    "skip_already_in_progress",
    "skip_archive_missing_from_grin",
    "skip_conversion_limit_reached",
    "skip_conversion_requested",
    "skip_database_backup_flag",
    "skip_etag_match",
    "skip_verified_unavailable",
]


@dataclass
class Result(Generic[TData]):
    """Base result class for operations with typed data (no barcode)."""

    task_type: TaskType
    action: TaskAction
    error: str | None = None
    data: TData | None = None
    reason: SKIP_REASONS | None = None  # For skipped operations

    @property
    def success(self) -> bool:
        """Operation succeeded if it completed or was intentionally skipped."""
        return self.action in (TaskAction.COMPLETED, TaskAction.SKIPPED)


@dataclass
class TaskResult(Generic[TData]):
    """Result from any task with typed data and barcode."""

    barcode: str
    task_type: TaskType
    action: TaskAction
    error: str | None = None
    data: TData | None = None
    reason: SKIP_REASONS | None = None  # For skipped tasks

    @property
    def success(self) -> bool:
        """Task succeeded if it completed or was intentionally skipped."""
        return self.action in (TaskAction.COMPLETED, TaskAction.SKIPPED)

    @property
    def should_continue_pipeline(self) -> bool:
        """Whether to continue processing dependent tasks."""
        return self.action == TaskAction.COMPLETED

    def next_tasks(self) -> list[TaskType]:
        """Return tasks that should run after this one."""

        # Handle recovery paths for failed tasks
        if self.action == TaskAction.FAILED:
            match self.task_type:
                case TaskType.CHECK if self.reason == "fail_archive_missing":
                    return [TaskType.REQUEST_CONVERSION]
                case _:
                    return []

        # Normal successful task flow
        if not self.should_continue_pipeline:
            return []

        # Dependency chain
        match self.task_type:
            case TaskType.CHECK:
                return [TaskType.DOWNLOAD]
            case TaskType.DOWNLOAD:
                return [TaskType.DECRYPT]
            case TaskType.DECRYPT:
                return [TaskType.UPLOAD, TaskType.UNPACK]
            case TaskType.UNPACK:
                return [TaskType.EXTRACT_MARC, TaskType.EXTRACT_OCR, TaskType.EXPORT_CSV]
            case TaskType.UPLOAD:
                return []  # CLEANUP is handled specially after all tasks complete
            case TaskType.REQUEST_CONVERSION:
                return []  # Conversion request never continues
            case _:
                return []


# Type aliases for specific task results
CheckResult = TaskResult[CheckData]
RequestConversionResult = TaskResult[RequestConversionData]
DownloadResult = TaskResult[DownloadData]
DecryptResult = TaskResult[DecryptData]
UnpackResult = TaskResult[UnpackData]
UploadResult = TaskResult[UploadData]
ExtractMarcResult = TaskResult[ExtractMarcData]
ExtractOcrResult = TaskResult[ExtractOcrData]
ExportCsvResult = TaskResult[ExportCsvData]
CleanupResult = TaskResult[CleanupData]

# Preflight operation results (no barcode)
DatabaseBackupResult = Result[DatabaseBackupData]
DatabaseUploadResult = Result[DatabaseUploadData]

# Teardown operation results (no barcode)
FinalDatabaseUploadResult = Result[FinalDatabaseUploadData]
StagingCleanupResult = Result[StagingCleanupData]


# Task function protocols for type safety
class CheckTaskFunc(Protocol):
    async def __call__(self, barcode: str, pipeline: SyncPipeline) -> CheckResult: ...


class RequestConversionTaskFunc(Protocol):
    async def __call__(self, barcode: str, pipeline: SyncPipeline) -> RequestConversionResult: ...


class DownloadTaskFunc(Protocol):
    async def __call__(self, barcode: str, pipeline: SyncPipeline) -> DownloadResult: ...


class DecryptTaskFunc(Protocol):
    async def __call__(self, barcode: str, download_data: DownloadData, pipeline: SyncPipeline) -> DecryptResult: ...


class UnpackTaskFunc(Protocol):
    async def __call__(self, barcode: str, decrypt_data: DecryptData, pipeline: SyncPipeline) -> UnpackResult: ...


class UploadTaskFunc(Protocol):
    async def __call__(
        self, barcode: str, download_data: DownloadData, decrypt_data: DecryptData, pipeline: SyncPipeline
    ) -> UploadResult: ...


class ExtractMarcTaskFunc(Protocol):
    async def __call__(self, barcode: str, unpack_data: UnpackData, pipeline: SyncPipeline) -> ExtractMarcResult: ...


class ExtractOcrTaskFunc(Protocol):
    async def __call__(self, barcode: str, unpack_data: UnpackData, pipeline: SyncPipeline) -> ExtractOcrResult: ...


class ExportCsvTaskFunc(Protocol):
    async def __call__(self, barcode: str, pipeline: SyncPipeline) -> ExportCsvResult: ...


class CleanupTaskFunc(Protocol):
    async def __call__(
        self, barcode: str, pipeline: SyncPipeline, all_results: dict[TaskType, TaskResult[Any]]
    ) -> CleanupResult: ...
