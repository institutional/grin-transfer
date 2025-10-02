"""Tests for task manager statistics tracking."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from grin_to_s3.sync.task_manager import TaskManager
from grin_to_s3.sync.tasks.task_types import TaskAction, TaskResult, TaskType


@pytest.mark.asyncio
class TestTaskManagerStatistics:
    """Test task manager statistics tracking for special cases."""

    @pytest.mark.parametrize(
        "reason,expected_needs_conversion,expected_failed",
        [
            ("fail_archive_missing", 1, 0),
            ("fail_known_conversion_failure", 1, 0),
            ("fail_unexpected_http_status_code", 0, 1),
        ],
    )
    async def test_check_failure_statistics(self, reason, expected_needs_conversion, expected_failed):
        """CHECK FAILED tasks should count as needs_conversion or failed based on reason."""
        manager = TaskManager({TaskType.CHECK: 10})

        async def check_task():
            return TaskResult(
                barcode="TEST",
                task_type=TaskType.CHECK,
                action=TaskAction.FAILED,
                reason=reason,
                error="Archive not available in GRIN",
            )

        mock_pipeline = MagicMock()
        mock_pipeline.book_record_updates = {}

        with patch(
            "grin_to_s3.sync.task_manager.get_updates_for_task",
            new_callable=AsyncMock,
            return_value={},
        ):
            result = await manager.run_task(
                TaskType.CHECK,
                "TEST",
                check_task,
                mock_pipeline,
                {},
            )

        assert result.action == TaskAction.FAILED
        assert result.reason == reason
        assert manager.stats[TaskType.CHECK]["started"] == 1
        assert manager.stats[TaskType.CHECK]["needs_conversion"] == expected_needs_conversion
        assert manager.stats[TaskType.CHECK]["failed"] == expected_failed
