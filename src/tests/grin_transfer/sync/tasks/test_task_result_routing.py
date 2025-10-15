#!/usr/bin/env python3
"""Tests for TaskResult.next_tasks() routing logic."""

import pytest

from grin_transfer.sync.tasks.task_types import CheckResult, TaskAction, TaskType


class TestCheckResultRouting:
    """Test routing logic for CHECK task results."""

    @pytest.mark.parametrize(
        "action,reason,expected_next_tasks",
        [
            (TaskAction.FAILED, "fail_known_conversion_failure", [TaskType.TRACK_CONVERSION_FAILURE]),
            (TaskAction.FAILED, "fail_archive_missing", [TaskType.REQUEST_CONVERSION]),
            (TaskAction.FAILED, "fail_unexpected_http_status_code", []),
            (TaskAction.COMPLETED, None, [TaskType.DOWNLOAD]),
            (TaskAction.SKIPPED, "skip_etag_match", []),
        ],
    )
    def test_check_result_routing(self, action, reason, expected_next_tasks):
        """CheckResult.next_tasks() should route based on action and reason."""
        result = CheckResult(
            barcode="TEST123",
            task_type=TaskType.CHECK,
            action=action,
            reason=reason,
            error="Error" if action == TaskAction.FAILED else None,
            data={"etag": "abc" if action == TaskAction.COMPLETED else None},
        )

        assert result.next_tasks() == expected_next_tasks
