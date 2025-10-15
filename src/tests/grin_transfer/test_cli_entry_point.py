"""
Smoke tests for the main CLI entry point.
"""

from unittest.mock import AsyncMock, patch

import pytest


class TestCLIEntryPoint:
    """Test main CLI entry point functionality."""

    @pytest.mark.asyncio
    async def test_help_command_shows_help(self, capsys):
        """grin --help should display help text without error."""
        import sys

        # Mock sys.argv to simulate --help command
        with patch.object(sys, "argv", ["grin", "--help"]):
            from grin import main

            assert await main() == 0
            captured = capsys.readouterr()
            assert "GRIN-to-S3" in captured.out
            assert "Available commands" in captured.out or "Commands:" in captured.out

    @pytest.mark.asyncio
    async def test_no_command_shows_help(self, capsys):
        """grin with no command should display help."""
        import sys

        with patch.object(sys, "argv", ["grin"]):
            from grin import main

            assert await main() == 1
            captured = capsys.readouterr()
            assert "GRIN-to-S3" in captured.out or "usage:" in captured.out

    @pytest.mark.asyncio
    async def test_unknown_command_shows_error(self, capsys):
        """grin with unknown command should show error."""
        import sys

        with patch.object(sys, "argv", ["grin", "unknown-command"]):
            from grin import main

            assert await main() == 1
            captured = capsys.readouterr()
            assert "Unknown command" in captured.out or "usage:" in captured.out

    @pytest.mark.asyncio
    async def test_auth_command_routes_correctly(self):
        """grin auth should route to auth module."""
        import sys

        with patch.object(sys, "argv", ["grin", "auth", "--help"]):
            # Mock the auth_main function to avoid actual execution
            with patch("grin.auth_main") as mock_auth:
                from grin import main

                await main()

                # Verify auth_main was called
                mock_auth.assert_called_once()

    @pytest.mark.asyncio
    async def test_collect_command_routes_correctly(self):
        """grin collect should route to collect module."""
        import sys

        with patch.object(sys, "argv", ["grin", "collect", "--help"]):
            # Mock the collect_main function
            with patch("grin.collect_main", new_callable=AsyncMock) as mock_collect:
                mock_collect.return_value = 0

                from grin import main

                await main()

                # Verify collect_main was called
                mock_collect.assert_called_once()

    @pytest.mark.asyncio
    async def test_sync_command_routes_correctly(self):
        """grin sync should route to sync module."""
        import sys

        with patch.object(sys, "argv", ["grin", "sync", "--help"]):
            with patch("grin.sync_main", new_callable=AsyncMock) as mock_sync:
                mock_sync.return_value = 0

                from grin import main

                await main()

                mock_sync.assert_called_once()

    @pytest.mark.asyncio
    async def test_storage_command_routes_correctly(self):
        """grin storage should route to storage module."""
        import sys

        with patch.object(sys, "argv", ["grin", "storage", "--help"]):
            with patch("grin.storage_main", new_callable=AsyncMock) as mock_storage:
                mock_storage.return_value = 0

                from grin import main

                await main()

                mock_storage.assert_called_once()

    @pytest.mark.asyncio
    async def test_session_lock_prevents_concurrent_runs(self, tmp_path):
        """Session lock should prevent concurrent runs for same run-name."""
        import sys

        from grin_transfer.common import SessionLock

        run_name = "test_run"
        lock_path = tmp_path / run_name / "session.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)

        # Acquire lock manually to simulate another process
        lock = SessionLock(lock_path)
        lock.acquire()

        try:
            with patch.object(sys, "argv", ["grin", "collect", "--run-name", run_name]):
                # Mock OUTPUT_DIR to use tmp_path
                with patch("grin.OUTPUT_DIR", tmp_path):
                    from grin import main

                    assert await main() == 1
        finally:
            lock.release()

    @pytest.mark.asyncio
    async def test_session_lock_not_applied_for_dry_run(self):
        """Session lock should not be applied for dry-run operations."""
        import sys

        with patch.object(sys, "argv", ["grin", "sync", "--dry-run", "--run-name", "test"]):
            # Mock sync_main to avoid actual execution
            with patch("grin.sync_main", new_callable=AsyncMock) as mock_sync:
                mock_sync.return_value = 0

                from grin import main

                assert await main() == 0
                mock_sync.assert_called_once()
