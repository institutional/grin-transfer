"""Integration tests for multipart upload coordination and timeouts."""

import asyncio
from dataclasses import dataclass, field
from typing import Any

import pytest

from grin_to_s3.storage.base import BackendConfig, Storage, UploadPartTimeoutError


@dataclass
class FakeMultipartClient:
    """Lightweight stub that mimics the async S3 multipart API used by Storage."""

    delays: dict[int, float] = field(default_factory=dict)
    failures: dict[int, Exception] = field(default_factory=dict)
    upload_id: str = "fake-upload-id"
    uploaded_parts: list[int] = field(default_factory=list)
    cancelled_parts: set[int] = field(default_factory=set)
    completed_parts: list[dict[str, Any]] | None = None
    abort_called: bool = False
    complete_called: bool = False

    async def create_multipart_upload(self, **kwargs: Any) -> dict[str, str]:
        return {"UploadId": self.upload_id}

    async def upload_part(
        self, *, Bucket: str, Key: str, PartNumber: int, UploadId: str, Body: bytes
    ) -> dict[str, str]:
        if UploadId != self.upload_id:
            raise AssertionError("Unexpected upload id passed to multipart client")

        delay = self.delays.get(PartNumber, 0.0)
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            # Record which parts were cancelled so tests can assert cleanup happened
            self.cancelled_parts.add(PartNumber)
            raise

        failure = self.failures.get(PartNumber)
        if failure:
            raise failure

        self.uploaded_parts.append(PartNumber)
        return {"ETag": f"etag-{PartNumber}"}

    async def complete_multipart_upload(self, **kwargs: Any) -> None:
        self.complete_called = True
        self.completed_parts = kwargs["MultipartUpload"]["Parts"]

    async def abort_multipart_upload(self, **kwargs: Any) -> None:
        self.abort_called = True


@pytest.mark.asyncio
async def test_multipart_upload_timeout_triggers_abort(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """Slow parts should timeout, raise UploadPartTimeoutError, and abort the upload."""

    storage = Storage(BackendConfig(protocol="s3"))

    # Keep part sizes predictable and shrink timeout so the test finishes fast.
    monkeypatch.setattr(Storage, "_calculate_part_size", lambda self, _: 1024)
    monkeypatch.setattr(Storage, "_calculate_upload_timeout", lambda self, __: 0.01)

    file_path = tmp_path / "timeout.bin"
    file_path.write_bytes(b"x" * 2048)  # Two parts when chunk size is 1024 bytes

    client = FakeMultipartClient(delays={1: 0.05})  # First part sleeps longer than our timeout

    with pytest.raises(UploadPartTimeoutError) as exc_info:
        await storage._multipart_upload_from_file(client, "bucket", "key", str(file_path))

    assert exc_info.value.part_number == 1
    assert client.abort_called is True
    assert client.complete_called is False
    assert client.cancelled_parts == {1}
    assert 1 not in client.uploaded_parts  # Timed-out part never completed


@pytest.mark.asyncio
async def test_multipart_upload_completes_out_of_order_parts(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """Successful uploads complete even when parts finish out of order."""

    storage = Storage(BackendConfig(protocol="s3"))

    monkeypatch.setattr(Storage, "_calculate_part_size", lambda self, _: 4)
    monkeypatch.setattr(Storage, "_calculate_upload_timeout", lambda self, __: 1)

    file_path = tmp_path / "success.bin"
    file_path.write_bytes(b"abcdefghij")  # 10 bytes -> 3 parts with 4-byte chunks

    # Delay part 1 long enough that parts 2 and 3 finish first to exercise sorting logic
    client = FakeMultipartClient(delays={1: 0.05, 2: 0.01, 3: 0.02})

    await storage._multipart_upload_from_file(client, "bucket", "key", str(file_path))

    assert client.abort_called is False
    assert client.complete_called is True
    assert set(client.uploaded_parts) == {1, 2, 3}
    assert client.completed_parts == [
        {"ETag": "etag-1", "PartNumber": 1},
        {"ETag": "etag-2", "PartNumber": 2},
        {"ETag": "etag-3", "PartNumber": 3},
    ]


@pytest.mark.asyncio
async def test_multipart_upload_producer_failure_aborts(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """Producer read errors should abort the upload without hanging queued tasks."""

    storage = Storage(BackendConfig(protocol="s3"))

    # Force predictable chunk math; actual value does not matter because the read will fail early.
    monkeypatch.setattr(Storage, "_calculate_part_size", lambda self, _: 1024)
    monkeypatch.setattr(Storage, "_calculate_upload_timeout", lambda self, __: 1)

    # Pretend the file exists so the path normalisation logic behaves normally.
    file_path = tmp_path / "producer.bin"
    file_path.write_bytes(b"irrelevant")

    class FailingReader:
        async def __aenter__(self):
            raise RuntimeError("simulated read failure")

        async def __aexit__(self, exc_type, exc, tb):
            return False

    def fake_open(*args, **kwargs):
        return FailingReader()

    monkeypatch.setattr("grin_to_s3.storage.base.aiofiles.open", fake_open)

    client = FakeMultipartClient()

    with pytest.raises(RuntimeError, match="simulated read failure"):
        await storage._multipart_upload_from_file(client, "bucket", "key", str(file_path))

    assert client.abort_called is True
    assert client.complete_called is False
    assert client.uploaded_parts == []
