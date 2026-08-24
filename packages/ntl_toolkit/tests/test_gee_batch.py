from __future__ import annotations

import json
from pathlib import Path

import pytest

from ntl_toolkit.core import gee_batch
from ntl_toolkit.core.gee_batch import (
    GeeBatchExportRequest,
    cancel_gee_batch_export,
    inspect_gee_batch_export,
    submit_gee_batch_export,
)


class _FakeTask:
    id = "task-123"

    def __init__(self) -> None:
        self.started = False

    def start(self) -> None:
        self.started = True

    def status(self) -> dict:
        return {"id": self.id, "state": "READY"}


class _FakeExportImage:
    def __init__(self, task: _FakeTask) -> None:
        self.task = task
        self.calls: list[tuple[str, dict]] = []

    def toDrive(self, **kwargs):
        self.calls.append(("drive", kwargs))
        return self.task

    def toCloudStorage(self, **kwargs):
        self.calls.append(("cloud_storage", kwargs))
        return self.task

    def toAsset(self, **kwargs):
        self.calls.append(("asset", kwargs))
        return self.task


class _FakeData:
    def __init__(self, state: str = "RUNNING") -> None:
        self.state = state
        self.cancelled: list[str] = []

    def getTaskStatus(self, task_id: str) -> list[dict]:
        return [{"id": task_id, "state": self.state}]

    def cancelTask(self, task_id: str) -> None:
        self.cancelled.append(task_id)


class _FakeGeometry:
    @staticmethod
    def Rectangle(bounds: list[float]):
        return "rectangle", tuple(bounds)


class _FakeEe:
    Geometry = _FakeGeometry

    def __init__(self, *, state: str = "RUNNING") -> None:
        self.task = _FakeTask()
        export_image = _FakeExportImage(self.task)
        self.batch = type("Batch", (), {"Export": type("Export", (), {"image": export_image})()})()
        self.data = _FakeData(state)


def _request(tmp_path: Path) -> GeeBatchExportRequest:
    return GeeBatchExportRequest(
        dataset_id="COPERNICUS/S2_SR_HARMONIZED",
        bands=["B4", "B3", "B2"],
        bbox=(120.0, 30.0, 120.5, 30.5),
        manifest_path=str(tmp_path / "export.json"),
        description="Sentinel 2 / test export",
        destination="drive",
        scale=10,
    )


def test_submit_writes_recoverable_manifest_and_safe_export_name(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    fake = _FakeEe()
    monkeypatch.setattr(gee_batch, "_initialize_ee", lambda _project: fake)
    monkeypatch.setattr(gee_batch, "_materialize_image", lambda _ee, _request: "image")

    result = submit_gee_batch_export(_request(tmp_path))

    payload = json.loads((tmp_path / "export.json").read_text(encoding="utf-8"))
    call_kind, call = fake.batch.Export.image.calls[0]
    assert result.status == "succeeded"
    assert result.job_id == "task-123"
    assert fake.task.started is True
    assert call_kind == "drive"
    assert call["description"] == "Sentinel_2_test_export"
    assert payload["schema"] == "ntl.gee.export.v1"
    assert payload["state"] == "READY"
    assert payload["progress_percent"] == 5
    assert payload["history"][-1]["state"] == "READY"


def test_submit_does_not_overwrite_existing_manifest_or_create_remote_task(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    manifest = tmp_path / "export.json"
    manifest.write_text('{"existing": true}', encoding="utf-8")
    monkeypatch.setattr(
        gee_batch,
        "_initialize_ee",
        lambda _project: (_ for _ in ()).throw(AssertionError("must not initialize")),
    )

    result = submit_gee_batch_export(_request(tmp_path))

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "GEE_BATCH_SUBMIT_FAILED"
    assert json.loads(manifest.read_text(encoding="utf-8")) == {"existing": True}


def test_inspect_refreshes_live_state_and_history(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fake = _FakeEe(state="RUNNING")
    manifest = tmp_path / "export.json"
    manifest.write_text(
        json.dumps({"schema": "ntl.gee.export.v1", "task_id": "task-123", "state": "READY"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(gee_batch, "_initialize_ee", lambda _project: fake)

    result = inspect_gee_batch_export(str(manifest))

    payload = json.loads(manifest.read_text(encoding="utf-8"))
    assert result.status == "succeeded"
    assert result.metrics["state"] == "RUNNING"
    assert result.metrics["progress_percent"] == 50
    assert payload["history"][-1]["state"] == "RUNNING"


def test_cancel_requests_non_terminal_task(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fake = _FakeEe(state="RUNNING")
    manifest = tmp_path / "export.json"
    manifest.write_text(
        json.dumps({"schema": "ntl.gee.export.v1", "task_id": "task-123", "state": "RUNNING"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(gee_batch, "_initialize_ee", lambda _project: fake)

    result = cancel_gee_batch_export(str(manifest))

    assert result.status == "succeeded"
    assert result.metrics["state"] == "CANCEL_REQUESTED"
    assert fake.data.cancelled == ["task-123"]


def test_cancel_does_not_touch_completed_task(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fake = _FakeEe(state="COMPLETED")
    manifest = tmp_path / "export.json"
    manifest.write_text(
        json.dumps({"schema": "ntl.gee.export.v1", "task_id": "task-123", "state": "COMPLETED"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(gee_batch, "_initialize_ee", lambda _project: fake)

    result = cancel_gee_batch_export(str(manifest))

    assert result.status == "succeeded"
    assert result.metrics["terminal"] is True
    assert fake.data.cancelled == []
    assert result.warnings


def test_invalid_manifest_fails_without_initializing_earth_engine(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    manifest = tmp_path / "invalid.json"
    manifest.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        gee_batch,
        "_initialize_ee",
        lambda _project: (_ for _ in ()).throw(AssertionError("must not initialize")),
    )

    result = inspect_gee_batch_export(str(manifest))

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "GEE_BATCH_STATUS_FAILED"
