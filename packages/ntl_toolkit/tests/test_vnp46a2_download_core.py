from __future__ import annotations

import csv
import json
from dataclasses import replace
from pathlib import Path

import pytest

from ntl_toolkit.core import vnp46a2_download
from ntl_toolkit.core.vnp46a2_download import (
    PhaseOutcome,
    Vnp46a2DownloadRequest,
    inspect_vnp46a2_run,
    run_vnp46a2_download,
)


def _request(tmp_path: Path, **overrides: object) -> Vnp46a2DownloadRequest:
    request = Vnp46a2DownloadRequest(
        start_date="2026-02-13",
        end_date="2026-02-14",
        countries=["ISR"],
        output_root=str(tmp_path / "runs"),
        phase="full",
        execution_mode="run",
        no_gee_latest=True,
    )
    return replace(request, **overrides)


def _write_audit(run_root: Path, statuses: list[str]) -> None:
    run_root.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "iso3": "ISR",
            "date": f"2026-02-{13 + index:02d}",
            "audit_status": status,
            "mosaic_file": str(run_root / f"{index}.tif"),
        }
        for index, status in enumerate(statuses)
    ]
    with (run_root / "vnp46a2_country_day_coverage_audit.csv").open(
        "w", newline="", encoding="utf-8"
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    (run_root / "vnp46a2_country_day_coverage_audit_summary.json").write_text(
        json.dumps({"status_counts": {status: statuses.count(status) for status in set(statuses)}}),
        encoding="utf-8",
    )


def test_vnp_full_mode_runs_all_phases_and_reports_progress(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    messages: list[str] = []
    request = _request(tmp_path)
    run_root = request.run_root
    _write_audit(run_root, ["mosaic_valid", "no_granules"])

    def fake_run_phase(
        phase: str,
        _command: list[str],
        _cwd: Path,
        _env: dict[str, str],
        on_line,
    ) -> PhaseOutcome:
        on_line(f"{phase}: Bearer no-leak")
        return PhaseOutcome(phase=phase, returncode=0, stdout=f"{phase} complete", stderr="")

    monkeypatch.setattr(vnp46a2_download, "_run_phase", fake_run_phase)
    monkeypatch.setenv("EARTHDATA_TOKEN", "test-token")

    result = run_vnp46a2_download(
        request,
        progress=lambda _current, _total, message: messages.append(message),
    )

    assert result.status == "succeeded"
    assert [message for message in messages if message in {"prepare", "download", "mosaic", "audit", "completed"}] == [
        "prepare",
        "download",
        "mosaic",
        "audit",
        "completed",
    ]
    assert "no-leak" not in messages
    assert messages.count("prepare: Bearer <REDACTED>") == 1
    phase_manifest = json.loads((run_root / "ntl_download_phase_download.json").read_text(encoding="utf-8"))
    assert "test-token" not in json.dumps(phase_manifest)
    runtime = json.loads((run_root / "ntl_download_runtime.json").read_text(encoding="utf-8"))
    assert runtime["status"] == "completed"
    assert runtime["completed_phases"] == ["prepare", "download", "mosaic", "audit"]


def test_plan_mode_preserves_retry_targets_and_never_spawns(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    request = _request(
        tmp_path,
        phase="download",
        execution_mode="plan",
        targets=["ISR:2026-02-14"],
        workers=3,
        download_timeout=720,
    )
    monkeypatch.setattr(
        vnp46a2_download,
        "_run_phase",
        lambda *_args, **_kwargs: pytest.fail("plan mode must not execute a subprocess"),
    )

    result = run_vnp46a2_download(request)

    assert result.status == "succeeded"
    command = result.metrics["commands"]["download"]
    assert "--targets" in command
    assert "ISR:2026-02-14" in command
    assert command[command.index("--workers") + 1] == "3"
    assert command[command.index("--download-timeout") + 1] == "720"


def test_run_requires_environment_token_only_for_download(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MISSING_TOKEN", raising=False)

    result = run_vnp46a2_download(_request(tmp_path, token_env="MISSING_TOKEN"))

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "EARTHDATA_TOKEN_MISSING"


def test_inspect_vnp_run_exposes_exact_retry_and_mosaic_targets(tmp_path: Path) -> None:
    _write_audit(tmp_path, ["mosaic_valid", "retry_download", "downloaded_without_mosaic", "no_granules"])

    result = inspect_vnp46a2_run(tmp_path)

    assert result.status == "failed"
    assert result.metrics["retry_targets"] == ["ISR:2026-02-14"]
    assert result.metrics["pending_mosaic_targets"] == ["ISR:2026-02-15"]
    assert result.error is not None
    assert result.error.code == "VNP46A2_AUDIT_INCOMPLETE"


def test_inspect_vnp_treats_no_granules_and_all_nodata_as_terminal(tmp_path: Path) -> None:
    _write_audit(tmp_path, ["no_granules", "mosaic_all_nodata"])

    result = inspect_vnp46a2_run(tmp_path)

    assert result.status == "succeeded"
    assert result.metrics["retry_targets"] == []


def test_inspect_vnp_reports_runtime_before_audit(tmp_path: Path) -> None:
    request = _request(tmp_path)
    _write_audit(request.run_root, ["mosaic_valid"])
    vnp46a2_download._write_runtime_state(
        request,
        status="running",
        current_phase="download",
        completed_phases=["prepare"],
    )

    result = inspect_vnp46a2_run(request.run_root)

    assert result.status == "succeeded"
    assert result.metrics["status"] == "running"
    assert result.metrics["current_phase"] == "download"
    assert result.metrics["completed_phases"] == ["prepare"]
