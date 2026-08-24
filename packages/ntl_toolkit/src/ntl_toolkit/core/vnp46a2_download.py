from __future__ import annotations

import csv
import json
import os
import re
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Literal

from ntl_toolkit.runtime.downloads import (
    DownloadProgress,
    sanitize_download_text,
    write_download_manifest,
)
from ntl_toolkit.schemas import OutputArtifact, ToolError, ToolResult

_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ISO3 = re.compile(r"^[A-Z]{3}$")
_TARGET = re.compile(r"^[A-Z]{3}:\d{4}-\d{2}-\d{2}$")
_PHASES = ("prepare", "download", "mosaic", "audit", "organize")
_SCRIPTS = {
    "prepare": "prepare_vnp46a2_osm_boundaries_2026.py",
    "download": "download_vnp46a2_official_h5_osm_countries_2026.py",
    "mosaic": "mosaic_vnp46a2_official_h5_osm_countries_2026.py",
    "audit": "audit_vnp46a2_country_coverage.py",
    "organize": "organize_vnp46a2_final_results.py",
}
_INCOMPLETE_STATUSES = {
    "downloaded_without_mosaic",
    "retry_download",
    "not_processed",
    "other_manifest_status",
}


@dataclass(frozen=True)
class Vnp46a2DownloadRequest:
    start_date: str
    end_date: str
    countries: list[str]
    output_root: str
    phase: Literal["full", "prepare", "download", "mosaic", "audit", "organize"] = "full"
    execution_mode: Literal["plan", "run"] = "plan"
    targets: list[str] = field(default_factory=list)
    limit_days: int = 0
    workers: int = 4
    download_timeout: int = 600
    token_env: str = "EARTHDATA_TOKEN"
    no_gee_latest: bool = False
    force: bool = False
    skip_pixel_scan: bool = False
    package_source_root: str = ""
    package_output_root: str = ""
    package_copy: bool = False

    def __post_init__(self) -> None:
        _validate_date(self.start_date, "start_date")
        _validate_date(self.end_date, "end_date")
        if self.end_date < self.start_date:
            raise ValueError("end_date must be on or after start_date")
        countries = sorted({str(value).strip().upper() for value in self.countries if str(value).strip()})
        if not countries or any(not _ISO3.fullmatch(value) for value in countries):
            raise ValueError("countries must contain one or more ISO3 codes")
        targets = sorted({str(value).strip().upper() for value in self.targets if str(value).strip()})
        if any(not _TARGET.fullmatch(value) for value in targets):
            raise ValueError("targets must use ISO3:YYYY-MM-DD")
        if any(value.split(":", 1)[0] not in countries for value in targets):
            raise ValueError("every retry target country must appear in countries")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", self.token_env):
            raise ValueError("token_env must be an environment variable name")
        if not 1 <= self.workers <= 8:
            raise ValueError("workers must be between 1 and 8")
        if not 60 <= self.download_timeout <= 1800:
            raise ValueError("download_timeout must be between 60 and 1800 seconds")
        if self.limit_days < 0:
            raise ValueError("limit_days must be non-negative")
        if not str(self.output_root).strip():
            raise ValueError("output_root must not be empty")
        object.__setattr__(self, "countries", countries)
        object.__setattr__(self, "targets", targets)

    @property
    def phase_list(self) -> list[str]:
        return ["prepare", "download", "mosaic", "audit"] if self.phase == "full" else [self.phase]

    @property
    def output_path(self) -> Path:
        return Path(self.output_root).expanduser().resolve(strict=False)

    @property
    def run_root(self) -> Path:
        return self.output_path / f"VNP46A2_unfilled_osm_0p001_{self.start_date}_to_{self.end_date}"


@dataclass(frozen=True)
class PhaseOutcome:
    phase: str
    returncode: int
    stdout: str
    stderr: str


def run_vnp46a2_download(
    request: Vnp46a2DownloadRequest,
    *,
    progress: DownloadProgress | None = None,
) -> ToolResult:
    """Plan or run the audited official VNP46A2 HDF5 country pipeline."""
    commands = _build_phase_commands(request)
    if request.execution_mode == "plan":
        return ToolResult.succeeded(
            tool="download_vnp46a2_official_h5_country",
            summary="Prepared VNP46A2 execution plan.",
            metrics={"commands": commands, "run_root": str(request.run_root)},
        )

    if "download" in request.phase_list and not os.getenv(request.token_env, "").strip():
        return _failed(
            "EARTHDATA_TOKEN_MISSING",
            f"{request.token_env} is not configured.",
            "Set the token in NTL_MCP_ENV_FILE or the process environment, then retry.",
            metrics={"run_root": str(request.run_root)},
        )

    request.output_path.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    total = len(request.phase_list)
    completed: list[str] = []
    _write_runtime_state(request, status="running", current_phase="", completed_phases=completed)

    try:
        for index, phase in enumerate(request.phase_list, start=1):
            _write_runtime_state(request, status="running", current_phase=phase, completed_phases=completed)
            _report(progress, index - 1, total, phase)
            outcome = _run_phase(
                phase,
                commands[phase],
                _script_dir(),
                env,
                lambda line: _report(progress, index - 1, total, sanitize_download_text(line)),
            )
            _write_phase_manifest(request.run_root, outcome)
            if outcome.returncode:
                _write_runtime_state(request, status="failed", current_phase=phase, completed_phases=completed)
                if phase == "audit" and (request.run_root / "vnp46a2_country_day_coverage_audit.csv").exists():
                    return inspect_vnp46a2_run(request.run_root)
                return _failed(
                    "VNP46A2_PHASE_FAILED",
                    f"{phase} exited with code {outcome.returncode}.",
                    "Inspect the stored phase manifest, then retry only the returned targets.",
                    metrics={
                        "phase": phase,
                        "run_root": str(request.run_root),
                        "stdout_tail": sanitize_download_text(outcome.stdout[-2400:]),
                        "stderr_tail": sanitize_download_text(outcome.stderr[-2400:]),
                    },
                )
            completed.append(phase)
            _write_runtime_state(request, status="running", current_phase="", completed_phases=completed)
    except Exception as exc:  # noqa: BLE001
        _write_runtime_state(request, status="failed", current_phase="", completed_phases=completed)
        return _failed(
            "VNP46A2_PHASE_FAILED",
            sanitize_download_text(str(exc) or type(exc).__name__),
            "Inspect the stored phase manifest and runtime status, then retry the affected phase.",
            metrics={"run_root": str(request.run_root), "completed_phases": completed},
        )

    _report(progress, total, total, "completed")
    _write_runtime_state(request, status="completed", current_phase="", completed_phases=completed)
    if "audit" in request.phase_list:
        return inspect_vnp46a2_run(request.run_root)
    return ToolResult.succeeded(
        tool="download_vnp46a2_official_h5_country",
        summary=f"Completed VNP46A2 {request.phase} phase.",
        metrics={"phase": request.phase, "run_root": str(request.run_root)},
    )


def inspect_vnp46a2_run(run_root: str | Path) -> ToolResult:
    """Read an official VNP46A2 audit and expose exact retry actions."""
    root = Path(run_root).expanduser().resolve(strict=False)
    audit_path = root / "vnp46a2_country_day_coverage_audit.csv"
    runtime_path = root / "ntl_download_runtime.json"
    if runtime_path.exists():
        runtime = json.loads(runtime_path.read_text(encoding="utf-8"))
        if runtime.get("status") in {"running", "failed"}:
            return ToolResult.succeeded(
                tool="download_vnp46a2_official_h5_country",
                summary=f"VNP46A2 run is {runtime['status']}.",
                metrics={"run_root": str(root), "runtime_path": str(runtime_path), **runtime},
            )
    if not audit_path.exists():
        if runtime_path.exists():
            return ToolResult.succeeded(
                tool="download_vnp46a2_official_h5_country",
                summary=f"VNP46A2 run is {runtime.get('status', 'running')}.",
                metrics={"run_root": str(root), "runtime_path": str(runtime_path), **runtime},
            )
        return _failed(
            "VNP46A2_AUDIT_NOT_FOUND",
            f"No country-day audit was found under {root}.",
            "Run the audit phase before inspecting or declaring this download complete.",
            metrics={"run_root": str(root)},
        )

    with audit_path.open("r", newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))
    statuses = Counter(str(row.get("audit_status") or "") for row in rows)
    retry_targets = sorted(
        f"{row['iso3']}:{row['date']}"
        for row in rows
        if row.get("audit_status") in {"retry_download", "not_processed"}
        and row.get("iso3")
        and row.get("date")
    )
    pending_mosaic_targets = sorted(
        f"{row['iso3']}:{row['date']}"
        for row in rows
        if row.get("audit_status") == "downloaded_without_mosaic"
        and row.get("iso3")
        and row.get("date")
    )
    metrics = {
        "run_root": str(root),
        "audit_path": str(audit_path),
        "status_counts": dict(statuses),
        "retry_targets": retry_targets,
        "pending_mosaic_targets": pending_mosaic_targets,
    }
    incomplete = sum(statuses.get(status, 0) for status in _INCOMPLETE_STATUSES)
    if incomplete:
        return _failed(
            "VNP46A2_AUDIT_INCOMPLETE",
            "The VNP46A2 audit reports unfinished country-day outputs.",
            "Retry only retry_targets, mosaic pending_mosaic_targets, then run audit again.",
            metrics=metrics,
            outputs=[OutputArtifact(path=str(audit_path), media_type="text/csv", role="audit")],
        )
    return ToolResult.succeeded(
        tool="download_vnp46a2_official_h5_country",
        summary="The VNP46A2 audit is complete.",
        metrics=metrics,
        outputs=[OutputArtifact(path=str(audit_path), media_type="text/csv", role="audit")],
    )


def _build_phase_commands(request: Vnp46a2DownloadRequest) -> dict[str, list[str]]:
    commands: dict[str, list[str]] = {}
    for phase in request.phase_list:
        if phase == "organize":
            source = Path(request.package_source_root).expanduser().resolve(strict=False) if request.package_source_root else request.run_root
            destination = Path(request.package_output_root).expanduser().resolve(strict=False) if request.package_output_root else request.output_path / "final_package"
            command = [sys.executable, str(_script_dir() / _SCRIPTS[phase]), "--source-root", str(source), "--output-root", str(destination)]
            if request.package_copy:
                command.append("--copy")
            commands[phase] = command
            continue

        command = [
            sys.executable,
            str(_script_dir() / _SCRIPTS[phase]),
            "--start",
            request.start_date,
            "--end",
            request.end_date,
            "--output-root",
            str(request.output_path),
            "--countries",
            *request.countries,
        ]
        if phase in {"download", "mosaic"} and request.targets:
            command += ["--targets", *request.targets]
        if phase in {"download", "mosaic"} and request.limit_days:
            command += ["--limit-days", str(request.limit_days)]
        if phase == "download":
            command += ["--workers", str(request.workers), "--download-timeout", str(request.download_timeout), "--token-env", request.token_env]
            if request.no_gee_latest:
                command.append("--no-gee-latest")
        if phase == "prepare" and request.no_gee_latest:
            command.append("--no-gee-latest")
        if phase in {"download", "mosaic"} and request.force:
            command.append("--force")
        if phase == "audit" and request.skip_pixel_scan:
            command.append("--skip-pixel-scan")
        commands[phase] = command
    return commands


def _run_phase(
    phase: str,
    command: list[str],
    cwd: Path,
    env: dict[str, str],
    on_line,
) -> PhaseOutcome:
    lines: list[str] = []
    process = subprocess.Popen(
        command,
        cwd=str(cwd),
        env=env,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    assert process.stdout is not None
    for line in process.stdout:
        safe = sanitize_download_text(line.rstrip())
        if safe:
            lines.append(safe)
            on_line(safe)
    return PhaseOutcome(phase=phase, returncode=process.wait(), stdout="\n".join(lines)[-4000:], stderr="")


def _write_phase_manifest(run_root: Path, outcome: PhaseOutcome) -> Path:
    return write_download_manifest(
        run_root / f"ntl_download_phase_{outcome.phase}.json",
        {
            "phase": outcome.phase,
            "returncode": outcome.returncode,
            "stdout_tail": outcome.stdout[-4000:],
            "stderr_tail": outcome.stderr[-4000:],
            "finished_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        },
    )


def _write_runtime_state(
    request: Vnp46a2DownloadRequest,
    *,
    status: str,
    current_phase: str,
    completed_phases: list[str],
) -> Path:
    return write_download_manifest(
        request.run_root / "ntl_download_runtime.json",
        {
            "product": "VNP46A2",
            "status": status,
            "current_phase": current_phase,
            "completed_phases": completed_phases,
            "countries": request.countries,
            "start_date": request.start_date,
            "end_date": request.end_date,
            "updated_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        },
    )


def _script_dir() -> Path:
    return Path(__file__).resolve().parents[5] / "tools" / "vnp46a2_official_h5"


def _validate_date(value: str, name: str) -> None:
    if not _DATE.fullmatch(value):
        raise ValueError(f"{name} must use YYYY-MM-DD")
    datetime.strptime(value, "%Y-%m-%d")


def _report(progress: DownloadProgress | None, current: float, total: float, message: str) -> None:
    if progress is not None:
        progress(float(current), float(total), message)


def _failed(
    code: str,
    message: str,
    suggestion: str,
    *,
    metrics: dict[str, object],
    outputs: list[OutputArtifact] | None = None,
) -> ToolResult:
    return ToolResult(
        status="failed",
        tool="download_vnp46a2_official_h5_country",
        summary=message,
        error=ToolError(code=code, message=message, suggestion=suggestion),
        metrics=metrics,
        outputs=outputs or [],
    )
