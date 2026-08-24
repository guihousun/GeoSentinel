from __future__ import annotations

import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Optional

from dotenv import dotenv_values
from langchain_core.runnables import RunnableConfig
from langchain_core.runnables.config import var_child_runnable_config
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field

from storage_manager import current_thread_id, storage_manager


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = REPO_ROOT / "tools" / "vnp46a2_official_h5"
_TARGET_RE = re.compile(r"^[A-Z]{3}:\d{4}-\d{2}-\d{2}$")
_SCRIPT_BY_PHASE = {
    "prepare": "prepare_vnp46a2_osm_boundaries_2026.py",
    "download": "download_vnp46a2_official_h5_osm_countries_2026.py",
    "mosaic": "mosaic_vnp46a2_official_h5_osm_countries_2026.py",
    "audit": "audit_vnp46a2_country_coverage.py",
    "organize": "organize_vnp46a2_final_results.py",
}


class OfficialVNP46A2H5CountryInput(BaseModel):
    start_date: str = Field(..., description="Inclusive UTC product start date in YYYY-MM-DD.")
    end_date: str = Field(..., description="Inclusive requested UTC product end date in YYYY-MM-DD.")
    countries: list[str] = Field(
        ...,
        min_length=1,
        description="One or more supported ISO3 codes, for example ['ISR'] or ['PAK', 'AUS'].",
    )
    output_root: str = Field(
        default="official_vnp46a2_h5_country_runs",
        description="Workspace-relative directory created under the current thread outputs/.",
    )
    phase: Literal["full", "prepare", "download", "mosaic", "audit", "organize"] = Field(
        default="full",
        description="Pipeline phase. full runs boundary preparation, download, mosaic, then audit in order.",
    )
    execution_mode: Literal["plan", "run"] = Field(
        default="plan",
        description="plan is side-effect free. run executes the requested phase inside the current thread workspace.",
    )
    targets: list[str] = Field(
        default_factory=list,
        description="Optional retry targets as ISO3:YYYY-MM-DD. Applies to download and mosaic phases.",
    )
    limit_days: int = Field(default=0, ge=0, le=366, description="Optional first-N-day limit for download or mosaic.")
    workers: int = Field(default=4, ge=1, le=8, description="Concurrent HDF5 download workers. Use 4 for retries.")
    download_timeout: int = Field(default=600, ge=60, le=1800, description="Per-HDF download timeout in seconds.")
    token_env: str = Field(default="EARTHDATA_TOKEN", description="Earthdata bearer-token environment variable name.")
    no_gee_latest: bool = Field(
        default=False,
        description="Skip Earth Engine latest-date clamping only when an authoritative product-end date is already known.",
    )
    force: bool = Field(default=False, description="Rebuild existing download or mosaic artifacts for the requested phase.")
    skip_pixel_scan: bool = Field(default=False, description="For audit only, skip GeoTIFF valid-pixel scan.")
    package_source_root: str = Field(
        default="",
        description="For organize only: workspace-relative audited run directory containing vnp46a2_country_day_coverage_audit.csv.",
    )
    package_output_root: str = Field(
        default="",
        description="For organize only: optional workspace-relative final-package directory. Defaults below output_root.",
    )
    package_copy: bool = Field(default=False, description="For organize only: copy GeoTIFFs instead of same-volume hard links.")


def _thread_id(config: Optional[RunnableConfig] = None) -> str:
    runtime_config = config if isinstance(config, dict) else var_child_runnable_config.get()
    if isinstance(runtime_config, dict):
        try:
            resolved = str(storage_manager.get_thread_id_from_config(runtime_config) or "").strip()
            if resolved:
                return resolved
        except Exception:
            pass
    return str(current_thread_id.get() or "debug").strip() or "debug"


def _workspace_output_root(output_root: str, thread_id: str) -> Path:
    workspace = storage_manager.get_workspace(thread_id=thread_id)
    outputs_root = (workspace / "outputs").resolve()
    raw = str(output_root or "").strip() or "official_vnp46a2_h5_country_runs"
    if raw.startswith("/data/processed/"):
        target = Path(storage_manager.resolve_deepagents_path(raw, thread_id=thread_id))
    else:
        candidate = Path(raw)
        if candidate.is_absolute() or ".." in candidate.parts:
            raise ValueError("output_root must be a workspace-relative outputs path without '..'.")
        if candidate.parts and candidate.parts[0] in {"inputs", "memory"}:
            raise PermissionError("output_root must be under outputs/, not inputs/ or memory/.")
        target = (workspace / candidate).resolve() if candidate.parts and candidate.parts[0] == "outputs" else (outputs_root / candidate).resolve()
    if target != outputs_root and outputs_root not in target.parents:
        raise PermissionError("output_root resolved outside the current thread outputs directory.")
    return target


def _workspace_existing_output_path(path_text: str, thread_id: str) -> Path:
    path = _workspace_output_root(path_text, thread_id)
    if not path.exists():
        raise FileNotFoundError(f"package_source_root was not found in current thread outputs: {path}")
    return path


def _date(value: str, name: str) -> str:
    try:
        return datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"{name} must be YYYY-MM-DD, got: {value}") from exc


def _countries(values: list[str]) -> list[str]:
    result = sorted({str(value).strip().upper() for value in values if str(value).strip()})
    if not result or any(not re.fullmatch(r"[A-Z]{3}", value) for value in result):
        raise ValueError("countries must contain one or more three-letter ISO3 codes.")
    return result


def _targets(values: list[str]) -> list[str]:
    result = sorted({str(value).strip().upper() for value in values if str(value).strip()})
    invalid = [value for value in result if not _TARGET_RE.fullmatch(value)]
    if invalid:
        raise ValueError("targets must use ISO3:YYYY-MM-DD, for example ISR:2026-02-13.")
    return result


def _safe_text(value: str) -> str:
    value = re.sub(r"(Authorization:\s*Bearer\s+)[^\s]+", r"\1<REDACTED>", value)
    return re.sub(r"Bearer\s+[^\s]+", "Bearer <REDACTED>", value)


def _runtime_env() -> dict[str, str]:
    env = os.environ.copy()
    dotenv_path = REPO_ROOT / ".env"
    if dotenv_path.exists():
        for key, value in dotenv_values(dotenv_path).items():
            if key and value and not env.get(key):
                env[key] = value
    env["PYTHONIOENCODING"] = "utf-8"
    return env


def _phase_list(phase: str) -> list[str]:
    return ["prepare", "download", "mosaic", "audit"] if phase == "full" else [phase]


def _command(
    phase: str,
    *,
    start_date: str,
    end_date: str,
    output_root: Path,
    countries: list[str],
    targets: list[str],
    limit_days: int,
    workers: int,
    download_timeout: int,
    token_env: str,
    no_gee_latest: bool,
    force: bool,
    skip_pixel_scan: bool,
    package_source_root: Path | None,
    package_output_root: Path | None,
    package_copy: bool,
) -> list[str]:
    if phase == "organize":
        if package_source_root is None or package_output_root is None:
            raise ValueError("organize requires package_source_root and package_output_root.")
        command = [
            sys.executable,
            str(SCRIPT_DIR / _SCRIPT_BY_PHASE[phase]),
            "--source-root",
            str(package_source_root),
            "--output-root",
            str(package_output_root),
        ]
        if package_copy:
            command.append("--copy")
        return command
    command = [sys.executable, str(SCRIPT_DIR / _SCRIPT_BY_PHASE[phase]), "--start", start_date, "--end", end_date, "--output-root", str(output_root), "--countries", *countries]
    if phase in {"download", "mosaic"} and targets:
        command += ["--targets", *targets]
    if phase in {"download", "mosaic"} and limit_days:
        command += ["--limit-days", str(limit_days)]
    if phase == "download":
        command += ["--workers", str(workers), "--download-timeout", str(download_timeout), "--token-env", token_env]
        if no_gee_latest:
            command.append("--no-gee-latest")
    if phase == "prepare" and no_gee_latest:
        command.append("--no-gee-latest")
    if phase in {"download", "mosaic"} and force:
        command.append("--force")
    if phase == "audit" and skip_pixel_scan:
        command.append("--skip-pixel-scan")
    return command


def run_official_vnp46a2_h5_country_mosaic(
    start_date: str,
    end_date: str,
    countries: list[str],
    output_root: str = "official_vnp46a2_h5_country_runs",
    phase: Literal["full", "prepare", "download", "mosaic", "audit", "organize"] = "full",
    execution_mode: Literal["plan", "run"] = "plan",
    targets: Optional[list[str]] = None,
    limit_days: int = 0,
    workers: int = 4,
    download_timeout: int = 600,
    token_env: str = "EARTHDATA_TOKEN",
    no_gee_latest: bool = False,
    force: bool = False,
    skip_pixel_scan: bool = False,
    package_source_root: str = "",
    package_output_root: str = "",
    package_copy: bool = False,
    config: Optional[RunnableConfig] = None,
) -> dict[str, Any]:
    """Plan or execute the audited official HDF5 VNP46A2 country-day pipeline."""
    start_date = _date(start_date, "start_date")
    end_date = _date(end_date, "end_date")
    if end_date < start_date:
        raise ValueError("end_date must be >= start_date")
    countries = _countries(countries)
    retry_targets = _targets(targets or [])
    if retry_targets and any(target.split(":", 1)[0] not in countries for target in retry_targets):
        raise ValueError("Each target ISO3 code must also be present in countries.")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", token_env):
        raise ValueError("token_env must be a valid environment variable name.")

    thread_id = _thread_id(config)
    resolved_output_root = _workspace_output_root(output_root, thread_id)
    phases = _phase_list(phase)
    package_source = None
    package_output = None
    if "organize" in phases:
        if not str(package_source_root or "").strip():
            raise ValueError("package_source_root is required when phase='organize'.")
        package_source = _workspace_existing_output_path(package_source_root, thread_id)
        default_package_output = str(Path(output_root or "official_vnp46a2_h5_country_runs") / "final_package")
        package_output = _workspace_output_root(package_output_root or default_package_output, thread_id)
    commands = [
        _command(
            item,
            start_date=start_date,
            end_date=end_date,
            output_root=resolved_output_root,
            countries=countries,
            targets=retry_targets,
            limit_days=limit_days,
            workers=workers,
            download_timeout=download_timeout,
            token_env=token_env,
            no_gee_latest=no_gee_latest,
            force=force,
            skip_pixel_scan=skip_pixel_scan,
            package_source_root=package_source,
            package_output_root=package_output,
            package_copy=package_copy,
        )
        for item in phases
    ]
    result: dict[str, Any] = {
        "tool": "official_vnp46a2_h5_country_mosaic_tool",
        "status": "plan" if execution_mode == "plan" else "running",
        "pipeline": "official_earthdata_hdf5_non_gap_filled_vnp46a2",
        "dataset": "VNP46A2",
        "band": "DNB_BRDF_Corrected_NTL",
        "thread_id": thread_id,
        "output_root": str(resolved_output_root),
        "countries": countries,
        "start_date": start_date,
        "requested_end_date": end_date,
        "phase": phase,
        "commands": commands,
        "audit_requirements": [
            "downloaded_without_mosaic must be 0 before completion",
            "no_granules is availability, not a transport failure",
            "mosaic_all_nodata is terminal only after valid HDF5, successful mosaic, and pixel scan",
        ],
    }
    if execution_mode == "plan":
        result["next_action"] = "Confirm parameters, then call again with execution_mode='run'."
        return result

    env = _runtime_env()
    if "download" in phases and not str(env.get(token_env) or "").strip():
        result.update(
            {
                "status": "needs_configuration",
                "error": f"{token_env} is required for official Earthdata downloads and was not found in environment or project .env.",
            }
        )
        return result

    runs: list[dict[str, Any]] = []
    resolved_output_root.mkdir(parents=True, exist_ok=True)
    for item, command in zip(phases, commands):
        started = datetime.now(timezone.utc)
        proc = subprocess.run(
            command,
            cwd=str(SCRIPT_DIR),
            env=env,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            timeout=7200 if item == "download" else 3600,
            check=False,
        )
        ended = datetime.now(timezone.utc)
        step = {
            "phase": item,
            "returncode": int(proc.returncode),
            "duration_sec": round((ended - started).total_seconds(), 3),
            "stdout_tail": _safe_text((proc.stdout or "")[-2400:]),
            "stderr_tail": _safe_text((proc.stderr or "")[-2400:]),
        }
        runs.append(step)
        if proc.returncode != 0:
            result.update({"status": "error", "failed_phase": item, "steps": runs})
            return result

    result.update({"status": "success", "steps": runs})
    return result


official_vnp46a2_h5_country_mosaic_tool = StructuredTool.from_function(
    func=run_official_vnp46a2_h5_country_mosaic,
    name="official_vnp46a2_h5_country_mosaic_tool",
    description=(
        "Plan or run the official Earthdata HDF5 route for country-scale, non-gap-filled VNP46A2 "
        "DNB_BRDF_Corrected_NTL. It prepares OSM 0.001-degree boundaries, downloads validated HDF5 "
        "granules, mosaics/clips GeoTIFFs, and audits country-days inside the current thread workspace."
    ),
    args_schema=OfficialVNP46A2H5CountryInput,
)
