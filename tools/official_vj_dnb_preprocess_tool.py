from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from langchain_core.runnables import RunnableConfig
from langchain_core.runnables.config import var_child_runnable_config
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field

from storage_manager import current_thread_id, storage_manager


REPO_ROOT = Path(__file__).resolve().parents[1]
PREPROCESS_SCRIPT = (
    REPO_ROOT
    / "experiments"
    / "official_daily_ntl_fastpath"
    / "convert_vj102_vj103_precise_to_tif.py"
)


class OfficialVJDNBPreprocessInput(BaseModel):
    input_dir: str = Field(
        ...,
        description=(
            "Input folder containing matched VJ102DNB*.nc and VJ103DNB*.nc. "
            "Supports workspace-relative paths and Deep Agents virtual paths "
            "(/data/raw/, /data/processed/, /memories/, /shared/)."
        ),
    )
    output_root: str = Field(
        default="official_vj_dnb_preprocess_runs",
        description="Output subfolder under current thread workspace outputs/.",
    )
    run_label: str = Field(
        default="",
        description="Optional run label; auto-generated if empty.",
    )
    date: str = Field(default="", description="Optional exact date: YYYY-MM-DD")
    start_date: str = Field(default="", description="Optional start date: YYYY-MM-DD")
    end_date: str = Field(default="", description="Optional end date: YYYY-MM-DD")
    bbox: str = Field(
        default="",
        description="Optional clip bbox in lon/lat: minx,miny,maxx,maxy",
    )
    composite: str = Field(
        default="mean",
        description="Daily compositing method: mean/max/min/best_view",
    )
    resolution_m: float = Field(default=500.0, description="Target output resolution in meters")
    radius_m: float = Field(default=2000.0, description="Nearest-neighbor radius in meters")
    radiance_scale: float = Field(default=1e9, description="Radiance scale factor (default 1e9)")
    disable_qf_mask: bool = Field(default=False, description="Disable observation quality-flag mask")
    disable_geo_mask: bool = Field(default=False, description="Disable geolocation quality-flag mask")
    edge_cols: int = Field(default=230, description="Edge-of-swath mask columns on each side")
    disable_edge_mask: bool = Field(default=False, description="Disable edge-of-swath mask")
    solar_zenith_min_deg: float = Field(default=118.5, description="Solar zenith minimum threshold")
    disable_solar_mask: bool = Field(default=False, description="Disable solar zenith mask")
    lunar_zenith_max_deg: float = Field(default=90.0, description="Lunar zenith maximum threshold")
    disable_lunar_mask: bool = Field(default=False, description="Disable lunar zenith mask")


def _resolve_thread_id_from_config(config: Optional[RunnableConfig] = None) -> str:
    runtime_config: Optional[RunnableConfig] = None
    if isinstance(config, dict):
        runtime_config = config
    else:
        inherited = var_child_runnable_config.get()
        if isinstance(inherited, dict):
            runtime_config = inherited

    if isinstance(runtime_config, dict):
        try:
            tid = str(storage_manager.get_thread_id_from_config(runtime_config) or "").strip()
            if tid:
                return tid
        except Exception:
            pass
    return str(current_thread_id.get() or "debug").strip() or "debug"


def _is_virtual_path(path_text: str) -> bool:
    return str(path_text or "").startswith(("/data/raw/", "/data/processed/", "/memories/", "/shared/"))


def _resolve_workspace_path(path_text: str, thread_id: str, *, writable: bool) -> Path:
    workspace = storage_manager.get_workspace(thread_id=thread_id)
    outputs_root = (workspace / "outputs").resolve()

    raw = (path_text or "").strip()
    if not raw:
        raise ValueError("Path is required.")

    if _is_virtual_path(raw):
        if writable and raw.startswith("/shared/"):
            raise PermissionError("Write path under /shared/ is not allowed.")
        return Path(storage_manager.resolve_deepagents_path(raw, thread_id=thread_id))

    p = Path(raw)
    if p.is_absolute():
        raise ValueError("Path must be workspace-relative (absolute path not allowed).")
    if ".." in p.parts:
        raise ValueError("Path must not contain '..'.")

    if writable:
        # Keep writes under workspace/outputs by default.
        if p.parts and p.parts[0] == "outputs":
            target = (workspace / p).resolve()
        elif p.parts and p.parts[0] in {"inputs", "memory"}:
            raise PermissionError("Writable path must be under outputs/, not inputs/ or memory/.")
        else:
            target = (outputs_root / p).resolve()
        if not str(target).startswith(str(outputs_root)):
            raise PermissionError("Writable path resolved outside workspace outputs root.")
        return target

    # Read path: prefer workspace-root relative path first.
    direct = (workspace / p).resolve()
    if direct.exists():
        return direct
    # Backward compatibility: some callers pass outputs-subpath.
    return (outputs_root / p).resolve()


def _ensure_date(v: str, name: str) -> str:
    try:
        datetime.strptime(v, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"{name} must be YYYY-MM-DD, got: {v}") from exc
    return v


def _run_cmd(cmd: list[str], cwd: Path) -> dict[str, Any]:
    started = datetime.utcnow()
    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="ignore",
    )
    ended = datetime.utcnow()
    return {
        "cmd": cmd,
        "returncode": int(proc.returncode),
        "stdout": proc.stdout or "",
        "stderr": proc.stderr or "",
        "started_utc": started.isoformat() + "Z",
        "ended_utc": ended.isoformat() + "Z",
        "duration_sec": max(0.0, (ended - started).total_seconds()),
    }


def _auto_run_label(date: str, start_date: str, end_date: str, run_label: str) -> str:
    if run_label.strip():
        return run_label.strip()
    if date:
        return f"vj_dnb_preprocess_{date.replace('-', '')}"
    if start_date or end_date:
        s = (start_date or "na").replace("-", "")
        e = (end_date or "na").replace("-", "")
        return f"vj_dnb_preprocess_{s}_{e}"
    return f"vj_dnb_preprocess_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"


def run_official_vj_dnb_preprocess(
    input_dir: str,
    output_root: str = "official_vj_dnb_preprocess_runs",
    run_label: str = "",
    date: str = "",
    start_date: str = "",
    end_date: str = "",
    bbox: str = "",
    composite: str = "mean",
    resolution_m: float = 500.0,
    radius_m: float = 2000.0,
    radiance_scale: float = 1e9,
    disable_qf_mask: bool = False,
    disable_geo_mask: bool = False,
    edge_cols: int = 230,
    disable_edge_mask: bool = False,
    solar_zenith_min_deg: float = 118.5,
    disable_solar_mask: bool = False,
    lunar_zenith_max_deg: float = 90.0,
    disable_lunar_mask: bool = False,
    config: Optional[RunnableConfig] = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    Preprocess local VJ102DNB/VJ103DNB NC files into daily GeoTIFF by calling
    convert_vj102_vj103_precise_to_tif.py.
    """
    if composite not in {"mean", "max", "min", "best_view"}:
        raise ValueError("composite must be one of: mean, max, min, best_view")
    if date:
        _ensure_date(date, "date")
    if start_date:
        _ensure_date(start_date, "start_date")
    if end_date:
        _ensure_date(end_date, "end_date")
    if start_date and end_date and end_date < start_date:
        raise ValueError("end_date must be >= start_date")

    thread_id = _resolve_thread_id_from_config(config)
    resolved_input = _resolve_workspace_path(input_dir, thread_id, writable=False)
    if not resolved_input.exists():
        raise FileNotFoundError(f"input_dir does not exist: {resolved_input}")

    out_root = _resolve_workspace_path(output_root, thread_id, writable=True)
    label = _auto_run_label(date, start_date, end_date, run_label)
    run_dir = out_root / label
    run_dir.mkdir(parents=True, exist_ok=True)
    processed_dir = run_dir / "processed_tif"
    processed_dir.mkdir(parents=True, exist_ok=True)

    _ = kwargs
    cmd = [
        sys.executable,
        str(PREPROCESS_SCRIPT),
        "--input-dir",
        str(resolved_input),
        "--output-dir",
        str(processed_dir),
        "--composite",
        composite,
        "--resolution-m",
        str(float(resolution_m)),
        "--radius-m",
        str(float(radius_m)),
        "--radiance-scale",
        str(float(radiance_scale)),
        "--edge-cols",
        str(int(edge_cols)),
        "--solar-zenith-min-deg",
        str(float(solar_zenith_min_deg)),
        "--lunar-zenith-max-deg",
        str(float(lunar_zenith_max_deg)),
    ]
    if date:
        cmd += ["--date", date]
    if start_date:
        cmd += ["--start-date", start_date]
    if end_date:
        cmd += ["--end-date", end_date]
    if bbox.strip():
        cmd += ["--bbox", bbox.strip()]
    if disable_qf_mask:
        cmd.append("--disable-qf-mask")
    if disable_geo_mask:
        cmd.append("--disable-geo-mask")
    if disable_edge_mask:
        cmd.append("--disable-edge-mask")
    if disable_solar_mask:
        cmd.append("--disable-solar-mask")
    if disable_lunar_mask:
        cmd.append("--disable-lunar-mask")

    step = _run_cmd(cmd, REPO_ROOT)
    if step["returncode"] != 0:
        raise RuntimeError(
            "Preprocess step failed.\n"
            f"cmd={' '.join(cmd)}\n"
            f"stderr={step['stderr'][-2000:]}\n"
            f"stdout={step['stdout'][-2000:]}"
        )

    daily_dir = processed_dir / "daily_4326"
    granule_dir = processed_dir / "granules_4326"
    output_files = sorted(str(p) for p in daily_dir.glob("*.tif")) if daily_dir.exists() else []

    manifest = {
        "status": "success",
        "tool": "official_vj_dnb_preprocess_tool",
        "params": {
            "thread_id": thread_id,
            "input_dir": input_dir,
            "output_root": output_root,
            "run_label": label,
            "date": date,
            "start_date": start_date,
            "end_date": end_date,
            "bbox": bbox,
            "composite": composite,
            "resolution_m": float(resolution_m),
            "radius_m": float(radius_m),
            "radiance_scale": float(radiance_scale),
            "disable_qf_mask": bool(disable_qf_mask),
            "disable_geo_mask": bool(disable_geo_mask),
            "edge_cols": int(edge_cols),
            "disable_edge_mask": bool(disable_edge_mask),
            "solar_zenith_min_deg": float(solar_zenith_min_deg),
            "disable_solar_mask": bool(disable_solar_mask),
            "lunar_zenith_max_deg": float(lunar_zenith_max_deg),
            "disable_lunar_mask": bool(disable_lunar_mask),
        },
        "paths": {
            "workspace": str(storage_manager.get_workspace(thread_id=thread_id)),
            "input_dir_resolved": str(resolved_input),
            "run_dir": str(run_dir),
            "processed_dir": str(processed_dir),
            "daily_dir": str(daily_dir),
            "granule_dir": str(granule_dir),
            "preprocess_summary": str(processed_dir / "precise_preprocess_summary.json"),
        },
        "output_files": output_files,
        "steps": {"preprocess": step},
    }
    out_manifest = run_dir / "preprocess_manifest.json"
    out_manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    manifest["paths"]["preprocess_manifest"] = str(out_manifest)
    return manifest


official_vj_dnb_preprocess_tool = StructuredTool.from_function(
    func=run_official_vj_dnb_preprocess,
    name="official_vj_dnb_preprocess_tool",
    description=(
        "Preprocess existing local VJ102DNB/VJ103DNB NC files into daily GeoTIFF "
        "using the precise official VIIRS DNB preprocessing pipeline."
    ),
    args_schema=OfficialVJDNBPreprocessInput,
)


convert_vj102_vj103_precise_to_tif_tool = StructuredTool.from_function(
    func=run_official_vj_dnb_preprocess,
    name="convert_vj102_vj103_precise_to_tif_tool",
    description=(
        "Wrapper of convert_vj102_vj103_precise_to_tif.py. "
        "Preprocess local VJ102DNB/VJ103DNB NC files into daily GeoTIFF with storage_manager-compatible paths."
    ),
    args_schema=OfficialVJDNBPreprocessInput,
)
