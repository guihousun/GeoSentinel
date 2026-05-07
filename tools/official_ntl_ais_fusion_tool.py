from __future__ import annotations

import json
import math
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
FUSION_SCRIPT = REPO_ROOT / "experiments" / "official_daily_ntl_fastpath" / "analyze_ntl_ais_fusion.py"


class OfficialNTLAISFusionInput(BaseModel):
    ntl_daily_dir: str = Field(..., description="Path to daily tif directory, e.g. .../processed_tif/daily_4326")
    start_date: str = Field(..., description="YYYY-MM-DD")
    end_date: str = Field(..., description="YYYY-MM-DD")
    bbox: str = Field(..., description="minx,miny,maxx,maxy")
    output_root: str = Field(default="official_ntl_ais_fusion_runs", description="Output folder under workspace outputs/")
    run_label: str = Field(default="", description="Optional run label")

    ais_path: str = Field(default="", description="Optional AIS CSV/GeoJSON path")
    ais_time_col: str = Field(default="timestamp", description="AIS timestamp column")
    ais_lon_col: str = Field(default="lon", description="AIS longitude column")
    ais_lat_col: str = Field(default="lat", description="AIS latitude column")
    ais_mmsi_col: str = Field(default="mmsi", description="AIS vessel id column")
    ais_sog_col: str = Field(default="sog", description="AIS speed-over-ground column")

    gate_lon: Optional[float] = Field(
        default=None,
        description="Optional gate longitude; omit to use bbox center.",
    )
    anchor_sog_threshold: float = Field(default=1.0, description="Anchored speed threshold")
    anchor_min_points: int = Field(default=3, description="Minimum points/day for anchored vessel")
    demo_mode: bool = Field(default=False, description="Use synthetic AIS demo data if ais_path is missing")
    ntl_cmap: str = Field(default="inferno", description="Matplotlib colormap for NTL rendering, e.g. inferno/gray")


def _resolve_thread_id(config: Optional[RunnableConfig] = None) -> str:
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


def _resolve_path(path_text: str, thread_id: str, writable: bool) -> Path:
    workspace = storage_manager.get_workspace(thread_id=thread_id)
    outputs_root = (workspace / "outputs").resolve()
    raw = (path_text or "").strip()
    if not raw:
        raise ValueError("path required")
    if raw.startswith(("/data/raw/", "/data/processed/", "/memories/", "/shared/")):
        if writable and raw.startswith("/shared/"):
            raise PermissionError("write under /shared/ is not allowed")
        return Path(storage_manager.resolve_deepagents_path(raw, thread_id=thread_id))
    p = Path(raw)
    if p.is_absolute():
        raise ValueError("path must be workspace-relative")
    if ".." in p.parts:
        raise ValueError("path must not contain '..'")
    if writable:
        target = (workspace / p).resolve() if (p.parts and p.parts[0] == "outputs") else (outputs_root / p).resolve()
        if not str(target).startswith(str(outputs_root)):
            raise PermissionError("write path outside outputs root")
        return target
    direct = (workspace / p).resolve()
    if direct.exists():
        return direct
    return (outputs_root / p).resolve()


def _run(cmd: list[str], cwd: Path) -> dict[str, Any]:
    t0 = datetime.utcnow()
    proc = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True, encoding="utf-8", errors="ignore")
    t1 = datetime.utcnow()
    return {
        "cmd": cmd,
        "returncode": int(proc.returncode),
        "stdout": proc.stdout or "",
        "stderr": proc.stderr or "",
        "started_utc": t0.isoformat() + "Z",
        "ended_utc": t1.isoformat() + "Z",
        "duration_sec": max(0.0, (t1 - t0).total_seconds()),
    }


def _optional_finite_float(value: Optional[float], name: str) -> Optional[float]:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a finite number or omitted.") from exc
    if not math.isfinite(numeric):
        raise ValueError(f"{name} must be a finite number or omitted.")
    return numeric


def run_official_ntl_ais_fusion(
    ntl_daily_dir: str,
    start_date: str,
    end_date: str,
    bbox: str,
    output_root: str = "official_ntl_ais_fusion_runs",
    run_label: str = "",
    ais_path: str = "",
    ais_time_col: str = "timestamp",
    ais_lon_col: str = "lon",
    ais_lat_col: str = "lat",
    ais_mmsi_col: str = "mmsi",
    ais_sog_col: str = "sog",
    gate_lon: Optional[float] = None,
    anchor_sog_threshold: float = 1.0,
    anchor_min_points: int = 3,
    demo_mode: bool = False,
    ntl_cmap: str = "inferno",
    config: Optional[RunnableConfig] = None,
    **kwargs,
) -> Dict[str, Any]:
    thread_id = _resolve_thread_id(config)
    gate_lon_value = _optional_finite_float(gate_lon, "gate_lon")
    ntl_dir = _resolve_path(ntl_daily_dir, thread_id, writable=False)
    if not ntl_dir.exists():
        raise FileNotFoundError(f"ntl_daily_dir not found: {ntl_dir}")
    out_root = _resolve_path(output_root, thread_id, writable=True)
    label = run_label.strip() or f"ntl_ais_{start_date.replace('-', '')}_{end_date.replace('-', '')}"
    run_dir = out_root / label
    run_dir.mkdir(parents=True, exist_ok=True)

    _ = kwargs
    cmd = [
        sys.executable,
        str(FUSION_SCRIPT),
        "--ntl-dir",
        str(ntl_dir),
        "--output-dir",
        str(run_dir),
        "--start-date",
        str(start_date),
        "--end-date",
        str(end_date),
        "--bbox",
        str(bbox),
        "--ais-time-col",
        str(ais_time_col),
        "--ais-lon-col",
        str(ais_lon_col),
        "--ais-lat-col",
        str(ais_lat_col),
        "--ais-mmsi-col",
        str(ais_mmsi_col),
        "--ais-sog-col",
        str(ais_sog_col),
        "--anchor-sog-threshold",
        str(float(anchor_sog_threshold)),
        "--anchor-min-points",
        str(int(anchor_min_points)),
        "--ntl-cmap",
        str(ntl_cmap),
    ]
    if gate_lon_value is not None:
        cmd += ["--gate-lon", str(gate_lon_value)]
    if ais_path.strip():
        resolved_ais = _resolve_path(ais_path, thread_id, writable=False)
        if not resolved_ais.exists():
            raise FileNotFoundError(f"ais_path not found: {resolved_ais}")
        cmd += ["--ais-path", str(resolved_ais)]
    if demo_mode:
        cmd += ["--demo-mode"]

    step = _run(cmd, REPO_ROOT)
    if step["returncode"] != 0:
        raise RuntimeError(
            "NTL-AIS fusion step failed.\n"
            f"cmd={' '.join(cmd)}\n"
            f"stderr={step['stderr'][-1600:]}\n"
            f"stdout={step['stdout'][-1600:]}"
        )

    summary_path = run_dir / "ntl_ais_fusion_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.exists() else {}
    return {
        "status": "success",
        "tool": "official_ntl_ais_fusion_tool",
        "thread_id": thread_id,
        "run_dir": str(run_dir),
        "summary_path": str(summary_path),
        "outputs": summary.get("outputs", {}),
        "ais_source": summary.get("ais_source"),
        "step": step,
    }


official_ntl_ais_fusion_tool = StructuredTool.from_function(
    func=run_official_ntl_ais_fusion,
    name="official_ntl_ais_fusion_tool",
    description=(
        "Fuse daily NTL rasters with AIS trajectories/points to estimate vessel activity, anchored vessels, "
        "and gate crossings, and export metrics + visualizations."
    ),
    args_schema=OfficialNTLAISFusionInput,
)
