from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from langchain_core.runnables import RunnableConfig
from langchain_core.runnables.config import var_child_runnable_config
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field

from experiments.official_daily_ntl_fastpath.source_registry import get_source_spec, parse_sources_arg
from storage_manager import current_thread_id, storage_manager


REPO_ROOT = Path(__file__).resolve().parents[1]
QUERY_SCRIPT = REPO_ROOT / "tools" / "query_vj_dnb_laads_json.py"
DOWNLOAD_SCRIPT = REPO_ROOT / "tools" / "download_vj_dnb.py"
PREPROCESS_SCRIPT = REPO_ROOT / "experiments" / "official_daily_ntl_fastpath" / "convert_vj102_vj103_precise_to_tif.py"
GIF_SCRIPT = REPO_ROOT / "experiments" / "official_daily_ntl_fastpath" / "make_ntl_daily_gif.py"
GRIDDED_DOWNLOAD_SCRIPT = REPO_ROOT / "experiments" / "official_daily_ntl_fastpath" / "download_official_ntl_by_bbox.py"


class OfficialVJDNBPipelineInput(BaseModel):
    start_date: str = Field(..., description="Start date in YYYY-MM-DD")
    end_date: str = Field(..., description="End date in YYYY-MM-DD")
    bbox: str = Field(..., description="minx,miny,maxx,maxy in lon/lat. Example: 44.03,25.08,63.33,39.77")
    output_root: str = Field(
        default="official_vj_dnb_pipeline_runs",
        description=(
            "Output subfolder under current thread workspace outputs/. "
            "Example: official_vj_dnb_pipeline_runs"
        ),
    )
    run_label: str = Field(
        default="",
        description="Optional run label. If empty, auto-generated from date range.",
    )
    sources: str = Field(
        default="VJ102DNB,VJ103DNB",
        description="Comma-separated sources for query step. Default includes both.",
    )
    composite: str = Field(default="mean", description="Daily composite method: mean or max")
    resolution_m: float = Field(default=500.0, description="Output target resolution in meters")
    radius_m: float = Field(default=2000.0, description="Nearest-neighbor radius in meters")
    radiance_scale: float = Field(default=1e9, description="Radiance scale factor, default 1e9")
    token_env: str = Field(default="EARTHDATA_TOKEN", description="Earthdata token env var name")
    page_size: int = Field(default=200, description="CMR query page size")
    skip_preprocess: bool = Field(default=False, description="If true, only query+download without preprocessing")
    qa_mode: str = Field(
        default="",
        description="Optional QA mode for gridded sources: balanced | strict | clear_only. Empty uses source default.",
    )
    generate_gif: bool = Field(default=True, description="If true, render daily tif series to GIF (step 4).")
    gif_style_palette: str = Field(
        default="report_dark",
        description="GIF style preset: report_dark | night_blue | mono_gray | impact_hot | white_viridis",
    )
    overlay_vector: str = Field(default="", description="Optional event vector file (GeoJSON/SHP/GPKG) for GIF overlay.")
    overlay_label_field: str = Field(default="", description="Optional overlay field used for labels in GIF frames.")
    overlay_point_class_field: str = Field(default="", description="Optional point class field for legend categories.")
    point_legend_label: str = Field(default="事件点", description="Single-style point legend label.")
    point_legend_title: str = Field(default="事件类型", description="Point legend title when class field is used.")
    show_point_legend: bool = Field(default=True, description="Whether to display point legend in GIF.")
    boundary_vector: str = Field(default="", description="Optional boundary vector file (GeoJSON/SHP/GPKG).")
    boundary_edge_color: str = Field(default="#3dd3ff", description="Boundary line color in GIF.")
    boundary_linewidth: float = Field(default=1.1, description="Boundary line width in GIF.")
    boundary_alpha: float = Field(default=0.95, description="Boundary line alpha in GIF.")
    gif_duration_ms: int = Field(default=900, description="GIF frame duration in milliseconds.")
    gif_fps: float = Field(default=0.0, description="Optional FPS for GIF; overrides duration when >0.")
    gif_percentile_min: float = Field(default=2.0, description="GIF display lower percentile.")
    gif_percentile_max: float = Field(default=98.0, description="GIF display upper percentile.")
    gif_cmap: str = Field(default="inferno", description="GIF colormap name.")
    ask_user_for_params: bool = Field(
        default=False,
        description="If true, do not execute; return a checklist of questions for user parameter confirmation.",
    )


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


def _resolve_output_root(output_root: str, thread_id: str) -> tuple[Path, str]:
    workspace = storage_manager.get_workspace(thread_id=thread_id)
    outputs_root = (workspace / "outputs").resolve()
    raw = (output_root or "").strip()
    if not raw:
        raw = "official_vj_dnb_pipeline_runs"

    if raw.startswith("/data/processed/"):
        return Path(storage_manager.resolve_deepagents_path(raw, thread_id=thread_id)), "deepagents_virtual"
    if raw.startswith("/shared/"):
        raise PermissionError("output_root under /shared/ is not allowed for writing.")
    if raw.startswith("/data/raw/") or raw.startswith("/memories/"):
        raise PermissionError("output_root must be under /data/processed/ or workspace outputs, not /data/raw or /memories.")

    p = Path(raw)
    if p.is_absolute():
        raise ValueError("output_root must be workspace-relative (do not use absolute path).")
    if ".." in p.parts:
        raise ValueError("output_root must not contain '..'.")

    if p.parts and p.parts[0] == "outputs":
        target = (workspace / p).resolve()
        mode = "workspace_relative_outputs_prefix"
    elif p.parts and p.parts[0] in {"inputs", "memory"}:
        raise PermissionError("output_root must be under outputs/, not inputs/ or memory/.")
    else:
        target = (outputs_root / p).resolve()
        mode = "workspace_outputs_relative"
    if not str(target).startswith(str(outputs_root)):
        raise PermissionError("output_root resolved outside workspace outputs root.")
    return target, mode


def _resolve_read_path(path_text: str, thread_id: str) -> Path:
    workspace = storage_manager.get_workspace(thread_id=thread_id)
    outputs_root = (workspace / "outputs").resolve()
    raw = (path_text or "").strip()
    if not raw:
        raise ValueError("Path is required.")
    if raw.startswith(("/data/raw/", "/data/processed/", "/memories/", "/shared/")):
        return Path(storage_manager.resolve_deepagents_path(raw, thread_id=thread_id))
    p = Path(raw)
    if p.is_absolute():
        raise ValueError("Read path must be workspace-relative (absolute path not allowed).")
    if ".." in p.parts:
        raise ValueError("Read path must not contain '..'.")
    direct = (workspace / p).resolve()
    if direct.exists():
        return direct
    return (outputs_root / p).resolve()


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


def _reset_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _load_daily_valid_ratios(summary_path: Path) -> list[float]:
    if not summary_path.exists():
        return []
    try:
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    out: list[float] = []
    for row in payload.get("daily_outputs", []):
        try:
            out.append(float(row.get("valid_ratio", 0.0)))
        except Exception:
            out.append(0.0)
    return out


def _ensure_date(v: str, name: str) -> str:
    try:
        datetime.strptime(v, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"{name} must be YYYY-MM-DD, got: {v}") from exc
    return v


def _build_run_label(start_date: str, end_date: str, run_label: str) -> str:
    if run_label.strip():
        return run_label.strip()
    return f"vj_dnb_{start_date.replace('-', '')}_{end_date.replace('-', '')}"


def _resolve_pipeline_mode(sources_raw: str) -> tuple[str, list[str]]:
    swath_sources = {"VJ102DNB", "VJ103DNB", "VJ102DNB_NRT", "VJ103DNB_NRT"}
    raw_parts = [part.strip().upper() for part in str(sources_raw or "").split(",") if part.strip()]
    if not raw_parts:
        raw_parts = ["VJ102DNB", "VJ103DNB"]

    deduped: list[str] = []
    for key in raw_parts:
        if key in swath_sources:
            if key not in deduped:
                deduped.append(key)
            continue
        spec = get_source_spec(key)
        if spec.processing_mode != "gridded_tile_clip":
            raise ValueError(f"Unsupported source for official_vj_dnb_fullchain_tool: {key}")
        if key not in deduped:
            deduped.append(key)

    has_swath = any(key in swath_sources for key in deduped)
    has_gridded = any(key not in swath_sources for key in deduped)
    if has_swath and has_gridded:
        raise ValueError("Mixed swath and gridded sources are not supported in one official_vj_dnb_fullchain_tool run.")
    if has_gridded:
        resolved = parse_sources_arg(",".join(key for key in deduped if key not in swath_sources))
        return "gridded_tile_clip", resolved
    return "swath_precise", deduped


def run_official_vj_dnb_fullchain(
    start_date: str,
    end_date: str,
    bbox: str,
    output_root: str = "official_vj_dnb_pipeline_runs",
    run_label: str = "",
    sources: str = "VJ102DNB,VJ103DNB",
    composite: str = "mean",
    resolution_m: float = 500.0,
    radius_m: float = 2000.0,
    radiance_scale: float = 1e9,
    token_env: str = "EARTHDATA_TOKEN",
    page_size: int = 200,
    skip_preprocess: bool = False,
    qa_mode: str = "",
    generate_gif: bool = True,
    gif_style_palette: str = "report_dark",
    overlay_vector: str = "",
    overlay_label_field: str = "",
    overlay_point_class_field: str = "",
    point_legend_label: str = "事件点",
    point_legend_title: str = "事件类型",
    show_point_legend: bool = True,
    boundary_vector: str = "",
    boundary_edge_color: str = "#3dd3ff",
    boundary_linewidth: float = 1.1,
    boundary_alpha: float = 0.95,
    gif_duration_ms: int = 900,
    gif_fps: float = 0.0,
    gif_percentile_min: float = 2.0,
    gif_percentile_max: float = 98.0,
    gif_cmap: str = "inferno",
    ask_user_for_params: bool = False,
    config: Optional[RunnableConfig] = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    Full chain pipeline for official VJ102/VJ103:
    1) Query download JSON by bbox+date
    2) Download VJ102DNB/VJ103DNB
    3) Precise preprocessing to daily GeoTIFF
    """
    if ask_user_for_params:
        return {
            "status": "need_user_input",
            "tool": "official_vj_dnb_fullchain_tool",
            "questions": [
                "请确认时空范围：start_date/end_date 与 bbox(minx,miny,maxx,maxy)。",
                "请确认数据源：默认 VJ102DNB,VJ103DNB 是否保持不变？",
                "预处理参数是否使用默认：composite=mean, resolution_m=500, radius_m=2000？",
                "是否生成 GIF？如是，请选择 gif_style_palette（report_dark/night_blue/mono_gray/impact_hot/white_viridis）。",
                "是否叠加事件点与行政区边界？如是请提供 overlay_vector 与 boundary_vector 路径。",
            ],
            "recommended_defaults": {
                "sources": "VJ102DNB,VJ103DNB",
                "composite": "mean",
                "resolution_m": 500.0,
                "radius_m": 2000.0,
                "generate_gif": True,
                "gif_style_palette": "report_dark",
            },
        }

    start_date = _ensure_date(start_date, "start_date")
    end_date = _ensure_date(end_date, "end_date")
    if end_date < start_date:
        raise ValueError("end_date must be >= start_date")
    if composite not in {"mean", "max"}:
        raise ValueError("composite must be mean or max")
    qa_mode = str(qa_mode or "").strip().lower()
    if qa_mode not in {"", "balanced", "strict", "clear_only"}:
        raise ValueError("qa_mode must be empty, balanced, strict, or clear_only")

    pipeline_mode, resolved_sources = _resolve_pipeline_mode(sources)

    thread_id = _resolve_thread_id_from_config(config)
    label = _build_run_label(start_date, end_date, run_label)
    root, path_mode = _resolve_output_root(output_root, thread_id=thread_id)
    run_dir = root / label
    query_dir = run_dir / "query"
    raw_dir = run_dir / "raw_nc"
    processed_dir = run_dir / "processed_tif"
    gridded_workspace = run_dir / "gridded_workspace"
    run_dir.mkdir(parents=True, exist_ok=True)
    # Deterministic rerun behavior for fixed run_label: avoid stale files from older runs.
    _reset_dir(query_dir)
    _reset_dir(raw_dir)
    _reset_dir(processed_dir)
    _reset_dir(gridded_workspace)

    query_json = query_dir / f"LAADS_query_{start_date}_{end_date}.json"

    _ = kwargs  # reserved for forward-compatible optional fields
    env = os.environ.copy()
    token_present = bool(env.get(token_env, "").strip())
    query_result: Optional[dict[str, Any]] = None
    download_result: Optional[dict[str, Any]] = None
    preprocess_result: Optional[dict[str, Any]] = None
    preprocess_fallback: Optional[dict[str, Any]] = None
    daily_outputs: list[str] = []
    gif_input_dir: Optional[Path] = None

    if pipeline_mode == "swath_precise":
        query_cmd = [
            sys.executable,
            str(QUERY_SCRIPT),
            "--start-date",
            start_date,
            "--end-date",
            end_date,
            "--bbox",
            bbox,
            "--sources",
            ",".join(resolved_sources),
            "--output",
            str(query_json),
            "--page-size",
            str(int(page_size)),
        ]
        query_result = _run_cmd(query_cmd, REPO_ROOT)
        if query_result["returncode"] != 0:
            raise RuntimeError(
                "Query step failed.\n"
                f"cmd={' '.join(query_cmd)}\n"
                f"stderr={query_result['stderr'][-1200:]}\n"
                f"stdout={query_result['stdout'][-1200:]}"
            )

        download_cmd = [
            sys.executable,
            str(DOWNLOAD_SCRIPT),
            "--input",
            str(query_json),
            "--output",
            str(raw_dir),
            "--token-env",
            token_env,
        ]
        download_result = _run_cmd(download_cmd, REPO_ROOT)
        if download_result["returncode"] != 0:
            raise RuntimeError(
                "Download step failed.\n"
                f"cmd={' '.join(download_cmd)}\n"
                f"stderr={download_result['stderr'][-1600:]}\n"
                f"stdout={download_result['stdout'][-1600:]}"
            )
        if not token_present:
            token_present = "token : configured" in (download_result.get("stdout", "").lower())

        if not skip_preprocess:
            preprocess_cmd = [
                sys.executable,
                str(PREPROCESS_SCRIPT),
                "--input-dir",
                str(raw_dir),
                "--output-dir",
                str(processed_dir),
                "--start-date",
                start_date,
                "--end-date",
                end_date,
                "--bbox",
                bbox,
                "--composite",
                composite,
                "--resolution-m",
                str(float(resolution_m)),
                "--radius-m",
                str(float(radius_m)),
                "--radiance-scale",
                str(float(radiance_scale)),
            ]
            preprocess_result = _run_cmd(preprocess_cmd, REPO_ROOT)
            if preprocess_result["returncode"] != 0:
                raise RuntimeError(
                    "Preprocess step failed.\n"
                    f"cmd={' '.join(preprocess_cmd)}\n"
                    f"stderr={preprocess_result['stderr'][-1600:]}\n"
                    f"stdout={preprocess_result['stdout'][-1600:]}"
                )
            summary_path = processed_dir / "precise_preprocess_summary.json"
            daily_valid_ratios = _load_daily_valid_ratios(summary_path)
            all_empty = bool(daily_valid_ratios) and all(v <= 0.0 for v in daily_valid_ratios)
            if all_empty:
                preprocess_cmd_fallback = preprocess_cmd + ["--disable-lunar-mask"]
                preprocess_fallback = _run_cmd(preprocess_cmd_fallback, REPO_ROOT)
                if preprocess_fallback["returncode"] != 0:
                    raise RuntimeError(
                        "Preprocess fallback step failed.\n"
                        f"cmd={' '.join(preprocess_cmd_fallback)}\n"
                        f"stderr={preprocess_fallback['stderr'][-1600:]}\n"
                        f"stdout={preprocess_fallback['stdout'][-1600:]}"
                    )

        if processed_dir.exists():
            gif_input_dir = processed_dir / "daily_4326"
            daily_outputs = sorted(str(p) for p in gif_input_dir.glob("*.tif"))
    else:
        skip_preprocess = False
        query_result = {
            "cmd": [],
            "returncode": 0,
            "stdout": "",
            "stderr": "",
            "notes": "gridded pipeline downloads and clips directly; no LAADS JSON query step",
        }
        download_cmd = [
            sys.executable,
            str(GRIDDED_DOWNLOAD_SCRIPT),
            "--sources",
            ",".join(resolved_sources),
            "--start-date",
            start_date,
            "--end-date",
            end_date,
            "--bbox",
            bbox,
            "--format",
            "clipped_tif",
            "--workspace",
            str(gridded_workspace),
            "--earthdata-token-env",
            token_env,
        ]
        if qa_mode:
            download_cmd += ["--qa-mode", qa_mode]
        download_result = _run_cmd(download_cmd, REPO_ROOT)
        if download_result["returncode"] != 0:
            raise RuntimeError(
                "Gridded download step failed.\n"
                f"cmd={' '.join(download_cmd)}\n"
                f"stderr={download_result['stderr'][-1600:]}\n"
                f"stdout={download_result['stdout'][-1600:]}"
            )
        manifest_path = gridded_workspace / "outputs" / "official_download_manifest.json"
        preprocess_result = {
            "cmd": download_cmd,
            "returncode": int(download_result["returncode"]),
            "stdout": download_result["stdout"],
            "stderr": download_result["stderr"],
            "notes": "gridded clipped_tif workflow handled by download_official_ntl_by_bbox.py",
            "manifest": str(manifest_path),
        }
        if len(resolved_sources) == 1:
            gif_input_dir = gridded_workspace / "outputs" / resolved_sources[0]
            daily_outputs = sorted(str(p) for p in gif_input_dir.glob("*/*.tif"))
        else:
            for source_name in resolved_sources:
                source_files = sorted(str(p) for p in (gridded_workspace / "outputs" / source_name).glob("*/*.tif"))
                daily_outputs.extend(source_files)
        token_present = token_present or bool(env.get(token_env, "").strip())

    gif_result: Optional[dict[str, Any]] = None
    gif_path: Optional[str] = None
    if generate_gif and daily_outputs and gif_input_dir is not None:
        gif_dir = run_dir / "visualization"
        gif_dir.mkdir(parents=True, exist_ok=True)
        gif_cmd = [
            sys.executable,
            str(GIF_SCRIPT),
            "--input-dir",
            str(gif_input_dir),
            "--output-dir",
            str(gif_dir),
            "--pattern",
            "*.tif",
            "--style-palette",
            str(gif_style_palette),
            "--duration-ms",
            str(int(gif_duration_ms)),
            "--fps",
            str(float(gif_fps)),
            "--percentile-min",
            str(float(gif_percentile_min)),
            "--percentile-max",
            str(float(gif_percentile_max)),
            "--cmap",
            str(gif_cmap),
            "--title-prefix",
            f"{'+'.join(resolved_sources)} ({start_date}..{end_date})",
        ]
        if str(overlay_vector).strip():
            resolved_overlay = _resolve_read_path(overlay_vector, thread_id=thread_id)
            if not resolved_overlay.exists():
                raise FileNotFoundError(f"overlay_vector not found: {resolved_overlay}")
            gif_cmd += ["--vector", str(resolved_overlay)]
            if str(overlay_label_field).strip():
                gif_cmd += ["--label-field", str(overlay_label_field).strip()]
        if str(overlay_point_class_field).strip():
            gif_cmd += ["--point-class-field", str(overlay_point_class_field).strip()]
        if str(point_legend_label).strip():
            gif_cmd += ["--point-legend-label", str(point_legend_label).strip()]
        if str(point_legend_title).strip():
            gif_cmd += ["--point-legend-title", str(point_legend_title).strip()]
        if bool(show_point_legend):
            gif_cmd += ["--show-point-legend"]
        if str(boundary_vector).strip():
            resolved_boundary = _resolve_read_path(boundary_vector, thread_id=thread_id)
            if not resolved_boundary.exists():
                raise FileNotFoundError(f"boundary_vector not found: {resolved_boundary}")
            gif_cmd += [
                "--boundary-vector",
                str(resolved_boundary),
                "--boundary-edge-color",
                str(boundary_edge_color),
                "--boundary-linewidth",
                str(float(boundary_linewidth)),
                "--boundary-alpha",
                str(float(boundary_alpha)),
            ]
        gif_result = _run_cmd(gif_cmd, REPO_ROOT)
        if gif_result["returncode"] != 0:
            raise RuntimeError(
                "GIF step failed.\n"
                f"cmd={' '.join(gif_cmd)}\n"
                f"stderr={gif_result['stderr'][-1600:]}\n"
                f"stdout={gif_result['stdout'][-1600:]}"
            )
        gif_summary = gif_dir / "gif_summary.json"
        if gif_summary.exists():
            try:
                gif_path = json.loads(gif_summary.read_text(encoding="utf-8")).get("gif_path")
            except Exception:
                gif_path = None
        if not gif_path:
            gif_path = str(gif_dir / "ntl_daily_animation.gif")

    download_manifest_path = raw_dir / "download_manifest.json"
    preprocess_summary_path = processed_dir / "precise_preprocess_summary.json"
    if pipeline_mode == "gridded_tile_clip":
        download_manifest_path = gridded_workspace / "outputs" / "official_download_manifest.json"
        preprocess_summary_path = gridded_workspace / "outputs" / "official_download_manifest.json"

    manifest = {
        "status": "success",
        "tool": "official_vj_dnb_fullchain_tool",
        "params": {
            "thread_id": thread_id,
            "start_date": start_date,
            "end_date": end_date,
            "bbox": bbox,
            "sources": sources,
            "resolved_sources": resolved_sources,
            "pipeline_mode": pipeline_mode,
            "composite": composite,
            "resolution_m": float(resolution_m),
            "radius_m": float(radius_m),
            "radiance_scale": float(radiance_scale),
            "token_env": token_env,
            "token_present": token_present,
            "skip_preprocess": bool(skip_preprocess),
            "qa_mode": qa_mode,
            "generate_gif": bool(generate_gif),
            "gif_style_palette": gif_style_palette,
            "overlay_vector": overlay_vector,
            "overlay_label_field": overlay_label_field,
            "overlay_point_class_field": overlay_point_class_field,
            "point_legend_label": point_legend_label,
            "point_legend_title": point_legend_title,
            "show_point_legend": bool(show_point_legend),
            "boundary_vector": boundary_vector,
            "boundary_edge_color": boundary_edge_color,
            "boundary_linewidth": float(boundary_linewidth),
            "boundary_alpha": float(boundary_alpha),
            "gif_duration_ms": int(gif_duration_ms),
            "gif_fps": float(gif_fps),
            "gif_percentile_min": float(gif_percentile_min),
            "gif_percentile_max": float(gif_percentile_max),
            "gif_cmap": gif_cmap,
            "ask_user_for_params": bool(ask_user_for_params),
            "path_mode": path_mode,
        },
        "paths": {
            "workspace": str(storage_manager.get_workspace(thread_id=thread_id)),
            "run_dir": str(run_dir),
            "query_json": str(query_json),
            "raw_dir": str(raw_dir),
            "processed_dir": str(processed_dir),
            "gridded_workspace": str(gridded_workspace),
            "download_manifest": str(download_manifest_path),
            "preprocess_summary": str(preprocess_summary_path),
            "gif_path": gif_path,
            "gif_summary": str(run_dir / "visualization" / "gif_summary.json"),
        },
        "output_files": daily_outputs,
        "steps": {
            "query": query_result,
            "download": download_result,
            "preprocess": preprocess_result,
            "preprocess_fallback": preprocess_fallback,
            "gif": gif_result,
        },
    }
    out_manifest = run_dir / "pipeline_manifest.json"
    out_manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    manifest["paths"]["pipeline_manifest"] = str(out_manifest)
    return manifest


official_vj_dnb_fullchain_tool = StructuredTool.from_function(
    func=run_official_vj_dnb_fullchain,
    name="official_vj_dnb_fullchain_tool",
    description=(
        "Official nighttime lights full chain pipeline. Supports VJ102DNB/VJ103DNB swath query+download+precise "
        "preprocess, and VJ146A1/VJ146A2 gridded clipped outputs with optional qa_mode and GIF rendering."
    ),
    args_schema=OfficialVJDNBPipelineInput,
)
