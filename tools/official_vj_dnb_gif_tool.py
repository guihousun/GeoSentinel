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
GIF_SCRIPT = REPO_ROOT / "tools" / "official_vj_dnb_map_renderer.py"


class OfficialVJDNBGifInput(BaseModel):
    input_dir: str = Field(..., description="Directory with daily GeoTIFF files (e.g., processed_tif/daily_4326).")
    output_root: str = Field(default="official_vj_dnb_gif_runs", description="Output subfolder under workspace outputs/.")
    run_label: str = Field(default="", description="Optional run label, auto-generated when empty.")
    tif_pattern: str = Field(default="*.tif", description="GeoTIFF filename glob pattern.")
    style_palette: str = Field(
        default="report_dark",
        description="Color style preset: report_dark | night_blue | mono_gray | impact_hot | white_viridis",
    )
    overlay_vector: str = Field(default="", description="Optional vector data path (GeoJSON/SHP/GPKG) for event points.")
    overlay_label_field: str = Field(default="", description="Optional field name used for overlay text labels.")
    overlay_point_class_field: str = Field(default="", description="Optional point class field for legend categories.")
    point_style_map: str = Field(
        default="",
        description="Optional JSON string or JSON file path mapping point classes to styles (marker/color/size/glow).",
    )
    point_legend_label: str = Field(default="事件点", description="Single-style point legend label.")
    point_legend_title: str = Field(default="事件类型", description="Point legend title when class field is used.")
    show_point_legend: bool = Field(default=True, description="Whether to display point legend.")
    boundary_vector: str = Field(default="", description="Optional boundary vector path (GeoJSON/SHP/GPKG).")
    boundary_edge_color: str = Field(default="#3dd3ff", description="Boundary line color.")
    boundary_linewidth: float = Field(default=1.1, description="Boundary line width.")
    boundary_alpha: float = Field(default=0.95, description="Boundary line alpha.")
    view_bbox: str = Field(default="", description="Optional display bbox: minx,miny,maxx,maxy.")
    point_size: float = Field(default=5.0, description="Point size.")
    point_color: str = Field(default="#ff3b30", description="Single-style point fill color.")
    point_edge: str = Field(default="#ffffff", description="Single-style point edge color.")
    duration_ms: int = Field(default=900, description="GIF frame duration in milliseconds.")
    fps: float = Field(default=0.0, description="Optional FPS; overrides duration_ms when >0.")
    percentile_min: float = Field(default=2.0, description="Visualization lower percentile.")
    percentile_max: float = Field(default=98.0, description="Visualization upper percentile.")
    cmap: str = Field(default="inferno", description="Matplotlib colormap name.")
    title_prefix: str = Field(default="Nighttime Light", description="Frame title prefix.")
    basemap_style: str = Field(default="dark", description="Basemap style: dark | light | none.")
    basemap_provider: str = Field(
        default="",
        description="Optional dotted xyzservices provider path, e.g. CartoDB.DarkMatter or Stadia.StamenTonerBlacklite.",
    )
    basemap_alpha: float = Field(default=0.92, description="Basemap alpha.")
    ntl_alpha: float = Field(default=0.65, description="Nighttime light raster alpha.")
    transparent_below: Optional[float] = Field(
        default=None,
        description="Optional threshold below which nighttime light pixels render transparent.",
    )
    classification_mode: str = Field(
        default="continuous",
        description="Classification mode: continuous | equal_interval | quantile | stddev.",
    )
    class_bins: int = Field(default=8, description="Class bin count for classified rendering.")
    stddev_range: float = Field(default=2.0, description="Stddev range used by stddev classification.")
    show_colorbar: bool = Field(default=True, description="Whether to render colorbar.")
    ask_user_for_params: bool = Field(
        default=False,
        description="If true, do not execute; return parameter questions so agent can ask user first.",
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


def _is_virtual(path_text: str) -> bool:
    return str(path_text or "").startswith(("/data/raw/", "/data/processed/", "/memories/", "/shared/"))


def _resolve_workspace_path(path_text: str, thread_id: str, *, writable: bool) -> Path:
    workspace = storage_manager.get_workspace(thread_id=thread_id)
    outputs_root = (workspace / "outputs").resolve()
    raw = (path_text or "").strip()
    if not raw:
        raise ValueError("Path is required.")
    if _is_virtual(raw):
        if writable and raw.startswith("/shared/"):
            raise PermissionError("Write path under /shared/ is not allowed.")
        return Path(storage_manager.resolve_deepagents_path(raw, thread_id=thread_id))
    p = Path(raw)
    if p.is_absolute():
        raise ValueError("Path must be workspace-relative (absolute path not allowed).")
    if ".." in p.parts:
        raise ValueError("Path must not contain '..'.")
    if writable:
        target = (workspace / p).resolve() if (p.parts and p.parts[0] == "outputs") else (outputs_root / p).resolve()
        if not str(target).startswith(str(outputs_root)):
            raise PermissionError("Writable path resolved outside workspace outputs root.")
        return target
    direct = (workspace / p).resolve()
    if direct.exists():
        return direct
    return (outputs_root / p).resolve()


def _run(cmd: list[str], cwd: Path) -> dict[str, Any]:
    started = datetime.utcnow()
    proc = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True, encoding="utf-8", errors="ignore")
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


def _auto_label(run_label: str) -> str:
    if run_label.strip():
        return run_label.strip()
    return f"vj_dnb_gif_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"


def run_official_vj_dnb_gif(
    input_dir: str,
    output_root: str = "official_vj_dnb_gif_runs",
    run_label: str = "",
    tif_pattern: str = "*.tif",
    style_palette: str = "report_dark",
    overlay_vector: str = "",
    overlay_label_field: str = "",
    overlay_point_class_field: str = "",
    point_style_map: str = "",
    point_legend_label: str = "事件点",
    point_legend_title: str = "事件类型",
    show_point_legend: bool = True,
    boundary_vector: str = "",
    boundary_edge_color: str = "#3dd3ff",
    boundary_linewidth: float = 1.1,
    boundary_alpha: float = 0.95,
    view_bbox: str = "",
    point_size: float = 5.0,
    point_color: str = "#ff3b30",
    point_edge: str = "#ffffff",
    duration_ms: int = 900,
    fps: float = 0.0,
    percentile_min: float = 2.0,
    percentile_max: float = 98.0,
    cmap: str = "inferno",
    title_prefix: str = "Nighttime Light",
    basemap_style: str = "dark",
    basemap_provider: str = "",
    basemap_alpha: float = 0.92,
    ntl_alpha: float = 0.65,
    transparent_below: Optional[float] = None,
    classification_mode: str = "continuous",
    class_bins: int = 8,
    stddev_range: float = 2.0,
    show_colorbar: bool = True,
    ask_user_for_params: bool = False,
    config: Optional[RunnableConfig] = None,
    **kwargs,
) -> Dict[str, Any]:
    if ask_user_for_params:
        return {
            "status": "need_user_input",
            "tool": "official_vj_dnb_gif_tool",
            "questions": [
                "请确认 style_palette：report_dark / night_blue / mono_gray / impact_hot。",
                "如需白底 viridis 专题图，可选择 style_palette=white_viridis。",
                "是否叠加事件点？如是，请提供 overlay_vector 路径与 overlay_label_field。",
                "点图例是否按字段分类？如是，请提供 overlay_point_class_field。",
                "如需差异化符号，请提供 point_style_map（JSON 文本或 JSON 文件路径）。",
                "是否叠加行政区边界？如是，请提供 boundary_vector。",
                "是否需要指定 view_bbox、basemap_style/provider、transparent_below？",
                "渲染偏好：classification_mode、class_bins、cmap、ntl_alpha、show_colorbar。",
                "动画速度偏好：duration_ms（每帧毫秒）或 fps（二选一）。",
            ],
            "recommended_defaults": {
                "style_palette": "report_dark",
                "show_point_legend": True,
                "basemap_style": "dark",
                "ntl_alpha": 0.65,
                "duration_ms": 900,
                "percentile_min": 2.0,
                "percentile_max": 98.0,
            },
        }

    thread_id = _resolve_thread_id_from_config(config)
    resolved_input = _resolve_workspace_path(input_dir, thread_id, writable=False)
    if not resolved_input.exists():
        raise FileNotFoundError(f"input_dir not found: {resolved_input}")
    out_root = _resolve_workspace_path(output_root, thread_id, writable=True)
    label = _auto_label(run_label)
    run_dir = out_root / label
    run_dir.mkdir(parents=True, exist_ok=True)

    _ = kwargs
    cmd = [
        sys.executable,
        str(GIF_SCRIPT),
        "--input-dir",
        str(resolved_input),
        "--output-dir",
        str(run_dir),
        "--pattern",
        str(tif_pattern),
        "--style-palette",
        str(style_palette),
        "--duration-ms",
        str(int(duration_ms)),
        "--fps",
        str(float(fps)),
        "--percentile-min",
        str(float(percentile_min)),
        "--percentile-max",
        str(float(percentile_max)),
        "--cmap",
        str(cmap),
        "--title-prefix",
        str(title_prefix),
        "--point-size",
        str(float(point_size)),
        "--point-color",
        str(point_color),
        "--point-edge",
        str(point_edge),
        "--basemap-style",
        str(basemap_style),
        "--basemap-provider",
        str(basemap_provider),
        "--basemap-alpha",
        str(float(basemap_alpha)),
        "--ntl-alpha",
        str(float(ntl_alpha)),
        "--classification-mode",
        str(classification_mode),
        "--class-bins",
        str(int(class_bins)),
        "--stddev-range",
        str(float(stddev_range)),
    ]
    if not bool(show_colorbar):
        cmd += ["--no-colorbar"]
    if transparent_below is not None:
        cmd += ["--transparent-below", str(float(transparent_below))]
    if str(overlay_vector).strip():
        resolved_vector = _resolve_workspace_path(overlay_vector, thread_id, writable=False)
        if not resolved_vector.exists():
            raise FileNotFoundError(f"overlay_vector not found: {resolved_vector}")
        cmd += ["--vector", str(resolved_vector)]
        if str(overlay_label_field).strip():
            cmd += ["--label-field", str(overlay_label_field).strip()]
    if str(overlay_point_class_field).strip():
        cmd += ["--point-class-field", str(overlay_point_class_field).strip()]
    if str(point_style_map).strip():
        resolved_style_map = _resolve_workspace_path(point_style_map, thread_id, writable=False)
        if resolved_style_map.exists():
            cmd += ["--point-style-map", str(resolved_style_map)]
        else:
            cmd += ["--point-style-map", str(point_style_map).strip()]
    if str(point_legend_label).strip():
        cmd += ["--point-legend-label", str(point_legend_label).strip()]
    if str(point_legend_title).strip():
        cmd += ["--point-legend-title", str(point_legend_title).strip()]
    if bool(show_point_legend):
        cmd += ["--show-point-legend"]
    if str(view_bbox).strip():
        cmd += ["--view-bbox", str(view_bbox).strip()]
    if str(boundary_vector).strip():
        resolved_boundary = _resolve_workspace_path(boundary_vector, thread_id, writable=False)
        if not resolved_boundary.exists():
            raise FileNotFoundError(f"boundary_vector not found: {resolved_boundary}")
        cmd += [
            "--boundary-vector",
            str(resolved_boundary),
            "--boundary-edge-color",
            str(boundary_edge_color),
            "--boundary-linewidth",
            str(float(boundary_linewidth)),
            "--boundary-alpha",
            str(float(boundary_alpha)),
        ]

    step = _run(cmd, REPO_ROOT)
    if step["returncode"] != 0:
        raise RuntimeError(
            "GIF render step failed.\n"
            f"cmd={' '.join(cmd)}\n"
            f"stderr={step['stderr'][-1600:]}\n"
            f"stdout={step['stdout'][-1600:]}"
        )

    summary_path = run_dir / "gif_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.exists() else {}
    return {
        "status": "success",
        "tool": "official_vj_dnb_gif_tool",
        "thread_id": thread_id,
        "input_dir": str(resolved_input),
        "run_dir": str(run_dir),
        "gif_path": summary.get("gif_path", str(run_dir / "ntl_daily_animation.gif")),
        "frame_count": summary.get("frame_count"),
        "summary_path": str(summary_path),
        "step": step,
    }


official_vj_dnb_gif_tool = StructuredTool.from_function(
    func=run_official_vj_dnb_gif,
    name="official_vj_dnb_gif_tool",
    description=(
        "Generate complete nighttime-light cartography and animated GIF from daily GeoTIFF series, "
        "supporting basemap provider selection, bbox view, transparency threshold, classified rendering, "
        "event/port overlays, custom point symbol mapping, and optional boundary overlay."
    ),
    args_schema=OfficialVJDNBGifInput,
)
