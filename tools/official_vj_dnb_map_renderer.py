from __future__ import annotations

import argparse
import json
import os
import tempfile
import traceback
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import matplotlib
import numpy as np
import rasterio
from PIL import Image


matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import BoundaryNorm
from matplotlib.lines import Line2D

_SAFE_CONTEXTILY_TMP = Path(
    os.getenv("NTL_CONTEXTILY_TMP") or (Path.cwd() / ".cache" / "contextily_joblib")
).resolve()
_SAFE_CONTEXTILY_TMP.mkdir(parents=True, exist_ok=True)
_SAFE_CONTEXTILY_FIXED_TMP = (_SAFE_CONTEXTILY_TMP / "contextily_fixed_tmp").resolve()
_SAFE_CONTEXTILY_FIXED_TMP.mkdir(parents=True, exist_ok=True)
for _key in ("JOBLIB_TEMP_FOLDER", "TMPDIR", "TMP", "TEMP"):
    os.environ.setdefault(_key, str(_SAFE_CONTEXTILY_TMP))
tempfile.tempdir = str(_SAFE_CONTEXTILY_TMP)


def _safe_mkdtemp(*args, **kwargs) -> str:
    _SAFE_CONTEXTILY_FIXED_TMP.mkdir(parents=True, exist_ok=True)
    return str(_SAFE_CONTEXTILY_FIXED_TMP)


tempfile.mkdtemp = _safe_mkdtemp

try:
    import contextily as ctx
    CONTEXTILY_IMPORT_ERROR = ""
except Exception:  # noqa: BLE001
    ctx = None
    CONTEXTILY_IMPORT_ERROR = traceback.format_exc()


plt.rcParams["font.sans-serif"] = [
    "Microsoft YaHei",
    "PingFang SC",
    "Noto Sans CJK SC",
    "WenQuanYi Micro Hei",
    "DejaVu Sans",
]
plt.rcParams["axes.unicode_minus"] = False


DEFAULT_BASEMAP_ALPHA = 0.92
DEFAULT_NTL_ALPHA = 0.65
STRIKE_POINT_COLOR = "#ff3b30"
STRIKE_POINT_EDGE = "#ffffff"
FALLBACK_BG_DARK = "#1b1f27"
FALLBACK_BG_LIGHT = "#f5f7fa"


PALETTES: dict[str, dict[str, str]] = {
    "report_dark": {
        "cmap": "inferno",
        "point_color": "#ff3b30",
        "point_edge": "#ffffff",
        "boundary_edge_color": "#00ffff",
    },
    "night_blue": {
        "cmap": "magma",
        "point_color": "#4fd1ff",
        "point_edge": "#e6f7ff",
        "boundary_edge_color": "#7dd3fc",
    },
    "mono_gray": {
        "cmap": "gray",
        "point_color": "#ffd166",
        "point_edge": "#111111",
        "boundary_edge_color": "#444444",
    },
    "impact_hot": {
        "cmap": "plasma",
        "point_color": "#ef4444",
        "point_edge": "#ffffff",
        "boundary_edge_color": "#22d3ee",
    },
    "white_viridis": {
        "cmap": "viridis",
        "point_color": "#0f172a",
        "point_edge": "#ffffff",
        "boundary_edge_color": "#4b5563",
    },
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Render daily NTL GeoTIFF series to animated GIF with optional point/boundary overlays."
    )
    p.add_argument("--input-dir", required=True, help="Folder containing daily GeoTIFFs")
    p.add_argument("--output-dir", required=True, help="Output folder")
    p.add_argument("--pattern", default="*.tif", help="Input tif filename glob pattern")

    p.add_argument("--vector", default="", help="Optional point vector path (GeoJSON/SHP/GPKG)")
    p.add_argument("--label-field", default="", help="Optional point field used as text label")
    p.add_argument("--point-class-field", default="", help="Optional point field used for category legend")
    p.add_argument("--point-legend-label", default="Event points", help="Legend label for single-style points")
    p.add_argument("--point-legend-title", default="Event class", help="Legend title when class field is used")
    p.add_argument("--show-point-legend", action="store_true", help="Show point legend")
    p.add_argument(
        "--point-style-map",
        default="",
        help="Optional JSON string or JSON file path mapping class values to point styles",
    )

    p.add_argument("--boundary-vector", default="", help="Optional boundary polygon/line vector path (GeoJSON/SHP/GPKG)")
    p.add_argument("--boundary-edge-color", default="#00ffff", help="Boundary line color")
    p.add_argument("--boundary-linewidth", type=float, default=1.1, help="Boundary line width")
    p.add_argument("--boundary-alpha", type=float, default=0.95, help="Boundary line alpha")

    p.add_argument("--duration-ms", type=int, default=900, help="GIF frame duration milliseconds")
    p.add_argument("--fps", type=float, default=0.0, help="Optional FPS; overrides duration when >0")
    p.add_argument(
        "--style-palette",
        default="report_dark",
        choices=sorted(PALETTES.keys()),
        help="Preset style palette (controls cmap + point/boundary colors)",
    )
    p.add_argument("--percentile-min", type=float, default=2.0, help="Visualization lower percentile")
    p.add_argument("--percentile-max", type=float, default=98.0, help="Visualization upper percentile")
    p.add_argument("--cmap", default="", help="Matplotlib colormap name (optional override)")
    p.add_argument("--title-prefix", default="Nighttime Light", help="Title prefix")
    p.add_argument("--point-size", type=float, default=5.0, help="Point size")
    p.add_argument("--point-color", default=STRIKE_POINT_COLOR, help="Single-style point color (optional override)")
    p.add_argument("--point-edge", default=STRIKE_POINT_EDGE, help="Single-style point edge color (optional override)")
    p.add_argument("--no-colorbar", action="store_true", help="Disable colorbar")
    p.add_argument("--view-bbox", default="", help="Optional display bbox: minx,miny,maxx,maxy (zoom only)")

    p.add_argument(
        "--basemap-style",
        default="dark",
        choices=["dark", "light", "none"],
        help="Basemap style under NTL layer",
    )
    p.add_argument(
        "--basemap-provider",
        default="",
        help="Optional dotted xyzservices provider path, e.g. Stadia.StamenTonerBlacklite",
    )
    p.add_argument("--basemap-alpha", type=float, default=DEFAULT_BASEMAP_ALPHA, help="Basemap alpha")
    p.add_argument("--ntl-alpha", type=float, default=DEFAULT_NTL_ALPHA, help="NTL raster alpha")
    p.add_argument(
        "--transparent-below",
        type=float,
        default=float("-inf"),
        help="Mask NTL pixels below this value so they render transparent",
    )

    p.add_argument(
        "--classification-mode",
        default="continuous",
        choices=["continuous", "equal_interval", "quantile", "stddev"],
        help="Classification mode for rendering",
    )
    p.add_argument("--class-bins", type=int, default=8, help="Class bin count for classified modes")
    p.add_argument("--stddev-range", type=float, default=2.0, help="Stddev range for stddev mode: mean ± N*std")
    return p.parse_args()


def _parse_bbox(raw: str) -> tuple[float, float, float, float] | None:
    txt = (raw or "").strip()
    if not txt:
        return None
    vals = [float(x.strip()) for x in txt.split(",")]
    if len(vals) != 4:
        raise ValueError("view_bbox must be minx,miny,maxx,maxy")
    minx, miny, maxx, maxy = vals
    if maxx <= minx or maxy <= miny:
        raise ValueError("view_bbox invalid: max must be greater than min")
    return (minx, miny, maxx, maxy)


def _load_vector(path: Path):
    try:
        import geopandas as gpd
    except Exception as exc:  # pragma: no cover - dependency issue path
        raise RuntimeError("geopandas is required for vector overlay") from exc
    gdf = gpd.read_file(path)
    if gdf.empty:
        return gdf
    gdf = gdf[gdf.geometry.notnull()].copy()
    return gdf


def _load_json_arg(raw: str) -> dict[str, Any]:
    txt = (raw or "").strip()
    if not txt:
        return {}
    p = Path(txt)
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return json.loads(txt)


def _array_limits(sample_values: np.ndarray, pmin: float, pmax: float) -> tuple[float, float]:
    if sample_values.size == 0:
        return 0.0, 1.0
    lo = float(np.nanpercentile(sample_values, pmin))
    hi = float(np.nanpercentile(sample_values, pmax))
    if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
        lo = float(np.nanmin(sample_values))
        hi = float(np.nanmax(sample_values))
    if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
        return 0.0, 1.0
    return lo, hi


def _build_class_breaks(
    values: np.ndarray,
    vmin: float,
    vmax: float,
    mode: str,
    bins: int,
    stddev_range: float,
) -> np.ndarray | None:
    if mode == "continuous":
        return None

    bins = max(2, int(bins))
    valid = values[np.isfinite(values)]
    if valid.size == 0:
        return np.linspace(vmin, vmax, bins + 1, dtype=np.float64)

    if mode == "equal_interval":
        edges = np.linspace(vmin, vmax, bins + 1, dtype=np.float64)
    elif mode == "quantile":
        q = np.linspace(0.0, 100.0, bins + 1, dtype=np.float64)
        edges = np.nanpercentile(valid, q)
    else:  # stddev
        mu = float(np.nanmean(valid))
        sigma = float(np.nanstd(valid))
        if not np.isfinite(sigma) or sigma <= 0:
            edges = np.linspace(vmin, vmax, bins + 1, dtype=np.float64)
        else:
            low = max(vmin, mu - stddev_range * sigma)
            high = min(vmax, mu + stddev_range * sigma)
            if high <= low:
                edges = np.linspace(vmin, vmax, bins + 1, dtype=np.float64)
            else:
                edges = np.linspace(low, high, bins + 1, dtype=np.float64)

    edges = np.asarray(edges, dtype=np.float64)
    edges = edges[np.isfinite(edges)]
    edges = np.unique(edges)
    if edges.size < 2:
        edges = np.array([vmin, vmax], dtype=np.float64)
    if edges[-1] <= edges[0]:
        edges = np.array([edges[0], edges[0] + 1.0], dtype=np.float64)
    return edges


def _local_figsize_from_extent(
    extent: tuple[float, float, float, float],
    long_side: float = 7.4,
    min_side: float = 5.2,
) -> tuple[float, float]:
    left, right, bottom, top = extent
    w = max(abs(float(right) - float(left)), 1e-9)
    h = max(abs(float(top) - float(bottom)), 1e-9)
    ratio = w / h
    if ratio >= 1.0:
        return long_side, max(min_side, long_side / ratio)
    return max(min_side, long_side * ratio), long_side


def _resolve_provider_path(provider_path: str):
    if not provider_path:
        return None
    if ctx is None:
        return None
    current = ctx.providers
    for part in provider_path.split("."):
        if not hasattr(current, part):
            raise ValueError(f"Unknown basemap provider path: {provider_path}")
        current = getattr(current, part)
    return current


def _basemap_source(style: str, provider_path: str = ""):
    custom = _resolve_provider_path(provider_path.strip())
    if custom is not None:
        return custom
    if style == "light":
        return ctx.providers.CartoDB.Positron
    if style == "dark":
        return ctx.providers.CartoDB.DarkMatter
    return None


def _add_basemap(ax, axis_crs: str | None, style: str, alpha: float, provider_path: str = "") -> tuple[bool, str | None]:
    if style == "none":
        return False, "basemap disabled"
    if ctx is None or axis_crs is None:
        return False, "contextily unavailable or raster CRS missing"
    src = _basemap_source(style, provider_path)
    if src is None:
        return False, "basemap source unresolved"
    try:
        xlim = ax.get_xlim()
        ylim = ax.get_ylim()
        ctx.add_basemap(
            ax,
            source=src,
            crs=axis_crs,
            attribution=False,
            alpha=alpha,
            zorder=0,
        )
        ax.set_xlim(xlim)
        ax.set_ylim(ylim)
        return True, None
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _render_frame(
    tif_path: Path,
    frame_path: Path,
    vmin: float,
    vmax: float,
    cmap: str,
    norm: BoundaryNorm | None,
    point_gdf,
    boundary_gdf,
    label_field: str,
    point_class_field: str,
    point_style_map: dict[str, Any],
    point_legend_label: str,
    point_legend_title: str,
    show_point_legend: bool,
    boundary_edge_color: str,
    boundary_linewidth: float,
    boundary_alpha: float,
    title_prefix: str,
    show_colorbar: bool,
    point_size: float,
    point_color: str,
    point_edge: str,
    view_bbox: tuple[float, float, float, float] | None,
    basemap_style: str,
    basemap_provider: str,
    basemap_alpha: float,
    ntl_alpha: float,
    transparent_below: float,
) -> dict[str, Any]:
    with rasterio.open(tif_path) as ds:
        arr = ds.read(1).astype(np.float32)
        nodata = ds.nodata
        if nodata is not None:
            arr = np.where(arr == nodata, np.nan, arr)
        arr[~np.isfinite(arr)] = np.nan
        arr[arr < 0] = np.nan
        if np.isfinite(transparent_below):
            arr[arr < float(transparent_below)] = np.nan

        extent = (ds.bounds.left, ds.bounds.right, ds.bounds.bottom, ds.bounds.top)
        axis_crs = str(ds.crs) if ds.crs else None
        fig, ax = plt.subplots(figsize=_local_figsize_from_extent(extent))
        cmap_obj = plt.get_cmap(cmap).copy()
        cmap_obj.set_bad((0.0, 0.0, 0.0, 0.0))

        im = ax.imshow(
            arr,
            extent=extent,
            origin="upper",
            cmap=cmap_obj,
            vmin=None if norm is not None else vmin,
            vmax=None if norm is not None else vmax,
            norm=norm,
            interpolation="nearest",
            alpha=max(0.0, min(1.0, float(ntl_alpha))),
            zorder=2,
        )

        basemap_ok, basemap_error = _add_basemap(ax, axis_crs, basemap_style, basemap_alpha, basemap_provider)
        if not basemap_ok:
            ax.set_facecolor(FALLBACK_BG_LIGHT if basemap_style == "light" else FALLBACK_BG_DARK)

        overlay_boundary_count = 0
        legend_handles: list[Line2D] = []
        boundary_label = "Boundary"
        if boundary_gdf is not None and len(boundary_gdf) > 0:
            bg = boundary_gdf
            if ds.crs and bg.crs and str(bg.crs) != str(ds.crs):
                bg = bg.to_crs(ds.crs)
            elif ds.crs and bg.crs is None:
                bg = bg.set_crs(ds.crs, allow_override=True)
            bg.boundary.plot(
                ax=ax,
                edgecolor=boundary_edge_color,
                linewidth=boundary_linewidth,
                alpha=boundary_alpha,
                zorder=7,
            )
            overlay_boundary_count = int(len(bg))
            legend_handles.append(Line2D([0], [0], color=boundary_edge_color, lw=1.4, label=boundary_label))

        overlay_point_count = 0
        if point_gdf is not None and len(point_gdf) > 0:
            pg = point_gdf
            if ds.crs and pg.crs and str(pg.crs) != str(ds.crs):
                pg = pg.to_crs(ds.crs)
            elif ds.crs and pg.crs is None:
                pg = pg.set_crs(ds.crs, allow_override=True)

            geom = pg.geometry
            is_point = geom.geom_type.isin(["Point", "MultiPoint"])
            pts = pg[is_point].copy()
            others = pg[~is_point].copy()
            if len(others) > 0:
                cent = others.copy()
                cent["geometry"] = cent.geometry.centroid
                pts = pts if len(pts) == 0 else pts.copy()
                pts = pts._append(cent, ignore_index=True)

            if len(pts) > 0:
                size = max(float(point_size), 5.0)
                if point_class_field and point_class_field in pts.columns:
                    classes = pts[point_class_field].fillna("Unknown").astype(str)
                    uniq = sorted(set(classes.tolist()))
                    for i, cls in enumerate(uniq):
                        part = pts[classes == cls]
                        style = dict((point_style_map or {}).get(cls, {}))
                        default_face = plt.get_cmap("tab10")(i % 10)
                        marker = str(style.get("marker", "o"))
                        face = style.get("facecolor", style.get("color", default_face))
                        edge = str(style.get("edgecolor", point_edge))
                        line_w = float(style.get("linewidth", 0.7))
                        alpha = float(style.get("alpha", 0.98))
                        size_cls = float(style.get("size", size))
                        glow_size = float(style.get("glow_size", 0.0))
                        glow_alpha = float(style.get("glow_alpha", 0.28))
                        glow_color = style.get("glow_color", face)
                        label_name = str(style.get("label", cls))
                        label_color = str(style.get("label_color", face))
                        if glow_size > 0:
                            ax.scatter(
                                part.geometry.x,
                                part.geometry.y,
                                s=glow_size,
                                c=glow_color,
                                edgecolors="none",
                                alpha=glow_alpha,
                                zorder=7.7,
                            )
                        ax.scatter(
                            part.geometry.x,
                            part.geometry.y,
                            s=size_cls,
                            c=face,
                            marker=marker,
                            edgecolors=edge,
                            linewidths=line_w,
                            alpha=alpha,
                            zorder=8,
                        )
                        legend_handles.append(
                            Line2D(
                                [0],
                                [0],
                                marker=marker,
                                color="none",
                                markerfacecolor=face,
                                markeredgecolor=edge,
                                markersize=5,
                                label=label_name,
                            )
                        )
                else:
                    ax.scatter(
                        pts.geometry.x,
                        pts.geometry.y,
                        s=size,
                        c=point_color,
                        edgecolors=point_edge,
                        linewidths=0.7,
                        alpha=0.98,
                        zorder=8,
                    )
                    legend_handles.append(
                        Line2D(
                            [0],
                            [0],
                            marker="o",
                            color="none",
                            markerfacecolor=point_color,
                            markeredgecolor=point_edge,
                            markersize=5,
                            label=point_legend_label,
                        )
                    )

                if label_field and label_field in pts.columns:
                    for _, r in pts.iterrows():
                        txt = str(r.get(label_field, "")).strip()
                        if txt:
                            ax.text(
                                r.geometry.x,
                                r.geometry.y,
                                txt,
                                fontsize=7,
                                color=(
                                    str(
                                        (point_style_map or {})
                                        .get(str(r.get(point_class_field, "")).strip(), {})
                                        .get("label_color", point_color)
                                    )
                                    if point_class_field and point_class_field in pts.columns
                                    else point_color
                                ),
                                ha="left",
                                va="bottom",
                                zorder=9,
                            )
                overlay_point_count = int(len(pts))

        if view_bbox is None:
            ax.set_xlim(extent[0], extent[1])
            ax.set_ylim(extent[2], extent[3])
        else:
            ax.set_xlim(view_bbox[0], view_bbox[2])
            ax.set_ylim(view_bbox[1], view_bbox[3])

        ax.set_aspect("equal", adjustable="box")
        ax.margins(0)
        ax.tick_params(labelsize=10)
        ax.set_xlabel("")
        ax.set_ylabel("")
        ax.set_title(f"{title_prefix} | {tif_path.stem}", fontsize=12)

        if show_colorbar:
            cbar = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
            cbar.set_label("Nighttime radiance (nW/cm^2/sr)", fontsize=10)
            cbar.ax.tick_params(labelsize=9)

        if show_point_legend and legend_handles:
            if point_class_field and any(
                h.get_label() not in (boundary_label, point_legend_label)
                for h in legend_handles
            ):
                ax.legend(
                    handles=legend_handles,
                    loc="lower right",
                    fontsize=9,
                    framealpha=0.9,
                    title=point_legend_title,
                )
            else:
                ax.legend(handles=legend_handles, loc="lower right", fontsize=9, framealpha=0.9)

        fig.tight_layout()
        fig.savefig(frame_path, dpi=150)
        plt.close(fig)

    finite = arr[np.isfinite(arr)]
    return {
        "tif": str(tif_path),
        "frame_png": str(frame_path),
        "valid_pixel_count": int(finite.size),
        "min": float(np.nanmin(finite)) if finite.size else None,
        "max": float(np.nanmax(finite)) if finite.size else None,
        "mean": float(np.nanmean(finite)) if finite.size else None,
            "overlay_feature_count": overlay_point_count,
            "overlay_boundary_count": overlay_boundary_count,
            "basemap_loaded": bool(basemap_ok),
            "basemap_error": basemap_error,
        }


def main() -> None:
    args = parse_args()
    in_dir = Path(args.input_dir).resolve()
    out_dir = Path(args.output_dir).resolve()
    frames_dir = out_dir / "frames"
    out_dir.mkdir(parents=True, exist_ok=True)
    frames_dir.mkdir(parents=True, exist_ok=True)

    tifs = sorted(in_dir.glob(args.pattern))
    if not tifs:
        raise FileNotFoundError(f"No tif files matched: {in_dir} / {args.pattern}")

    point_gdf = None
    point_meta: dict[str, Any] = {"path": None, "feature_count": 0}
    if str(args.vector).strip():
        vp = Path(args.vector).resolve()
        if not vp.exists():
            raise FileNotFoundError(f"Vector file not found: {vp}")
        point_gdf = _load_vector(vp)
        point_meta = {"path": str(vp), "feature_count": int(len(point_gdf))}

    boundary_gdf = None
    boundary_meta: dict[str, Any] = {"path": None, "feature_count": 0}
    if str(args.boundary_vector).strip():
        bp = Path(args.boundary_vector).resolve()
        if not bp.exists():
            raise FileNotFoundError(f"Boundary vector file not found: {bp}")
        boundary_gdf = _load_vector(bp)
        boundary_meta = {"path": str(bp), "feature_count": int(len(boundary_gdf))}

    view_bbox = _parse_bbox(str(args.view_bbox))
    point_style_map = _load_json_arg(str(args.point_style_map))

    palette = PALETTES.get(str(args.style_palette), PALETTES["report_dark"])
    cmap_name = str(args.cmap).strip() or str(palette["cmap"])
    point_color = str(args.point_color).strip() or str(palette["point_color"])
    point_edge = str(args.point_edge).strip() or str(palette["point_edge"])
    boundary_edge_color = str(args.boundary_edge_color).strip() or str(palette["boundary_edge_color"])

    sample_vals = []
    for tif in tifs:
        with rasterio.open(tif) as ds:
            arr = ds.read(1).astype(np.float32)
            nodata = ds.nodata
            if nodata is not None:
                arr = np.where(arr == nodata, np.nan, arr)
            arr[~np.isfinite(arr)] = np.nan
            arr[arr < 0] = np.nan
            vals = arr[np.isfinite(arr)]
            if vals.size > 0:
                sample_vals.append(vals)
    stacked = np.concatenate(sample_vals) if sample_vals else np.array([], dtype=np.float32)
    vmin, vmax = _array_limits(stacked, float(args.percentile_min), float(args.percentile_max))

    breaks = _build_class_breaks(
        stacked,
        vmin=vmin,
        vmax=vmax,
        mode=str(args.classification_mode),
        bins=int(args.class_bins),
        stddev_range=float(args.stddev_range),
    )
    norm = None if breaks is None else BoundaryNorm(breaks, ncolors=plt.get_cmap(cmap_name).N, clip=True)

    frame_records = []
    frame_pngs: list[Path] = []
    for idx, tif in enumerate(tifs, start=1):
        frame_png = frames_dir / f"frame_{idx:03d}.png"
        rec = _render_frame(
            tif_path=tif,
            frame_path=frame_png,
            vmin=vmin,
            vmax=vmax,
            cmap=cmap_name,
            norm=norm,
            point_gdf=point_gdf,
            boundary_gdf=boundary_gdf,
            label_field=str(args.label_field).strip(),
            point_class_field=str(args.point_class_field).strip(),
            point_style_map=point_style_map,
            point_legend_label=str(args.point_legend_label).strip(),
            point_legend_title=str(args.point_legend_title).strip(),
            show_point_legend=bool(args.show_point_legend),
            boundary_edge_color=boundary_edge_color,
            boundary_linewidth=float(args.boundary_linewidth),
            boundary_alpha=float(args.boundary_alpha),
            title_prefix=args.title_prefix,
            show_colorbar=(not args.no_colorbar),
            point_size=float(args.point_size),
            point_color=point_color,
            point_edge=point_edge,
            view_bbox=view_bbox,
            basemap_style=str(args.basemap_style),
            basemap_provider=str(args.basemap_provider or ""),
            basemap_alpha=float(args.basemap_alpha),
            ntl_alpha=float(args.ntl_alpha),
            transparent_below=float(args.transparent_below),
        )
        frame_records.append(rec)
        frame_pngs.append(frame_png)
        print(f"[{idx}/{len(tifs)}] frame {frame_png.name}")

    if args.fps and args.fps > 0:
        duration_ms = max(1, int(round(1000.0 / float(args.fps))))
    else:
        duration_ms = max(1, int(args.duration_ms))

    images = [Image.open(p) for p in frame_pngs]
    gif_path = out_dir / "ntl_daily_animation.gif"
    images[0].save(
        gif_path,
        save_all=True,
        append_images=images[1:],
        duration=duration_ms,
        loop=0,
        optimize=False,
    )
    for im in images:
        im.close()

    summary = {
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "input_dir": str(in_dir),
        "output_dir": str(out_dir),
        "gif_path": str(gif_path),
        "frame_count": len(frame_pngs),
        "duration_ms": duration_ms,
        "style_palette": str(args.style_palette),
        "classification_mode": str(args.classification_mode),
        "class_bins": int(args.class_bins),
        "class_breaks": None if breaks is None else [float(x) for x in breaks],
        "visual_range": {"vmin": vmin, "vmax": vmax},
        "basemap_style": str(args.basemap_style),
        "basemap_provider": str(args.basemap_provider or ""),
        "basemap_alpha": float(args.basemap_alpha),
        "ntl_alpha": float(args.ntl_alpha),
        "transparent_below": None if not np.isfinite(args.transparent_below) else float(args.transparent_below),
        "view_bbox": {
            "minx": view_bbox[0],
            "miny": view_bbox[1],
            "maxx": view_bbox[2],
            "maxy": view_bbox[3],
        }
        if view_bbox is not None
        else None,
        "point_vector": point_meta,
        "point_style_map": point_style_map,
        "boundary_vector": boundary_meta,
        "frames": frame_records,
    }
    summary_path = out_dir / "gif_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[gif] {gif_path}")
    print(f"[summary] {summary_path}")


if __name__ == "__main__":
    main()
import traceback
