#!/usr/bin/env python3
"""
Q24 reference implementation (ArcGIS-style cartography):
Generate daily NTL animation for Fuding City (2020-01-01 to 2020-01-07).
"""

from __future__ import annotations

import math
import re
import io
from pathlib import Path
from typing import Iterable, Tuple

import contextily as ctx
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import rasterio
from matplotlib.colors import BoundaryNorm, LinearSegmentedColormap
from matplotlib.offsetbox import AnnotationBbox, OffsetImage
from matplotlib.patches import Polygon, Rectangle
from matplotlib_scalebar.scalebar import ScaleBar
from PIL import Image
from rasterio.crs import CRS
from rasterio.warp import Resampling, calculate_default_transform, reproject, transform_bounds

# Try common CJK fonts; fallback to default if unavailable.
matplotlib.rcParams["font.sans-serif"] = [
    "SimHei",
    "Microsoft YaHei",
    "WenQuanYi Micro Hei",
    "Noto Sans CJK SC",
    "DejaVu Sans",
]
matplotlib.rcParams["axes.unicode_minus"] = False

INPUTS_DIR = Path("inputs")
OUTPUTS_DIR = Path("outputs")

INPUT_FILENAMES = [
    "fuding_vnp46a2_20200101_20200107_2020-01-01.tif",
    "fuding_vnp46a2_20200101_20200107_2020-01-02.tif",
    "fuding_vnp46a2_20200101_20200107_2020-01-03.tif",
    "fuding_vnp46a2_20200101_20200107_2020-01-04.tif",
    "fuding_vnp46a2_20200101_20200107_2020-01-05.tif",
    "fuding_vnp46a2_20200101_20200107_2020-01-06.tif",
    "fuding_vnp46a2_20200101_20200107_2020-01-07.tif",
]

DATE_LABELS = [
    "2020-01-01",
    "2020-01-02",
    "2020-01-03",
    "2020-01-04",
    "2020-01-05",
    "2020-01-06",
    "2020-01-07",
]

SHOW_GRATICULE_DEFAULT = False
USE_BASEMAP_DEFAULT = True
NORTH_ARROW_ASSET_PATH = Path(r"E:\Download\north-arrow-svgrepo-com.svg")


def load_ntl_data(filepath: Path):
    with rasterio.open(filepath) as src:
        data = src.read(1)
        nodata = src.nodata
        if nodata is not None:
            masked = np.ma.masked_equal(data, nodata)
        else:
            masked = np.ma.masked_invalid(data)
        return masked, src.bounds, src.crs, src.transform


def calculate_global_range(file_paths: Iterable[Path]) -> Tuple[float, float]:
    vals = []
    for fp in file_paths:
        arr, *_ = load_ntl_data(fp)
        if arr.count() > 0:
            vals.append(float(arr.min()))
            vals.append(float(arr.max()))
    if not vals:
        raise RuntimeError("No valid pixels found across all input rasters.")
    return min(vals), max(vals)


def create_ntl_colormap() -> LinearSegmentedColormap:
    # ArcGIS-like dark-to-bright ramp.
    colors = [
        (0.00, 0.00, 0.00),
        (0.02, 0.03, 0.25),
        (0.02, 0.20, 0.55),
        (0.08, 0.45, 0.75),
        (0.22, 0.68, 0.40),
        (0.75, 0.85, 0.20),
        (0.98, 0.92, 0.35),
        (0.98, 0.98, 0.98),
    ]
    return LinearSegmentedColormap.from_list("NTL_ColorMap_ArcGIS", colors, N=256)


def _nice_step(value: float) -> float:
    if value <= 0:
        return 1.0
    exp = math.floor(math.log10(value))
    base = 10 ** exp
    ratio = value / base
    if ratio <= 1.0:
        m = 1.0
    elif ratio <= 2.0:
        m = 2.0
    elif ratio <= 5.0:
        m = 5.0
    else:
        m = 10.0
    return m * base


def build_class_breaks(vmin: float, vmax: float, target_classes: int = 8) -> np.ndarray:
    span = max(vmax - vmin, 1e-6)
    step = _nice_step(span / float(max(target_classes, 2)))
    start = math.floor(vmin / step) * step
    end = math.ceil(vmax / step) * step
    values = np.arange(start, end + step * 0.5, step, dtype=float)
    if values.size < 3:
        values = np.array([start, start + step, start + step * 2], dtype=float)
    return values


def haversine_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    r = 6371.0088
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2.0) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2.0) ** 2
    return 2.0 * r * math.asin(math.sqrt(a))


def estimate_map_width_km(bounds, crs: CRS | None) -> float:
    if crs is None:
        return 50.0
    wgs84_bounds = transform_bounds(crs, "EPSG:4326", bounds.left, bounds.bottom, bounds.right, bounds.top)
    left, bottom, right, top = wgs84_bounds
    mid_lat = (top + bottom) / 2.0
    width_km = haversine_km(left, mid_lat, right, mid_lat)
    return max(width_km, 1.0)


def choose_scalebar_km(map_width_km: float) -> float:
    target = map_width_km * 0.20
    if target <= 0:
        return 1.0
    exp = math.floor(math.log10(target))
    base = 10 ** exp
    for m in (1, 2, 5, 10):
        candidate = m * base
        if candidate >= target:
            return candidate
    return 10 * base


def draw_scalebar(ax, map_width_km: float) -> None:
    length_km = choose_scalebar_km(map_width_km)
    frac = min(max(length_km / map_width_km, 0.10), 0.30)

    x0 = 0.08
    y0 = 0.09
    x1 = x0 + frac
    xm = (x0 + x1) / 2.0
    h = 0.012

    ax.add_patch(Rectangle((x0, y0), xm - x0, h, transform=ax.transAxes, facecolor="black", edgecolor="black", lw=0.6, zorder=7))
    ax.add_patch(Rectangle((xm, y0), x1 - xm, h, transform=ax.transAxes, facecolor="white", edgecolor="black", lw=0.6, zorder=7))

    for xt in (x0, xm, x1):
        ax.plot([xt, xt], [y0, y0 + h + 0.005], transform=ax.transAxes, color="black", lw=0.8, zorder=8)

    ax.text(x0, y0 + h + 0.008, "0", transform=ax.transAxes, ha="center", va="bottom", fontsize=9)
    ax.text(xm, y0 + h + 0.008, f"{int(round(length_km / 2.0))}", transform=ax.transAxes, ha="center", va="bottom", fontsize=9)
    ax.text(x1, y0 + h + 0.008, f"{int(round(length_km))} km", transform=ax.transAxes, ha="center", va="bottom", fontsize=9)


def add_professional_scalebar(ax, bounds, crs, width_px: int) -> bool:
    """
    Add ScaleBar based on estimated meters-per-pixel from georeferenced bounds.
    Returns True when successful, False to allow fallback.
    """
    try:
        if width_px <= 0:
            return False
        if crs is None:
            return False
        left_m, bottom_m, right_m, top_m = transform_bounds(crs, "EPSG:3857", bounds.left, bounds.bottom, bounds.right, bounds.top)
        map_width_m = float(max(right_m - left_m, 1.0))
        meters_per_pixel = map_width_m / float(width_px)
        scalebar = ScaleBar(
            meters_per_pixel,
            units="m",
            dimension="si-length",
            location="lower left",
            length_fraction=0.20,
            width_fraction=0.015,
            color="black",
            box_color="white",
            box_alpha=0.95,
            border_pad=0.6,
            pad=0.3,
            font_properties={"size": 8},
            scale_loc="top",
        )
        ax.add_artist(scalebar)
        return True
    except Exception:
        return False


def _load_north_arrow_image(asset_path: Path):
    """
    Load north-arrow asset as image array.
    - For SVG: try cairosvg rasterization if available.
    - For raster images: fallback to matplotlib.imread.
    """
    if not asset_path.exists():
        raise FileNotFoundError(f"North arrow asset not found: {asset_path}")

    suffix = asset_path.suffix.lower()
    if suffix == ".svg":
        try:
            import cairosvg  # optional dependency
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("SVG north arrow requires cairosvg (`pip install cairosvg`).") from exc
        png_bytes = cairosvg.svg2png(url=str(asset_path))
        return np.array(Image.open(io.BytesIO(png_bytes)).convert("RGBA"))

    return plt.imread(str(asset_path))


def draw_north_arrow(ax, transform) -> None:
    # Use external north-arrow asset first.
    if NORTH_ARROW_ASSET_PATH.exists():
        try:
            img = _load_north_arrow_image(NORTH_ARROW_ASSET_PATH)
            ab = AnnotationBbox(
                OffsetImage(img, zoom=0.22),
                (0.88, 0.90),
                xycoords=ax.transAxes,
                frameon=False,
                box_alignment=(0.5, 0.5),
                zorder=14,
            )
            ax.add_artist(ab)
            return
        except Exception:
            # Fallback vector arrow if asset rendering is unavailable.
            pass

    north_up = abs(transform.b) < 1e-9 and abs(transform.d) < 1e-9 and transform.e < 0
    x = 0.88
    y = 0.86
    w = 0.02
    h = 0.07

    tri_black = Polygon(
        [(x, y + h), (x - w / 2.0, y), (x + w / 2.0, y)],
        closed=True,
        transform=ax.transAxes,
        facecolor="black",
        edgecolor="black",
        lw=0.8,
        zorder=12,
    )
    tri_white = Polygon(
        [(x, y + h * 0.86), (x - w * 0.18, y + h * 0.20), (x, y + h * 0.44)],
        closed=True,
        transform=ax.transAxes,
        facecolor="white",
        edgecolor="none",
        zorder=13,
    )
    ax.add_patch(tri_black)
    ax.add_patch(tri_white)
    ax.text(x, y + h + 0.012, "N", transform=ax.transAxes, ha="center", va="bottom", fontsize=10)
    if not north_up:
        ax.text(x, y - 0.02, "rotated", transform=ax.transAxes, ha="center", va="top", fontsize=7, color="#6b6b6b")


def draw_fixed_scalebar(ax) -> None:
    """Sample-like segmented scalebar: 0-10-20 千米."""
    x0 = 0.07
    y0 = 0.045
    total_w = 0.30
    h = 0.012
    seg_w = total_w / 2.0

    ax.add_patch(
        Rectangle((x0, y0), seg_w, h, transform=ax.transAxes, facecolor="black", edgecolor="black", lw=0.8, zorder=14)
    )
    ax.add_patch(
        Rectangle((x0 + seg_w, y0), seg_w, h, transform=ax.transAxes, facecolor="white", edgecolor="black", lw=0.8, zorder=14)
    )
    tick_x = [x0, x0 + seg_w / 2.0, x0 + seg_w, x0 + seg_w + seg_w / 2.0, x0 + total_w]
    for tx in tick_x:
        ax.plot([tx, tx], [y0, y0 + h + 0.006], transform=ax.transAxes, color="black", lw=0.8, zorder=15)

    ax.text(x0, y0 + h + 0.008, "0", transform=ax.transAxes, ha="center", va="bottom", fontsize=10)
    ax.text(x0 + seg_w, y0 + h + 0.008, "10", transform=ax.transAxes, ha="center", va="bottom", fontsize=10)
    ax.text(x0 + total_w, y0 + h + 0.008, "20 千米", transform=ax.transAxes, ha="center", va="bottom", fontsize=10)


def reproject_masked_to_3857(masked_arr: np.ma.MaskedArray, src_transform, src_crs: CRS):
    """Reproject one raster frame to EPSG:3857 for basemap overlay."""
    src_data = np.ma.filled(masked_arr, np.nan).astype(np.float32)
    if src_crs is None:
        raise ValueError("Raster CRS is required for basemap mode.")

    src_h, src_w = src_data.shape
    dst_transform, dst_w, dst_h = calculate_default_transform(
        src_crs, "EPSG:3857", src_w, src_h, left=0, bottom=0, right=src_w, top=src_h, resolution=None
    )
    # Use georeferenced bounds to avoid pixel-space transform assumptions.
    # Recompute dst transform by bounds for robust extent mapping.
    # Bounds in source CRS:
    # NOTE: src_transform maps pixel->map, so derive bounds directly:
    x0, y0 = src_transform * (0, 0)
    x1, y1 = src_transform * (src_w, src_h)
    left = min(x0, x1)
    right = max(x0, x1)
    bottom = min(y0, y1)
    top = max(y0, y1)
    dst_transform, dst_w, dst_h = calculate_default_transform(
        src_crs, "EPSG:3857", src_w, src_h, left=left, bottom=bottom, right=right, top=top
    )

    dst_data = np.full((dst_h, dst_w), np.nan, dtype=np.float32)
    reproject(
        source=src_data,
        destination=dst_data,
        src_transform=src_transform,
        src_crs=src_crs,
        dst_transform=dst_transform,
        dst_crs="EPSG:3857",
        src_nodata=np.nan,
        dst_nodata=np.nan,
        resampling=Resampling.bilinear,
    )
    a = dst_transform.a
    e = dst_transform.e
    c = dst_transform.c
    f = dst_transform.f
    extent = [c, c + a * dst_w, f + e * dst_h, f]
    return np.ma.masked_invalid(dst_data), extent


def draw_discrete_legend_box(ax, class_breaks: np.ndarray, colormap) -> None:
    """
    Draw compact ArcGIS-style discrete legend at lower-left.
    """
    panel = ax.inset_axes([0.035, 0.11, 0.195, 0.28], transform=ax.transAxes, zorder=25)
    panel.set_facecolor((1, 1, 1, 0.82))
    for spine in panel.spines.values():
        spine.set_color("#666666")
        spine.set_linewidth(0.7)
    panel.set_xticks([])
    panel.set_yticks([])
    panel.set_xlim(0, 1)
    panel.set_ylim(0, 1)

    box_x = 0.12
    box_y = 0.08
    box_w = 0.42
    box_h = 0.72
    n_bins = max(len(class_breaks) - 1, 1)
    panel.text(
        box_x,
        box_y + box_h + 0.012,
        "夜间灯光强度",
        transform=panel.transAxes,
        ha="left",
        va="bottom",
        fontsize=10.5,
        fontweight="bold",
    )

    sw_h = box_h / n_bins
    for i in range(n_bins):
        y = box_y + i * sw_h
        mid = i + 0.5
        color = colormap(mid / n_bins)
        panel.add_patch(
            Rectangle(
                (box_x, y),
                box_w * 0.52,
                sw_h * 0.86,
                transform=panel.transAxes,
                facecolor=color,
                edgecolor="white",
                lw=0.25,
            )
        )

    panel.text(
        box_x + box_w * 0.60,
        box_y + box_h - sw_h * 0.45,
        "高",
        transform=panel.transAxes,
        ha="left",
        va="center",
        fontsize=12,
        fontweight="bold",
    )
    panel.text(
        box_x + box_w * 0.60,
        box_y + sw_h * 0.40,
        "低",
        transform=panel.transAxes,
        ha="left",
        va="center",
        fontsize=12,
        fontweight="bold",
    )


def _format_lon(lon: float) -> str:
    hemi = "E" if lon >= 0 else "W"
    return f"{abs(lon):.3f}\N{DEGREE SIGN}{hemi}"


def _format_lat(lat: float) -> str:
    hemi = "N" if lat >= 0 else "S"
    return f"{abs(lat):.3f}\N{DEGREE SIGN}{hemi}"


def add_graticule_and_geo_ticks(ax, data_shape: Tuple[int, int], bounds, crs) -> bool:
    """
    Draw ArcGIS-style graticule and lon/lat tick labels.
    Returns True on success, False to allow graceful fallback.
    """
    try:
        h, w = int(data_shape[0]), int(data_shape[1])
        if h <= 0 or w <= 0 or crs is None:
            return False

        lon_left, lat_bottom, lon_right, lat_top = transform_bounds(
            crs, "EPSG:4326", bounds.left, bounds.bottom, bounds.right, bounds.top
        )
        if not np.isfinite([lon_left, lat_bottom, lon_right, lat_top]).all():
            return False
        if lon_right <= lon_left or lat_top <= lat_bottom:
            return False

        x_fracs = [0.10, 0.35, 0.60, 0.85]
        y_fracs = [0.10, 0.35, 0.60, 0.85]
        xticks = [f * w for f in x_fracs]
        yticks = [f * h for f in y_fracs]

        lon_vals = [lon_left + f * (lon_right - lon_left) for f in x_fracs]
        lat_vals = [lat_top - f * (lat_top - lat_bottom) for f in y_fracs]

        ax.set_xticks(xticks)
        ax.set_yticks(yticks)
        ax.set_xticklabels([_format_lon(v) for v in lon_vals], fontsize=8, color="#404040")
        ax.set_yticklabels([_format_lat(v) for v in lat_vals], fontsize=8, color="#404040")

        ax.tick_params(
            axis="both",
            which="both",
            direction="out",
            length=3.5,
            width=0.6,
            color="#666666",
            top=True,
            right=True,
            labeltop=False,
            labelright=False,
        )

        # Subtle graticule lines similar to desktop GIS defaults.
        ax.grid(True, color="#c9c9c9", linestyle=(0, (2.0, 2.0)), linewidth=0.55, alpha=0.75)
        return True
    except Exception:
        return False


def render_frame(
    data,
    vmin: float,
    vmax: float,
    colormap,
    output_path: Path,
    date_label: str,
    bounds,
    crs,
    transform,
    show_graticule: bool = SHOW_GRATICULE_DEFAULT,
    use_basemap: bool = USE_BASEMAP_DEFAULT,
):
    fig = plt.figure(figsize=(12.8, 7.2), dpi=150, facecolor="white")
    ax = fig.add_subplot(111)

    ax.set_facecolor("white")
    class_breaks = build_class_breaks(vmin, vmax, target_classes=8)
    norm = BoundaryNorm(class_breaks, ncolors=colormap.N, clip=True)
    plot_data = data
    extent = None
    im = None
    if use_basemap and crs is not None:
        try:
            plot_data, extent = reproject_masked_to_3857(data, transform, crs)
            im = ax.imshow(plot_data, cmap=colormap, norm=norm, origin="upper", interpolation="nearest", extent=extent, zorder=8)
            ctx.add_basemap(
                ax,
                source=ctx.providers.CartoDB.PositronNoLabels,
                crs="EPSG:3857",
                attribution=False,
                zorder=1,
            )
            ax.set_xlim(extent[0], extent[1])
            ax.set_ylim(extent[2], extent[3])
        except Exception:
            im = ax.imshow(data, cmap=colormap, norm=norm, origin="upper", interpolation="nearest")
    else:
        im = ax.imshow(data, cmap=colormap, norm=norm, origin="upper", interpolation="nearest")

    if extent is None:
        valid_mask = np.where(np.ma.getmaskarray(data), 0.0, 1.0)
        ax.contour(valid_mask, levels=[0.5], colors="#4d4d4d", linewidths=0.5, alpha=0.95)

    for s in ax.spines.values():
        s.set_visible(True)
        s.set_color("#5e5e5e")
        s.set_linewidth(0.8)

    ax.set_title(
        "福建省福鼎市夜间灯光日变化\n"
        "VIIRS VNP46A2",
        fontsize=16,
        fontweight="bold",
        pad=12,
    )

    ax.text(
        0.02,
        0.98,
        f"日期: {date_label}",
        transform=ax.transAxes,
        va="top",
        fontsize=11,
        bbox=dict(boxstyle="round,pad=0.25", facecolor="#f7f2d3", edgecolor="#666666", alpha=0.95),
    )

    draw_north_arrow(ax, transform)

    draw_fixed_scalebar(ax)

    draw_discrete_legend_box(ax, class_breaks, colormap)

    source_text = "Source: VIIRS VNP46A2"
    ax.text(
        0.90,
        0.02,
        source_text,
        transform=ax.transAxes,
        ha="right",
        va="bottom",
        fontsize=8.5,
        zorder=20,
        bbox=dict(boxstyle="round,pad=0.25", facecolor="#f0f0f0", edgecolor="#6b6b6b", alpha=0.95),
    )

    if show_graticule:
        target_shape = plot_data.shape if hasattr(plot_data, "shape") else data.shape
        if not add_graticule_and_geo_ticks(ax, target_shape, bounds, crs):
            ax.set_xticks([])
            ax.set_yticks([])
    else:
        ax.set_xticks([])
        ax.set_yticks([])
    fig.subplots_adjust(left=0.04, right=0.98, top=0.92, bottom=0.05)
    plt.savefig(output_path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"[frame] {output_path.name} ({date_label})")


def create_gif(png_files: list[Path], output_gif: Path, duration_ms: int = 800) -> None:
    images = [Image.open(fp) for fp in png_files]
    images[0].save(
        output_gif,
        save_all=True,
        append_images=images[1:],
        duration=duration_ms,
        loop=0,
        optimize=True,
    )
    print(f"[gif] {output_gif}")


def next_output_version(outputs_dir: Path) -> int:
    """
    Auto-increment map export version based on existing files in outputs/.
    Matches filenames like *_hd_v2.png / *_v2.gif and returns next integer version.
    """
    pattern = re.compile(r"_v(\d+)\.(?:png|gif)$", re.IGNORECASE)
    max_v = 0
    if outputs_dir.exists():
        for fp in outputs_dir.iterdir():
            if not fp.is_file():
                continue
            m = pattern.search(fp.name)
            if m:
                max_v = max(max_v, int(m.group(1)))
    return max_v + 1


def main() -> None:
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    run_version = next_output_version(OUTPUTS_DIR)
    version_tag = f"v{run_version}"
    ntl_files = [INPUTS_DIR / fn for fn in INPUT_FILENAMES]
    missing = [str(fp) for fp in ntl_files if not fp.exists()]
    if missing:
        raise FileNotFoundError(f"Missing input files: {missing}")

    print("=" * 68)
    print("Q24 | Fuding daily NTL animation (2020-01-01 to 2020-01-07)")
    print("=" * 68)

    vmin, vmax = calculate_global_range(ntl_files)
    print(f"[range] global radiance range: [{vmin:.4f}, {vmax:.4f}]")
    print(f"[version] output version: {version_tag}")

    cmap = create_ntl_colormap()
    png_files: list[Path] = []

    for tif_path, date_label in zip(ntl_files, DATE_LABELS):
        arr, bounds, crs, transform = load_ntl_data(tif_path)
        out_png = OUTPUTS_DIR / f"fuding_ntl_{date_label}_hd_{version_tag}.png"
        render_frame(arr, vmin, vmax, cmap, out_png, date_label, bounds, crs, transform)
        png_files.append(out_png)

    out_gif = OUTPUTS_DIR / f"fuding_ntl_20200101_20200107_animation_hd_{version_tag}.gif"
    create_gif(png_files, out_gif, duration_ms=800)

    print("=" * 68)
    print("[done] generated files:")
    for fp in png_files:
        print(f"  - {fp}")
    print(f"  - {out_gif}")


if __name__ == "__main__":
    main()
