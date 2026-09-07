# ntl.script.contract.v2
NTL_SCRIPT_CONTRACT = {
    "schema": "ntl.script.contract.v2",
    "schema_version": "ntl.script.contract.v2",
    "name": "plot_us_strikes_iran_targets",
    "objective": "Render a schematic map figure of 'US strikes on Iran' main target locations from the accepted event-context summary, clearly marking evidence status (verified vs background knowledge) and limitations.",
    "input_manifest": [
        {"path": "inputs/iran_admin0.geojson", "role": "basemap_country_outline", "media_type": "application/geo+json"},
        {"path": "inputs/iran_admin1.geojson", "role": "basemap_province_boundaries", "media_type": "application/geo+json"}
    ],
    "method_steps": [
        "Load Iran ADM0/ADM1 GeoJSON boundaries (EPSG:4326, geoBoundaries)",
        "Highlight Qom (IR-26) and Isfahan (IR-04) provinces",
        "Plot Fordow and Natanz target markers with ~5 km uncertainty ellipses",
        "Annotate evidence status (as_of), legend, title and bottom notes",
        "Save figure to outputs/us_strikes_iran_targets_map.png"
    ],
    "parameters": {
        "dpi": 150,
        "uncertainty_km": 5.0,
        "targets": [
            {"name": "Fordow", "lat": 34.88, "lon": 50.99},
            {"name": "Natanz", "lat": 33.72, "lon": 51.73}
        ]
    },
    "output_manifest": [
        {"path": "outputs/us_strikes_iran_targets_map.png", "role": "schematic_map_figure", "media_type": "image/png"}
    ],
    "validation_checks": [
        "outputs/us_strikes_iran_targets_map.png exists and is non-empty",
        "Qom and Isfahan province features matched from ADM1",
        "Target markers fall within Iran bounds"
    ],
    "failure_gates": [
        "fail if output file missing or empty",
        "fail if target count < 2",
        "fail if Qom or Isfahan province not matched"
    ],
    "execution": {
        "timeout_seconds": 600,
        "network_scope": [],
        "overwrite_policy": "version",
        "test_strategy": "auto"
    }
}
"""
Purpose: Render a schematic map figure of "US strikes on Iran" targets from the
event-context summary (GeoSentinel event-tracking result).

Inputs (workspace-relative):
  - inputs/iran_admin0.geojson : Iran country outline, EPSG:4326 (geoBoundaries)
  - inputs/iran_admin1.geojson : Iran province boundaries, EPSG:4326 (geoBoundaries)

Outputs:
  - outputs/us_strikes_iran_targets_map.png : schematic figure

Method / scientific notes:
  - Target markers are BACKGROUND KNOWLEDGE (unverified), facility-level
    coordinates (precision approx +/-2-5 km). They are NOT verified by a
    first-hand source in the accepted event-context summary.
  - The 2026 ISW-tracked US/Israel strikes event family exists, but its target
    locations were NOT retrievable; this is shown as an annotation, not a point.
  - Basemap is schematic only (geoBoundaries generalised); no spatial statistics,
    no nighttime-light or damage conclusions are implied.
"""

import json
import math
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.font_manager import fontManager
from matplotlib.lines import Line2D
from matplotlib.patches import Circle

# ----------------------------------------------------------------------------
# CJK font selection (Chinese labels)
# ----------------------------------------------------------------------------
CJK_CANDIDATES = [
    "Noto Sans CJK SC", "Noto Sans CJK JP", "Noto Sans CJK TC",
    "Source Han Sans SC", "Source Han Sans CN", "WenQuanYi Zen Hei",
    "WenQuanYi Micro Hei", "SimHei", "Microsoft YaHei", "PingFang SC",
    "AR PL UMing CN", "Droid Sans Fallback",
]
AVAILABLE = {f.name for f in fontManager.ttflist}
CJK = next((c for c in CJK_CANDIDATES if c in AVAILABLE), None)
plt.rcParams["font.sans-serif"] = [CJK] if CJK else ["DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


def load_geojson(path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def iter_polygons(feature):
    geom = feature.get("geometry") or {}
    gtype = geom.get("type")
    coords = geom.get("coordinates") or []
    if gtype == "Polygon":
        yield coords
    elif gtype == "MultiPolygon":
        for poly in coords:
            yield poly


def plot_geojson(ax, gj, facecolor="none", edgecolor="0.35", lw=0.6, zorder=2):
    for feat in gj.get("features", []):
        for poly in iter_polygons(feat):
            if not poly:
                continue
            outer = poly[0]
            if len(outer) < 3:
                continue
            xs = [p[0] for p in outer]
            ys = [p[1] for p in outer]
            ax.fill(xs, ys, facecolor=facecolor, edgecolor=edgecolor,
                    lw=lw, zorder=zorder)


def gj_bounds(gj):
    minx = miny = float("inf")
    maxx = maxy = float("-inf")
    for feat in gj.get("features", []):
        for poly in iter_polygons(feat):
            for ring in poly:
                for pt in ring:
                    minx = min(minx, pt[0])
                    maxx = max(maxx, pt[0])
                    miny = min(miny, pt[1])
                    maxy = max(maxy, pt[1])
    return minx, miny, maxx, maxy


def match_province_feature(gj, iso_set, name_keys):
    """Return list of feature polygons for provinces matching ISO code or name."""
    out = []
    for feat in gj.get("features", []):
        props = feat.get("properties") or {}
        iso = str(props.get("shapeISO", "")).upper()
        name = " ".join(str(props.get(k, "")) for k in name_keys).lower()
        hit = iso in iso_set or any(k in name for k in name_keys)
        if hit:
            out.append(feat)
    return out


# ----------------------------------------------------------------------------
# Load basemap
# ----------------------------------------------------------------------------
gj0 = load_geojson("inputs/iran_admin0.geojson")
gj1 = load_geojson("inputs/iran_admin1.geojson")

minx, miny, maxx, maxy = gj_bounds(gj0)
mid_lat = (miny + maxy) / 2.0
ASPECT = 1.0 / math.cos(math.radians(mid_lat))
MARGIN = 0.5

qom_feats = match_province_feature(gj1, {"IR-26"}, {"qom"})
isf_feats = match_province_feature(gj1, {"IR-04"}, {"isfahan", "esfahan"})

# ----------------------------------------------------------------------------
# Target facts (from accepted event-context summary; BACKGROUND, UNVERIFIED)
# ----------------------------------------------------------------------------
TARGETS = [
    {
        "name_cn": "福尔多燃料浓缩厂",
        "name_en": "Fordow Fuel Enrichment Plant",
        "prov": "库姆省 (Qom)",
        "lat": 34.88, "lon": 50.99,
    },
    {
        "name_cn": "纳坦兹浓缩设施",
        "name_en": "Natanz Enrichment Facility",
        "prov": "伊斯法罕省 (Isfahan)",
        "lat": 33.72, "lon": 51.73,
    },
]

# Approx +/-5 km uncertainty circle in degrees (1 deg lat ~= 111.32 km)
UNC_KM = 5.0


def uncircle_radius_deg(lat):
    dlat = UNC_KM / 111.32
    dlon = UNC_KM / (111.32 * math.cos(math.radians(lat)))
    return dlon, dlat  # ellipse axes (dx, dy)


# ----------------------------------------------------------------------------
# Figure
# ----------------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(12.5, 9.0))
fig.patch.set_facecolor("white")

# Provinces highlight (Qom / Isfahan)
for feat in qom_feats + isf_feats:
    plot_geojson(ax, {"type": "FeatureCollection", "features": [feat]},
                 facecolor="#d7e3f4", edgecolor="#3d6ea5", lw=0.8, zorder=1)

# Remaining provinces (light) and country outline
plot_geojson(ax, gj1, facecolor="#f2f2f2", edgecolor="#9aa0a6", lw=0.5, zorder=1)
plot_geojson(ax, gj0, facecolor="none", edgecolor="#3c3c3c", lw=1.4, zorder=3)

# Grid
ax.grid(True, linestyle=":", linewidth=0.5, color="0.75", zorder=0)
ax.set_axisbelow(True)

# Target markers + uncertainty ellipses + labels
for t in TARGETS:
    dx, dy = uncircle_radius_deg(t["lat"])
    ax.add_patch(Circle((t["lon"], t["lat"]), dx, fill=False,
                        linestyle="--", linewidth=1.1,
                        edgecolor="#b23b3b", alpha=0.75, zorder=4))
    ax.add_patch(Circle((t["lon"], t["lat"]), dy, fill=False,
                        linestyle="--", linewidth=1.1,
                        edgecolor="#b23b3b", alpha=0.75, zorder=4))
    ax.plot(t["lon"], t["lat"], marker="*", markersize=17,
            color="#c62828", markeredgecolor="white", markeredgewidth=0.8,
            zorder=6)
    ax.annotate(
        f'{t["name_cn"]}\n{t["prov"]}（背景·待核验）',
        xy=(t["lon"], t["lat"]),
        xytext=(t["lon"] + 1.35, t["lat"] + 0.9),
        fontsize=10.5, color="#7a1010", fontweight="bold",
        arrowprops=dict(arrowstyle="->", color="#7a1010", lw=1.1),
        zorder=7, bbox=dict(boxstyle="round,pad=0.35", fc="#fff5f5",
                            ec="#c62828", alpha=0.92),
    )

# Map extent
ax.set_xlim(minx - MARGIN, maxx + MARGIN)
ax.set_ylim(miny - MARGIN, maxy + MARGIN)
ax.set_aspect(ASPECT)

# Title
ax.set_title("美国对伊朗主要袭击目标位置示意图（事件溯源结果）",
             fontsize=15, fontweight="bold", pad=14, color="#1a1a1a")

# Annotation box (evidence status)
STATUS_BOX = (
    "证据状态（as_of 2026-08-25T02:27Z）\n"
    "● 已核验：ISW/CTP 正在跟踪『2026 年美国与以色列对伊朗联合打击』\n"
    "    事件家族（源更新 2026-08-24），但其目标位置本轮未取回（图层需令牌）\n"
    "● 待核验背景（2025-06-22 『午夜锤』行动）：主要目标为福尔多、纳坦兹\n"
    "● 坐标为设施级（±2–5 km），未独立核实；本图仅示意，不构成损伤/责任结论"
)
ax.text(0.012, 0.985, STATUS_BOX, transform=ax.transAxes, fontsize=9.2,
        va="top", ha="left", color="#222222",
        bbox=dict(boxstyle="round,pad=0.55", fc="#f6f8fa", ec="#8a9199",
                  alpha=0.95), zorder=8)

# Legend
legend_handles = [
    Line2D([0], [0], marker="*", color="w", markerfacecolor="#c62828",
           markeredgecolor="white", markersize=13,
           label="主要袭击目标（背景知识·待核验）"),
    Line2D([0], [0], color="#3c3c3c", lw=1.4,
           label="伊朗国家轮廓（ADM0）"),
    Line2D([0], [0], color="#9aa0a6", lw=0.7,
           label="省级边界（ADM1）"),
    Line2D([0], [0], color="#d7e3f4", lw=4,
           label="库姆省 / 伊斯法罕省（高亮）"),
    Line2D([0], [0], color="#b23b3b", lw=1.2, linestyle="--",
           label="±5 km 精度示意（设施级，未核验）"),
    Line2D([0], [0], marker="D", color="w", markerfacecolor="#757575",
           markeredgecolor="#444444", markersize=10,
           label="2026 事件家族目标：位置未取回，无法标绘"),
]
leg = ax.legend(handles=legend_handles, loc="lower left", fontsize=8.6,
                frameon=True, facecolor="white", edgecolor="#8a9199",
                framealpha=0.95, borderpad=0.8, labelspacing=0.7)
ax.add_artist(leg)

# Bottom note
ax.text(0.99, 0.012,
        "底图：geoBoundaries（EPSG:4326，概化边界，仅示意）｜"
        "坐标：设施级、背景知识、未核验｜不含夜光/空间统计/损伤推断",
        transform=ax.transAxes, fontsize=8.2, va="bottom", ha="right",
        color="#555555", style="italic")

# North arrow (approximate, schematic)
ax.annotate("N", xy=(0.965, 0.90), xytext=(0.965, 0.83),
            xycoords="axes fraction", textcoords="axes fraction",
            ha="center", fontsize=13, fontweight="bold", color="#333333",
            arrowprops=dict(arrowstyle="-|>", color="#333333", lw=1.6))

out_path = "outputs/us_strikes_iran_targets_map.png"
os.makedirs("outputs", exist_ok=True)
fig.savefig(out_path, dpi=150, bbox_inches="tight", facecolor="white")
plt.close(fig)

# ----------------------------------------------------------------------------
# Validation printout
# ----------------------------------------------------------------------------
print("contract=ntl.script.contract.v2")
print(f"cjk_font={CJK or 'NONE_FALLBACK_DEJAVU'}")
print(f"admin0_features={len(gj0.get('features', []))}")
print(f"admin1_features={len(gj1.get('features', []))}")
print(f"qom_feature_hits={len(qom_feats)} isf_feature_hits={len(isf_feats)}")
print(f"bounds_deg=({minx:.2f},{miny:.2f})-({maxx:.2f},{maxy:.2f}) aspect={ASPECT:.3f}")
print(f"target_count={len(TARGETS)}")
print(f"output_exists={os.path.exists(out_path)}")
print(f"output_bytes={os.path.getsize(out_path) if os.path.exists(out_path) else 0}")
print("DONE")
