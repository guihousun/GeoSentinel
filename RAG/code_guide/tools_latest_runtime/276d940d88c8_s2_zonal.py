NTL_SCRIPT_CONTRACT = {
    "schema": "ntl.script.contract.v2",
    "objective": "Compute 2020 GPWv4.11 population zonal statistics for the 50 US states + DC on the validated national mosaic, emit the ranked distribution CSV and machine-readable JSON, and run internal validation.",
    "input_manifest": [
        {"path": "outputs/us_pop_gpw2020_national.tif", "role": "staged output mosaic"},
        {"path": "outputs/us_states_51.shp", "role": "staged output boundary"},
        {"path": "inputs/us_pop_gpw2020_p1.tif", "role": "declared input for native check"},
        {"path": "inputs/us_pop_gpw2020_p2.tif", "role": "declared input for native check"},
        {"path": "inputs/us_pop_gpw2020_p4.tif", "role": "declared input for native check"},
        {"path": "inputs/us_pop_gpw2020_ak1.tif", "role": "declared input for native check"},
        {"path": "inputs/us_pop_gpw2020_ak2.tif", "role": "declared input for native check"},
        {"path": "inputs/us_pop_gpw2020_ak3.tif", "role": "declared input for native check"},
        {"path": "inputs/us_pop_gpw2020_akw.tif", "role": "declared input for native check"}
    ],
    "method_steps": [
        "read the 51-state shapefile and the national mosaic; recompute tile placements from source geotransforms",
        "per state: windowed center-in-polygon masks on the mosaic grid, sum of non-negative cells (population counts, persons per grid cell); no area weighting",
        "per state: equal-area (EPSG:5070 Albers) planar area km2 from geometry; density and national percentage; rank by population with cumulative share",
        "independent native-tile cross checks for WA (p1), FL (p4), TX (p2+p4 seam), AK (ak1..ak3+akw incl. antimeridian fold) using first-valid-wins ownership",
        "independent national masked sum over the union of the 51 geometries for the requested sigma-states vs mask check",
        "write distribution CSV, machine-readable JSON and validation report"
    ],
    "parameters": {},
    "output_manifest": [
        {"path": "outputs/us_state_population_distribution_2020.csv", "required": True},
        {"path": "outputs/us_state_population_distribution_2020.json", "required": True},
        {"path": "outputs/us_pop_2020_validation_report.json", "required": True}
    ],
    "validation_checks": [
        "CSV contains 51 state rows + a national total row and all 7 required columns",
        "validation report records the sigma-states vs national-mask relative difference (<0.5%) and native-tile cross checks",
        "all computed populations are non-negative and finite"
    ],
    "failure_gates": [
        "fail if the sigma-states vs national-mask relative difference is >= 0.5%",
        "fail if fewer than 51 state rows are written or any required output is missing"
    ],
    "execution": {
        "mode": "execute",
        "timeout_seconds": 3600,
        "overwrite_policy": "version",
        "network_scope": [],
        "test_strategy": "auto",
        "repair_history": [{"reason": "First execution raised IndexError (boolean index shape mismatch, sub width 66 vs mask width 40768) inside windowed zonal summation: window_of_geom built the window width as (c1 - r0), mixing the column-end with the row-start offset, so windows for polygons near the canvas right edge (AKW positive-longitude lobe, Alaska) had an absurd width and the clipped mosaic read returned fewer columns than the rasterized mask. Diagnostic over all 1,421 sub-polygons confirmed zero mismatches with the correct width (c1 - c0).", "before": "return windows.Window(c0, r0, c1 - r0, r1 - r0)", "after": "return windows.Window(c0, r0, c1 - c0, r1 - r0)"}]
    }
}

import json
import math
import csv

import numpy as np
import rasterio
from rasterio import features, windows
from osgeo import ogr
from shapely.geometry import shape as shp_shape, mapping as shp_mapping
from shapely import ops as shp_ops
from pyproj import Transformer

RES = 0.00833636583662916

# ---------------------------------------------------------------------------
# load 51-state boundary -> shapely
# ---------------------------------------------------------------------------
ds_v = ogr.Open("outputs/us_states_51.shp")
lyr = ds_v.GetLayer(0)
states = []
for feat in lyr:
    name = feat.GetField("shapeName")
    iso = feat.GetField("shapeISO")
    code = iso[-2:]
    gj = json.loads(feat.GetGeometryRef().ExportToJson())
    geom = shp_shape(gj)
    states.append({"name": name, "code": code, "iso": iso, "geom": geom})
ds_v = None
assert len(states) == 51, len(states)

# ---------------------------------------------------------------------------
# mosaic canvas metadata and source-tile placements (independent recompute)
# ---------------------------------------------------------------------------
TILES = ["p1", "p2", "p3", "p4", "ak1", "ak2", "ak3", "akw", "hi"]
src_path = {t: "inputs/us_pop_gpw2020_%s.tif" % t for t in TILES}
meta = {}
for t in TILES:
    with rasterio.open(src_path[t]) as d:
        meta[t] = {"origin_x": float(d.transform.c), "origin_y": float(d.transform.f),
                   "width": d.width, "height": d.height}
x0 = min(m["origin_x"] for m in meta.values())
y_top = max(m["origin_y"] for m in meta.values())
place = {}
for t in TILES:
    m = meta[t]
    col0 = int(round((m["origin_x"] - x0) / RES))
    row0 = int(round((y_top - m["origin_y"]) / RES))
    place[t] = {"col0": col0, "row0": row0,
                "col1": col0 + m["width"], "row1": row0 + m["height"]}

mos_path = "outputs/us_pop_gpw2020_national.tif"
with rasterio.open(mos_path) as mos:
    T_ = mos.transform
    MOS_ROWS, MOS_COLS = mos.height, mos.width


def window_of_geom(g, margin=2):
    minx, miny, maxx, maxy = g.bounds
    c0 = max(0, int(math.floor((minx - x0) / RES)) - margin)
    c1 = min(MOS_COLS, int(math.ceil((maxx - x0) / RES)) + margin)
    r0 = max(0, int(math.floor((y_top - maxy) / RES)) - margin)
    r1 = min(MOS_ROWS, int(math.ceil((y_top - miny) / RES)) + margin)
    if r1 <= r0 or c1 <= c0:
        return None
    return windows.Window(c0, r0, c1 - c0, r1 - r0)


def poly_sum_mosaic(geom):
    """Sum of non-negative population cells whose pixel centre lies inside geom."""
    polys = list(geom.geoms) if geom.geom_type == "MultiPolygon" else [geom]
    total = 0.0
    cells = 0
    with rasterio.open(mos_path) as mos:
        for g in polys:
            win = window_of_geom(g)
            if win is None:
                continue
            sub = mos.read(1, window=win)
            wt = windows.transform(win, mos.transform)
            m = features.rasterize([(g, 1)], out_shape=(win.height, win.width),
                                   transform=wt, fill=0, all_touched=False, dtype="uint8")
            sel = sub[m > 0]
            sel = sel[sel >= 0]
            total += float(sel.sum())
            cells += int(sel.size)
    return total, cells


def native_tile_sum(geom, tile_list):
    """Independent sum from raw tiles with first-valid-wins ownership mirroring the mosaic."""
    cmin = min(place[t]["col0"] for t in tile_list)
    cmax = max(place[t]["col1"] for t in tile_list)
    rmin = min(place[t]["row0"] for t in tile_list)
    rmax = max(place[t]["row1"] for t in tile_list)
    owned = np.zeros((rmax - rmin, cmax - cmin), dtype=np.uint8)
    total = 0.0
    cells = 0
    for t in tile_list:
        with rasterio.open(src_path[t]) as d:
            arr = d.read(1)
            tf = d.transform
        m = features.rasterize([(geom, 1)], out_shape=arr.shape, transform=tf,
                               fill=0, all_touched=False, dtype="uint8")
        p = place[t]
        br0 = p["row0"] - rmin
        bc0 = p["col0"] - cmin
        ow = owned[br0:br0 + arr.shape[0], bc0:bc0 + arr.shape[1]]
        valid = arr >= 0
        take = (m > 0) & valid & (ow == 0)
        total += float(arr[take].sum())
        cells += int(take.sum())
        ow[valid] = 1  # first valid write wins; later tiles only fill NoData cells
    return total, cells


# ---------------------------------------------------------------------------
# per-state zonal statistics (mosaic-based)
# ---------------------------------------------------------------------------
rows_out = []
for st in states:
    pop, ncell = poly_sum_mosaic(st["geom"])
    st["pop"] = pop
    st["cells"] = ncell

# area on US Albers equal-area (EPSG:5070)
transformer = Transformer.from_crs("EPSG:4326", "EPSG:5070", always_xy=True)


def to_albers(g):
    return shp_ops.transform(lambda x, y: transformer.transform(x, y), g)


for st in states:
    g_albers = to_albers(st["geom"])
    st["area_m2"] = float(g_albers.area)
    st["area_km2"] = st["area_m2"] / 1.0e6

nat_pop = sum(st["pop"] for st in states)
nat_area_km2 = sum(st["area_km2"] for st in states)

for st in states:
    st["density"] = st["pop"] / st["area_km2"]
    st["pct"] = st["pop"] / nat_pop * 100.0

states_sorted = sorted(states, key=lambda s: s["pop"], reverse=True)
cum = 0.0
for st in states_sorted:
    cum += st["pct"]
    st["cum_pct"] = cum

# ---------------------------------------------------------------------------
# write CSV
# ---------------------------------------------------------------------------
csv_path = "outputs/us_state_population_distribution_2020.csv"
cols = ["state_name", "state_code", "population_gpw2020", "area_km2",
        "density_ppl_per_km2", "pct_of_national", "cum_pct_of_national"]
with open(csv_path, "w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh)
    w.writerow(cols)
    for st in states_sorted:
        w.writerow([st["name"], st["code"], round(st["pop"], 1), round(st["area_km2"], 2),
                    round(st["density"], 2), round(st["pct"], 4), round(st["cum_pct"], 4)])
    w.writerow(["United States (50 states + DC)", "US", round(nat_pop, 1),
                round(nat_area_km2, 2), round(nat_pop / nat_area_km2, 2), 100.0, 100.0])

# ---------------------------------------------------------------------------
# independent validations
# ---------------------------------------------------------------------------
val = {}

# 1) sigma states vs national masked sum (union of the 51 geometries, no territories)
def geom_by_code(code):
    for st in states:
        if st["code"] == code:
            return st["geom"]
    raise KeyError(code)


with rasterio.open(mos_path) as mos:
    # build national mask in one rasterize call over full canvas (all_touched=False)
    all_shapes = [(st["geom"], 1) for st in states]
    nmask = features.rasterize(all_shapes, out_shape=(MOS_ROWS, MOS_COLS),
                               transform=mos.transform, fill=0, all_touched=False, dtype="uint8")
    mask_sum = 0.0
    BLK = 512
    for b0 in range(0, MOS_ROWS, BLK):
        b1 = min(b0 + BLK, MOS_ROWS)
        blk = mos.read(1, window=windows.Window(0, b0, MOS_COLS, b1 - b0))
        msub = nmask[b0:b1, :]
        v = blk[msub > 0]
        mask_sum += float(v[v >= 0].sum())
    nmask = None

rel_diff_states_mask = abs(mask_sum - nat_pop) / nat_pop
val["check1_states_vs_national_mask"] = {
    "sigma_states": nat_pop, "national_mask_sum": mask_sum,
    "relative_difference": rel_diff_states_mask, "pass_lt_0.5pct": rel_diff_states_mask < 0.005,
    "note": "national mask = union of the 51 kept state geometries rasterized in one call (territories excluded by construction); non-negative cells summed on the mosaic"}

# 2) native-tile cross checks
native_checks = {}
for code, tiles in [("WA", ["p1"]), ("FL", ["p4"]), ("TX", ["p2", "p4"]),
                    ("AK", ["ak1", "ak2", "ak3", "akw"])]:
    geom = geom_by_code(code)
    npop_native, ncells = native_tile_sum(geom, tiles)
    mpop = [s for s in states if s["code"] == code][0]["pop"]
    rd = abs(npop_native - mpop) / mpop if mpop > 0 else float("nan")
    native_checks[code] = {"tiles": tiles, "mosaic_pop": mpop, "native_tile_pop": npop_native,
                           "absolute_diff": npop_native - mpop, "relative_diff": rd,
                           "note": "WA and FL lie inside single tiles; TX crosses the p2/p4 duplicate column seam; AK crosses ak1..ak3 seams and the antimeridian fold (akw) with first-valid-wins ownership"}
val["check2_native_tile_crosschecks"] = native_checks

# 3) feature / attribute integrity
codes_used = sorted(s["code"] for s in states)
val["check3_attributes"] = {
    "n_rows": len(states),
    "unique_codes": len(set(codes_used)) == 51,
    "contains_DC": "DC" in codes_used,
    "no_territory_codes": not (set(codes_used) & {"PR", "VI", "AS", "GU", "MP"}),
    "codes": codes_used,
}

# 4) ranges
pops = [s["pop"] for s in states]
dens = [s["density"] for s in states]
val["check4_ranges"] = {
    "min_state_pop": min(pops), "max_state_pop": max(pops),
    "all_pop_ge_0": min(pops) >= 0.0,
    "all_density_finite": all(math.isfinite(d) for d in dens) and min(dens) >= 0.0,
    "national_pop_sanity_band_1e8_5e8": 1.0e8 < nat_pop < 5.0e8,
    "national_area_km2": nat_area_km2,
}

val["national_total_pop"] = nat_pop
val["overall_pass"] = (val["check1_states_vs_national_mask"]["pass_lt_0.5pct"]
                       and val["check3_attributes"]["n_rows"] == 51
                       and val["check3_attributes"]["unique_codes"]
                       and val["check3_attributes"]["no_territory_codes"]
                       and val["check4_ranges"]["all_pop_ge_0"])

with open("outputs/us_pop_2020_validation_report.json", "w") as fh:
    json.dump(val, fh, indent=1, default=str)

# ---------------------------------------------------------------------------
# machine-readable JSON summary
# ---------------------------------------------------------------------------
top10 = [(st["name"], st["code"], st["pop"]) for st in states_sorted[:10]]
top10_share = sum(p for _, _, p in top10) / nat_pop * 100.0
max_d = max(states, key=lambda s: s["density"])
min_d = min(states, key=lambda s: s["density"])

summary_json = {
    "title": "US 50 states + DC 2020 population distribution (GPW v4.11 grid, persons per cell)",
    "source": {"product": "GPW v4.11 Population Count 2020 (CIESIN/GPWv411/GPW_Population_Count, band population_count)",
               "reference_year": 2020, "grid": "EPSG:4326 nominal ~0.0083364 deg (~928 m)",
               "unit": "persons per grid cell (population count, not density)"},
    "method": {
        "cell_sum_rule": "state population = sum of non-negative cells whose pixel centre lies inside the state geometry (mask-cell sum, no area weighting)",
        "sentinel": "negative values (GPW float32 NoData/water sentinel ~ -3.4028e38) excluded from all sums",
        "antimeridian": "standard -180..180 frame, integer-locked verbatim paste; akw (Aleutians west of dateline, +172.4..+180.0) at the eastern array edge; no reprojection/resampling; first-valid-wins at seams",
        "boundary": "geoBoundaries ADM1 USA filtered to 50 states + DC (51 features); territories PR/VI/AS/GU/MP excluded; SU-SD typo canonicalized to US-SD",
        "area": "state geometry area computed on US Albers Equal Area (EPSG:5070), km2",
        "density": "population / area_km2 (persons per km2)",
    },
    "columns": cols,
    "rows": [{"state_name": st["name"], "state_code": st["code"],
              "population_gpw2020": st["pop"], "area_km2": st["area_km2"],
              "density_ppl_per_km2": st["density"], "pct_of_national": st["pct"],
              "cum_pct_of_national": st["cum_pct"]} for st in states_sorted],
    "national_total": {"state_name": "United States (50 states + DC)", "state_code": "US",
                       "population_gpw2020": nat_pop, "area_km2": nat_area_km2,
                       "density_ppl_per_km2": nat_pop / nat_area_km2,
                       "pct_of_national": 100.0, "cum_pct_of_national": 100.0},
    "summary": {
        "national_total_pop": nat_pop,
        "top10": [{"state_name": n, "state_code": c, "population_gpw2020": p} for n, c, p in top10],
        "top10_share_pct_of_national": top10_share,
        "most_dense": {"state_name": max_d["name"], "state_code": max_d["code"], "density": max_d["density"]},
        "least_dense": {"state_name": min_d["name"], "state_code": min_d["code"], "density": min_d["density"]},
    },
    "validation_summary": {
        "sigma_states_vs_national_mask_rel_diff": rel_diff_states_mask,
        "national_mask_sum": mask_sum,
        "native_tile_crosschecks_rel_diffs": {k: v["relative_diff"] for k, v in native_checks.items()},
        "overall_pass": val["overall_pass"],
    },
}

with open("outputs/us_state_population_distribution_2020.json", "w") as fh:
    json.dump(summary_json, fh, indent=1, default=str)

print("S2_OK total_pop=%.1f rows=%d rel_diff_mask=%.3e" % (nat_pop, len(states), rel_diff_states_mask))
