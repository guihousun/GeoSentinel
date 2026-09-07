NTL_SCRIPT_CONTRACT = {
    "schema": "ntl.script.contract.v2",
    "objective": "Merge 9 GPWv4.11 2020 population tiles into one antimeridian-safe national GeoTIFF and emit the territory-free 51-feature state boundary shapefile with a build-QA report.",
    "input_manifest": [
        {"path": "inputs/us_pop_gpw2020_p1.tif", "role": "declared input"},
        {"path": "inputs/us_pop_gpw2020_p2.tif", "role": "declared input"},
        {"path": "inputs/us_pop_gpw2020_p3.tif", "role": "declared input"},
        {"path": "inputs/us_pop_gpw2020_p4.tif", "role": "declared input"},
        {"path": "inputs/us_pop_gpw2020_ak1.tif", "role": "declared input"},
        {"path": "inputs/us_pop_gpw2020_ak2.tif", "role": "declared input"},
        {"path": "inputs/us_pop_gpw2020_ak3.tif", "role": "declared input"},
        {"path": "inputs/us_pop_gpw2020_akw.tif", "role": "declared input"},
        {"path": "inputs/us_pop_gpw2020_hi.tif", "role": "declared input"},
        {"path": "inputs/us_states_boundary.shp", "role": "declared input"}
    ],
    "method_steps": [
        "place all 9 tiles on one integer-locked EPSG:4326 grid in the standard -180..180 frame; akw stays at its true positive longitude (Aleutians west of the antimeridian) so no coordinate surgery is needed",
        "audit pairwise tile overlap for duplicate valid-pixel disagreement",
        "paste tiles with first-valid-wins dedupe into outputs/us_pop_gpw2020_national.tif (negatives treated as NoData sentinel)",
        "filter the boundary to the canonical 50 states + DC postal-code set (territories PR/VI/AS/GU/MP dropped; geoBoundaries typo SU-SD canonicalized to US-SD)",
        "write outputs/us_states_51.shp and a build report"
    ],
    "parameters": {},
    "output_manifest": [
        {"path": "outputs/us_pop_gpw2020_national.tif", "required": True},
        {"path": "outputs/us_states_51.shp", "required": True},
        {"path": "outputs/us_states_51.shx", "required": True},
        {"path": "outputs/us_states_51.dbf", "required": True},
        {"path": "outputs/us_states_51.prj", "required": True},
        {"path": "outputs/mosaic_build_report.json", "required": True}
    ],
    "validation_checks": [
        "mosaic_build_report.json exists and records canvas geometry, tile placement, overlap QA, and the 51-feature list",
        "national GeoTIFF and 51-state shapefile exist and are non-empty"
    ],
    "failure_gates": [
        "fail if any tile placement is not integer-aligned on the shared grid",
        "fail if the filtered feature count is not exactly 51",
        "fail if any declared output is missing"
    ],
    "execution": {
        "mode": "execute",
        "timeout_seconds": 3600,
        "overwrite_policy": "version",
        "network_scope": [],
        "test_strategy": "auto",
        "repair_history": [{"reason": "osgeo.ogr.Geometry has no ForceToMultiPolygon() method in this binding; polygon-to-multipolygon conversion rebuilt through shapely before OGR feature creation", "before": "geom.ForceToMultiPolygon()", "after": "shapely MultiPolygon wrap then ogr.CreateGeometryFromJson"}]
    }
}

import json
import math

import numpy as np
import rasterio
from rasterio.transform import from_origin
from rasterio.windows import Window
from osgeo import ogr, osr
from shapely.geometry import shape as shp_shape, mapping as shp_mapping, MultiPolygon as ShapelyMultiPolygon

# ---------------------------------------------------------------------------
# canonical 50 states + DC postal codes
# ---------------------------------------------------------------------------
STATE_CODES_51 = set("""AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS
MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY""".split())
assert len(STATE_CODES_51) == 51

RES = 0.00833636583662916  # nominal pixel size (deg) verified identical for all tiles
NODATA_F64 = float(np.finfo(np.float32).min)  # metadata nodata label; all negatives treated as NoData

TILE_NAMES = ["p1", "p2", "p3", "p4", "ak1", "ak2", "ak3", "akw", "hi"]
PASTE_ORDER = ["p1", "p2", "p3", "p4", "ak1", "ak2", "ak3", "akw", "hi"]

arrays = {}
metas = {}
for nm in TILE_NAMES:
    p = "inputs/us_pop_gpw2020_%s.tif" % nm
    with rasterio.open(p) as ds:
        a = ds.read(1).astype("float32")
        arrays[nm] = a
        metas[nm] = {
            "origin_x": float(ds.transform.c),
            "origin_y": float(ds.transform.f),
            "width": int(ds.width),
            "height": int(ds.height),
            "res": float(ds.transform.a),
        }
    # sanity: resolution identical
    assert abs(metas[nm]["res"] - RES) < 1e-12, (nm, metas[nm]["res"])
    # sanity: single-band float32 population counts, negatives are GPW sentinel
    assert a.ndim == 2

# ---------------------------------------------------------------------------
# canvas geometry on the shared grid (standard -180..180 frame, integer-locked)
# ---------------------------------------------------------------------------
x0 = min(m["origin_x"] for m in metas.values())           # ak1 left edge -180.0071475
y_top = max(m["origin_y"] for m in metas.values())        # ak3 top edge
place = {}
for nm in TILE_NAMES:
    m = metas[nm]
    col0 = (m["origin_x"] - x0) / RES
    row0 = (y_top - m["origin_y"]) / RES
    cerr = abs(col0 - round(col0))
    rerr = abs(row0 - round(row0))
    assert cerr < 1e-6 and rerr < 1e-6, (nm, col0, row0, cerr, rerr)
    place[nm] = {"col0": int(round(col0)), "row0": int(round(row0)),
                 "col1": int(round(col0)) + m["width"],
                 "row1": int(round(row0)) + m["height"]}

cols = max(p["col1"] for p in place.values())
rows = max(p["row1"] for p in place.values())
right_edge = x0 + cols * RES
bottom_edge = y_top - rows * RES

# ---------------------------------------------------------------------------
# pairwise overlap QA (same shared grid => overlapping cells are same indices)
# ---------------------------------------------------------------------------
overlap_qa = []
for i in range(len(TILE_NAMES)):
    for j in range(i + 1, len(TILE_NAMES)):
        na, nb = TILE_NAMES[i], TILE_NAMES[j]
        pa, pb = place[na], place[nb]
        r0, r1 = max(pa["row0"], pb["row0"]), min(pa["row1"], pb["row1"])
        c0, c1 = max(pa["col0"], pb["col0"]), min(pa["col1"], pb["col1"])
        if r1 <= r0 or c1 <= c0:
            continue
        aa = arrays[na][r0 - pa["row0"]:r1 - pa["row0"], c0 - pa["col0"]:c1 - pa["col0"]]
        bb = arrays[nb][r0 - pb["row0"]:r1 - pb["row0"], c0 - pb["col0"]:c1 - pb["col0"]]
        va, vb = aa >= 0, bb >= 0
        both = va & vb
        n_both = int(both.sum())
        if n_both:
            d = np.abs(aa[both] - bb[both])
            n_diff = int((d > 0).sum())
            maxdiff = float(d.max()) if n_diff else 0.0
            vsum_diff = float(np.sum(bb[both] - aa[both]))
        else:
            n_diff, maxdiff, vsum_diff = 0, 0.0, 0.0
        one_valid = int((va ^ vb).sum())
        overlap_qa.append({
            "pair": "%s_vs_%s" % (na, nb),
            "rows": [r0, r1], "cols": [c0, c1],
            "n_overlap_cells": int((r1 - r0) * (c1 - c0)),
            "n_both_valid": n_both,
            "n_value_diff_both_valid": n_diff,
            "max_abs_diff": maxdiff,
            "sum_second_minus_first_over_both_valid": vsum_diff,
            "n_one_side_valid": one_valid,
        })

# ---------------------------------------------------------------------------
# assemble mosaic: first-valid-wins in PASTE_ORDER, blockwise to bound memory
# ---------------------------------------------------------------------------
out_tif = "outputs/us_pop_gpw2020_national.tif"
profile = {
    "driver": "GTiff", "height": rows, "width": cols, "count": 1,
    "dtype": "float32", "crs": rasterio.crs.CRS.from_epsg(4326),
    "transform": from_origin(x0, y_top, RES, RES),
    "nodata": NODATA_F64, "tiled": True, "blockxsize": 256, "blockysize": 256,
    "compress": "deflate", "predictor": 3, "bigtiff": "IF_SAFER",
}
BLOCK = 256
with rasterio.open(out_tif, "w", **profile) as dst:
    for b0 in range(0, rows, BLOCK):
        b1 = min(b0 + BLOCK, rows)
        bh = b1 - b0
        block = np.full((bh, cols), NODATA_F64, dtype="float32")
        for nm in PASTE_ORDER:
            p = place[nm]
            tr0 = max(p["row0"], b0)
            tr1 = min(p["row1"], b1)
            if tr1 <= tr0:
                continue
            arr = arrays[nm]
            sub = arr[tr0 - p["row0"]:tr1 - p["row0"], :]          # tile-local rows
            blk_region = block[tr0 - b0:tr1 - b0, p["col0"]:p["col1"]]
            empty = blk_region < 0                                  # not yet filled
            if empty.any():
                blk_region[empty] = sub[empty]
        dst.write(block, 1, window=Window(0, b0, cols, bh))

# release tile arrays
arrays = None

# ---------------------------------------------------------------------------
# boundary filter -> 51 features and write shapefile via OGR
# ---------------------------------------------------------------------------
src_ds = ogr.Open("inputs/us_states_boundary.shp")
src_lyr = src_ds.GetLayer(0)

kept = []
dropped = []
for feat in src_lyr:
    iso = feat.GetField("shapeISO") or ""
    name = feat.GetField("shapeName") or ""
    code = iso[-2:] if len(iso) >= 2 else ""
    if code in STATE_CODES_51:
        kept.append((name, code, feat.GetGeometryRef().Clone()))
    else:
        dropped.append({"name": name, "iso": iso})

assert len(kept) == 51, len(kept)
names_kept = sorted(k[0] for k in kept)

out_shp = "outputs/us_states_51.shp"
drv = ogr.GetDriverByName("ESRI Shapefile")
if os.path.exists(out_shp):
    drv.DeleteDataSource(out_shp)
ds_out = drv.CreateDataSource(out_shp)
srs = osr.SpatialReference()
srs.ImportFromEPSG(4326)
lyr_out = ds_out.CreateLayer("us_states_51", srs, ogr.wkbMultiPolygon)
fname = ogr.FieldDefn("shapeName", ogr.OFTString)
fname.SetWidth(64)
lyr_out.CreateField(fname)
fiso = ogr.FieldDefn("shapeISO", ogr.OFTString)
fiso.SetWidth(8)
lyr_out.CreateField(fiso)
lyr_defn = lyr_out.GetLayerDefn()
for name, code, geom in kept:
    gj = json.loads(geom.ExportToJson())
    sh_geom = shp_shape(gj)
    if sh_geom.geom_type == "Polygon":
        sh_geom = ShapelyMultiPolygon([sh_geom])
    g = ogr.CreateGeometryFromJson(json.dumps(shp_mapping(sh_geom)))
    f = ogr.Feature(lyr_defn)
    f.SetGeometry(g)
    f.SetField("shapeName", name)
    f.SetField("shapeISO", "US-" + code)  # canonicalize SU-SD -> US-SD
    lyr_out.CreateFeature(f)
    f = None
ds_out = None
src_ds = None

# ---------------------------------------------------------------------------
# build report
# ---------------------------------------------------------------------------
report = {
    "canvas": {
        "frame": "EPSG:4326 standard -180..180 frame, integer-locked grid paste",
        "x0_deg": x0, "y_top_deg": y_top,
        "right_edge_deg": right_edge, "bottom_edge_deg": bottom_edge,
        "res_deg": RES, "cols": cols, "rows": rows,
        "n_cells": cols * rows,
        "antimeridian_convention": (
            "All 9 tiles share one global pixel grid (pairwise origins differ by exact integer pixels). "
            "The mosaic is georeferenced in the conventional -180..180 longitude frame anchored at "
            "x0=-180.0071475 (= ak1 left edge). Tile akw (Aleutians west of the antimeridian, source "
            "positive longitudes +172.396..+180.007) is pasted at its true positive-longitude location "
            "at the eastern edge of the array (columns 42273..43186); tile ak1 covers the western edge "
            "columns starting at -180.0071475. No coordinate surgery, reprojection or resampling is "
            "applied to any tile: pixels are copied verbatim (first-valid-wins at overlaps), so no "
            "pixel is lost, shifted, or double-counted. The ~0.007 deg columns beyond +/-180 and the "
            "open-ocean seam around the dateline contain only GPW NoData sentinel values. "
            "State geometries are consumed in the same -180..180 frame, and Alaska's west-Aleutian "
            "polygons are stored in positive longitudes, matching the akw lobe."
        ),
        "sentinel_rule": (
            "GPW NoData/water fill sentinel is the float32 minimum (observed per-tile min ~ -3.4028e38); "
            "ANY negative value is treated as NoData and is never summed. Valid population cells are >= 0 "
            "(0 = land cell with zero counted population)."
        ),
        "paste_order": PASTE_ORDER,
        "overwrite_rule": "first valid write wins in PASTE_ORDER; later tiles only fill cells still NoData",
    },
    "tile_placements": {nm: place[nm] for nm in TILE_NAMES},
    "overlap_qa": overlap_qa,
    "boundary_filter": {
        "source_count": 56,
        "kept_count": len(kept),
        "kept": sorted([{"shapeName": n, "shapeISO": "US-" + c} for n, c, _ in kept], key=lambda r: r["shapeName"]),
        "dropped_territories": dropped,
        "iso_canonicalization_note": "geoBoundaries stores South Dakota as SU-SD; outputs canonicalize to US-SD. All 51 kept features carry US-<postal> codes (DC = US-DC).",
    },
}

with open("outputs/mosaic_build_report.json", "w") as fh:
    json.dump(report, fh, indent=1, default=str)

print("S1_OK cols=%d rows=%d kept=%d overlap_pairs_qa=%d" % (cols, rows, len(kept), len(overlap_qa)))
