NTL_SCRIPT_CONTRACT = {
    "schema": "ntl.script.contract.v2",
    "objective": "Mosaic the 18 validated SRTM90 DEM strips for Myanmar into one EPSG:4326 grid with a documented overlap rule, then reproject to UTM zone 47N at 90 m for metric terrain analysis.",
    "input_manifest": [
        {"path": "inputs/mmr_srtm90_dem_b01.tif", "role": "declared input"},
        {"path": "inputs/mmr_srtm90_dem_b02.tif", "role": "declared input"},
        {"path": "inputs/mmr_srtm90_dem_b03.tif", "role": "declared input"},
        {"path": "inputs/mmr_srtm90_dem_b04.tif", "role": "declared input"},
        {"path": "inputs/mmr_srtm90_dem_b05.tif", "role": "declared input"},
        {"path": "inputs/mmr_srtm90_dem_b06.tif", "role": "declared input"},
        {"path": "inputs/mmr_srtm90_dem_b07.tif", "role": "declared input"},
        {"path": "inputs/mmr_srtm90_dem_b08.tif", "role": "declared input"},
        {"path": "inputs/mmr_srtm90_dem_b09.tif", "role": "declared input"},
        {"path": "inputs/mmr_srtm90_dem_b10.tif", "role": "declared input"},
        {"path": "inputs/mmr_srtm90_dem_b11.tif", "role": "declared input"},
        {"path": "inputs/mmr_srtm90_dem_b12.tif", "role": "declared input"},
        {"path": "inputs/mmr_srtm90_dem_b13.tif", "role": "declared input"},
        {"path": "inputs/mmr_srtm90_dem_b14.tif", "role": "declared input"},
        {"path": "inputs/mmr_srtm90_dem_b15.tif", "role": "declared input"},
        {"path": "inputs/mmr_srtm90_dem_b16.tif", "role": "declared input"},
        {"path": "inputs/mmr_srtm90_dem_b17.tif", "role": "declared input"},
        {"path": "inputs/mmr_srtm90_dem_b18.tif", "role": "declared input"}
    ],
    "method_steps": [
        "Read geotransforms of all 18 strips and verify a common global column grid",
        "Allocate global int16 DEM grid covering the union of strip extents with the common pixel size",
        "Place each strip by rounded pixel-center row offset; in vertical overlaps the higher-numbered (more northern) strip takes priority and its values overwrite the lower strip",
        "Compute overlap statistics (rows and value agreement) for audit",
        "Reproject the mosaic to EPSG:32647 (UTM 47N) at 90 m x/y using bilinear resampling and keep NoData=-32768"
    ],
    "parameters": {
        "overlap_rule": "later/higher band number wins in overlap (posterior-priority)",
        "target_crs": "EPSG:32647",
        "target_resolution_m": 90.0,
        "src_nodata": -32768,
        "resampling": "bilinear"
    },
    "output_manifest": [
        {"path": "outputs/myanmar_dem_mosaic_4326.tif", "required": True},
        {"path": "outputs/myanmar_dem_utm47n_90m.tif", "required": True},
        {"path": "outputs/myanmar_mosaic_report.json", "required": True}
    ],
    "validation_checks": [
        "mosaic GeoTIFF exists with expected dimensions and CRS EPSG:4326",
        "UTM DEM exists with CRS EPSG:32647 and 90 m resolution",
        "report JSON records strip row offsets, overlaps, gap check and per-strip fill counts"
    ],
    "failure_gates": [
        "fail if any horizontal gap (rows not covered by any strip) is detected",
        "fail if the mosaic or UTM output is missing or empty"
    ],
    "execution": {
        "mode": "execute",
        "timeout_seconds": 2400,
        "overwrite_policy": "version",
        "network_scope": [],
        "test_strategy": "auto",
        "repair_history": [{"reason": "coverage audit iterated over strip metadata that has no row_start; offsets live in report strips", "before": "coverage[m[\"row_start\"]...] for m in meta", "after": "coverage[m[\"row_start\"]...] for m in report[\"strips\"]"}]
    }
}

import json
import os

import numpy as np
import rasterio
from rasterio.transform import Affine
from osgeo import gdal

NODATA = -32768
TILE_FILES = ["inputs/mmr_srtm90_dem_b%02d.tif" % i for i in range(1, 19)]

# ---- 1) gather strip metadata ----
meta = []
for t in TILE_FILES:
    with rasterio.open(t) as ds:
        meta.append({
            "file": t,
            "width": ds.width,
            "height": ds.height,
            "crs": ds.crs.to_string(),
            "res": ds.res,
            "bounds": list(ds.bounds),
            "nodata": ds.nodata,
            "dtype": ds.dtypes[0],
            "transform": ds.transform
        })

# ---- 2) global grid from union ----
left = min(m["bounds"][0] for m in meta)
right = max(m["bounds"][2] for m in meta)
bottom = min(m["bounds"][1] for m in meta)
top = max(m["bounds"][3] for m in meta)
res = meta[0]["res"][0]

# all strips must share one column grid
for m in meta:
    assert abs(m["bounds"][0] - left) < 1e-9, "x0 differs between strips"
    assert abs(m["res"][0] - res) < 1e-12, "pixel size differs"

cols = meta[0]["width"]
rows = int(round((top - bottom) / res))
# snap so that rows*res >= extent
while bottom + rows * res < top - 1e-9:
    rows += 1

transform = Affine(res, 0.0, left, 0.0, -res, top)

report = {
    "global_grid": {
        "left": left, "right": right, "bottom": bottom, "top": top,
        "pixel_deg": res, "cols": cols, "rows": rows
    },
    "strips": []
}

# ---- 3) fill mosaic with priority of higher band number in overlaps ----
mosaic = np.full((rows, cols), NODATA, dtype=np.int16)

for idx, m in enumerate(meta):
    row_start = int(round((top - m["bounds"][3]) / res))
    row_end = row_start + m["height"]
    if row_start < 0 or row_end > rows:
        raise ValueError("strip %s does not fit into global grid" % m["file"])
    with rasterio.open(m["file"]) as ds:
        arr = ds.read(1)
    valid = arr != NODATA
    target = mosaic[row_start:row_end, :]
    # posterior (higher number / northern) priority: overwrite where new data valid
    target[valid] = arr[valid]
    report["strips"].append({
        "file": m["file"],
        "height_px": m["height"],
        "row_start": row_start,
        "row_end": row_end,
        "valid_px": int(valid.sum())
    })

# ---- 4) gap / overlap audit (rows must be fully covered with no gaps) ----
coverage = np.zeros(rows, dtype=np.int64)
for m in report["strips"]:
    coverage[m["row_start"]:m["row_end"]] += 1
report["coverage_min_overlaps"] = int(coverage.min())
report["coverage_max_overlaps"] = int(coverage.max())
report["rows_without_coverage"] = int((coverage == 0).sum())
if report["rows_without_coverage"] > 0:
    raise RuntimeError("gaps detected in mosaic coverage")

# overlap value agreement (sample the top overlap of each pair)
agreement = []
order = sorted(range(len(meta)), key=lambda i: -meta[i]["bounds"][3])
for a, b in zip(order, order[1:]):
    ma, mb = meta[a], meta[b]
    rs_a, re_a = report["strips"][a]["row_start"], report["strips"][a]["row_end"]
    rs_b, re_b = report["strips"][b]["row_start"], report["strips"][b]["row_end"]
    lo = max(rs_a, rs_b)
    hi = min(re_a, re_b)
    if hi <= lo:
        continue
    with rasterio.open(ma["file"]) as da, rasterio.open(mb["file"]) as db:
        a_data = da.read(1)[lo - rs_a:hi - rs_a, :]
        b_data = db.read(1)[lo - rs_b:hi - rs_b, :]
    both = (a_data != NODATA) & (b_data != NODATA)
    if both.sum() == 0:
        continue
    diff = np.abs(a_data[both].astype(np.int64) - b_data[both].astype(np.int64))
    agreement.append({
        "pair": "%s|%s" % (ma["file"].split("/")[-1], mb["file"].split("/")[-1]),
        "overlap_rows": int(hi - lo),
        "compared_px": int(both.sum()),
        "max_abs_diff_m": int(diff.max()) if diff.size else None,
        "mean_abs_diff_m": float(diff.mean()) if diff.size else None,
        "frac_identical": float((diff == 0).mean()) if diff.size else None
    })
report["overlap_agreement"] = agreement

# ---- 5) write mosaic ----
profile = {
    "driver": "GTiff", "width": cols, "height": rows, "count": 1,
    "dtype": "int16", "crs": "EPSG:4326", "transform": transform,
    "nodata": NODATA, "tiled": True, "blockxsize": 256, "blockysize": 256,
    "compress": "deflate", "predictor": 2, "bigtiff": "IF_SAFER"
}
mos_path = "outputs/myanmar_dem_mosaic_4326.tif"
with rasterio.open(mos_path, "w", **profile) as dst:
    dst.write(mosaic, 1)
del mosaic

report["mosaic_path"] = mos_path
report["mosaic_bytes"] = os.path.getsize(mos_path)

# ---- 6) reproject to UTM 47N 90 m ----
src_ds = gdal.Open(mos_path, gdal.GA_ReadOnly)
dst_path = "outputs/myanmar_dem_utm47n_90m.tif"
if os.path.exists(dst_path):
    os.remove(dst_path)
warp_opts = gdal.WarpOptions(
    format="GTiff",
    dstSRS="EPSG:32647",
    xRes=90.0, yRes=90.0,
    resampleAlg=gdal.GRA_Bilinear,
    srcNodata=NODATA, dstNodata=NODATA,
    creationOptions=["TILED=YES", "COMPRESS=DEFLATE", "PREDICTOR=2", "BIGTIFF=IF_SAFER"],
    outputType=gdal.GDT_Int16
)
out_ds = gdal.Warp(dst_path, src_ds, options=warp_opts)
if out_ds is None:
    raise RuntimeError("gdal.Warp to UTM failed")
out_ds.FlushCache()
gt = out_ds.GetGeoTransform()
report["utm47n"] = {
    "path": dst_path,
    "crs": out_ds.GetProjection()[:120],
    "cols": out_ds.RasterXSize,
    "rows": out_ds.RasterYSize,
    "geotransform": list(gt),
    "bytes": os.path.getsize(dst_path)
}
out_ds = None
src_ds = None

with open("outputs/myanmar_mosaic_report.json", "w", encoding="utf-8") as fh:
    json.dump(report, fh, ensure_ascii=False, indent=1, default=str)

print(json.dumps(report, ensure_ascii=False, indent=1, default=str)[:3000])
