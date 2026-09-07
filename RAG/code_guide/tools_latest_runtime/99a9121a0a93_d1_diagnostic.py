NTL_SCRIPT_CONTRACT = {
    "schema": "ntl.script.contract.v2",
    "objective": "Diagnose rasterize/read window shape mismatch during windowed zonal summation on the national mosaic.",
    "input_manifest": [
        {"path": "outputs/us_pop_gpw2020_national.tif", "role": "staged output mosaic"},
        {"path": "outputs/us_states_51.shp", "role": "staged output boundary"}
    ],
    "method_steps": [
        "iterate states and sub-polygons, compute a clamped canvas window, read mosaic window and rasterize polygon on the same window grid",
        "record the first cases where read and rasterize shapes differ with full diagnostics"
    ],
    "parameters": {},
    "output_manifest": [
        {"path": "outputs/window_shape_diagnostic.json", "required": True}
    ],
    "validation_checks": [
        "diagnostic json exists and reports shape mismatches if any"
    ],
    "failure_gates": [
        "fail if the mosaic or shapefile cannot be opened"
    ],
    "execution": {
        "mode": "execute",
        "timeout_seconds": 1800,
        "overwrite_policy": "version",
        "network_scope": [],
        "test_strategy": "auto",
        "repair_history": []
    }
}

import json
import math

import numpy as np
import rasterio
from rasterio import features, windows
from osgeo import ogr
from shapely.geometry import shape as shp_shape

RES = 0.00833636583662916
mos_path = "outputs/us_pop_gpw2020_national.tif"
with rasterio.open(mos_path) as mos:
    x0 = mos.transform.c
    y_top = mos.transform.f
    MOS_ROWS, MOS_COLS = mos.height, mos.width

ds_v = ogr.Open("outputs/us_states_51.shp")
lyr = ds_v.GetLayer(0)
states = []
for feat in lyr:
    gj = json.loads(feat.GetGeometryRef().ExportToJson())
    states.append({"name": feat.GetField("shapeName"), "code": feat.GetField("shapeISO")[-2:],
                   "geom": shp_shape(gj)})

issues = []
n_checked = 0
with rasterio.open(mos_path) as mos:
    for st in states:
        polys = list(st["geom"].geoms) if st["geom"].geom_type == "MultiPolygon" else [st["geom"]]
        for k, g in enumerate(polys):
            minx, miny, maxx, maxy = g.bounds
            c0 = max(0, int(math.floor((minx - x0) / RES)) - 2)
            c1 = min(MOS_COLS, int(math.ceil((maxx - x0) / RES)) + 2)
            r0 = max(0, int(math.floor((y_top - maxy) / RES)) - 2)
            r1 = min(MOS_ROWS, int(math.ceil((y_top - miny) / RES)) + 2)
            if r1 <= r0 or c1 <= c0:
                continue
            n_checked += 1
            win = windows.Window(c0, r0, c1 - c0, r1 - r0)
            sub = mos.read(1, window=win)
            wt = windows.transform(win, mos.transform)
            m = features.rasterize([(g, 1)], out_shape=(win.height, win.width),
                                   transform=wt, fill=0, all_touched=False, dtype="uint8")
            if sub.shape != m.shape:
                issues.append({
                    "state": st["name"], "poly_index": k,
                    "bbox": [minx, miny, maxx, maxy],
                    "win_cols_rows": [c1 - c0, r1 - r0],
                    "win_off": [c0, r0],
                    "read_shape": list(sub.shape),
                    "mask_shape": list(m.shape),
                })
                if len(issues) >= 5:
                    break
        if len(issues) >= 5:
            break

res = {"n_polygons_checked": n_checked, "n_shape_mismatches": len(issues), "issues": issues}
with open("outputs/window_shape_diagnostic.json", "w") as fh:
    json.dump(res, fh, indent=1)
print("DIAG_OK checked=%d mismatches=%d" % (n_checked, len(issues)))
