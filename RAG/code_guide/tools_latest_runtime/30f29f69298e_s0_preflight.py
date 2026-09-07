NTL_SCRIPT_CONTRACT = {
    "schema": "ntl.script.contract.v2",
    "objective": "Characterize staged GPWv4.11 2020 population tiles and the geoBoundaries USA ADM1 boundary to finalize the national mosaic and zonal-statistics design.",
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
        "probe python module availability via importlib",
        "read each tif band and report dtype, transform, res, min, max, counts of negative/sentinel/zero/positive values",
        "report grid phase alignment of tile origins modulo pixel size",
        "list all boundary features with shapeName/shapeISO and geometry type",
        "report per-part longitude bounds of the Alaska feature and any part spanning the antimeridian fold"
    ],
    "parameters": {},
    "output_manifest": [
        {"path": "outputs/preflight_report.json", "required": True}
    ],
    "validation_checks": [
        "preflight_report.json exists, parses, and contains per-tile and vector sections",
        "every staged input listed in the report was actually opened"
    ],
    "failure_gates": [
        "fail if any staged input cannot be opened or the report cannot be written"
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
import importlib.util

RPT = {}

# ---------- module availability ----------
avail = {}
for m in ["numpy", "rasterio", "fiona", "shapely", "pyproj", "geopandas",
          "pandas", "matplotlib", "shapefile", "scipy"]:
    avail[m] = importlib.util.find_spec(m) is not None
try:
    import osgeo  # noqa
    avail["osgeo"] = True
except Exception:
    avail["osgeo"] = False
RPT["modules"] = avail

# ---------- rasters ----------
import numpy as np

TILES = ["p1", "p2", "p3", "p4", "ak1", "ak2", "ak3", "akw", "hi"]
rasterio = None
if avail.get("rasterio"):
    import rasterio

ras = {}
if rasterio is not None:
    for name in TILES:
        p = "inputs/us_pop_gpw2020_%s.tif" % name
        with rasterio.open(p) as ds:
            a = ds.read(1)
            t = ds.transform
            info = {
                "path": p,
                "crs": str(ds.crs),
                "dtype": str(a.dtype),
                "width": int(ds.width),
                "height": int(ds.height),
                "res": [float(t.a), float(-t.e)],
                "origin_x": float(t.c),
                "origin_y": float(t.f),
                "right": float(t.c + t.a * ds.width),
                "bottom": float(t.f + t.e * ds.height),
                "min": float(np.min(a)),
                "max": float(np.max(a)),
                "n_negative": int(np.count_nonzero(a < 0)),
                "n_eq_sentinel": int(np.count_nonzero(a == -3.4028234663852886e38)),
                "n_zero": int(np.count_nonzero(a == 0)),
                "n_positive": int(np.count_nonzero(a > 0)),
                "n_finite": int(np.count_nonzero(np.isfinite(a))),
            }
            # phase of origin modulo pixel size
            info["phase_x_frac"] = float((t.c / t.a) - round(t.c / t.a))
            info["phase_y_frac"] = float((t.f / t.e) - round(t.f / t.e))
            ras[name] = info
    # alignment between each pair in x and y: (ox_a-ox_b)/res closeness to integer
    keys = list(TILES)
    align = {}
    for i in range(len(keys)):
        for j in range(i + 1, len(keys)):
            a, b = keys[i], keys[j]
            dx = (ras[a]["origin_x"] - ras[b]["origin_x"]) / ras[a]["res"][0]
            dy = (ras[a]["origin_y"] - ras[b]["origin_y"]) / ras[a]["res"][1]
            align["%s_vs_%s" % (a, b)] = {
                "x_px_off": float(dx),
                "x_round_err": float(abs(dx - round(dx))),
                "y_px_off": float(dy),
                "y_round_err": float(abs(dy - round(dy))),
            }
    RPT["rasters"] = ras
    RPT["alignment_pairs"] = align
else:
    RPT["rasters"] = {"error": "rasterio unavailable"}

# ---------- vector ----------
vec = {"features": [], "error": None}
shapefile_lib = None
if avail.get("fiona"):
    import fiona
    with fiona.open("inputs/us_states_boundary.shp") as src:
        vec["crs"] = str(src.crs)
        vec["driver"] = src.driver
        fields = list(src.schema["properties"].keys())
        vec["fields"] = fields
        feats = list(src)
        vec["count"] = len(feats)
        for f in feats:
            props = f["properties"]
            geom = f["geometry"]
            name = props.get("shapeName")
            iso = props.get("shapeISO")
            rec = {"shapeName": name, "shapeISO": iso,
                   "geom_type": geom["type"]}
            if name is not None and "Alaska" in str(name):
                rec["_is_alaska"] = True
                bbox_list = []
                coords = geom["coordinates"]
                polys = coords if geom["type"] == "MultiPolygon" else [coords]
                for poly in polys:
                    rings = poly if geom["type"] == "MultiPolygon" else [poly]
                    for ring in rings:
                        xs = [c[0] for c in ring]
                        ys = [c[1] for c in ring]
                        bbox_list.append({"minx": float(min(xs)), "maxx": float(max(xs)),
                                          "miny": float(min(ys)), "maxy": float(max(ys)),
                                          "n_vert": len(ring),
                                          "has_pos": any(x > 0 for x in xs),
                                          "has_neg": any(x < 0 for x in xs)})
                rec["alaska_rings_bbox"] = bbox_list
            vec["features"].append(rec)
elif avail.get("shapefile"):
    shapefile_lib = "pyshp"
    import shapefile as shp
    sf = shp.Reader("inputs/us_states_boundary.shp")
    fields = [f[0] for f in sf.fields[1:]]
    vec["fields"] = fields
    feats = list(zip(sf.shapes(), sf.records()))
    vec["count"] = len(feats)
    for s, r in zip(sf.shapes(), sf.records()):
        rec = dict(zip(fields, r))
        name = rec.get("shapeName")
        iso = rec.get("shapeISO")
        geom_type = s.shapeTypeName
        out = {"shapeName": name, "shapeISO": iso, "geom_type": geom_type}
        vec["features"].append(out)
else:
    vec["error"] = "no fiona or pyshp available"
RPT["vector"] = vec
RPT["shapefile_lib"] = shapefile_lib

with open("outputs/preflight_report.json", "w") as fh:
    json.dump(RPT, fh, indent=1, default=str)

# minimal in-run gate
if rasterio is None:
    raise SystemExit("rasterio unavailable - cannot proceed")
if vec["count"] is None:
    raise SystemExit("vector read failed - cannot proceed")

print("PREFLIGHT_OK vector_features=%s" % vec.get("count"))
