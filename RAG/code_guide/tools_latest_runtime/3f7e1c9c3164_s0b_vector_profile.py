NTL_SCRIPT_CONTRACT = {
    "schema": "ntl.script.contract.v2",
    "objective": "Inspect Alaska geometry ring structure and all-feature geometry types to decide antimeridian-safe rasterization design.",
    "input_manifest": [
        {"path": "inputs/us_states_boundary.shp", "role": "declared input"}
    ],
    "method_steps": [
        "read raw shapefile parts with pyshp and report per-part longitude extents for Alaska",
        "read geometries through osgeo.ogr and report OGR geometry type per feature and GeoJSON geometry type per feature",
        "detect any ring or OGR polygon part spanning the antimeridian fold (vertices on both sides of +/-180)"
    ],
    "parameters": {},
    "output_manifest": [
        {"path": "outputs/vector_geom_profile.json", "required": True}
    ],
    "validation_checks": [
        "vector_geom_profile.json exists, parses, and contains per-feature ogr geometry types and Alaska ring statistics",
        "56 features enumerated"
    ],
    "failure_gates": [
        "fail if the shapefile cannot be read or the profile cannot be written"
    ],
    "execution": {
        "mode": "execute",
        "timeout_seconds": 900,
        "overwrite_policy": "version",
        "network_scope": [],
        "test_strategy": "auto",
        "repair_history": []
    }
}

import json

import shapefile as shp
from osgeo import ogr

OUT = {}

# ---- pyshp raw part structure for every feature ----
sf = shp.Reader("inputs/us_states_boundary.shp")
fields = [f[0] for f in sf.fields[1:]]
recs = list(sf.records())
fcount = len(sf.shapes())
OUT["n_features"] = fcount
per_feat = []
for idx, (s, r) in enumerate(zip(sf.shapes(), recs)):
    rec = dict(zip(fields, r))
    name = rec.get("shapeName")
    iso = rec.get("shapeISO")
    parts_idx = list(s.parts) + [len(s.points)]
    ring_stats = []
    for k in range(len(s.parts)):
        pts = s.points[parts_idx[k]:parts_idx[k + 1]]
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        ring_stats.append({
            "ring": k,
            "n_vert": len(pts),
            "minx": float(min(xs)),
            "maxx": float(max(xs)),
            "miny": float(min(ys)),
            "maxy": float(max(ys)),
            "has_pos": any(x > 0 for x in xs),
            "has_neg": any(x < 0 for x in xs),
        })
    entry = {"name": name, "iso": iso, "n_rings": len(s.parts), "rings": ring_stats}
    per_feat.append(entry)
OUT["pyshp_rings"] = per_feat

# ---- osgeo.ogr geometry types ----
ogr_ds = ogr.Open("inputs/us_states_boundary.shp")
lyr = ogr_ds.GetLayer(0)
ogr_types = []
for feat in lyr:
    g = feat.GetGeometryRef()
    name = feat.GetField("shapeName")
    iso = feat.GetField("shapeISO")
    gj = json.loads(g.ExportToJson())
    ogr_types.append({"name": name, "iso": iso,
                      "ogr_type": g.GetGeometryName(),
                      "geojson_type": gj.get("type"),
                      "n_geojson_polys": len(gj.get("coordinates", [])) if gj.get("type") == "MultiPolygon" else 1})
OUT["ogr_types"] = ogr_types
ogr_ds = None

# quick global sanity: geometry ring that spans both >170 and <-170 (fold-crossing)
fold_issues = []
for entry in per_feat:
    for r in entry["rings"]:
        if r["has_pos"] and r["has_neg"] and (r["maxx"] > 150.0 and r["minx"] < -150.0):
            fold_issues.append({"feature": entry["name"], "ring": r["ring"],
                                "minx": r["minx"], "maxx": r["maxx"]})
OUT["fold_crossing_rings"] = fold_issues

with open("outputs/vector_geom_profile.json", "w") as fh:
    json.dump(OUT, fh, indent=1, default=str)

print("VECPROFILE_OK n=%d fold_crossing_rings=%d" % (fcount, len(fold_issues)))
