NTL_SCRIPT_CONTRACT = {
    "schema": "ntl.script.contract.v2",
    "objective": "Probe available python geospatial libraries, CJK fonts, DEM tile geometry alignment, and admin1 names for the Myanmar terrain-population analysis.",
    "input_manifest": [
        {"path": "inputs/mmr_srtm90_dem_b01.tif", "role": "declared input"},
        {"path": "inputs/mmr_srtm90_dem_b18.tif", "role": "declared input"},
        {"path": "inputs/mmr_admin1.shp", "role": "declared input"}
    ],
    "method_steps": [
        "Probe importability and versions of numpy/scipy/pandas/rasterio/fiona/geopandas/osgeo/shapely/pyproj/matplotlib/PIL",
        "List candidate CJK font families available to matplotlib",
        "Read geotransform and bounds of DEM tiles b01..b18 and report global grid alignment and overlaps",
        "Read admin1 shapeName attribute list through any available vector reader"
    ],
    "parameters": {},
    "output_manifest": [{"path": "outputs/myanmar_env_probe.json", "required": True}],
    "validation_checks": [
        "outputs/myanmar_env_probe.json exists, is valid JSON and contains versions, tiles, fonts and admin1 names"
    ],
    "failure_gates": [
        "fail if the probe output cannot be written or does not contain the tile geometry table"
    ],
    "execution": {
        "mode": "execute",
        "timeout_seconds": 600,
        "overwrite_policy": "version",
        "network_scope": [],
        "test_strategy": "auto",
        "repair_history": []
    }
}

import importlib
import json
import math
import sys

report = {
    "python": sys.version,
    "modules": {},
    "cjk_fonts": [],
    "tiles": [],
    "admin1_names": [],
    "global_grid": None
}

mods = ["numpy", "scipy", "pandas", "rasterio", "fiona", "geopandas",
        "osgeo", "shapely", "pyproj", "matplotlib", "PIL", "skimage"]
for m in mods:
    try:
        mod = importlib.import_module(m)
        report["modules"][m] = getattr(mod, "__version__", "available")
    except Exception as exc:  # noqa: BLE001
        report["modules"][m] = "MISSING: %s" % type(exc).__name__

try:
    from matplotlib import font_manager
    seen = set()
    for f in font_manager.fontManager.ttflist:
        name = f.name
        if name not in seen and any(k in name for k in
                                    ("CJK", "Hei", "Song", "Yai", "YaHei", "Noto", "WenQuanYi", "SimSun", "Kai", "Ming", "Han")):
            seen.add(name)
            report["cjk_fonts"].append(name)
except Exception as exc:  # noqa: BLE001
    report["cjk_fonts"].append("font probe failed: %s" % type(exc).__name__)

# --- tile geometry ---
try:
    import rasterio
    tiles = ["inputs/mmr_srtm90_dem_b%02d.tif" % i for i in range(1, 19)]
    geoms = []
    for t in tiles:
        with rasterio.open(t) as ds:
            geoms.append({
                "tile": t,
                "width": ds.width,
                "height": ds.height,
                "crs": str(ds.crs),
                "res": ds.res,
                "bounds": list(ds.bounds),
                "nodata": ds.nodata,
                "dtype": str(ds.dtypes[0])
            })
    report["tiles"] = geoms

    xs0 = set()
    pixs = set()
    tops = []
    bottoms = []
    for g in geoms:
        xs0.add(round(g["bounds"][0], 6))
        pixs.add(round(g["res"][0], 12))
        tops.append(g["bounds"][3])
        bottoms.append(g["bounds"][1])
    report["global_grid"] = {
        "minx_uniform": len(xs0) == 1 and list(xs0)[0],
        "pixsize_uniform": len(pixs) == 1 and list(pixs)[0],
        "n_tiles": len(geoms),
        "min_top": min(tops),
        "max_top": max(tops),
        "min_bottom": min(bottoms),
        "max_bottom": max(bottoms),
        "total_height_px_est": round((max(tops) - min(bottoms)) / list(pixs)[0])
    }
except Exception as exc:  # noqa: BLE001
    report["tiles"] = ["rasterio failed: %s" % repr(exc)]

# --- admin1 names via any available reader ---
names = None
try:
    import geopandas as gpd
    gdf = gpd.read_file("inputs/mmr_admin1.shp")
    names = gdf["shapeName"].tolist()
    report["admin1_names"] = names
    report["admin1_crs"] = str(gdf.crs)
except Exception:  # noqa: BLE001
    try:
        import fiona
        with fiona.open("inputs/mmr_admin1.shp") as src:
            names = [feat["properties"]["shapeName"] for feat in src]
        report["admin1_names"] = names
        report["admin1_crs"] = "fiona-read"
    except Exception as exc:  # noqa: BLE001
        report["admin1_names"] = ["vector read failed: %s" % repr(exc)]

with open("outputs/myanmar_env_probe.json", "w", encoding="utf-8") as fh:
    json.dump(report, fh, ensure_ascii=False, indent=1, default=str)

print(json.dumps(report, ensure_ascii=False, indent=1, default=str)[:4000])
