"""Bounded acquisition adapted from NTL-GPT GaoDe_tool/global_admin_boundary_fetch.

No arbitrary URLs, scripts or silent substitution of parent for child divisions.
"""
import hashlib
import json
import math
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import geopandas as gpd
import requests
from shapely.geometry import shape, mapping, MultiPolygon
from shapely.validation import make_valid
from shapely.ops import transform

HOSTS = {"restapi.amap.com", "geo.datav.aliyun.com", "www.geoboundaries.org",
         "github.com", "raw.githubusercontent.com", "media.githubusercontent.com"}

def fetch_json(url, params=None):
    for _ in range(5):
        parsed = urlparse(url)
        if parsed.scheme != "https" or parsed.hostname not in HOSTS or parsed.username:
            raise ValueError("Boundary source URL is not allowed")
        try:
            response = requests.get(url, params=params, timeout=(15, 90), stream=True, allow_redirects=False)
        except requests.RequestException:
            raise ValueError("Boundary source connection failed; check network/proxy") from None
        with response:
            if response.is_redirect:
                from urllib.parse import urljoin
                url = urljoin(url, response.headers["Location"])
                params = None
                continue
            if response.status_code != 200:
                raise ValueError(f"Boundary source returned HTTP {response.status_code}")
            chunks, size = [], 0
            for chunk in response.iter_content(65536):
                size += len(chunk)
                if size > 64 * 1024 * 1024:
                    raise ValueError("Boundary source exceeds 64 MiB; select a smaller source")
                chunks.append(chunk)
            return json.loads(b"".join(chunks))
    raise ValueError("Too many boundary redirects")

def gcj02_to_wgs84(x, y, z=None):
    # Same approximate inverse used by the NTL-GPT acquisition tool.
    if not (72.004 <= x <= 137.8347 and 0.8293 <= y <= 55.8271):
        return x, y
    lng, lat = x - 105, y - 35
    a, eccentricity = 6378245.0, 0.00669342162296594323
    dlat = -100 + 2*lng + 3*lat + .2*lat*lat + .1*lng*lat + .2*math.sqrt(abs(lng))
    dlat += (20*math.sin(6*lng*math.pi) + 20*math.sin(2*lng*math.pi))*2/3
    dlat += (20*math.sin(lat*math.pi) + 40*math.sin(lat/3*math.pi))*2/3
    dlat += (160*math.sin(lat/12*math.pi) + 320*math.sin(lat*math.pi/30))*2/3
    dlng = 300 + lng + 2*lat + .1*lng*lng + .1*lng*lat + .1*math.sqrt(abs(lng))
    dlng += (20*math.sin(6*lng*math.pi) + 20*math.sin(2*lng*math.pi))*2/3
    dlng += (20*math.sin(lng*math.pi) + 40*math.sin(lng/3*math.pi))*2/3
    dlng += (150*math.sin(lng/12*math.pi) + 300*math.sin(lng/30*math.pi))*2/3
    rad = y/180*math.pi
    magic = 1-eccentricity*math.sin(rad)**2
    dlat = dlat*180/((a*(1-eccentricity))/(magic*math.sqrt(magic))*math.pi)
    dlng = dlng*180/(a/math.sqrt(magic)*math.cos(rad)*math.pi)
    return x-dlng, y-dlat

def datav(parameters):
    code = parameters.get("adcode", "")
    name = parameters.get("city", "")
    if not code:
        key = os.environ.get("AMAP_API_KEY")
        if not key:
            raise ValueError("AMAP_API_KEY is missing; configure it or provide a verified six-digit adcode")
        data = fetch_json("https://restapi.amap.com/v3/config/district", {"key": key, "keywords": name, "subdistrict": 0})
        candidates = data.get("districts", []) if data.get("status") == "1" else []
        if len(candidates) != 1:
            raise ValueError("Administrative name is missing or ambiguous; use a verified adcode")
        code, name = str(candidates[0]["adcode"]), candidates[0]["name"]
    if not re.fullmatch(r"\d{6}", str(code)):
        raise ValueError("A six-digit adcode is required")
    scope = parameters.get("scope", "children")
    if scope not in {"children", "self"}:
        raise ValueError("Invalid boundary scope")
    url = f"https://geo.datav.aliyun.com/areas_v3/bound/geojson?code={code}{'_full' if scope == 'children' else ''}"
    data = fetch_json(url)
    if scope == "children" and (not data.get("features") or any(str(f.get("properties", {}).get("adcode")) == code for f in data["features"])):
        raise ValueError("Requested child divisions are unavailable; parent boundary was not substituted")
    for feature in data.get("features", []):
        feature["geometry"] = mapping(transform(gcj02_to_wgs84, shape(feature["geometry"])))
    return data, {"provider": "Amap adcode / Aliyun DataV", "url": url, "adcode": code, "resolved_name": name,
                  "scope": scope, "name_field": "name", "coordinate_conversion": "GCJ-02 to WGS-84 approximate inverse (NTL-GPT)",
                  "limitations": ["Current public reference boundary, not a guaranteed boundary vintage for the raster year.", "Coordinate inversion is approximate, not a survey-grade transformation."]}

def geoboundaries(parameters):
    country, level = parameters.get("country", ""), parameters.get("adm_level", 1)
    if not re.fullmatch(r"[A-Z]{3}", country) or type(level) is not int or not 0 <= level <= 4:
        raise ValueError("geoBoundaries requires uppercase ISO3 and ADM0-ADM4")
    url = f"https://www.geoboundaries.org/api/current/gbOpen/{country}/ADM{level}/"
    metadata = fetch_json(url)
    data = fetch_json(metadata["gjDownloadURL"])
    if parameters.get("place_name"):
        target = parameters["place_name"].strip().casefold()
        data["features"] = [f for f in data.get("features", []) if str(f.get("properties", {}).get("shapeName", "")).strip().casefold() == target]
    return data, {"provider": "geoBoundaries gbOpen", "url": url, "download_url": metadata["gjDownloadURL"],
                  "country": country, "adm_level": level, "name_field": "shapeName",
                  "license": metadata.get("boundaryLicense"), "boundary_year": metadata.get("boundaryYearRepresented"),
                  "limitations": ["Administrative levels differ between countries; verify feature names and boundary year."]}

def gee(parameters):
    import ee
    dataset = parameters.get("dataset_id")
    if not isinstance(dataset, str) or not dataset:
        raise ValueError("A verified FeatureCollection asset ID is required")
    ee.Initialize(project=os.environ["GEE_DEFAULT_PROJECT_ID"])
    collection = ee.FeatureCollection(dataset)
    if parameters.get("bbox"):
        bbox = parameters["bbox"]
        if len(bbox) != 4 or not all(isinstance(v, (int, float)) and math.isfinite(v) for v in bbox) or not (-180 <= bbox[0] < bbox[2] <= 180 and -90 <= bbox[1] < bbox[3] <= 90):
            raise ValueError("Invalid geographic bbox")
        collection = collection.filterBounds(ee.Geometry.Rectangle(bbox))
    if parameters.get("filter_property"):
        collection = collection.filter(ee.Filter.eq(parameters["filter_property"], parameters["filter_value"]))
    data = collection.limit(5001).getInfo()
    return data, {"provider": "Google Earth Engine", "dataset_id": dataset,
                  "limitations": ["Source asset controls boundary vintage and administrative level; inspect fields before statistics."]}

def download_boundary(parameters):
    provider = parameters.get("provider")
    handlers = {"datav": datav, "geoboundaries": geoboundaries, "gee": gee}
    if provider not in handlers:
        raise ValueError("Unsupported boundary provider")
    data, metadata = handlers[provider](parameters)
    features = data.get("features", [])
    if not 1 <= len(features) <= 5000:
        raise ValueError("Expected 1-5000 administrative features")
    expected = parameters.get("expected_count")
    if expected is not None and (type(expected) is not int or len(features) != expected):
        raise ValueError(f"Boundary count mismatch: expected {expected}, received {len(features)}")
    frame = gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")
    repaired = []
    def polygonal(geometry):
        if geometry.geom_type == "Polygon":
            return [geometry]
        return [polygon for part in getattr(geometry, "geoms", []) for polygon in polygonal(part)]
    for index, geometry in frame.geometry.items():
        if geometry is not None and not geometry.is_valid:
            polygons = polygonal(make_valid(geometry))
            if not polygons:
                raise ValueError("Geometry repair would drop an administrative feature")
            frame.at[index, "geometry"] = polygons[0] if len(polygons) == 1 else MultiPolygon(polygons)
            repaired.append(int(index))
    metadata["geometry_repairs"] = {"count": len(repaired), "feature_indices": repaired, "method": "make_valid; retain polygonal parts; never drop features"}
    if frame.geometry.is_empty.any() or frame.geometry.isna().any() or not frame.geometry.is_valid.all() or not frame.geom_type.isin(["Polygon", "MultiPolygon"]).all():
        raise ValueError("Invalid or non-polygon administrative geometry; no silent geometry repair")
    bounds = frame.total_bounds.tolist()
    if not all(math.isfinite(v) for v in bounds) or bounds[0] < -180 or bounds[2] > 180 or bounds[1] < -90 or bounds[3] > 90:
        raise ValueError("Invalid WGS-84 extent")
    output = Path("outputs/boundary.geojson")
    output.write_text(frame.to_json(drop_id=True), encoding="utf-8")
    field = metadata.get("name_field")
    metadata.update({"status": "success", "feature_count": len(frame), "crs": "EPSG:4326", "bounds": bounds,
                     "fields": [c for c in frame.columns if c != "geometry"], "geometry_valid": True,
                     "feature_names": frame[field].astype(str).tolist() if field in frame.columns else [],
                     "primary_file": "boundary.geojson", "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
                     "retrieved_at": datetime.now(timezone.utc).isoformat()})
    Path("outputs/boundary-source.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    return metadata
