"""Fixed calls to the unchanged NTL-GPT toolkit; never dynamic model code."""
import importlib
from pathlib import Path

OPERATIONS = {
    "inspect_vector": "vector", "validate_geodata": "raster", "clip_raster": "raster",
    "reproject_raster": "raster", "mosaic_rasters": "raster", "calculate_zonal_statistics": "ntl",
    "calculate_ntl_metrics_for_raster": "ntl", "composite_ntl_rasters": "ntl", "analyze_ntl_trend": "ntl",
    "detect_ntl_anomaly": "ntl", "filter_points_by_polygon": "vector", "buffer_points_aeqd": "vector",
    "spatial_join_points_to_admin": "vector", "dissolve_intersections": "vector",
}

def scoped_path(value, output=False):
    if not isinstance(value, str) or "\\" in value or ":" in value:
        raise ValueError("Use inputs/, previous/ or outputs/ relative paths")
    parts = Path(value).parts
    if not parts or ".." in parts or parts[0] not in ({"outputs"} if output else {"inputs", "previous", "outputs"}):
        raise ValueError("Workspace-relative path required")
    # Agent-visible outputs refer to completed earlier jobs, not the new output mount.
    if not output and parts[0] == "outputs":
        parts = ("previous", *parts[1:])
    result = Path("/workspace", *parts).resolve()
    result.relative_to(Path("/workspace", parts[0]).resolve())
    return str(result.relative_to(Path("/workspace")))

def run_gis(request):
    operation = request["operation"]
    if operation not in OPERATIONS:
        raise ValueError("Unsupported GIS operation")
    parameters = dict(request["parameters"])
    for key, value in parameters.items():
        if key in {"output_path", "output_prefix"}:
            parameters[key] = scoped_path(value, output=True)
        elif key == "path" or key.endswith("_path"):
            parameters[key] = scoped_path(value)
        elif key.endswith("_paths"):
            if not isinstance(value, list):
                raise ValueError("Paths must be a list")
            parameters[key] = [scoped_path(item) for item in value]
    module = importlib.import_module("ntl_toolkit.core." + OPERATIONS[operation])
    result = getattr(module, operation)(**parameters).model_dump(mode="json")
    if result.get("error"):
        raise ValueError(str(result["error"]))
    return result
