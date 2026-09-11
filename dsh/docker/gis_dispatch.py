"""Fixed calls to the unchanged NTL-GPT toolkit; never dynamic model code."""
import importlib
import re
from pathlib import Path

# Every job the platform creates is named `<YYYYMMDD-HHMMSS>-<tool>-<hash>`, and its
# artifacts land in the chat's `outputs/` tree. Agents routinely pass one of those
# directories without the `outputs/` prefix (`20260911-180003-gee-235ce3/imagery.tif`),
# which used to fail with the bare "Workspace-relative path required" — measured on
# four jobs across two benchmark cases (2026-09-12). The shape is unambiguous, so it is
# read as the earlier-job form (`previous/…`), exactly like an `outputs/…` argument.
JOB_DIR = re.compile(r"^\d{8}-\d{6}-")

OPERATIONS = {
    "inspect_vector": "vector", "validate_geodata": "raster", "clip_raster": "raster",
    "reproject_raster": "raster", "mosaic_rasters": "raster", "calculate_zonal_statistics": "ntl",
    "calculate_ntl_metrics_for_raster": "ntl", "composite_ntl_rasters": "ntl", "analyze_ntl_trend": "ntl",
    "detect_ntl_anomaly": "ntl", "filter_points_by_polygon": "vector", "buffer_points_aeqd": "vector",
    "spatial_join_points_to_admin": "vector", "dissolve_intersections": "vector",
}

def scoped_path(value, output=False, root="/workspace"):
    """Resolve one agent-supplied path against the container's workspace roots.

    `root` is injectable so the accepted forms can be unit-tested outside Linux
    (`Path("/workspace")` is drive-relative on Windows and cannot be compared).
    """
    forms = "inputs/<文件>、previous/<作业ID>/<文件>、outputs/<文件>（更早作业写成 outputs/<作业ID>/<文件>）、share/<根名>/<文件>"
    if not isinstance(value, str) or "\\" in value or ":" in value:
        raise ValueError(
            f"路径必须是容器内的相对路径（收到 {value!r}）：可用形式 {forms}；"
            "Windows 反斜杠、盘符和绝对路径都不接受。"
        )
    parts = Path(value).parts
    # `share/` is the administrator's shared data library: readable input only,
    # mounted read-only, never accepted as an output target.
    readable = {"inputs", "previous", "outputs", "share"}
    if not parts or ".." in parts:
        raise ValueError(
            f"路径不能为空、不能含 '..'（收到 {value!r}）：可用形式 {forms}。"
        )
    if parts[0] not in ({"outputs"} if output else readable):
        if not output and JOB_DIR.match(parts[0]):
            # A bare job directory means the earlier job's artifacts.
            parts = ("previous", *parts)
        elif output and "/" not in value and "\\" not in value:
            # A bare file name as an output can only mean this job's outputs
            # directory, so accept it instead of failing a call the agent cannot
            # disambiguate (measured 2026-09-12: a clip job lost its whole turn to
            # "Workspace-relative path required" over `shanghai_ntl_2020_clip.tif`).
            parts = ("outputs", *parts)
        else:
            accepted = "outputs/" if output else "inputs/, outputs/<作业ID>/, previous/ 或 share/"
            raise ValueError(
                f"路径必须以 {accepted} 开头（收到 {value!r}）：更早作业的产物写成 "
                "outputs/<作业ID>/<文件>，本次作业的新产物写成 outputs/<文件>。"
            )
    if output and parts[0] != "outputs":
        raise ValueError("Outputs must be written under outputs/")
    # Agent-visible outputs refer to completed earlier jobs, not the new output mount.
    if not output and parts[0] == "outputs":
        parts = ("previous", *parts[1:])
    result = Path(root, *parts).resolve()
    result.relative_to(Path(root, parts[0]).resolve())
    return str(result.relative_to(Path(root)))

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
