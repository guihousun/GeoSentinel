from __future__ import annotations

from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd
from shapely.geometry.base import BaseGeometry

from ntl_toolkit.runtime import (
    require_input_path,
    reserve_output_path,
    resolve_local_path,
    runtime_workdir,
)
from ntl_toolkit.schemas import OutputArtifact, ToolError, ToolResult

_VECTOR_TOOL_NAME = {
    "inspect": "inspect_vector",
    "filter": "filter_points_by_polygon",
    "join": "spatial_join_points_to_admin",
    "buffer": "buffer_points_aeqd",
    "dissolve": "dissolve_intersections",
}
_POINT_VECTOR_SUFFIXES = {".geojson", ".json", ".gpkg", ".shp"}
_SUPPORTED_PREDICATES = {"within", "intersects"}


class _KnownVectorFailure(Exception):
    def __init__(self, error: ToolError) -> None:
        super().__init__(error.message)
        self.error = error


def _fail(code: str, message: str, *, details: dict[str, Any] | None = None) -> None:
    raise _KnownVectorFailure(
        ToolError(
            code=code,
            message=message,
            details=details or {},
        )
    )


def _media_type_for(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".geojson", ".json"}:
        return "application/geo+json"
    if suffix == ".gpkg":
        return "application/geopackage+sqlite3"
    if suffix == ".shp":
        return "application/x-esri-shapefile"
    return "application/octet-stream"


def _artifact_for(path: Path) -> OutputArtifact:
    return OutputArtifact(path=str(path), media_type=_media_type_for(path))


def _tool_failure(tool: str, error: ToolError) -> ToolResult:
    return ToolResult.failed(tool=tool, error=error)


def _resolve_input_path(path: str | Path) -> Path:
    return require_input_path(path, runtime_workdir())


def _resolve_output_path(path: str | Path) -> Path:
    requested = resolve_local_path(path, runtime_workdir())
    return reserve_output_path(requested)


def _require_columns(frame: pd.DataFrame, columns: list[str]) -> None:
    for column in columns:
        if column not in frame.columns:
            _fail(
                "COLUMN_NOT_FOUND",
                f"Required column '{column}' was not found.",
                details={"column": column},
            )


def _parse_radius_km(raw_value: Any) -> float:
    try:
        radius_km = float(raw_value)
    except (TypeError, ValueError):
        _fail(
            "INVALID_PARAMETER",
            "radius_km must be a numeric value in kilometers.",
            details={"parameter": "radius_km", "value": raw_value},
        )

    if radius_km <= 0:
        _fail(
            "INVALID_PARAMETER",
            "radius_km must be greater than zero.",
            details={"parameter": "radius_km", "value": radius_km},
        )
    return radius_km


def _validate_crs(gdf: gpd.GeoDataFrame, path: Path) -> None:
    if gdf.crs is None:
        _fail(
            "CRS_MISSING",
            f"Vector dataset '{path}' does not define a CRS.",
            details={"path": str(path)},
        )


def _geometry_problem(geometry: BaseGeometry | None) -> str | None:
    if geometry is None:
        return "missing"
    if geometry.is_empty:
        return "empty"
    if not geometry.is_valid:
        return "invalid"
    return None


def _validate_geometry(gdf: gpd.GeoDataFrame, path: Path) -> None:
    if gdf.empty:
        _fail(
            "INVALID_GEOMETRY",
            f"Vector dataset '{path}' contains no features.",
            details={"path": str(path), "reason": "empty_dataset"},
        )

    for index, geometry in enumerate(gdf.geometry):
        problem = _geometry_problem(geometry)
        if problem is not None:
            _fail(
                "INVALID_GEOMETRY",
                f"Vector dataset '{path}' contains {problem} geometry.",
                details={"path": str(path), "feature_index": index, "reason": problem},
            )


def _read_points(path: str | Path, lon_col: str, lat_col: str) -> gpd.GeoDataFrame:
    input_path = _resolve_input_path(path)
    if input_path.suffix.lower() in _POINT_VECTOR_SUFFIXES:
        points = _read_vector(input_path)
        geometry_types = set(points.geometry.geom_type.unique())
        if geometry_types != {"Point"}:
            _fail(
                "INVALID_GEOMETRY",
                f"Point dataset '{input_path}' must contain only Point geometries.",
                details={"path": str(input_path), "geometry_types": sorted(geometry_types)},
            )
        return points

    frame = pd.read_csv(input_path, encoding="utf-8-sig")
    _require_columns(frame, [lon_col, lat_col])
    frame = frame.copy()
    frame[lon_col] = pd.to_numeric(frame[lon_col], errors="coerce")
    frame[lat_col] = pd.to_numeric(frame[lat_col], errors="coerce")
    frame = frame[
        frame[lon_col].between(-180.0, 180.0) & frame[lat_col].between(-90.0, 90.0)
    ].copy()
    if frame.empty:
        _fail(
            "INVALID_GEOMETRY",
            f"Point table '{input_path}' has no valid longitude/latitude rows.",
            details={"path": str(input_path), "reason": "empty_dataset"},
        )
    return gpd.GeoDataFrame(
        frame,
        geometry=gpd.points_from_xy(frame[lon_col], frame[lat_col]),
        crs="EPSG:4326",
    )


def _read_vector(path: str | Path) -> gpd.GeoDataFrame:
    input_path = _resolve_input_path(path)
    gdf = gpd.read_file(input_path)
    _validate_crs(gdf, input_path)
    _validate_geometry(gdf, input_path)
    return gdf


def _write_vector(gdf: gpd.GeoDataFrame, output_path: str | Path) -> Path:
    reserved = _resolve_output_path(output_path)
    suffix = reserved.suffix.lower()
    driver = "GeoJSON"
    if suffix == ".gpkg":
        driver = "GPKG"
    elif suffix == ".shp":
        driver = "ESRI Shapefile"
    gdf.to_file(reserved, driver=driver, index=False)
    return reserved


def inspect_vector(path: str | Path) -> ToolResult:
    """Return metadata for a vector dataset."""
    tool = _VECTOR_TOOL_NAME["inspect"]
    try:
        input_path = _resolve_input_path(path)
        gdf = _read_vector(input_path)
    except FileNotFoundError as exc:
        return _tool_failure(
            tool,
            ToolError(
                code="INPUT_NOT_FOUND",
                message=f"Input vector was not found: {exc}.",
                details={"path": str(exc)},
            ),
        )
    except _KnownVectorFailure as exc:
        return _tool_failure(tool, exc.error)

    metrics = {
        "path": str(input_path),
        "feature_count": int(len(gdf)),
        "crs": str(gdf.crs),
        "geometry_types": sorted(str(value) for value in gdf.geometry.geom_type.dropna().unique()),
        "columns": [str(column) for column in gdf.columns],
        "bounds": [float(value) for value in gdf.total_bounds],
    }
    return ToolResult.succeeded(
        tool=tool,
        summary=f"Inspected {len(gdf)} feature(s).",
        metrics=metrics,
    )


def filter_points_by_polygon(
    points_path: str | Path,
    polygon_path: str | Path,
    output_path: str | Path,
    *,
    lon_col: str = "longitude",
    lat_col: str = "latitude",
    predicate: str = "within",
) -> ToolResult:
    """Filter points by a polygon spatial predicate."""
    tool = _VECTOR_TOOL_NAME["filter"]
    try:
        if predicate not in _SUPPORTED_PREDICATES:
            _fail(
                "UNSUPPORTED_PREDICATE",
                f"Predicate '{predicate}' is not supported.",
                details={"predicate": predicate},
            )
        points = _read_points(points_path, lon_col, lat_col)
        points_crs = points.crs
        polygons = _read_vector(polygon_path)
        projected_points = points if points.crs == polygons.crs else points.to_crs(polygons.crs)
        joined = gpd.sjoin(
            projected_points,
            polygons[["geometry"]],
            how="inner",
            predicate=predicate,
        )
        joined = joined.loc[~joined.index.duplicated(keep="first"), projected_points.columns].copy()
        result = joined if points_crs == joined.crs else joined.to_crs(points_crs)
        written = _write_vector(result, output_path)
    except FileNotFoundError as exc:
        return _tool_failure(
            tool,
            ToolError(
                code="INPUT_NOT_FOUND",
                message=f"Input dataset was not found: {exc}.",
                details={"path": str(exc)},
            ),
        )
    except _KnownVectorFailure as exc:
        return _tool_failure(tool, exc.error)

    return ToolResult.succeeded(
        tool=tool,
        summary=f"Kept {len(result)} point(s).",
        outputs=[_artifact_for(written)],
        metrics={"feature_count": int(len(result))},
    )


def spatial_join_points_to_admin(
    points_path: str | Path,
    admin_path: str | Path,
    output_path: str | Path,
    *,
    lon_col: str = "longitude",
    lat_col: str = "latitude",
    admin_name_col: str = "shapeName",
    admin_iso_col: str = "iso3",
    prefix: str = "admin",
) -> ToolResult:
    """Attach admin attributes to points."""
    tool = _VECTOR_TOOL_NAME["join"]
    try:
        points = _read_points(points_path, lon_col, lat_col)
        points_crs = points.crs
        admin = _read_vector(admin_path)
        _require_columns(admin, [admin_name_col, admin_iso_col])
        keep = ["geometry", admin_name_col, admin_iso_col]
        projected_points = points if points.crs == admin.crs else points.to_crs(admin.crs)
        joined = gpd.sjoin(
            projected_points,
            admin[keep],
            how="left",
            predicate="within",
        )
        joined = joined.loc[~joined.index.duplicated(keep="first")].copy()
        joined = joined.rename(
            columns={
                admin_name_col: f"{prefix}_name",
                admin_iso_col: f"{prefix}_iso3",
            }
        )
        if "index_right" in joined.columns:
            joined = joined.drop(columns=["index_right"])
        result = joined if points_crs == joined.crs else joined.to_crs(points_crs)
        matched_count = int(result[f"{prefix}_name"].notna().sum())
        written = _write_vector(result, output_path)
    except FileNotFoundError as exc:
        return _tool_failure(
            tool,
            ToolError(
                code="INPUT_NOT_FOUND",
                message=f"Input dataset was not found: {exc}.",
                details={"path": str(exc)},
            ),
        )
    except _KnownVectorFailure as exc:
        return _tool_failure(tool, exc.error)

    return ToolResult.succeeded(
        tool=tool,
        summary=f"Joined admin attributes onto {len(result)} point(s).",
        outputs=[_artifact_for(written)],
        metrics={
            "feature_count": int(len(result)),
            "matched_count": matched_count,
            "unmatched_count": int(len(result) - matched_count),
        },
    )


def buffer_points_aeqd(
    points_path: str | Path,
    output_path: str | Path,
    *,
    radius_km: float,
    lon_col: str = "longitude",
    lat_col: str = "latitude",
) -> ToolResult:
    """Create AEQD point buffers."""
    tool = _VECTOR_TOOL_NAME["buffer"]
    try:
        radius_km = _parse_radius_km(radius_km)
        points = _read_points(points_path, lon_col, lat_col).to_crs("EPSG:4326")
        center_lon = float(points.geometry.x.mean())
        center_lat = float(points.geometry.y.mean())
        aeqd_crs = (
            f"+proj=aeqd +lat_0={center_lat} +lon_0={center_lon} "
            "+datum=WGS84 +units=m +no_defs"
        )
        buffered = points.to_crs(aeqd_crs).copy()
        buffered["geometry"] = buffered.geometry.buffer(radius_km * 1000.0)
        buffered = buffered.to_crs("EPSG:4326")
        _validate_geometry(buffered, Path(points_path))
        written = _write_vector(buffered, output_path)
    except FileNotFoundError as exc:
        return _tool_failure(
            tool,
            ToolError(
                code="INPUT_NOT_FOUND",
                message=f"Input dataset was not found: {exc}.",
                details={"path": str(exc)},
            ),
        )
    except _KnownVectorFailure as exc:
        error = exc.error
        if error.code == "INVALID_PARAMETER" and error.suggestion is None:
            error = error.model_copy(
                update={
                    "suggestion": "Provide a numeric radius in kilometers greater than zero.",
                }
            )
        return _tool_failure(tool, error)

    return ToolResult.succeeded(
        tool=tool,
        summary=f"Buffered {len(buffered)} point(s).",
        outputs=[_artifact_for(written)],
        metrics={
            "feature_count": int(len(buffered)),
            "radius_km": radius_km,
            "center_lon": center_lon,
            "center_lat": center_lat,
        },
    )


def dissolve_intersections(
    polygons_path: str | Path,
    output_path: str | Path,
    *,
    id_col: str = "cluster_id",
) -> ToolResult:
    """Dissolve intersecting polygons into clusters."""
    tool = _VECTOR_TOOL_NAME["dissolve"]
    try:
        polygons = _read_vector(polygons_path)
        sindex = polygons.sindex
        visited: set[int] = set()
        component_ids = [-1] * len(polygons)
        component = 0

        for start in range(len(polygons)):
            if start in visited:
                continue
            stack = [start]
            visited.add(start)
            while stack:
                current = stack.pop()
                component_ids[current] = component
                geometry = polygons.geometry.iloc[current]
                for candidate in sindex.query(geometry, predicate="intersects"):
                    candidate = int(candidate)
                    if candidate not in visited:
                        visited.add(candidate)
                        stack.append(candidate)
            component += 1

        members = polygons.copy()
        members[id_col] = component_ids
        dissolved = members.dissolve(by=id_col, as_index=False)
        member_counts = members.groupby(id_col).size().rename("member_count")
        dissolved = dissolved.merge(member_counts, on=id_col, how="left")
        dissolved = dissolved.sort_values(by=id_col).reset_index(drop=True)
        written = _write_vector(dissolved, output_path)
    except FileNotFoundError as exc:
        return _tool_failure(
            tool,
            ToolError(
                code="INPUT_NOT_FOUND",
                message=f"Input dataset was not found: {exc}.",
                details={"path": str(exc)},
            ),
        )
    except _KnownVectorFailure as exc:
        return _tool_failure(tool, exc.error)

    return ToolResult.succeeded(
        tool=tool,
        summary=f"Dissolved {len(polygons)} polygon(s) into {len(dissolved)} cluster(s).",
        outputs=[_artifact_for(written)],
        metrics={
            "feature_count": int(len(dissolved)),
            "cluster_count": int(len(dissolved)),
            "member_count": int(len(polygons)),
        },
    )
