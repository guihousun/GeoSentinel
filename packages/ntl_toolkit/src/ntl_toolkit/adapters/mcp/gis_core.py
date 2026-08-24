from __future__ import annotations

import inspect
import importlib
import json
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.types import TextContent, Tool, ToolAnnotations

from ntl_toolkit.core import ntl as ntl_core
from ntl_toolkit.core import raster as raster_core
from ntl_toolkit.core import vector as vector_core
from ntl_toolkit.runtime import load_runtime_environment, resolve_local_path, runtime_workdir
from ntl_toolkit.schemas import ToolError, ToolResult

_CAPABILITIES_PATH = Path(__file__).with_name("gis_capabilities.json")
_SERVER_INSTRUCTIONS = (
    "Atomic local GIS and nighttime-light tools for validation, vector processing, "
    "raster processing, and NTL analytics. Tools write new outputs only and do not "
    "overwrite existing files."
)
_DEPENDENCIES = (
    "geopandas",
    "rasterio",
    "shapely",
    "pyproj",
    "numpy",
    "pandas",
    "scipy",
)
_READ_ONLY = ToolAnnotations(
    readOnlyHint=True,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=False,
)
_WRITE_NEW = ToolAnnotations(
    readOnlyHint=False,
    destructiveHint=False,
    idempotentHint=False,
    openWorldHint=False,
)


class StrictFastMCP(FastMCP):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._allowed_fields_by_tool: dict[str, list[str]] = {}

    def add_tool(
        self,
        fn: Any,
        name: str | None = None,
        title: str | None = None,
        description: str | None = None,
        annotations: ToolAnnotations | None = None,
        icons: list[Any] | None = None,
        meta: dict[str, Any] | None = None,
        structured_output: bool | None = None,
    ) -> None:
        tool_name = name or fn.__name__
        parameters = inspect.signature(fn).parameters
        self._allowed_fields_by_tool[tool_name] = [parameter.name for parameter in parameters.values()]
        super().add_tool(
            fn,
            name=name,
            title=title,
            description=description,
            annotations=annotations,
            icons=icons,
            meta=meta,
            structured_output=structured_output,
        )

    async def list_tools(self) -> list[Tool]:
        tools = await super().list_tools()
        sealed_tools: list[Tool] = []
        for tool in tools:
            input_schema = dict(tool.inputSchema)
            input_schema["additionalProperties"] = False
            sealed_tools.append(
                Tool.model_validate(
                    {
                        **tool.model_dump(mode="python"),
                        "inputSchema": input_schema,
                    }
                )
            )
        return sealed_tools

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> Any:
        allowed_fields = self._allowed_fields_by_tool.get(name)
        if allowed_fields is not None:
            unexpected_fields = sorted(
                field for field in arguments if field not in allowed_fields
            )
            if unexpected_fields:
                payload = _invalid_fields_payload(
                    tool=name,
                    unexpected_fields=unexpected_fields,
                    allowed_fields=allowed_fields,
                )
                return (
                    [TextContent(type="text", text=json.dumps(payload, ensure_ascii=False, indent=2))],
                    payload,
                )
        return await super().call_tool(name, arguments)


def _payload(result: ToolResult) -> dict[str, Any]:
    return result.model_dump(mode="json")


def _invalid_parameter(
    tool: str,
    parameter: str,
    value: Any,
    reason: str,
) -> dict[str, Any]:
    return _payload(
        ToolResult.failed(
            tool=tool,
            error=ToolError(
                code="INVALID_PARAMETER",
                message=f"Invalid path for '{parameter}'.",
                details={
                    "parameter": parameter,
                    "value": value,
                    "reason": reason,
                },
                suggestion=(
                    "Use an ordinary relative path or a fully qualified absolute Windows path."
                ),
            ),
        )
    )


def _invalid_fields_payload(
    *,
    tool: str,
    unexpected_fields: list[str],
    allowed_fields: list[str],
) -> dict[str, Any]:
    return _payload(
        ToolResult.failed(
            tool=tool,
            error=ToolError(
                code="INVALID_PARAMETER",
                message="Unexpected tool arguments were provided.",
                details={
                    "unexpected_fields": unexpected_fields,
                    "allowed_fields": allowed_fields,
                },
                suggestion="Remove unsupported fields and retry with only the documented tool parameters.",
            ),
        )
    )


def _resolve_path(
    *,
    tool: str,
    parameter: str,
    value: str,
    workdir: Path,
) -> str:
    try:
        return str(resolve_local_path(value, workdir))
    except ValueError as exc:
        raise _ResolvedPathError(
            _invalid_parameter(tool, parameter, value, str(exc))
        ) from exc


def _resolve_optional_path_list(
    *,
    tool: str,
    parameter: str,
    values: list[str] | None,
    workdir: Path,
) -> list[str] | None:
    if values is None:
        return None
    resolved: list[str] = []
    for value in values:
        resolved.append(
            _resolve_path(tool=tool, parameter=parameter, value=value, workdir=workdir)
        )
    return resolved


def validate_environment() -> dict[str, Any]:
    """Report whether the required local GIS dependencies are importable."""
    versions: dict[str, str] = {}
    missing: list[str] = []
    errors: dict[str, dict[str, str]] = {}

    for module_name in _DEPENDENCIES:
        try:
            module = importlib.import_module(module_name)
        except (ImportError, OSError) as exc:
            missing.append(module_name)
            errors[module_name] = {
                "type": type(exc).__name__,
                "message": str(exc),
            }
            continue
        versions[module_name] = str(getattr(module, "__version__", "unknown"))

    if missing:
        return _payload(
            ToolResult.failed(
                tool="validate_environment",
                error=ToolError(
                    code="DEPENDENCY_MISSING",
                    message="One or more GIS dependencies are missing.",
                    details={
                        "missing": missing,
                        "available_versions": versions,
                        "errors": errors,
                    },
                    suggestion=(
                        "Install the missing packages in the active runtime environment "
                        "and retry validate_environment."
                    ),
                ),
            )
        )

    return _payload(
        ToolResult.succeeded(
            tool="validate_environment",
            summary="Validated the local GIS runtime environment.",
            metrics={"versions": versions},
        )
    )


def build_gis_core_mcp() -> FastMCP:
    """Build the local GIS MCP server with fixed tools and resources."""
    load_runtime_environment()
    captured_workdir = runtime_workdir()
    mcp = StrictFastMCP("ntl-gis-core", instructions=_SERVER_INSTRUCTIONS)

    @mcp.resource(
        "ntl://gis/capabilities",
        name="gis-capabilities",
        description="Static capability definitions for the ntl-gis-core tool catalog.",
        mime_type="application/json",
    )
    def gis_capabilities_resource() -> str:
        return _CAPABILITIES_PATH.read_text(encoding="utf-8")

    @mcp.resource(
        "ntl://schemas/result-v1",
        name="tool-result-schema",
        description="JSON schema for the ntl.tool.result.v1 payload.",
        mime_type="application/json",
    )
    def result_schema_resource() -> str:
        return json.dumps(ToolResult.model_json_schema(), ensure_ascii=False, indent=2)

    @mcp.tool(
        name="validate_environment",
        description="Check whether the required local GIS and NTL Python dependencies are installed.",
        annotations=_READ_ONLY,
        structured_output=True,
    )
    def validate_environment_tool() -> dict[str, Any]:
        """Validate local GIS dependency availability without touching the filesystem."""
        return validate_environment()

    @mcp.tool(
        name="inspect_vector",
        description="Inspect a vector dataset and return feature, CRS, column, and bounds metadata.",
        annotations=_READ_ONLY,
        structured_output=True,
    )
    def inspect_vector(path: str) -> dict[str, Any]:
        """Inspect a vector dataset at the provided path."""
        try:
            resolved_path = _resolve_path(
                tool="inspect_vector",
                parameter="path",
                value=path,
                workdir=captured_workdir,
            )
        except _ResolvedPathError as exc:
            return exc.payload
        return _payload(vector_core.inspect_vector(resolved_path))

    @mcp.tool(
        name="inspect_raster",
        description="Inspect a raster dataset and optionally sample band statistics.",
        annotations=_READ_ONLY,
        structured_output=True,
    )
    def inspect_raster(
        path: str,
        mode: str = "full",
        sample_pixels: int = 0,
    ) -> dict[str, Any]:
        """Inspect a raster dataset at the provided path."""
        try:
            resolved_path = _resolve_path(
                tool="inspect_raster",
                parameter="path",
                value=path,
                workdir=captured_workdir,
            )
        except _ResolvedPathError as exc:
            return exc.payload
        return _payload(
            raster_core.inspect_raster(
                resolved_path,
                mode=mode,
                sample_pixels=sample_pixels,
            )
        )

    @mcp.tool(
        name="filter_points_by_polygon",
        description="Filter point features or point tables by a polygon predicate and write a new vector file.",
        annotations=_WRITE_NEW,
        structured_output=True,
    )
    def filter_points_by_polygon(
        points: str,
        polygon: str,
        output: str,
        lon_col: str = "longitude",
        lat_col: str = "latitude",
        predicate: str = "within",
    ) -> dict[str, Any]:
        """Filter point records by polygon membership and write a new dataset."""
        try:
            resolved_points = _resolve_path(
                tool="filter_points_by_polygon",
                parameter="points",
                value=points,
                workdir=captured_workdir,
            )
            resolved_polygon = _resolve_path(
                tool="filter_points_by_polygon",
                parameter="polygon",
                value=polygon,
                workdir=captured_workdir,
            )
            resolved_output = _resolve_path(
                tool="filter_points_by_polygon",
                parameter="output",
                value=output,
                workdir=captured_workdir,
            )
        except _ResolvedPathError as exc:
            return exc.payload
        return _payload(
            vector_core.filter_points_by_polygon(
                resolved_points,
                resolved_polygon,
                resolved_output,
                lon_col=lon_col,
                lat_col=lat_col,
                predicate=predicate,
            )
        )

    @mcp.tool(
        name="spatial_join_points_to_admin",
        description="Attach admin identifiers to points and write a new vector file.",
        annotations=_WRITE_NEW,
        structured_output=True,
    )
    def spatial_join_points_to_admin(
        points: str,
        admin: str,
        output: str,
        lon_col: str = "longitude",
        lat_col: str = "latitude",
        admin_name_col: str = "shapeName",
        admin_iso_col: str = "iso3",
        prefix: str = "admin",
    ) -> dict[str, Any]:
        """Spatially join point features to an administrative polygon layer."""
        try:
            resolved_points = _resolve_path(
                tool="spatial_join_points_to_admin",
                parameter="points",
                value=points,
                workdir=captured_workdir,
            )
            resolved_admin = _resolve_path(
                tool="spatial_join_points_to_admin",
                parameter="admin",
                value=admin,
                workdir=captured_workdir,
            )
            resolved_output = _resolve_path(
                tool="spatial_join_points_to_admin",
                parameter="output",
                value=output,
                workdir=captured_workdir,
            )
        except _ResolvedPathError as exc:
            return exc.payload
        return _payload(
            vector_core.spatial_join_points_to_admin(
                resolved_points,
                resolved_admin,
                resolved_output,
                lon_col=lon_col,
                lat_col=lat_col,
                admin_name_col=admin_name_col,
                admin_iso_col=admin_iso_col,
                prefix=prefix,
            )
        )

    @mcp.tool(
        name="buffer_points_aeqd",
        description="Buffer point features in an AEQD projection and write a new polygon dataset.",
        annotations=_WRITE_NEW,
        structured_output=True,
    )
    def buffer_points_aeqd(
        points: str,
        output: str,
        radius_km: float,
        lon_col: str = "longitude",
        lat_col: str = "latitude",
    ) -> dict[str, Any]:
        """Create AEQD buffers from point features."""
        try:
            resolved_points = _resolve_path(
                tool="buffer_points_aeqd",
                parameter="points",
                value=points,
                workdir=captured_workdir,
            )
            resolved_output = _resolve_path(
                tool="buffer_points_aeqd",
                parameter="output",
                value=output,
                workdir=captured_workdir,
            )
        except _ResolvedPathError as exc:
            return exc.payload
        return _payload(
            vector_core.buffer_points_aeqd(
                resolved_points,
                resolved_output,
                radius_km=radius_km,
                lon_col=lon_col,
                lat_col=lat_col,
            )
        )

    @mcp.tool(
        name="dissolve_intersections",
        description="Dissolve intersecting polygons into clusters and write a new vector dataset.",
        annotations=_WRITE_NEW,
        structured_output=True,
    )
    def dissolve_intersections(
        polygons: str,
        output: str,
        id_col: str = "cluster_id",
    ) -> dict[str, Any]:
        """Dissolve intersecting polygons into clustered geometries."""
        try:
            resolved_polygons = _resolve_path(
                tool="dissolve_intersections",
                parameter="polygons",
                value=polygons,
                workdir=captured_workdir,
            )
            resolved_output = _resolve_path(
                tool="dissolve_intersections",
                parameter="output",
                value=output,
                workdir=captured_workdir,
            )
        except _ResolvedPathError as exc:
            return exc.payload
        return _payload(
            vector_core.dissolve_intersections(
                resolved_polygons,
                resolved_output,
                id_col=id_col,
            )
        )

    @mcp.tool(
        name="clip_raster",
        description="Clip a raster by vector geometries and write a new raster file.",
        annotations=_WRITE_NEW,
        structured_output=True,
    )
    def clip_raster(
        raster: str,
        vector: str,
        output: str,
        all_touched: bool = False,
    ) -> dict[str, Any]:
        """Clip a raster dataset against vector geometries."""
        try:
            resolved_raster = _resolve_path(
                tool="clip_raster",
                parameter="raster",
                value=raster,
                workdir=captured_workdir,
            )
            resolved_vector = _resolve_path(
                tool="clip_raster",
                parameter="vector",
                value=vector,
                workdir=captured_workdir,
            )
            resolved_output = _resolve_path(
                tool="clip_raster",
                parameter="output",
                value=output,
                workdir=captured_workdir,
            )
        except _ResolvedPathError as exc:
            return exc.payload
        return _payload(
            raster_core.clip_raster(
                resolved_raster,
                resolved_vector,
                resolved_output,
                all_touched=all_touched,
            )
        )

    @mcp.tool(
        name="reproject_raster",
        description="Reproject a raster into a destination CRS and write a new raster file.",
        annotations=_WRITE_NEW,
        structured_output=True,
    )
    def reproject_raster(
        raster: str,
        output: str,
        dst_crs: str,
        resampling: str = "bilinear",
    ) -> dict[str, Any]:
        """Reproject a raster dataset into a destination CRS."""
        try:
            resolved_raster = _resolve_path(
                tool="reproject_raster",
                parameter="raster",
                value=raster,
                workdir=captured_workdir,
            )
            resolved_output = _resolve_path(
                tool="reproject_raster",
                parameter="output",
                value=output,
                workdir=captured_workdir,
            )
        except _ResolvedPathError as exc:
            return exc.payload
        return _payload(
            raster_core.reproject_raster(
                resolved_raster,
                resolved_output,
                dst_crs=dst_crs,
                resampling=resampling,
            )
        )

    @mcp.tool(
        name="mosaic_rasters",
        description="Mosaic aligned rasters and write a new raster file.",
        annotations=_WRITE_NEW,
        structured_output=True,
    )
    def mosaic_rasters(
        raster_paths: list[str],
        output: str,
        method: str = "first",
    ) -> dict[str, Any]:
        """Mosaic multiple raster datasets into a new raster."""
        try:
            resolved_raster_paths = _resolve_optional_path_list(
                tool="mosaic_rasters",
                parameter="raster_paths",
                values=raster_paths,
                workdir=captured_workdir,
            )
            resolved_output = _resolve_path(
                tool="mosaic_rasters",
                parameter="output",
                value=output,
                workdir=captured_workdir,
            )
        except _ResolvedPathError as exc:
            return exc.payload
        return _payload(
            raster_core.mosaic_rasters(
                resolved_raster_paths or [],
                resolved_output,
                method=method,
            )
        )

    @mcp.tool(
        name="calculate_zonal_statistics",
        description="Calculate zonal NTL metrics for rasters and write a new tabular output.",
        annotations=_WRITE_NEW,
        structured_output=True,
    )
    def calculate_zonal_statistics(
        raster_paths: list[str],
        vector: str,
        output: str,
        selected_indices: list[str] | None = None,
        only_global: bool = False,
    ) -> dict[str, Any]:
        """Calculate zonal nighttime-light statistics for a vector layer."""
        try:
            resolved_raster_paths = _resolve_optional_path_list(
                tool="calculate_zonal_statistics",
                parameter="raster_paths",
                values=raster_paths,
                workdir=captured_workdir,
            )
            resolved_vector = _resolve_path(
                tool="calculate_zonal_statistics",
                parameter="vector",
                value=vector,
                workdir=captured_workdir,
            )
            resolved_output = _resolve_path(
                tool="calculate_zonal_statistics",
                parameter="output",
                value=output,
                workdir=captured_workdir,
            )
        except _ResolvedPathError as exc:
            return exc.payload
        return _payload(
            ntl_core.calculate_zonal_statistics(
                raster_paths=resolved_raster_paths or [],
                vector_path=resolved_vector,
                output_path=resolved_output,
                selected_indices=selected_indices,
                only_global=only_global,
            )
        )

    @mcp.tool(
        name="calculate_ntl_metrics",
        description="Calculate NTL metrics for a raster band without writing any output files.",
        annotations=_READ_ONLY,
        structured_output=True,
    )
    def calculate_ntl_metrics(
        raster_path: str,
        band: int = 1,
        selected_indices: list[str] | None = None,
    ) -> dict[str, Any]:
        """Calculate nighttime-light metrics for a single raster band."""
        try:
            resolved_raster = _resolve_path(
                tool="calculate_ntl_metrics",
                parameter="raster_path",
                value=raster_path,
                workdir=captured_workdir,
            )
        except _ResolvedPathError as exc:
            return exc.payload
        return _payload(
            ntl_core.calculate_ntl_metrics_for_raster(
                resolved_raster,
                band=band,
                selected=selected_indices,
            )
        )

    @mcp.tool(
        name="composite_ntl_rasters",
        description="Create a new composite raster from aligned nighttime-light rasters.",
        annotations=_WRITE_NEW,
        structured_output=True,
    )
    def composite_ntl_rasters(
        raster_paths: list[str],
        output: str,
        method: str = "mean",
    ) -> dict[str, Any]:
        """Compose aligned nighttime-light rasters into a new raster."""
        try:
            resolved_raster_paths = _resolve_optional_path_list(
                tool="composite_ntl_rasters",
                parameter="raster_paths",
                values=raster_paths,
                workdir=captured_workdir,
            )
            resolved_output = _resolve_path(
                tool="composite_ntl_rasters",
                parameter="output",
                value=output,
                workdir=captured_workdir,
            )
        except _ResolvedPathError as exc:
            return exc.payload
        return _payload(
            ntl_core.composite_ntl_rasters(
                resolved_raster_paths or [],
                resolved_output,
                method=method,
            )
        )

    @mcp.tool(
        name="analyze_ntl_trend",
        description="Analyze temporal NTL trend across aligned rasters and write new slope and p-value rasters.",
        annotations=_WRITE_NEW,
        structured_output=True,
    )
    def analyze_ntl_trend(
        raster_paths: list[str],
        vector: str,
        output_prefix: str,
    ) -> dict[str, Any]:
        """Analyze trend across aligned nighttime-light rasters inside a vector extent."""
        try:
            resolved_raster_paths = _resolve_optional_path_list(
                tool="analyze_ntl_trend",
                parameter="raster_paths",
                values=raster_paths,
                workdir=captured_workdir,
            )
            resolved_vector = _resolve_path(
                tool="analyze_ntl_trend",
                parameter="vector",
                value=vector,
                workdir=captured_workdir,
            )
            resolved_prefix = _resolve_path(
                tool="analyze_ntl_trend",
                parameter="output_prefix",
                value=output_prefix,
                workdir=captured_workdir,
            )
        except _ResolvedPathError as exc:
            return exc.payload
        return _payload(
            ntl_core.analyze_ntl_trend(
                resolved_raster_paths or [],
                resolved_vector,
                resolved_prefix,
            )
        )

    @mcp.tool(
        name="detect_ntl_anomaly",
        description="Detect anomalies across aligned NTL rasters and write a new anomaly mask raster.",
        annotations=_WRITE_NEW,
        structured_output=True,
    )
    def detect_ntl_anomaly(
        raster_paths: list[str],
        output: str,
        target_index: int | None = None,
        k_sigma: float = 3.0,
    ) -> dict[str, Any]:
        """Detect anomalous nighttime-light pixels from an aligned raster series."""
        try:
            resolved_raster_paths = _resolve_optional_path_list(
                tool="detect_ntl_anomaly",
                parameter="raster_paths",
                values=raster_paths,
                workdir=captured_workdir,
            )
            resolved_output = _resolve_path(
                tool="detect_ntl_anomaly",
                parameter="output",
                value=output,
                workdir=captured_workdir,
            )
        except _ResolvedPathError as exc:
            return exc.payload
        return _payload(
            ntl_core.detect_ntl_anomaly(
                resolved_raster_paths or [],
                resolved_output,
                target_index=target_index,
                k_sigma=k_sigma,
            )
        )

    @mcp.tool(
        name="validate_geodata",
        description="Validate raster and vector inputs for readability, CRS alignment, and spatial compatibility.",
        annotations=_READ_ONLY,
        structured_output=True,
    )
    def validate_geodata(
        raster_paths: list[str] | None = None,
        vector_paths: list[str] | None = None,
    ) -> dict[str, Any]:
        """Validate raster and vector datasets without writing outputs."""
        try:
            resolved_rasters = _resolve_optional_path_list(
                tool="validate_geodata",
                parameter="raster_paths",
                values=raster_paths,
                workdir=captured_workdir,
            )
            resolved_vectors = _resolve_optional_path_list(
                tool="validate_geodata",
                parameter="vector_paths",
                values=vector_paths,
                workdir=captured_workdir,
            )
        except _ResolvedPathError as exc:
            return exc.payload
        return _payload(
            raster_core.validate_geodata(
                raster_paths=resolved_rasters,
                vector_paths=resolved_vectors,
            )
        )

    return mcp


class _ResolvedPathError(Exception):
    def __init__(self, payload: dict[str, Any]) -> None:
        super().__init__("invalid path")
        self.payload = payload


def main() -> None:
    build_gis_core_mcp().run(transport="stdio")


if __name__ == "__main__":
    main()
