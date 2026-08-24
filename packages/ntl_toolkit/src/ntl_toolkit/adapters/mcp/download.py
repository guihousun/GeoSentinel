from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import os
from collections.abc import Callable
from dataclasses import fields
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import Context, FastMCP
from mcp.types import TextContent, Tool, ToolAnnotations

from ntl_toolkit.core.gee_batch import (
    GeeBatchExportRequest,
    cancel_gee_batch_export,
    inspect_gee_batch_export,
    submit_gee_batch_export,
)
from ntl_toolkit.core.gee_download import GeeDownloadRequest, download_gee_raster
from ntl_toolkit.core.vnp46a2_download import (
    Vnp46a2DownloadRequest,
    inspect_vnp46a2_run,
    run_vnp46a2_download,
)
from ntl_toolkit.core.vnp46a1_download import (
    Vnp46a1DownloadRequest,
    inspect_vnp46a1_run,
    run_vnp46a1_download,
)
from ntl_toolkit.runtime import (
    load_runtime_environment,
    resolve_local_path,
    runtime_workdir,
    sanitize_download_text,
)
from ntl_toolkit.schemas import ToolError, ToolResult

_CAPABILITIES_PATH = Path(__file__).with_name("download_capabilities.json")
_SERVER_INSTRUCTIONS = (
    "Local direct GEE, recoverable GEE batch export, and official VNP Earthdata download tools. "
    "Long calls emit progress and all asynchronous exports write sanitized manifests. "
    "Credentials are read only from environment variables or NTL_MCP_ENV_FILE."
)
_READ_ONLY = ToolAnnotations(
    readOnlyHint=True,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=True,
)
_WRITE_NEW = ToolAnnotations(
    readOnlyHint=False,
    destructiveHint=False,
    idempotentHint=False,
    openWorldHint=True,
)
_READ_UPDATE = ToolAnnotations(
    readOnlyHint=False,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=True,
)
_DEPENDENCIES = ("ee", "geemap", "h5py", "rasterio", "geopandas", "osmnx")


class StrictFastMCP(FastMCP):
    """FastMCP with sealed client schemas and context-aware argument checking."""

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
        self._allowed_fields_by_tool[tool_name] = [
            parameter.name
            for parameter in inspect.signature(fn).parameters.values()
            if parameter.name != "ctx" and parameter.annotation is not Context
        ]
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
        return [
            Tool.model_validate(
                {
                    **tool.model_dump(mode="python"),
                    "inputSchema": {**dict(tool.inputSchema), "additionalProperties": False},
                }
            )
            for tool in tools
        ]

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> Any:
        allowed_fields = self._allowed_fields_by_tool.get(name)
        if allowed_fields is not None:
            unexpected_fields = sorted(set(arguments) - set(allowed_fields))
            if unexpected_fields:
                payload = _failed_payload(
                    tool=name,
                    code="INVALID_PARAMETER",
                    message="Unexpected tool arguments were provided.",
                    suggestion="Remove unsupported fields and retry with only documented arguments.",
                    details={"unexpected_fields": unexpected_fields, "allowed_fields": allowed_fields},
                )
                return [TextContent(type="text", text=json.dumps(payload, ensure_ascii=False))], payload
        return await super().call_tool(name, arguments)


def validate_download_environment(
    *,
    token_env: str = "EARTHDATA_TOKEN",
    initialize_gee: bool = False,
    project: str | None = None,
) -> dict[str, Any]:
    """Report local download readiness without exposing credential values."""
    versions, errors = _dependency_versions()
    metrics: dict[str, Any] = {
        "dependencies": versions,
        "earthdata_token_env": token_env,
        "earthdata_token_configured": bool(os.getenv(token_env, "").strip()),
        "gee_initialization_requested": initialize_gee,
    }
    if errors:
        return _failed_payload(
            tool="validate_download_environment",
            code="DEPENDENCY_MISSING",
            message="One or more download dependencies are unavailable.",
            suggestion="Install the missing packages in NTL-GPT-Stable and retry.",
            details={"errors": errors, **metrics},
        )
    if initialize_gee:
        try:
            from ntl_toolkit.core.gee_download import _initialize_ee

            _initialize_ee(project)
            metrics["gee_initialized"] = True
        except Exception as exc:  # noqa: BLE001
            metrics["gee_initialized"] = False
            metrics["gee_error"] = sanitize_download_text(str(exc))
            return _failed_payload(
                tool="validate_download_environment",
                code="GEE_NOT_INITIALIZED",
                message="Earth Engine could not be initialized without OAuth.",
                suggestion="Use EasyGEE to inspect or authorize the local Earth Engine setup, then retry.",
                details=metrics,
            )
    return _payload(
        ToolResult.succeeded(
            tool="validate_download_environment",
            summary="Validated local download runtime availability.",
            metrics=metrics,
        )
    )


async def _run_vnp_tool(
    ctx: Context,
    arguments: dict[str, Any],
    workdir: Path,
) -> dict[str, Any]:
    try:
        values = {
            item.name: arguments[item.name]
            for item in fields(Vnp46a2DownloadRequest)
            if item.name in arguments
        }
        values["targets"] = values.get("targets") or []
        request = Vnp46a2DownloadRequest(
            **{
                **values,
                "output_root": str(resolve_local_path(values["output_root"], workdir)),
                "package_source_root": _resolve_optional_path(values.get("package_source_root"), workdir),
                "package_output_root": _resolve_optional_path(values.get("package_output_root"), workdir),
            }
        )
    except (KeyError, ValueError, TypeError) as exc:
        return _failed_payload(
            tool="download_vnp46a2_official_h5_country",
            code="INVALID_PARAMETER",
            message=str(exc),
            suggestion="Correct the VNP46A2 request and retry.",
        )
    result = await _run_with_progress(ctx, lambda progress: run_vnp46a2_download(request, progress=progress))
    return _payload(result)


async def _run_vnp46a1_tool(ctx: Context, arguments: dict[str, Any], workdir: Path) -> dict[str, Any]:
    try:
        values = {item.name: arguments[item.name] for item in fields(Vnp46a1DownloadRequest) if item.name in arguments}
        values["countries"] = values.get("countries") or []
        values["targets"] = values.get("targets") or []
        request = Vnp46a1DownloadRequest(**{**values, "output_root": str(resolve_local_path(values["output_root"], workdir))})
    except (KeyError, ValueError, TypeError) as exc:
        return _failed_payload(tool="download_vnp46a1_official_h5", code="INVALID_PARAMETER", message=str(exc), suggestion="Provide exactly one target mode, valid dates, and an output path.")
    return _payload(await _run_with_progress(ctx, lambda progress: run_vnp46a1_download(request, progress=progress)))


async def _run_gee_tool(
    ctx: Context,
    arguments: dict[str, Any],
    workdir: Path,
) -> dict[str, Any]:
    try:
        values = {
            name: arguments[name]
            for name in GeeDownloadRequest.model_fields
            if name in arguments
        }
        request = GeeDownloadRequest(
            **{**values, "output": str(resolve_local_path(values["output"], workdir))}
        )
    except (KeyError, ValueError, TypeError) as exc:
        return _failed_payload(
            tool="download_gee_raster",
            code="INVALID_PARAMETER",
            message=str(exc),
            suggestion="Correct the dataset, bands, asset type, dates, bbox, processing preset, and output path then retry.",
        )
    result = await _run_with_progress(ctx, lambda progress: download_gee_raster(request, progress=progress))
    return _payload(result)


def _submit_gee_batch_tool(arguments: dict[str, Any], workdir: Path) -> dict[str, Any]:
    try:
        values = {
            name: arguments[name]
            for name in GeeBatchExportRequest.model_fields
            if name in arguments
        }
        request = GeeBatchExportRequest(
            **{
                **values,
                "manifest_path": str(resolve_local_path(values["manifest_path"], workdir)),
            }
        )
    except (KeyError, ValueError, TypeError) as exc:
        return _failed_payload(
            tool="submit_gee_batch_export",
            code="INVALID_PARAMETER",
            message=str(exc),
            suggestion="Correct the validated batch export request, destination, and manifest path then retry.",
        )
    return _payload(submit_gee_batch_export(request))


def _inspect_gee_batch_tool(manifest_path: str, project: str | None, workdir: Path) -> dict[str, Any]:
    try:
        path = resolve_local_path(manifest_path, workdir)
    except ValueError as exc:
        return _failed_payload(
            tool="inspect_gee_batch_export",
            code="INVALID_PARAMETER",
            message=str(exc),
            suggestion="Use an ordinary relative path or a fully-qualified absolute manifest path.",
        )
    return _payload(inspect_gee_batch_export(str(path), project=project))


def _cancel_gee_batch_tool(manifest_path: str, project: str | None, workdir: Path) -> dict[str, Any]:
    try:
        path = resolve_local_path(manifest_path, workdir)
    except ValueError as exc:
        return _failed_payload(
            tool="cancel_gee_batch_export",
            code="INVALID_PARAMETER",
            message=str(exc),
            suggestion="Use an ordinary relative path or a fully-qualified absolute manifest path.",
        )
    return _payload(cancel_gee_batch_export(str(path), project=project))


async def _run_with_progress(
    ctx: Context,
    operation: Callable[[Callable[[float, float | None, str], None]], ToolResult],
) -> ToolResult:
    """Run blocking download work in a thread while forwarding progress notifications."""
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[tuple[float, float | None, str]] = asyncio.Queue()

    def report(progress: float, total: float | None, message: str) -> None:
        loop.call_soon_threadsafe(queue.put_nowait, (float(progress), total, message))

    task = asyncio.create_task(asyncio.to_thread(operation, report))
    while not task.done():
        try:
            progress, total, message = await asyncio.wait_for(queue.get(), timeout=0.1)
        except TimeoutError:
            continue
        await ctx.report_progress(progress, total, message)
    result = await task
    while not queue.empty():
        progress, total, message = queue.get_nowait()
        await ctx.report_progress(progress, total, message)
    return result


def build_download_mcp() -> FastMCP:
    """Build the local synchronous download MCP server."""
    load_runtime_environment()
    captured_workdir = runtime_workdir()
    mcp = StrictFastMCP("ntl-download", instructions=_SERVER_INSTRUCTIONS)

    @mcp.resource(
        "ntl://download/capabilities",
        name="download-capabilities",
        description="Static capability definitions for local download tools.",
        mime_type="application/json",
    )
    def download_capabilities_resource() -> str:
        return _CAPABILITIES_PATH.read_text(encoding="utf-8")

    @mcp.resource(
        "ntl://schemas/result-v1",
        name="tool-result-schema",
        description="JSON schema for the shared structured result payload.",
        mime_type="application/json",
    )
    def result_schema_resource() -> str:
        return json.dumps(ToolResult.model_json_schema(), ensure_ascii=False, indent=2)

    @mcp.tool(
        name="validate_download_environment",
        description="Check local download dependencies and optional GEE initialization without printing credentials.",
        annotations=_READ_ONLY,
        structured_output=True,
    )
    def validate_download_environment_tool(
        token_env: str = "EARTHDATA_TOKEN",
        initialize_gee: bool = False,
        project: str | None = None,
    ) -> dict[str, Any]:
        return validate_download_environment(
            token_env=token_env,
            initialize_gee=initialize_gee,
            project=project,
        )

    @mcp.tool(
        name="download_gee_raster",
        description=(
            "Export a validated GEE Image or ImageCollection reduction to a local GeoTIFF. Supports multiple "
            "bands and controlled Sentinel-2, Landsat, MODIS, and normalized-difference preprocessing presets."
        ),
        annotations=_WRITE_NEW,
        structured_output=True,
    )
    async def download_gee_raster_tool(
        ctx: Context,
        dataset_id: str,
        bbox: list[float],
        output: str,
        band: str | None = None,
        bands: list[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        asset_type: str = "auto",
        reducer: str = "first",
        scale: int = 500,
        crs: str = "EPSG:4326",
        project: str | None = None,
        processing_preset: str = "raw",
        quality_threshold: float = 0.60,
        index_bands: list[str] | None = None,
        output_band_name: str = "index",
    ) -> dict[str, Any]:
        return await _run_gee_tool(ctx, locals(), captured_workdir)

    @mcp.tool(
        name="submit_gee_batch_export",
        description=(
            "Submit a validated large Earth Engine raster export to Drive, Cloud Storage, or an EE asset. "
            "Returns immediately with a task id and writes a recoverable status manifest."
        ),
        annotations=_WRITE_NEW,
        structured_output=True,
    )
    def submit_gee_batch_export_tool(
        dataset_id: str,
        bands: list[str],
        bbox: list[float],
        manifest_path: str,
        description: str,
        destination: str = "drive",
        start_date: str | None = None,
        end_date: str | None = None,
        asset_type: str = "auto",
        reducer: str = "median",
        scale: int = 500,
        crs: str = "EPSG:4326",
        project: str | None = None,
        processing_preset: str = "raw",
        quality_threshold: float = 0.60,
        index_bands: list[str] | None = None,
        output_band_name: str = "index",
        file_name_prefix: str | None = None,
        drive_folder: str | None = None,
        bucket: str | None = None,
        bucket_prefix: str | None = None,
        asset_id: str | None = None,
        max_pixels: int = 10_000_000_000_000,
    ) -> dict[str, Any]:
        return _submit_gee_batch_tool(locals(), captured_workdir)

    @mcp.tool(
        name="inspect_gee_batch_export",
        description="Read a GEE export manifest, refresh its live task state, and report progress or exact failure details.",
        annotations=_READ_UPDATE,
        structured_output=True,
    )
    def inspect_gee_batch_export_tool(
        manifest_path: str,
        project: str | None = None,
    ) -> dict[str, Any]:
        return _inspect_gee_batch_tool(manifest_path, project, captured_workdir)

    @mcp.tool(
        name="cancel_gee_batch_export",
        description="Request cancellation of a non-terminal GEE export represented by a local export manifest.",
        annotations=_WRITE_NEW,
        structured_output=True,
    )
    def cancel_gee_batch_export_tool(
        manifest_path: str,
        project: str | None = None,
    ) -> dict[str, Any]:
        return _cancel_gee_batch_tool(manifest_path, project, captured_workdir)

    @mcp.tool(
        name="download_vnp46a1_official_h5",
        description="Run official VNP46A1 Earthdata HDF5 retrieval for one country or WGS84 BBox, with optional UTC_Time GeoTIFF output.",
        annotations=_WRITE_NEW,
        structured_output=True,
    )
    async def download_vnp46a1_official_h5(
        ctx: Context,
        start_date: str,
        end_date: str,
        output_root: str,
        countries: list[str] | None = None,
        bbox: list[float] | None = None,
        include_utc_time: bool = False,
        phase: str = "full",
        execution_mode: str = "plan",
        targets: list[str] | None = None,
        workers: int = 4,
        download_timeout: int = 600,
        token_env: str = "EARTHDATA_TOKEN",
        force: bool = False,
    ) -> dict[str, Any]:
        return await _run_vnp46a1_tool(ctx, locals(), captured_workdir)

    @mcp.tool(
        name="download_vnp46a2_official_h5_country",
        description="Run the audited official VNP46A2 HDF5 country mosaic pipeline with progress and recoverable manifests.",
        annotations=_WRITE_NEW,
        structured_output=True,
    )
    async def download_vnp46a2_official_h5_country(
        ctx: Context,
        start_date: str,
        end_date: str,
        countries: list[str],
        output_root: str,
        phase: str = "full",
        execution_mode: str = "plan",
        targets: list[str] | None = None,
        limit_days: int = 0,
        workers: int = 4,
        download_timeout: int = 600,
        token_env: str = "EARTHDATA_TOKEN",
        no_gee_latest: bool = False,
        force: bool = False,
        skip_pixel_scan: bool = False,
        package_source_root: str = "",
        package_output_root: str = "",
        package_copy: bool = False,
    ) -> dict[str, Any]:
        return await _run_vnp_tool(ctx, locals(), captured_workdir)

    @mcp.tool(
        name="inspect_download_run",
        description="Inspect a VNP46A1 or VNP46A2 run audit and return actual artifacts, status counts, and exact retry targets.",
        annotations=_READ_ONLY,
        structured_output=True,
    )
    def inspect_download_run(run_root: str) -> dict[str, Any]:
        try:
            root = resolve_local_path(run_root, captured_workdir)
            result = inspect_vnp46a1_run(root) if ((root / "vnp46a1_audit.json").exists() or (root / "vnp46a1_runtime.json").exists()) else inspect_vnp46a2_run(root)
            return _payload(result)
        except ValueError as exc:
            return _failed_payload(
                tool="inspect_download_run",
                code="INVALID_PARAMETER",
                message=str(exc),
                suggestion="Use an ordinary relative path or a fully-qualified absolute path.",
            )

    return mcp


def _dependency_versions() -> tuple[dict[str, str], dict[str, str]]:
    versions: dict[str, str] = {}
    errors: dict[str, str] = {}
    for module_name in _DEPENDENCIES:
        try:
            module = importlib.import_module(module_name)
            versions[module_name] = str(getattr(module, "__version__", "available"))
        except (ImportError, OSError) as exc:
            errors[module_name] = f"{type(exc).__name__}: {exc}"
    return versions, errors


def _resolve_optional_path(value: Any, workdir: Path) -> str:
    if not str(value or "").strip():
        return ""
    return str(resolve_local_path(str(value), workdir))


def _payload(result: ToolResult) -> dict[str, Any]:
    return result.model_dump(mode="json")


def _failed_payload(
    *,
    tool: str,
    code: str,
    message: str,
    suggestion: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return _payload(
        ToolResult.failed(
            tool=tool,
            error=ToolError(code=code, message=message, suggestion=suggestion, details=details or {}),
        )
    )


def main() -> None:
    build_download_mcp().run(transport="stdio")


if __name__ == "__main__":
    main()
