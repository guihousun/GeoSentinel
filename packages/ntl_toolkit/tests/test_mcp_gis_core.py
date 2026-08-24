from __future__ import annotations

import asyncio
import importlib.util
import io
import json
import os
import subprocess
import sys
import zipfile
from contextlib import redirect_stdout
from pathlib import Path

import rasterio
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

from ntl_toolkit.schemas import ToolResult

EXPECTED_TOOL_NAMES = [
    "validate_environment",
    "inspect_vector",
    "inspect_raster",
    "filter_points_by_polygon",
    "spatial_join_points_to_admin",
    "buffer_points_aeqd",
    "dissolve_intersections",
    "clip_raster",
    "reproject_raster",
    "mosaic_rasters",
    "calculate_zonal_statistics",
    "calculate_ntl_metrics",
    "composite_ntl_rasters",
    "analyze_ntl_trend",
    "detect_ntl_anomaly",
    "validate_geodata",
]

READ_ONLY_ANNOTATIONS = {
    "title": None,
    "readOnlyHint": True,
    "destructiveHint": False,
    "idempotentHint": True,
    "openWorldHint": False,
}

WRITE_NEW_ANNOTATIONS = {
    "title": None,
    "readOnlyHint": False,
    "destructiveHint": False,
    "idempotentHint": False,
    "openWorldHint": False,
}

EXPECTED_INPUT_PROPERTIES = {
    "validate_environment": [],
    "inspect_vector": ["path"],
    "inspect_raster": ["path", "mode", "sample_pixels"],
    "filter_points_by_polygon": [
        "points",
        "polygon",
        "output",
        "lon_col",
        "lat_col",
        "predicate",
    ],
    "spatial_join_points_to_admin": [
        "points",
        "admin",
        "output",
        "lon_col",
        "lat_col",
        "admin_name_col",
        "admin_iso_col",
        "prefix",
    ],
    "buffer_points_aeqd": [
        "points",
        "output",
        "radius_km",
        "lon_col",
        "lat_col",
    ],
    "dissolve_intersections": ["polygons", "output", "id_col"],
    "clip_raster": ["raster", "vector", "output", "all_touched"],
    "reproject_raster": ["raster", "output", "dst_crs", "resampling"],
    "mosaic_rasters": ["raster_paths", "output", "method"],
    "calculate_zonal_statistics": [
        "raster_paths",
        "vector",
        "output",
        "selected_indices",
        "only_global",
    ],
    "calculate_ntl_metrics": ["raster_path", "band", "selected_indices"],
    "composite_ntl_rasters": ["raster_paths", "output", "method"],
    "analyze_ntl_trend": ["raster_paths", "vector", "output_prefix"],
    "detect_ntl_anomaly": ["raster_paths", "output", "target_index", "k_sigma"],
    "validate_geodata": ["raster_paths", "vector_paths"],
}

READ_ONLY_TOOLS = {
    "validate_environment",
    "inspect_vector",
    "inspect_raster",
    "calculate_ntl_metrics",
    "validate_geodata",
}

REPO_ROOT = Path(__file__).resolve().parents[3]
PACKAGE_ROOT = Path(__file__).resolve().parents[1]
SERVER_FILE = REPO_ROOT / "mcp_servers" / "gis_core_server.py"


def _load_server_module():
    spec = importlib.util.spec_from_file_location("gis_core_server_under_test", SERVER_FILE)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    stdout = io.StringIO()
    with redirect_stdout(stdout):
        spec.loader.exec_module(module)
    assert stdout.getvalue() == ""
    return module


def _resource_text(contents: list[object]) -> str:
    assert len(contents) == 1
    content = contents[0]
    text = getattr(content, "text", None)
    if text is None:
        text = getattr(content, "content", None)
    assert isinstance(text, str)
    return text


def _tool_payloads(server: object) -> list[dict[str, object]]:
    tools = asyncio.run(server.list_tools())
    return [tool.model_dump(mode="json") for tool in tools]


def _call_tool(server: object, name: str, arguments: dict[str, object]) -> dict[str, object]:
    result = asyncio.run(server.call_tool(name, arguments))
    assert isinstance(result, tuple)
    assert len(result) == 2
    content, structured = result
    assert isinstance(content, list)
    assert isinstance(structured, dict)
    return structured


def test_tool_catalog_annotations_and_input_schemas() -> None:
    from ntl_toolkit.adapters.mcp.gis_core import build_gis_core_mcp

    server = build_gis_core_mcp()
    tools = _tool_payloads(server)

    assert [tool["name"] for tool in tools] == EXPECTED_TOOL_NAMES

    for tool in tools:
        name = str(tool["name"])
        assert tool["inputSchema"]["type"] == "object"
        assert tool["inputSchema"]["additionalProperties"] is False
        assert list(tool["inputSchema"]["properties"]) == EXPECTED_INPUT_PROPERTIES[name]
        expected_annotations = (
            READ_ONLY_ANNOTATIONS if name in READ_ONLY_TOOLS else WRITE_NEW_ANNOTATIONS
        )
        assert tool["annotations"] == expected_annotations


def test_resource_catalog_and_payloads_match_expected_schema() -> None:
    from ntl_toolkit.adapters.mcp.gis_core import build_gis_core_mcp

    server = build_gis_core_mcp()

    resources = asyncio.run(server.list_resources())
    resource_payloads = [resource.model_dump(mode="json") for resource in resources]

    assert [payload["uri"] for payload in resource_payloads] == [
        "ntl://gis/capabilities",
        "ntl://schemas/result-v1",
    ]

    capabilities = json.loads(
        _resource_text(asyncio.run(server.read_resource("ntl://gis/capabilities")))
    )
    assert isinstance(capabilities, list)
    assert [entry["name"] for entry in capabilities] == EXPECTED_TOOL_NAMES
    for entry in capabilities:
        assert sorted(entry) == [
            "accepted_formats",
            "common_error_codes",
            "name",
            "purpose",
            "side_effects",
        ]

    result_schema = json.loads(
        _resource_text(asyncio.run(server.read_resource("ntl://schemas/result-v1")))
    )
    assert result_schema == ToolResult.model_json_schema()


def test_validate_environment_reports_versions_on_success() -> None:
    from ntl_toolkit.adapters.mcp import gis_core

    result = gis_core.validate_environment()

    assert result["schema"] == "ntl.tool.result.v1"
    assert result["status"] == "succeeded"
    assert result["tool"] == "validate_environment"
    assert sorted(result["metrics"]["versions"]) == [
        "geopandas",
        "numpy",
        "pandas",
        "pyproj",
        "rasterio",
        "scipy",
        "shapely",
    ]


def test_validate_environment_reports_missing_dependencies(monkeypatch) -> None:
    from ntl_toolkit.adapters.mcp import gis_core

    original_import_module = gis_core.importlib.import_module

    def fake_import_module(name: str):
        if name == "scipy":
            raise ModuleNotFoundError("No module named 'scipy'")
        return original_import_module(name)

    monkeypatch.setattr(gis_core.importlib, "import_module", fake_import_module)

    result = gis_core.validate_environment()

    assert result["status"] == "failed"
    assert result["error"]["code"] == "DEPENDENCY_MISSING"
    assert result["error"]["details"]["missing"] == ["scipy"]
    assert "available_versions" in result["error"]["details"]
    assert result["error"]["suggestion"]


def test_validate_environment_reports_import_and_os_errors(monkeypatch) -> None:
    from ntl_toolkit.adapters.mcp import gis_core

    original_import_module = gis_core.importlib.import_module

    def fake_import_module(name: str):
        if name == "scipy":
            raise ImportError("scipy ABI mismatch")
        if name == "rasterio":
            raise OSError("gdal_data missing")
        return original_import_module(name)

    monkeypatch.setattr(gis_core.importlib, "import_module", fake_import_module)

    result = gis_core.validate_environment()

    assert result["status"] == "failed"
    assert result["error"]["code"] == "DEPENDENCY_MISSING"
    assert result["error"]["details"]["missing"] == ["rasterio", "scipy"]
    assert result["error"]["details"]["errors"] == {
        "rasterio": {"type": "OSError", "message": "gdal_data missing"},
        "scipy": {"type": "ImportError", "message": "scipy ABI mismatch"},
    }


def test_manager_level_inspect_raster_returns_schema_result_and_uses_captured_workdir(
    runtime_workspace: Path,
    monkeypatch,
    sample_raster_path: Path,
) -> None:
    from ntl_toolkit.adapters.mcp.gis_core import build_gis_core_mcp

    server = build_gis_core_mcp()
    monkeypatch.setenv("NTL_MCP_WORKDIR", str(runtime_workspace / "other-workdir"))

    result = _call_tool(server, "inspect_raster", {"path": "inputs/sample.tif"})

    assert result["schema"] == "ntl.tool.result.v1"
    assert result["status"] == "succeeded"
    assert result["tool"] == "inspect_raster"
    assert result["metrics"]["path"] == str(sample_raster_path.resolve())


def test_manager_level_clip_reserves_distinct_outputs_and_written_rasters_reopen(
    runtime_workspace: Path,
    sample_raster_path: Path,
    clip_polygon_path: Path,
) -> None:
    from ntl_toolkit.adapters.mcp.gis_core import build_gis_core_mcp

    server = build_gis_core_mcp()
    arguments = {
        "raster": str(sample_raster_path),
        "vector": str(clip_polygon_path),
        "output": "outputs/clipped.tif",
    }

    first = _call_tool(server, "clip_raster", arguments)
    second = _call_tool(server, "clip_raster", arguments)

    first_output = Path(first["outputs"][0]["path"])
    second_output = Path(second["outputs"][0]["path"])

    assert first["status"] == "succeeded"
    assert second["status"] == "succeeded"
    assert first_output != second_output
    assert first_output.exists()
    assert second_output.exists()

    with rasterio.open(first_output) as dataset:
        assert dataset.count == 1
    with rasterio.open(second_output) as dataset:
        assert dataset.count == 1


def test_partial_windows_paths_fail_as_invalid_parameter(
    sample_raster_path: Path,
    clip_polygon_path: Path,
) -> None:
    from ntl_toolkit.adapters.mcp.gis_core import build_gis_core_mcp

    server = build_gis_core_mcp()

    inspect_result = _call_tool(server, "inspect_raster", {"path": "C:sample.tif"})
    clip_result = _call_tool(
        server,
        "clip_raster",
        {
            "raster": str(sample_raster_path),
            "vector": str(clip_polygon_path),
            "output": "C:clipped.tif",
        },
    )

    assert inspect_result["status"] == "failed"
    assert inspect_result["error"]["code"] == "INVALID_PARAMETER"
    assert clip_result["status"] == "failed"
    assert clip_result["error"]["code"] == "INVALID_PARAMETER"


def test_server_entrypoint_import_is_quiet_and_main_runs_stdio(monkeypatch) -> None:
    module = _load_server_module()
    calls: list[str] = []

    def fake_main() -> None:
        calls.append("delegated")

    monkeypatch.setattr(module, "package_main", fake_main)

    stdout = io.StringIO()
    with redirect_stdout(stdout):
        module.main()

    assert stdout.getvalue() == ""
    assert calls == ["delegated"]


def test_wheel_contains_capabilities_json_and_console_entrypoint(tmp_path: Path) -> None:
    wheel_dir = tmp_path / "wheelhouse"
    wheel_dir.mkdir()

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "wheel",
            "--no-deps",
            "--wheel-dir",
            str(wheel_dir),
            str(PACKAGE_ROOT),
        ],
        cwd=str(PACKAGE_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr

    wheel_paths = sorted(wheel_dir.glob("ntl_toolkit-*.whl"))
    assert len(wheel_paths) == 1

    with zipfile.ZipFile(wheel_paths[0]) as archive:
        names = set(archive.namelist())
        assert "ntl_toolkit/adapters/mcp/gis_capabilities.json" in names
        entry_points_name = next(
            name for name in names if name.endswith(".dist-info/entry_points.txt")
        )
        entry_points = archive.read(entry_points_name).decode("utf-8")

    assert "[console_scripts]" in entry_points
    assert "ntl-gis-core = ntl_toolkit.adapters.mcp.gis_core:main" in entry_points


def test_stdio_initialize_list_resources_and_call_tools_smoke(
    runtime_workspace: Path,
    sample_raster_path: Path,
) -> None:
    async def exercise_stdio() -> tuple[
        dict[str, object],
        list[str],
        list[str],
        dict[str, object],
        dict[str, object],
    ]:
        parameters = StdioServerParameters(
            command=sys.executable,
            args=["-m", "ntl_toolkit.adapters.mcp.gis_core"],
            cwd=str(REPO_ROOT),
            env={
                **os.environ,
                "NTL_MCP_WORKDIR": str(runtime_workspace),
            },
        )
        async with stdio_client(parameters) as (read_stream, write_stream):
            async with ClientSession(read_stream, write_stream) as session:
                initialized = await session.initialize()
                tools = await session.list_tools()
                resources = await session.list_resources()
                inspect_result = await session.call_tool(
                    "inspect_raster",
                    {"path": "inputs/sample.tif"},
                )
                invalid_result = await session.call_tool(
                    "inspect_raster",
                    {"path": "inputs/sample.tif", "unexpected": True},
                )
                return (
                    initialized.model_dump(mode="json"),
                    [tool.name for tool in tools.tools],
                    [str(resource.uri) for resource in resources.resources],
                    inspect_result.model_dump(mode="json"),
                    invalid_result.model_dump(mode="json"),
                )

    initialized, tools, resources, inspect_result, invalid_result = asyncio.run(
        asyncio.wait_for(exercise_stdio(), timeout=20)
    )

    assert initialized["serverInfo"]["name"] == "ntl-gis-core"
    assert tools == EXPECTED_TOOL_NAMES
    assert resources == [
        "ntl://gis/capabilities",
        "ntl://schemas/result-v1",
    ]
    assert inspect_result["isError"] is False
    assert inspect_result["structuredContent"]["status"] == "succeeded"
    assert inspect_result["structuredContent"]["metrics"]["path"] == str(
        sample_raster_path.resolve()
    )
    assert invalid_result["isError"] is False
    assert invalid_result["structuredContent"]["status"] == "failed"
    assert invalid_result["structuredContent"]["error"]["code"] == "INVALID_PARAMETER"
    assert invalid_result["structuredContent"]["error"]["details"] == {
        "unexpected_fields": ["unexpected"],
        "allowed_fields": ["path", "mode", "sample_pixels"],
    }
