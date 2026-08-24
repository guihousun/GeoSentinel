from __future__ import annotations

import asyncio
import importlib.util
import io
import json
import os
import sys
from contextlib import redirect_stdout
from pathlib import Path

from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

from ntl_toolkit.schemas import ToolResult

EXPECTED_TOOLS = [
    "validate_download_environment",
    "download_gee_raster",
    "submit_gee_batch_export",
    "inspect_gee_batch_export",
    "cancel_gee_batch_export",
    "download_vnp46a1_official_h5",
    "download_vnp46a2_official_h5_country",
    "inspect_download_run",
]
REPO_ROOT = Path(__file__).resolve().parents[3]
SERVER_FILE = REPO_ROOT / "mcp_servers" / "download_server.py"


class _FakeContext:
    def __init__(self) -> None:
        self.events: list[tuple[float, float | None, str | None]] = []

    async def report_progress(
        self,
        progress: float,
        total: float | None = None,
        message: str | None = None,
    ) -> None:
        self.events.append((progress, total, message))


def _load_server_module():
    spec = importlib.util.spec_from_file_location("download_server_under_test", SERVER_FILE)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    stdout = io.StringIO()
    with redirect_stdout(stdout):
        spec.loader.exec_module(module)
    assert stdout.getvalue() == ""
    return module


def test_download_catalog_is_sealed_and_excludes_context_parameter() -> None:
    from ntl_toolkit.adapters.mcp.download import build_download_mcp

    tools = asyncio.run(build_download_mcp().list_tools())

    assert [tool.name for tool in tools] == EXPECTED_TOOLS
    assert all(tool.inputSchema["additionalProperties"] is False for tool in tools)
    vnp_tool = next(tool for tool in tools if tool.name == "download_vnp46a2_official_h5_country")
    assert "ctx" not in vnp_tool.inputSchema["properties"]


def test_vnp_adapter_forwards_threaded_progress(monkeypatch, runtime_workspace: Path) -> None:
    from ntl_toolkit.adapters.mcp import download

    def fake_vnp(_request, *, progress):
        progress(0, 4, "prepare")
        progress(4, 4, "completed")
        return ToolResult.succeeded(
            tool="download_vnp46a2_official_h5_country",
            summary="done",
        )

    monkeypatch.setattr(download, "run_vnp46a2_download", fake_vnp)
    context = _FakeContext()

    result = asyncio.run(
        download._run_vnp_tool(
            context,
            {
                "start_date": "2026-02-13",
                "end_date": "2026-02-14",
                "countries": ["ISR"],
                "output_root": str(runtime_workspace / "outputs" / "runs"),
            },
            runtime_workspace,
        )
    )

    assert result["status"] == "succeeded"
    assert context.events == [(0.0, 4.0, "prepare"), (4.0, 4.0, "completed")]


def test_environment_readiness_only_reports_token_presence(monkeypatch) -> None:
    from ntl_toolkit.adapters.mcp import download

    monkeypatch.setenv("EARTHDATA_TOKEN", "do-not-print")
    monkeypatch.setattr(download, "_dependency_versions", lambda: ({"h5py": "x"}, {}))

    result = download.validate_download_environment()

    assert result["status"] == "succeeded"
    assert result["metrics"]["earthdata_token_configured"] is True
    assert "do-not-print" not in json.dumps(result)


def test_environment_readiness_redacts_gee_initialization_errors(monkeypatch) -> None:
    from ntl_toolkit.adapters.mcp import download
    from ntl_toolkit.core import gee_download

    monkeypatch.setattr(download, "_dependency_versions", lambda: ({"h5py": "x"}, {}))
    monkeypatch.setattr(
        gee_download,
        "_initialize_ee",
        lambda _project: (_ for _ in ()).throw(RuntimeError("Bearer do-not-print")),
    )

    result = download.validate_download_environment(initialize_gee=True)

    assert result["status"] == "failed"
    assert result["error"]["code"] == "GEE_NOT_INITIALIZED"
    assert "do-not-print" not in json.dumps(result)
    assert "<REDACTED>" in result["error"]["details"]["gee_error"]


def test_launcher_import_is_quiet_and_stdio_smoke(runtime_workspace: Path) -> None:
    _load_server_module()

    async def exercise() -> tuple[dict[str, object], list[str], list[str], dict[str, object]]:
        parameters = StdioServerParameters(
            command=sys.executable,
            args=["-m", "ntl_toolkit.adapters.mcp.download"],
            cwd=str(REPO_ROOT),
            env={**os.environ, "NTL_MCP_WORKDIR": str(runtime_workspace)},
        )
        async with stdio_client(parameters) as (read_stream, write_stream):
            async with ClientSession(read_stream, write_stream) as session:
                initialized = await session.initialize()
                tools = await session.list_tools()
                resources = await session.list_resources()
                plan = await session.call_tool(
                    "download_vnp46a2_official_h5_country",
                    {
                        "start_date": "2026-02-13",
                        "end_date": "2026-02-14",
                        "countries": ["ISR"],
                        "output_root": "outputs/runs",
                    },
                )
                return (
                    initialized.model_dump(mode="json"),
                    [tool.name for tool in tools.tools],
                    [str(resource.uri) for resource in resources.resources],
                    plan.model_dump(mode="json"),
                )

    initialized, tools, resources, plan = asyncio.run(asyncio.wait_for(exercise(), timeout=20))

    assert initialized["serverInfo"]["name"] == "ntl-download"
    assert tools == EXPECTED_TOOLS
    assert resources == ["ntl://download/capabilities", "ntl://schemas/result-v1"]
    assert plan["isError"] is False
    assert plan["structuredContent"]["status"] == "succeeded"
