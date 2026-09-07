"""LangChain adapter for the official VNP46A1 Earthdata HDF5 route.

The download implementation already lives in ``ntl_toolkit.core`` and is also
exposed by the local ``ntl-download`` MCP server.  This small adapter makes the
same bounded bbox/country route callable by the active NTL_Data_Searcher role;
it does not create a second downloader or silently substitute GEE/VNP46A2.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal, Optional

from langchain_core.runnables import RunnableConfig
from langchain_core.runnables.config import var_child_runnable_config
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field, model_validator

from ntl_toolkit.core.vnp46a1_download import (
    Vnp46a1DownloadRequest,
    run_vnp46a1_download,
)
from storage_manager import current_thread_id, storage_manager


class OfficialVNP46A1H5Input(BaseModel):
    start_date: str = Field(..., description="Inclusive UTC product date in YYYY-MM-DD.")
    end_date: str = Field(..., description="Inclusive UTC product date in YYYY-MM-DD.")
    output_root: str = Field(
        default="official_vnp46a1_h5_runs",
        description="Workspace-relative directory under the current thread outputs/.",
    )
    countries: list[str] = Field(
        default_factory=list,
        description="Optional ISO3 country list. Use exactly one country when bbox is omitted.",
    )
    bbox: list[float] | None = Field(
        default=None,
        description="Optional WGS84 [west, south, east, north] AOI. Use this for a city or local AOI.",
    )
    include_utc_time: bool = Field(
        default=True,
        description="Write the official UTC_Time raster alongside at-sensor radiance.",
    )
    phase: Literal["full", "prepare", "download", "mosaic", "audit"] = "full"
    execution_mode: Literal["plan", "run"] = "plan"
    targets: list[str] = Field(default_factory=list, description="Optional retry targets TARGET_ID:YYYY-MM-DD.")
    workers: int = Field(default=4, ge=1, le=8)
    download_timeout: int = Field(default=600, ge=60, le=1800)
    token_env: str = Field(default="EARTHDATA_TOKEN", description="Earthdata bearer-token variable name.")
    force: bool = False

    @model_validator(mode="after")
    def _validate_target_mode(self) -> "OfficialVNP46A1H5Input":
        if bool(self.countries) == bool(self.bbox):
            raise ValueError("Provide exactly one target mode: countries or bbox.")
        if self.bbox is not None and len(self.bbox) != 4:
            raise ValueError("bbox must be [west, south, east, north].")
        return self


def _thread_id(config: Optional[RunnableConfig] = None) -> str:
    runtime_config = config if isinstance(config, dict) else var_child_runnable_config.get()
    if isinstance(runtime_config, dict):
        try:
            resolved = str(storage_manager.get_thread_id_from_config(runtime_config) or "").strip()
            if resolved:
                return resolved
        except Exception:
            pass
    return str(current_thread_id.get() or "debug").strip() or "debug"


def _workspace_output_root(output_root: str, thread_id: str):
    workspace = storage_manager.get_workspace(thread_id=thread_id)
    outputs_root = (workspace / "outputs").resolve()
    raw = str(output_root or "").strip() or "official_vnp46a1_h5_runs"
    candidate = Path(raw)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise ValueError("output_root must be a workspace-relative outputs path without '..'.")
    if candidate.parts and candidate.parts[0] in {"inputs", "memory"}:
        raise PermissionError("output_root must be under outputs/, not inputs/ or memory/.")
    target = (workspace / candidate).resolve() if candidate.parts and candidate.parts[0] == "outputs" else (outputs_root / candidate).resolve()
    if target != outputs_root and outputs_root not in target.parents:
        raise PermissionError("output_root resolved outside the current thread outputs directory.")
    return target


def _dump(result: Any) -> dict[str, Any]:
    if hasattr(result, "model_dump"):
        return result.model_dump(mode="json", by_alias=True)
    return dict(result)


def run_official_vnp46a1_h5(
    start_date: str,
    end_date: str,
    output_root: str = "official_vnp46a1_h5_runs",
    countries: Optional[list[str]] = None,
    bbox: Optional[list[float]] = None,
    include_utc_time: bool = True,
    phase: Literal["full", "prepare", "download", "mosaic", "audit"] = "full",
    execution_mode: Literal["plan", "run"] = "plan",
    targets: Optional[list[str]] = None,
    workers: int = 4,
    download_timeout: int = 600,
    token_env: str = "EARTHDATA_TOKEN",
    force: bool = False,
    config: Optional[RunnableConfig] = None,
) -> dict[str, Any]:
    """Plan or execute official VNP46A1 radiance + UTC_Time retrieval."""

    thread_id = _thread_id(config)
    resolved_root = _workspace_output_root(output_root, thread_id)
    request = Vnp46a1DownloadRequest(
        start_date=start_date,
        end_date=end_date,
        output_root=str(resolved_root),
        countries=list(countries or []),
        bbox=bbox,
        include_utc_time=include_utc_time,
        phase=phase,
        execution_mode=execution_mode,
        targets=list(targets or []),
        workers=workers,
        download_timeout=download_timeout,
        token_env=token_env,
        force=force,
    )
    if execution_mode == "plan":
        return {
            "tool": "official_vnp46a1_h5_tool",
            "status": "plan",
            "thread_id": thread_id,
            "product": "VNP46A1",
            "band": "DNB_At_Sensor_Radiance_500m",
            "include_utc_time": include_utc_time,
            "target_mode": request.target_mode,
            "target_id": request.target_id,
            "start_date": start_date,
            "end_date": end_date,
            "output_root": str(resolved_root),
            "next_action": "Call again with execution_mode='run' only after the official Earthdata route is accepted.",
        }
    result = run_vnp46a1_download(request)
    payload = _dump(result)
    payload.update({"tool": "official_vnp46a1_h5_tool", "thread_id": thread_id, "output_root": str(resolved_root)})
    return payload


official_vnp46a1_h5_tool = StructuredTool.from_function(
    func=run_official_vnp46a1_h5,
    name="official_vnp46a1_h5_tool",
    description=(
        "Plan or run the official NASA Earthdata VNP46A1 HDF5 route for a bounded country or WGS84 bbox. "
        "It preserves at-sensor DNB radiance and optionally writes UTC_Time; it is not a VNP46A2 or GEE substitute."
    ),
    args_schema=OfficialVNP46A1H5Input,
)


__all__ = ["OfficialVNP46A1H5Input", "official_vnp46a1_h5_tool", "run_official_vnp46a1_h5"]

