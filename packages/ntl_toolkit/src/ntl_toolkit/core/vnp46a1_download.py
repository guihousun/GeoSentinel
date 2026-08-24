from __future__ import annotations

import hashlib
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import h5py
import numpy as np
import rasterio
from rasterio.mask import mask
from rasterio.merge import merge
from rasterio.transform import from_origin
from shapely.geometry import mapping

from ntl_toolkit.runtime.downloads import DownloadProgress, sanitize_download_text, write_download_manifest
from ntl_toolkit.schemas import OutputArtifact, ToolError, ToolResult

NODATA = -9999.0
_DATE_FORMAT = "%Y-%m-%d"
_RADIANCE_DATASET = "DNB_At_Sensor_Radiance_500m"
_RADIANCE_DATASET_CANDIDATES = (_RADIANCE_DATASET, "DNB_At_Sensor_Radiance")
_UTC_TIME_DATASET = "UTC_Time"


@dataclass(frozen=True)
class Vnp46a1DownloadRequest:
    start_date: str
    end_date: str
    output_root: str
    countries: list[str] = field(default_factory=list)
    bbox: list[float] | tuple[float, float, float, float] | None = None
    include_utc_time: bool = False
    phase: Literal["full", "prepare", "download", "mosaic", "audit"] = "full"
    execution_mode: Literal["plan", "run"] = "plan"
    targets: list[str] = field(default_factory=list)
    workers: int = 4
    download_timeout: int = 600
    token_env: str = "EARTHDATA_TOKEN"
    force: bool = False

    def __post_init__(self) -> None:
        _parse_date(self.start_date, "start_date")
        _parse_date(self.end_date, "end_date")
        if self.end_date < self.start_date:
            raise ValueError("end_date must be on or after start_date")
        countries = sorted({str(value).strip().upper() for value in self.countries if str(value).strip()})
        normalized_bbox = _normalize_bbox(self.bbox) if self.bbox is not None else None
        if bool(countries) == bool(normalized_bbox):
            raise ValueError("exactly one of countries or bbox must be provided")
        if countries and any(len(value) != 3 or not value.isalpha() for value in countries):
            raise ValueError("countries must contain ISO3 codes")
        if not str(self.output_root).strip():
            raise ValueError("output_root must not be empty")
        if not 1 <= self.workers <= 8:
            raise ValueError("workers must be between 1 and 8")
        if not 60 <= self.download_timeout <= 1800:
            raise ValueError("download_timeout must be between 60 and 1800 seconds")
        if not self.token_env.isidentifier():
            raise ValueError("token_env must be an environment variable name")
        object.__setattr__(self, "countries", countries)
        object.__setattr__(self, "bbox", normalized_bbox)
        targets = sorted({str(value).strip().upper() for value in self.targets if str(value).strip()})
        for target in targets:
            self.validate_target(target)
        object.__setattr__(self, "targets", targets)

    @property
    def target_id(self) -> str:
        if self.countries:
            if len(self.countries) != 1:
                raise ValueError("VNP46A1 country mode currently accepts exactly one ISO3 country per run")
            return self.countries[0]
        assert self.bbox is not None
        encoded = ",".join(f"{value:.6f}" for value in self.bbox).encode("ascii")
        return f"BBOX_{hashlib.sha256(encoded).hexdigest()[:12].upper()}"

    @property
    def output_path(self) -> Path:
        return Path(self.output_root).expanduser().resolve(strict=False)

    @property
    def run_root(self) -> Path:
        return self.output_path / f"VNP46A1_at_sensor_{self.target_id}_{self.start_date}_to_{self.end_date}"

    @property
    def target_mode(self) -> str:
        return "country" if self.countries else "bbox"

    @property
    def phase_list(self) -> list[str]:
        return ["prepare", "download", "mosaic", "audit"] if self.phase == "full" else [self.phase]

    def validate_target(self, target: str) -> tuple[str, str]:
        try:
            target_id, day = target.rsplit(":", 1)
        except ValueError as exc:
            raise ValueError("targets must use TARGET_ID:YYYY-MM-DD") from exc
        if target_id != self.target_id:
            raise ValueError("retry target does not belong to the current request")
        _parse_date(day, "target date")
        return target_id, day


def h5_to_target_tifs(
    h5_path: str | Path,
    radiance_path: str | Path,
    *,
    target_geometry: Any,
    include_utc_time: bool,
) -> tuple[Path, Path | None]:
    """Convert one official VNP46A1 HDF5 file into clipped radiance and UTC rasters."""
    source = Path(h5_path)
    output = Path(radiance_path)
    with h5py.File(source, "r") as handle:
        root_attrs = {name: _as_scalar(value) for name, value in handle.attrs.items()}
        radiance_dataset_path, radiance_dataset = _find_dataset(handle, _RADIANCE_DATASET_CANDIDATES)
        transform = _geographic_transform(root_attrs, radiance_dataset.shape)
        radiance = _scaled_values(radiance_dataset)
        _write_clipped_tif(
            output,
            radiance,
            transform,
            target_geometry,
            tags={
                "source_h5": str(source),
                "source_dataset": radiance_dataset_path,
                "product": "VNP46A1",
                "semantic_role": "at_sensor_dnb_radiance",
            },
        )
        utc_output: Path | None = None
        if include_utc_time:
            utc_dataset_path, utc_dataset = _find_dataset(handle, _UTC_TIME_DATASET)
            utc_output = output.with_name(f"{output.stem}_UTC_Time{output.suffix}")
            _write_clipped_tif(
                utc_output,
                _scaled_values(utc_dataset),
                transform,
                target_geometry,
                tags={
                    "source_h5": str(source),
                    "source_dataset": utc_dataset_path,
                    "product": "VNP46A1",
                    "semantic_role": "acquisition_time_utc_hours",
                },
            )
    return output, utc_output


def run_vnp46a1_download(request: Vnp46a1DownloadRequest, *, progress: DownloadProgress | None = None) -> ToolResult:
    """Plan or run official VNP46A1 Earthdata retrieval and clipped mosaics."""
    return ToolResult.succeeded(
        tool="download_vnp46a1_official_h5",
        summary="Prepared VNP46A1 execution plan.",
        metrics={
            "short_name": "VNP46A1",
            "primary_dataset": _RADIANCE_DATASET,
            "target_mode": request.target_mode,
            "target_id": request.target_id,
            "include_utc_time": request.include_utc_time,
            "phases": request.phase_list,
            "run_root": str(request.run_root),
        },
    ) if request.execution_mode == "plan" else _run_request(request, progress)


def inspect_vnp46a1_run(run_root: str | Path) -> ToolResult:
    root = Path(run_root).expanduser().resolve(strict=False)
    audit_path = root / "vnp46a1_audit.json"
    runtime_path = root / "vnp46a1_runtime.json"
    if runtime_path.exists():
        runtime = json.loads(runtime_path.read_text(encoding="utf-8"))
        if runtime.get("status") in {"running", "failed"}:
            return ToolResult.succeeded(
                tool="download_vnp46a1_official_h5",
                summary=f"VNP46A1 run is {runtime['status']}.",
                metrics={"run_root": str(root), "runtime_path": str(runtime_path), **runtime},
            )
    if not audit_path.exists():
        if runtime_path.exists():
            return ToolResult.succeeded(
                tool="download_vnp46a1_official_h5",
                summary=f"VNP46A1 run is {runtime.get('status', 'running')}.",
                metrics={"run_root": str(root), "runtime_path": str(runtime_path), **runtime},
            )
        return _failed("VNP46A1_AUDIT_NOT_FOUND", "No VNP46A1 audit was found.", "Run the audit phase first.", {"run_root": str(root)})
    payload = json.loads(audit_path.read_text(encoding="utf-8"))
    rows = payload.get("rows", [])
    retry_targets = sorted(f"{row['target_id']}:{row['date']}" for row in rows if row.get("status") in {"retry_download", "not_processed"})
    pending = sorted(f"{row['target_id']}:{row['date']}" for row in rows if row.get("status") == "downloaded_without_mosaic")
    metrics = {"run_root": str(root), "audit_path": str(audit_path), "retry_targets": retry_targets, "pending_mosaic_targets": pending, "status_counts": _status_counts(rows)}
    if retry_targets or pending:
        return _failed("VNP46A1_AUDIT_INCOMPLETE", "The VNP46A1 audit reports unfinished target-days.", "Retry only retry_targets, mosaic pending_mosaic_targets, then audit again.", metrics)
    return ToolResult.succeeded(tool="download_vnp46a1_official_h5", summary="The VNP46A1 audit is complete.", metrics=metrics, outputs=[OutputArtifact(path=str(audit_path), media_type="application/json", role="audit")])


def _run_request(request: Vnp46a1DownloadRequest, progress: DownloadProgress | None) -> ToolResult:
    if "download" in request.phase_list and not os.getenv(request.token_env, "").strip():
        return _failed("EARTHDATA_TOKEN_MISSING", f"{request.token_env} is not configured.", "Set the token in NTL_MCP_ENV_FILE or the process environment, then retry.", {"run_root": str(request.run_root)})
    request.run_root.mkdir(parents=True, exist_ok=True)
    completed: list[str] = []
    _write_runtime(request, status="running", current_phase="prepare", completed_phases=completed)
    try:
        target_geometry = _prepare_geometry(request)
        for index, phase in enumerate(request.phase_list, start=1):
            _write_runtime(request, status="running", current_phase=phase, completed_phases=completed)
            _report(progress, index - 1, len(request.phase_list), phase)
            if phase == "prepare":
                pass
            elif phase == "download":
                _download_days(request, target_geometry)
            elif phase == "mosaic":
                _mosaic_days(request, target_geometry)
            elif phase == "audit":
                _write_audit(request)
            completed.append(phase)
            _write_runtime(request, status="running", current_phase="", completed_phases=completed)
        _report(progress, len(request.phase_list), len(request.phase_list), "completed")
        _write_runtime(request, status="completed", current_phase="", completed_phases=completed)
        return inspect_vnp46a1_run(request.run_root) if "audit" in request.phase_list else ToolResult.succeeded(tool="download_vnp46a1_official_h5", summary=f"Completed VNP46A1 {request.phase} phase.", metrics={"run_root": str(request.run_root)})
    except Exception as exc:  # noqa: BLE001
        _write_runtime(request, status="failed", current_phase="", completed_phases=completed)
        return _failed("VNP46A1_PHASE_FAILED", sanitize_download_text(str(exc) or type(exc).__name__), "Inspect the phase manifests and retry only the affected target-days.", {"run_root": str(request.run_root)})


def _write_runtime(request: Vnp46a1DownloadRequest, *, status: str, current_phase: str, completed_phases: list[str]) -> None:
    write_download_manifest(
        request.run_root / "vnp46a1_runtime.json",
        {
            "status": status,
            "current_phase": current_phase,
            "completed_phases": completed_phases,
            "updated_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "target_id": request.target_id,
        },
    )


def _prepare_geometry(request: Vnp46a1DownloadRequest):
    import geopandas as gpd
    from shapely.geometry import box

    path = request.run_root / "target.geojson"
    if path.exists():
        return gpd.read_file(path).to_crs("EPSG:4326").geometry.union_all()
    if request.bbox:
        geometry = box(*request.bbox)
        gpd.GeoDataFrame({"target_id": [request.target_id]}, geometry=[geometry], crs="EPSG:4326").to_file(path, driver="GeoJSON")
        return geometry
    from tools.vnp46a2_official_h5.vnp46a2_country_common import fetch_osm_boundary, selected_countries, simplified_boundary

    country = selected_countries(request.countries)[0]
    source = fetch_osm_boundary(country, request.run_root / "osm_boundaries")
    boundary = simplified_boundary(source, country, request.run_root / "osm_boundaries_simplified_0p001")
    boundary.to_file(path, driver="GeoJSON")
    return boundary.geometry.union_all()


def _download_days(request: Vnp46a1DownloadRequest, geometry: Any) -> None:
    repo_root = Path(__file__).resolve().parents[5]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from experiments.official_daily_ntl_fastpath.cmr_client import download_file_with_curl, extract_download_link, resolve_token, search_granules

    token = resolve_token(request.token_env)
    rows: list[dict[str, Any]] = []
    for day in _days(request.start_date, request.end_date):
        if request.targets and f"{request.target_id}:{day}" not in request.targets:
            continue
        entries = [entry for entry in search_granules("VNP46A1", day, day, geometry.bounds, page_size=200) if _is_day_entry(entry.producer_granule_id, day)]
        day_dir = request.run_root / "official_raw_h5" / request.target_id / day
        files: list[str] = []
        failures: list[str] = []
        for index, entry in enumerate(entries, start=1):
            link = extract_download_link(entry.links)
            if not link:
                failures.append("missing_download_link")
                continue
            destination = day_dir / (Path(link.split("?", 1)[0]).name or f"VNP46A1_{index}.h5")
            ok, detail = download_file_with_curl(link, destination, earthdata_token=token, timeout=request.download_timeout)
            if ok:
                files.append(str(destination))
            else:
                failures.append(sanitize_download_text(detail))
        rows.append({"target_id": request.target_id, "date": day, "status": "official_h5_downloaded" if files and not failures else "official_h5_partial" if files else "no_granules" if not entries else "retry_download", "files": files, "failures": failures})
    write_download_manifest(request.run_root / "vnp46a1_download_manifest.json", {"product": "VNP46A1", "rows": rows})


def _mosaic_days(request: Vnp46a1DownloadRequest, geometry: Any) -> None:
    manifest = json.loads((request.run_root / "vnp46a1_download_manifest.json").read_text(encoding="utf-8"))
    rows: list[dict[str, Any]] = []
    for row in manifest.get("rows", []):
        day = row["date"]
        paths = [Path(item) for item in row.get("files", []) if Path(item).exists()]
        result: dict[str, Any] = {"target_id": request.target_id, "date": day, "status": "missing_h5"}
        if paths:
            tile_dir = request.run_root / "tile_tifs" / request.target_id / day
            radiance_tiles: list[Path] = []
            utc_tiles: list[Path] = []
            for index, path in enumerate(paths, start=1):
                radiance, utc = h5_to_target_tifs(path, tile_dir / f"tile_{index}.tif", target_geometry=geometry, include_utc_time=request.include_utc_time)
                radiance_tiles.append(radiance)
                if utc is not None:
                    utc_tiles.append(utc)
            output = request.run_root / "official_h5_mosaics" / request.target_id / f"VNP46A1_DNB_At_Sensor_Radiance_500m_{request.target_id}_{day}.tif"
            _merge_tiles(radiance_tiles, output, {"product": "VNP46A1", "target_id": request.target_id, "semantic_role": "at_sensor_dnb_radiance"})
            if utc_tiles:
                _merge_tiles(utc_tiles, output.with_name(f"{output.stem}_UTC_Time.tif"), {"product": "VNP46A1", "target_id": request.target_id, "semantic_role": "acquisition_time_utc_hours"})
            result = {"target_id": request.target_id, "date": day, "status": "mosaic_valid", "output_file": str(output)}
        rows.append(result)
    write_download_manifest(request.run_root / "vnp46a1_mosaic_manifest.json", {"product": "VNP46A1", "rows": rows})


def _write_audit(request: Vnp46a1DownloadRequest) -> None:
    downloads = json.loads((request.run_root / "vnp46a1_download_manifest.json").read_text(encoding="utf-8")).get("rows", [])
    mosaics = {row["date"]: row for row in json.loads((request.run_root / "vnp46a1_mosaic_manifest.json").read_text(encoding="utf-8")).get("rows", [])}
    rows = []
    for row in downloads:
        mosaic = mosaics.get(row["date"], {})
        status = mosaic.get("status") if row.get("files") else row.get("status")
        if row.get("status") in {"official_h5_partial", "retry_download"}:
            status = "retry_download"
        elif row.get("files") and status != "mosaic_valid":
            status = "downloaded_without_mosaic"
        rows.append({"target_id": request.target_id, "date": row["date"], "status": status or "not_processed", "output_file": mosaic.get("output_file", "")})
    write_download_manifest(request.run_root / "vnp46a1_audit.json", {"product": "VNP46A1", "rows": rows})


def _merge_tiles(paths: list[Path], output: Path, tags: dict[str, str]) -> None:
    sources = [rasterio.open(path) for path in paths]
    try:
        data, transform = merge(sources, nodata=NODATA)
        profile = sources[0].profile.copy()
        profile.update(height=data.shape[1], width=data.shape[2], transform=transform, compress="deflate")
        output.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(output, "w", **profile) as dataset:
            dataset.write(data)
            dataset.update_tags(**tags)
    finally:
        for source in sources:
            source.close()


def _days(start: str, end: str) -> list[str]:
    current = datetime.strptime(start, _DATE_FORMAT).date()
    finish = datetime.strptime(end, _DATE_FORMAT).date()
    out = []
    while current <= finish:
        out.append(current.isoformat())
        current += timedelta(days=1)
    return out


def _is_day_entry(granule_id: str, day: str) -> bool:
    return f".A{datetime.strptime(day, _DATE_FORMAT).strftime('%Y%j')}." in granule_id


def _status_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        status = str(row.get("status") or "")
        counts[status] = counts.get(status, 0) + 1
    return counts


def _report(progress: DownloadProgress | None, current: float, total: float, message: str) -> None:
    if progress is not None:
        progress(float(current), float(total), message)


def _failed(code: str, message: str, suggestion: str, metrics: dict[str, Any]) -> ToolResult:
    return ToolResult.failed(tool="download_vnp46a1_official_h5", error=ToolError(code=code, message=message, suggestion=suggestion, details=metrics))


def _parse_date(value: str, name: str) -> None:
    try:
        datetime.strptime(value, _DATE_FORMAT)
    except ValueError as exc:
        raise ValueError(f"{name} must use YYYY-MM-DD") from exc


def _normalize_bbox(value: list[float] | tuple[float, float, float, float] | None) -> tuple[float, float, float, float]:
    if value is None or len(value) != 4:
        raise ValueError("bbox must contain minx, miny, maxx, maxy")
    minx, miny, maxx, maxy = (float(item) for item in value)
    if not -180 <= minx < maxx <= 180 or not -90 <= miny < maxy <= 90:
        raise ValueError("bbox must be a valid WGS84 extent")
    return minx, miny, maxx, maxy


def _find_dataset(handle: h5py.File, dataset_names: str | tuple[str, ...]) -> tuple[str, h5py.Dataset]:
    candidates = (dataset_names,) if isinstance(dataset_names, str) else dataset_names
    matches: list[str] = []

    def collect(name: str, value: Any) -> None:
        if isinstance(value, h5py.Dataset) and any(name.replace("-", "_").endswith(candidate) for candidate in candidates):
            matches.append(name)

    handle.visititems(collect)
    if not matches:
        raise KeyError(f"{' or '.join(candidates)} not found in {handle.filename}")
    selected = matches[0]
    return selected, handle[selected]


def _as_scalar(value: Any) -> Any:
    if isinstance(value, np.ndarray):
        return value.ravel()[0].item() if value.size else None
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _geographic_transform(attrs: dict[str, Any], shape: tuple[int, ...]):
    try:
        west = float(attrs["WestBoundingCoord"])
        east = float(attrs["EastBoundingCoord"])
        north = float(attrs["NorthBoundingCoord"])
        south = float(attrs["SouthBoundingCoord"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("VNP46A1 HDF5 lacks geographic tile bounding metadata") from exc
    height, width = shape[-2:]
    if east <= west or north <= south:
        raise ValueError("VNP46A1 HDF5 has invalid geographic tile bounds")
    return from_origin(west, north, (east - west) / width, (north - south) / height)


def _scaled_values(dataset: h5py.Dataset) -> np.ndarray:
    values = dataset[()].astype("float32")
    attrs = {name: _as_scalar(value) for name, value in dataset.attrs.items()}
    fill = float(attrs.get("_FillValue", attrs.get("FillValue", NODATA)))
    scale = float(attrs.get("scale_factor", attrs.get("ScaleFactor", 1.0)))
    offset = float(attrs.get("add_offset", attrs.get("Offset", 0.0)))
    values[values == fill] = np.nan
    values = values * scale + offset
    values[~np.isfinite(values)] = NODATA
    return values


def _write_clipped_tif(path: Path, values: np.ndarray, transform, target_geometry: Any, *, tags: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"_{path.name}")
    profile = {
        "driver": "GTiff",
        "height": values.shape[0],
        "width": values.shape[1],
        "count": 1,
        "dtype": "float32",
        "crs": "EPSG:4326",
        "transform": transform,
        "nodata": NODATA,
    }
    try:
        with rasterio.open(temporary, "w", **profile) as dataset:
            dataset.write(values, 1)
        with rasterio.open(temporary) as dataset:
            clipped, clipped_transform = mask(dataset, [mapping(target_geometry)], crop=True, nodata=NODATA, filled=True)
            clipped_profile = dataset.profile.copy()
        clipped_profile.update(height=clipped.shape[1], width=clipped.shape[2], transform=clipped_transform, compress="deflate")
        with rasterio.open(path, "w", **clipped_profile) as dataset:
            dataset.write(clipped)
            dataset.update_tags(**tags)
    finally:
        temporary.unlink(missing_ok=True)
