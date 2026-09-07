"""Shared artifact and path-contract helpers for official VIIRS HDF5 routes.

Official granule filenames are intentionally descriptive and can be long.  A
benchmark workspace, however, can already consume most of the Windows legacy
path budget.  This module keeps operational directories compact while moving
the human-readable request semantics into a manifest.  It deliberately does
not make a failed provider request look successful.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Iterable

from .downloads import sanitize_download_text


# Keep a margin for GDAL/Rasterio temporary siblings and sidecar files rather
# than relying on the nominal 260-character legacy Windows limit.
WINDOWS_SAFE_PATH_LIMIT = 235


def compact_run_id(product: str, scope: str, start_date: str, end_date: str) -> str:
    """Return a stable, short run identifier without losing request semantics.

    The full product, target scope and date interval must be persisted in the
    companion manifest.  The identifier is a namespace component only; it is
    not a replacement for provenance.
    """

    normalized = "|".join(
        (
            str(product or "").strip().upper(),
            str(scope or "").strip().upper(),
            str(start_date or "").strip(),
            str(end_date or "").strip(),
        )
    )
    prefix = "a1" if normalized.startswith("VNP46A1|") else "a2"
    return f"{prefix}-{hashlib.sha256(normalized.encode('utf-8')).hexdigest()[:10]}"


def path_budget_diagnostic(run_root: Path, longest_relative_path: str) -> dict[str, Any] | None:
    """Describe an unsafe Windows target path, or return ``None`` when safe."""

    probe = run_root / Path(longest_relative_path)
    length = len(str(probe))
    if os.name == "nt" and length >= WINDOWS_SAFE_PATH_LIMIT:
        return {
            "code": "OFFICIAL_H5_PATH_TOO_LONG",
            "message": "The requested official-HDF5 workspace path exceeds the safe Windows path budget.",
            "suggestion": "Use a short workspace-relative output_root such as 'h5a1' or 'h5a2', then retry the same request.",
            "details": {
                "run_root": str(run_root),
                "longest_expected_path": str(probe),
                "path_length": length,
                "safe_limit": WINDOWS_SAFE_PATH_LIMIT,
            },
        }
    return None


def workspace_relative_path(path: Path, workspace: Path) -> str | None:
    """Return a normalized path rooted at a thread workspace, if safe."""

    try:
        return path.resolve(strict=False).relative_to(workspace.resolve(strict=False)).as_posix()
    except (OSError, ValueError):
        return None


def build_artifact_handoff(
    *,
    workspace: Path,
    run_root: Path,
    tool: str,
    product: str,
    request: dict[str, Any],
    status: str,
    runtime_manifest_names: Iterable[str],
    artifact_patterns: Iterable[str],
) -> dict[str, Any]:
    """Persist and return a bounded, workspace-relative artifact handoff.

    Only existing regular files below ``run_root`` are declared.  A missing
    runtime manifest is exposed explicitly so callers can report a diagnosable
    failed acquisition rather than allowing a later collector to raise an
    unrelated ``FileNotFoundError``.
    """

    root = run_root.resolve(strict=False)
    workspace_root = workspace.resolve(strict=False)
    root_relative = workspace_relative_path(root, workspace_root)
    base: dict[str, Any] = {
        "schema": "ntl.official-h5.artifact-handoff.v1",
        "tool": tool,
        "product": product,
        "status": status,
        "request": _json_safe(request),
        "workspace_relative_run_root": root_relative,
        "runtime_manifest": None,
        "artifacts": [],
        "missing_required_artifacts": [],
        "artifact_manifest": None,
    }
    if root_relative is None or not root.exists() or not root.is_dir():
        base["status"] = "run_root_missing"
        base["missing_required_artifacts"] = ["run_root"]
        return base

    runtime_names = tuple(str(name) for name in runtime_manifest_names)
    runtime_path = next((root / name for name in runtime_names if (root / name).is_file()), None)
    if runtime_path is None:
        base["missing_required_artifacts"] = ["runtime_manifest"]
    else:
        base["runtime_manifest"] = workspace_relative_path(runtime_path, workspace_root)

    selected: list[Path] = []
    seen: set[Path] = set()
    for pattern in artifact_patterns:
        for path in sorted(root.glob(pattern)):
            if path in seen or not path.is_file() or path.is_symlink():
                continue
            relative = workspace_relative_path(path, workspace_root)
            if relative is None:
                continue
            seen.add(path)
            selected.append(path)
    for path in selected:
        relative = workspace_relative_path(path, workspace_root)
        if relative is None:
            continue
        base["artifacts"].append(
            {
                "path": relative,
                "media_type": _media_type(path),
                "role": _artifact_role(path),
            }
        )

    if base["missing_required_artifacts"]:
        base["status"] = "runtime_manifest_missing"

    manifest_path = root / "official_h5_artifact_handoff.json"
    # Do not include this file in itself; it is declared separately below.
    manifest_path.write_text(
        json.dumps(_json_safe(base), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    manifest_relative = workspace_relative_path(manifest_path, workspace_root)
    base["artifact_manifest"] = manifest_relative
    if manifest_relative is not None:
        base["artifacts"].append(
            {
                "path": manifest_relative,
                "media_type": "application/json",
                "role": "artifact_handoff_manifest",
            }
        )
    return base


def _artifact_role(path: Path) -> str:
    name = path.name.lower()
    if name.endswith(".tif") or name.endswith(".tiff"):
        return "analysis_ready_raster"
    if "audit" in name:
        return "audit"
    if "manifest" in name or "summary" in name or name.endswith(".json"):
        return "manifest"
    return "artifact"


def _media_type(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".json":
        return "application/json"
    if suffix == ".csv":
        return "text/csv"
    if suffix in {".tif", ".tiff"}:
        return "image/tiff"
    if suffix == ".txt":
        return "text/plain"
    if suffix in {".geojson", ".jsonl"}:
        return "application/geo+json" if suffix == ".geojson" else "application/jsonl"
    return "application/octet-stream"


def _json_safe(value: Any) -> Any:
    """Keep saved handoffs serializable and redact accidental bearer values."""

    if isinstance(value, str):
        return sanitize_download_text(value)
    if isinstance(value, Path):
        return sanitize_download_text(str(value))
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return value

