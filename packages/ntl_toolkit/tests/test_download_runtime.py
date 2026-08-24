from __future__ import annotations

import json
from pathlib import Path

import pytest

from ntl_toolkit.runtime.downloads import (
    read_download_manifest,
    resolve_download_output,
    sanitize_download_text,
    write_download_manifest,
)


def test_manifest_redacts_bearer_and_keeps_progress(tmp_path: Path) -> None:
    manifest = write_download_manifest(
        tmp_path / "run.json",
        {
            "phase": "download",
            "note": "Authorization: Bearer abc.def.ghi",
            "nested": ["Bearer another-secret"],
            "completed": 2,
        },
    )

    payload = read_download_manifest(manifest)

    assert payload["phase"] == "download"
    assert payload["note"] == "Authorization: Bearer <REDACTED>"
    assert payload["nested"] == ["Bearer <REDACTED>"]
    assert payload["completed"] == 2
    assert "abc.def.ghi" not in manifest.read_text(encoding="utf-8")


def test_download_output_reserves_existing_path(runtime_workspace: Path) -> None:
    first = resolve_download_output("outputs/export.tif", runtime_workspace)
    first.parent.mkdir(parents=True, exist_ok=True)
    first.write_bytes(b"existing")

    second = resolve_download_output("outputs/export.tif", runtime_workspace)

    assert first.name == "export.tif"
    assert second.name == "export_001.tif"
    assert second.parent == first.parent


def test_manifest_read_rejects_invalid_json(tmp_path: Path) -> None:
    manifest = tmp_path / "broken.json"
    manifest.write_text("{not-json", encoding="utf-8")

    with pytest.raises(ValueError, match="invalid JSON"):
        read_download_manifest(manifest)


def test_sanitize_download_text_preserves_ordinary_text() -> None:
    assert sanitize_download_text("downloaded 3 files") == "downloaded 3 files"
    assert sanitize_download_text("Bearer xyz") == "Bearer <REDACTED>"


def test_manifest_is_json_object(tmp_path: Path) -> None:
    manifest = write_download_manifest(tmp_path / "run.json", {"phase": "audit"})

    assert json.loads(manifest.read_text(encoding="utf-8")) == {"phase": "audit"}
