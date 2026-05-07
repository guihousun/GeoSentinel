from __future__ import annotations

import json
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from .env_utils import get_env_or_dotenv


CMR_GRANULES_API = "https://cmr.earthdata.nasa.gov/search/granules.json"
HDF5_SIGNATURE = b"\x89HDF\r\n\x1a\n"


@dataclass(frozen=True)
class GranuleRecord:
    producer_granule_id: str
    time_start: str
    updated: str | None
    day_night_flag: str | None
    links: list[dict[str, Any]]
    polygons: list[str]
    raw: dict[str, Any]

    @property
    def day(self) -> str:
        return self.time_start[:10]


def _require_curl() -> str:
    for candidate in ("curl.exe", "curl"):
        if _which(candidate):
            return candidate
    raise RuntimeError("curl executable is required but was not found in PATH.")


def _which(name: str) -> str | None:
    from shutil import which

    return which(name)


def _run_curl_json(url: str, headers: list[str] | None = None, timeout: int = 120) -> dict[str, Any]:
    curl_bin = _require_curl()
    cmd = [curl_bin, "--silent", "--show-error", "--location", url]
    for h in headers or []:
        cmd.extend(["--header", h])
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=max(int(timeout) + 45, 120),
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"curl failed ({proc.returncode}): {proc.stderr.strip()}")
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed to parse JSON from CMR response: {exc}") from exc


def build_cmr_query_url(
    short_name: str,
    start_date: str,
    end_date: str,
    bbox: tuple[float, float, float, float] | None = None,
    page_size: int = 200,
    descending: bool = True,
) -> str:
    params: list[tuple[str, str]] = [
        ("short_name", short_name),
        ("temporal", f"{start_date}T00:00:00Z,{end_date}T23:59:59Z"),
        ("page_size", str(page_size)),
    ]
    if bbox is not None:
        minx, miny, maxx, maxy = bbox
        params.append(("bounding_box", f"{minx},{miny},{maxx},{maxy}"))
    if descending:
        params.append(("sort_key[]", "-start_date"))
    return f"{CMR_GRANULES_API}?{urlencode(params)}"


def parse_granules_payload(payload: dict[str, Any]) -> list[GranuleRecord]:
    feed = payload.get("feed", {})
    entries = feed.get("entry", [])
    out: list[GranuleRecord] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        time_start = str(entry.get("time_start") or "")
        pid = str(entry.get("producer_granule_id") or "")
        if not time_start or not pid:
            continue
        out.append(
            GranuleRecord(
                producer_granule_id=pid,
                time_start=time_start,
                updated=entry.get("updated"),
                day_night_flag=entry.get("day_night_flag"),
                links=entry.get("links") or [],
                polygons=entry.get("polygons") or [],
                raw=entry,
            )
        )
    return out


def search_granules(
    short_name: str,
    start_date: str,
    end_date: str,
    bbox: tuple[float, float, float, float] | None,
    page_size: int = 200,
) -> list[GranuleRecord]:
    url = build_cmr_query_url(
        short_name=short_name,
        start_date=start_date,
        end_date=end_date,
        bbox=bbox,
        page_size=page_size,
        descending=True,
    )
    payload = _run_curl_json(url)
    return parse_granules_payload(payload)


def group_granules_by_day(granules: list[GranuleRecord], night_only: bool = False) -> dict[str, list[GranuleRecord]]:
    groups: dict[str, list[GranuleRecord]] = {}
    for granule in granules:
        if night_only and str(granule.day_night_flag or "").upper() != "NIGHT":
            continue
        groups.setdefault(granule.day, []).append(granule)
    return groups


def select_latest_day_entries(
    granules: list[GranuleRecord], night_only: bool = False
) -> tuple[str | None, list[GranuleRecord]]:
    groups = group_granules_by_day(granules, night_only=night_only)
    if not groups:
        return None, []
    latest_day = sorted(groups.keys())[-1]
    return latest_day, groups[latest_day]


def extract_download_link(links: list[dict[str, Any]]) -> str | None:
    data_links: list[str] = []
    fallback_links: list[str] = []
    for item in links:
        if not isinstance(item, dict):
            continue
        href = str(item.get("href") or "").strip()
        if not href:
            continue
        rel = str(item.get("rel") or "")
        if "data#" in rel and href.startswith("https://"):
            data_links.append(href)
        elif href.startswith("https://"):
            fallback_links.append(href)
    if data_links:
        return data_links[0]
    return fallback_links[0] if fallback_links else None


def download_file_with_curl(
    url: str,
    output_path: Path,
    earthdata_token: str | None,
    timeout: int = 600,
    show_progress: bool = False,
) -> tuple[bool, str]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    curl_bin = _require_curl()

    cmd = _build_download_curl_cmd(
        curl_bin=curl_bin,
        url=url,
        output_path=output_path,
        earthdata_token=earthdata_token,
        timeout=timeout,
        show_progress=show_progress,
    )
    if show_progress:
        proc = subprocess.run(cmd, timeout=timeout, check=False)
    else:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=False)
    if proc.returncode != 0:
        # In unstable networks curl may report a transport error after bytes were already written.
        # If payload is structurally valid, accept it and continue.
        if output_path.exists() and output_path.stat().st_size > 0:
            valid, reason = validate_download_payload(output_path)
            if valid:
                return True, "curl_nonzero_but_payload_valid"

        body_hint = _read_small_text(output_path)
        stderr_text = _normalize_error_text(proc.stderr) if hasattr(proc, "stderr") else ""
        detail = stderr_text or f"curl exited with code {proc.returncode}"
        if body_hint:
            detail = f"{detail}; body_hint={body_hint}"
        output_path.unlink(missing_ok=True)
        return False, detail
    if not output_path.exists() or output_path.stat().st_size <= 0:
        return False, "Downloaded file is missing or empty."
    valid, reason = validate_download_payload(output_path)
    if not valid:
        output_path.unlink(missing_ok=True)
        return False, reason
    return True, ""


def _build_download_curl_cmd(
    *,
    curl_bin: str,
    url: str,
    output_path: Path,
    earthdata_token: str | None,
    timeout: int,
    show_progress: bool,
) -> list[str]:
    cmd = [
        curl_bin,
        "--show-error",
        "--location",
        "--fail-with-body",
        "--ipv4",
        "--retry",
        "3",
        "--retry-delay",
        "2",
        "--retry-all-errors",
        "--connect-timeout",
        "30",
        "--max-time",
        str(max(60, int(timeout))),
    ]
    if show_progress:
        cmd.append("--progress-bar")
    else:
        cmd.append("--silent")
    if earthdata_token:
        cmd.extend(["--header", f"Authorization: Bearer {earthdata_token}"])
    cmd.extend([url, "--output", str(output_path)])
    return cmd


def latest_granule_day(granules: list[GranuleRecord], night_only: bool = False) -> str | None:
    day, _entries = select_latest_day_entries(granules, night_only=night_only)
    return day


def parse_iso_day(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d")
    except ValueError:
        return None


def resolve_token(env_name: str) -> str | None:
    fallback = ("EARTHDATA_BEARER_TOKEN", "EDL_TOKEN") if env_name == "EARTHDATA_TOKEN" else ()
    return get_env_or_dotenv(env_name, fallback_names=fallback)


def validate_download_payload(path: Path) -> tuple[bool, str]:
    try:
        head = path.read_bytes()[:64]
    except OSError as exc:
        return False, f"Failed to inspect downloaded file: {exc}"

    if len(head) < 4:
        return False, "Downloaded file is too small to be a valid granule."

    is_hdf5 = head.startswith(HDF5_SIGNATURE)
    is_netcdf_classic = head.startswith(b"CDF\x01") or head.startswith(b"CDF\x02")
    if is_hdf5 or is_netcdf_classic:
        return True, ""

    hint = _read_small_text(path)
    if "access denied" in hint.lower():
        return False, f"Downloaded payload is not HDF/netCDF (access denied): {hint}"
    return False, f"Downloaded payload is not HDF/netCDF: {hint or 'unknown header'}"


def _read_small_text(path: Path) -> str:
    if not path.exists():
        return ""
    try:
        raw = path.read_bytes()[:200]
    except OSError:
        return ""
    if not raw:
        return ""
    if raw.startswith(HDF5_SIGNATURE):
        return "binary_hdf5_signature_detected"

    printable = sum(1 for b in raw if 32 <= b < 127 or b in (9, 10, 13))
    if printable / len(raw) < 0.6:
        return f"binary_payload_head_hex={raw[:16].hex()}"

    text = raw.decode("utf-8", errors="replace")
    text = _normalize_error_text(text)
    return text[:160]


def _normalize_error_text(text: str | None) -> str:
    if not text:
        return ""
    cleaned = "".join(ch if ch.isprintable() else " " for ch in text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:300]
