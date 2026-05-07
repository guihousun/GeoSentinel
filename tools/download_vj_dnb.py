"""
Download VIIRS files from a LAADS-style query JSON.

Expected input JSON format:
{
  "query": "...",
  "123456": {"url": "https://.../VJ102DNB....nc", "size": 123, "source": "VJ102DNB"},
  "123457": {"url": "https://.../VJ146A1....h5", "size": 456, "source": "VJ146A1"}
}
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Tuple

from dotenv import load_dotenv


def _load_env() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    load_dotenv(repo_root / ".env", override=False)
    load_dotenv(override=False)


def _infer_source_from_url(url: str) -> str:
    name = url.split("/")[-1]
    m = re.match(r"^([A-Za-z0-9_]+)\.", name)
    if m:
        return m.group(1).upper()
    m2 = re.match(r"^([A-Za-z0-9_]+)[._-]", name)
    if m2:
        return m2.group(1).upper()
    return "UNKNOWN"


def _extract_urls_from_json(json_file: Path) -> Dict[str, List[str]]:
    with json_file.open("r", encoding="utf-8") as f:
        data = json.load(f)

    urls: Dict[str, List[str]] = {}
    for key, value in data.items():
        if key == "query":
            continue
        if not isinstance(value, dict):
            continue
        url = str(value.get("url", "")).strip()
        if not url:
            continue
        src = str(value.get("source", "")).strip().upper() or _infer_source_from_url(url)
        urls.setdefault(src, []).append(url)

    # Preserve order while de-duplicating.
    for k in list(urls.keys()):
        seen = set()
        deduped = []
        for u in urls[k]:
            if u in seen:
                continue
            seen.add(u)
            deduped.append(u)
        urls[k] = deduped
    return urls


def _parse_source_filter(raw: str) -> set[str]:
    out: set[str] = set()
    for part in (raw or "").split(","):
        s = part.strip().upper()
        if s:
            out.add(s)
    return out


def _check_curl_available() -> str:
    for cmd in ("curl.exe", "curl"):
        try:
            p = subprocess.run([cmd, "--version"], capture_output=True, text=True)
            if p.returncode == 0:
                return cmd
        except Exception:
            continue
    return ""


def _is_valid_download(path: Path) -> Tuple[bool, str]:
    if not path.exists() or path.stat().st_size <= 0:
        return False, "empty file"
    head = path.read_bytes()[:64]
    # HDF5 signature or NetCDF classic signatures.
    if head.startswith(b"\x89HDF"):
        return True, ""
    if head.startswith(b"CDF\x01") or head.startswith(b"CDF\x02"):
        return True, ""
    # HTML/error response.
    head_lower = head.lower()
    if b"<html" in head_lower or b"doctype html" in head_lower:
        return False, "received HTML page instead of data file"
    return False, "file signature is not HDF5/netCDF"


def _download_with_curl(
    curl_bin: str,
    url: str,
    output_path: Path,
    earthdata_token: str,
    timeout: int = 1200,
) -> Tuple[bool, str]:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        curl_bin,
        "--silent",
        "--show-error",
        "--location",
        "--fail-with-body",
        "--ipv4",
        "--retry",
        "4",
        "--retry-delay",
        "3",
        "--retry-all-errors",
        "--connect-timeout",
        "30",
        "--max-time",
        str(max(120, int(timeout))),
        "--output",
        str(output_path),
        url,
    ]

    # Windows + Schannel often fails revocation checks in some networks.
    if platform.system().lower().startswith("win"):
        cmd.append("--ssl-no-revoke")

    if earthdata_token:
        cmd.extend(["--header", f"Authorization: Bearer {earthdata_token}"])

    try:
        p = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            timeout=timeout + 90,
        )
        if p.returncode != 0:
            if output_path.exists():
                output_path.unlink()
            err = (p.stderr or p.stdout or "").strip()
            return False, f"curl rc={p.returncode}: {err}"

        ok, reason = _is_valid_download(output_path)
        if not ok:
            if output_path.exists():
                output_path.unlink()
            return False, reason
        return True, ""
    except subprocess.TimeoutExpired:
        if output_path.exists():
            output_path.unlink()
        return False, f"timeout after {timeout}s"
    except Exception as exc:
        if output_path.exists():
            output_path.unlink()
        return False, str(exc)


def _download_group(
    curl_bin: str,
    urls: List[str],
    output_dir: Path,
    token: str,
    group_name: str,
) -> Dict[str, int]:
    downloaded = 0
    skipped = 0
    failed = 0

    print(f"\n[{group_name}] total urls: {len(urls)}")
    print("-" * 80)

    for idx, url in enumerate(urls, start=1):
        filename = url.split("/")[-1]
        out = output_dir / filename

        if out.exists() and out.stat().st_size > 1_048_576:
            print(f"[{idx}/{len(urls)}] skip existing: {filename}")
            skipped += 1
            continue

        if out.exists():
            out.unlink()

        print(f"[{idx}/{len(urls)}] downloading: {filename}")
        ok, err = _download_with_curl(
            curl_bin=curl_bin,
            url=url,
            output_path=out,
            earthdata_token=token,
        )
        if ok:
            size_mb = out.stat().st_size / 1024 / 1024
            print(f"  success: {size_mb:.2f} MB")
            downloaded += 1
        else:
            print(f"  failed: {err}")
            failed += 1

    print(
        f"[{group_name}] done: downloaded={downloaded}, skipped={skipped}, failed={failed}"
    )
    return {"downloaded": downloaded, "skipped": skipped, "failed": failed}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download VIIRS files from LAADS query JSON."
    )
    parser.add_argument("--input", type=Path, required=True, help="Query JSON path")
    parser.add_argument("--output", type=Path, required=True, help="Output folder")
    parser.add_argument(
        "--token-env",
        type=str,
        default="EARTHDATA_TOKEN",
        help="Environment variable name for Earthdata token",
    )
    parser.add_argument(
        "--sources",
        type=str,
        default="",
        help="Optional comma list of sources to download, e.g. VJ146A1,VJ102DNB",
    )
    args = parser.parse_args()

    _load_env()

    if not args.input.exists():
        print(f"error: input json not found: {args.input}")
        sys.exit(1)

    args.output.mkdir(parents=True, exist_ok=True)

    curl_bin = _check_curl_available()
    if not curl_bin:
        print("error: curl not found in PATH")
        sys.exit(1)

    token = os.getenv(args.token_env, "").strip()
    if not token:
        print(f"warning: {args.token_env} is empty; authenticated download may fail")

    urls = _extract_urls_from_json(args.input)
    source_filter = _parse_source_filter(args.sources)
    if source_filter:
        urls = {k: v for k, v in urls.items() if k in source_filter}

    print("VIIRS DNB downloader")
    print("=" * 80)
    print(f"input : {args.input}")
    print(f"output: {args.output}")
    print(f"curl  : {curl_bin}")
    print(f"token : {'configured' if token else 'missing'}")
    if not urls:
        print("No matching URLs found in input JSON (after source filter).")
        sys.exit(1)
    for src in sorted(urls.keys()):
        print(f"{src} urls: {len(urls[src])}")

    per_source_stats: Dict[str, Dict[str, int]] = {}
    total_downloaded = 0
    total_failed = 0
    total_skipped = 0
    for src in sorted(urls.keys()):
        stats = _download_group(curl_bin, urls[src], args.output, token, src)
        per_source_stats[src] = stats
        total_downloaded += stats["downloaded"]
        total_failed += stats["failed"]
        total_skipped += stats["skipped"]

    manifest = {
        "input_json": str(args.input),
        "output_dir": str(args.output),
        "token_env": args.token_env,
        "source_filter": sorted(source_filter),
        "per_source_stats": per_source_stats,
        "total_downloaded": total_downloaded,
        "total_skipped": total_skipped,
        "total_failed": total_failed,
    }
    manifest_path = args.output / "download_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print("\nSummary")
    print("=" * 80)
    for src in sorted(per_source_stats.keys()):
        print(f"{src}: {per_source_stats[src]}")
    print(f"manifest: {manifest_path}")

    if total_failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
