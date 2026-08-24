from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import h5py

from vnp46a2_country_common import (  # noqa: E402
    BAND,
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_START,
    run_dir,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate official VNP46A2 H5 files and optionally delete invalid ones.")
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--delete-invalid", action="store_true")
    return parser.parse_args()


def validate_h5(path: Path) -> tuple[bool, str]:
    try:
        with h5py.File(path, "r") as h5:
            hits: list[str] = []

            def walk(name, obj):
                normalized = name.replace("-", "_")
                if isinstance(obj, h5py.Dataset) and normalized.endswith(BAND) and "Gap_Filled" not in normalized:
                    hits.append(name)

            h5.visititems(walk)
            if not hits:
                return False, f"{BAND} dataset missing"
            ds = h5[hits[0]]
            _ = ds.shape
    except Exception as exc:  # noqa: BLE001
        return False, repr(exc)
    return True, ""


def write_manifest(path: Path, rows: list[dict[str, object]]) -> None:
    fields = ["path", "status", "note"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    base_dir = run_dir(Path(args.output_root), args.start, args.end)
    h5_root = base_dir / "official_raw_h5"
    rows: list[dict[str, object]] = []
    invalid: list[Path] = []
    for path in sorted(h5_root.rglob("*.h5")):
        ok, note = validate_h5(path)
        if ok:
            rows.append({"path": str(path), "status": "valid", "note": ""})
        else:
            rows.append({"path": str(path), "status": "invalid_deleted" if args.delete_invalid else "invalid", "note": note[:500]})
            invalid.append(path)
            if args.delete_invalid:
                path.unlink(missing_ok=True)

    manifest = base_dir / "vnp46a2_official_h5_validation_manifest.csv"
    write_manifest(manifest, rows)
    summary = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "h5_files_checked": len(rows),
        "invalid_count": len(invalid),
        "delete_invalid": bool(args.delete_invalid),
        "manifest": str(manifest),
    }
    summary_path = base_dir / "vnp46a2_official_h5_validation_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if not invalid else 2


if __name__ == "__main__":
    raise SystemExit(main())
