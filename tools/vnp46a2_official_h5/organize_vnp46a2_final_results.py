#!/usr/bin/env python3
"""Organize audited VNP46A2 country-day mosaics into a clean final package."""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
from collections import Counter, defaultdict
from pathlib import Path


INDEX_FIELDS = [
    "iso3",
    "date",
    "audit_status",
    "has_geotiff",
    "relative_geotiff",
    "valid_pixel_probe",
    "h5_count",
    "valid_h5_count",
]
RASTER_STATUSES = {"mosaic_valid", "mosaic_all_nodata"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument(
        "--copy",
        action="store_true",
        help="Copy rasters instead of using same-volume NTFS hard links.",
    )
    return parser.parse_args()


def link_or_copy(source: Path, destination: Path, copy_mode: bool) -> str:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if destination.stat().st_size != source.stat().st_size:
            raise RuntimeError(f"Existing destination has wrong size: {destination}")
        return "existing"

    if copy_mode:
        shutil.copy2(source, destination)
        return "copied"

    try:
        os.link(source, destination)
        return "hardlinked"
    except OSError:
        shutil.copy2(source, destination)
        return "copied_fallback"


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=INDEX_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    source_root = args.source_root.resolve()
    output_root = args.output_root.resolve()
    audit_path = source_root / "vnp46a2_country_day_coverage_audit.csv"
    if not audit_path.is_file():
        raise FileNotFoundError(audit_path)

    output_root.mkdir(parents=True, exist_ok=True)
    with audit_path.open("r", encoding="utf-8-sig", newline="") as handle:
        audit_rows = list(csv.DictReader(handle))

    final_rows: list[dict[str, str]] = []
    country_rows: dict[str, list[dict[str, str]]] = defaultdict(list)
    status_counts: Counter[str] = Counter()
    transfer_counts: Counter[str] = Counter()
    missing_declared_mosaics: list[str] = []
    expected_tifs: set[Path] = set()

    for row in audit_rows:
        iso3 = row["iso3"]
        date = row["date"]
        status = row["audit_status"]
        status_counts[status] += 1
        source_value = row.get("mosaic_file", "").strip()
        relative_tif = ""
        has_geotiff = "false"

        if status in RASTER_STATUSES and source_value:
            source_tif = Path(source_value)
            if source_tif.is_file():
                destination = output_root / iso3 / source_tif.name
                transfer_counts[link_or_copy(source_tif, destination, args.copy)] += 1
                expected_tifs.add(destination)
                relative_tif = destination.relative_to(output_root).as_posix()
                has_geotiff = "true"
            elif row.get("mosaic_exists", "").lower() == "true":
                missing_declared_mosaics.append(f"{iso3}:{date}:{source_tif}")

        final_row = {
            "iso3": iso3,
            "date": date,
            "audit_status": status,
            "has_geotiff": has_geotiff,
            "relative_geotiff": relative_tif,
            "valid_pixel_probe": row.get("valid_pixel_probe", ""),
            "h5_count": row.get("h5_count", ""),
            "valid_h5_count": row.get("valid_h5_count", ""),
        }
        final_rows.append(final_row)
        country_rows[iso3].append(final_row)

    if missing_declared_mosaics:
        raise RuntimeError(
            "Audit declared mosaics that are missing:\n" + "\n".join(missing_declared_mosaics)
        )

    for existing_tif in output_root.glob("*/*.tif"):
        if existing_tif not in expected_tifs:
            existing_tif.unlink()
            transfer_counts["removed_stale"] += 1

    final_rows.sort(key=lambda item: (item["iso3"], item["date"]))
    write_csv(output_root / "all_country_dates_index.csv", final_rows)
    for iso3, rows in sorted(country_rows.items()):
        rows.sort(key=lambda item: item["date"])
        write_csv(output_root / iso3 / "coverage_status.csv", rows)

    summary = {
        "source_root": str(source_root),
        "output_root": str(output_root),
        "country_count": len(country_rows),
        "country_date_count": len(final_rows),
        "geotiff_count": sum(row["has_geotiff"] == "true" for row in final_rows),
        "status_counts": dict(sorted(status_counts.items())),
        "transfer_counts": dict(sorted(transfer_counts.items())),
        "countries": sorted(country_rows),
    }
    (output_root / "package_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    readme = f"""# VNP46A2 国家逐日最终成果

- 时间范围：2026-01-27 至 2026-05-31（共 125 天）
- 国家数：{summary['country_count']}
- 国家-日期组合：{summary['country_date_count']}
- GeoTIFF 数：{summary['geotiff_count']}
- 数据层：VNP46A2 `DNB_BRDF_Corrected_NTL`（未填补波段）
- 行政边界：OSM ADM0，简化容差 0.001 度
- 文件组织：每个 ISO3 子目录存放该国逐日 GeoTIFF 与 `coverage_status.csv`

## 状态解释

- `mosaic_valid`：存在具有有效像元的国家逐日 GeoTIFF。
- `mosaic_all_nodata`：官方 H5 存在且完成镶嵌，但该日期国界内未发现有效的未填补 NTL 像元；仍保留 GeoTIFF。
- `no_granules`：CMR 未返回该国家-日期所需 granule，因此没有 GeoTIFF，这不是下载失败。

根目录的 `all_country_dates_index.csv` 完整列出所有国家和日期，`package_summary.json` 提供汇总统计。

## 存储方式

GeoTIFF 默认通过同一 NTFS 卷上的硬链接整理，不额外复制影像内容。文件可以像普通 GeoTIFF 一样读取、移动或复制；删除本整理目录不会删除源成果。
"""
    (output_root / "README.md").write_text(readme, encoding="utf-8-sig")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
