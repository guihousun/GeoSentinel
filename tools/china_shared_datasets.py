from __future__ import annotations

import csv
import json
import zipfile
from functools import lru_cache
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

from storage_manager import storage_manager

XLSX_NS = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

SHARED_DATASET_SPECS: dict[str, dict[str, Any]] = {
    "resident_population": {
        "path": "省总人口_万人.xlsx",
        "format": "wide",
        "unit": "10k persons",
        "source_provider": "Shared base_data workbook",
        "source_status": "shared_base_data",
        "source_label": "province_total_population",
        "aliases": {
            "resident_population",
            "total_population",
            "population_total",
            "population",
            "population_resident",
            "省总人口",
            "总人口",
        },
    },
    "electricity_consumption": {
        "path": "省电力消费量_亿千瓦每小时.xlsx",
        "format": "wide",
        "unit": "100 million kWh",
        "source_provider": "Shared base_data workbook",
        "source_status": "shared_base_data",
        "source_label": "province_electricity_consumption",
        "aliases": {
            "electricity_consumption",
            "power_consumption",
            "electricity",
            "electricity_use",
            "省电力消费量",
            "电力消费量",
            "用电量",
        },
    },
    "co2_emissions": {
        "path": "省级CO2排放_mt_1998-2022.xlsx",
        "format": "long",
        "unit": "Mt",
        "source_provider": "Shared base_data workbook",
        "source_status": "shared_base_data",
        "source_label": "province_co2_emissions",
        "aliases": {
            "co2_emissions",
            "co2",
            "carbon_emissions",
            "carbon_dioxide_emissions",
            "省级co2排放",
            "co2排放",
            "碳排放",
        },
    },
}


def normalize_shared_indicator_name(raw: str) -> str:
    key = str(raw or "").strip().lower()
    for canonical, spec in SHARED_DATASET_SPECS.items():
        if key == canonical or key in {alias.lower() for alias in spec["aliases"]}:
            return canonical
    return key


def shared_dataset_csv_path() -> Path:
    return storage_manager.shared_dir / "省级GDP_共享缓存.csv"


def _cell_value(shared_strings: list[str], cell: ET.Element) -> Any:
    cell_type = cell.get("t")
    value_node = cell.find("x:v", XLSX_NS)
    if value_node is None or value_node.text is None:
        return None
    raw = value_node.text
    if cell_type == "s":
        try:
            return shared_strings[int(raw)]
        except Exception:
            return raw
    try:
        number = float(raw)
        if number.is_integer():
            return int(number)
        return number
    except Exception:
        return raw


def _column_index(ref: str) -> int:
    letters = "".join(ch for ch in ref if ch.isalpha()).upper()
    idx = 0
    for ch in letters:
        idx = idx * 26 + (ord(ch) - 64)
    return idx - 1


def _read_xlsx_sheet_rows(path: Path) -> list[list[Any]]:
    with zipfile.ZipFile(path) as zf:
        shared_strings: list[str] = []
        if "xl/sharedStrings.xml" in zf.namelist():
            root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in root.findall("x:si", XLSX_NS):
                text = "".join(t.text or "" for t in si.findall(".//x:t", XLSX_NS))
                shared_strings.append(text)
        sheet_xml = ET.fromstring(zf.read("xl/worksheets/sheet1.xml"))
    rows: list[list[Any]] = []
    for row in sheet_xml.findall(".//x:sheetData/x:row", XLSX_NS):
        row_values: list[Any] = []
        for cell in row.findall("x:c", XLSX_NS):
            idx = _column_index(cell.get("r", "A1"))
            while len(row_values) <= idx:
                row_values.append(None)
            row_values[idx] = _cell_value(shared_strings, cell)
        rows.append(row_values)
    return rows


def _parse_wide_year_matrix(path: Path, resolve_region_code) -> dict[int, dict[str, float]]:
    rows = _read_xlsx_sheet_rows(path)
    if not rows:
        return {}
    header = rows[0]
    year_columns: dict[int, int] = {}
    for col_idx, value in enumerate(header[1:], start=1):
        if value is None:
            continue
        try:
            year_columns[col_idx] = int(str(value).strip())
        except Exception:
            continue
    values_by_year: dict[int, dict[str, float]] = {}
    for row in rows[1:]:
        if not row:
            continue
        region_name = str(row[0] or "").strip()
        region_code = resolve_region_code(region_name)
        if not region_code:
            continue
        for col_idx, year in year_columns.items():
            if col_idx >= len(row):
                continue
            value = row[col_idx]
            if value in (None, ""):
                continue
            try:
                values_by_year.setdefault(year, {})[region_code] = float(value)
            except Exception:
                continue
    return values_by_year


def _parse_long_year_rows(path: Path, resolve_region_code) -> dict[int, dict[str, float]]:
    rows = _read_xlsx_sheet_rows(path)
    if not rows:
        return {}
    header = [str(x or "").strip().lower() for x in rows[0]]
    province_idx = header.index("province")
    year_idx = header.index("year")
    value_idx = header.index("co2_emissions")
    values_by_year: dict[int, dict[str, float]] = {}
    for row in rows[1:]:
        if len(row) <= max(province_idx, year_idx, value_idx):
            continue
        region_code = resolve_region_code(str(row[province_idx] or "").strip())
        if not region_code:
            continue
        try:
            year = int(row[year_idx])
            value = float(row[value_idx])
        except Exception:
            continue
        values_by_year.setdefault(year, {})[region_code] = value
    return values_by_year


@lru_cache(maxsize=1)
def load_shared_indicator_values(resolve_region_code) -> dict[str, dict[int, dict[str, float]]]:
    payload: dict[str, dict[int, dict[str, float]]] = {}
    for indicator, spec in SHARED_DATASET_SPECS.items():
        path = storage_manager.shared_dir / spec["path"]
        if not path.exists():
            payload[indicator] = {}
            continue
        if spec["format"] == "wide":
            payload[indicator] = _parse_wide_year_matrix(path, resolve_region_code)
        else:
            payload[indicator] = _parse_long_year_rows(path, resolve_region_code)
    return payload


@lru_cache(maxsize=1)
def load_shared_gdp_rows() -> list[dict[str, Any]]:
    path = shared_dataset_csv_path()
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def query_shared_gdp_row(region_code: str, year: int) -> dict[str, Any] | None:
    for row in load_shared_gdp_rows():
        if row.get("region_code") == region_code and str(row.get("year")) == str(year):
            return row
    return None


def build_shared_indicator_record(
    *,
    indicator: str,
    region: dict[str, str],
    year: int,
    resolve_region_code,
) -> dict[str, Any]:
    spec = SHARED_DATASET_SPECS[indicator]
    values = load_shared_indicator_values(resolve_region_code).get(indicator, {})
    row = values.get(year, {})
    value = row.get(region["code"])
    return {
        "indicator": indicator,
        "year": year,
        "region_code": region["code"],
        "region_name": region["name_zh"],
        "region_name_en": region["name_en"],
        "value": value,
        "unit": spec["unit"],
        "source_provider": spec["source_provider"],
        "source_url": str((storage_manager.shared_dir / spec["path"]).name),
        "source_status": spec["source_status"] if value is not None else "shared_base_data_missing",
        "error": None if value is not None else f"{indicator}_year_or_region_not_available",
    }


def write_shared_gdp_cache(rows: list[dict[str, Any]]) -> Path:
    path = shared_dataset_csv_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "indicator",
        "year",
        "region_code",
        "region_name",
        "region_name_en",
        "value",
        "unit",
        "source_provider",
        "source_url",
        "source_status",
        "error",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    load_shared_gdp_rows.cache_clear()
    return path
