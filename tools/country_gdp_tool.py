from __future__ import annotations

import csv
import json
import subprocess
from pathlib import Path
from typing import Any, Optional

import requests
from bs4 import BeautifulSoup
from langchain_core.runnables import RunnableConfig
from langchain_core.runnables.config import var_child_runnable_config
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field

from storage_manager import current_thread_id, storage_manager

WORLD_BANK_INDICATOR = "NY.GDP.MKTP.CD"
WORLD_BANK_URL_TEMPLATE = (
    "https://api.worldbank.org/v2/country/{country_code}/indicator/"
    + WORLD_BANK_INDICATOR
    + "?format=json&date={start_year}:{end_year}"
)
WORLD_BANK_PROVIDER = "World Bank Indicators API"
WORLDOMETERS_URL = "https://www.worldometers.info/gdp/gdp-by-country/"
WORLDOMETERS_PROVIDER = "Worldometers GDP by Country"

COUNTRY_ALIASES = {
    "china": "CHN",
    "中国": "CHN",
    "united states": "USA",
    "usa": "USA",
    "us": "USA",
    "美国": "USA",
    "japan": "JPN",
    "日本": "JPN",
    "germany": "DEU",
    "德国": "DEU",
    "france": "FRA",
    "法国": "FRA",
    "united kingdom": "GBR",
    "uk": "GBR",
    "britain": "GBR",
    "英国": "GBR",
    "india": "IND",
    "印度": "IND",
    "hong kong": "HKG",
    "hong kong sar, china": "HKG",
    "香港": "HKG",
    "macau": "MAC",
    "macao": "MAC",
    "macao sar, china": "MAC",
    "澳门": "MAC",
    "taiwan": "TWN",
    "taiwan, china": "TWN",
    "台湾": "TWN",
}


class CountryGDPInput(BaseModel):
    countries: str = Field(..., description="Comma-separated country names or ISO3 codes, e.g. China,USA,HKG.")
    start_year: Optional[int] = Field(default=None, description="Start year for World Bank historical GDP.")
    end_year: Optional[int] = Field(default=None, description="End year for World Bank historical GDP.")
    source_preference: str = Field(
        default="auto",
        description="auto, world_bank, or worldometers. Worldometers is latest snapshot only.",
    )
    output_csv_filename: Optional[str] = Field(default=None, description="Optional CSV filename saved under inputs/.")


def _resolve_thread_id_from_config(config: Optional[RunnableConfig] = None) -> str:
    runtime_config: Optional[RunnableConfig] = config if isinstance(config, dict) else None
    if runtime_config is None:
        inherited = var_child_runnable_config.get()
        if isinstance(inherited, dict):
            runtime_config = inherited
    if isinstance(runtime_config, dict):
        tid = str(runtime_config.get("configurable", {}).get("thread_id", "") or "").strip()
        if tid:
            return tid
    return str(current_thread_id.get() or "debug").strip() or "debug"


def _default_output_name(prefix: str, start_year: Optional[int], end_year: Optional[int]) -> str:
    safe = "".join(ch if ch.isalnum() else "_" for ch in prefix.strip()) or "country_gdp"
    year_part = "latest" if start_year is None and end_year is None else f"{start_year or 'na'}_{end_year or 'na'}"
    return f"{safe}_{year_part}.csv"


def _write_records_csv(records: list[dict[str, Any]], output_name: str, config: Optional[RunnableConfig]) -> str:
    thread_id = _resolve_thread_id_from_config(config)
    input_dir = storage_manager.get_workspace(thread_id) / "inputs"
    input_dir.mkdir(parents=True, exist_ok=True)
    out_path = input_dir / Path(output_name).name
    fieldnames = [
        "country_input",
        "country_code",
        "country_name",
        "year",
        "value",
        "unit",
        "source_provider",
        "source_url",
        "source_status",
        "gdp_display",
        "gdp_growth",
        "gdp_per_capita",
        "share_of_world_gdp",
        "error",
    ]
    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)
    return out_path.name


def _resolve_country_code(country: str) -> str:
    raw = str(country or "").strip()
    lowered = raw.lower()
    if lowered in COUNTRY_ALIASES:
        return COUNTRY_ALIASES[lowered]
    if len(raw) in {2, 3} and raw.isascii():
        return raw.upper()
    return raw.upper()


def _run_curl_json_request(url: str, timeout_s: int = 25) -> Any:
    proc = subprocess.run(
        ["curl.exe", "-L", url, "--max-time", str(timeout_s)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"curl_failed:{proc.stderr.strip() or proc.stdout.strip()}")
    return json.loads(proc.stdout)


def _query_world_bank_country_gdp(country_code: str, start_year: int, end_year: int) -> list[dict[str, Any]]:
    payload = _run_curl_json_request(
        WORLD_BANK_URL_TEMPLATE.format(country_code=country_code, start_year=start_year, end_year=end_year)
    )
    if not isinstance(payload, list) or len(payload) < 2 or not isinstance(payload[1], list):
        raise RuntimeError("world_bank_unexpected_payload")
    rows: list[dict[str, Any]] = []
    for item in payload[1]:
        value = item.get("value")
        try:
            year = int(item.get("date"))
        except Exception:
            continue
        rows.append(
            {
                "country_input": country_code,
                "country_code": item.get("countryiso3code") or country_code,
                "country_name": (item.get("country") or {}).get("value") or country_code,
                "year": year,
                "value": float(value) if value is not None else None,
                "unit": "current US$",
                "source_provider": WORLD_BANK_PROVIDER,
                "source_url": WORLD_BANK_URL_TEMPLATE.format(
                    country_code=country_code, start_year=start_year, end_year=end_year
                ),
                "source_status": "official_world_bank",
                "error": None if value is not None else "world_bank_missing_value",
            }
        )
    return rows


def _query_worldometers_latest(countries: list[str]) -> list[dict[str, Any]]:
    resp = requests.get(WORLDOMETERS_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    table = soup.find("table")
    if table is None:
        raise RuntimeError("worldometers_table_not_found")
    requested = {country.lower(): country for country in countries}
    alias_to_requested = {
        key: raw
        for raw in countries
        for key in {raw.lower(), raw.lower().replace(",", ""), COUNTRY_ALIASES.get(raw.lower(), "").lower()}
        if key
    }
    records: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        cells = [cell.get_text(" ", strip=True) for cell in tr.find_all(["th", "td"])]
        if len(cells) < 6 or cells[0] == "#":
            continue
        country_name = cells[1]
        normalized = country_name.lower()
        matched_input = None
        for raw in countries:
            candidate_code = _resolve_country_code(raw)
            if normalized == raw.lower() or normalized == candidate_code.lower() or raw.lower() in normalized:
                matched_input = raw
                break
        if not matched_input:
            continue
        full_value = cells[3].replace("$", "").replace(",", "").strip()
        try:
            numeric_value = float(full_value)
        except Exception:
            numeric_value = None
        records.append(
            {
                "country_input": matched_input,
                "country_code": _resolve_country_code(matched_input),
                "country_name": country_name,
                "year": None,
                "value": numeric_value,
                "unit": "current US$",
                "source_provider": WORLDOMETERS_PROVIDER,
                "source_url": WORLDOMETERS_URL,
                "source_status": "public_worldometers_latest_snapshot",
                "gdp_display": cells[2],
                "gdp_growth": cells[4],
                "gdp_per_capita": cells[5],
                "share_of_world_gdp": cells[6] if len(cells) > 6 else None,
                "error": None,
            }
        )
    return records


def country_gdp_search_tool(
    countries: str,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    source_preference: str = "auto",
    output_csv_filename: Optional[str] = None,
    config: Optional[RunnableConfig] = None,
) -> str:
    requested = [item.strip() for item in str(countries or "").split(",") if item.strip()]
    if not requested:
        return json.dumps({"status": "error", "message": "countries is required"}, ensure_ascii=False)
    preference = str(source_preference or "auto").strip().lower()
    if preference not in {"auto", "world_bank", "worldometers"}:
        return json.dumps({"status": "error", "message": f"Unsupported source_preference: {source_preference}"}, ensure_ascii=False)
    if (start_year is None) ^ (end_year is None):
        return json.dumps({"status": "error", "message": "start_year and end_year must be both set or both omitted"}, ensure_ascii=False)
    if start_year is not None and end_year is not None and start_year > end_year:
        return json.dumps({"status": "error", "message": "start_year must be <= end_year"}, ensure_ascii=False)

    records: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    if preference in {"auto", "world_bank"}:
        if start_year is None:
            start_year = end_year = 2024
        for raw_country in requested:
            country_code = _resolve_country_code(raw_country)
            try:
                wb_rows = _query_world_bank_country_gdp(country_code, start_year, end_year)
                if wb_rows:
                    for row in wb_rows:
                        row["country_input"] = raw_country
                    records.extend(wb_rows)
                else:
                    failures.append({"country_input": raw_country, "country_code": country_code, "error": "world_bank_no_rows"})
            except Exception as exc:  # noqa: BLE001
                failures.append({"country_input": raw_country, "country_code": country_code, "error": str(exc)})

    if preference == "worldometers" or (preference == "auto" and start_year == end_year == 2024 and failures):
        try:
            latest_rows = _query_worldometers_latest(requested)
            existing = {(row["country_input"], row.get("year")) for row in records}
            for row in latest_rows:
                key = (row["country_input"], row.get("year"))
                if key not in existing:
                    records.append(row)
        except Exception as exc:  # noqa: BLE001
            failures.append({"country_input": ",".join(requested), "country_code": None, "error": f"worldometers_failed:{exc}"})

    output_name = output_csv_filename or _default_output_name("country_gdp", start_year, end_year)
    output_file = _write_records_csv(records, output_name, config=config) if records else None
    covered_countries = {str(row.get("country_input")) for row in records}
    requested_countries = set(requested)
    if records and covered_countries >= requested_countries:
        status = "success"
    elif records:
        status = "partial"
    else:
        status = "no_data"
    report = {
        "status": status,
        "provider": "Country GDP federated search tool",
        "records": sorted(records, key=lambda row: (str(row.get("country_input")), str(row.get("year") or "")), reverse=False),
        "request": {
            "countries": requested,
            "start_year": start_year,
            "end_year": end_year,
            "source_preference": preference,
        },
        "coverage": {
            "record_count": len(records),
            "failure_count": len(failures),
            "failures": failures,
        },
        "output_file": output_file,
    }
    return json.dumps(report, ensure_ascii=False)


Country_GDP_Search_tool = StructuredTool.from_function(
    func=country_gdp_search_tool,
    name="Country_GDP_Search_tool",
    description=(
        "Fetch country-scale GDP. Uses World Bank historical GDP (current US$) as primary source and "
        "Worldometers latest GDP snapshot as fallback when applicable. Saves CSV into inputs/."
    ),
    args_schema=CountryGDPInput,
)
