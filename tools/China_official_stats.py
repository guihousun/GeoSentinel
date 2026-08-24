from __future__ import annotations

import csv
import contextlib
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from langchain_core.runnables import RunnableConfig
from langchain_core.runnables.config import var_child_runnable_config
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field

from .china_shared_datasets import (
    build_shared_indicator_record,
    normalize_shared_indicator_name,
    query_shared_gdp_row,
)
from storage_manager import current_thread_id, storage_manager


_NBS_ENDPOINT = "https://data.stats.gov.cn/easyquery.htm"
_NBS_REFERER = "https://data.stats.gov.cn/easyquery.htm?cn=E0103"
_GDP_INDICATOR_CODE = "A020101"  # Regional GDP, 100 million CNY.

_MAINLAND_GDP_SOURCE = {
    "provider": "National Bureau of Statistics of China",
    "official_url": "https://data.stats.gov.cn/easyquery.htm?cn=E0103",
    "indicator_code": _GDP_INDICATOR_CODE,
    "unit": "100 million CNY",
}

_MAINLAND_GDP_YEARBOOK_SOURCE = {
    "provider": "National Bureau of Statistics of China, China Statistical Yearbook",
    "official_url": "https://www.stats.gov.cn/sj/ndsj/2021/html/C03-09.xls",
    "indicator": "Regional GDP by province-level region, current prices",
    "unit": "100 million CNY",
}

_MAINLAND_CENSUS_SOURCE = {
    "provider": "National Bureau of Statistics of China",
    "official_url": "https://www.stats.gov.cn/sj/zxfb/202302/t20230203_1901085.html",
    "indicator": "Seventh National Population Census, population by region",
    "unit": "persons",
}

_SPECIAL_REGION_SOURCES = {
    "hong_kong": {
        "gdp_provider": "Hong Kong Census and Statistics Department",
        "gdp_url": "https://www.censtatd.gov.hk/en/data/stat_report/product/B1030002/att/B10300022020AN20E0100.pdf",
        "population_provider": "Hong Kong Census and Statistics Department",
        "population_url": "https://www.censtatd.gov.hk/en/scode461.html",
    },
    "macau": {
        "gdp_provider": "Statistics and Census Service, Macao SAR Government",
        "gdp_url": "https://www.dsec.gov.mo/getAttachment/b7112c92-c92f-452b-b858-9059478076ce/E_GNI_FR_2020_Y.aspx",
        "population_provider": "Statistics and Census Service, Macao SAR Government",
        "population_url": "https://www.dsec.gov.mo/",
    },
    "taiwan": {
        "gdp_provider": "Directorate-General of Budget, Accounting and Statistics, Taiwan",
        "gdp_url": "https://ws.dgbas.gov.tw/public/data/dgbas03/bs2/yearbook_eng/yearbook2021.pdf",
        "population_provider": "Department of Household Registration, MOI, Taiwan",
        "population_url": "https://www.ris.gov.tw/",
    },
}


_REGIONS: List[Dict[str, str]] = [
    {"code": "110000", "slug": "beijing", "name_zh": "北京市", "name_en": "Beijing", "scope": "mainland"},
    {"code": "120000", "slug": "tianjin", "name_zh": "天津市", "name_en": "Tianjin", "scope": "mainland"},
    {"code": "130000", "slug": "hebei", "name_zh": "河北省", "name_en": "Hebei", "scope": "mainland"},
    {"code": "140000", "slug": "shanxi", "name_zh": "山西省", "name_en": "Shanxi", "scope": "mainland"},
    {"code": "150000", "slug": "inner_mongolia", "name_zh": "内蒙古自治区", "name_en": "Inner Mongolia", "scope": "mainland"},
    {"code": "210000", "slug": "liaoning", "name_zh": "辽宁省", "name_en": "Liaoning", "scope": "mainland"},
    {"code": "220000", "slug": "jilin", "name_zh": "吉林省", "name_en": "Jilin", "scope": "mainland"},
    {"code": "230000", "slug": "heilongjiang", "name_zh": "黑龙江省", "name_en": "Heilongjiang", "scope": "mainland"},
    {"code": "310000", "slug": "shanghai", "name_zh": "上海市", "name_en": "Shanghai", "scope": "mainland"},
    {"code": "320000", "slug": "jiangsu", "name_zh": "江苏省", "name_en": "Jiangsu", "scope": "mainland"},
    {"code": "330000", "slug": "zhejiang", "name_zh": "浙江省", "name_en": "Zhejiang", "scope": "mainland"},
    {"code": "340000", "slug": "anhui", "name_zh": "安徽省", "name_en": "Anhui", "scope": "mainland"},
    {"code": "350000", "slug": "fujian", "name_zh": "福建省", "name_en": "Fujian", "scope": "mainland"},
    {"code": "360000", "slug": "jiangxi", "name_zh": "江西省", "name_en": "Jiangxi", "scope": "mainland"},
    {"code": "370000", "slug": "shandong", "name_zh": "山东省", "name_en": "Shandong", "scope": "mainland"},
    {"code": "410000", "slug": "henan", "name_zh": "河南省", "name_en": "Henan", "scope": "mainland"},
    {"code": "420000", "slug": "hubei", "name_zh": "湖北省", "name_en": "Hubei", "scope": "mainland"},
    {"code": "430000", "slug": "hunan", "name_zh": "湖南省", "name_en": "Hunan", "scope": "mainland"},
    {"code": "440000", "slug": "guangdong", "name_zh": "广东省", "name_en": "Guangdong", "scope": "mainland"},
    {"code": "450000", "slug": "guangxi", "name_zh": "广西壮族自治区", "name_en": "Guangxi", "scope": "mainland"},
    {"code": "460000", "slug": "hainan", "name_zh": "海南省", "name_en": "Hainan", "scope": "mainland"},
    {"code": "500000", "slug": "chongqing", "name_zh": "重庆市", "name_en": "Chongqing", "scope": "mainland"},
    {"code": "510000", "slug": "sichuan", "name_zh": "四川省", "name_en": "Sichuan", "scope": "mainland"},
    {"code": "520000", "slug": "guizhou", "name_zh": "贵州省", "name_en": "Guizhou", "scope": "mainland"},
    {"code": "530000", "slug": "yunnan", "name_zh": "云南省", "name_en": "Yunnan", "scope": "mainland"},
    {"code": "540000", "slug": "tibet", "name_zh": "西藏自治区", "name_en": "Tibet", "scope": "mainland"},
    {"code": "610000", "slug": "shaanxi", "name_zh": "陕西省", "name_en": "Shaanxi", "scope": "mainland"},
    {"code": "620000", "slug": "gansu", "name_zh": "甘肃省", "name_en": "Gansu", "scope": "mainland"},
    {"code": "630000", "slug": "qinghai", "name_zh": "青海省", "name_en": "Qinghai", "scope": "mainland"},
    {"code": "640000", "slug": "ningxia", "name_zh": "宁夏回族自治区", "name_en": "Ningxia", "scope": "mainland"},
    {"code": "650000", "slug": "xinjiang", "name_zh": "新疆维吾尔自治区", "name_en": "Xinjiang", "scope": "mainland"},
    {"code": "710000", "slug": "taiwan", "name_zh": "台湾省", "name_en": "Taiwan", "scope": "taiwan"},
    {"code": "810000", "slug": "hong_kong", "name_zh": "香港特别行政区", "name_en": "Hong Kong", "scope": "hong_kong"},
    {"code": "820000", "slug": "macau", "name_zh": "澳门特别行政区", "name_en": "Macau", "scope": "macau"},
]

_ALIASES: Dict[str, str] = {}
for _region in _REGIONS:
    for _alias in {
        _region["code"],
        _region["slug"],
        _region["slug"].replace("_", " "),
        _region["name_zh"],
        _region["name_zh"].removesuffix("省").removesuffix("市").removesuffix("自治区").removesuffix("特别行政区"),
        _region["name_en"],
    }:
        _ALIASES[_alias.strip().lower()] = _region["code"]
_ALIASES.update(
    {
        "all": "ALL_34",
        "all_34": "ALL_34",
        "china_34": "ALL_34",
        "各省": "ALL_34",
        "全国各省": "ALL_34",
        "34个省级行政区": "ALL_34",
        "内蒙古": "150000",
        "广西": "450000",
        "西藏": "540000",
        "宁夏": "640000",
        "新疆": "650000",
        "香港": "810000",
        "澳门": "820000",
        "shaanxi": "610000",
        "shanxi province": "140000",
        "shaanxi province": "610000",
    }
)

_REGION_BY_CODE = {row["code"]: row for row in _REGIONS}
_COMPACT_MAINLAND_NAME_TO_CODE = {
    row["name_zh"]
    .removesuffix("省")
    .removesuffix("市")
    .removesuffix("自治区")
    .removesuffix("壮族自治区")
    .removesuffix("回族自治区")
    .removesuffix("维吾尔自治区")
    .replace(" ", ""): row["code"]
    for row in _REGIONS
    if row["scope"] == "mainland"
}
_COMPACT_MAINLAND_NAME_TO_CODE.update(
    {
        "内蒙古": "150000",
        "广西": "450000",
        "西藏": "540000",
        "宁夏": "640000",
        "新疆": "650000",
    }
)

_MAINLAND_GDP_CACHE_2020: Dict[str, float] = {
    # Source: China Statistical Yearbook 2021, table 3-9 地区生产总值 (2020年).
    "110000": 36102.55,
    "120000": 14083.73,
    "130000": 36206.89,
    "140000": 17651.93,
    "150000": 17359.82,
    "210000": 25114.96,
    "220000": 12311.32,
    "230000": 13698.50,
    "310000": 38700.58,
    "320000": 102718.98,
    "330000": 64613.34,
    "340000": 38680.63,
    "350000": 43903.89,
    "360000": 25691.50,
    "370000": 73129.00,
    "410000": 54997.07,
    "420000": 43443.46,
    "430000": 41781.49,
    "440000": 110760.94,
    "450000": 22156.69,
    "460000": 5532.39,
    "500000": 25002.79,
    "510000": 48598.76,
    "520000": 17826.56,
    "530000": 24521.90,
    "540000": 1902.74,
    "610000": 26181.86,
    "620000": 9016.70,
    "630000": 3005.92,
    "640000": 3920.55,
    "650000": 13797.58,
}
_MAINLAND_GDP_LOCAL_CACHE: Dict[int, Dict[str, float]] = {2020: _MAINLAND_GDP_CACHE_2020}
_SPECIAL_GDP_LOCAL_CACHE: Dict[int, Dict[str, Dict[str, Any]]] = {
    2020: {
        "710000": {
            "value": 19798597,
            "unit": "million NT$",
            "source_provider": _SPECIAL_REGION_SOURCES["taiwan"]["gdp_provider"],
            "source_url": _SPECIAL_REGION_SOURCES["taiwan"]["gdp_url"],
            "note": "DGBAS Statistical Yearbook 2021, GDP at current prices.",
        },
        "810000": {
            "value": 2710730,
            "unit": "million HK$",
            "source_provider": _SPECIAL_REGION_SOURCES["hong_kong"]["gdp_provider"],
            "source_url": _SPECIAL_REGION_SOURCES["hong_kong"]["gdp_url"],
            "note": "Hong Kong 2020 Gross Domestic Product report, Table 1 GDP by major expenditure component.",
        },
        "820000": {
            "value": 204410,
            "unit": "million MOP",
            "source_provider": _SPECIAL_REGION_SOURCES["macau"]["gdp_provider"],
            "source_url": _SPECIAL_REGION_SOURCES["macau"]["gdp_url"],
            "note": "Macao DSEC 2020 Gross National Income release, GDP at current prices.",
        },
    }
}
_YEARBOOK_GDP_TABLE_BY_DATA_YEAR: Dict[int, str] = {
    2020: "https://www.stats.gov.cn/sj/ndsj/2021/html/C03-09.xls",
}
_YEARBOOK_GDP_CACHE: Dict[int, Tuple[Dict[str, float], Optional[str]]] = {}
_PUBLIC_GITHUB_GDP_SOURCE = {
    "provider": "henrylin03/china-gdp public CSV derived from China provincial GDP data",
    "official_url": "https://raw.githubusercontent.com/henrylin03/china-gdp/main/input/Chinas%20GDP%20in%20Province%20Zh.csv",
    "unit": "100 million CNY",
}
_PUBLIC_WIKIPEDIA_GDP_SOURCE = {
    "provider": "Wikipedia China provincial GDP table, citing NBS revised provincial GDP data",
    "official_url": (
        "https://zh.wikipedia.org/wiki/"
        "%E4%B8%AD%E5%8D%8E%E4%BA%BA%E6%B0%91%E5%85%B1%E5%92%8C%E5%9B%BD"
        "%E7%9C%81%E7%BA%A7%E8%A1%8C%E6%94%BF%E5%8C%BA%E5%9C%B0%E5%8C%BA"
        "%E7%94%9F%E4%BA%A7%E6%80%BB%E5%80%BC%E5%88%97%E8%A1%A8"
    ),
    "unit": "100 million CNY",
}
_PUBLIC_GITHUB_GDP_CACHE: Optional[Tuple[Dict[int, Dict[str, float]], Optional[str]]] = None
_PUBLIC_WIKIPEDIA_GDP_CACHE: Optional[Tuple[Dict[int, Dict[str, float]], Optional[str]]] = None

_CENSUS_POPULATION_2020: Dict[str, Dict[str, Any]] = {
    "110000": {"value": 21893095, "source": _MAINLAND_CENSUS_SOURCE},
    "120000": {"value": 13866009, "source": _MAINLAND_CENSUS_SOURCE},
    "130000": {"value": 74610235, "source": _MAINLAND_CENSUS_SOURCE},
    "140000": {"value": 34915616, "source": _MAINLAND_CENSUS_SOURCE},
    "150000": {"value": 24049155, "source": _MAINLAND_CENSUS_SOURCE},
    "210000": {"value": 42591407, "source": _MAINLAND_CENSUS_SOURCE},
    "220000": {"value": 24073453, "source": _MAINLAND_CENSUS_SOURCE},
    "230000": {"value": 31850088, "source": _MAINLAND_CENSUS_SOURCE},
    "310000": {"value": 24870895, "source": _MAINLAND_CENSUS_SOURCE},
    "320000": {"value": 84748016, "source": _MAINLAND_CENSUS_SOURCE},
    "330000": {"value": 64567588, "source": _MAINLAND_CENSUS_SOURCE},
    "340000": {"value": 61027171, "source": _MAINLAND_CENSUS_SOURCE},
    "350000": {"value": 41540086, "source": _MAINLAND_CENSUS_SOURCE},
    "360000": {"value": 45188635, "source": _MAINLAND_CENSUS_SOURCE},
    "370000": {"value": 101527453, "source": _MAINLAND_CENSUS_SOURCE},
    "410000": {"value": 99365519, "source": _MAINLAND_CENSUS_SOURCE},
    "420000": {"value": 57752557, "source": _MAINLAND_CENSUS_SOURCE},
    "430000": {"value": 66444864, "source": _MAINLAND_CENSUS_SOURCE},
    "440000": {"value": 126012510, "source": _MAINLAND_CENSUS_SOURCE},
    "450000": {"value": 50126804, "source": _MAINLAND_CENSUS_SOURCE},
    "460000": {"value": 10081232, "source": _MAINLAND_CENSUS_SOURCE},
    "500000": {"value": 32054159, "source": _MAINLAND_CENSUS_SOURCE},
    "510000": {"value": 83674866, "source": _MAINLAND_CENSUS_SOURCE},
    "520000": {"value": 38562148, "source": _MAINLAND_CENSUS_SOURCE},
    "530000": {"value": 47209277, "source": _MAINLAND_CENSUS_SOURCE},
    "540000": {"value": 3648100, "source": _MAINLAND_CENSUS_SOURCE},
    "610000": {"value": 39528999, "source": _MAINLAND_CENSUS_SOURCE},
    "620000": {"value": 25019831, "source": _MAINLAND_CENSUS_SOURCE},
    "630000": {"value": 5923957, "source": _MAINLAND_CENSUS_SOURCE},
    "640000": {"value": 7202654, "source": _MAINLAND_CENSUS_SOURCE},
    "650000": {"value": 25852345, "source": _MAINLAND_CENSUS_SOURCE},
    "710000": {
        "value": 23561236,
        "source": {
            "provider": _SPECIAL_REGION_SOURCES["taiwan"]["population_provider"],
            "official_url": _SPECIAL_REGION_SOURCES["taiwan"]["population_url"],
            "indicator": "Official year-end registered population, used as Taiwan population total for 2020.",
            "unit": "persons",
        },
    },
    "810000": {
        "value": 7474200,
        "source": {
            "provider": _SPECIAL_REGION_SOURCES["hong_kong"]["population_provider"],
            "official_url": _SPECIAL_REGION_SOURCES["hong_kong"]["population_url"],
            "indicator": "Official 2020 year-end population estimate.",
            "unit": "persons",
        },
    },
    "820000": {
        "value": 683218,
        "source": {
            "provider": _SPECIAL_REGION_SOURCES["macau"]["population_provider"],
            "official_url": _SPECIAL_REGION_SOURCES["macau"]["population_url"],
            "indicator": "Official 2020 year-end population estimate.",
            "unit": "persons",
        },
    },
}


class ChinaOfficialGDPInput(BaseModel):
    region: str = Field(..., description="Region name/code, e.g. Shanghai/上海/all_34.")
    start_year: int = Field(..., description="Start year, e.g. 2013.")
    end_year: int = Field(..., description="End year, e.g. 2022.")
    indicator: str = Field(default="GDP", description="Compatibility field. GDP only.")
    output_csv_filename: Optional[str] = Field(default=None, description="Optional CSV filename saved under inputs/.")


class ChinaOfficialStatsInput(BaseModel):
    regions: str = Field(default="all_34", description="Comma-separated regions or all_34.")
    start_year: int = Field(default=2020, description="Start year for annual GDP.")
    end_year: int = Field(default=2020, description="End year for annual GDP.")
    indicators: str = Field(
        default="both",
        description="gdp, census_population, resident_population, electricity_consumption, co2_emissions, or both.",
    )
    census_year: int = Field(default=2020, description="Supported census/reference population year, currently 2020.")
    output_csv_filename: Optional[str] = Field(default=None, description="Optional CSV filename saved under inputs/.")


def _resolve_thread_id_from_config(config: Optional[RunnableConfig] = None) -> str:
    runtime_config: Optional[RunnableConfig] = config if isinstance(config, dict) else None
    if runtime_config is None:
        inherited = var_child_runnable_config.get()
        if isinstance(inherited, dict):
            runtime_config = inherited
    if isinstance(runtime_config, dict):
        try:
            tid = str(storage_manager.get_thread_id_from_config(runtime_config) or "").strip()
            if tid:
                return tid
        except Exception:
            pass
    return str(current_thread_id.get() or "debug").strip() or "debug"


def _normalize_region_key(region: str) -> str:
    return str(region or "").strip().lower()


def _resolve_region_code(region: str) -> Optional[str]:
    key = _normalize_region_key(region)
    if not key:
        return None
    return _ALIASES.get(key)


def _resolve_region_codes(regions: str) -> Tuple[List[str], List[str]]:
    requested = [part.strip() for part in str(regions or "").split(",") if part.strip()]
    if not requested:
        requested = ["all_34"]
    resolved: List[str] = []
    unsupported: List[str] = []
    for item in requested:
        code = _resolve_region_code(item)
        if code == "ALL_34":
            for row in _REGIONS:
                resolved.append(row["code"])
            continue
        if code:
            resolved.append(code)
        else:
            unsupported.append(item)
    deduped = list(dict.fromkeys(resolved))
    return deduped, unsupported


def _parse_indicators(indicators: str) -> List[str]:
    raw = str(indicators or "both").strip().lower()
    if raw in {"both", "all"}:
        return ["gdp", "census_population"]
    aliases = {
        "gdp": "gdp",
        "gross domestic product": "gdp",
        "census": "census_population",
        "population": "census_population",
        "census_population": "census_population",
        "人口普查": "census_population",
        "人口": "census_population",
    }
    parsed: List[str] = []
    for part in [p.strip() for p in raw.replace(";", ",").split(",") if p.strip()]:
        normalized = aliases.get(part, part)
        parsed.append(normalize_shared_indicator_name(normalized))
    return list(dict.fromkeys(parsed))


def _query_nbs_region_gdp(region_code: str, year: int, timeout_s: int = 20) -> Tuple[Optional[float], Optional[str]]:
    params = {
        "m": "QueryData",
        "dbcode": "fsnd",
        "rowcode": "reg",
        "colcode": "sj",
        "wds": json.dumps([{"wdcode": "zb", "valuecode": _GDP_INDICATOR_CODE}], ensure_ascii=False),
        "dfwds": json.dumps(
            [
                {"wdcode": "reg", "valuecode": region_code},
                {"wdcode": "sj", "valuecode": str(year)},
            ],
            ensure_ascii=False,
        ),
        "k1": str(int(time.time() * 1000)),
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": _NBS_REFERER,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }
    try:
        with requests.Session() as session:
            session.headers.update(headers)
            with contextlib.suppress(Exception):
                session.get(_NBS_REFERER, timeout=min(10, timeout_s))
            resp = session.get(_NBS_ENDPOINT, params=params, timeout=timeout_s)
        if resp.status_code == 403:
            return None, "nbs_403_forbidden"
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:  # noqa: BLE001
        return None, f"nbs_request_failed:{exc}"
    nodes = payload.get("returndata", {}).get("datanodes", []) if isinstance(payload, dict) else []
    for node in nodes:
        data = node.get("data", {}) if isinstance(node, dict) else {}
        if not data.get("hasdata"):
            continue
        value = data.get("data")
        try:
            return float(value), None
        except Exception:
            return None, f"nbs_non_numeric_value:{value}"
    return None, "nbs_no_data"


def _normalize_yearbook_region_name(value: Any) -> str:
    return str(value or "").replace(" ", "").replace("\u3000", "").strip()


def _parse_yearbook_gdp_xls(content: bytes) -> Dict[str, float]:
    try:
        import xlrd  # type: ignore[import-untyped]
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("xlrd_not_installed_for_official_yearbook_xls") from exc

    workbook = xlrd.open_workbook(file_contents=content)
    sheet = workbook.sheet_by_index(0)
    values: Dict[str, float] = {}
    for row_idx in range(sheet.nrows):
        region_name = _normalize_yearbook_region_name(sheet.cell_value(row_idx, 0))
        region_code = _COMPACT_MAINLAND_NAME_TO_CODE.get(region_name)
        if not region_code:
            continue
        try:
            values[region_code] = float(sheet.cell_value(row_idx, 1))
        except Exception:
            continue
    if len(values) < 31:
        raise RuntimeError(f"yearbook_gdp_table_incomplete:{len(values)}")
    return values


def _query_yearbook_region_gdp(region_code: str, year: int, timeout_s: int = 20) -> Tuple[Optional[float], Optional[str], str]:
    url = _YEARBOOK_GDP_TABLE_BY_DATA_YEAR.get(year)
    if not url:
        return None, "yearbook_xls_not_registered_for_year", ""
    if year not in _YEARBOOK_GDP_CACHE:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": f"https://www.stats.gov.cn/sj/ndsj/{year + 1}/left.htm",
            "Accept": "application/vnd.ms-excel,application/octet-stream,*/*;q=0.8",
        }
        try:
            resp = requests.get(url, headers=headers, timeout=timeout_s)
            if resp.status_code == 404:
                _YEARBOOK_GDP_CACHE[year] = ({}, "yearbook_xls_404_not_found")
            elif resp.status_code == 403:
                _YEARBOOK_GDP_CACHE[year] = ({}, "yearbook_xls_403_forbidden")
            else:
                resp.raise_for_status()
                _YEARBOOK_GDP_CACHE[year] = (_parse_yearbook_gdp_xls(resp.content), None)
        except Exception as exc:  # noqa: BLE001
            _YEARBOOK_GDP_CACHE[year] = ({}, f"yearbook_xls_failed:{exc}")
    values, error = _YEARBOOK_GDP_CACHE[year]
    if region_code in values:
        return values[region_code], None, url
    return None, error or "yearbook_xls_no_region_value", url


def _query_local_cached_gdp(region_code: str, year: int) -> Tuple[Optional[float], Optional[str], str]:
    values = _MAINLAND_GDP_LOCAL_CACHE.get(year)
    if not values:
        return None, "official_local_cache_not_registered_for_year", ""
    if region_code not in values:
        return None, "official_local_cache_no_region_value", _MAINLAND_GDP_YEARBOOK_SOURCE["official_url"]
    return values[region_code], None, _MAINLAND_GDP_YEARBOOK_SOURCE["official_url"]


def _query_shared_base_data_gdp(region_code: str, year: int) -> Tuple[Optional[float], Optional[str], str, Optional[Dict[str, Any]]]:
    row = query_shared_gdp_row(region_code, year)
    if not row:
        return None, "shared_gdp_base_data_no_region_year_value", "", None
    try:
        value = float(str(row.get("value") or "").replace(",", ""))
    except Exception:
        return None, "shared_gdp_base_data_non_numeric", "", row
    return value, None, str(row.get("source_url") or ""), row


def _parse_public_github_gdp_csv(text: str) -> Dict[int, Dict[str, float]]:
    rows = list(csv.reader(text.splitlines()))
    if not rows:
        raise RuntimeError("public_github_csv_empty")
    header = rows[0]
    column_codes: List[Optional[str]] = [None]
    for name in header[1:]:
        column_codes.append(_resolve_region_code(name))
    values_by_year: Dict[int, Dict[str, float]] = {}
    for row in rows[1:]:
        if not row or not str(row[0]).strip().isdigit():
            continue
        year = int(str(row[0]).strip())
        values: Dict[str, float] = {}
        for idx, raw_value in enumerate(row[1:], start=1):
            if idx >= len(column_codes):
                continue
            region_code = column_codes[idx]
            if not region_code:
                continue
            try:
                values[region_code] = float(str(raw_value).replace(",", "").strip())
            except Exception:
                continue
        if values:
            values_by_year[year] = values
    if not values_by_year:
        raise RuntimeError("public_github_csv_no_values")
    return values_by_year


def _get_public_github_gdp_cache(timeout_s: int = 20) -> Tuple[Dict[int, Dict[str, float]], Optional[str]]:
    global _PUBLIC_GITHUB_GDP_CACHE
    if _PUBLIC_GITHUB_GDP_CACHE is None:
        try:
            resp = requests.get(
                _PUBLIC_GITHUB_GDP_SOURCE["official_url"],
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=timeout_s,
            )
            resp.raise_for_status()
            _PUBLIC_GITHUB_GDP_CACHE = (_parse_public_github_gdp_csv(resp.text), None)
        except Exception as exc:  # noqa: BLE001
            _PUBLIC_GITHUB_GDP_CACHE = ({}, f"public_github_gdp_failed:{exc}")
    return _PUBLIC_GITHUB_GDP_CACHE


def _query_public_github_gdp(region_code: str, year: int, timeout_s: int = 20) -> Tuple[Optional[float], Optional[str], str]:
    values_by_year, error = _get_public_github_gdp_cache(timeout_s=timeout_s)
    values = values_by_year.get(year, {})
    if region_code in values:
        return values[region_code], None, _PUBLIC_GITHUB_GDP_SOURCE["official_url"]
    return None, error or "public_github_gdp_no_region_year_value", _PUBLIC_GITHUB_GDP_SOURCE["official_url"]


def _parse_public_wikipedia_gdp_html(text: str) -> Dict[int, Dict[str, float]]:
    try:
        from bs4 import BeautifulSoup  # type: ignore[import-untyped]
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("bs4_not_installed_for_public_wikipedia_gdp") from exc

    soup = BeautifulSoup(text, "lxml")
    for table in soup.find_all("table"):
        caption_text = table.get_text(" ", strip=True)
        rows = table.find_all("tr")
        if not rows:
            continue
        header_cells = rows[0].find_all(["th", "td"])
        header_text = " ".join(cell.get_text(" ", strip=True) for cell in header_cells)
        if "主要年份" not in header_text and "2024" not in header_text and "2023" not in header_text:
            continue
        if "广东" not in caption_text:
            continue
        years: List[Optional[int]] = []
        for cell in header_cells[1:]:
            label = cell.get_text(" ", strip=True)
            digits = "".join(ch for ch in label if ch.isdigit())
            years.append(int(digits[:4]) if len(digits) >= 4 else None)
        values_by_year: Dict[int, Dict[str, float]] = {}
        for tr in rows[1:]:
            cells = tr.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            region_name = _normalize_yearbook_region_name(cells[0].get_text(" ", strip=True))
            region_code = _COMPACT_MAINLAND_NAME_TO_CODE.get(region_name)
            if not region_code:
                continue
            for idx, cell in enumerate(cells[1:]):
                if idx >= len(years) or years[idx] is None:
                    continue
                raw = cell.get_text("", strip=True).replace(",", "").replace("—", "").strip()
                if not raw:
                    continue
                try:
                    # Wikipedia table is in million RMB; the tool standardizes mainland GDP to 100 million CNY.
                    values_by_year.setdefault(years[idx], {})[region_code] = round(float(raw) / 100.0, 2)
                except Exception:
                    continue
        if values_by_year:
            return values_by_year
    raise RuntimeError("public_wikipedia_gdp_table_not_found")


def _get_public_wikipedia_gdp_cache(timeout_s: int = 20) -> Tuple[Dict[int, Dict[str, float]], Optional[str]]:
    global _PUBLIC_WIKIPEDIA_GDP_CACHE
    if _PUBLIC_WIKIPEDIA_GDP_CACHE is None:
        try:
            resp = requests.get(
                _PUBLIC_WIKIPEDIA_GDP_SOURCE["official_url"],
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=timeout_s,
            )
            resp.raise_for_status()
            _PUBLIC_WIKIPEDIA_GDP_CACHE = (_parse_public_wikipedia_gdp_html(resp.text), None)
        except Exception as exc:  # noqa: BLE001
            _PUBLIC_WIKIPEDIA_GDP_CACHE = ({}, f"public_wikipedia_gdp_failed:{exc}")
    return _PUBLIC_WIKIPEDIA_GDP_CACHE


def _query_public_wikipedia_gdp(region_code: str, year: int, timeout_s: int = 20) -> Tuple[Optional[float], Optional[str], str]:
    values_by_year, error = _get_public_wikipedia_gdp_cache(timeout_s=timeout_s)
    values = values_by_year.get(year, {})
    if region_code in values:
        return values[region_code], None, _PUBLIC_WIKIPEDIA_GDP_SOURCE["official_url"]
    return None, error or "public_wikipedia_gdp_no_region_year_value", _PUBLIC_WIKIPEDIA_GDP_SOURCE["official_url"]


def _shared_indicator_record(region: Dict[str, str], year: int, indicator: str) -> Dict[str, Any]:
    return build_shared_indicator_record(
        indicator=indicator,
        region=region,
        year=year,
        resolve_region_code=_resolve_region_code,
    )


def _gdp_record(region: Dict[str, str], year: int, timeout_s: int = 20) -> Dict[str, Any]:
    if region["scope"] != "mainland":
        source = _SPECIAL_REGION_SOURCES[region["scope"]]
        cached = _SPECIAL_GDP_LOCAL_CACHE.get(year, {}).get(region["code"])
        if cached:
            return {
                "indicator": "gdp",
                "year": year,
                "region_code": region["code"],
                "region_name": region["name_zh"],
                "region_name_en": region["name_en"],
                "value": cached["value"],
                "unit": cached["unit"],
                "source_provider": cached["source_provider"],
                "source_url": cached["source_url"],
                "source_status": "official_local_cache",
                "error": None,
            }
        return {
            "indicator": "gdp",
            "year": year,
            "region_code": region["code"],
            "region_name": region["name_zh"],
            "region_name_en": region["name_en"],
            "value": None,
            "unit": "100 million local currency or converted CNY; not auto-normalized",
            "source_provider": source["gdp_provider"],
            "source_url": source["gdp_url"],
            "source_status": "official_source_registered_manual_fetch_required",
            "error": "Special administrative/Taiwan GDP uses separate official statistical systems; automatic parsing is not enabled yet.",
        }
    value, error = _query_nbs_region_gdp(region["code"], year, timeout_s=timeout_s)
    source_provider = _MAINLAND_GDP_SOURCE["provider"]
    source_url = _MAINLAND_GDP_SOURCE["official_url"]
    source_status = "official_api"
    if value is None:
        nbs_error = error
        value, yearbook_error, yearbook_url = _query_yearbook_region_gdp(region["code"], year, timeout_s=timeout_s)
        if value is not None:
            error = None
            source_provider = _MAINLAND_GDP_YEARBOOK_SOURCE["provider"]
            source_url = yearbook_url or _MAINLAND_GDP_YEARBOOK_SOURCE["official_url"]
            source_status = "official_yearbook_xls"
        else:
            value, shared_error, shared_url, shared_row = _query_shared_base_data_gdp(region["code"], year)
            if value is not None:
                error = None
                source_provider = str(shared_row.get("source_provider") or "Shared GDP base_data cache")
                source_url = shared_url or str(shared_row.get("source_url") or "")
                source_status = str(shared_row.get("source_status") or "shared_base_data")
            else:
                value, cache_error, cache_url = _query_local_cached_gdp(region["code"], year)
                if value is not None:
                    error = None
                    source_provider = _MAINLAND_GDP_YEARBOOK_SOURCE["provider"]
                    source_url = cache_url or _MAINLAND_GDP_YEARBOOK_SOURCE["official_url"]
                    source_status = "official_local_cache"
                else:
                    value, github_error, github_url = _query_public_github_gdp(region["code"], year, timeout_s=timeout_s)
                    if value is not None:
                        error = None
                        source_provider = _PUBLIC_GITHUB_GDP_SOURCE["provider"]
                        source_url = github_url
                        source_status = "public_github_dataset"
                    else:
                        value, wiki_error, wiki_url = _query_public_wikipedia_gdp(region["code"], year, timeout_s=timeout_s)
                        if value is not None:
                            error = None
                            source_provider = _PUBLIC_WIKIPEDIA_GDP_SOURCE["provider"]
                            source_url = wiki_url
                            source_status = "public_wikipedia_dataset"
                        else:
                            error = ";".join(
                                x
                                for x in [nbs_error, yearbook_error, shared_error, cache_error, github_error, wiki_error]
                                if x
                            )
                            source_status = "official_and_public_sources_unavailable"
    return {
        "indicator": "gdp",
        "year": year,
        "region_code": region["code"],
        "region_name": region["name_zh"],
        "region_name_en": region["name_en"],
        "value": value,
        "unit": _MAINLAND_GDP_SOURCE["unit"],
        "source_provider": source_provider,
        "source_url": source_url,
        "source_status": source_status if value is not None else "official_sources_unavailable",
        "error": error,
    }


def _census_population_record(region: Dict[str, str], census_year: int) -> Dict[str, Any]:
    payload = _CENSUS_POPULATION_2020.get(region["code"]) if census_year == 2020 else None
    source = dict((payload or {}).get("source") or _MAINLAND_CENSUS_SOURCE)
    value = (payload or {}).get("value")
    return {
        "indicator": "census_population",
        "year": census_year,
        "region_code": region["code"],
        "region_name": region["name_zh"],
        "region_name_en": region["name_en"],
        "value": value,
        "unit": source.get("unit", "persons"),
        "source_provider": source.get("provider", ""),
        "source_url": source.get("official_url", ""),
        "source_status": "official_curated" if value is not None else "unsupported_census_year",
        "error": None if value is not None else f"census_population_year_{census_year}_not_available",
    }


def _indicator_record(region: Dict[str, str], year: int, indicator: str, timeout_s: int = 20) -> Dict[str, Any]:
    if indicator == "gdp":
        return _gdp_record(region, year, timeout_s=timeout_s)
    if indicator == "census_population":
        return _census_population_record(region, year)
    return _shared_indicator_record(region, year, indicator)


def _default_output_name(prefix: str, start_year: int, end_year: int) -> str:
    safe = "".join(c if c.isalnum() else "_" for c in prefix.strip()) or "china_official_stats"
    return f"{safe}_{start_year}_{end_year}.csv"


def _write_records_csv(records: List[Dict[str, Any]], output_name: str, config: Optional[RunnableConfig]) -> str:
    thread_id = _resolve_thread_id_from_config(config)
    input_dir = storage_manager.get_workspace(thread_id) / "inputs"
    input_dir.mkdir(parents=True, exist_ok=True)
    out_path = input_dir / Path(output_name).name
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
    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)
    return out_path.name


def _build_report(
    *,
    records: List[Dict[str, Any]],
    unsupported_regions: List[str],
    output_file: Optional[str],
    requested_indicators: List[str],
    start_year: int,
    end_year: int,
    census_year: int,
) -> Dict[str, Any]:
    values_found = [r for r in records if r.get("value") is not None]
    missing = [r for r in records if r.get("value") is None]
    if records and not missing and not unsupported_regions:
        status = "success"
    elif values_found:
        status = "partial"
    else:
        status = "no_data"
    return {
        "status": status,
        "provider": "China official statistics federated tool",
        "records": records,
        "output_file": output_file,
        "request": {
            "indicators": requested_indicators,
            "gdp_year_range": {"start_year": start_year, "end_year": end_year},
            "census_year": census_year,
        },
        "coverage": {
            "record_count": len(records),
            "value_count": len(values_found),
            "missing_count": len(missing),
            "unsupported_regions": unsupported_regions,
            "missing_records": [
                {
                    "indicator": r.get("indicator"),
                    "year": r.get("year"),
                    "region_name": r.get("region_name"),
                    "error": r.get("error"),
                    "source_url": r.get("source_url"),
                }
                for r in missing
            ],
        },
        "reliability": {
            "level": "high_when_value_present",
            "reason": (
                "Rows with values are from official structured NBS API, official yearbook tables, "
                "curated official census/publication values, or clearly-labelled public dataset fallbacks. "
                "Rows without values are not imputed."
            ),
        },
    }


def china_official_stats_tool(
    regions: str = "all_34",
    start_year: int = 2020,
    end_year: int = 2020,
    indicators: str = "both",
    census_year: int = 2020,
    output_csv_filename: Optional[str] = None,
    config: Optional[RunnableConfig] = None,
) -> str:
    if start_year > end_year:
        return json.dumps({"status": "error", "message": "start_year must be <= end_year"}, ensure_ascii=False)
    requested_indicators = _parse_indicators(indicators)
    supported_indicators = {
        "gdp",
        "census_population",
        "resident_population",
        "electricity_consumption",
        "co2_emissions",
    }
    unsupported_indicators = [x for x in requested_indicators if x not in supported_indicators]
    if unsupported_indicators:
        return json.dumps(
            {"status": "error", "message": f"Unsupported indicators: {unsupported_indicators}"},
            ensure_ascii=False,
        )
    region_codes, unsupported_regions = _resolve_region_codes(regions)
    selected_regions = [_REGION_BY_CODE[code] for code in region_codes if code in _REGION_BY_CODE]
    records: List[Dict[str, Any]] = []
    for region in selected_regions:
        for indicator_name in requested_indicators:
            if indicator_name == "census_population":
                records.append(_indicator_record(region, census_year, indicator_name))
                continue
            for year in range(start_year, end_year + 1):
                records.append(_indicator_record(region, year, indicator_name))
    output_name = output_csv_filename or _default_output_name("china_official_stats", start_year, end_year)
    output_file = _write_records_csv(records, output_name, config=config) if records else None
    return json.dumps(
        _build_report(
            records=records,
            unsupported_regions=unsupported_regions,
            output_file=output_file,
            requested_indicators=requested_indicators,
            start_year=start_year,
            end_year=end_year,
            census_year=census_year,
        ),
        ensure_ascii=False,
    )


def china_official_gdp_tool(
    region: str,
    start_year: int,
    end_year: int,
    indicator: str = "GDP",
    output_csv_filename: Optional[str] = None,
    config: Optional[RunnableConfig] = None,
) -> str:
    if str(indicator or "").strip().lower() not in {"gdp", "gross domestic product"}:
        return json.dumps({"status": "error", "message": "Only GDP is supported by this compatibility tool."}, ensure_ascii=False)
    return china_official_stats_tool(
        regions=region,
        start_year=start_year,
        end_year=end_year,
        indicators="gdp",
        output_csv_filename=output_csv_filename or _default_output_name(region, start_year, end_year),
        config=config,
    )


China_Official_Stats_tool = StructuredTool.from_function(
    func=china_official_stats_tool,
    name="China_Official_Stats_tool",
    description=(
        "Fetch China province-level statistics. Supports GDP from NBS, shared base_data GDP cache, official yearbook, "
        "and labelled public dataset fallbacks; supports official/cached 2020 GDP for Taiwan, Hong Kong, and Macau; "
        "supports official 2020 census/reference population totals plus shared base_data resident population, electricity "
        "consumption, and CO2 emissions; saves CSV into inputs/."
    ),
    args_schema=ChinaOfficialStatsInput,
)

China_Official_GDP_tool = StructuredTool.from_function(
    func=china_official_gdp_tool,
    name="China_Official_GDP_tool",
    description=(
        "Compatibility wrapper for annual regional GDP. Uses China_Official_Stats_tool internally with official and "
        "labelled public dataset fallbacks, and saves CSV into inputs/. "
        "For all-34 GDP plus census population requests, prefer China_Official_Stats_tool."
    ),
    args_schema=ChinaOfficialGDPInput,
)
