import os
import re
import json
import hashlib
import ast
import html
import io
import zipfile
import base64
import textwrap
import uuid
import socket
import time
from pathlib import Path
from typing import Optional
from datetime import UTC, datetime, timedelta
from urllib.request import Request, urlopen
from urllib.error import URLError
import subprocess
import sys

# --- 1.  ---
if 'CONDA_PREFIX' in os.environ:
    proj_path = os.path.join(os.environ['CONDA_PREFIX'], 'Library', 'share', 'proj')
    if os.path.exists(proj_path):
        os.environ['PROJ_LIB'] = proj_path
        os.environ['PROJ_DATA'] = proj_path
        try:
            import pyproj
            pyproj.datadir.set_data_dir(proj_path)
        except:
            pass

# --- 2. 第三方依赖 ---
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import numpy as np
import geopandas as gpd
import folium
import rasterio
from streamlit_folium import st_folium
from PIL import Image
from matplotlib import cm
from langchain_core.messages import HumanMessage, AIMessage, ToolMessage

# --- 3. 项目内部依赖 ---
import app_state 
import app_logic
import history_store
import model_config
from storage_manager import storage_manager
from map_view_policy import build_layer_signature, advance_map_view_state

if hasattr(st, "bottom"):
    st._bottom = st.bottom

try:
    from st_chat_input_multimodal import multimodal_chat_input
except Exception:  # noqa: BLE001
    multimodal_chat_input = None
    _MULTIMODAL_CHAT_INPUT_IMPORT_ERROR = sys.exc_info()[1]
else:
    _MULTIMODAL_CHAT_INPUT_IMPORT_ERROR = None


def _is_en() -> bool:
    return st.session_state.get("ui_lang", "EN") == "EN"


_MOJIBAKE_TOKENS = ("锛", "銆", "鈥", "鈩", "鍙", "鎺", "鏆", "璇", "宸", "妯", "闈", "绗")


def _looks_mojibake(text: str) -> bool:
    s = str(text or "")
    if not s:
        return False
    if "\ufffd" in s:
        return True
    hits = sum(1 for token in _MOJIBAKE_TOKENS if token in s)
    return hits >= 2 and any(ord(ch) > 127 for ch in s)


def _tr(zh: str, en: str) -> str:
    if _is_en():
        return en
    return en if _looks_mojibake(zh) else zh


APP_ROOT = Path(__file__).resolve().parent


def _project_path(*parts: str) -> Path:
    return APP_ROOT.joinpath(*parts)


def _html_data_uri(html_content: str) -> str:
    encoded = base64.b64encode(str(html_content or "").encode("utf-8")).decode("ascii")
    return f"data:text/html;charset=utf-8;base64,{encoded}"


def _render_html_iframe(html_content: str, *, height: int, scrolling: bool = False) -> None:
    iframe = getattr(st, "iframe", None)
    uri = _html_data_uri(html_content)
    if iframe is not None:
        iframe(uri, height=max(1, height))
        return
    components.iframe(uri, height=height, scrolling=scrolling)


def _render_dom_script(html_content: str, *, height: int = 0) -> None:
    components.html(html_content, height=height)


def _normalize_monitor_base_url(raw: Optional[str], default: str) -> str:
    value = (raw or "").strip() or default
    if not re.match(r"^https?://", value, re.IGNORECASE):
        value = f"http://{value}"
    if not value.endswith("/"):
        value = f"{value}/"
    return value


MONITOR_UI_URL = _normalize_monitor_base_url(
    os.getenv("NTL_MONITOR_PUBLIC_URL"),
    "http://139.9.165.59:8765",
)
_MONITOR_API_URL_OVERRIDE = (os.getenv("NTL_MONITOR_API_URL") or "").strip()
if _MONITOR_API_URL_OVERRIDE:
    MONITOR_API_URL = _MONITOR_API_URL_OVERRIDE
elif ("127.0.0.1" in MONITOR_UI_URL) or ("localhost" in MONITOR_UI_URL):
    MONITOR_API_URL = "http://127.0.0.1:8765/api/latest"
else:
    MONITOR_API_URL = f"{MONITOR_UI_URL}api/latest"
_NTL_AVAIL_SNAPSHOT_KEY = "ntl_data_availability_snapshot_v1"
_NTL_SCAN_SCRIPT_CANDIDATES = [
    _project_path("experiments", "official_daily_ntl_fastpath", "scan_ntl_availability.py"),
    _project_path("experiments", "official_daily_ntl_fastpath", "scan_official_ntl_availability.py"),
]
_NTL_SCAN_SCRIPT_PATH = next((p for p in _NTL_SCAN_SCRIPT_CANDIDATES if p.exists()), _NTL_SCAN_SCRIPT_CANDIDATES[0])
_NTL_SCAN_OUTPUT_DIR = _project_path("experiments", "official_daily_ntl_fastpath", "workspace_monitor", "outputs")
_NTL_SCAN_TIMEOUT_SECONDS = 160
_NTL_SCAN_REFRESH_SECONDS = 3600
_NTL_SCAN_LOCK_FILE = _NTL_SCAN_OUTPUT_DIR / ".ntl_availability_refresh.lock"
_NTL_SCAN_LOCK_STALE_SECONDS = max(_NTL_SCAN_TIMEOUT_SECONDS * 2, 600)
_TEST_CASE_FILES = [_project_path("example", "test_cases_mini.xlsx")]
_TEST_CASE_CATEGORY_EN = {
    "data retrieval and preprocessing": "Data Retrieval and Preprocessing",
    "ntl spatial statistic and querying": "NTL Spatial Statistic and Querying",
    "ntl application and modeling": "NTL Application and Modeling",
}
_TEST_CASE_CATEGORY_ZH = {
    "data retrieval and preprocessing": "数据检索与预处理",
    "ntl spatial statistic and querying": "夜间灯光空间统计与查询",
    "ntl application and modeling": "夜间灯光应用与建模",
}
_TEST_CASE_LABEL_ZH_BY_ID = {
    "1": "NPP VIIRS 年度数据",
    "2": "DMSP-OLS 年度数据",
    "3": "类 NPP-VIIRS 年度数据",
    "4": "NPP VIIRS 月度数据",
    "5": "VNP46A1 日度数据",
    "6": "VNP46A2 日度数据",
    "7": "月度合成",
    "8": "事件新闻",
    "9": "行政边界",
    "10": "兴趣点",
    "11": "坐标查询",
    "12": "SDGSAT-1 条带噪声去除",
    "13": "SDGSAT-1 辐射定标",
    "14": "NOAA-20 预处理",
    "15": "SDGSAT-1 RRLI 指数",
    "16": "分区统计",
    "17": "最亮像元坐标",
    "18": "最暗像元坐标",
    "19": "点位灯光强度",
    "20": "最大夜间灯光强度 [MaxNTL(P)]",
    "21": "总夜间灯光强度 [TNTL]",
    "22": "平均夜间灯光强度 [ANTL]",
    "23": "阈值处理",
    "24": "复合年增长率统计",
    "25": "逐年增长率统计",
    "26": "最高增长率区县识别",
    "27": "异常检测",
    "28": "GEE 日度统计",
    "29": "GEE 年度统计",
    "31": "阈值建成区提取",
    "32": "灯光强度差异",
    "33": "可视化",
    "34": "城市对比",
    "35": "趋势分析",
    "36": "地震影响评估",
    "37": "GDP 建模",
    "38": "COVID-19 封控影响",
    "39": "建成区提取",
    "40": "城市主干道路提取",
    "41": "路灯类型分类",
}
_TEST_CASE_QUERY_ZH_BY_ID = {
    "1": "检索 2019 至 2020 年上海市 NPP VIIRS 年度夜间灯光数据。",
    "2": "检索 2009 至 2010 年上海市 DMSP-OLS 年度夜间灯光数据。",
    "3": "检索 2010 至 2020 年上海市类 NPP-VIIRS 年度夜间灯光数据。",
    "4": "检索 2020 年 1 月至 12 月上海市 NPP VIIRS 月度夜间灯光数据。",
    "5": "检索 2020 年 12 月 1 日至 12 月 7 日上海市 NPP VIIRS VNP46A1 日度夜间灯光数据。",
    "6": "检索 2020 年 1 月 1 日至 1 月 7 日上海市 NPP VIIRS VNP46A2 日度夜间灯光数据。",
    "7": "检索 2020 年 1 月 1 日至 1 月 31 日上海市 NPP VIIRS VNP46A2 日度夜间灯光数据，并将其合成为月度夜间灯光影像。",
    "8": "从 USGS 和 ReliefWeb 检索 2025 年缅甸地震的事件信息。",
    "9": "从高德地图检索上海市行政边界。",
    "10": "从高德地图检索东方明珠塔周边 200 米范围内的兴趣点数据。",
    "11": "检索上海东方明珠塔的坐标。",
    "12": "对 inputs/ 目录中的 SDGSAT-1 GLI 影像执行条带噪声去除。",
    "13": "对 inputs/ 目录中的 SDGSAT-1 GLI 影像执行辐射定标。",
    "14": "对保存在 inputs/ 目录中的 NOAA-20 VIIRS 数据进行预处理。",
    "15": "基于 inputs/ 目录中的 SDGSAT-1 GLI 影像计算 RRLI 指数。",
    "16": "在 2022 年类 NPP-VIIRS 影像中，识别上海市 ANTL 最高的区。",
    "17": "识别 2022 年上海市 NPP-VIIRS 夜间灯光影像中最亮像元的 WGS84 坐标。",
    "18": "识别 2022 年 6 月上海市 NPP-VIIRS 夜间灯光影像中最暗像元的 WGS84 坐标。",
    "19": "报告 2020 年 NPP-VIIRS 影像中东方明珠塔位置的夜间灯光强度。",
    "20": "计算 2020 年上海市各区类 NPP-VIIRS 年度夜间灯光影像的最大夜间灯光强度。",
    "21": "计算 2020 年上海市各区类 NPP-VIIRS 年度夜间灯光影像的总夜间灯光强度。",
    "22": "计算 2020 年上海市各区类 NPP-VIIRS 年度夜间灯光影像的平均夜间灯光强度。",
    "23": "在上海市 2020 年行政边界内，计算 NPP-VIIRS 夜间灯光像元值大于 50 的比例。",
    "24": "计算 2015 至 2020 年上海市总夜间灯光强度的复合年增长率。",
    "25": "计算 2015 至 2020 年上海市总夜间灯光强度的逐年增长率。",
    "26": "识别 2019 至 2020 年上海市夜间灯光增长率最高的区。",
    "27": "检测 2015 至 2020 年上海市的异常像元。",
    "28": "使用 GEE Python 代码计算 2022 年 1 月 1 日至 1 月 31 日上海市每日 VNP46A2 平均夜间灯光强度（ANTL），并保存为 CSV。",
    "29": "使用 GEE Python 代码计算 2015 至 2022 年上海市每年 NPP-VIIRS 平均夜间灯光强度（ANTL），并保存为 CSV。",
    "31": "使用阈值 10 提取 2018 年上海市建成区。",
    "32": "计算并可视化 2020 年与 2021 年上海市夜间灯光强度差异。",
    "33": "下载 2020 年上海市类 NPP-VIIRS 影像，将小于 1 的噪声值设为 NaN，并使用 viridis 色带进行可视化。",
    "34": "比较 2022 年上海市和北京市的 ANTL，并说明哪个城市更亮。",
    "35": "对 2000 至 2020 年上海市类 NPP-VIIRS 年度夜间灯光数据进行斜率趋势分析。",
    "36": "使用日度 NPP-VIIRS VNP46A2 影像评估 2025 年缅甸地震影响。从官方来源（USGS、ReliefWeb）检索地震详情以确定震中，使用 GEE Python API 计算事件前、事件后以及震后首夜的 ANTL，并汇总结果用于损害评估。",
    "37": "检索上海市 GDP 数据以及 2013 至 2022 年 NPP-VIIRS 夜间灯光影像，使用多种回归模型分析 ANTL 与 GDP 的关系，并选择拟合效果最佳的模型。",
    "38": "计算武汉市官方封控期（2020 年 1 月 23 日至 4 月 8 日）的 ANTL，并与 2019 年同期进行比较。",
    "39": "下载 2020 年上海市夜间灯光影像，使用 SVM 方法提取建成区，并计算上海市各区建成区占比。",
    "40": "基于 inputs\\SDG_rgb.tif 中的 SDGSAT-1 GLI 夜间灯光影像提取城市主干道路，并将结果转换为 shapefile（.shp）。",
    "41": "基于 inputs\\SDG_rgb.tif 中的 RGB 夜间灯光影像计算红光比值指数（RRLI）和蓝光比值指数（RBLI），并按照 Jia 等（2024）的方法对像元进行类型分类。",
}


def _normalize_test_case_id(value) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    try:
        number = float(text)
    except ValueError:
        return text
    if number.is_integer():
        return str(int(number))
    return text


def _test_case_id_from_row(row: pd.Series) -> str:
    for key in ("Unnamed: 0", "No", "ID", "Id", "id"):
        if key in row.index:
            case_id = _normalize_test_case_id(row.get(key))
            if case_id:
                return case_id
    if len(row.index) > 0:
        return _normalize_test_case_id(row.iloc[0])
    return ""


def _localized_test_case(category: str, label: str, query: str, case_id: str) -> dict:
    category_key = str(category or "").strip().lower()
    if _is_en():
        display_category = _TEST_CASE_CATEGORY_EN.get(category_key, str(category or "General").strip().title())
        return {"id": case_id, "category": display_category, "label": label, "query": query}
    display_category = _TEST_CASE_CATEGORY_ZH.get(category_key, str(category or "通用").strip())
    return {
        "id": case_id,
        "category": display_category,
        "label": _TEST_CASE_LABEL_ZH_BY_ID.get(case_id, label),
        "query": _TEST_CASE_QUERY_ZH_BY_ID.get(case_id, query),
    }


def _normalize_availability_rows(payload: dict) -> list[dict]:
    rows = []
    all_rows = []
    if isinstance(payload.get("gee_rows"), list):
        all_rows.extend(payload.get("gee_rows") or [])
    if isinstance(payload.get("rows"), list):
        all_rows.extend(payload.get("rows") or [])

    for row in all_rows:
        if not isinstance(row, dict):
            continue
        rows.append(
            {
                "source": row.get("source") or row.get("dataset_id") or "-",
                "source_type": row.get("source_type"),
                "latest_global_date": row.get("latest_global_date") or row.get("latest_global_granule_date") or "-",
                "latest_global_lag_days": row.get("latest_global_lag_days", "-"),
                "latest_bbox_date": row.get("latest_bbox_date") or "-",
                "latest_bbox_lag_days": row.get("latest_bbox_lag_days", "-"),
                "range_start": row.get("collection_time_start") or row.get("range_start") or "-",
                "range_end": row.get("collection_time_end") or row.get("range_end") or "-",
            }
        )
    return rows


def _is_gee_row(row: dict) -> bool:
    source_type = str(row.get("source_type") or "").strip().lower()
    if source_type == "gee":
        return True
    source = str(row.get("source") or "").strip().lower()
    return source.startswith("gee ")


def _order_availability_rows(rows: list[dict]) -> list[dict]:
    return sorted(rows, key=lambda r: (0 if _is_gee_row(r) else 1, str(r.get("source") or "")))


def _build_snapshot_from_payload(payload: dict, source: str) -> dict:
    return {
        "ok": True,
        "generated_at_utc": payload.get("generated_at_utc"),
        "start_date": payload.get("start_date") or payload.get("granule_start_date"),
        "end_date": payload.get("end_date") or payload.get("granule_end_date"),
        "rows": _order_availability_rows(_normalize_availability_rows(payload)),
        "state": "ok",
        "error": None,
        "snapshot_source": source,
    }


def _find_latest_scan_json() -> Optional[Path]:
    if not _NTL_SCAN_OUTPUT_DIR.exists():
        return None
    files = sorted(_NTL_SCAN_OUTPUT_DIR.glob("official_ntl_availability_*.json"))
    return files[-1] if files else None


def _scan_age_seconds(path: Path) -> int:
    if not path.exists():
        return 10**9
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    return int((datetime.now(UTC) - mtime).total_seconds())


def _is_scan_fresh(path: Path, refresh_seconds: int = _NTL_SCAN_REFRESH_SECONDS) -> bool:
    return _scan_age_seconds(path) <= int(refresh_seconds)


def _try_acquire_scan_refresh_lock() -> bool:
    _NTL_SCAN_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if _NTL_SCAN_LOCK_FILE.exists():
        age = _scan_age_seconds(_NTL_SCAN_LOCK_FILE)
        if age > _NTL_SCAN_LOCK_STALE_SECONDS:
            _NTL_SCAN_LOCK_FILE.unlink(missing_ok=True)
    try:
        fd = os.open(str(_NTL_SCAN_LOCK_FILE), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(
                json.dumps(
                    {"pid": os.getpid(), "created_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")},
                    ensure_ascii=False,
                )
            )
        return True
    except FileExistsError:
        return False


def _release_scan_refresh_lock() -> None:
    _NTL_SCAN_LOCK_FILE.unlink(missing_ok=True)


def _run_ntl_scan_script() -> tuple[bool, Optional[str]]:
    if not _NTL_SCAN_SCRIPT_PATH.exists():
        return False, f"scan script not found: {_NTL_SCAN_SCRIPT_PATH}"

    today = datetime.now(UTC).date()
    start_day = today - timedelta(days=45)
    cmd = [
        sys.executable,
        str(_NTL_SCAN_SCRIPT_PATH),
        "--sources",
        "nrt_priority",
        "--granule-start-date",
        start_day.strftime("%Y-%m-%d"),
        "--granule-end-date",
        today.strftime("%Y-%m-%d"),
        "--include-gee",
    ]
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(Path(__file__).resolve().parent),
            capture_output=True,
            text=True,
            timeout=_NTL_SCAN_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return False, f"local scan timed out ({_NTL_SCAN_TIMEOUT_SECONDS}s)"
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)

    if proc.returncode != 0:
        err = (proc.stderr or "").strip() or (proc.stdout or "").strip()
        return False, err or f"scan failed (exit={proc.returncode})"
    return True, None


def _load_snapshot_from_scan_file(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    snapshot = _build_snapshot_from_payload(payload, source="local_scan")
    if not snapshot.get("generated_at_utc"):
        snapshot["generated_at_utc"] = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    return snapshot


def _load_ntl_availability_snapshot_once() -> dict:
    """Load availability once per page session with shared 1h cache and single refresher lock."""
    if _NTL_AVAIL_SNAPSHOT_KEY in st.session_state:
        return st.session_state[_NTL_AVAIL_SNAPSHOT_KEY]

    snapshot = {
        "ok": False,
        "generated_at_utc": None,
        "start_date": None,
        "end_date": None,
        "rows": [],
        "state": "error",
        "error": None,
        "snapshot_source": None,
    }

    local_json = _find_latest_scan_json()
    if local_json is not None and _is_scan_fresh(local_json):
        try:
            snapshot = _load_snapshot_from_scan_file(local_json)
            snapshot["snapshot_source"] = "local_cache_fresh_1h"
            st.session_state[_NTL_AVAIL_SNAPSHOT_KEY] = snapshot
            return snapshot
        except Exception as exc:  # noqa: BLE001
            snapshot["error"] = str(exc).strip()

    ok_local = False
    local_err: Optional[str] = None
    acquired = _try_acquire_scan_refresh_lock()
    if acquired:
        try:
            ok_local, local_err = _run_ntl_scan_script()
        finally:
            _release_scan_refresh_lock()
    else:
        local_err = "shared refresh in progress, using local cache"

    local_json = _find_latest_scan_json()

    if local_json is not None:
        try:
            snapshot = _load_snapshot_from_scan_file(local_json)
            snapshot["snapshot_source"] = (
                "local_cache_fresh_1h_after_refresh"
                if _is_scan_fresh(local_json)
                else "local_cache_stale"
            )
            if local_err and (not ok_local or not acquired):
                snapshot["error"] = local_err
            st.session_state[_NTL_AVAIL_SNAPSHOT_KEY] = snapshot
            return snapshot
        except Exception as exc:  # noqa: BLE001
            local_err = local_err or str(exc).strip()

    if local_err:
        snapshot["state"] = "waiting" if "timed out" in str(local_err).lower() else "error"
        snapshot["error"] = local_err

    if not snapshot.get("ok"):
        try:
            req = Request(MONITOR_API_URL, headers={"User-Agent": "NTL-GPT-UI/1.0"})
            with urlopen(req, timeout=6) as resp:  # noqa: S310
                payload = json.loads(resp.read().decode("utf-8", errors="replace"))
            snapshot = _build_snapshot_from_payload(payload, source="monitor_api")
        except Exception as exc:  # noqa: BLE001
            err = str(exc).strip()
            err_low = err.lower()
            waiting = False
            if isinstance(exc, (TimeoutError, socket.timeout)):
                waiting = True
            elif isinstance(exc, URLError):
                reason = str(getattr(exc, "reason", "")).lower()
                waiting = any(
                    token in reason
                    for token in ("timed out", "connection refused", "failed to establish a new connection", "winerror 10061")
                )
            if any(token in err_low for token in ("timed out", "connection refused", "winerror 10061", "errno 111", "max retries exceeded")):
                waiting = True
            snapshot["state"] = "waiting" if waiting else "error"
            snapshot["error"] = snapshot.get("error") or err

    st.session_state[_NTL_AVAIL_SNAPSHOT_KEY] = snapshot
    return snapshot

def _render_monitor_jump_button(label: str) -> None:
    if hasattr(st, "link_button"):
        st.link_button(label, MONITOR_UI_URL, width="stretch")
        return

    safe_label = html.escape(label)
    safe_url = html.escape(MONITOR_UI_URL, quote=True)
    st.markdown(
        (
            f'<a class="ntl-sidebar-link-btn" href="{safe_url}" '
            f'target="_blank" rel="noopener noreferrer">{safe_label}</a>'
        ),
        unsafe_allow_html=True,
    )

def _render_data_availability_block() -> None:
    """Render NTL data availability table from local cached scan, with shared refresh."""
    if _NTL_AVAIL_SNAPSHOT_KEY not in st.session_state:
        first_hit = _find_latest_scan_json()
        if first_hit is not None and _is_scan_fresh(first_hit):
            snapshot = _load_ntl_availability_snapshot_once()
        else:
            with st.spinner(_tr("正在加载 NTL 可用性（本地扫描）...", "Loading NTL availability (local scan)...")):
                snapshot = _load_ntl_availability_snapshot_once()
    else:
        snapshot = _load_ntl_availability_snapshot_once()

    if snapshot.get("ok"):
        rows = snapshot.get("rows") or []
        if _is_en():
            df = pd.DataFrame(rows).rename(
                columns={
                    "source": "Source",
                    "latest_global_date": "Latest",
                    "range_start": "Start",
                }
            )
            if not df.empty and "Source" in df.columns:
                df = df.set_index("Source")[["Start", "Latest"]]
            st.caption(f"Snapshot: {snapshot.get('generated_at_utc') or '-'}")
            jump_label = "Open NTL Data Monitor"
        else:
            df = pd.DataFrame(rows).rename(
                columns={
                    "source": "数据源",
                    "latest_global_date": "Latest",
                    "range_start": "Start",
                }
            )
            if not df.empty and "数据源" in df.columns:
                df = df.set_index("数据源")[["Start", "Latest"]]
            st.caption(f"快照时间: {snapshot.get('generated_at_utc') or '-'} | 来源={snapshot.get('snapshot_source') or '-'}")
            if snapshot.get("start_date") or snapshot.get("end_date"):
                st.caption(f"查询窗口: {snapshot.get('start_date') or '-'} -> {snapshot.get('end_date') or '-'}")
            jump_label = "打开夜光遥感数据监控界面"

        st.dataframe(df, width="stretch", hide_index=False, height=260)
        # if snapshot.get("error"):
        #     st.caption(_tr(f"本地扫描告警：{snapshot.get('error')}", f"Local scan warning: {snapshot.get('error')}"))
        # _render_monitor_jump_button(jump_label)
        return

    msg = snapshot.get("error") or "unknown"
    if snapshot.get("state") == "waiting":
        st.info(_tr("正在等待数据服务响应，请稍后重试。", "Waiting for data service to respond. Please retry shortly."))
        st.caption(_tr(f"等待原因：{msg}", f"Wait reason: {msg}"))
    else:
        st.warning(_tr(f"未能读取 Monitor 快照：{msg}", f"Failed to load monitor snapshot: {msg}"))
    _render_monitor_jump_button(_tr("打开夜光遥感数据监控界面", "Open NTL Data Monitor"))
    if st.button(_tr("重试加载", "Retry Loading"), key="retry_ntl_availability_snapshot", width="stretch"):
        if _NTL_AVAIL_SNAPSHOT_KEY in st.session_state:
            del st.session_state[_NTL_AVAIL_SNAPSHOT_KEY]
        st.rerun()


def _get_nasa_bg_data_uri() -> str:
    if "nasa_bg_data_uri" in st.session_state:
        return st.session_state["nasa_bg_data_uri"]
    img_path = _project_path("assets", "nasa_black_marble.jpg")
    if not img_path.exists():
        st.session_state["nasa_bg_data_uri"] = ""
        return ""
    with img_path.open("rb") as f:
        encoded = base64.b64encode(f.read()).decode("ascii")
    uri = f"data:image/jpeg;base64,{encoded}"
    st.session_state["nasa_bg_data_uri"] = uri
    return uri

# ==============================================================================
# SECTION A: Core HTML Templates
# ==============================================================================

BOT_TEMPLATE = """
<div class="chat-message bot">
    <div class="avatar"><span class="bot-badge">AI</span></div>
    <div class="message" >{{MSG}}</div>
</div>
"""

USER_TEMPLATE = """
<div class="chat-message user">
    <div class="avatar"><span class="user-badge">U</span></div>
    <div class="message" >{{MSG}}</div>
</div>
"""

# ==============================================================================
# SECTION B: 样式 (CSS) 与脚本 (JS)
# ==============================================================================

def inject_css():
    """Inject global CSS styles."""
    bg_uri = _get_nasa_bg_data_uri()
    bg_css = f"url('{bg_uri}')" if bg_uri else "none"
    css = """
    <style>
    :root {
        --ntl-bg: #f3f7f7;
        --ntl-border: #d7e3e0;
        --ntl-panel-border: rgba(255, 255, 255, 0.52);
        --ntl-chat-user: #123b62;
        --ntl-chat-bot: #0f766e;
        --ntl-subtle: #61717a;
    }
    .stApp {
        background:
            linear-gradient(rgba(14, 18, 33, 0.58), rgba(14, 18, 33, 0.58)),
            __BG__,
            radial-gradient(900px 300px at -20% -20%, #d6f2ed 0%, transparent 55%),
            radial-gradient(700px 240px at 120% 0%, #dbeafe 0%, transparent 50%),
            var(--ntl-bg);
        background-size: cover, cover, auto, auto, auto;
        background-attachment: fixed;
        background-position: center;
        color: #e8edf8;
    }
    header, header[data-testid="stHeader"] {
        background: transparent !important;
        height: 0 !important;
        min-height: 0 !important;
        display: block !important;
        overflow: visible !important;
        pointer-events: none !important;
    }
    div[data-testid="stDecoration"] { display: none !important; }
    [data-testid="stToolbar"] { display: none !important; }
    [data-testid="stSidebarCollapsedControl"] {
        display: flex !important;
        position: fixed !important;
        top: 0.75rem !important;
        left: 0.75rem !important;
        z-index: 1200 !important;
        pointer-events: auto !important;
    }
    [data-testid="stSidebarCollapsedControl"] button {
        width: 2.5rem !important;
        height: 2.5rem !important;
        min-height: 2.5rem !important;
        border: 1px solid rgba(149, 176, 255, 0.46) !important;
        border-radius: 8px !important;
        background: rgba(10, 18, 40, 0.94) !important;
        color: #eaf1ff !important;
        box-shadow: 0 8px 20px rgba(0, 0, 0, 0.28) !important;
    }
    [data-testid="stSidebarCollapsedControl"] button svg {
        fill: #eaf1ff !important;
        color: #eaf1ff !important;
    }
    [data-testid="stSidebar"] [data-testid="stBaseButton-headerNoPadding"] {
        width: 2.5rem !important;
        height: 2.5rem !important;
        min-height: 2.5rem !important;
        border: 1px solid rgba(149, 176, 255, 0.46) !important;
        border-radius: 8px !important;
        background: rgba(10, 18, 40, 0.94) !important;
        color: #eaf1ff !important;
        box-shadow: 0 8px 20px rgba(0, 0, 0, 0.28) !important;
    }
    [data-testid="stSidebar"] [data-testid="stBaseButton-headerNoPadding"] *,
    [data-testid="stSidebarCollapsedControl"] button * {
        color: #eaf1ff !important;
        fill: #eaf1ff !important;
        -webkit-text-fill-color: #eaf1ff !important;
        opacity: 1 !important;
    }
    [data-testid="stAppViewContainer"] {
        background: transparent !important;
    }
    .stDeployButton { visibility: hidden; }
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
    .block-container { padding: 3.1rem 1.5rem 8.0rem 1.5rem; max-width: 1700px; }
    [data-testid="stAppViewContainer"] [data-testid="stVerticalBlockBorderWrapper"],
    [data-testid="stAppViewContainer"] [data-testid="stVerticalBlock"][overflow="auto"][height="600px"] {
        border: 1.2px solid var(--ntl-panel-border) !important;
        box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.08) !important;
        border-radius: 12px !important;
    }
    [data-testid="stChatInput"] {
        position: fixed !important;
        bottom: calc(clamp(12px, 1.8vh, 20px) + env(safe-area-inset-bottom));
        left: 18px;
        transform: none !important;
        width: min(58vw, 760px);
        max-width: calc(100vw - 2rem);
        box-sizing: border-box;
        z-index: 900 !important;
        opacity: 1;
        height: auto !important;
        background: transparent !important;
    }
    [data-testid="stChatInput"] > div {
        width: 100% !important;
        max-width: 100% !important;
        background: rgba(15, 24, 48, 0.90) !important;
        border: 1.2px solid rgba(149, 176, 255, 0.40) !important;
        border-radius: 999px !important;
        box-shadow: inset 0 0 0 1px rgba(255,255,255,0.06) !important;
        min-height: 56px !important;
        padding: 0 0.55rem !important;
        display: flex !important;
        align-items: center !important;
    }
    [data-testid="stChatInput"] > div > div,
    [data-testid="stChatInput"] [data-baseweb],
    [data-testid="stChatInput"] [data-baseweb="base-input"],
    [data-testid="stChatInput"] [data-baseweb="textarea"] {
        background: transparent !important;
        border: none !important;
        box-shadow: none !important;
        outline: none !important;
        min-height: 42px !important;
        display: flex !important;
        align-items: center !important;
    }
    [data-testid="stChatInput"] textarea,
    [data-testid="stChatInput"] input {
        color: #e8edf8 !important;
        -webkit-text-fill-color: #e8edf8 !important;
        background: transparent !important;
        caret-color: #dbeafe !important;
        min-height: 42px !important;
        line-height: 42px !important;
        padding-top: 0 !important;
        padding-bottom: 0 !important;
        display: flex !important;
        align-items: center !important;
    }
    [data-testid="stChatInput"] textarea::placeholder,
    [data-testid="stChatInput"] input::placeholder {
        color: rgba(226, 236, 255, 0.58) !important;
        -webkit-text-fill-color: rgba(226, 236, 255, 0.58) !important;
    }
    [data-testid="stChatInput"] button {
        color: #b7cbff !important;
        background: transparent !important;
        border: none !important;
        box-shadow: none !important;
        min-height: 42px !important;
        height: 42px !important;
        display: inline-flex !important;
        align-items: center !important;
        justify-content: center !important;
    }
    iframe[title*="st_chat_input_multimodal"] {
        border: none !important;
        border-radius: 0 !important;
        background: transparent !important;
        box-shadow: none !important;
        outline: none !important;
        width: 100% !important;
        min-height: 96px !important;
        overflow: visible !important;
    }
    .st-key-main_chat_input_mm,
    .st-key-main_chat_input_mm > div,
    .st-key-main_chat_input_mm [data-testid="stElementContainer"],
    .st-key-main_chat_input_mm [data-testid="stCustomComponentV1"] {
        background: transparent !important;
        border: none !important;
        box-shadow: none !important;
        outline: none !important;
        padding: 0 !important;
        overflow: visible !important;
    }
    [data-testid="stBottom"],
    [data-testid="stBottomBlockContainer"] {
        background: transparent !important;
        border: none !important;
        box-shadow: none !important;
        padding: 0 !important;
    }
    [data-testid="stBottom"] > div,
    [data-testid="stBottomBlockContainer"] > div,
    [data-testid="stBottomBlockContainer"] [data-testid="stElementContainer"],
    div:has(> [data-testid="stBottomBlockContainer"]) {
        background: transparent !important;
        border: none !important;
        box-shadow: none !important;
        padding: 0 !important;
    }
    [data-testid="stBottomBlockContainer"] [data-testid="stChatInput"],
    [data-testid="stBottomBlockContainer"] [data-testid="stChatInput"] * {
        border-color: rgba(149, 176, 255, 0.40) !important;
    }
    div[data-testid="stElementContainer"]:has(iframe[title*="st_chat_input_multimodal"]) {
        background: transparent !important;
    }
    .chat-message {
        display: grid;
        grid-template-columns: 2.1rem 1fr;
        gap: 0.8rem;
        align-items: start;
        padding: 0.95rem 1rem;
        border-radius: 12px;
        margin-bottom: 0.7rem;
        color: #fff;
        box-shadow: 0 8px 20px rgba(16,24,40,0.08);
        max-width: 100%;
        overflow: hidden;
        min-width: 0;
    }
    .chat-message.user { background: linear-gradient(120deg, var(--ntl-chat-user), #1a5f95); }
    .chat-message.bot { background: linear-gradient(120deg, var(--ntl-chat-bot), #0d5f59); }
    .chat-message .avatar { font-size: 1.25rem; line-height: 1.4; }
    .chat-message .message {
        font-size: 0.97rem;
        line-height: 1.55;
        word-break: break-word;
        overflow-wrap: anywhere;
        max-width: 100%;
        min-width: 0;
    }
    .chat-message .message pre,
    .chat-message .message code,
    .chat-message .message table {
        max-width: 100% !important;
        box-sizing: border-box;
    }
    .chat-message .message pre {
        overflow-x: auto !important;
        white-space: pre-wrap !important;
        word-break: break-word !important;
        overflow-wrap: anywhere !important;
    }
    .chat-message .message code {
        white-space: pre-wrap !important;
        word-break: break-word !important;
        overflow-wrap: anywhere !important;
    }
    .chat-message .message table {
        display: block;
        overflow-x: auto;
    }
    .bot-badge, .user-badge {
        display: inline-flex;
        width: 1.45rem;
        height: 1.45rem;
        border-radius: 999px;
        align-items: center;
        justify-content: center;
        font-size: 0.72rem;
        font-weight: 700;
        letter-spacing: 0.02em;
        color: #ffffff;
    }
    .bot-badge { background: #0f172a; border: 1px solid rgba(255,255,255,0.35); }
    .user-badge { background: #1d4ed8; border: 1px solid rgba(255,255,255,0.35); }
    .stExpander { border: 1px solid var(--ntl-border); border-radius: 10px; }
    .stCode { border-radius: 10px; }
    [data-testid="stSidebar"] {
        min-width: 290px !important;
        max-width: 360px !important;
        border-right: 1px solid var(--ntl-border);
        background:
            linear-gradient(rgba(14, 18, 33, 0.52), rgba(14, 18, 33, 0.52)),
            __BG__,
            linear-gradient(180deg, #f8fbfb 0%, #edf6f6 100%);
        background-size: cover;
        background-position: center;
    }
    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3,
    [data-testid="stSidebar"] h4,
    [data-testid="stSidebar"] label {
        color: #f0f5ff !important;
    }
    [data-testid="stSidebar"] h3 {
        margin-top: 0.38rem !important;
        margin-bottom: 0.42rem !important;
    }
    .ntl-sidebar-divider-tight {
        height: 1px;
        margin: 0.32rem 0 0.38rem 0;
        background: linear-gradient(
            90deg,
            rgba(159, 182, 248, 0.00),
            rgba(159, 182, 248, 0.45),
            rgba(159, 182, 248, 0.00)
        );
    }
    [data-testid="stSidebar"] input, [data-testid="stSidebar"] textarea {
        color: #111827 !important;
        -webkit-text-fill-color: #111827 !important;
        background: #ffffff !important;
    }
    [data-testid="stSidebar"] input::placeholder,
    [data-testid="stSidebar"] textarea::placeholder {
        color: #9ca3af !important;
        -webkit-text-fill-color: #9ca3af !important;
    }
    [data-testid="stSidebar"] [data-baseweb="select"] > div,
    [data-testid="stSidebar"] [data-baseweb="select"] span,
    [data-testid="stSidebar"] [data-baseweb="select"] input {
        color: #111827 !important;
        -webkit-text-fill-color: #111827 !important;
    }
    [data-testid="stSidebar"] [data-testid="stTextInput"] input,
    [data-testid="stSidebar"] [data-testid="stNumberInput"] input {
        color: #111827 !important;
        -webkit-text-fill-color: #111827 !important;
    }
    [data-testid="stSidebar"] .stSelectbox svg {
        fill: #334155 !important;
    }
    [data-testid="stSidebar"] [data-testid="stWidgetLabel"] p {
        color: #f0f5ff !important;
    }
    [data-testid="stSidebar"] [data-testid="stMetricLabel"] p,
    [data-testid="stSidebar"] [data-testid="stMetricValue"] {
        color: #f8fbff !important;
    }
    [data-testid="stSidebar"] .stButton button {
        color: #0f172a !important;
        background: #ffffff !important;
        border: 1px solid #d7e3e0 !important;
        font-weight: 600 !important;
        opacity: 1 !important;
    }
    [data-testid="stSidebar"] .stButton button span {
        color: #0f172a !important;
        opacity: 1 !important;
    }
    [data-testid="stSidebar"] .stButton button:disabled,
    [data-testid="stSidebar"] .stButton button:disabled span {
        color: #64748b !important;
        opacity: 0.75 !important;
    }
    [data-testid="stSidebar"] code {
        color: #0f172a !important;
        background: #f8fafc !important;
    }
    [data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] {
        background: #ffffff !important;
        border: 1px solid #d7e3e0 !important;
    }
    [data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] * {
        color: #334155 !important;
        -webkit-text-fill-color: #334155 !important;
    }
    .stMarkdown p, .stText, .stAlert, .stInfo { color: #eaf1ff; }
    [data-testid="stWidgetLabel"] p { color: #eaf1ff !important; font-weight: 550; }
    [data-testid="stAppViewContainer"] [data-baseweb="select"] > div {
        background: rgba(15, 23, 42, 0.72) !important;
        border: 1px solid #334155 !important;
    }
    [data-testid="stAppViewContainer"] [data-baseweb="select"] span,
    [data-testid="stAppViewContainer"] [data-baseweb="select"] input {
        color: #eaf1ff !important;
        -webkit-text-fill-color: #eaf1ff !important;
    }
    [data-testid="stAppViewContainer"] [data-baseweb="select"] svg {
        fill: #dbe7ff !important;
    }
    /* Improve main-panel select contrast (e.g., Preview Output selector) */
    [data-testid="stAppViewContainer"] [data-baseweb="select"] > div {
        background: #ffffff !important;
        border: 1px solid #334155 !important;
    }
    [data-testid="stAppViewContainer"] [data-baseweb="select"] span,
    [data-testid="stAppViewContainer"] [data-baseweb="select"] input,
    [data-testid="stAppViewContainer"] [data-baseweb="select"] div[role="combobox"] {
        color: #0f172a !important;
        -webkit-text-fill-color: #0f172a !important;
        opacity: 1 !important;
    }
    [data-testid="stAppViewContainer"] [data-baseweb="select"] svg {
        fill: #334155 !important;
        opacity: 1 !important;
    }
    [data-testid="stVerticalBlock"] > div { gap: 0.5rem !important; padding-bottom: 0px !important; }
    .stMarkdown div p { margin-bottom: 5px !important; }
    .stCaption { color: #cdd8ee; }
    button[data-baseweb="tab"] p { font-size: 0.95rem !important; font-weight: 650 !important; color: #ffffff !important; }
    button[aria-selected="true"][data-baseweb="tab"] p { color: #ff5f5f !important; }
    button[data-baseweb="tab"] { padding-right: 14px !important; padding-left: 14px !important; }
    [data-testid="stMain"] [data-testid="stTabs"] [role="tablist"] {
        gap: 0.25rem !important;
    }
    [data-testid="stMain"] [data-testid="stTabs"] button[data-baseweb="tab"] {
        padding-left: 0.4rem !important;
        padding-right: 0.4rem !important;
    }
    [data-testid="stMain"] [data-testid="stTabs"] button[data-baseweb="tab"] p {
        font-size: 0.8rem !important;
        white-space: nowrap !important;
    }
    [data-testid="stSidebar"] button[data-baseweb="tab"] p {
        color: #ffffff !important;
        -webkit-text-fill-color: #ffffff !important;
        opacity: 1 !important;
    }
    [data-testid="stSidebar"] button[aria-selected="true"][data-baseweb="tab"] p { color: #ef4444 !important; }
    .stDownloadButton button, .stButton > button { border-radius: 8px; }
    [data-testid="stSidebar"] .stDownloadButton button { min-height: 30px !important; }
    div[data-testid="stRadio"] label p { font-size: 0.78rem !important; }
    /* Language switch (CN / EN) contrast */
    .st-key-ui_lang_switch {
        min-width: 7.5rem !important;
        width: 100% !important;
    }
    .st-key-ui_lang_switch [role="radiogroup"] {
        display: flex !important;
        flex-wrap: nowrap !important;
        justify-content: flex-end !important;
        gap: 0.75rem !important;
        min-width: 7.5rem !important;
    }
    .st-key-ui_lang_switch label,
    .st-key-ui_lang_switch label p,
    .st-key-ui_lang_switch label span {
        flex: 0 0 auto !important;
        min-width: max-content !important;
        white-space: nowrap !important;
        word-break: keep-all !important;
    }
    [data-testid="stRadio"] label,
    [data-testid="stRadio"] label p,
    [data-testid="stRadio"] label span,
    [data-testid="stRadio"] [role="radiogroup"] * {
        color: #eaf1ff !important;
        -webkit-text-fill-color: #eaf1ff !important;
        opacity: 1 !important;
        font-weight: 700 !important;
    }
    [data-testid="stSidebar"] [data-testid="InputInstructions"] {
        display: none !important;
        pointer-events: none !important;
    }
    [data-testid="stSidebar"] [data-testid="stTextInput"] button {
        position: relative !important;
        z-index: 2 !important;
        pointer-events: auto !important;
    }
    .ntl-card { border:1px solid #d7e3e0;border-radius:10px;background:rgba(255,255,255,0.94);padding:10px 12px; color:#111827; }
    .ntl-card *, .ntl-card p, .ntl-card span, .ntl-card div { color:#111827 !important; }
    [data-testid="stExpander"] details {
        background: rgba(255,255,255,0.96);
        border-radius: 10px;
    }
    [data-testid="stExpander"] summary p,
    [data-testid="stExpander"] label,
    [data-testid="stExpander"] span,
    [data-testid="stExpander"] p,
    [data-testid="stExpander"] li,
    [data-testid="stExpander"] div {
        color: #111827 !important;
    }
    [data-testid="stExpander"] .stCaption { color: #334155 !important; }
    [data-testid="stMain"] [data-testid="stExpander"] details,
    [data-testid="stMain"] [data-testid="stExpander"] details > summary,
    [data-testid="stMain"] [data-testid="stExpander"] details[open] > summary {
        background: #f8fafc !important;
    }
    [data-testid="stMain"] [data-testid="stExpander"] summary p,
    [data-testid="stMain"] [data-testid="stExpander"] summary span,
    [data-testid="stMain"] [data-testid="stExpander"] summary svg {
        color: #0f172a !important;
        fill: #0f172a !important;
        -webkit-text-fill-color: #0f172a !important;
        opacity: 1 !important;
    }
    [data-testid="stMain"] [data-testid="stJson"],
    [data-testid="stMain"] .react-json-view,
    [data-testid="stMain"] .pretty-json-container {
        background: #0b1220 !important;
        color: #e5eefc !important;
    }
    [data-testid="stMain"] [data-testid="stJson"] *,
    [data-testid="stMain"] .react-json-view *,
    [data-testid="stMain"] .pretty-json-container * {
        color: #e5eefc !important;
        -webkit-text-fill-color: #e5eefc !important;
        opacity: 1 !important;
    }
    [data-testid="stMain"] .react-json-view .variable-value > div[style*="background-color"],
    [data-testid="stMain"] .pretty-json-container .variable-value > div[style*="background-color"] {
        background: #263449 !important;
        color: #fecaca !important;
        -webkit-text-fill-color: #fecaca !important;
    }
    /* Reasoning panel: keep controls and generated markdown readable on white surfaces. */
    [data-testid="stMain"] .stPopover button,
    [data-testid="stMain"] [data-testid="stPopover"] button {
        min-width: 8.5rem !important;
        background: #f8fafc !important;
        border: 1px solid #94a3b8 !important;
        color: #0f172a !important;
        opacity: 1 !important;
    }
    [data-testid="stMain"] .stPopover button *,
    [data-testid="stMain"] [data-testid="stPopover"] button * {
        color: #0f172a !important;
        fill: #0f172a !important;
        -webkit-text-fill-color: #0f172a !important;
        opacity: 1 !important;
    }
    [data-testid="stMain"] [data-testid="stExpander"] .stMarkdown p,
    [data-testid="stMain"] [data-testid="stExpander"] .stMarkdown li,
    [data-testid="stMain"] [data-testid="stExpander"] .stMarkdown h1,
    [data-testid="stMain"] [data-testid="stExpander"] .stMarkdown h2,
    [data-testid="stMain"] [data-testid="stExpander"] .stMarkdown h3,
    [data-testid="stMain"] [data-testid="stExpander"] .stMarkdown h4 {
        color: #111827 !important;
        -webkit-text-fill-color: #111827 !important;
        opacity: 1 !important;
    }
    [data-testid="stMain"] [data-testid="stExpander"] .stMarkdown code:not(pre code) {
        background: #e2e8f0 !important;
        color: #064e3b !important;
        -webkit-text-fill-color: #064e3b !important;
    }
    [data-testid="stMain"] [data-testid="stExpander"] [data-testid="stCodeBlock"],
    [data-testid="stMain"] [data-testid="stExpander"] .stCode {
        background: #0b1220 !important;
        border: 1px solid #334155 !important;
    }
    [data-testid="stMain"] [data-testid="stExpander"] [data-testid="stCodeBlock"] *,
    [data-testid="stMain"] [data-testid="stExpander"] .stCode * {
        color: #f8fafc !important;
        -webkit-text-fill-color: #f8fafc !important;
        opacity: 1 !important;
    }
    .ntl-reasoning-role {
        color: #7c2d12 !important;
        font-size: 0.9rem !important;
        font-weight: 750 !important;
        margin-bottom: 0.25rem !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] details {
        background: #ffffff !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] summary p,
    [data-testid="stSidebar"] [data-testid="stExpander"] summary span {
        color: #0f172a !important;
    }
    /* Final contrast overrides for sidebar controls and white-surface blocks */
    [data-testid="stSidebar"] [data-baseweb="select"] > div {
        background: #ffffff !important;
        border: 1px solid #cbd5e1 !important;
    }
    [data-testid="stSidebar"] [data-baseweb="select"] span,
    [data-testid="stSidebar"] [data-baseweb="select"] input,
    [data-testid="stSidebar"] [data-baseweb="select"] div[role="combobox"] {
        color: #0f172a !important;
        -webkit-text-fill-color: #0f172a !important;
        opacity: 1 !important;
    }
    [data-testid="stSidebar"] [data-baseweb="select"] svg {
        fill: #334155 !important;
        opacity: 1 !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] .stMarkdown p,
    [data-testid="stSidebar"] [data-testid="stExpander"] .stMarkdown span,
    [data-testid="stSidebar"] [data-testid="stExpander"] .stMarkdown li,
    [data-testid="stSidebar"] [data-testid="stExpander"] .stCaption {
        color: #334155 !important;
        -webkit-text-fill-color: #334155 !important;
        opacity: 1 !important;
    }
    /* Premium sidebar style overrides (NTL-GPT night console) */
    [data-testid="stSidebar"] [data-baseweb="select"] > div {
        background: rgba(10, 18, 40, 0.86) !important;
        border: 1px solid rgba(110, 151, 255, 0.42) !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.08), 0 8px 20px rgba(5, 12, 30, 0.28) !important;
    }
    [data-testid="stSidebar"] [data-baseweb="select"] span,
    [data-testid="stSidebar"] [data-baseweb="select"] input,
    [data-testid="stSidebar"] [data-baseweb="select"] div[role="combobox"] {
        color: #eaf2ff !important;
        -webkit-text-fill-color: #eaf2ff !important;
    }
    [data-testid="stSidebar"] [data-baseweb="select"] svg {
        fill: #c9dcff !important;
    }
    [data-testid="stSidebar"] .stButton > button {
        min-height: 2.5rem !important;
        height: 2.5rem !important;
        border-radius: 10px !important;
        border: 1px solid rgba(121, 161, 255, 0.35) !important;
        background: linear-gradient(180deg, rgba(14, 28, 58, 0.90), rgba(7, 16, 36, 0.92)) !important;
        color: #dce8ff !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.07), 0 6px 16px rgba(0,0,0,0.24) !important;
        font-weight: 600 !important;
        font-size: 0.86rem !important;
        white-space: nowrap !important;
        line-height: 1 !important;
        padding: 0.1rem 0.4rem !important;
        letter-spacing: 0.01em !important;
        transition: transform 140ms ease, box-shadow 140ms ease, border-color 140ms ease !important;
    }
    [data-testid="stSidebar"] .stButton > button span {
        color: #dce8ff !important;
    }
    [data-testid="stSidebar"] .stButton > button:hover {
        transform: translateY(-1px) !important;
        border-color: rgba(142, 184, 255, 0.72) !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.1), 0 10px 18px rgba(0,0,0,0.3) !important;
    }
    [data-testid="stSidebar"] .stButton > button[kind="primary"] {
        border: 1px solid rgba(123, 243, 255, 0.92) !important;
        background: linear-gradient(135deg, #00a8ff 0%, #2d6dff 100%) !important;
        color: #ffffff !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.26), 0 8px 18px rgba(16, 127, 255, 0.42) !important;
    }
    [data-testid="stSidebar"] .stButton > button[kind="primary"] span {
        color: #ffffff !important;
    }
    [data-testid="stSidebar"] .stButton > button[kind="secondary"] {
        border: 1px solid rgba(121, 161, 255, 0.35) !important;
        background: linear-gradient(180deg, rgba(14, 28, 58, 0.90), rgba(7, 16, 36, 0.92)) !important;
        color: #dce8ff !important;
    }
    [data-testid="stSidebar"] .stButton > button[kind="secondary"] span {
        color: #dce8ff !important;
    }
    [data-testid="stSidebar"] .stButton > button[kind="tertiary"] {
        border: 1px solid rgba(255, 125, 125, 0.55) !important;
        background: linear-gradient(180deg, rgba(67, 20, 26, 0.88), rgba(45, 16, 24, 0.90)) !important;
        color: #ffd6d6 !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.08), 0 6px 16px rgba(32, 6, 8, 0.30) !important;
    }
    [data-testid="stSidebar"] .stButton > button[kind="tertiary"] span {
        color: #ffd6d6 !important;
    }
    [data-testid="stSidebar"] .st-key-activate_btn button {
        border: 1px solid rgba(121, 161, 255, 0.35) !important;
        background: linear-gradient(180deg, rgba(14, 28, 58, 0.90), rgba(7, 16, 36, 0.92)) !important;
        color: #dce8ff !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.07), 0 6px 16px rgba(0,0,0,0.24) !important;
        font-weight: 700 !important;
    }
    [data-testid="stSidebar"] .st-key-activate_btn button span {
        color: #dce8ff !important;
    }
    [data-testid="stSidebar"] .st-key-reset_btn button {
        border: 1px solid rgba(121, 161, 255, 0.35) !important;
        background: linear-gradient(180deg, rgba(14, 28, 58, 0.90), rgba(7, 16, 36, 0.92)) !important;
        color: #dce8ff !important;
    }
    [data-testid="stSidebar"] .st-key-reset_btn button span {
        color: #dce8ff !important;
    }
    [data-testid="stSidebar"] .st-key-delete_selected_thread_inline_trigger button,
    [data-testid="stSidebar"] .stElementContainer.st-key-delete_selected_thread_inline_trigger [data-testid="stBaseButton-secondary"] {
        border: 1px solid rgba(255, 125, 125, 0.58) !important;
        background: linear-gradient(180deg, rgba(67, 20, 26, 0.88), rgba(45, 16, 24, 0.90)) !important;
        color: #ffd6d6 !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.08), 0 6px 16px rgba(32, 6, 8, 0.30) !important;
        font-weight: 700 !important;
    }
    [data-testid="stSidebar"] .st-key-delete_selected_thread_inline_trigger button span,
    [data-testid="stSidebar"] .stElementContainer.st-key-delete_selected_thread_inline_trigger [data-testid="stBaseButton-secondary"] p,
    [data-testid="stSidebar"] .stElementContainer.st-key-delete_selected_thread_inline_trigger [data-testid="stBaseButton-secondary"] span {
        color: #ffd6d6 !important;
        -webkit-text-fill-color: #ffd6d6 !important;
    }
    [data-testid="stSidebar"] .st-key-delete_selected_thread_inline_trigger button p {
        display: none !important;
    }
    [data-testid="stSidebar"] .st-key-delete_selected_thread_inline_trigger [data-testid="stIconMaterial"] {
        color: #ffd6d6 !important;
        -webkit-text-fill-color: #ffd6d6 !important;
        font-size: 1.15rem !important;
        opacity: 1 !important;
    }
    [data-testid="stSidebar"] .st-key-delete_selected_thread_inline_trigger button:hover,
    [data-testid="stSidebar"] .stElementContainer.st-key-delete_selected_thread_inline_trigger [data-testid="stBaseButton-secondary"]:hover {
        border-color: rgba(255, 156, 156, 0.78) !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.1), 0 10px 18px rgba(32, 6, 8, 0.36) !important;
    }
    [data-testid="stSidebar"] .st-key-interrupt_current_run_btn button {
        border: 1px solid rgba(255, 125, 125, 0.60) !important;
        background: linear-gradient(180deg, rgba(67, 20, 26, 0.88), rgba(45, 16, 24, 0.90)) !important;
        color: #ffd6d6 !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.08), 0 6px 16px rgba(32, 6, 8, 0.30) !important;
    }
    [data-testid="stSidebar"] .st-key-interrupt_current_run_btn button span {
        color: #ffd6d6 !important;
    }
    [data-testid="stSidebar"] .stElementContainer.st-key-interrupt_current_run_btn [data-testid="stBaseButton-secondary"] {
        border: 1px solid rgba(255, 125, 125, 0.60) !important;
        background: linear-gradient(180deg, rgba(67, 20, 26, 0.88), rgba(45, 16, 24, 0.90)) !important;
        color: #ffd6d6 !important;
    }
    [data-testid="stSidebar"] .stElementContainer.st-key-interrupt_current_run_btn [data-testid="stBaseButton-secondary"] p,
    [data-testid="stSidebar"] .stElementContainer.st-key-interrupt_current_run_btn [data-testid="stBaseButton-secondary"] span {
        color: #ffd6d6 !important;
        -webkit-text-fill-color: #ffd6d6 !important;
    }
    [data-testid="stSidebar"] .stButton > button:disabled,
    [data-testid="stSidebar"] .stButton > button:disabled span {
        color: #6f88b0 !important;
        border-color: rgba(111, 136, 176, 0.28) !important;
        opacity: 0.7 !important;
    }
    [data-testid="stSidebar"] .stLinkButton > a,
    [data-testid="stSidebar"] a.ntl-sidebar-link-btn {
        display: inline-flex !important;
        align-items: center !important;
        justify-content: center !important;
        width: 100% !important;
        min-height: 2.5rem !important;
        border-radius: 10px !important;
        border: 1px solid rgba(121, 161, 255, 0.35) !important;
        background: linear-gradient(180deg, rgba(14, 28, 58, 0.90), rgba(7, 16, 36, 0.92)) !important;
        color: #dce8ff !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.07), 0 6px 16px rgba(0,0,0,0.24) !important;
        font-weight: 600 !important;
        font-size: 0.86rem !important;
        text-decoration: none !important;
        letter-spacing: 0.01em !important;
        transition: transform 140ms ease, box-shadow 140ms ease, border-color 140ms ease !important;
        padding: 0.1rem 0.4rem !important;
        box-sizing: border-box !important;
    }
    [data-testid="stSidebar"] .stLinkButton > a *,
    [data-testid="stSidebar"] a.ntl-sidebar-link-btn * {
        color: #dce8ff !important;
        -webkit-text-fill-color: #dce8ff !important;
    }
    [data-testid="stSidebar"] .stLinkButton > a:visited,
    [data-testid="stSidebar"] a.ntl-sidebar-link-btn:visited {
        color: #dce8ff !important;
    }
    [data-testid="stSidebar"] .stLinkButton > a:hover,
    [data-testid="stSidebar"] a.ntl-sidebar-link-btn:hover {
        transform: translateY(-1px) !important;
        border-color: rgba(142, 184, 255, 0.72) !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.1), 0 10px 18px rgba(0,0,0,0.3) !important;
        color: #e8f2ff !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] details {
        background: linear-gradient(180deg, rgba(13,22,46,0.90), rgba(8,16,35,0.88)) !important;
        border: 1px solid rgba(121, 161, 255, 0.20) !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.06), 0 6px 14px rgba(0,0,0,0.20) !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] summary p,
    [data-testid="stSidebar"] [data-testid="stExpander"] .stMarkdown p,
    [data-testid="stSidebar"] [data-testid="stExpander"] .stMarkdown span,
    [data-testid="stSidebar"] [data-testid="stExpander"] .stMarkdown li,
    [data-testid="stSidebar"] [data-testid="stExpander"] .stCaption {
        color: #dbe8ff !important;
        -webkit-text-fill-color: #dbe8ff !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] .stMarkdown strong,
    [data-testid="stSidebar"] [data-testid="stExpander"] .stMarkdown b {
        color: #f4f8ff !important;
        -webkit-text-fill-color: #f4f8ff !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] .stButton > button {
        border: 1px solid rgba(96, 146, 255, 0.44) !important;
        background: linear-gradient(180deg, rgba(11, 24, 52, 0.90), rgba(7, 16, 36, 0.92)) !important;
        color: #dce8ff !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] .stButton > button span,
    [data-testid="stSidebar"] [data-testid="stExpander"] .stButton > button p {
        color: #dce8ff !important;
        -webkit-text-fill-color: #dce8ff !important;
    }
    [data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] {
        background: linear-gradient(180deg, rgba(13,22,46,0.86), rgba(8,16,35,0.86)) !important;
        border: 1px dashed rgba(129, 168, 255, 0.50) !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.05) !important;
    }
    [data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] * {
        color: #dbe8ff !important;
        -webkit-text-fill-color: #dbe8ff !important;
    }
    [data-testid="stSidebar"] code {
        color: #d9e7ff !important;
        background: rgba(20, 35, 66, 0.88) !important;
        border: 1px solid rgba(121, 161, 255, 0.35) !important;
    }
    .ntl-thread-status-row {
        display: flex;
        align-items: center;
        justify-content: space-between;
        flex-wrap: wrap;
        column-gap: 0.8rem;
        row-gap: 0.32rem;
        color: #dce8ff;
        font-size: 0.93rem;
        margin-top: 0.2rem;
    }
    .ntl-thread-status-item {
        display: inline-flex;
        align-items: center;
        gap: 0.22rem;
        white-space: nowrap;
    }
    .ntl-thread-status-item:last-child {
        flex-basis: 100%;
    }
    .ntl-thread-status-value {
        color: #f1f6ff;
        font-weight: 600;
    }
    .ntl-status-text.active {
        color: #b4ffe0;
    }
    .ntl-status-text.inactive {
        color: #ffd2d2;
    }
    /* Sidebar harmonization: enforce readable contrast in all nested states */
    [data-testid="stSidebar"] [data-baseweb="select"] * {
        color: #e7f1ff !important;
        -webkit-text-fill-color: #e7f1ff !important;
    }
    [data-testid="stSidebar"] [data-baseweb="select"] p,
    [data-testid="stSidebar"] [data-baseweb="select"] div,
    [data-testid="stSidebar"] [data-baseweb="select"] span,
    [data-testid="stSidebar"] [data-baseweb="select"] input {
        color: #e7f1ff !important;
        -webkit-text-fill-color: #e7f1ff !important;
    }
    [data-testid="stSidebar"] [data-baseweb="select"] div[role="combobox"] {
        align-items: center !important;
    }
    [data-testid="stSidebar"] [data-baseweb="select"] input {
        caret-color: transparent !important;
        width: 0 !important;
        min-width: 0 !important;
        padding: 0 !important;
        margin: 0 !important;
        opacity: 0 !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] details,
    [data-testid="stSidebar"] [data-testid="stExpander"] details * {
        color: #dbe8ff !important;
        -webkit-text-fill-color: #dbe8ff !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] summary,
    [data-testid="stSidebar"] [data-testid="stExpander"] details > summary,
    [data-testid="stSidebar"] [data-testid="stExpander"] details[open] > summary {
        background: linear-gradient(180deg, rgba(16, 30, 62, 0.92), rgba(9, 18, 40, 0.92)) !important;
        color: #dbe8ff !important;
        -webkit-text-fill-color: #dbe8ff !important;
        border-bottom: 1px solid rgba(121, 161, 255, 0.30) !important;
        border-radius: 8px !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] details[open] > summary {
        border-bottom-left-radius: 0 !important;
        border-bottom-right-radius: 0 !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] summary p,
    [data-testid="stSidebar"] [data-testid="stExpander"] summary span,
    [data-testid="stSidebar"] [data-testid="stExpander"] summary svg {
        color: #dbe8ff !important;
        fill: #dbe8ff !important;
        -webkit-text-fill-color: #dbe8ff !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] .stCaption,
    [data-testid="stSidebar"] [data-testid="stExpander"] .stCaption * {
        color: #b8cbec !important;
        -webkit-text-fill-color: #b8cbec !important;
    }
    [data-testid="stSidebar"] [data-testid="stExpander"] .stButton > button:hover {
        border-color: rgba(142, 184, 255, 0.72) !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.1), 0 10px 18px rgba(0,0,0,0.3) !important;
    }
    [data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] button {
        border: 1px solid rgba(132, 168, 255, 0.45) !important;
        background: linear-gradient(180deg, rgba(14, 28, 58, 0.88), rgba(7, 16, 36, 0.90)) !important;
        color: #dce8ff !important;
    }
    [data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] button span {
        color: #dce8ff !important;
        -webkit-text-fill-color: #dce8ff !important;
    }
    @media (max-width: 768px) {
        .block-container {
            padding: 4.25rem 1rem 7rem 1rem !important;
        }
        [data-testid="stSidebar"] {
            width: min(82vw, 300px) !important;
            min-width: min(82vw, 300px) !important;
            max-width: min(82vw, 300px) !important;
        }
        [data-testid="stSidebar"][aria-expanded="false"] {
            transform: translateX(calc(-100% + 52px)) !important;
            background: transparent !important;
            border-right: 0 !important;
            box-shadow: none !important;
        }
        [data-testid="stSidebar"][aria-expanded="true"] {
            transform: none !important;
        }
        [data-testid="stSidebar"][aria-expanded="false"] [data-testid="stSidebarContent"] {
            background: transparent !important;
            border-right: 0 !important;
            box-shadow: none !important;
        }
        [data-testid="stSidebar"][aria-expanded="false"] [data-testid="stSidebarUserContent"] {
            display: none !important;
        }
        [data-testid="stSidebar"] [data-testid="stBaseButton-headerNoPadding"] {
            position: relative !important;
            z-index: 1201 !important;
            transform: translateX(30px) !important;
            width: 2.5rem !important;
            height: 2.5rem !important;
            border: 1px solid rgba(149, 176, 255, 0.46) !important;
            border-radius: 8px !important;
            background: rgba(10, 18, 40, 0.94) !important;
            color: #eaf1ff !important;
            box-shadow: 0 8px 20px rgba(0, 0, 0, 0.28) !important;
        }
        [data-testid="stSidebar"][aria-expanded="false"]
        [data-testid="stBaseButton-headerNoPadding"] [data-testid="stIconMaterial"] {
            transform: rotate(180deg) !important;
        }
        [data-testid="stSidebarCollapsedControl"] {
            top: 0.8rem !important;
            left: 0.8rem !important;
        }
        .st-key-ui_lang_switch [role="radiogroup"] {
            justify-content: flex-start !important;
        }
    }
    .ntl-step .streamlit-expanderHeader {
        font-weight: 600;
    }
    </style>
    """
    st.markdown(css.replace("__BG__", bg_css), unsafe_allow_html=True)
def scroll_to_bottom():
    """Inject JS to scroll containers to the bottom."""
    js = f"""
    <script>
        function findMainPanels(doc) {{
            var blocks = Array.from(doc.querySelectorAll('div[data-testid="stVerticalBlock"][overflow="auto"]'));
            blocks = blocks.filter(function(el) {{
                var r = el.getBoundingClientRect();
                if (!r || r.width <= 280 || r.height <= 200) return false;
                if (el.closest('section[data-testid="stSidebar"]')) return false;
                return true;
            }});
            blocks.sort(function(a, b) {{
                return a.getBoundingClientRect().left - b.getBoundingClientRect().left;
            }});
            return blocks;
        }}
        function styleMainPanels(doc) {{
            var panels = findMainPanels(doc);
            for (var i = 0; i < panels.length; i++) {{
                var panel = panels[i];
                panel.style.border = '1.2px solid rgba(255,255,255,0.52)';
                panel.style.borderRadius = '12px';
                panel.style.boxShadow = 'inset 0 0 0 1px rgba(255,255,255,0.08)';
            }}
            return panels;
        }}
        function scroll() {{
            var doc = window.parent.document;
            var panels = findMainPanels(doc);
            if (panels.length > 0) {{
                panels[0].scrollTop = panels[0].scrollHeight;
            }}
        }}
        function alignChatInput() {{
            var doc = window.parent.document;
            function styleMultimodalFrame(frame) {{
                try {{
                    if (!frame) return;
                    var host = frame.closest('div[data-testid="stElementContainer"]') || frame.parentElement;
                    if (host) {{
                        host.style.setProperty('background', 'transparent', 'important');
                        host.style.setProperty('border', 'none', 'important');
                        host.style.setProperty('box-shadow', 'none', 'important');
                        host.style.setProperty('outline', 'none', 'important');
                        host.style.setProperty('padding', '0px', 'important');
                        host.style.setProperty('border-radius', '0px', 'important');
                    }}
                    var parent = frame.parentElement;
                    if (parent) {{
                        parent.style.setProperty('background', 'transparent', 'important');
                        parent.style.setProperty('border', 'none', 'important');
                        parent.style.setProperty('box-shadow', 'none', 'important');
                        parent.style.setProperty('outline', 'none', 'important');
                        parent.style.setProperty('padding', '0px', 'important');
                    }}
                    frame.style.setProperty('background', 'transparent', 'important');
                    frame.style.setProperty('border', 'none', 'important');
                    frame.style.setProperty('border-radius', '0px', 'important');
                    frame.style.setProperty('box-shadow', 'none', 'important');
                    frame.style.setProperty('outline', 'none', 'important');
                    // Do not force a small fixed iframe height. Let component expand
                    // when file chips/preview rows are present, otherwise typing/sending
                    // can be clipped after paste.
                    frame.style.setProperty('min-height', '96px', 'important');
                    frame.style.setProperty('height', 'auto', 'important');
                    frame.style.setProperty('overflow', 'visible', 'important');
                    if (host) {{
                        host.style.setProperty('overflow', 'visible', 'important');
                    }}
                    if (parent) {{
                        parent.style.setProperty('overflow', 'visible', 'important');
                    }}
                    var idoc = frame.contentDocument || (frame.contentWindow && frame.contentWindow.document);
                    if (!idoc) return;
                    if (!idoc.getElementById('__ntl_mm_style')) {{
                        var st = idoc.createElement('style');
                        st.id = '__ntl_mm_style';
                        st.textContent = `
                            html, body, #root {{
                                background: transparent !important;
                                margin: 0 !important;
                                padding: 0 !important;
                            }}
                            #root > div {{
                                background: transparent !important;
                                padding: 0 !important;
                            }}
                            #root > div > div {{
                                background-color: rgba(15, 24, 48, 0.86) !important;
                                border: 1.2px solid rgba(149, 176, 255, 0.40) !important;
                                border-radius: 999px !important;
                                box-shadow: inset 0 0 0 1px rgba(255,255,255,0.06) !important;
                                min-height: 56px !important;
                            }}
                            input, textarea {{
                                color: #e8edf8 !important;
                            }}
                            textarea {{
                                background: transparent !important;
                                border: none !important;
                                outline: none !important;
                            }}
                            textarea::placeholder {{
                                color: rgba(226, 236, 255, 0.58) !important;
                            }}
                            button {{
                                color: #b7cbff !important;
                                background: transparent !important;
                                border-radius: 999px !important;
                            }}
                        `;
                        idoc.head.appendChild(st);
                    }}
                }} catch (e) {{}}
            }}

            var nativeInput = doc.querySelector('div[data-testid="stChatInput"]');
            var mmFrame = null;
            var mmHost = null;
            if (!nativeInput) {{
                var frames = Array.from(doc.querySelectorAll('iframe'));
                mmFrame = frames.find(function(fr) {{
                    var t = String(fr.getAttribute('title') || '').toLowerCase();
                    return t.indexOf('st_chat_input_multimodal') >= 0;
                }});
                if (mmFrame) {{
                    mmHost = mmFrame.closest('div[data-testid="stElementContainer"]') || mmFrame.parentElement || mmFrame;
                }}
            }}

            var input = nativeInput || mmHost;
            if (!input) return;
            var panels = styleMainPanels(doc);
            if (!panels.length) return;
            var chatPanel = panels.reduce(function(best, panel) {{
                if (!best) return panel;
                var rb = best.getBoundingClientRect();
                var rp = panel.getBoundingClientRect();
                if (rp.width > rb.width + 8) return panel;
                if (Math.abs(rp.width - rb.width) <= 8 && rp.left < rb.left) return panel;
                return best;
            }}, null);
            var chatRect = chatPanel.getBoundingClientRect();
            if (!chatRect || chatRect.width < 220 || chatRect.height < 120) return;

            var viewportW = window.parent.innerWidth || doc.documentElement.clientWidth || 1200;
            var sidebar = doc.querySelector('section[data-testid="stSidebar"]');
            var sidebarRight = 0;
            if (sidebar) {{
                var sb = sidebar.getBoundingClientRect();
                if (sb && sb.width > 40) sidebarRight = sb.right;
            }}
            var mobileSidebarOpen = viewportW <= 768 && sidebar
                && sidebar.getAttribute('aria-expanded') === 'true';
            if (mobileSidebarOpen) {{
                input.style.setProperty('visibility', 'hidden', 'important');
                input.style.setProperty('pointer-events', 'none', 'important');
                return;
            }}
            input.style.setProperty('visibility', 'visible', 'important');
            input.style.setProperty('pointer-events', 'auto', 'important');
            var minLeft = Math.max(12, Math.round(sidebarRight + 12));

            var left = Math.max(minLeft, Math.round(chatRect.left + 10));
            var width = Math.round(chatRect.width - 20);
            width = Math.max(280, Math.min(width, viewportW - left - 12));
            input.style.setProperty('position', 'fixed', 'important');
            input.style.setProperty('left', left + 'px', 'important');
            input.style.setProperty('width', width + 'px', 'important');
            input.style.setProperty('max-width', width + 'px', 'important');
            input.style.setProperty('top', 'auto', 'important');
            input.style.setProperty('bottom', '12px', 'important');
            input.style.setProperty('right', 'auto', 'important');
            input.style.setProperty('height', 'auto', 'important');
            input.style.setProperty('min-height', '0px', 'important');
            input.style.setProperty('transform', 'none', 'important');
            input.style.setProperty('z-index', '900', 'important');
            input.style.setProperty('opacity', '1', 'important');
            if (mmFrame) {{
                input.style.setProperty('background', 'transparent', 'important');
                input.style.setProperty('border', 'none', 'important');
                input.style.setProperty('border-radius', '0px', 'important');
                input.style.setProperty('backdrop-filter', 'none', 'important');
                input.style.setProperty('padding', '0px', 'important');
                input.style.setProperty('box-shadow', 'none', 'important');
                styleMultimodalFrame(mmFrame);
            }}
        }}
        function initAlignChatInput() {{
            alignChatInput();
            if (!window.parent.__ntlChatInputBound) {{
                window.parent.addEventListener('resize', alignChatInput);
                window.parent.addEventListener('scroll', alignChatInput, true);
                window.parent.__ntlChatInputBound = true;
            }}
            var parentDoc = window.parent.document;
            var sidebar = parentDoc.querySelector('section[data-testid="stSidebar"]');
            var existingObserver = window.parent.__ntlSidebarAlignObserver;
            if (sidebar && (!existingObserver || existingObserver.sidebar !== sidebar)) {{
                if (existingObserver && existingObserver.observer) {{
                    existingObserver.observer.disconnect();
                }}
                var observer = new MutationObserver(function(mutations) {{
                    var changed = mutations.some(function(item) {{
                        return item.type === 'attributes' && item.attributeName === 'aria-expanded';
                    }});
                    if (changed) {{
                        [40, 220, 420].forEach(function(delay) {{
                            setTimeout(alignChatInput, delay);
                        }});
                    }}
                }});
                observer.observe(sidebar, {{ attributes: true, attributeFilter: ['aria-expanded'] }});
                window.parent.__ntlSidebarAlignObserver = {{ sidebar: sidebar, observer: observer }};
            }}
        }}
        setTimeout(scroll, 300);
        setTimeout(initAlignChatInput, 40);
        setTimeout(alignChatInput, 400);
        setTimeout(alignChatInput, 900);
    </script>
    """
    _render_dom_script(js, height=0)

# ==============================================================================
# SECTION C: UI Rendering Helpers (Reusable)
# ==============================================================================

def _extract_json(s: str):
    """Extract JSON from mixed text and strip JS-style comments."""
    if not isinstance(s, str): return None, s
    for open_ch, close_ch in [('{','}'), ('[',']')]:
        stack, start = 0, None
        for i, ch in enumerate(s):
            if ch == open_ch:
                if stack == 0: start = i
                stack += 1
            elif ch == close_ch and stack > 0:
                stack -= 1
                if stack == 0 and start is not None:
                    frag = s[start:i+1]
                    try:
                        clean_frag = re.sub(r'(?<!:)\/\/.*', '', frag)
                        return json.loads(clean_frag), (s[:start] + s[i+1:]).strip()
                    except Exception: 
                        pass
    return None, s


def _extract_all_json_chunks(s: str):
    """Extract consecutive JSON chunks from text like '{}{}' or '{}\\n{}'."""
    if not isinstance(s, str):
        return [], s
    chunks = []
    rest = s
    for _ in range(64):
        obj, new_rest = _extract_json(rest)
        if obj is None:
            break
        chunks.append(obj)
        if not isinstance(new_rest, str) or new_rest == rest:
            rest = new_rest if isinstance(new_rest, str) else ""
            break
        rest = new_rest
    return chunks, rest


def _parse_literature_records_from_text(text: str) -> list[dict]:
    """Best-effort parser for plain literature outputs when JSON is unavailable."""
    if not isinstance(text, str) or not text.strip():
        return []
    blocks = [b.strip() for b in re.split(r"(?=\\bTitle\\s*:)", text) if b.strip()]
    records = []
    for block in blocks:
        title = ""
        year = ""
        source = ""
        chunk = ""
        m_title = re.search(r"Title\\s*:\\s*(.+?)(?=\\s+Year\\s*:|\\s+Source\\s*:|\\s+Chunk\\s*:|$)", block, flags=re.IGNORECASE | re.DOTALL)
        if m_title:
            title = m_title.group(1).strip()
        m_year = re.search(r"Year\\s*:\\s*([0-9]{4})", block, flags=re.IGNORECASE)
        if m_year:
            year = m_year.group(1).strip()
        m_source = re.search(r"Source\\s*:\\s*(.+?)(?=\\s+Chunk\\s*:|$)", block, flags=re.IGNORECASE | re.DOTALL)
        if m_source:
            source = m_source.group(1).strip()
        m_chunk = re.search(r"Chunk\\s*:\\s*(.+)$", block, flags=re.IGNORECASE | re.DOTALL)
        if m_chunk:
            chunk = m_chunk.group(1).strip()
        if not any([title, year, source, chunk]):
            continue
        rec = {}
        if title:
            rec["title"] = title
        if year:
            rec["year"] = year
        if source:
            rec["source"] = source
        if chunk:
            rec["chunk"] = chunk
        records.append(rec)
    return records


def render_kb_tool_output(raw_content, tool_name: str = ""):
    """Render KB tool outputs robustly (single JSON, multi-JSON, or plain literature text)."""
    if isinstance(raw_content, (dict, list)):
        st.json(_sanitize_paths_in_obj(raw_content))
        return
    text = str(raw_content or "")
    if not text.strip():
        return

    parsed = None
    try:
        parsed = json.loads(text)
    except Exception:
        parsed = None
    if parsed is not None:
        st.json(_sanitize_paths_in_obj(parsed))
        return

    chunks, rest = _extract_all_json_chunks(text)
    if chunks:
        if len(chunks) == 1 and (not isinstance(rest, str) or not rest.strip()):
            st.json(_sanitize_paths_in_obj(chunks[0]))
        else:
            st.json(_sanitize_paths_in_obj(chunks))
            if isinstance(rest, str) and rest.strip():
                st.caption(_sanitize_paths_in_text(rest))
        return

    tool_name_norm = str(tool_name or "").strip().lower()
    if "literature_knowledge" in tool_name_norm:
        records = _parse_literature_records_from_text(text)
        if records:
            st.json(_sanitize_paths_in_obj(records))
            return

    st.write(_sanitize_paths_in_text(text))


def _normalize_kb_payload(data: dict) -> dict:
    """Normalize heterogeneous KB payloads into a render-friendly schema."""
    if not isinstance(data, dict):
        return {}

    normalized = dict(data)
    schema = str(normalized.get("schema") or "").strip().lower()
    workflow = normalized.get("workflow")
    if isinstance(workflow, dict):
        for key in ("task_id", "task_name", "category", "description", "steps", "output", "result", "sources"):
            if key not in normalized and key in workflow:
                normalized[key] = workflow.get(key)

    if schema == "ntl.kb.response.v2":
        normalized["mode"] = normalized.get("mode") or ""
        if not isinstance(normalized.get("intent"), dict):
            normalized["intent"] = {}
        intent = normalized.get("intent") if isinstance(normalized.get("intent"), dict) else {}
        if "proposed_task_level" not in normalized and intent.get("proposed_task_level"):
            normalized["proposed_task_level"] = intent.get("proposed_task_level")
        if "task_level_reason_codes" not in normalized and isinstance(intent.get("task_level_reason_codes"), list):
            normalized["task_level_reason_codes"] = intent.get("task_level_reason_codes")
        if "task_level_confidence" not in normalized and intent.get("task_level_confidence") is not None:
            normalized["task_level_confidence"] = intent.get("task_level_confidence")
        if "message" not in normalized and normalized.get("reason"):
            normalized["message"] = normalized.get("reason")
        if "reason" not in normalized and normalized.get("message"):
            normalized["reason"] = normalized.get("message")
    elif schema == "ntl.kb.subagent.response.v1":
        intent_block = normalized.get("intent_analysis") if isinstance(normalized.get("intent_analysis"), dict) else {}
        response_block = normalized.get("response") if isinstance(normalized.get("response"), dict) else {}
        for key in ("task_id", "task_name", "category", "description", "steps", "output", "result"):
            if key not in normalized and key in response_block:
                normalized[key] = response_block.get(key)
        if "proposed_task_level" not in normalized and intent_block.get("proposed_task_level"):
            normalized["proposed_task_level"] = intent_block.get("proposed_task_level")
        if "task_level_reason_codes" not in normalized and isinstance(intent_block.get("task_level_reason_codes"), list):
            normalized["task_level_reason_codes"] = intent_block.get("task_level_reason_codes")
        if "task_level_confidence" not in normalized and intent_block.get("task_level_confidence") is not None:
            normalized["task_level_confidence"] = intent_block.get("task_level_confidence")
        if "mode" not in normalized:
            normalized["mode"] = "workflow"

    task_name = normalized.get("task_name") or normalized.get("task") or normalized.get("title") or normalized.get("task_id")
    if task_name:
        normalized["task_name"] = task_name

    normalized["category"] = normalized.get("category") or normalized.get("type") or ""
    if "output" not in normalized and "result" in normalized:
        normalized["output"] = normalized.get("result")

    steps_raw = normalized.get("steps")
    if steps_raw is None and isinstance(workflow, dict):
        steps_raw = workflow.get("steps")

    if isinstance(steps_raw, dict):
        try:
            keys = sorted(steps_raw.keys(), key=lambda x: int(str(x)) if str(x).isdigit() else str(x))
        except Exception:
            keys = list(steps_raw.keys())
        steps = [steps_raw[k] for k in keys]
    elif isinstance(steps_raw, list):
        steps = steps_raw
    else:
        steps = []

    normalized_steps = []
    for step in steps:
        if isinstance(step, dict):
            normalized_steps.append(step)
        elif step is not None:
            normalized_steps.append({"type": "note", "name": str(step)})
    normalized["steps"] = normalized_steps
    return normalized


def _strip_legacy_stream_marker(content):
    """Remove legacy inline streaming headings from historical text payloads."""
    if not isinstance(content, str):
        return content
    patterns = [
        r"^\s*\*{0,2}\s*Data_Searcher\s*\(streaming\)\s*\*{0,2}\s*:?\s*\n?",
        r"^\s*\*{0,2}\s*Code_Assistant\s*\(streaming\)\s*\*{0,2}\s*:?\s*\n?",
    ]
    text = content
    for p in patterns:
        text = re.sub(p, "", text, flags=re.IGNORECASE | re.MULTILINE)
    return text.strip()

def _render_popover(title: str):
    """Render popover if available, fallback to expander."""
    try:
        return st.popover(title)
    except Exception:
        return st.expander(title, expanded=False)

def render_label_human(msg):
    st.write(USER_TEMPLATE.replace("{{MSG}}", msg), unsafe_allow_html=True)

def render_bot_message(msg):
    st.write(BOT_TEMPLATE.replace("{{MSG}}", msg), unsafe_allow_html=True)

def render_label_ai(agent_name):
    color = "#0b5cab" if agent_name.lower() == "code_assistant" else "#0f766e"
    st.markdown(f"<div style='color:{color};font-weight:700;font-size:16px;'>AI: {agent_name}</div>", unsafe_allow_html=True)

def render_label_tool(tool_name):
    st.markdown(f"<div style='color:#0b5cab;font-weight:700;font-size:15px;'>Tool ({tool_name}) Output:</div>", unsafe_allow_html=True)

def render_label_function(tool_name):
    st.markdown(f"<div style='color:#8a6f00;font-weight:700;font-size:15px;'>Function Call to `{tool_name}`:</div>", unsafe_allow_html=True)

def render_divider():
    st.markdown("<hr style='margin: 15px 0; border: 1px dashed #ccc;'>", unsafe_allow_html=True)

def render_event_header(index):
    st.markdown(f"""
    <div style="border: 1px solid #ccc; border-radius: 4px; padding: 10px; margin: 12px 0; background-color: #f8f9fa;">
        <div style="display: flex; align-items: center; gap: 6px;">
            <span style="font-size: 18px; line-height: 1;">#</span>
            <span style="color: #4a6fa5; font-size: 18px; font-weight: 600;">{_tr('推理事件', 'Reasoning Event')} {index}</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

def _human_message_display_label(content) -> str:
    """Distinguish end-user input from supervisor-to-subagent handoffs."""
    text = _normalize_content_to_text(content)
    match = re.search(
        r"\byou\s+are\s+the\s+([A-Za-z][A-Za-z0-9_-]*)\s+agent\b",
        str(text or ""),
        flags=re.IGNORECASE,
    )
    if match:
        target = _normalize_subagent_name(match.group(1)) or match.group(1)
        return _tr(
            f"任务交接 → {_display_agent_label(target)}",
            f"Task Handoff → {_display_agent_label(target)}",
        )
    return _tr("用户输入", "User")


def render_event_human(content):
    label = _human_message_display_label(content)
    st.markdown(
        f"<div class='ntl-reasoning-role'>{label}</div>",
        unsafe_allow_html=True,
    )
    text = _normalize_content_to_text(content)
    st.markdown(_sanitize_paths_in_text(str(text or "")))


_CHAT_INPUT_FILE_TYPES = [
    "png",
    "jpg",
    "jpeg",
    "webp",
    "bmp",
    "tif",
    "tiff",
    "pdf",
]


def _extract_chat_input_text_and_files(chat_value):
    """Normalize st.chat_input return value to (text, files)."""
    if chat_value is None:
        return "", []
    if isinstance(chat_value, str):
        return chat_value.strip(), []

    text = ""
    files = []
    if isinstance(chat_value, dict):
        text = str(chat_value.get("text", "") or "").strip()
        files = list(chat_value.get("files", []) or [])
    else:
        text = str(getattr(chat_value, "text", "") or "").strip()
        files = list(getattr(chat_value, "files", []) or [])
    return text, files


def _next_available_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    idx = 1
    while True:
        candidate = parent / f"{stem}_{idx}{suffix}"
        if not candidate.exists():
            return candidate
        idx += 1


def _decode_chat_file_bytes(file_obj) -> bytes:
    if hasattr(file_obj, "getbuffer"):
        return bytes(file_obj.getbuffer())
    if isinstance(file_obj, dict):
        raw = str(file_obj.get("data", "") or "").strip()
        if raw:
            if "," in raw and raw.lower().startswith("data:"):
                raw = raw.split(",", 1)[1]
            return base64.b64decode(raw)
    raise ValueError("Unsupported chat file payload")


def _save_chat_input_files_to_workspace(files, thread_id: str) -> dict:
    workspace = storage_manager.get_workspace(thread_id)
    input_dir = workspace / "inputs"
    input_dir.mkdir(parents=True, exist_ok=True)

    saved = []
    errors = []
    for uploaded_file in files or []:
        if isinstance(uploaded_file, dict):
            name = os.path.basename(str(uploaded_file.get("name", "") or "")).strip()
        else:
            name = os.path.basename(str(getattr(uploaded_file, "name", "") or "")).strip()
        if not name:
            continue
        target = _next_available_path(input_dir / name)
        try:
            with open(target, "wb") as f:
                f.write(_decode_chat_file_bytes(uploaded_file))
            saved.append(target.name)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{name}: {exc}")

    return {"saved": saved, "errors": errors}


def get_user_input(*, disabled: bool = False):
    placeholder = _tr(
        "Query",
        "Query",
    )
    use_custom_mm = str(os.getenv("NTL_USE_CUSTOM_MULTIMODAL_CHAT_INPUT", "0")).strip().lower() in {"1", "true", "yes", "on"}
    force_native = str(os.getenv("NTL_FORCE_NATIVE_CHAT_INPUT", "1")).strip().lower() in {"1", "true", "yes", "on"}
    if (
        use_custom_mm
        and not force_native
        and _MULTIMODAL_CHAT_INPUT_IMPORT_ERROR is not None
        and not st.session_state.get("_ntl_mm_input_error_shown")
    ):
        st.session_state["_ntl_mm_input_error_shown"] = True
        st.sidebar.warning(
            _tr(
                "多模态输入组件未加载，已临时使用原生输入框。请在当前 conda 环境安装 st-chat-input-multimodal。",
                "Multimodal input component failed to load; using native input temporarily. Install st-chat-input-multimodal in the active conda environment.",
            )
        )
    if use_custom_mm and multimodal_chat_input is not None and not force_native and not disabled:
        return multimodal_chat_input(
            placeholder=placeholder,
            accepted_file_types=_CHAT_INPUT_FILE_TYPES,
            max_file_size_mb=200,
            enable_voice_input=False,
            key="main_chat_input_mm",
        )
    return st.chat_input(
        placeholder,
        accept_file="multiple",
        file_type=_CHAT_INPUT_FILE_TYPES,
        key="main_chat_input",
        disabled=bool(disabled),
    )


def _is_current_thread_running() -> bool:
    current_tid = str(st.session_state.get("thread_id") or "")
    active_tid = str(st.session_state.get("active_run_thread_id") or "")
    return bool(st.session_state.get("is_running", False)) and bool(current_tid) and (current_tid == active_tid)


def _is_current_thread_stopping() -> bool:
    current_tid = str(st.session_state.get("thread_id") or "")
    active_tid = str(st.session_state.get("active_run_thread_id") or "")
    return bool(st.session_state.get("stopping", False)) and bool(current_tid) and (current_tid == active_tid)

# ==============================================================================
# SECTION D: Searcher/KB Output Rendering
# ==============================================================================

def render_data_searcher_output(raw_content):
    """Render structured Data Searcher output."""
    data, rest = (None, raw_content)
    if isinstance(raw_content, dict): 
        data, rest = raw_content, ""
    elif isinstance(raw_content, str): 
        data, rest = _extract_json(raw_content)

    if not isinstance(data, dict):
        if isinstance(rest, str) and rest.strip(): st.write(rest)
        return

    # SCHEMA B: News / Events
    if "event_overview" in data and isinstance(data["event_overview"], dict):
        ov = data["event_overview"]
        ov = _sanitize_paths_in_obj(ov)
        st.markdown("### Event Overview")
        st.markdown(f"- **Title**: {ov.get('Title','-')}")
        st.markdown(f"- **Event Time (UTC)**: {ov.get('Event_time_utc','-')}")
        st.markdown(f"- **Location**: {ov.get('Location','-')}")
        st.markdown(f"- **Magnitude / Scale**: {ov.get('Magnitude_or_scale', '-')}")
        
        if ov.get("Event_details"): 
            st.markdown("**Detailed Information**")
            st.write(ov["Event_details"])
        if ov.get("Summary"): 
            st.markdown("**Summary**")
            st.info(ov["Summary"])

        srcs = data.get("sources", [])
        if isinstance(srcs, dict): srcs = [srcs]
        srcs = _sanitize_paths_in_obj(srcs)
        if srcs:
            st.markdown("### Reliable Sources")
            for i, s in enumerate(srcs, 1):
                with st.container(border=True):
                    st.markdown(f"**{i}. {s.get('Publisher','-')}** | {s.get('Domain','')}")
                    st.markdown(f"- **Title**: {s.get('Title','-')}")
                    if s.get("URL"): st.markdown(f"- **URL**: [{s['URL']}]({s['URL']})")
                    if s.get("Snippet"): st.caption(s["Snippet"])

        with _render_popover("View Raw JSON"): st.json(_sanitize_paths_in_obj(data))
        if isinstance(rest, str) and rest.strip(): st.write(_sanitize_paths_in_text(rest))
        return

    # Non-contract fallback: avoid rendering blank geospatial cards for guardrail/runtime payloads.
    retrieval_schema = str(data.get("schema", "")).strip()
    looks_like_geospatial_contract = (
        retrieval_schema == "ntl.retrieval.contract.v1"
        or any(
            key in data
            for key in (
                "Data_source",
                "Product",
                "Temporal_coverage",
                "Spatial_coverage",
                "Storage_location",
                "Files_name",
                "Auxiliary_data",
                "GEE_execution_plan",
                "Boundary_validation",
            )
        )
    )
    if not looks_like_geospatial_contract:
        status = str(data.get("status", "")).strip() or "unknown"
        reason = str(
            data.get("reason")
            or data.get("message")
            or data.get("guidance")
            or ""
        ).strip()
        st.warning(f"Data Searcher ({status})")
        if reason:
            st.write(_sanitize_paths_in_text(reason))
        with _render_popover("View Raw JSON"): st.json(_sanitize_paths_in_obj(data))
        if isinstance(rest, str) and rest.strip():
            st.write(_sanitize_paths_in_text(rest))
        return

    # SCHEMA A: Geospatial Data Retrieval
    Data_source        = data.get("Data_source", "")
    Product            = data.get("Product", "")
    Temporal_coverage  = data.get("Temporal_coverage", "")
    Spatial_coverage   = data.get("Spatial_coverage", "")
    Spatial_resolution = data.get("Spatial_resolution", "")
    Files_name         = data.get("Files_name", [])
    Storage_location   = data.get("Storage_location", "")
    Auxiliary_data     = data.get("Auxiliary_data", [])

    if isinstance(Data_source, list):
        Data_source = ", ".join(str(x) for x in Data_source if x)

    if isinstance(Files_name, str): Files_name = [Files_name]
    if isinstance(Auxiliary_data, dict): Auxiliary_data = [Auxiliary_data]
    if not isinstance(Auxiliary_data, list): Auxiliary_data = []
    Storage_location = _sanitize_paths_in_text(Storage_location)

    st.markdown("### Geospatial Data Acquisition")
    cols = st.columns(2)
    with cols[0]:
        st.markdown("**Data Source**"); st.write(Data_source or "-")
        st.markdown("**Temporal Coverage**"); st.write(Temporal_coverage or "-")
        st.markdown("**Product Identifier**"); st.code(Product or "-", language="text")
    with cols[1]:
        st.markdown("**Spatial Coverage**"); st.write(Spatial_coverage or "-")
        st.markdown("**Spatial Resolution**"); st.write(Spatial_resolution or "-")
        st.markdown("**Storage Location**"); st.code(Storage_location or "Local Workspace (inputs/)")

    if Files_name:
        st.markdown("**Generated Output Files**")
        for p in Files_name: 
            # Ensure we only show the filename, avoiding double paths if agent returns full path
            fname = os.path.basename(str(p))
            st.markdown(f"- `inputs/{fname}`")

    if Auxiliary_data:
        st.markdown("### Auxiliary / Socio-economic Data")
        for i, item in enumerate(Auxiliary_data, 1):
            if not isinstance(item, dict):
                st.markdown(f"- {item}")
                continue
            with st.container(border=True):
                st.markdown(f"**{i}. {item.get('Data_type', 'Auxiliary Data')}**")
                c1, c2 = st.columns(2)
                with c1:
                    st.markdown("**Source**"); st.write(item.get("Source", "-"))
                    st.markdown("**Product**"); st.write(item.get("Product", "-"))
                    st.markdown("**Temporal Coverage**"); st.write(item.get("Temporal_coverage", "-"))
                with c2:
                    st.markdown("**Spatial Coverage**"); st.write(item.get("Spatial_coverage", "-"))
                    aux_files = item.get("Files_name", [])
                    if isinstance(aux_files, str):
                        aux_files = [aux_files]
                    st.markdown("**Files**")
                    if aux_files:
                        for fp in aux_files:
                            st.markdown(f"- `inputs/{os.path.basename(str(fp))}`")
                    else:
                        st.write("-")
                if item.get("Notes"):
                    st.caption(_sanitize_paths_in_text(str(item.get("Notes"))))

    with _render_popover("View Raw JSON"): st.json(_sanitize_paths_in_obj(data))
    if isinstance(rest, str) and rest.strip(): st.write(_sanitize_paths_in_text(rest))


def render_kb_output(kb_content):
    """Render NTL knowledge-base output."""
    data, rest = None, kb_content
    if isinstance(kb_content, dict): data, rest = kb_content, ""
    elif isinstance(kb_content, str): data, rest = _extract_json(kb_content)

    if not isinstance(data, dict):
        if isinstance(rest, str) and rest.strip(): st.write(rest)
        return

    normalized = _normalize_kb_payload(data)
    status = str(normalized.get("status") or "").strip().lower()
    if status in {"no_valid_tool", "empty_store", "code_corpus_unavailable", "error"}:
        reason = normalized.get("reason") or normalized.get("message") or "No details provided."
        st.warning(f"Knowledge base ({status}): {_sanitize_paths_in_text(reason)}")
        if normalized.get("sources"):
            with _render_popover("Sources"): st.json(_sanitize_paths_in_obj(normalized["sources"]))
        with _render_popover("Raw JSON"): st.json(_sanitize_paths_in_obj(data))
        if isinstance(rest, str) and rest.strip(): st.write(_sanitize_paths_in_text(rest))
        return

    if (
        not normalized.get("steps")
        and not normalized.get("description")
        and not normalized.get("output")
        and (normalized.get("reason") or normalized.get("message"))
    ):
        st.warning(_sanitize_paths_in_text(f"Knowledge base: {normalized.get('reason') or normalized.get('message')}"))
        if normalized.get("sources"):
            with _render_popover("Sources"): st.json(_sanitize_paths_in_obj(normalized["sources"]))
        with _render_popover("Raw JSON"): st.json(_sanitize_paths_in_obj(data))
        if isinstance(rest, str) and rest.strip(): st.write(_sanitize_paths_in_text(rest))
        return

    task = normalized.get("task_name") or normalized.get("task_id") or "Knowledge Base Task"
    category = normalized.get("category", "")
    mode = normalized.get("mode", "")
    schema = normalized.get("schema", "")
    st.markdown(f"""
        <div style="border:1px solid #e3e7ef;border-radius:8px;padding:14px;background:#f9fbff">
          <div style="font-size:18px;font-weight:700;color:#2b4a8b;">task: {task}</div>
          <div style="margin-top:4px;color:#5f6b7a;">category: {category}</div>
          <div style="margin-top:2px;color:#6b7280;font-size:0.88rem;">mode: {mode or '-'} | schema: {schema or '-'}</div>
        </div>
        """, unsafe_allow_html=True)

    proposed_level = str(normalized.get("proposed_task_level") or "").strip()
    reason_codes = normalized.get("task_level_reason_codes")
    if not isinstance(reason_codes, list):
        reason_codes = []
    confidence = normalized.get("task_level_confidence")
    if proposed_level:
        confidence_text = "-"
        try:
            confidence_text = f"{float(confidence):.2f}"
        except Exception:
            confidence_text = "-"
        reason_text = ", ".join(str(code) for code in reason_codes) if reason_codes else "-"
        st.markdown(
            f"**KB preliminary level**: `{proposed_level}`  |  **reason codes**: `{reason_text}`  |  **confidence**: `{confidence_text}`"
        )

    if normalized.get("description"):
        st.markdown("**Description**")
        st.write(normalized["description"])

    steps = normalized.get("steps") or []

    if steps:
        st.markdown("**Steps**")
        for i, step in enumerate(steps, 1):
            typ, name = step.get("type", ""), step.get("name", "")
            note, desc = step.get("note", ""), step.get("description", "")
            st.markdown(f"- **{i}. {name or typ}**")
            
            has_input = "input" in step and step["input"]
            main_info = note or desc
            
            if has_input:
                with _render_popover(main_info or "Step Details"): st.json(_sanitize_paths_in_obj(step["input"]))
            elif main_info:
                st.markdown(
                    f"&nbsp;&nbsp;&nbsp;&nbsp;<span style='color:gray; font-size:0.95em;'>{_sanitize_paths_in_text(main_info)}</span>",
                    unsafe_allow_html=True,
                )

            if typ in ("geospatial_code", "code"):
                code_text = step.get("code") or step.get("Code_description")
                if code_text:
                    lang = (step.get("language") or "python").lower()
                    with _render_popover(f"View {lang} Code"): st.code(code_text, language=lang)

            if step.get("sources"):
                srcs = step["sources"]
                if isinstance(srcs, list):
                    # Clean up list formatting
                    sources_str = ", ".join(str(s) for s in srcs)
                else:
                    sources_str = str(srcs)
                st.markdown(f"*Sources for this step: {_sanitize_paths_in_text(sources_str)}*")

    if normalized.get("output"):
        st.markdown("**Output**")
        st.write(_sanitize_paths_in_text(str(normalized["output"])))
    elif not steps and not normalized.get("description"):
        st.info("Knowledge base returned structured data without workflow steps.")
        summary = {}
        for key in ("task_name", "task_id", "category", "store", "message", "reason", "query"):
            if normalized.get(key):
                summary[key] = normalized.get(key)
        if summary:
            st.json(_sanitize_paths_in_obj(summary))

    with _render_popover("Raw JSON"): st.json(_sanitize_paths_in_obj(data))
    
    if normalized.get("supplementary_text"):
        st.markdown("---")
        with st.expander("Supplementary Knowledge & Code (Mixed Mode)", expanded=False):
            st.markdown(_sanitize_paths_in_text(str(normalized.get("supplementary_text"))))

    if isinstance(rest, str) and rest.strip(): 
        # Keep supplementary content in an expander and downgrade headings
        # to avoid visually overriding the main response area.
        import re
        formatted_rest = re.sub(r'^(#+)\s', r'#### ', rest, flags=re.MULTILINE)

        st.markdown("---")
        with st.expander("Supplementary Knowledge & Code (Mixed Mode)", expanded=False):
            st.markdown(formatted_rest)


def render_uploaded_understanding_output(raw_content, tool_name: str = ""):
    """Render structured output for uploaded PDF/image understanding tools."""
    data, rest = (None, raw_content)
    if isinstance(raw_content, dict):
        data, rest = raw_content, ""
    elif isinstance(raw_content, str):
        data, rest = _extract_json(raw_content)

    if not isinstance(data, dict):
        if isinstance(rest, str) and rest.strip():
            st.write(_sanitize_paths_in_text(rest))
        return

    status = str(data.get("status", "")).strip() or "unknown"
    targets = data.get("targets", [])
    if isinstance(targets, str):
        targets = [targets]
    if not isinstance(targets, list):
        targets = []
    warnings = data.get("warnings", [])
    if isinstance(warnings, str):
        warnings = [warnings]
    if not isinstance(warnings, list):
        warnings = []
    snippets = data.get("snippets", [])
    if not isinstance(snippets, list):
        snippets = []

    if "pdf" in str(tool_name).lower():
        st.markdown("### Uploaded PDF Understanding")
    elif "image" in str(tool_name).lower():
        st.markdown("### Uploaded Image Understanding")
    else:
        st.markdown("### Uploaded File Understanding")

    st.markdown(f"**Status**: `{status}`")

    if targets:
        st.markdown("**Target Files**")
        for f in targets:
            st.markdown(f"- `inputs/{os.path.basename(str(f))}`")

    merge_stats = data.get("merge_stats", {})
    if isinstance(merge_stats, dict) and merge_stats:
        st.caption(
            _tr(
                f"注入统计：新增 {merge_stats.get('inserted', 0)}，更新 {merge_stats.get('updated', 0)}，总计 {merge_stats.get('total', 0)}",
                f"Injection stats: inserted {merge_stats.get('inserted', 0)}, updated {merge_stats.get('updated', 0)}, total {merge_stats.get('total', 0)}",
            )
        )

    if warnings:
        for w in warnings:
            st.warning(_sanitize_paths_in_text(str(w)))

    if snippets:
        st.markdown("**Relevant Snippets**")
        for i, s in enumerate(snippets, 1):
            if not isinstance(s, dict):
                continue
            with st.container(border=True):
                source = os.path.basename(str(s.get("source_file", "")))
                file_type = str(s.get("file_type", "") or "-")
                page = s.get("page")
                score = s.get("score")
                meta = [f"`{source}`", f"type={file_type}"]
                if page not in (None, "", 0):
                    meta.append(f"page={page}")
                if score not in (None, ""):
                    meta.append(f"score={score}")
                st.markdown(f"**{i}.** " + " | ".join(meta))
                text = _sanitize_paths_in_text(str(s.get("text", "") or "").strip())
                if text:
                    st.write(text)
    elif status == "context_injected_no_match":
        st.info(
            _tr(
                "文件已成功解析并注入上下文，但当前问题未命中相关片段。可换更具体问法（文件名、页码、关键词）。",
                "File context was parsed and injected successfully, but this query found no direct snippet match. "
                "Try a more specific question (file name, page, or keywords).",
            )
        )
    elif status != "success":
        message = data.get("message")
        if message:
            st.info(_sanitize_paths_in_text(str(message)))

    with _render_popover("View Raw JSON"):
        st.json(_sanitize_paths_in_obj(data))
    if isinstance(rest, str) and rest.strip():
        st.write(_sanitize_paths_in_text(rest))

# ==============================================================================
# SECTION E: Sidebar, Download, Upload
# ==============================================================================

def _apply_authenticated_sidebar_session(account: dict) -> None:
    user_id = str(account.get("user_id") or "").strip()
    username = str(account.get("username") or "").strip()
    if not user_id or not username:
        raise ValueError("Authenticated account is missing user identity.")
    st.cache_resource.clear()
    app_state.apply_authenticated_user(user_id, username)
    _queue_sidebar_thread_selection(st.session_state.get("thread_id", ""))
    st.rerun()


def _logout_sidebar_session() -> None:
    if st.session_state.get("is_running", False):
        app_logic.request_stop_active_run(detach_session=True)
    st.cache_resource.clear()
    app_state.clear_authenticated_user()
    st.rerun()


_SIDEBAR_THREAD_SELECTOR_KEY = "sidebar_thread_selector"
_PENDING_SIDEBAR_THREAD_SELECTOR_KEY = "_pending_sidebar_thread_selector"


def _queue_sidebar_thread_selection(thread_id: str) -> None:
    selected = str(thread_id or "").strip()
    if selected:
        st.session_state[_PENDING_SIDEBAR_THREAD_SELECTOR_KEY] = selected


def _apply_pending_sidebar_thread_selection(thread_ids: list[str]) -> str | None:
    pending = str(
        st.session_state.pop(_PENDING_SIDEBAR_THREAD_SELECTOR_KEY, "") or ""
    ).strip()
    if not pending or pending not in thread_ids:
        return None
    st.session_state[_SIDEBAR_THREAD_SELECTOR_KEY] = pending
    return pending


_AUTH_ERROR_TRANSLATIONS = {
    "Username is required.": "请输入用户名。",
    "That username is reserved.": "该用户名为系统保留名称，请更换。",
    "Username must be 3-40 characters using English letters, numbers, '_' or '-'.": (
        "用户名须为 3-40 个字符，只能包含英文字母、数字、下划线或连字符。"
    ),
    "Username normalization failed.": "用户名格式无法规范化，请更换。",
    "Password must be at least 8 characters.": "密码至少需要 8 个字符。",
    "That username is already registered.": "该用户名已被注册。",
    "History DB is not configured.": "账号数据库尚未配置。",
    "User authentication requires NTL_HISTORY_DB_URL or NTL_LANGGRAPH_POSTGRES_URL.": (
        "账号登录需要配置 NTL_HISTORY_DB_URL 或 NTL_LANGGRAPH_POSTGRES_URL。"
    ),
}


def _localized_auth_error(exc: Exception) -> str:
    message = str(exc).strip()
    if message.startswith("History database is unavailable."):
        return _tr("账号数据库暂时不可用，请检查 PostgreSQL 服务。", message)
    translated = _AUTH_ERROR_TRANSLATIONS.get(message)
    if translated:
        return _tr(translated, message)
    return message


def _render_auth_panel() -> None:
    st.info(
        _tr(
            "请先注册或登录。账号会绑定你的线程历史和长期记忆。",
            "Register or log in first. Threads and memory are bound to your account.",
        )
    )
    login_tab, register_tab = st.tabs([_tr("登录", "Login"), _tr("注册", "Register")])

    with login_tab:
        with st.form("sidebar_login_form", clear_on_submit=False):
            login_username = st.text_input(
                _tr("用户名（仅英文）", "Username (only English)"),
                key="auth_login_username",
            )
            login_password = st.text_input(
                _tr("密码", "Password"),
                type="password",
                key="auth_login_password",
            )
            login_submitted = st.form_submit_button(_tr("登录", "Login"), width="stretch")
        if login_submitted:
            try:
                account = history_store.authenticate_user(login_username, login_password)
            except (RuntimeError, ValueError) as exc:
                st.error(_localized_auth_error(exc))
            else:
                if not account:
                    st.error(_tr("用户名或密码错误。", "Invalid username or password."))
                else:
                    _apply_authenticated_sidebar_session(account)

    with register_tab:
        with st.form("sidebar_register_form", clear_on_submit=False):
            register_username = st.text_input(
                _tr("用户名（仅英文）", "Username (only English)"),
                key="auth_register_username",
            )
            register_password = st.text_input(
                _tr("密码（至少 8 位）", "Password (min 8 chars)"),
                type="password",
                key="auth_register_password",
            )
            register_password_confirm = st.text_input(
                _tr("确认密码", "Confirm password"),
                type="password",
                key="auth_register_password_confirm",
            )
            register_submitted = st.form_submit_button(_tr("注册", "Register"), width="stretch")
        if register_submitted:
            if register_password != register_password_confirm:
                st.error(_tr("两次输入的密码不一致。", "Passwords do not match."))
            else:
                try:
                    account = history_store.register_user(register_username, register_password)
                except (RuntimeError, ValueError) as exc:
                    st.error(_localized_auth_error(exc))
                else:
                    migrated_from = str(account.get("legacy_migrated_from") or "").strip()
                    if migrated_from:
                        st.success(
                            _tr(
                                f"注册成功，已接管 legacy 用户 `{migrated_from}` 的历史线程和记忆。",
                                f"Registered successfully. Legacy data from `{migrated_from}` has been attached to this account.",
                            )
                        )
                    _apply_authenticated_sidebar_session(account)


def render_sidebar():
    """Render all sidebar controls."""
    with st.sidebar:
        st.subheader(_tr("NTL-GPT 控制台", "NTL-GPT Console"))

        workspace = storage_manager.get_workspace(st.session_state.get("thread_id", "debug"))
        if not st.session_state.get("authenticated"):
            _render_auth_panel()
            return

        current_user_id = str(st.session_state.get("auth_user_id") or st.session_state.get("user_id") or "").strip()
        current_user_name = str(st.session_state.get("auth_username") or st.session_state.get("user_name") or current_user_id)
        username_ready = bool(current_user_id)
        st.caption(f"{_tr('当前用户', 'Current user')}: {current_user_name}")
        if st.button(_tr("退出登录", "Logout"), key="logout_btn", width="stretch"):
            _logout_sidebar_session()

        if username_ready:
            user_threads = history_store.list_user_threads(current_user_id, limit=100)
            thread_ids = [str(row.get("thread_id")) for row in user_threads if row.get("thread_id")]
            thread_ids = [tid for tid in thread_ids if tid]
            current_tid = str(st.session_state.get("thread_id", "debug") or "debug")

            if not thread_ids:
                new_thread_id = history_store.generate_thread_id(current_user_id)
                app_state.set_active_thread(new_thread_id)
                history_store.bind_thread_to_user(current_user_id, new_thread_id)
                _queue_sidebar_thread_selection(new_thread_id)
                st.rerun()

            if current_tid not in thread_ids:
                app_state.set_active_thread(thread_ids[0])
                _queue_sidebar_thread_selection(thread_ids[0])
                st.rerun()

            _apply_pending_sidebar_thread_selection(thread_ids)

            thread_label_map = {}
            for row in user_threads:
                tid = str(row.get("thread_id", "")).strip()
                if not tid:
                    continue
                q = str(row.get("last_question", "") or "").strip()
                q_short = (q[:28] + "...") if len(q) > 28 else q
                label = f"{tid} | {q_short}" if q_short else tid
                thread_label_map[tid] = label
            default_idx = thread_ids.index(current_tid) if current_tid in thread_ids else 0
            thread_sel_col, thread_del_col = st.columns([0.84, 0.16], gap="small")
            with thread_sel_col:
                selected_tid = st.selectbox(
                    _tr("历史线程", "History Threads"),
                    options=thread_ids,
                    index=default_idx,
                    format_func=lambda tid: thread_label_map.get(tid, tid),
                    key=_SIDEBAR_THREAD_SELECTOR_KEY,
                    label_visibility="visible",
                )
            with thread_del_col:
                st.markdown("<div style='height: 1.78rem;'></div>", unsafe_allow_html=True)
                if st.button(
                    _tr("删除", "Delete"),
                    key="delete_selected_thread_inline_trigger",
                    help=_tr("删除当前选中线程", "Delete selected thread"),
                    icon=":material/delete:",
                    width="stretch",
                ):
                    st.session_state["confirm_delete_selected_thread_inline"] = True

            if selected_tid != current_tid:
                if st.session_state.get("is_running", False):
                    app_logic.request_stop_active_run(thread_id=current_tid)
                app_state.set_active_thread(selected_tid)
                history_store.bind_thread_to_user(current_user_id, selected_tid)
                st.rerun()

            if st.session_state.get("confirm_delete_selected_thread_inline", False):
                st.warning(
                    _tr(
                        f"删除线程 `{selected_tid}`？该操作会删除历史与线程文件。",
                        f"Delete thread `{selected_tid}`? This will remove history and workspace files.",
                    )
                )
                confirm_cols = st.columns([1, 1], gap="small")
                with confirm_cols[0]:
                    if st.button(
                        _tr("确认删除", "Confirm Delete"),
                        key="delete_selected_thread_inline_confirm_btn",
                        width="stretch",
                    ):
                        if st.session_state.get("is_running", False):
                            app_logic.request_stop_active_run(thread_id=selected_tid, detach_session=True)

                        delete_result = history_store.delete_user_thread(
                            current_user_id,
                            selected_tid,
                            delete_workspace=True,
                        )
                        remaining = history_store.list_user_threads(current_user_id, limit=100)
                        remaining_ids = [str(row.get("thread_id")) for row in remaining if row.get("thread_id")]

                        if not remaining_ids:
                            new_thread_id = history_store.generate_thread_id(current_user_id)
                            history_store.bind_thread_to_user(current_user_id, new_thread_id)
                            app_state.set_active_thread(new_thread_id)
                            _queue_sidebar_thread_selection(new_thread_id)
                        else:
                            app_state.set_active_thread(remaining_ids[0])
                            _queue_sidebar_thread_selection(remaining_ids[0])

                        st.session_state["confirm_delete_selected_thread_inline"] = False
                        if delete_result.get("deleted"):
                            st.success(_tr("线程已删除。", "Thread deleted."))
                        else:
                            st.warning(_tr("未找到可删除的线程记录。", "No thread record was deleted."))
                        st.rerun()
                with confirm_cols[1]:
                    if st.button(
                        _tr("取消", "Cancel"),
                        key="delete_selected_thread_inline_cancel_btn",
                        width="stretch",
                    ):
                        st.session_state["confirm_delete_selected_thread_inline"] = False
                        st.rerun()


        # in_count = len(list((workspace / "inputs").glob("*.*"))) if (workspace / "inputs").exists() else 0
        # out_count = len(list((workspace / "outputs").glob("*.*"))) if (workspace / "outputs").exists() else 0
        current_model = st.session_state.get("cfg_model", app_state.MODEL_OPTIONS[0])
        if current_model not in app_state.MODEL_OPTIONS:
            current_model = app_state.MODEL_OPTIONS[0]
            st.session_state["cfg_model"] = current_model
        if st.session_state.get("model_selector") not in app_state.MODEL_OPTIONS:
            st.session_state["model_selector"] = current_model
        selected_model = st.selectbox(
            _tr("模型", "Model"),
            app_state.MODEL_OPTIONS,
            index=app_state.MODEL_OPTIONS.index(current_model),
            key="model_selector"
        )
        current_thread_running = _is_current_thread_running()
        current_thread_stopping = _is_current_thread_stopping()
        if selected_model != current_model:
            if current_thread_running and not current_thread_stopping:
                st.session_state["pending_model_change"] = selected_model
                st.session_state["model_selector"] = current_model
                st.info(
                    _tr(
                        "模型变更已暂存，将在当前任务结束后生效。",
                        "Model change queued and will apply after current run finishes.",
                    )
                )
            else:
                st.session_state["cfg_model"] = selected_model
                st.session_state["pending_model_change"] = None
                current_model = selected_model
        pending_model = str(st.session_state.get("pending_model_change") or "").strip()
        if pending_model and current_thread_running and not current_thread_stopping:
            st.caption(_tr(f"待生效模型: {pending_model}", f"Pending model: {pending_model}"))
        selected_model = current_model

        selected_model_config = model_config.get_model_config(selected_model)
        key_label = selected_model_config.key_label
        use_env_api_key = selected_model_config.uses_env_api_key
        env_api_key = model_config.get_env_api_key(selected_model)
        missing_model_env = model_config.missing_env_for_model(selected_model)
        user_api_key = ""
        if use_env_api_key:
            if missing_model_env:
                missing_text = ", ".join(missing_model_env)
                st.warning(
                    _tr(
                        f"未检测到 .env 中的 {missing_text}，激活会失败。",
                        f"{missing_text} not found in .env; activation will fail.",
                    )
                )
        else:
            user_api_key = st.text_input(
                label=_tr(f"输入 {key_label}", f"Enter {key_label}"),
                type="password",
                help=_tr("激活系统所必需。", "Required to activate the agent system."),
                key="user_api_key_input"
            )

        action_cols = st.columns(3, gap="small")
        with action_cols[0]:
            if st.button(
                _tr("激活", "Activate"),
                key="activate_btn",
                width="stretch",
                type="secondary",
                disabled=not username_ready,
            ):
                effective_api_key = ""
                can_activate = True
                if use_env_api_key:
                    effective_api_key = env_api_key
                    if missing_model_env or not effective_api_key:
                        missing_text = ", ".join(missing_model_env or [selected_model_config.api_key_env or "API key"])
                        st.error(
                            _tr(
                                f"请先在 .env 中配置 {missing_text}。",
                                f"Please set {missing_text} in .env first.",
                            )
                        )
                        can_activate = False
                else:
                    effective_api_key = (user_api_key or "").strip()
                    if not effective_api_key:
                        st.error(_tr(f"请输入 {key_label}！", f"Please enter your {key_label}!"))
                        can_activate = False

                if can_activate and effective_api_key:
                    if current_thread_running and not current_thread_stopping:
                        st.session_state["pending_activate_request"] = {
                            "user_api_key": effective_api_key,
                            "model": selected_model,
                        }
                        st.info(
                            _tr(
                                "激活请求已暂存，将在当前任务结束后自动生效。",
                                "Activate request queued and will apply after current run finishes.",
                            )
                        )
                    else:
                        st.session_state["user_api_key"] = effective_api_key
                        st.session_state["initialized"] = True
                        app_logic.ensure_conversation_initialized()
                        st.success(_tr("已激活！", "Activated!"))
                        st.rerun()
                # ---------------------

        with action_cols[1]:
            if st.button(
                _tr("新建", "New"),
                key="reset_btn",
                width="stretch",
                type="secondary",
                disabled=not username_ready,
            ):
                if st.session_state.get("is_running", False):
                    app_logic.request_stop_active_run()
                st.cache_resource.clear()
                st.session_state["initialized"] = False
                st.session_state.chat_history = []
                st.session_state.analysis_logs = []
                st.session_state.analysis_history = []
                st.session_state.last_question = ""
                st.session_state["cancel_requested"] = False
                st.session_state["stopping"] = False

                if "user_api_key" in st.session_state:
                    del st.session_state["user_api_key"]

                uid = str(st.session_state.get("user_id", "") or "").strip()
                if not uid or history_store.is_reserved_user_id(uid):
                    uid = history_store.generate_anonymous_user_id()
                    st.session_state["user_id"] = uid
                    st.session_state["user_name"] = ""
                new_thread_id = history_store.generate_thread_id(uid)
                app_state.set_active_thread(new_thread_id)
                history_store.bind_thread_to_user(uid, new_thread_id)
                _queue_sidebar_thread_selection(new_thread_id)
                st.warning(_tr("已创建新会话。", "New session created."))
                st.rerun()

        with action_cols[2]:
            if st.button(
                _tr("中断", "Stop"),
                key="interrupt_current_run_btn",
                width="stretch",
                type="secondary",
                help=_tr("请求立即中断当前回答。", "Request immediate interruption of the current run."),
            ):
                if st.session_state.get("is_running", False):
                    # Request backend stop and immediately release UI run lock
                    # so Activate can be applied without waiting for remote timeout.
                    app_logic.request_stop_active_run(detach_session=True)
                    st.rerun()

        if (not current_thread_running) or current_thread_stopping:
            pending_model = str(st.session_state.get("pending_model_change") or "").strip()
            if pending_model and pending_model in app_state.MODEL_OPTIONS:
                st.session_state["cfg_model"] = pending_model
                st.session_state["model_selector"] = pending_model
                st.session_state["pending_model_change"] = None
            pending_activate = st.session_state.get("pending_activate_request")
            if isinstance(pending_activate, dict):
                pending_key = str(pending_activate.get("user_api_key") or "").strip()
                pending_model_from_activate = str(pending_activate.get("model") or "").strip()
                if pending_model_from_activate in app_state.MODEL_OPTIONS:
                    st.session_state["cfg_model"] = pending_model_from_activate
                    st.session_state["model_selector"] = pending_model_from_activate
                if pending_key:
                    st.session_state["user_api_key"] = pending_key
                    st.session_state["initialized"] = True
                    app_logic.ensure_conversation_initialized()
                st.session_state["pending_activate_request"] = None
                st.success(_tr("已应用待生效设置。", "Queued settings applied."))
                st.rerun()

        status = _tr("已激活", "Active") if st.session_state.get("initialized") else _tr("未激活", "Inactive")
        status_class = "active" if st.session_state.get("initialized") else "inactive"
        st.markdown(
            (
                "<div class='ntl-thread-status-row'>"
                f"<span class='ntl-thread-status-item'><span>{_tr('线程 ID', 'Thread ID')}:</span>"
                f"<span class='ntl-thread-status-value'>{st.session_state.thread_id}</span></span>"
                f"<span class='ntl-thread-status-item'><span>{_tr('状态', 'Status')}:</span>"
                f"<span class='ntl-status-text {status_class}'>{status}</span></span>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
        limit_snapshot = app_logic.get_run_limit_snapshot(current_user_id)
        global_limit = int(limit_snapshot.get("global_limit", 0) or 0)
        user_limit = int(limit_snapshot.get("user_limit", 0) or 0)
        global_usage = (
            f"{int(limit_snapshot.get('global_active', 0) or 0)}/{global_limit}"
            if global_limit
            else f"{int(limit_snapshot.get('global_active', 0) or 0)}/∞"
        )
        user_usage = (
            f"{int(limit_snapshot.get('user_active', 0) or 0)}/{user_limit}"
            if user_limit
            else f"{int(limit_snapshot.get('user_active', 0) or 0)}/∞"
        )
        st.caption(f"MAX_RUNS {global_usage} · MAX_RUNS_PER_USER {user_usage}")
        st.markdown("<hr style='margin: 8px 0;'>", unsafe_allow_html=True)

        with st.expander("NTL Data Availability", expanded=False):
            _render_data_availability_block()

        with st.expander(_tr("测试用例", "Test Cases"), expanded=False):
            try:
                case_files = list(_TEST_CASE_FILES)
                loaded_names = []
                frames = []
                for fp in case_files:
                    if fp.exists():
                        frames.append(pd.read_excel(fp))
                        loaded_names.append(fp.name)

                if not frames:
                    expected = ", ".join(str(p) for p in case_files)
                    raise FileNotFoundError(f"No test case file found. Expected one of: {expected}")

                df_cases = pd.concat(frames, ignore_index=True)
                df_cases = df_cases.dropna(subset=['Case'])
                df_cases = df_cases.drop_duplicates(subset=['Case'], keep='first')
                df_cases['Category'] = df_cases['Category'].fillna("General").astype(str)
                df_cases['Label'] = df_cases['Label'].fillna("Unnamed Task").astype(str)
                categories = {}
                for _, row in df_cases.iterrows():
                    case_id = _test_case_id_from_row(row)
                    case = _localized_test_case(
                        row['Category'].strip(),
                        row['Label'].strip(),
                        str(row['Case']).strip(),
                        case_id,
                    )
                    cat = case["category"]
                    if cat not in categories:
                        categories[cat] = []
                    categories[cat].append(case)

                for cat, cases in categories.items():
                    with st.expander(f"{cat}", expanded=False):
                        for i, case in enumerate(cases, 1):
                            st.markdown(f"**{i}. {case['label']}**")
                            st.markdown(
                                f"<div style='color:#cfe1ff;font-size:0.95rem;line-height:1.55;'>{case['query']}</div>",
                                unsafe_allow_html=True,
                            )
                            case_key = case["id"] or f"{cat}_{i}"
                            if st.button(_tr("运行用例", "Run Case"), key=f"run_case_{case_key}", width="stretch"):
                                st.session_state["pending_question"] = case["query"]
                                st.rerun()
            except Exception as e:
                st.error(f"Failed to load test case files: {e}")

def render_download_center():
    """Render input/output download center in sidebar."""
    try:
        workspace = storage_manager.get_workspace(st.session_state.get("thread_id", "debug"))
        sidecar_exts = {".dbf", ".prj", ".shx", ".cpg"}
        shp_bundle_exts = {
            ".shp", ".dbf", ".prj", ".shx", ".cpg",
            ".qix", ".sbn", ".sbx", ".ain", ".aih", ".atx", ".ixs", ".mxs",
        }

        def _build_shp_bundle_zip_bytes(shp_file: Path) -> bytes:
            stem = shp_file.stem
            folder = shp_file.parent
            related = [
                p for p in folder.glob(f"{stem}.*")
                if p.is_file() and p.suffix.lower() in shp_bundle_exts
            ]
            related = sorted(related, key=lambda p: (0 if p.suffix.lower() == ".shp" else 1, p.name.lower()))
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                for p in related:
                    zf.write(p, arcname=p.name)
            return buf.getvalue()

        def get_valid_files(directory, include_py=False):
            if not directory.exists(): return []
            files = [
                f
                for f in directory.glob("*.*")
                if f.suffix.lower() not in ([".zip", ".tmp"] + ([] if include_py else [".py"]))
                and not f.name.startswith(".")
            ]
            shp_stems = {f.stem for f in files if f.suffix.lower() == ".shp"}
            filtered = []
            for f in files:
                ext = f.suffix.lower()
                if ext in sidecar_exts and f.stem in shp_stems:
                    # If same-named .shp exists, hide sidecar files from list display.
                    continue
                filtered.append(f)
            return filtered

        in_files = get_valid_files(workspace / "inputs", include_py=False)
        out_files = get_valid_files(workspace / "outputs", include_py=True)

        if not in_files and not out_files: return

        # st.sidebar.markdown("<div class='ntl-sidebar-divider-tight'></div>", unsafe_allow_html=True)
        # st.sidebar.subheader(_tr("数据中心", "Data Center"))
        tab_in, tab_out = st.sidebar.tabs([_tr("输入", "Inputs"), _tr("输出", "Outputs")])

        with tab_in:
            if in_files:
                for f in in_files:
                    if f.suffix.lower() == ".shp":
                        st.download_button(
                            label=f"{f.name}",
                            data=_build_shp_bundle_zip_bytes(f),
                            file_name=f"{f.stem}.zip",
                            mime="application/zip",
                            key=f"dl_in_{f.name}",
                            width="stretch",
                        )
                    else:
                        with open(f, "rb") as file_data:
                            st.download_button(
                                label=f"{f.name}",
                                data=file_data,
                                file_name=f.name,
                                key=f"dl_in_{f.name}",
                                width="stretch",
                            )
            else:
                st.caption(_tr("暂无", "Empty"))

        with tab_out:
            if out_files:
                for f in out_files:
                    if f.suffix.lower() == ".shp":
                        st.download_button(
                            label=f"{f.name}",
                            data=_build_shp_bundle_zip_bytes(f),
                            file_name=f"{f.stem}.zip",
                            mime="application/zip",
                            key=f"dl_out_{f.name}",
                            width="stretch",
                        )
                    else:
                        with open(f, "rb") as file_data:
                            st.download_button(
                                label=f"{f.name}",
                                data=file_data,
                                file_name=f.name,
                                key=f"dl_out_{f.name}",
                                width="stretch",
                            )
            else:
                st.caption(_tr("暂无", "Empty"))
    except Exception as e:
        st.sidebar.error(f"Download Center Error: {e}")

def render_file_uploader():
    """Render file uploader in sidebar."""
    st.markdown("<hr style='margin: 8px 0;'>", unsafe_allow_html=True)
    
    uploaded_files = st.sidebar.file_uploader(
        _tr("上传数据文件", "Upload Data Files"),
        accept_multiple_files=True,
        type=['tif', 'tiff', 'shp', 'dbf', 'prj', 'shx', 'geojson', 'csv', 'xlsx', 'xls', 'zip', 'pdf', 'png', 'jpg', 'jpeg', 'webp', 'bmp'],
        key="data_uploader"
    )

    if uploaded_files:
        workspace = storage_manager.get_workspace(st.session_state.get("thread_id", "debug"))
        input_dir = workspace / "inputs"
        user_id = str(st.session_state.get("user_id") or "").strip()
        thread_id = str(st.session_state.get("thread_id") or "debug")
        for uploaded_file in uploaded_files:
            target_path = input_dir / uploaded_file.name
            if not target_path.exists():
                payload = uploaded_file.getbuffer()
                quota_rejection = app_logic._workspace_quota_rejection(thread_id, user_id, additional_bytes=len(payload))
                if quota_rejection:
                    limit = quota_rejection.get("limit_label") or quota_rejection.get("limit_bytes") or "?"
                    projected = quota_rejection.get("projected_label") or quota_rejection.get("projected_bytes") or "?"
                    st.sidebar.error(
                        _tr(
                            f"上传会超过工作区配额：{projected} / {limit}。",
                            f"Upload would exceed workspace quota: {projected} / {limit}.",
                        )
                    )
                    continue
                if uploaded_file.name.endswith(".zip"):
                    with zipfile.ZipFile(uploaded_file, "r") as zip_ref:
                        zip_ref.extractall(input_dir)
                    st.sidebar.success(_tr(f"已解压：{uploaded_file.name}", f"Extracted: {uploaded_file.name}"))
                else:
                    with open(target_path, "wb") as f:
                        f.write(payload)
                st.sidebar.success(_tr(f"已上传：{uploaded_file.name}", f"Uploaded: {uploaded_file.name}"))


def render_file_understanding_panel():
    """Manual file-understanding panel (no auto-token usage on upload)."""
    st.sidebar.markdown("<hr style='margin: 8px 0;'>", unsafe_allow_html=True)
    st.sidebar.subheader(_tr("文件理解（手动触发）", "File Understanding (Manual)"))
    st.sidebar.caption(
        _tr(
            "上传后不会自动理解。仅在你点击按钮时，才会解析并注入相关片段到上下文。",
            "Upload stays passive. Parsing/context injection only runs when you click the action button.",
        )
    )

    workspace = storage_manager.get_workspace(st.session_state.get("thread_id", "debug"))
    input_dir = workspace / "inputs"
    supported_ext = {".pdf", ".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff"}
    files = []
    if input_dir.exists():
        files = sorted(
            [p for p in input_dir.glob("*.*") if p.suffix.lower() in supported_ext],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
    if not files:
        st.sidebar.info(_tr("当前线程 inputs 下没有可理解文件。", "No supported files in current thread inputs."))
        return

    file_options = [p.name for p in files]
    default_pick = file_options[: min(3, len(file_options))]
    selected_files = st.sidebar.multiselect(
        _tr("选择文件", "Select Files"),
        options=file_options,
        default=default_pick,
        key="manual_understanding_file_selector",
    )
    default_max_pages = 120
    default_top_n = 4
    default_max_chars = 6000
    st.session_state["manual_understanding_max_pages"] = default_max_pages
    st.session_state["injected_context_top_n"] = default_top_n
    st.session_state["injected_context_max_chars"] = default_max_chars
    st.sidebar.caption(
        _tr(
            "默认参数：PDF页数=120，Top-N=4，最大注入字符=6000（固定）。",
            "Defaults: Max PDF Pages=120, Top-N=4, Max Injected Chars=6000 (fixed).",
        )
    )

    if st.sidebar.button(_tr("理解并注入上下文", "Analyze and Inject Context"), width="stretch"):
        if not selected_files:
            st.sidebar.warning(_tr("请至少选择一个文件。", "Please select at least one file."))
        else:
            with st.sidebar:
                with st.spinner(_tr("正在解析文件...", "Parsing files...")):
                    result = app_logic.inject_selected_files_to_context(
                        file_names=selected_files,
                        max_pages=default_max_pages,
                    )
            merge_stats = result.get("merge_stats", {}) or {}
            st.sidebar.success(
                _tr(
                    f"完成：新增 {merge_stats.get('inserted', 0)}，更新 {merge_stats.get('updated', 0)}，总计 {merge_stats.get('total', 0)}。",
                    f"Done: inserted {merge_stats.get('inserted', 0)}, updated {merge_stats.get('updated', 0)}, total {merge_stats.get('total', 0)}.",
                )
            )
            for w in result.get("warnings", []) or []:
                st.sidebar.warning(str(w))
            if result.get("unsupported_files"):
                st.sidebar.caption(
                    _tr(
                        f"不支持：{', '.join(result.get('unsupported_files'))}",
                        f"Unsupported: {', '.join(result.get('unsupported_files'))}",
                    )
                )
            if result.get("missing_files"):
                st.sidebar.caption(
                    _tr(
                        f"缺失：{', '.join(result.get('missing_files'))}",
                        f"Missing: {', '.join(result.get('missing_files'))}",
                    )
                )

    current_injected = history_store.injected_file_overview(str(st.session_state.get("thread_id", "debug")))
    if current_injected:
        st.sidebar.caption(_tr("当前已注入文件", "Currently Injected Files"))
        for row in current_injected[:12]:
            st.sidebar.markdown(
                f"- `{row.get('source_file')}` | {row.get('chunks', 0)} chunks"
            )
        if st.sidebar.button(_tr("清空注入上下文", "Clear Injected Context"), width="stretch"):
            app_logic.clear_injected_context()
            st.sidebar.success(_tr("已清空。", "Cleared."))
            st.rerun()

def show_history(
    chat_history,
    *,
    show_running_notice_under_latest_user: bool = False,
    show_stopping_notice_under_latest_user: bool = False,
):
    """Render chat history, images, and tables."""
    latest_user_idx = -1
    if show_running_notice_under_latest_user:
        for i, (role, _content) in enumerate(chat_history):
            if role == "user":
                latest_user_idx = i

    for i, (role, content) in enumerate(chat_history):
        if role == "user":
            st.write(USER_TEMPLATE.replace("{{MSG}}", content), unsafe_allow_html=True)
            if i == latest_user_idx and (show_running_notice_under_latest_user or show_stopping_notice_under_latest_user):
                if show_stopping_notice_under_latest_user:
                    st.caption(
                        _tr(
                            "正在停止当前任务，请等待中断确认。",
                            "Stopping current task... Please wait until interruption is confirmed.",
                        )
                    )
                else:
                    st.caption(
                        _tr(
                            "后台处理中，可点击 Stop/New 中断当前任务。",
                            "Running in background. Click Stop/New to interrupt this task.",
                        )
                    )
        elif role == "assistant":
            st.write(BOT_TEMPLATE.replace("{{MSG}}", content), unsafe_allow_html=True)
        elif role == "assistant_img":
            file_name = os.path.basename(content)
            st.image(content, width=600, caption=_tr(f"图像结果: {file_name}", f"Plot: {file_name}"))
        elif role == "assistant_table":
            file_name = os.path.basename(content)
            try:
                df = pd.read_csv(content)
                with st.expander(_tr(f"查看统计表: {file_name}", f"View statistics table: {file_name}"), expanded=False):
                    st.dataframe(df, width="stretch")
            except Exception as e:
                st.error(_tr(f"读取表格失败 {file_name}: {e}", f"Failed to load table {file_name}: {e}"))
    scroll_to_bottom()

import matplotlib.colors as mcolors

def render_map_view():
    """Render map view for vector/raster layers."""
    default_map_center = [35.0, 104.0]  # China-centered default viewport
    default_map_zoom = 4
    thread_id = str(st.session_state.get("thread_id", "debug"))
    opened_once_by_thread = st.session_state.setdefault("map_opened_once_by_thread", {})
    last_layer_sig_by_thread = st.session_state.setdefault("map_last_layer_sig_by_thread", {})
    reset_nonce_by_thread = st.session_state.setdefault("map_reset_nonce_by_thread", {})
    workspace = storage_manager.get_workspace(st.session_state.get("thread_id", "debug"))
    geo_files = []
    for folder in ["inputs", "outputs"]:
        path = workspace / folder
        if path.exists():
            geo_files.extend([f for f in path.glob("*.*") if f.suffix.lower() in [".tif", ".tiff", ".shp", ".geojson"]])

    if not geo_files:
        st.info(_tr("暂无地理数据可视化。", "No geospatial data available yet."))
        return

    geo_files = sorted(geo_files, key=lambda p: p.stat().st_mtime)
    option_keys = {str(p): p for p in geo_files}
    pending_focus = str(st.session_state.get("pending_map_focus_layer") or "")
    pending_layer = option_keys.get(pending_focus) if pending_focus else None
    if pending_focus:
        st.session_state["pending_map_focus_layer"] = None
    current_layer = st.session_state.get("current_map_layer") or st.session_state.get("current_map_tif")
    preferred = next((f for f in geo_files if current_layer and str(f) == str(current_layer)), None)
    default_layer = preferred or geo_files[-1]

    raw_selected = st.session_state.get("selected_layers")
    if pending_layer is not None:
        sanitized_selected = [pending_layer]
    elif raw_selected is None:
        sanitized_selected = [default_layer]
    else:
        if not isinstance(raw_selected, list):
            raw_selected = [raw_selected]
        sanitized_selected = []
        for item in raw_selected:
            key = str(item)
            if key in option_keys:
                sanitized_selected.append(option_keys[key])
        if not sanitized_selected:
            sanitized_selected = [default_layer]
    st.session_state["selected_layers"] = sanitized_selected

    selected_layers = st.multiselect(
        _tr("选择显示图层", "Select Layers to Display"),
        options=geo_files,
        format_func=lambda x: f"[{x.parent.name}] {x.name}",
        key="selected_layers",
    )

    layer_signature = build_layer_signature(selected_layers)
    policy_state = advance_map_view_state(
        thread_id=thread_id,
        layer_signature=layer_signature,
        opened_once_by_thread=opened_once_by_thread,
        last_layer_sig_by_thread=last_layer_sig_by_thread,
        reset_nonce_by_thread=reset_nonce_by_thread,
    )
    is_first_open = bool(policy_state["is_first_open"])
    map_nonce = int(policy_state["map_nonce"])
    map_component_key = f"main_map_{thread_id}_{map_nonce}"

    if not selected_layers:
        st.warning(_tr("请至少选择一个图层进行可视化。", "Please select at least one layer to visualize."))
        m = folium.Map(location=default_map_center, zoom_start=default_map_zoom, control_scale=True, tiles=None)
        folium.TileLayer("CartoDB dark_matter", name="Dark Canvas", show=True).add_to(m)
        st_folium(m, width="stretch", height=520, key=f"{map_component_key}_empty")
        return

    if "layer_styles" not in st.session_state:
        st.session_state["layer_styles"] = {}

    with st.expander(_tr("图层样式", "Layer Styling"), expanded=False):
        active_layer_file = st.selectbox(
            _tr("配置图层", "Configure Layer"),
            options=selected_layers,
            format_func=lambda x: x.name,
            key="style_target_selector",
        )

        layer_key = str(active_layer_file.name)
        current_style = st.session_state["layer_styles"].get(layer_key, {
            "opacity": 0.8,
            "vis_mode": "Auto",
            "colormap": "viridis",
            "mask_color": "#FF0000",
        })

        col_s1, col_s2, col_s3 = st.columns(3)
        with col_s1:
            new_opacity = st.slider(_tr("透明度", "Opacity"), 0.0, 1.0, current_style["opacity"], key=f"op_{layer_key}")
        with col_s2:
            vis_modes = ["Auto", "Stretched (Continuous)", "Discrete (Categorical/Mask)"]
            try:
                idx = vis_modes.index(current_style["vis_mode"])
            except Exception:
                idx = 0
            new_vis_mode = st.selectbox(_tr("可视化模式", "Visualization Mode"), vis_modes, index=idx, key=f"vm_{layer_key}")
        with col_s3:
            if new_vis_mode == "Discrete (Categorical/Mask)":
                new_mask_color = st.color_picker(_tr("掩膜颜色（值=1）", "Mask Color (for value=1)"), current_style["mask_color"], key=f"mc_{layer_key}")
                new_colormap = current_style["colormap"]
            else:
                colormap_options = ["gray", "magma", "viridis", "inferno", "cividis", "plasma", "coolwarm"]
                try:
                    idx_cm = colormap_options.index(current_style["colormap"])
                except Exception:
                    idx_cm = 2
                new_colormap = st.selectbox("Colormap", colormap_options, index=idx_cm, key=f"cm_{layer_key}")
                new_mask_color = current_style["mask_color"]

        st.session_state["layer_styles"][layer_key] = {
            "opacity": new_opacity,
            "vis_mode": new_vis_mode,
            "colormap": new_colormap,
            "mask_color": new_mask_color,
        }

    def _add_bound(bounds_acc, min_lat, min_lon, max_lat, max_lon):
        vals = [min_lat, min_lon, max_lat, max_lon]
        if not all(np.isfinite(v) for v in vals):
            return
        # Clamp to valid geographic bounds to avoid invalid fit bounds.
        min_lat = max(-90.0, min(90.0, float(min_lat)))
        max_lat = max(-90.0, min(90.0, float(max_lat)))
        min_lon = max(-180.0, min(180.0, float(min_lon)))
        max_lon = max(-180.0, min(180.0, float(max_lon)))
        if max_lat <= min_lat or max_lon <= min_lon:
            return
        lat_span = max_lat - min_lat
        lon_span = max_lon - min_lon
        # Guardrail: ignore accidental near-global extents (usually CRS/bounds issue)
        # so initial map won't jump to full-world unexpectedly.
        if lat_span >= 170.0 and lon_span >= 340.0:
            return
        bounds_acc.append([min_lat, min_lon, max_lat, max_lon])

    overall_bounds = []
    m = folium.Map(location=default_map_center, zoom_start=default_map_zoom, control_scale=True, tiles=None)
    folium.TileLayer("CartoDB dark_matter", name="Dark Canvas", show=True).add_to(m)
    folium.TileLayer(
        "https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}",
        attr="Google",
        name="Satellite",
        show=False,
    ).add_to(m)
    for file_path in selected_layers:
        try:
            layer_name = file_path.name
            style = st.session_state["layer_styles"].get(layer_name, {
                "opacity": 0.8, "vis_mode": "Auto", "colormap": "viridis", "mask_color": "#FF0000"
            })

            if file_path.suffix.lower() in [".tif", ".tiff"]:
                with rasterio.open(file_path) as src:
                    from rasterio.warp import transform_bounds as rio_transform_bounds

                    src_crs = src.crs if src.crs else "EPSG:4326"
                    bounds = rio_transform_bounds(src_crs, "EPSG:4326", *src.bounds, densify_pts=21)
                    _add_bound(overall_bounds, bounds[1], bounds[0], bounds[3], bounds[2])

                    if src.count >= 3:
                        rgb_data = []
                        for b in [1, 2, 3]:
                            band = src.read(b).astype(float)
                            p2, p98 = np.percentile(band[band > 0], [2, 98]) if np.any(band > 0) else (0, 1)
                            if p98 - p2 == 0:
                                p98 += 1e-6
                            band = np.clip((band - p2) / (p98 - p2), 0, 1)
                            rgb_data.append(band)
                        img_array = np.stack(rgb_data, axis=-1)
                        alpha = np.where(np.sum(img_array, axis=-1) > 0, 1.0, 0.0)
                        img_array = np.dstack([img_array, alpha])
                        img_pil = Image.fromarray((img_array * 255).astype(np.uint8))
                    else:
                        data = src.read(1)
                        is_categorical = False
                        if style["vis_mode"] == "Discrete (Categorical/Mask)":
                            is_categorical = True
                        elif style["vis_mode"] == "Auto":
                            unique_vals = np.unique(data)
                            if len(unique_vals) < 15:
                                is_categorical = True

                        if is_categorical:
                            img_rgba = np.zeros((data.shape[0], data.shape[1], 4), dtype=np.uint8)
                            mask_c = mcolors.to_rgb(style["mask_color"])
                            mask_rgb = [int(c * 255) for c in mask_c]
                            foreground_mask = (data > 0) & (data != src.nodata)
                            img_rgba[foreground_mask, 0] = mask_rgb[0]
                            img_rgba[foreground_mask, 1] = mask_rgb[1]
                            img_rgba[foreground_mask, 2] = mask_rgb[2]
                            img_rgba[foreground_mask, 3] = 255
                            img_pil = Image.fromarray(img_rgba)
                        else:
                            data = data.astype(float)
                            valid_mask = (data != src.nodata) & (~np.isnan(data))
                            if np.any(valid_mask):
                                valid_data = data[valid_mask]
                                p2, p98 = np.percentile(valid_data, [2, 98])
                                if p98 - p2 == 0:
                                    p98 += 1e-6
                                data_norm = np.clip((data - p2) / (p98 - p2), 0, 1)
                            else:
                                data_norm = np.zeros_like(data)
                            colormap = cm.get_cmap(style["colormap"])
                            img_array = colormap(data_norm)
                            img_array[..., 3] = np.where(valid_mask & (data > 0), 1.0, 0.0)
                            img_pil = Image.fromarray((img_array * 255).astype(np.uint8))

                    folium.raster_layers.ImageOverlay(
                        image=np.array(img_pil),
                        bounds=[[bounds[1], bounds[0]], [bounds[3], bounds[2]]],
                        opacity=style["opacity"],
                        name=f"{file_path.name}",
                        interactive=True,
                        cross_origin=False,
                    ).add_to(m)

            elif file_path.suffix.lower() in [".shp", ".geojson"]:
                gdf = gpd.read_file(file_path)
                if gdf.empty:
                    continue
                if gdf.crs is None:
                    gdf = gdf.set_crs(epsg=4326, allow_override=True)
                elif str(gdf.crs).upper() != "EPSG:4326":
                    gdf = gdf.to_crs(epsg=4326)
                b = gdf.total_bounds
                _add_bound(overall_bounds, b[1], b[0], b[3], b[2])

                folium.GeoJson(
                    gdf,
                    name=f"{file_path.name}",
                    style_function=lambda x: {"color": "#ff0000", "weight": 2, "opacity": 0.7, "fillOpacity": 0.1},
                ).add_to(m)

        except Exception as e:
            st.error(_tr(f"图层加载失败 {file_path.name}: {e}", f"Error loading {file_path.name}: {e}"))

    if overall_bounds and not is_first_open:
        try:
            min_lat = min(b[0] for b in overall_bounds)
            min_lon = min(b[1] for b in overall_bounds)
            max_lat = max(b[2] for b in overall_bounds)
            max_lon = max(b[3] for b in overall_bounds)
            if max_lat - min_lat < 1e-6:
                max_lat += 0.001
                min_lat -= 0.001
            if max_lon - min_lon < 1e-6:
                max_lon += 0.001
                min_lon -= 0.001
            m.fit_bounds([[min_lat, min_lon], [max_lat, max_lon]])
        except Exception:
            m.location = default_map_center
            m.zoom_start = default_map_zoom
    else:
        m.location = default_map_center
        m.zoom_start = default_map_zoom

    folium.LayerControl().add_to(m)
    map_output = st_folium(m, width="stretch", height=540, key=map_component_key)

    if map_output and map_output.get("last_clicked"):
        click_lat = map_output["last_clicked"]["lat"]
        click_lon = map_output["last_clicked"]["lng"]
        st.session_state["map_center"] = [click_lat, click_lon]

        last_tif = next((f for f in reversed(selected_layers) if f.suffix.lower() in [".tif", ".tiff"]), None)
        if last_tif:
            try:
                with rasterio.open(last_tif) as src:
                    if src.crs is None:
                        return
                    from pyproj import Transformer

                    transformer = Transformer.from_crs("EPSG:4326", src.crs, always_xy=True)
                    x, y = transformer.transform(click_lon, click_lat)
                    if (src.bounds.left <= x <= src.bounds.right) and (src.bounds.bottom <= y <= src.bounds.top):
                        row, col = src.index(x, y)
                        val = src.read(1, window=rasterio.windows.Window(col, row, 1, 1))
                        if val.size > 0:
                            v = val[0, 0]
                            st.info(_tr(f"识别结果\n- 图层: `{last_tif.name}`\n- 值: `{v}`", f"Identify Result\n- Layer: `{last_tif.name}`\n- Value: `{v}`"))
            except Exception as e:
                st.warning(_tr(f"像元识别失败: {e}", f"Pixel identify failed: {e}"))

def _build_reasoning_sections(events):
    """Group adjacent messages for compact single-panel reasoning view."""
    grouped = []
    for event in events:
        if not isinstance(event, dict):
            continue
        kb_records = []
        notice_records = []
        raw_kb = event.get("kb_progress")
        if isinstance(raw_kb, list):
            kb_records.extend([x for x in raw_kb if isinstance(x, dict)])
        raw_custom = event.get("custom")
        if isinstance(raw_custom, list):
            kb_records.extend(
                [x for x in raw_custom if isinstance(x, dict) and x.get("event_type") == "kb_progress"]
            )
            notice_records.extend(
                [
                    x
                    for x in raw_custom
                    if isinstance(x, dict)
                    and x.get("event_type")
                    in {"auto_image_understanding_triggered", "auto_pdf_understanding_triggered"}
                ]
            )
        if kb_records:
            if grouped and grouped[-1]["kind"] == "kb_progress":
                grouped[-1]["records"].extend(kb_records)
            else:
                grouped.append({"kind": "kb_progress", "records": list(kb_records)})
        if notice_records:
            if grouped and grouped[-1]["kind"] == "custom_notice":
                grouped[-1]["records"].extend(notice_records)
            else:
                grouped.append({"kind": "custom_notice", "records": list(notice_records)})

        msgs = event.get("messages", [])
        if not isinstance(msgs, list):
            continue

        for msg in msgs:
            md = getattr(msg, "response_metadata", None)
            if not isinstance(md, dict) and isinstance(msg, dict):
                md = msg.get("response_metadata")
            if isinstance(md, dict) and md.get("__is_handoff_back"):
                continue

            if isinstance(msg, AIMessage):
                raw_agent = msg.name or "AI"
                agent = _normalize_subagent_name(raw_agent) or raw_agent
                if "(streaming)" in agent.lower():
                    # Skip transient streaming markers; final messages are rendered below.
                    continue
                if grouped and grouped[-1]["kind"] == "ai" and grouped[-1]["agent"] == agent:
                    grouped[-1]["messages"].append(msg)
                else:
                    grouped.append({"kind": "ai", "agent": agent, "messages": [msg]})
            elif isinstance(msg, ToolMessage):
                tool_name = msg.name or "tool"
                if grouped and grouped[-1]["kind"] == "tool" and grouped[-1]["tool"] == tool_name:
                    grouped[-1]["messages"].append(msg)
                else:
                    grouped.append({"kind": "tool", "tool": tool_name, "messages": [msg]})
            elif isinstance(msg, HumanMessage):
                if grouped and grouped[-1]["kind"] == "human":
                    grouped[-1]["messages"].append(msg)
                else:
                    grouped.append({"kind": "human", "messages": [msg]})
    return grouped


def _dedupe_tool_messages(messages: list[ToolMessage]) -> list[ToolMessage]:
    """Deduplicate repeated tool messages emitted from mixed stream modes."""
    seen = set()
    unique: list[ToolMessage] = []
    for msg in messages:
        if not isinstance(msg, ToolMessage):
            continue
        key = (
            str(getattr(msg, "name", "") or "").strip(),
            str(getattr(msg, "tool_call_id", "") or "").strip(),
            _normalize_content_to_text(getattr(msg, "content", "")),
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(msg)
    return unique


def _normalize_content_to_text(content) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                text = item.get("text") or item.get("content")
                if text:
                    parts.append(str(text))
        return "\n".join(parts)
    if content is None:
        return ""
    return str(content)


def _get_ui_thread_id() -> str:
    return str(st.session_state.get("thread_id", "debug") or "debug")


def _to_ui_relative_path(path_str: str, thread_id: Optional[str] = None) -> str:
    """Convert absolute/local paths into UI-safe relative paths."""
    raw = str(path_str or "").strip()
    if not raw:
        return raw

    tid = str(thread_id or _get_ui_thread_id())
    repo_root = Path.cwd().resolve()
    workspace = storage_manager.get_workspace(tid).resolve()

    candidate = raw.strip("\"'`")
    candidate_norm = candidate.replace("/", "\\")

    # Explicitly normalize any user_data absolute-like path.
    m = re.search(r"(user_data[\\/][^\\/]+[\\/](?:inputs|outputs)[\\/][^\s\"'`]+)", candidate, flags=re.IGNORECASE)
    if m:
        rel_user_data = m.group(1).replace("\\", "/")
        parts = rel_user_data.split("/")
        if len(parts) >= 3:
            parts[1] = tid
        return "/".join(parts)

    try:
        p = Path(candidate)
        if p.is_absolute():
            resolved = p.resolve()
            try:
                rel_to_ws = resolved.relative_to(workspace)
                return rel_to_ws.as_posix()
            except Exception:
                pass
            try:
                rel_to_repo = resolved.relative_to(repo_root)
                rel_repo = rel_to_repo.as_posix()
                if rel_repo.startswith("user_data/"):
                    parts = rel_repo.split("/")
                    if len(parts) >= 3:
                        parts[1] = tid
                        return "/".join(parts)
                return rel_repo
            except Exception:
                pass
    except Exception:
        pass

    if re.search(r"^[A-Za-z]:[\\/]", candidate_norm) or candidate_norm.startswith("/"):
        return os.path.basename(candidate_norm) or raw
    return raw


def _sanitize_paths_in_text(text: str, thread_id: Optional[str] = None) -> str:
    value = str(text or "")
    tid = str(thread_id or _get_ui_thread_id())

    def _replace_abs(m):
        return _to_ui_relative_path(m.group(0), tid)

    value = re.sub(r"[A-Za-z]:[\\/][^\s\"'`]+", _replace_abs, value)
    value = re.sub(r"/(?:home|Users|mnt|tmp)/[^\s\"'`]+", _replace_abs, value)

    def _replace_user_data(m):
        return _to_ui_relative_path(m.group(1), tid)

    value = re.sub(
        r"(?i)(user_data[\\/][^\\/]+[\\/](?:inputs|outputs)[\\/][^\s\"'`]+)",
        _replace_user_data,
        value,
    )
    return value


def _sanitize_paths_in_obj(obj, thread_id: Optional[str] = None, parent_key: str = ""):
    tid = str(thread_id or _get_ui_thread_id())
    key = str(parent_key or "").lower()
    path_key_hit = any(token in key for token in ("path", "dir", "location", "workspace", "output", "file"))

    if isinstance(obj, dict):
        return {k: _sanitize_paths_in_obj(v, tid, str(k)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_paths_in_obj(v, tid, parent_key) for v in obj]
    if isinstance(obj, str):
        if path_key_hit:
            return _to_ui_relative_path(obj, tid)
        return _sanitize_paths_in_text(obj, tid)
    return obj


def _truncate_text(value: str, limit: int = 240) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)] + "..."


def _agent_node_id(agent_name: str) -> str:
    key = re.sub(r"[^a-z0-9]+", "_", str(agent_name or "ai").strip().lower()).strip("_")
    return f"ai_{key or 'default'}"


def _infer_transfer_target_agent(tool_name: str):
    name = str(tool_name or "").strip().lower()
    if not name:
        return None
    if "transfer_to_code_assistant" in name or "transfer_to_code" in name:
        return "Code_Assistant"
    if (
        "transfer_back_to_ntl_engineer" in name
        or "transfer_to_ntl_engineer" in name
        or "transfer_back_to_engineer" in name
    ):
        return "NTL_Engineer"
    m = re.search(r"transfer_to_([a-z0-9_]+)", name)
    if m:
        suffix = m.group(1)
        return "_".join(part.capitalize() for part in suffix.split("_"))
    return None


def _kb_phase_specs() -> list[tuple[str, str, str]]:
    return [
        ("query_received", _tr("已接收查询", "Query Received"), _tr("已进入检索流程", "Entered retrieval flow")),
        (
            "knowledge_retrieval",
            _tr("知识检索", "Knowledge Retrieval"),
            _tr("正在检索知识库候选内容", "Retrieving candidate context from KB"),
        ),
        (
            "workflow_assembly",
            _tr("LLM 响应", "LLM Response"),
            _tr("LLM 正在生成响应", "LLM is generating response"),
        ),
        (
            "structured_output",
            _tr("结构化输出", "Structured Output"),
            _tr("正在准备可渲染 JSON 合约", "Preparing renderable JSON contract"),
        ),
    ]


def _build_kb_progress_nodes_from_records(records: list[dict]) -> list[dict]:
    if not records:
        return []
    latest_by_phase = {}
    for item in records:
        phase = str(item.get("phase", "")).strip()
        if phase:
            latest_by_phase[phase] = item

    nodes = []
    for phase_key, label, default_detail in _kb_phase_specs():
        record = latest_by_phase.get(phase_key)
        status = str((record or {}).get("status", "")).strip().lower()
        done = status == "done"
        running = status == "running"
        error = status == "error"
        meta = (record or {}).get("meta")
        error_summary = ""
        if isinstance(meta, dict):
            error_summary = str(meta.get("error_summary", "")).strip()
        detail = str((record or {}).get("label", "")).strip() or default_detail
        if error and error_summary:
            detail = f"{detail} | {error_summary}"
        nodes.append(
            {
                "key": phase_key,
                "label": label,
                "done": done,
                "running": running,
                "error": error,
                "detail": detail,
            }
        )
    return nodes


def _render_kb_progress_nodes(nodes: list[dict], caption_text: str):
    if not nodes:
        return
    done = sum(1 for n in nodes if n.get("done"))
    total = len(nodes)
    st.caption(caption_text)
    st.progress(done / total if total else 0.0)
    cols = st.columns(total)
    for idx, node in enumerate(nodes):
        icon = "..."
        if node.get("error"):
            icon = "x"
        elif node.get("done"):
            icon = "ok"
        elif node.get("running"):
            icon = "..."
        with cols[idx]:
            st.markdown(f"**{icon} {node.get('label', '')}**")
            if node.get("error"):
                st.caption(node.get("detail", ""))


def _build_reasoning_graph_payload(events, show_sub_steps: bool = False):
    grouped = _build_reasoning_sections(events)
    if not grouped:
        return None
    has_final_kb_tool = any(
        step.get("kind") == "tool"
        and any(
            isinstance(msg, ToolMessage)
            and "ntl_knowledge_base" in str(getattr(msg, "name", "")).strip().lower()
            for msg in (step.get("messages") or [])
        )
        for step in grouped
    )

    nodes = [
        {"data": {"id": "start", "label": "START", "kind": "system"}, "classes": "system"},
    ]
    edges = []
    seen_nodes = {"start"}
    last_anchor = "start"
    last_ai = None
    human_idx = 0
    tool_idx = 0
    last_tool_cluster = None

    def add_node(node_id: str, label: str, kind: str = "default"):
        if node_id in seen_nodes:
            return
        nodes.append(
            {
                "data": {"id": node_id, "label": _truncate_text(label, 140), "kind": kind},
                "classes": kind,
            }
        )
        seen_nodes.add(node_id)

    def set_node_label(node_id: str, label: str):
        for node in nodes:
            data = node.get("data", {})
            if data.get("id") == node_id:
                data["label"] = _truncate_text(label, 140)
                return

    def add_edge(src: str, dst: str, cls: str = "flow"):
        edges.append(
            {
                "data": {"id": f"e_{len(edges) + 1}", "source": src, "target": dst},
                "classes": cls,
            }
        )

    for step in grouped:
        kind = step.get("kind")
        if kind == "human":
            human_idx += 1
            node_id = f"h{human_idx}"
            add_node(node_id, "Human Query", "human")
            add_edge(last_anchor, node_id, "flow")
            last_anchor = node_id
            last_ai = None
            last_tool_cluster = None
            continue

        if kind == "ai":
            agent = str(step.get("agent") or "AI")
            node_id = _agent_node_id(agent)
            add_node(node_id, f"AI: {agent}", "ai")
            prev_ai = last_ai
            if last_anchor.startswith("ai_") and node_id.startswith("ai_") and last_anchor != node_id:
                add_edge(last_anchor, node_id, "handoff_edge")
            elif last_anchor != node_id:
                add_edge(last_anchor, node_id, "flow")
            last_anchor = node_id
            last_ai = node_id
            if prev_ai != node_id:
                # Agent handoff or context switch: avoid cross-agent tool clustering.
                last_tool_cluster = None
            continue

        if kind == "kb_progress":
            if has_final_kb_tool:
                continue
            node_id = f"kbp_{len([n for n in nodes if str((n.get('data') or {}).get('id', '')).startswith('kbp_')]) + 1}"
            add_node(node_id, "KB Progress", "tool_kb")
            add_edge(last_ai or last_anchor, node_id, "tool_call_edge")
            add_edge(node_id, last_ai or last_anchor, "return_edge")
            last_anchor = last_ai or last_anchor
            last_tool_cluster = None
            continue

        if kind == "tool":
            tool_messages = [m for m in step.get("messages", []) if isinstance(m, ToolMessage)]
            if not tool_messages:
                continue

            tool_name = str(getattr(tool_messages[0], "name", "") or "tool")
            call_count = len(tool_messages)
            start_seq = tool_idx + 1
            end_seq = start_seq + call_count - 1
            tool_idx = end_seq

            transfer_target = _infer_transfer_target_agent(tool_name)
            if transfer_target and tool_name.lower().startswith("transfer"):
                target_node = _agent_node_id(transfer_target)
                add_node(target_node, f"AI: {transfer_target}", "ai")
                add_edge(last_ai or last_anchor, target_node, "handoff_edge")
                last_anchor = target_node
                last_ai = target_node
                last_tool_cluster = None
                continue

            caller_anchor = last_ai or last_anchor
            normalized_tool_name = tool_name.strip().lower()
            if (
                isinstance(last_tool_cluster, dict)
                and last_tool_cluster.get("tool_name") == normalized_tool_name
                and last_tool_cluster.get("caller_anchor") == caller_anchor
                and last_tool_cluster.get("node_id") in seen_nodes
            ):
                merged_start = int(last_tool_cluster.get("start_seq") or start_seq)
                merged_count = int(last_tool_cluster.get("count") or 0) + int(call_count)
                merged_end = int(last_tool_cluster.get("end_seq") or merged_start) + int(call_count)
                merged_label = (
                    f"#{merged_start}-{merged_end} {tool_name}*{merged_count}"
                    if merged_count > 1
                    else f"#{merged_start} {tool_name}"
                )
                set_node_label(str(last_tool_cluster.get("node_id")), merged_label)
                last_tool_cluster["count"] = merged_count
                last_tool_cluster["end_seq"] = merged_end
                last_anchor = caller_anchor
                continue

            node_id = f"tc_{start_seq}"
            if call_count > 1:
                tool_label = f"#{start_seq}-{end_seq} {tool_name}*{call_count}"
            else:
                tool_label = f"#{start_seq} {tool_name}"
            add_node(
                node_id,
                _truncate_text(tool_label, 120),
                "tool_kb" if tool_name.strip().lower() == "ntl_knowledge_base" else "tool",
            )
            add_edge(caller_anchor, node_id, "tool_call_edge")
            add_edge(node_id, caller_anchor, "return_edge")
            last_anchor = caller_anchor
            last_tool_cluster = {
                "node_id": node_id,
                "tool_name": normalized_tool_name,
                "caller_anchor": caller_anchor,
                "start_seq": start_seq,
                "end_seq": end_seq,
                "count": call_count,
            }

    add_node("end", "END", "system")
    add_edge(last_anchor, "end", "flow")
    return {"nodes": nodes, "edges": edges, "main_edge_ids": []}


def _escape_dot_label(text: str) -> str:
    value = str(text or "")
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _build_reasoning_dot(payload: dict) -> str:
    nodes = payload.get("nodes") or []
    edges = payload.get("edges") or []
    lines = [
        "digraph ReasoningMap {",
        "rankdir=LR;",
        'graph [bgcolor="#ffffff", splines=true, overlap=false];',
        'node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=10];',
        'edge [color="#475569"];',
    ]
    for node in nodes:
        data = node.get("data", {})
        node_id = str(data.get("id", "n")).replace("-", "_")
        label = _escape_dot_label(data.get("label") or node_id)
        kind = str(data.get("kind", "default"))
        fill = "#f1f5f9"
        if kind == "human":
            fill = "#dbeafe"
        elif kind == "ai":
            fill = "#dcfce7"
        elif kind == "tool":
            fill = "#fef3c7"
        elif kind == "tool_kb":
            fill = "#ccfbf1"
        elif kind == "system":
            fill = "#e2e8f0"
        lines.append(f'{node_id} [label="{label}", fillcolor="{fill}"];')
    for edge in edges:
        data = edge.get("data", {})
        src = str(data.get("source", "")).replace("-", "_")
        dst = str(data.get("target", "")).replace("-", "_")
        cls = str(edge.get("classes", "flow"))
        attrs = []
        if cls == "handoff_edge":
            attrs.extend(['color="#2563eb"', 'penwidth=2.0'])
        elif cls == "return_edge":
            attrs.extend(['style="dashed"', 'color="#0ea5e9"'])
        style = f" [{', '.join(attrs)}]" if attrs else ""
        if src and dst:
            lines.append(f"{src} -> {dst}{style};")
    lines.append("}")
    return "\n".join(lines)


def _json_for_html_script(data) -> str:
    return json.dumps(data, ensure_ascii=False).replace("</", "<\\/")


def _compute_reasoning_graph_signature(events) -> str:
    """Compute a lightweight signature for graph redraw dirty-check."""
    if not isinstance(events, list) or not events:
        return "empty:0"
    last_event = events[-1]
    try:
        last_text = json.dumps(last_event, ensure_ascii=False, sort_keys=True, default=str)
    except Exception:
        last_text = str(last_event)
    last_hash = hashlib.sha1(last_text.encode("utf-8", errors="ignore")).hexdigest()[:12]
    return f"{len(events)}:{last_hash}"


def render_reasoning_map(events, interactive: bool = True, show_sub_steps: bool = False):
    payload = _build_reasoning_graph_payload(events, show_sub_steps=show_sub_steps)
    if not payload:
        st.caption(_tr("暂无可视化推理路径。", "No reasoning graph yet."))
        return

    if not interactive:
        dot = _build_reasoning_dot(payload)
        if dot:
            st.graphviz_chart(dot, width="stretch")
        return

    elements = payload["nodes"] + payload["edges"]
    container_id = f"reasoning-map-{uuid.uuid4().hex[:8]}"
    graph_height = min(700, max(360, 260 + len(payload["nodes"]) * 20))
    js_elements = _json_for_html_script(elements)
    html = f"""
    <div id="{container_id}" style="width:100%;height:{graph_height}px;border:1px solid #d1d5db;border-radius:12px;background:#ffffff;"></div>
    <script src="https://unpkg.com/cytoscape@3.30.2/dist/cytoscape.min.js"></script>
    <script>
    (function() {{
      const root = document.getElementById("{container_id}");
      const elements = {js_elements};
      if (!root) return;
      function render() {{
        if (typeof cytoscape !== "function") {{
          root.innerHTML = "<div style='padding:12px;color:#cbd5e1'>Graph library unavailable.</div>";
          return;
        }}
        if (root.__cy) root.__cy.destroy();
        const cy = cytoscape({{
          container: root,
          elements: elements,
          style: [
            {{ selector: "node", style: {{ "label": "data(label)", "font-size": 10, "color": "#111827", "text-wrap": "wrap", "text-max-width": 160, "shape": "round-rectangle", "background-color": "#f3f4f6", "border-width": 1, "border-color": "#6b7280" }} }},
            {{ selector: "node.human", style: {{ "background-color": "#dbeafe" }} }},
            {{ selector: "node.ai", style: {{ "background-color": "#dcfce7" }} }},
            {{ selector: "node.tool", style: {{ "background-color": "#fef3c7" }} }},
            {{ selector: "node.tool_kb", style: {{ "background-color": "#ccfbf1" }} }},
            {{ selector: "edge", style: {{ "curve-style": "bezier", "target-arrow-shape": "triangle", "line-color": "#6b7280", "target-arrow-color": "#6b7280", "width": 1.4 }} }},
            {{ selector: "edge.handoff_edge", style: {{ "line-color": "#1d4ed8", "target-arrow-color": "#1d4ed8", "width": 2.0 }} }},
            {{ selector: "edge.return_edge", style: {{ "line-style": "dashed", "line-color": "#0f766e", "target-arrow-color": "#0f766e" }} }}
          ],
          layout: {{ name: "breadthfirst", directed: true, padding: 20, spacingFactor: 1.15 }}
        }});
        root.__cy = cy;
        setTimeout(() => {{
          try {{ cy.fit(undefined, 24); }} catch (e) {{}}
        }}, 0);
      }}
      render();
      const ro = new ResizeObserver(() => {{ if (root.__cy) {{ root.__cy.resize(); root.__cy.fit(undefined, 24); }} }});
      ro.observe(root);
      const mo = new MutationObserver(() => {{ if (root.__cy) {{ root.__cy.resize(); }} }});
      mo.observe(document.body, {{ childList: true, subtree: true }});
      document.addEventListener("visibilitychange", () => {{ if (document.visibilityState === "visible" && root.__cy) {{ root.__cy.resize(); root.__cy.fit(undefined, 24); }} }});
    }})();
    </script>
    """
    _render_html_iframe(html, height=graph_height + 8, scrolling=False)


def _render_code_assistant_message(raw_content: str) -> None:
    """Keep review status concise while preserving details in a collapsed panel."""
    raw_text = raw_content if isinstance(raw_content, str) else str(raw_content)
    parsed = raw_content if isinstance(raw_content, (dict, list)) else None
    rest = ""
    if parsed is None:
        parsed, rest = _extract_json(raw_text)

    status = ""
    summary = ""
    if isinstance(parsed, dict):
        status = str(parsed.get("status") or parsed.get("stage") or "").strip()
        summary = str(
            parsed.get("summary")
            or parsed.get("recommended_next_action")
            or parsed.get("error_message")
            or ""
        ).strip()
    if not summary:
        lines = [line.strip() for line in str(rest or raw_text).splitlines() if line.strip()]
        summary = lines[0] if lines else _tr("复核详情已记录", "Review details recorded")
    summary = _sanitize_paths_in_text(summary)
    if len(summary) > 180:
        summary = summary[:177] + "..."
    status_label = status or _tr("复核更新", "Review update")
    st.caption(f"{status_label}: {summary}")

    with st.expander(_tr("查看复核详情", "Review details"), expanded=False):
        if isinstance(parsed, (dict, list)):
            if isinstance(rest, str) and rest.strip():
                st.write(_sanitize_paths_in_text(rest))
            st.json(_sanitize_paths_in_obj(parsed))
        else:
            st.code(_sanitize_paths_in_text(raw_text), language="text")


def _extract_todos_payload(raw_content) -> list[dict]:
    """Best-effort extraction for write_todos outputs."""
    todos = []

    if isinstance(raw_content, list):
        todos = raw_content
    elif isinstance(raw_content, dict):
        # Prefer structured runtime update payload when available.
        update = raw_content.get("update")
        if isinstance(update, dict) and isinstance(update.get("todos"), list):
            todos = update.get("todos") or []
        elif isinstance(raw_content.get("todos"), list):
            todos = raw_content.get("todos") or []
    else:
        text = str(raw_content or "").strip()
        if not text:
            return []

        parsed = None
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = None

        if isinstance(parsed, dict) and isinstance(parsed.get("todos"), list):
            todos = parsed.get("todos") or []
        elif isinstance(parsed, list):
            todos = parsed
        else:
            start = text.find("[")
            end = text.rfind("]")
            if start != -1 and end != -1 and end > start:
                chunk = text[start : end + 1]
                try:
                    maybe = ast.literal_eval(chunk)
                    if isinstance(maybe, list):
                        todos = maybe
                except Exception:
                    todos = []

    def _normalize_todo_status(raw_status: str) -> str:
        status = str(raw_status or "").strip().lower()
        if status in {"done", "completed", "complete", "finished"}:
            return "completed"
        if status in {"in_progress", "in-progress", "running", "active", "working"}:
            return "in_progress"
        return "pending"

    normalized = []
    for item in todos:
        if isinstance(item, dict):
            content = str(item.get("content", "") or "").strip()
            status = _normalize_todo_status(item.get("status", "pending"))
        else:
            content = str(item or "").strip()
            status = "pending"
        if not content:
            continue
        normalized.append({"content": content, "status": status})
    return normalized


def render_write_todos_output(raw_content) -> None:
    todos = _extract_todos_payload(raw_content)
    if not todos:
        st.write(_sanitize_paths_in_text(str(raw_content)))
        return

    status_order = {"completed": 0, "in_progress": 1, "pending": 2}
    todos = sorted(todos, key=lambda x: status_order.get(x["status"], 9))
    total = len(todos)
    completed = sum(1 for x in todos if x["status"] == "completed")
    running = sum(1 for x in todos if x["status"] == "in_progress")
    pending = total - completed - running

    st.caption(
        _tr(
            f"Todo list: total {total} | completed {completed} | in-progress {running} | pending {pending}",
            f"Todo list: total {total} | completed {completed} | in-progress {running} | pending {pending}",
        )
    )

    with st.expander(_tr("View task list", "View task list"), expanded=True):
        for i, item in enumerate(todos, start=1):
            status = item["status"]
            if status == "completed":
                badge = "[completed]"
            elif status == "in_progress":
                badge = "[in_progress]"
            else:
                badge = "[pending]"
            st.markdown(f"{i}. {badge}  {item['content']}")


def render_reasoning_content(events):
    """Render one-round reasoning in a single panel (no Step 1/2/3)."""
    grouped = _build_reasoning_sections(events)
    if not grouped:
        st.caption(_tr("等待推理事件...", "Waiting for reasoning events..."))
        return
    has_final_kb_response = any(
        (
            step.get("kind") == "ai"
            and str(step.get("agent") or "").strip().lower()
            in {"knowledge_base_searcher", "knowledge_base_subagent"}
            and any(
                str(
                    _normalize_content_to_text(
                        _strip_legacy_stream_marker(getattr(msg, "content", ""))
                    )
                    or ""
                ).strip()
                for msg in (step.get("messages") or [])
            )
        )
        or (
            step.get("kind") == "tool"
            and any(
                str(getattr(msg, "name", "") or "").strip().lower() == "ntl_knowledge_base"
                for msg in (step.get("messages") or [])
            )
        )
        for step in grouped
    )

    for step in grouped:
        if step["kind"] == "human":
            for msg in step["messages"]:
                render_event_human(msg.content)
                st.markdown("<hr style='margin: 10px 0; border: 1px dashed #64748b;'>", unsafe_allow_html=True)
        elif step["kind"] == "ai":
            agent_name = _normalize_subagent_name(step["agent"]) or step["agent"]
            render_label_ai(agent_name)
            effective_messages = []
            for msg in step["messages"]:
                msg_content = _normalize_content_to_text(_strip_legacy_stream_marker(msg.content))
                if not isinstance(msg_content, str):
                    msg_content = str(msg_content)
                if not msg_content.strip():
                    continue
                effective_messages.append(msg_content)

            if not effective_messages:
                st.markdown("<hr style='margin: 10px 0; border: 1px dashed #64748b;'>", unsafe_allow_html=True)
                continue

            for msg_content in effective_messages:
                if agent_name.lower() == "data_searcher":
                    render_data_searcher_output(msg_content)
                elif agent_name.lower() == "code_assistant":
                    _render_code_assistant_message(msg_content)
                elif agent_name.lower() == "knowledge_base_searcher" or str(agent_name or "").strip().lower() == "knowledge_base_subagent":
                    render_kb_output(msg_content)
                else:
                    st.markdown(
                        f"<div style='margin-left:15px;font-size:16px;'>{msg_content}</div>",
                        unsafe_allow_html=True,
                    )
            st.markdown("<hr style='margin: 10px 0; border: 1px dashed #64748b;'>", unsafe_allow_html=True)
        elif step["kind"] == "tool":
            tool_messages = _dedupe_tool_messages([m for m in step["messages"] if isinstance(m, ToolMessage)])
            for msg in tool_messages:
                exp_title = _tr(f"工具输出: {msg.name}", f"Tool Output: {msg.name}")
                with st.expander(exp_title, expanded=False):
                    tool_name_norm = str(msg.name or "").strip().lower()
                    if tool_name_norm == "ntl_knowledge_base":
                        render_kb_output(msg.content)
                    elif tool_name_norm == "write_todos":
                        render_write_todos_output(msg.content)
                    elif tool_name_norm in {
                        "uploaded_pdf_understanding_tool",
                        "uploaded_image_understanding_tool",
                        "uploaded_file_understanding_tool",
                    }:
                        render_uploaded_understanding_output(msg.content, tool_name=str(msg.name or ""))
                    elif any(
                        token in str(msg.name or "").strip().lower()
                        for token in (
                            "ntl_solution_knowledge",
                            "ntl_literature_knowledge",
                            "ntl_code_knowledge",
                            "solution_knowledge",
                            "literature_knowledge",
                            "code_knowledge",
                        )
                    ):
                        render_kb_tool_output(msg.content, tool_name=str(msg.name or ""))
                    else:
                        try:
                            st.json(_sanitize_paths_in_obj(json.loads(msg.content)))
                        except Exception:
                            st.write(_sanitize_paths_in_text(str(msg.content)))
            st.markdown("<hr style='margin: 10px 0; border: 1px dashed #64748b;'>", unsafe_allow_html=True)
        elif step["kind"] == "kb_progress":
            if has_final_kb_response:
                continue
            records = [x for x in step.get("records", []) if isinstance(x, dict)]
            if not records:
                continue
            nodes = _build_kb_progress_nodes_from_records(records)
            if not nodes:
                continue
            exp_title = _tr("工具输出: NTL_Knowledge_Base", "Tool Output: NTL_Knowledge_Base")
            with st.expander(exp_title, expanded=False):
                _render_kb_progress_nodes(
                    nodes,
                    _tr("NTL_Knowledge_Base_Searcher 节点进度（流式）", "NTL_Knowledge_Base_Searcher Node Progress (Streaming)"),
                )
            st.markdown("<hr style='margin: 10px 0; border: 1px dashed #64748b;'>", unsafe_allow_html=True)
        elif step["kind"] == "custom_notice":
            for record in (step.get("records") or []):
                if not isinstance(record, dict):
                    continue
                et = str(record.get("event_type", "")).strip()
                if et not in {"auto_image_understanding_triggered", "auto_pdf_understanding_triggered"}:
                    continue
                files = record.get("files") or []
                if isinstance(files, str):
                    files = [files]
                files = [str(x).strip() for x in files if str(x).strip()]
                reason = str(record.get("trigger_reason", "")).strip()
                suffix = f" ({reason})" if reason else ""
                if et == "auto_pdf_understanding_triggered":
                    msg = _tr(
                        f"已自动触发 PDF 理解: {', '.join(files) if files else 'pdf'}{suffix}",
                        f"Auto PDF understanding triggered: {', '.join(files) if files else 'pdf'}{suffix}",
                    )
                else:
                    msg = _tr(
                        f"已自动触发图像理解: {', '.join(files) if files else 'image'}{suffix}",
                        f"Auto image understanding triggered: {', '.join(files) if files else 'image'}{suffix}",
                    )
                st.info(msg)
            st.markdown("<hr style='margin: 10px 0; border: 1px dashed #64748b;'>", unsafe_allow_html=True)


def _classify_code_assistant_stage(tool_name, tool_payload):
    name = str(tool_name or "").strip().lower()
    if not name:
        return None

    def _status_from_payload(payload):
        if isinstance(payload, dict):
            return str(payload.get("status", "")).strip().lower()
        if isinstance(payload, str):
            try:
                obj = json.loads(payload)
                if isinstance(obj, dict):
                    return str(obj.get("status", "")).strip().lower()
            except Exception:
                return ""
        return ""

    status = _status_from_payload(tool_payload)
    if name in {"geocode_cot_validation_tool", "execute_geospatial_script_tool"}:
        if status == "success":
            return "Success"
        if status in {"needs_engineer_decision", "fail"}:
            return "Escalate"
        return "Validate/Execute"
    if name in {"transfer_back_to_ntl_engineer", "transfer_to_ntl_engineer"}:
        return "Escalate"
    return None


def _collect_workspace_output_mismatch_records() -> list[dict]:
    records: list[dict] = []
    current_thread = str(st.session_state.get("thread_id", "debug"))

    def _scan_events(events):
        if not isinstance(events, list):
            return
        for event in events:
            if not isinstance(event, dict):
                continue
            for msg in (event.get("messages") or []):
                if not isinstance(msg, ToolMessage):
                    continue
                name = str(getattr(msg, "name", "") or "")
                if "execute_geospatial_script_tool" not in name:
                    continue
                payload, _ = _extract_json(getattr(msg, "content", ""))
                if not isinstance(payload, dict):
                    continue
                audit = payload.get("artifact_audit")
                if not isinstance(audit, dict):
                    continue
                recovered = bool(payload.get("cross_workspace_recovered", False))
                if bool(audit.get("pass", True)) and not recovered:
                    continue
                records.append(
                    {
                        "thread_id": str(audit.get("thread_id") or current_thread),
                        "workspace_outputs_dir": str(audit.get("workspace_outputs_dir") or ""),
                        "out_of_workspace_paths": list(audit.get("out_of_workspace_paths") or []),
                        "cross_workspace_recovered": recovered,
                        "auto_migrated_files": list(payload.get("auto_migrated_files") or []),
                        "recovery_note": str(payload.get("recovery_note") or ""),
                    }
                )

    _scan_events(st.session_state.get("analysis_logs", []))
    history = st.session_state.get("analysis_history", [])
    if isinstance(history, list):
        for item in history[-3:]:
            if isinstance(item, dict):
                _scan_events(item.get("logs", []))
    return records


def _render_output_workspace_mismatch_notice():
    mismatches = _collect_workspace_output_mismatch_records()
    if not mismatches:
        return
    latest = mismatches[-1]
    workspace = _to_ui_relative_path(str(latest.get("workspace_outputs_dir") or ""))
    out_paths = [_to_ui_relative_path(str(x)) for x in (latest.get("out_of_workspace_paths") or []) if str(x).strip()]
    recovered = bool(latest.get("cross_workspace_recovered", False))
    migrated_paths = [
        _to_ui_relative_path(str(x))
        for x in (latest.get("auto_migrated_files") or [])
        if str(x).strip()
    ]

    if recovered:
        st.info(
            _tr(
                "检测到跨线程输出并已自动恢复到当前会话 outputs。",
                "Cross-thread outputs detected and auto-recovered to current session outputs.",
            )
        )
    else:
        st.warning(
            _tr(
                "检测到跨线程输出：部分结果未写入当前会话 outputs，可能导致本页看不到文件。",
                "Cross-thread outputs detected: some files were written outside current session outputs.",
            )
        )

    if workspace:
        st.caption(_tr(f"当前线程 outputs: {workspace}", f"Current thread outputs: {workspace}"))
    if recovered and migrated_paths:
        st.caption(_tr("已恢复文件：", "Recovered files:"))
        for p in migrated_paths:
            st.code(p, language="text")
        note = str(latest.get("recovery_note") or "").strip()
        if note:
            st.caption(_sanitize_paths_in_text(note))
        return

    if out_paths:
        st.caption(_tr("实际落盘路径：", "Actual output paths:"))
        for p in out_paths:
            st.code(p, language="text")
        dst = workspace or f"user_data/{st.session_state.get('thread_id', 'debug')}/outputs"
        copy_cmd = "\n".join(
            [f"Copy-Item \"{p}\" \"{dst}\" -Force" for p in out_paths]
        )
        st.caption(_tr("建议修复命令（可复制）：", "Suggested recovery commands (copy):"))
        st.code(copy_cmd, language="powershell")


def _render_output_preview():
    workspace = storage_manager.get_workspace(st.session_state.get("thread_id", "debug"))
    output_dir = workspace / "outputs"
    files = [f for f in output_dir.glob("*.*") if f.is_file()] if output_dir.exists() else []
    if not files:
        st.info(_tr("暂无输出文件。", "No output files available."))
        return

    selected = st.selectbox(_tr("预览输出文件", "Preview Output"), options=files, format_func=lambda p: p.name)
    suffix = selected.suffix.lower()
    if suffix == ".csv":
        try:
            df = pd.read_csv(selected)
            st.dataframe(df, width="stretch", height=360)
        except Exception as e:
            st.error(_tr(f"预览失败 {selected.name}: {e}", f"Failed to preview {selected.name}: {e}"))
    elif suffix in [".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"]:
        st.image(str(selected), width="stretch")
    elif suffix in [".tif", ".tiff"]:
        with rasterio.open(selected) as src:
            st.caption(f"CRS: {src.crs} | Size: {src.width} x {src.height} | Bands: {src.count}")
    elif suffix == ".shp":
        try:
            gdf = gpd.read_file(selected)
            st.caption(
                _tr(
                    f"要素数: {len(gdf)} | CRS: {gdf.crs}",
                    f"Features: {len(gdf)} | CRS: {gdf.crs}",
                )
            )
            table_df = gdf.drop(columns=["geometry"], errors="ignore")
            if table_df.empty:
                st.info(_tr("属性表为空（仅几何列）。", "Attribute table is empty (geometry-only)."))
            else:
                preview_rows = min(500, len(table_df))
                st.caption(
                    _tr(
                        f"属性表预览（前 {preview_rows} 行）",
                        f"Attribute table preview (first {preview_rows} rows)",
                    )
                )
                st.dataframe(table_df.head(preview_rows), width="stretch", height=360)
        except Exception as e:
            st.error(_tr(f"预览失败 {selected.name}: {e}", f"Failed to preview {selected.name}: {e}"))
    elif suffix == ".py":
        try:
            code_text = selected.read_text(encoding="utf-8")
            st.code(code_text, language="python")
        except Exception:
            try:
                code_text = selected.read_text(encoding="gbk")
                st.code(code_text, language="python")
            except Exception as e:
                st.error(_tr(f"预览失败 {selected.name}: {e}", f"Failed to preview {selected.name}: {e}"))
    elif suffix in [".json", ".geojson"]:
        try:
            payload = json.loads(selected.read_text(encoding="utf-8"))
            st.json(payload)
        except Exception:
            try:
                text = selected.read_text(encoding="utf-8", errors="replace")
                st.code(text, language="json")
            except Exception as e:
                st.error(_tr(f"预览失败 {selected.name}: {e}", f"Failed to preview {selected.name}: {e}"))
    elif suffix in [".jsonl", ".ndjson"]:
        rows = []
        bad_lines = 0
        preview_limit = 500
        try:
            with selected.open("r", encoding="utf-8") as f:
                for i, line in enumerate(f, start=1):
                    if i > preview_limit:
                        break
                    text = line.strip()
                    if not text:
                        continue
                    try:
                        obj = json.loads(text)
                        if isinstance(obj, dict):
                            rows.append(obj)
                        else:
                            rows.append({"_value": obj})
                    except Exception:
                        bad_lines += 1
                        rows.append({"_line": i, "_raw": text})
            st.caption(
                _tr(
                    f"JSONL 预览：最多显示前 {preview_limit} 行；解析失败 {bad_lines} 行。",
                    f"JSONL preview: first {preview_limit} lines max; parse failures: {bad_lines}.",
                )
            )
            if rows:
                st.dataframe(pd.DataFrame(rows), width="stretch", height=360)
            else:
                st.info(_tr("JSONL 文件为空。", "JSONL file is empty."))
        except Exception as e:
            st.error(_tr(f"预览失败 {selected.name}: {e}", f"Failed to preview {selected.name}: {e}"))
    elif suffix in [".txt", ".log", ".md"]:
        preview_limit_chars = 20000
        try:
            text = selected.read_text(encoding="utf-8", errors="replace")
            if len(text) > preview_limit_chars:
                st.caption(
                    _tr(
                        f"文本过长，仅显示前 {preview_limit_chars} 个字符。",
                        f"Text is long. Showing first {preview_limit_chars} characters only.",
                    )
                )
                text = text[:preview_limit_chars]
            st.text(text)
        except Exception as e:
            st.error(_tr(f"预览失败 {selected.name}: {e}", f"Failed to preview {selected.name}: {e}"))
    elif suffix == ".zip":
        try:
            with zipfile.ZipFile(selected, "r") as zf:
                names = zf.namelist()
            if not names:
                st.info(_tr("ZIP 文件为空。", "ZIP file is empty."))
            else:
                st.caption(_tr(f"ZIP 内文件数: {len(names)}", f"ZIP entries: {len(names)}"))
                st.dataframe(
                    pd.DataFrame({"file": names}),
                    width="stretch",
                    height=360,
                    hide_index=True,
                )
        except Exception as e:
            st.error(_tr(f"预览失败 {selected.name}: {e}", f"Failed to preview {selected.name}: {e}"))
    else:
        st.caption(_tr("该文件类型暂不支持预览，请在 Data Center 下载。", "Preview is not available for this file type. Use Data Center to download."))


def _render_chat_history_with_run_notice() -> None:
    current_thread_running = _is_current_thread_running()
    current_thread_stopping = _is_current_thread_stopping()
    show_history(
        st.session_state.get("chat_history", []),
        show_running_notice_under_latest_user=current_thread_running and not current_thread_stopping,
        show_stopping_notice_under_latest_user=current_thread_stopping,
    )


_SUBAGENT_CARD_ORDER = [
    "Data_Searcher",
    "Code_Assistant",
    "NTL_Engineer",
]


def _normalize_subagent_name(raw_name: str) -> str:
    name = str(raw_name or "").strip().lower()
    if not name:
        return ""
    if "knowledge_base" in name:
        return "Knowledge_Base_Searcher"
    if "data_searcher" in name:
        return "Data_Searcher"
    if "code_assistant" in name:
        return "Code_Assistant"
    if "engineer" in name or name in {"ntl-gpt", "general-purpose", "general_purpose"}:
        return "NTL_Engineer"
    return ""


def _extract_latest_agent_text(logs: list, target_agent: str) -> str:
    target = str(target_agent or "").strip().lower()
    for event in reversed(logs or []):
        if not isinstance(event, dict):
            continue
        for msg in reversed(event.get("messages", []) or []):
            if not isinstance(msg, AIMessage):
                continue
            msg_agent = _normalize_subagent_name(getattr(msg, "name", "") or "")
            if str(msg_agent).strip().lower() != target:
                continue
            text = _normalize_content_to_text(_strip_legacy_stream_marker(getattr(msg, "content", "")))
            text = _sanitize_paths_in_text(str(text or "")).strip()
            if text:
                return " ".join(text.split())[:96]
    return ""


def _display_agent_label(agent_name: str) -> str:
    mapping = {
        "Knowledge_Base_Searcher": "Knowledge Base",
        "Data_Searcher": "Data Searcher",
        "Code_Assistant": "Code Assistant",
        "NTL_Engineer": "NTL Engineer",
    }
    return mapping.get(str(agent_name or "").strip(), str(agent_name or "").replace("_", " "))


def _code_assistant_review_stage(logs: list, is_running: bool) -> str:
    current_agent = ""
    encountered = False
    stage = "not_requested"

    for event in logs or []:
        if not isinstance(event, dict):
            continue
        for msg in event.get("messages", []) or []:
            if isinstance(msg, AIMessage):
                normalized_agent = _normalize_subagent_name(getattr(msg, "name", "") or "")
                if normalized_agent:
                    current_agent = normalized_agent
                if current_agent != "Code_Assistant":
                    continue
                encountered = True
                stage = "reviewing"
                content = _normalize_content_to_text(getattr(msg, "content", "")).lower()
                if "needs_engineer_decision" in content or "needs decision" in content:
                    stage = "needs_decision"
                elif '"status": "failed"' in content or '"status":"failed"' in content:
                    stage = "failed"
                elif '"status": "done"' in content or '"status":"done"' in content:
                    stage = "done"
                elif "validating outputs" in content or "output validation" in content:
                    stage = "validating"
                elif "sample testing" in content or "sample test" in content:
                    stage = "testing"
                elif "static preflight" in content or "preflight" in content:
                    stage = "preflight"

                for call in getattr(msg, "tool_calls", None) or []:
                    call_name = str(call.get("name") or "").strip().lower() if isinstance(call, dict) else ""
                    if "geocode_cot_validation" in call_name:
                        stage = "testing"
                    elif "execute_geospatial_script" in call_name:
                        stage = "executing"
                    elif "geocode_knowledge_recipes" in call_name:
                        stage = "reviewing"
            elif isinstance(msg, ToolMessage) and current_agent == "Code_Assistant":
                encountered = True
                tool_name = str(getattr(msg, "name", "") or "").strip().lower()
                if "geocode_cot_validation" in tool_name:
                    stage = "testing"
                elif "execute_geospatial_script" in tool_name:
                    payload, _ = _extract_json(_normalize_content_to_text(getattr(msg, "content", "")))
                    tool_status = str(payload.get("status") or "").strip().lower() if isinstance(payload, dict) else ""
                    if tool_status in {"success", "success_with_warnings", "planned"}:
                        stage = "validating"
                    elif tool_status == "fail":
                        stage = "needs_decision"

    if not encountered:
        return "not_requested"
    if not is_running and stage not in {"needs_decision", "failed"}:
        return "done"
    if is_running and current_agent != "Code_Assistant" and stage not in {"needs_decision", "failed"}:
        return "done"
    return stage


def _build_subagent_lifecycle_state(logs: list, is_running: bool, last_terminal_kind: str = "") -> dict:
    state = {
        name: {"status": "pending", "summary": ""}
        for name in _SUBAGENT_CARD_ORDER
    }
    state["Code_Assistant"]["status"] = "not_requested"

    grouped = _build_reasoning_sections(logs or [])
    encountered: list[str] = []
    for step in grouped:
        if step.get("kind") != "ai":
            continue
        agent = _normalize_subagent_name(step.get("agent", ""))
        if agent and agent not in encountered:
            encountered.append(agent)

    if not encountered:
        if is_running:
            state["NTL_Engineer"]["status"] = "running"
            state["NTL_Engineer"]["summary"] = _tr("任务已启动", "Run started")
        return state

    for idx, agent in enumerate(encountered):
        # Keep cards compact: only show detail for currently active (or final) agent.
        state[agent]["summary"] = ""
        if idx < len(encountered) - 1:
            state[agent]["status"] = "done"
        else:
            state[agent]["status"] = "running" if is_running else "done"
            state[agent]["summary"] = _extract_latest_agent_text(logs, agent)

    terminal = str(last_terminal_kind or "").strip().lower()
    if terminal == "error":
        state["NTL_Engineer"]["status"] = "error"
    elif terminal == "interrupted":
        state["NTL_Engineer"]["status"] = "interrupted"

    state["Code_Assistant"]["status"] = _code_assistant_review_stage(logs, is_running=is_running)
    state["Code_Assistant"]["summary"] = ""

    return state


def _render_subagent_lifecycle_cards(logs: list, is_running: bool, last_terminal_kind: str = "") -> None:
    lifecycle = _build_subagent_lifecycle_state(logs, is_running=is_running, last_terminal_kind=last_terminal_kind)
    status_map = {
        "pending": ("#94a3b8", _tr("待命", "Pending")),
        "not_requested": ("#94a3b8", _tr("未请求复核", "Not requested")),
        "reviewing": ("#60a5fa", _tr("审阅中", "Reviewing")),
        "preflight": ("#60a5fa", _tr("预检中", "Preflight")),
        "testing": ("#a78bfa", _tr("样本测试", "Sample testing")),
        "executing": ("#38bdf8", _tr("执行中", "Executing")),
        "validating": ("#2dd4bf", _tr("验证输出", "Validating outputs")),
        "needs_decision": ("#fbbf24", _tr("需要决策", "Needs decision")),
        "failed": ("#fb7185", _tr("复核失败", "Failed")),
        "running": ("#60a5fa", _tr("运行中", "Running")),
        "done": ("#34d399", _tr("完成", "Done")),
        "error": ("#fb7185", _tr("错误", "Error")),
        "interrupted": ("#fbbf24", _tr("中断", "Interrupted")),
    }
    # st.markdown("<div style='margin-bottom:1px;'>", unsafe_allow_html=True)
    cols = st.columns(len(_SUBAGENT_CARD_ORDER), gap="small")
    for idx, name in enumerate(_SUBAGENT_CARD_ORDER):
        item = lifecycle.get(name, {})
        raw_status = str(item.get("status", "pending")).strip().lower()
        color, label = status_map.get(raw_status, status_map["pending"])
        label_name = _display_agent_label(name)
        with cols[idx]:
            st.markdown(
                (
                    "<div style='border:1px solid rgba(148,163,184,0.35);border-radius:10px;padding:5px 8px;"
                    "background:rgba(15,23,42,0.18);min-height:38px;display:flex;flex-direction:column;gap:1px;'>"
                    f"<div style='font-size:12px;font-weight:700;color:#dbeafe;line-height:1.1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;'>{label_name}</div>"
                    f"<div style='font-size:12px;font-weight:700;color:{color};line-height:1.1;'>● {label}</div>"
                    "</div>"
                ),
                unsafe_allow_html=True,
            )
    st.markdown("</div>", unsafe_allow_html=True)


def _rerun_app():
    try:
        st.rerun(scope="app")
    except TypeError:
        st.rerun()


def render_content_layout():
    """Render dual-column layout: chat and analysis/map/results."""
    workspace = storage_manager.get_workspace(st.session_state.get("thread_id", "debug"))
    _ = app_logic.consume_active_run_events()
    if bool(st.session_state.pop("ui_force_refresh_once", False)):
        _rerun_app()
    # st.markdown(
    #     f"<div class='ntl-card'>"
    #     f"<b>{_tr('工作空间', 'Workspace')}</b><br><span style='color:#61717a;font-size:0.88rem;'>{workspace}</span></div>",
    #     unsafe_allow_html=True,
    # )

    col_chat, col_analysis = st.columns([0.58, 0.42], gap="medium")

    chat_live_placeholder = None
    lifecycle_placeholder = None
    reasoning_live_placeholder = None
    reasoning_graph_live_placeholder = None

    with col_chat:
        chat_container = st.container(height=getattr(app_state, "CHAT_CONTAINER_HEIGHT", 640))
        with chat_container:
            chat_live_placeholder = st.empty()
            chat_live_placeholder.empty()
            with chat_live_placeholder.container():
                _render_chat_history_with_run_notice()
        chat_input_value = get_user_input(disabled=_is_current_thread_stopping())
        user_question, chat_files = _extract_chat_input_text_and_files(chat_input_value)
        if chat_files:
            thread_id = str(st.session_state.get("thread_id", "debug"))
            save_result = _save_chat_input_files_to_workspace(chat_files, thread_id)
            saved = save_result.get("saved", []) or []
            errors = save_result.get("errors", []) or []
            if saved:
                st.success(
                    _tr(
                        f"已从输入框上传到 inputs/: {', '.join(saved)}",
                        f"Uploaded from chat input to inputs/: {', '.join(saved)}",
                    )
                )
            for err in errors:
                st.warning(_sanitize_paths_in_text(str(err)))
            if saved and not user_question:
                st.session_state.chat_history.append(
                    (
                        "assistant",
                        _tr(
                            f"已接收并保存文件到 `inputs/`：{', '.join(saved)}。请继续提问我如何分析这些文件。",
                            f"Files saved to `inputs/`: {', '.join(saved)}. Ask what you want me to analyze from them.",
                        ),
                    )
                )
                st.rerun()
        pending_question = st.session_state.pop("pending_question", None) if "pending_question" in st.session_state else None
        if pending_question and not user_question:
            user_question = pending_question
        runtime_notice = st.session_state.pop("runtime_recovered_notice", None)
        if runtime_notice:
            st.info(_tr("检测到上轮任务状态残留，已自动恢复，可继续提问。", runtime_notice))

    with col_analysis:
        analysis_panel = st.container(height=getattr(app_state, "ANALYSIS_CONTAINER_HEIGHT", 640))
        with analysis_panel:
            tab_reasoning, tab_reasoning_graph, tab_map, tab_outputs = st.tabs([
                _tr("推理过程", "Reasoning"),
                _tr("推理图谱", "Reasoning Graph"),
                _tr("地图视图", "Map View"),
                _tr("结果预览", "Result Preview"),
            ])

            reasoning_placeholder = None
            reasoning_graph_placeholder = None
            reasoning_graph_show_sub_steps = False
            with tab_reasoning:
                try:
                    lifecycle_placeholder = st.empty()
                    lifecycle_placeholder.empty()
                    reasoning_live_placeholder = st.empty()
                    reasoning_live_placeholder.empty()
                    reasoning_placeholder = reasoning_live_placeholder
                    history = st.session_state.get("analysis_history", [])
                    is_running_now = st.session_state.get("is_running", False)
                    last_terminal_kind = str(st.session_state.get("run_last_terminal_kind", "") or "")
                    with lifecycle_placeholder.container():
                        _render_subagent_lifecycle_cards(
                            st.session_state.get("analysis_logs", []),
                            is_running=is_running_now,
                            last_terminal_kind=last_terminal_kind,
                        )
                    if history and not user_question and not is_running_now:
                        for item in reversed(history):
                            q = str(item.get("question", "")).strip()
                            title_q = (q[:56] + "...") if len(q) > 56 else q
                            title = _tr(
                                f"历史推理: {title_q or '上一轮'}",
                                f"Previous Reasoning: {title_q or 'Previous round'}",
                            )
                            with st.expander(title, expanded=False):
                                render_reasoning_content(item.get("logs", []))
                    if st.session_state.analysis_logs and not user_question:
                        with reasoning_placeholder.container():
                            with st.expander(_tr("本轮推理过程", "Reasoning Flow"), expanded=True):
                                render_reasoning_content(st.session_state.analysis_logs)
                except Exception as e:
                    st.error(_tr(f"推理面板渲染异常: {e}", f"Reasoning panel render error: {e}"))

            with tab_reasoning_graph:
                try:
                    reasoning_graph_placeholder = st.empty()
                    reasoning_graph_placeholder.empty()
                    reasoning_graph_live_placeholder = reasoning_graph_placeholder
                    history = st.session_state.get("analysis_history", [])
                    is_running_now = st.session_state.get("is_running", False)
                    if history and not user_question and not is_running_now:
                        for item in reversed(history):
                            q = str(item.get("question", "")).strip()
                            title_q = (q[:56] + "...") if len(q) > 56 else q
                            title = _tr(
                                f"历史图谱: {title_q or '上一轮'}",
                                f"Previous Graph: {title_q or 'Previous round'}",
                            )
                            with st.expander(title, expanded=False):
                                render_reasoning_map(
                                    item.get("logs", []),
                                    interactive=False,
                                    show_sub_steps=reasoning_graph_show_sub_steps,
                                )
                    if st.session_state.analysis_logs and not user_question:
                        with reasoning_graph_placeholder.container():
                            with st.expander(_tr("本轮推理图谱", "Reasoning Graph"), expanded=True):
                                render_reasoning_map(
                                    st.session_state.analysis_logs,
                                    interactive=False,
                                    show_sub_steps=reasoning_graph_show_sub_steps,
                                )
                except Exception as e:
                    st.error(_tr(f"图谱面板渲染异常: {e}", f"Graph panel render error: {e}"))

            with tab_map:
                try:
                    render_map_view()
                except Exception as e:
                    st.error(_tr(f"地图面板渲染异常: {e}", f"Map panel render error: {e}"))

            with tab_outputs:
                try:
                    _render_output_preview()
                except Exception as e:
                    st.error(_tr(f"输出面板渲染异常: {e}", f"Outputs panel render error: {e}"))

        if user_question:
            if not st.session_state.chat_history or st.session_state.chat_history[-1] != ("user", user_question):
                st.session_state.chat_history.append(("user", user_question))
            if not st.session_state.get("conversation"):
                app_logic.ensure_conversation_initialized()
            if not st.session_state.get("conversation"):
                st.session_state.chat_history.append(
                    (
                        "assistant",
                        _tr(
                            "会话初始化失败，请先在侧边栏重新激活后再提问。",
                            "Conversation initialization failed. Please reactivate in sidebar and retry.",
                        ),
                    )
                )
                st.rerun()
                return
            run_result = app_logic.start_user_run(user_question)
            if not run_result.get("started"):
                reason = str(run_result.get("reason") or "")
                if reason == "thread_run_in_progress":
                    st.info(_tr("当前线程已有运行中的任务。", "A task is already running for this thread."))
                elif reason == "global_run_limit_reached":
                    st.warning(
                        _tr(
                            f"系统并发任务已达上限：{run_result.get('active_runs')} / {run_result.get('limit')}。",
                            f"Global active run limit reached: {run_result.get('active_runs')} / {run_result.get('limit')}.",
                        )
                    )
                elif reason == "user_run_limit_reached":
                    st.warning(
                        _tr(
                            f"当前用户并发任务已达上限：{run_result.get('active_runs')} / {run_result.get('limit')}。",
                            f"User active run limit reached: {run_result.get('active_runs')} / {run_result.get('limit')}.",
                        )
                    )
                elif reason in {"thread_workspace_quota_reached", "user_workspace_quota_reached"}:
                    st.warning(
                        _tr(
                            f"工作区配额不足：{run_result.get('projected_label')} / {run_result.get('limit_label')}。",
                            f"Workspace quota exceeded: {run_result.get('projected_label')} / {run_result.get('limit_label')}.",
                        )
                    )
                elif reason == "conversation_uninitialized":
                    st.warning(
                        _tr(
                            "会话初始化失败，请在侧边栏点击 Activate 后重试。",
                            "Conversation initialization failed. Please activate in sidebar and retry.",
                        )
                    )
                st.rerun()
                return

        graph_force_refresh_once = bool(st.session_state.get("reasoning_graph_force_refresh_once", False))
        if hasattr(st, "fragment") and (st.session_state.get("is_running", False) or graph_force_refresh_once):
            @st.fragment(run_every=1.4)
            def _streaming_live_fragment_main():
                was_running = bool(st.session_state.get("_streaming_was_running", False))
                app_logic.consume_active_run_events()
                is_running_now = bool(st.session_state.get("is_running", False))
                if was_running and not is_running_now:
                    st.session_state["reasoning_graph_force_refresh_once"] = True
                st.session_state["_streaming_was_running"] = is_running_now

                if chat_live_placeholder is not None:
                    with chat_live_placeholder.container():
                        _render_chat_history_with_run_notice()
                if lifecycle_placeholder is not None:
                    with lifecycle_placeholder.container():
                        _render_subagent_lifecycle_cards(
                            st.session_state.get("analysis_logs", []),
                            is_running=is_running_now,
                            last_terminal_kind=str(st.session_state.get("run_last_terminal_kind", "") or ""),
                        )
                if reasoning_live_placeholder is not None and st.session_state.get("analysis_logs"):
                    with reasoning_live_placeholder.container():
                        with st.expander(_tr("本轮推理过程", "Reasoning Flow"), expanded=True):
                            render_reasoning_content(st.session_state.analysis_logs)
                if bool(st.session_state.get("ui_force_refresh_once", False)):
                    _rerun_app()

            @st.fragment(run_every=3.6)
            def _streaming_live_fragment_graph():
                if reasoning_graph_live_placeholder is None:
                    return
                if not bool(st.session_state.get("reasoning_graph_refresh_enabled", True)):
                    return
                logs = st.session_state.get("analysis_logs") or []
                if not logs:
                    return

                current_sig = _compute_reasoning_graph_signature(logs)
                last_sig = str(st.session_state.get("reasoning_graph_last_sig", "") or "")
                force_refresh = bool(st.session_state.get("reasoning_graph_force_refresh_once", False))
                if (not force_refresh) and (current_sig == last_sig):
                    return

                with reasoning_graph_live_placeholder.container():
                    with st.expander(_tr("本轮推理图谱", "Reasoning Graph"), expanded=True):
                        render_reasoning_map(
                            logs,
                            interactive=False,
                            show_sub_steps=False,
                        )
                st.session_state["reasoning_graph_last_sig"] = current_sig
                st.session_state["reasoning_graph_last_render_at"] = float(time.time())
                if force_refresh:
                    st.session_state["reasoning_graph_force_refresh_once"] = False

            _streaming_live_fragment_main()
            _streaming_live_fragment_graph()

