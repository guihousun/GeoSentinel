import os
import re
import json
import hashlib
import ast
import html
import io
import shutil
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

# --- 1. 閻滈柊宥囩枂 ---
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
import gee_auth
import history_store
import model_config
from storage_manager import storage_manager
from map_view_policy import build_layer_signature, advance_map_view_state

try:
    from st_chat_input_multimodal import multimodal_chat_input
except Exception:  # noqa: BLE001
    multimodal_chat_input = None


def _is_en() -> bool:
    return False


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
    return zh


APP_ROOT = Path(__file__).resolve().parent
IMAGE_PREVIEW_SUFFIXES = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"}
IMAGE_DATA_LINK_MAX_BYTES = 16 * 1024 * 1024


def _project_path(*parts: str) -> Path:
    return APP_ROOT.joinpath(*parts)


def _image_mime_type(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".jpg", ".jpeg"}:
        return "image/jpeg"
    if suffix == ".png":
        return "image/png"
    if suffix == ".gif":
        return "image/gif"
    if suffix == ".webp":
        return "image/webp"
    if suffix == ".bmp":
        return "image/bmp"
    return "application/octet-stream"


def _resolve_workspace_artifact_ref(ref: str, thread_id: str | None = None) -> Path:
    raw = str(ref or "").strip()
    if not raw:
        raise ValueError("Empty artifact path.")
    path = Path(raw)
    if path.is_absolute():
        return path
    workspace = storage_manager.get_workspace(thread_id or st.session_state.get("thread_id", "debug"))
    return storage_manager.resolve_workspace_relative_path(
        raw,
        thread_id=thread_id or st.session_state.get("thread_id", "debug"),
        default_root="outputs",
        create_parent=False,
    )


def _artifact_display_ref(path: Path, thread_id: str | None = None) -> str:
    workspace = storage_manager.get_workspace(thread_id or st.session_state.get("thread_id", "debug"))
    try:
        return path.resolve().relative_to(workspace.resolve()).as_posix()
    except Exception:
        return str(path)


def _image_data_uri(path: Path) -> str | None:
    try:
        if path.stat().st_size > IMAGE_DATA_LINK_MAX_BYTES:
            return None
        encoded = base64.b64encode(path.read_bytes()).decode("ascii")
        return f"data:{_image_mime_type(path)};base64,{encoded}"
    except Exception:
        return None


def _should_render_image_as_html_gif(image_ref: str | Path) -> bool:
    return Path(str(image_ref or "")).suffix.lower() == ".gif"


def _code_language_label(language: str) -> str:
    lang = str(language or "text").strip().lower()
    return {
        "python": "Python script",
        "py": "Python script",
        "json": "JSON",
        "geojson": "GeoJSON",
        "csv": "CSV",
        "powershell": "PowerShell",
        "bash": "Shell",
        "markdown": "Markdown",
        "yaml": "YAML",
        "text": "Text",
    }.get(lang, lang.upper() if lang else "Text")


def _static_code_block_html(code_text: str, *, language: str = "text", title: str = "") -> str:
    safe_code = html.escape(str(code_text or ""))
    safe_lang = html.escape(_code_language_label(language))
    safe_title = html.escape(str(title or ""))
    lang_class = re.sub(r"[^a-z0-9_-]+", "-", str(language or "text").strip().lower()).strip("-") or "text"
    title_html = f'<span class="ntl-code-title">{safe_title}</span>' if safe_title else ""
    return f"""
    <div class="ntl-code-block ntl-code-kind-{html.escape(lang_class)}">
      <div class="ntl-code-head">
        <span class="ntl-code-lang">{safe_lang}</span>
        {title_html}
      </div>
      <pre><code>{safe_code}</code></pre>
    </div>
    """


def _render_static_code_block(code_text: str, *, language: str = "text", title: str = "") -> None:
    lang = str(language or "text").strip().lower()
    if lang in {"python", "py"}:
        st.code(
            str(code_text or ""),
            language="python",
            line_numbers=False,
            wrap_lines=True,
            height="content",
            width="stretch",
        )
        return
    st.markdown(
        _static_code_block_html(code_text, language=language, title=title),
        unsafe_allow_html=True,
    )


def _json_block_text(payload) -> str:
    try:
        return json.dumps(_sanitize_paths_in_obj(payload), ensure_ascii=False, indent=2)
    except Exception:
        return _sanitize_paths_in_text(str(payload))


def _render_static_json_block(payload, *, title: str = "JSON") -> None:
    _render_static_code_block(_json_block_text(payload), language="json", title=title)


def _render_image_artifact(
    image_ref: str | Path,
    *,
    key_prefix: str,
    caption: str | None = None,
    thread_id: str | None = None,
) -> None:
    try:
        path = _resolve_workspace_artifact_ref(str(image_ref), thread_id=thread_id)
    except Exception as exc:
        st.warning(_tr(f"图像路径无效: {exc}", f"Invalid image path: {exc}"))
        return

    if not path.exists() or not path.is_file():
        st.warning(_tr(f"图像文件不存在: {image_ref}", f"Image file not found: {image_ref}"))
        return

    file_name = path.name
    display_ref = _artifact_display_ref(path, thread_id=thread_id)
    safe_name = html.escape(file_name)
    safe_ref = html.escape(display_ref)
    data_uri = _image_data_uri(path)

    st.markdown(
        f"""
        <div class="ntl-image-card">
          <div class="ntl-image-card-head">
            <div class="ntl-image-title" title="{safe_ref}">{safe_name}</div>
            <div class="ntl-image-actions">
              {f'<a href="{data_uri}" target="_blank" rel="noopener">Open</a>' if data_uri else ''}
              {f'<a href="{data_uri}" download="{safe_name}">Download</a>' if data_uri else ''}
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if data_uri:
        safe_caption = html.escape(str(caption or ""))
        preview_class = "ntl-animated-gif" if _should_render_image_as_html_gif(path) else "ntl-image-preview"
        st.markdown(
            f"""
            <figure class="{preview_class}">
              <img src="{data_uri}" alt="{safe_caption or safe_name}" loading="eager">
              {f'<figcaption>{safe_caption}</figcaption>' if safe_caption else ''}
            </figure>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.image(str(path), caption=caption, width="stretch")
    if data_uri is None:
        with path.open("rb") as fh:
            st.download_button(
                _tr("下载原图", "Download original"),
                data=fh,
                file_name=file_name,
                mime=_image_mime_type(path),
                key=f"{key_prefix}_download_{hashlib.md5(str(path).encode('utf-8')).hexdigest()}",
            )


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


def _env_float(name: str, default: float, *, min_value: float, max_value: float) -> float:
    try:
        value = float(str(os.getenv(name, "") or "").strip() or default)
    except (TypeError, ValueError):
        return default
    return max(min_value, min(max_value, value))


_STREAMING_MAIN_REFRESH_SECONDS = _env_float(
    "NTL_STREAMING_MAIN_REFRESH_SECONDS",
    3.0,
    min_value=1.0,
    max_value=30.0,
)
_STREAMING_GRAPH_REFRESH_SECONDS = _env_float(
    "NTL_STREAMING_GRAPH_REFRESH_SECONDS",
    15.0,
    min_value=2.0,
    max_value=60.0,
)
_TEST_CASE_FILES = [_project_path("example", "test_cases_mini.xlsx")]
_EXCLUDED_TEST_CASE_IDS = {"12", "13", "15", "40", "41"}
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
    "14": "NOAA-20 预处理",
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
    "14": "对保存在 inputs/ 目录中的 NOAA-20 VIIRS 数据进行预处理。",
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
            req = Request(MONITOR_API_URL, headers={"User-Agent": "地缘环境智能计算平台-UI/1.0"})
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
        display: none !important;
    }
    div[data-testid="stDecoration"] { display: none !important; }
    [data-testid="stToolbar"] { display: none !important; }
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
    }
    [data-testid="stChatInput"] > div {
        width: 100% !important;
        max-width: 100% !important;
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
    }
    div:has(> [data-testid="stBottomBlockContainer"]) {
        background: transparent !important;
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
    .ntl-image-card {
        margin: 0.55rem 0 0.25rem 0;
        padding: 0.45rem 0.55rem;
        border: 1px solid rgba(180, 201, 255, 0.26);
        border-radius: 8px;
        background: rgba(9, 20, 42, 0.42);
    }
    .ntl-image-card-head {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 0.75rem;
        min-width: 0;
    }
    .ntl-image-title {
        color: #e7eefc;
        font-size: 0.86rem;
        line-height: 1.25;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        min-width: 0;
    }
    .ntl-image-actions {
        display: inline-flex;
        align-items: center;
        gap: 0.6rem;
        flex: 0 0 auto;
    }
    .ntl-image-actions a {
        color: #b9cdfb !important;
        font-size: 0.78rem;
        line-height: 1;
        text-decoration: none !important;
    }
    .ntl-image-actions a:hover {
        color: #ffffff !important;
        text-decoration: underline !important;
        text-underline-offset: 3px;
    }
    .ntl-animated-gif {
        margin: 0.35rem 0 0.55rem 0;
        overflow: hidden;
        border-radius: 8px;
        background: rgba(5, 10, 22, 0.78);
        border: 1px solid rgba(180, 201, 255, 0.24);
    }
    .ntl-animated-gif img {
        display: block;
        width: 100%;
        height: auto;
    }
    .ntl-image-preview {
        margin: 0.35rem 0 0.55rem 0;
        overflow: hidden;
        border-radius: 8px;
        background: rgba(5, 10, 22, 0.78);
        border: 1px solid rgba(180, 201, 255, 0.24);
    }
    .ntl-image-preview img {
        display: block;
        width: 100%;
        height: auto;
    }
    .ntl-image-preview figcaption {
        padding: 0.34rem 0.55rem 0.42rem 0.55rem;
        color: #cfdaf0;
        font-size: 0.78rem;
        text-align: center;
    }
    .ntl-animated-gif figcaption {
        padding: 0.34rem 0.55rem 0.42rem 0.55rem;
        color: #cfdaf0;
        font-size: 0.78rem;
        text-align: center;
    }
    .stExpander { border: 1px solid rgba(160, 184, 235, 0.35); border-radius: 10px; }
    [data-testid="stAppViewContainer"] [data-testid="stExpander"] details {
        background: rgba(8, 14, 29, 0.84) !important;
        border: 1px solid rgba(160, 184, 235, 0.35) !important;
        border-radius: 10px !important;
        color: #eaf1ff !important;
    }
    [data-testid="stAppViewContainer"] [data-testid="stExpander"] summary {
        background: rgba(14, 24, 48, 0.96) !important;
        border-radius: 9px !important;
        color: #eaf1ff !important;
    }
    [data-testid="stAppViewContainer"] [data-testid="stExpander"] details[open] > summary {
        border-bottom: 1px solid rgba(160, 184, 235, 0.24) !important;
        border-bottom-left-radius: 0 !important;
        border-bottom-right-radius: 0 !important;
    }
    [data-testid="stAppViewContainer"] [data-testid="stExpander"] summary *,
    [data-testid="stAppViewContainer"] [data-testid="stExpander"] details * {
        color: #eaf1ff !important;
        -webkit-text-fill-color: #eaf1ff !important;
    }
    [data-testid="stAppViewContainer"] [data-testid="stExpander"] svg {
        fill: #dbe8ff !important;
    }
    .stCode { border-radius: 10px; }
    .stCode pre,
    .stCode pre div,
    .stCode pre code,
    .stCode pre span {
        background: #081222 !important;
        color: #eaf1ff !important;
        -webkit-text-fill-color: #eaf1ff !important;
        border-radius: 10px !important;
        text-shadow: none !important;
    }
    .stMarkdown :not(pre) > code,
    [data-testid="stMarkdownContainer"] :not(pre) > code {
        display: inline;
        max-width: 100%;
        background: rgba(151, 196, 255, 0.18) !important;
        color: #dbeafe !important;
        -webkit-text-fill-color: #dbeafe !important;
        border: 1px solid rgba(151, 196, 255, 0.24) !important;
        border-radius: 5px !important;
        padding: 0.08rem 0.28rem !important;
        font-size: 0.88em !important;
        font-weight: 650 !important;
        white-space: normal !important;
        overflow-wrap: anywhere !important;
        word-break: break-word !important;
        box-decoration-break: clone;
        -webkit-box-decoration-break: clone;
    }
    .stMarkdown h1,
    .stMarkdown h2,
    .stMarkdown h3,
    .stMarkdown h4,
    .stMarkdown h5,
    .stMarkdown h6,
    [data-testid="stMarkdownContainer"] h1,
    [data-testid="stMarkdownContainer"] h2,
    [data-testid="stMarkdownContainer"] h3,
    [data-testid="stMarkdownContainer"] h4,
    [data-testid="stMarkdownContainer"] h5,
    [data-testid="stMarkdownContainer"] h6 {
        color: #f8fbff !important;
        -webkit-text-fill-color: #f8fbff !important;
        letter-spacing: 0 !important;
    }
    .stJson,
    [data-testid="stJson"],
    [data-testid="stJson"] *,
    .stJson *,
    .object-key-val,
    .object-key-val *,
    .variable-row,
    .variable-row * {
        background: #071021 !important;
        color: #dbeafe !important;
        -webkit-text-fill-color: #dbeafe !important;
        text-shadow: none !important;
    }
    [data-testid="stJson"],
    .stJson {
        border: 1px solid rgba(151, 196, 255, 0.24) !important;
        border-radius: 10px !important;
        overflow: auto !important;
    }
    .ntl-code-block {
        margin: 0.45rem 0 0.7rem 0;
        border-radius: 10px;
        border: 1px solid rgba(151, 196, 255, 0.28);
        background: linear-gradient(180deg, rgba(8, 18, 34, 0.98), rgba(5, 12, 26, 0.98));
        overflow: hidden;
        max-width: 100%;
        min-width: 0;
        box-sizing: border-box;
    }
    .ntl-code-head {
        display: flex;
        align-items: center;
        justify-content: space-between;
        flex-wrap: wrap;
        gap: 0.75rem;
        padding: 0.52rem 0.68rem;
        background: linear-gradient(90deg, rgba(27, 53, 96, 0.94), rgba(15, 31, 63, 0.94));
        border-bottom: 1px solid rgba(151, 196, 255, 0.20);
        min-width: 0;
    }
    .ntl-code-lang {
        color: #bfdbfe !important;
        -webkit-text-fill-color: #bfdbfe !important;
        font-weight: 780;
        font-size: 0.76rem;
        letter-spacing: 0;
    }
    .ntl-code-title {
        min-width: 0;
        color: #eaf1ff !important;
        -webkit-text-fill-color: #eaf1ff !important;
        font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
        font-size: 0.76rem;
        overflow-wrap: anywhere;
        word-break: break-word;
        white-space: normal;
        text-align: right;
    }
    .ntl-code-block pre {
        margin: 0;
        padding: 0.78rem 0.86rem;
        max-height: 620px;
        overflow-x: hidden;
        overflow-y: auto;
        background: #071021;
        color: #eaf1ff !important;
        -webkit-text-fill-color: #eaf1ff !important;
        font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
        font-size: 0.82rem;
        line-height: 1.58;
        white-space: pre-wrap;
        overflow-wrap: anywhere;
        word-break: break-word;
        tab-size: 4;
    }
    .ntl-code-block code {
        background: transparent !important;
        color: inherit !important;
        -webkit-text-fill-color: inherit !important;
        border: 0 !important;
        padding: 0 !important;
        font: inherit !important;
        white-space: inherit !important;
        overflow-wrap: inherit !important;
        word-break: inherit !important;
    }
    .ntl-code-kind-text pre,
    .ntl-code-kind-csv pre,
    .ntl-code-kind-json pre,
    .ntl-code-kind-geojson pre {
        max-height: 320px;
    }
    .ntl-code-kind-text {
        border-color: rgba(151, 196, 255, 0.20);
        background: rgba(5, 14, 31, 0.72);
    }
    .ntl-code-kind-text .ntl-code-head {
        padding: 0.42rem 0.58rem;
        background: rgba(31, 62, 111, 0.72);
    }
    .ntl-code-kind-text pre {
        padding: 0.62rem 0.68rem;
        font-size: 0.79rem;
        line-height: 1.46;
    }
    .ntl-tool-card {
        margin: 0 0 0.5rem 0;
        padding: 0.72rem;
        border: 1px solid rgba(160, 184, 235, 0.28);
        border-radius: 9px;
        background: linear-gradient(180deg, rgba(13, 28, 57, 0.88), rgba(7, 15, 32, 0.88));
        color: #eaf1ff;
        font-size: 0.88rem;
        line-height: 1.45;
    }
    .ntl-tool-card-head {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 0.75rem;
        margin-bottom: 0.46rem;
    }
    .ntl-tool-title {
        color: #ffffff !important;
        -webkit-text-fill-color: #ffffff !important;
        font-weight: 750;
        letter-spacing: 0;
    }
    .ntl-tool-count {
        color: #98b8f8 !important;
        -webkit-text-fill-color: #98b8f8 !important;
        font-size: 0.74rem;
        white-space: nowrap;
    }
    .ntl-file-list {
        display: grid;
        gap: 0.36rem;
    }
    .ntl-file-row {
        display: grid;
        grid-template-columns: minmax(4.8rem, 7.6rem) minmax(0, 1fr);
        align-items: center;
        gap: 0.62rem;
        padding: 0.38rem 0.5rem;
        border-radius: 8px;
        background: rgba(68, 103, 173, 0.22);
        border: 1px solid rgba(160, 184, 235, 0.18);
        min-width: 0;
    }
    .ntl-file-kind {
        color: #b9d7ff !important;
        -webkit-text-fill-color: #b9d7ff !important;
        font-weight: 720;
        font-size: 0.77rem;
        line-height: 1.15;
    }
    .ntl-file-path {
        color: #dbe8ff !important;
        -webkit-text-fill-color: #dbe8ff !important;
        font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
        font-size: 0.76rem;
        line-height: 1.28;
        word-break: break-word;
        min-width: 0;
    }
    .ntl-change-grid {
        display: grid;
        grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
        gap: 0.5rem;
        margin-top: 0.5rem;
    }
    .ntl-change-pane {
        min-width: 0;
        padding: 0.48rem;
        border-radius: 8px;
        border: 1px solid rgba(160, 184, 235, 0.18);
        background: rgba(4, 10, 22, 0.48);
    }
    .ntl-change-label {
        display: block;
        margin-bottom: 0.28rem;
        color: #9fb9ef !important;
        -webkit-text-fill-color: #9fb9ef !important;
        font-size: 0.72rem;
        font-weight: 720;
        text-transform: uppercase;
    }
    .ntl-change-text {
        margin: 0;
        color: #eaf1ff !important;
        -webkit-text-fill-color: #eaf1ff !important;
        font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
        font-size: 0.74rem;
        line-height: 1.35;
        white-space: pre-wrap;
        word-break: break-word;
    }
    .ntl-tool-message {
        margin-top: 0.5rem;
        color: #cfdaf0 !important;
        -webkit-text-fill-color: #cfdaf0 !important;
        font-size: 0.82rem;
    }
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
    div[class*="st-key-ui_lang_switch"] [role="radiogroup"] {
        display: flex !important;
        flex-direction: row !important;
        flex-wrap: nowrap !important;
        align-items: center !important;
        justify-content: flex-end !important;
        gap: 0.35rem !important;
        min-width: 5.4rem !important;
    }
    div[class*="st-key-ui_lang_switch"] [role="radiogroup"] label {
        display: inline-flex !important;
        align-items: center !important;
        white-space: nowrap !important;
        margin: 0 !important;
        width: auto !important;
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
    /* Premium sidebar style overrides (地缘环境智能计算平台 night console) */
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
    [data-testid="stSidebar"] div[class*="st-key-thread_open_btn_v3_"] button {
        min-height: 2.35rem !important;
        padding: 0.18rem 0.7rem !important;
        border-radius: 14px !important;
        justify-content: flex-start !important;
        text-align: left !important;
        overflow: hidden !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-thread_open_btn_v3_"] button p,
    [data-testid="stSidebar"] div[class*="st-key-thread_open_btn_v3_"] button span {
        width: 100% !important;
        overflow: hidden !important;
        text-overflow: ellipsis !important;
        white-space: nowrap !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-thread_rename_menu_v3_"] button,
    [data-testid="stSidebar"] div[class*="st-key-thread_delete_menu_v3_"] button {
        min-height: 2rem !important;
        padding: 0.1rem 0.7rem !important;
        border-radius: 10px !important;
        font-size: 0.9rem !important;
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
        gap: 0.8rem;
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
    .ntl-thread-status-value {
        color: #f1f6ff;
        font-weight: 600;
    }
    .ntl-thread-quota-row {
        display: flex;
        gap: 0.45rem;
        flex-wrap: wrap;
        margin-top: 0.38rem;
    }
    .ntl-thread-quota-pill {
        display: inline-flex;
        align-items: center;
        gap: 0.28rem;
        padding: 0.16rem 0.46rem;
        border-radius: 999px;
        background: rgba(15, 29, 58, 0.88);
        border: 1px solid rgba(120, 162, 255, 0.28);
        color: #dce8ff;
        font-size: 0.76rem;
        line-height: 1.1;
    }
    .ntl-thread-quota-pill strong {
        color: #f4f8ff;
        font-weight: 700;
    }
    [data-testid="stSidebar"] div[class*="st-key-thread_item_btn_v4_"] button,
    [data-testid="stSidebar"] div[class*="st-key-thread_current_btn_v4_"] button {
        min-height: 2.02rem !important;
        padding: 0.05rem 0.62rem !important;
        border-radius: 10px !important;
        justify-content: flex-start !important;
        text-align: left !important;
        box-shadow: none !important;
        transition: background 120ms ease, border-color 120ms ease !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-thread_item_btn_v4_"] button {
        background: transparent !important;
        border: 1px solid transparent !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-thread_item_btn_v4_"] button:hover {
        background: rgba(255,255,255,0.06) !important;
        border-color: rgba(255,255,255,0.08) !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-thread_current_btn_v4_"] button {
        background: rgba(111, 168, 255, 0.14) !important;
        border: 1px solid rgba(129, 178, 255, 0.24) !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-thread_current_btn_v4_"] button:hover {
        background: rgba(111, 168, 255, 0.18) !important;
        border-color: rgba(150, 194, 255, 0.30) !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-thread_item_btn_v4_"] button p,
    [data-testid="stSidebar"] div[class*="st-key-thread_item_btn_v4_"] button span,
    [data-testid="stSidebar"] div[class*="st-key-thread_current_btn_v4_"] button p,
    [data-testid="stSidebar"] div[class*="st-key-thread_current_btn_v4_"] button span {
        width: 100% !important;
        overflow: hidden !important;
        text-overflow: ellipsis !important;
        white-space: nowrap !important;
        color: #e7efff !important;
        -webkit-text-fill-color: #e7efff !important;
        font-size: 0.95rem !important;
        line-height: 1.1 !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-thread_rename_inline_v6_"] button,
    [data-testid="stSidebar"] div[class*="st-key-thread_delete_inline_v6_"] button {
        min-height: 0.72rem !important;
        padding: 0 !important;
        border-radius: 0 !important;
        font-size: 0.66rem !important;
        background: transparent !important;
        border: none !important;
        outline: none !important;
        box-shadow: none !important;
        justify-content: flex-end !important;
        text-align: right !important;
        min-width: auto !important;
        line-height: 0.9 !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-thread_rename_inline_v6_"] [data-testid="stBaseButton-secondary"],
    [data-testid="stSidebar"] div[class*="st-key-thread_delete_inline_v6_"] [data-testid="stBaseButton-secondary"] {
        background: transparent !important;
        border: none !important;
        outline: none !important;
        box-shadow: none !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-thread_rename_inline_v6_"] button p,
    [data-testid="stSidebar"] div[class*="st-key-thread_rename_inline_v6_"] button span {
        color: rgba(219, 232, 255, 0.88) !important;
        -webkit-text-fill-color: rgba(219, 232, 255, 0.88) !important;
        font-weight: 500 !important;
        font-size: 0.66rem !important;
        line-height: 0.9 !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-thread_delete_inline_v6_"] button p,
    [data-testid="stSidebar"] div[class*="st-key-thread_delete_inline_v6_"] button span {
        color: rgba(255, 174, 174, 0.88) !important;
        -webkit-text-fill-color: rgba(255, 174, 174, 0.88) !important;
        font-weight: 500 !important;
        font-size: 0.66rem !important;
        line-height: 0.9 !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-thread_rename_inline_v6_"] button:hover,
    [data-testid="stSidebar"] div[class*="st-key-thread_delete_inline_v6_"] button:hover {
        background: transparent !important;
        border: 0 !important;
        text-decoration: underline !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-rename_thread_input_v7_"] input {
        min-height: 2rem !important;
        border-radius: 10px !important;
        padding: 0.18rem 0.7rem !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-rename_thread_save_v7_"] button,
    [data-testid="stSidebar"] div[class*="st-key-rename_thread_cancel_v7_"] button {
        min-height: 1.45rem !important;
        min-width: 3.7rem !important;
        padding: 0.08rem 0.58rem !important;
        border-radius: 999px !important;
        font-size: 0.72rem !important;
        box-shadow: none !important;
        justify-content: flex-end !important;
        text-align: right !important;
        white-space: nowrap !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-rename_thread_save_v7_"] button {
        background: rgba(111, 168, 255, 0.16) !important;
        border: 1px solid rgba(129, 178, 255, 0.28) !important;
        outline: none !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-rename_thread_save_v7_"] button p,
    [data-testid="stSidebar"] div[class*="st-key-rename_thread_save_v7_"] button span {
        color: rgba(219, 232, 255, 0.9) !important;
        -webkit-text-fill-color: rgba(219, 232, 255, 0.9) !important;
        font-weight: 500 !important;
        font-size: 0.68rem !important;
        line-height: 1 !important;
        letter-spacing: 0.01em !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-rename_thread_cancel_v7_"] button {
        background: transparent !important;
        border: none !important;
        outline: none !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-rename_thread_cancel_v7_"] button p,
    [data-testid="stSidebar"] div[class*="st-key-rename_thread_cancel_v7_"] button span {
        color: rgba(194, 207, 232, 0.8) !important;
        -webkit-text-fill-color: rgba(194, 207, 232, 0.8) !important;
        font-weight: 500 !important;
        font-size: 0.68rem !important;
        line-height: 1 !important;
        letter-spacing: 0.01em !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-rename_thread_save_v7_"] button:hover {
        background: rgba(111, 168, 255, 0.22) !important;
        border-color: rgba(150, 194, 255, 0.34) !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-rename_thread_cancel_v7_"] button:hover {
        background: transparent !important;
        border: none !important;
        text-decoration: underline !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-delete_thread_confirm_v8_"] button,
    [data-testid="stSidebar"] div[class*="st-key-delete_thread_cancel_v8_"] button {
        min-height: 1.45rem !important;
        min-width: 3.7rem !important;
        padding: 0.08rem 0.58rem !important;
        border-radius: 999px !important;
        font-size: 0.72rem !important;
        box-shadow: none !important;
        justify-content: flex-end !important;
        text-align: right !important;
        white-space: nowrap !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-delete_thread_confirm_v8_"] button {
        background: rgba(140, 34, 42, 0.22) !important;
        border: 1px solid rgba(255, 120, 120, 0.24) !important;
        outline: none !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-delete_thread_confirm_v8_"] button p,
    [data-testid="stSidebar"] div[class*="st-key-delete_thread_confirm_v8_"] button span {
        color: rgba(255, 174, 174, 0.92) !important;
        -webkit-text-fill-color: rgba(255, 174, 174, 0.92) !important;
        font-weight: 500 !important;
        font-size: 0.68rem !important;
        line-height: 1 !important;
        letter-spacing: 0.01em !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-delete_thread_cancel_v8_"] button {
        background: transparent !important;
        border: none !important;
        outline: none !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-delete_thread_cancel_v8_"] button p,
    [data-testid="stSidebar"] div[class*="st-key-delete_thread_cancel_v8_"] button span {
        color: rgba(194, 207, 232, 0.8) !important;
        -webkit-text-fill-color: rgba(194, 207, 232, 0.8) !important;
        font-weight: 500 !important;
        font-size: 0.68rem !important;
        line-height: 1 !important;
        letter-spacing: 0.01em !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-delete_thread_confirm_v8_"] button:hover {
        background: rgba(140, 34, 42, 0.28) !important;
        border-color: rgba(255, 138, 138, 0.30) !important;
    }
    [data-testid="stSidebar"] div[class*="st-key-delete_thread_cancel_v8_"] button:hover {
        background: transparent !important;
        border: none !important;
        text-decoration: underline !important;
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
    /* Reasoning panel hierarchy: distinguish user, agent, and tool text without leaving the night-console theme. */
    [data-testid="stAppViewContainer"] [data-testid="stExpander"] details {
        background: rgba(6, 13, 29, 0.88) !important;
        border: 1px solid rgba(132, 168, 255, 0.28) !important;
        border-radius: 12px !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.06), 0 10px 22px rgba(0,0,0,0.18) !important;
    }
    [data-testid="stAppViewContainer"] [data-testid="stExpander"] summary,
    [data-testid="stAppViewContainer"] [data-testid="stExpander"] details > summary {
        background: linear-gradient(180deg, rgba(13, 27, 58, 0.96), rgba(7, 16, 36, 0.96)) !important;
        border-radius: 11px !important;
        border-bottom: 1px solid rgba(132, 168, 255, 0.22) !important;
        color: #eaf1ff !important;
        -webkit-text-fill-color: #eaf1ff !important;
    }
    [data-testid="stAppViewContainer"] [data-testid="stExpander"] details[open] > summary {
        border-bottom-left-radius: 0 !important;
        border-bottom-right-radius: 0 !important;
    }
    [data-testid="stAppViewContainer"] [data-testid="stExpander"] summary p,
    [data-testid="stAppViewContainer"] [data-testid="stExpander"] summary span,
    [data-testid="stAppViewContainer"] [data-testid="stExpander"] summary svg {
        color: #eaf1ff !important;
        fill: #dbeafe !important;
        -webkit-text-fill-color: #eaf1ff !important;
        font-weight: 720 !important;
    }
    [data-testid="stAppViewContainer"] [data-testid="stExpander"] details p,
    [data-testid="stAppViewContainer"] [data-testid="stExpander"] details li,
    [data-testid="stAppViewContainer"] [data-testid="stExpander"] details span,
    [data-testid="stAppViewContainer"] [data-testid="stExpander"] details div {
        color: #dbeafe !important;
        -webkit-text-fill-color: #dbeafe !important;
    }
    .ntl-reasoning-header {
        display: grid;
        grid-template-columns: auto minmax(0, 1fr);
        grid-template-areas:
            "kind title"
            "kind subtitle";
        align-items: center;
        column-gap: 0.58rem;
        row-gap: 0.08rem;
        margin: 0.42rem 0 0.36rem 0;
        padding: 0.42rem 0.56rem 0.44rem 0.56rem;
        border-left: 3px solid var(--ntl-reasoning-accent);
        border-radius: 9px;
        background: linear-gradient(90deg, rgba(14, 29, 61, 0.72), rgba(7, 15, 33, 0.28));
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.05);
    }
    .ntl-reasoning-header-plain {
        display: flex;
        align-items: center;
        gap: 0.62rem;
        min-height: 3.1rem;
        padding-top: 0.46rem;
        padding-bottom: 0.46rem;
    }
    .ntl-reasoning-kind {
        grid-area: kind;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        min-width: 2.48rem;
        min-height: 1.22rem;
        padding: 0.14rem 0.34rem;
        border-radius: 999px;
        border: 1px solid color-mix(in srgb, var(--ntl-reasoning-accent) 62%, transparent);
        background: color-mix(in srgb, var(--ntl-reasoning-accent) 16%, transparent);
        color: var(--ntl-reasoning-accent) !important;
        -webkit-text-fill-color: var(--ntl-reasoning-accent) !important;
        font-size: 0.66rem;
        line-height: 1;
        font-weight: 820;
        text-align: center;
        letter-spacing: 0;
    }
    .ntl-reasoning-title {
        grid-area: title;
        min-width: 0;
        align-self: center;
        color: #f8fbff !important;
        -webkit-text-fill-color: #f8fbff !important;
        font-weight: 760;
        font-size: 0.94rem;
        line-height: 1.16;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        letter-spacing: 0;
    }
    .ntl-reasoning-subtitle {
        grid-area: subtitle;
        color: #9fb8df !important;
        -webkit-text-fill-color: #9fb8df !important;
        font-size: 0.74rem;
        line-height: 1.18;
        font-weight: 600;
        letter-spacing: 0;
    }
    .ntl-reasoning-text {
        margin: 0.18rem 0 0.58rem 0.72rem;
        padding: 0.52rem 0.66rem;
        border-left: 1px solid color-mix(in srgb, var(--ntl-reasoning-accent) 48%, rgba(132,168,255,0.15));
        border-radius: 0 8px 8px 0;
        background:
            linear-gradient(90deg, color-mix(in srgb, var(--ntl-reasoning-accent) 10%, transparent), rgba(4, 10, 22, 0.34) 42%),
            rgba(4, 10, 22, 0.34);
        color: #dbeafe !important;
        -webkit-text-fill-color: #dbeafe !important;
        white-space: pre-wrap;
        word-break: break-word;
        overflow-wrap: anywhere;
        font-size: 0.91rem;
        line-height: 1.55;
    }
    .ntl-reasoning-rich {
        white-space: normal;
    }
    .ntl-reasoning-rich p {
        margin: 0.1rem 0 0.52rem 0;
        color: #dbeafe !important;
        -webkit-text-fill-color: #dbeafe !important;
    }
    .ntl-reasoning-rich p:last-child,
    .ntl-reasoning-rich ul:last-child,
    .ntl-reasoning-rich table:last-child {
        margin-bottom: 0;
    }
    .ntl-reasoning-md-heading {
        margin: 0.1rem 0 0.48rem 0;
        color: #f8fbff !important;
        -webkit-text-fill-color: #f8fbff !important;
        font-size: 1.02rem;
        line-height: 1.22;
        font-weight: 820;
        letter-spacing: 0;
    }
    .ntl-reasoning-rich strong {
        color: #ffffff !important;
        -webkit-text-fill-color: #ffffff !important;
        font-weight: 790;
    }
    .ntl-reasoning-rich code {
        display: inline-block;
        max-width: 100%;
        padding: 0.06rem 0.26rem;
        border-radius: 5px;
        border: 1px solid color-mix(in srgb, var(--ntl-reasoning-accent) 32%, rgba(151,196,255,0.18));
        background: rgba(7, 18, 38, 0.88) !important;
        color: #eaf6ff !important;
        -webkit-text-fill-color: #eaf6ff !important;
        font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
        font-size: 0.86em;
        font-weight: 680;
        white-space: normal;
        overflow-wrap: anywhere;
    }
    .ntl-reasoning-rich ul {
        margin: 0.2rem 0 0.58rem 1rem;
        padding: 0;
    }
    .ntl-reasoning-rich li {
        margin: 0.18rem 0;
        color: #dbeafe !important;
        -webkit-text-fill-color: #dbeafe !important;
    }
    .ntl-reasoning-rich table {
        width: 100%;
        margin: 0.36rem 0 0.68rem 0;
        border-collapse: collapse;
        border: 1px solid rgba(147, 197, 253, 0.24);
        border-radius: 8px;
        overflow: hidden;
        background: rgba(5, 14, 31, 0.58);
        table-layout: fixed;
    }
    .ntl-reasoning-rich th,
    .ntl-reasoning-rich td {
        padding: 0.42rem 0.48rem;
        border-bottom: 1px solid rgba(147, 197, 253, 0.14);
        color: #dbeafe !important;
        -webkit-text-fill-color: #dbeafe !important;
        font-size: 0.84rem;
        line-height: 1.35;
        vertical-align: top;
        overflow-wrap: anywhere;
    }
    .ntl-reasoning-rich th {
        background: color-mix(in srgb, var(--ntl-reasoning-accent) 18%, rgba(13, 27, 58, 0.96));
        color: #ffffff !important;
        -webkit-text-fill-color: #ffffff !important;
        font-weight: 760;
    }
    .ntl-reasoning-rich tr:last-child td {
        border-bottom: 0;
    }
    .ntl-reasoning-divider {
        height: 1px;
        margin: 0.64rem 0;
        background: linear-gradient(90deg, rgba(125, 167, 232, 0), rgba(125, 167, 232, 0.36), rgba(125, 167, 232, 0));
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
        }}
        setTimeout(scroll, 300);
        setTimeout(initAlignChatInput, 40);
        setTimeout(alignChatInput, 400);
        setTimeout(alignChatInput, 900);
    </script>
    """
    components.html(js, height=0)

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


def _reasoning_agent_meta(agent_name: str) -> dict:
    normalized = str(agent_name or "AI").strip()
    key = normalized.lower()
    if key == "ntl_engineer" or key == "engineer":
        return {
            "label": "Engineer",
            "role": "",
            "accent": "#5eead4",
            "tone": "teal",
        }
    if key == "code_assistant":
        return {
            "label": "Code_Assistant",
            "role": "",
            "accent": "#7dd3fc",
            "tone": "blue",
        }
    if key == "data_searcher":
        return {
            "label": "Data_Searcher",
            "role": "",
            "accent": "#fbbf24",
            "tone": "amber",
        }
    return {
        "label": normalized or "AI",
        "role": "",
        "accent": "#bfdbfe",
        "tone": "default",
    }


def _reasoning_header_html(kind: str, title: str, subtitle: str = "", *, accent: str = "#bfdbfe") -> str:
    safe_kind = html.escape(str(kind or "AI"))
    safe_title = html.escape(str(title or ""))
    safe_subtitle = html.escape(str(subtitle or ""))
    safe_accent = html.escape(str(accent or "#bfdbfe"))
    subtitle_html = f'<span class="ntl-reasoning-subtitle">{safe_subtitle}</span>' if safe_subtitle else ""
    header_class = "ntl-reasoning-header ntl-reasoning-header-stacked" if safe_subtitle else "ntl-reasoning-header ntl-reasoning-header-plain"
    return f"""
    <div class="{header_class}" style="--ntl-reasoning-accent:{safe_accent};">
      <span class="ntl-reasoning-kind">{safe_kind}</span>
      <span class="ntl-reasoning-title">{safe_title}</span>
      {subtitle_html}
    </div>
    """


def _reasoning_text_html(text: str, *, accent: str = "#bfdbfe") -> str:
    safe_text = html.escape(str(text or ""))
    safe_accent = html.escape(str(accent or "#bfdbfe"))
    return f"""
    <div class="ntl-reasoning-text" style="--ntl-reasoning-accent:{safe_accent};">
      {safe_text}
    </div>
    """


def _reasoning_inline_html(text: str) -> str:
    safe = html.escape(str(text or ""))
    safe = re.sub(r"`([^`\n]+)`", r"<code>\1</code>", safe)
    safe = re.sub(r"\*\*([^*\n]+)\*\*", r"<strong>\1</strong>", safe)
    return safe


_MARKDOWN_IMAGE_LINE_RE = re.compile(r"^\s*!\[[^\]]*\]\([^)]+\)\s*$")


def _is_markdown_image_line(line: str) -> bool:
    return bool(_MARKDOWN_IMAGE_LINE_RE.match(str(line or "")))


def _is_markdown_table_separator(line: str) -> bool:
    cells = [cell.strip() for cell in str(line or "").strip().strip("|").split("|")]
    return bool(cells) and all(re.fullmatch(r":?-{2,}:?", cell or "") for cell in cells)


def _is_markdown_table_row(line: str) -> bool:
    stripped = str(line or "").strip()
    return stripped.startswith("|") and stripped.endswith("|") and stripped.count("|") >= 2


def _reasoning_table_html(lines: list[str]) -> str:
    rows = [
        [cell.strip() for cell in str(line or "").strip().strip("|").split("|")]
        for line in lines
        if _is_markdown_table_row(line) and not _is_markdown_table_separator(line)
    ]
    if not rows:
        return ""
    header = rows[0]
    body = rows[1:]
    head_html = "".join(f"<th>{_reasoning_inline_html(cell)}</th>" for cell in header)
    body_html = "".join(
        "<tr>" + "".join(f"<td>{_reasoning_inline_html(cell)}</td>" for cell in row) + "</tr>"
        for row in body
    )
    return f"<table><thead><tr>{head_html}</tr></thead><tbody>{body_html}</tbody></table>"


def _reasoning_rich_text_html(text: str, *, accent: str = "#bfdbfe") -> str:
    raw = _sanitize_paths_in_text(str(text or ""))
    safe_accent = html.escape(str(accent or "#bfdbfe"))
    parts: list[str] = []
    paragraph: list[str] = []
    bullets: list[str] = []
    table_lines: list[str] = []

    def flush_paragraph() -> None:
        if paragraph:
            parts.append(f"<p>{'<br>'.join(_reasoning_inline_html(line) for line in paragraph)}</p>")
            paragraph.clear()

    def flush_bullets() -> None:
        if bullets:
            parts.append("<ul>" + "".join(f"<li>{_reasoning_inline_html(item)}</li>" for item in bullets) + "</ul>")
            bullets.clear()

    def flush_table() -> None:
        if table_lines:
            table_html = _reasoning_table_html(table_lines)
            if table_html:
                parts.append(table_html)
            else:
                paragraph.extend(table_lines)
            table_lines.clear()

    for line in raw.splitlines():
        stripped = line.strip()
        if _is_markdown_image_line(stripped):
            flush_table()
            flush_bullets()
            flush_paragraph()
            continue
        if not stripped:
            flush_table()
            flush_bullets()
            flush_paragraph()
            continue
        if _is_markdown_table_row(stripped):
            flush_bullets()
            flush_paragraph()
            table_lines.append(stripped)
            continue
        flush_table()
        heading = re.match(r"^#{1,4}\s+(.+)$", stripped)
        if heading:
            flush_bullets()
            flush_paragraph()
            parts.append(f'<div class="ntl-reasoning-md-heading">{_reasoning_inline_html(heading.group(1))}</div>')
            continue
        bullet = re.match(r"^[-*]\s+(.+)$", stripped)
        if bullet:
            flush_paragraph()
            bullets.append(bullet.group(1))
            continue
        flush_bullets()
        paragraph.append(stripped)

    flush_table()
    flush_bullets()
    flush_paragraph()
    body = "\n".join(parts) if parts else "<p></p>"
    return f"""
    <div class="ntl-reasoning-text ntl-reasoning-rich" style="--ntl-reasoning-accent:{safe_accent};">
      {body}
    </div>
    """


def _reasoning_divider_html() -> str:
    return '<div class="ntl-reasoning-divider"></div>'


def _render_reasoning_text(text: str, *, accent: str = "#bfdbfe", rich: bool = True) -> None:
    html_text = (
        _reasoning_rich_text_html(text, accent=accent)
        if rich
        else _reasoning_text_html(_sanitize_paths_in_text(str(text or "")), accent=accent)
    )
    st.markdown(html_text, unsafe_allow_html=True)


def render_label_ai(agent_name):
    meta = _reasoning_agent_meta(agent_name)
    st.markdown(
        _reasoning_header_html("AI", meta["label"], meta["role"], accent=meta["accent"]),
        unsafe_allow_html=True,
    )

def render_label_tool(tool_name):
    st.markdown(
        _reasoning_header_html("TOOL", str(tool_name or "tool"), _tr("工具输出", "Tool output"), accent="#93c5fd"),
        unsafe_allow_html=True,
    )

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

def render_event_human(content):
    st.markdown(
        _reasoning_header_html("USER", _tr("用户请求", "Human query"), "", accent="#fb7185"),
        unsafe_allow_html=True,
    )
    _render_reasoning_text(str(content or ""), accent="#fb7185")


def render_event_instruction(content):
    st.markdown(
        _reasoning_header_html("TASK", _tr("子任务指令", "Agent instruction"), "", accent="#a78bfa"),
        unsafe_allow_html=True,
    )
    _render_reasoning_text(str(content or ""), accent="#a78bfa")


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


def _workspace_quota_error_message(quota_rejection: dict) -> str:
    reason = str(quota_rejection.get("reason") or "")
    projected = str(quota_rejection.get("projected_label") or quota_rejection.get("projected_bytes") or "?")
    limit = str(quota_rejection.get("limit_label") or quota_rejection.get("limit_bytes") or "?")
    if reason == "thread_workspace_quota_reached":
        return _tr(
            f"当前线程工作区配额已超限，预计占用 {projected} / {limit}。",
            f"Thread workspace quota would be exceeded ({projected} / {limit}).",
        )
    if reason == "user_workspace_quota_reached":
        return _tr(
            f"当前用户工作区总配额已超限，预计占用 {projected} / {limit}。",
            f"User workspace quota would be exceeded ({projected} / {limit}).",
        )
    return _tr("工作区配额不足。", "Workspace quota exceeded.")


def _zip_member_target_paths(input_dir: Path, archive: zipfile.ZipFile) -> list[tuple[zipfile.ZipInfo, Path]]:
    targets: list[tuple[zipfile.ZipInfo, Path]] = []
    for info in archive.infolist():
        if info.is_dir():
            continue
        rel = Path(str(info.filename or "").replace("\\", "/"))
        if not rel.parts or rel.is_absolute() or ".." in rel.parts:
            raise ValueError(f"Unsafe zip member path: {info.filename}")
        targets.append((info, input_dir.joinpath(*rel.parts)))
    return targets


def _save_chat_input_files_to_workspace(files, thread_id: str, user_id: str = "") -> dict:
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
            payload = _decode_chat_file_bytes(uploaded_file)
            quota_rejection = app_logic._workspace_quota_rejection(thread_id, user_id, additional_bytes=len(payload))
            if quota_rejection:
                errors.append(f"{name}: {_workspace_quota_error_message(quota_rejection)}")
                continue
            with open(target, "wb") as f:
                f.write(payload)
            saved.append(target.name)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{name}: {exc}")

    return {"saved": saved, "errors": errors}


def get_user_input(*, disabled: bool = False):
    placeholder = _tr(
        "Query",
        "Query",
    )
    force_native = str(os.getenv("NTL_FORCE_NATIVE_CHAT_INPUT", "0")).strip().lower() in {"1", "true", "yes", "on"}
    if multimodal_chat_input is not None and not force_native and not disabled:
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
        if isinstance(rest, str) and rest.strip(): _render_reasoning_text(rest, accent="#fbbf24")
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

        with _render_popover("View Raw JSON"):
            _render_static_json_block(data, title="Raw JSON")
        if isinstance(rest, str) and rest.strip(): _render_reasoning_text(rest, accent="#fbbf24")
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
            _render_reasoning_text(reason, accent="#fbbf24")
        with _render_popover("View Raw JSON"):
            _render_static_json_block(data, title="Raw JSON")
        if isinstance(rest, str) and rest.strip():
            _render_reasoning_text(rest, accent="#fbbf24")
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
        st.markdown("**Product Identifier**"); _render_static_code_block(Product or "-", language="text", title="Product")
    with cols[1]:
        st.markdown("**Spatial Coverage**"); st.write(Spatial_coverage or "-")
        st.markdown("**Spatial Resolution**"); st.write(Spatial_resolution or "-")
        st.markdown("**Storage Location**"); _render_static_code_block(Storage_location or "Local Workspace (inputs/)", language="text", title="Storage")

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

    with _render_popover("View Raw JSON"):
        _render_static_json_block(data, title="Raw JSON")
    if isinstance(rest, str) and rest.strip(): _render_reasoning_text(rest, accent="#fbbf24")


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
        _render_static_json_block(data, title="Raw JSON")
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
    st.rerun()


def _logout_sidebar_session() -> None:
    if st.session_state.get("is_running", False):
        app_logic.request_stop_active_run(detach_session=True)
    st.cache_resource.clear()
    app_state.clear_authenticated_user()
    st.rerun()


def _render_auth_panel() -> None:
    _handle_gee_oauth_callback("")
    st.info(
        _tr(
            "请先注册或登录。账号与线程历史、长期记忆、并发配额都会绑定到真实 user_id。",
            "Register or log in first. Threads, memory, and quotas are bound to the real user_id.",
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
                st.error(_sanitize_paths_in_text(str(exc)))
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
                    st.error(_sanitize_paths_in_text(str(exc)))
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
        st.subheader(_tr("地缘环境智能计算平台 控制台", "地缘环境智能计算平台 Console"))

        workspace = storage_manager.get_workspace(st.session_state.get("thread_id", "debug"))
        # User/session scope: each user owns independent thread history in workspace.
        current_user_id = str(st.session_state.get("user_id", "") or "").strip()
        if not current_user_id or history_store.is_reserved_user_id(current_user_id):
            current_user_id = history_store.generate_anonymous_user_id()
            st.session_state["user_id"] = current_user_id
            st.session_state["user_name"] = ""

        current_user_name = str(st.session_state.get("user_name", "") or "")
        bootstrap_anonymous = str(current_user_id).startswith("anon-")
        typed_user_name = st.text_input(
            _tr("用户名（仅英文）", "Username (only English)"),
            value=current_user_name,
            key="sidebar_user_name_input",
            help=_tr("用于隔离用户历史记录。", "Used to isolate per-user history."),
        )

        normalized_typed_user = history_store.normalize_user_id(typed_user_name)
        stripped_user_name = str(typed_user_name or "").strip()
        username_ready = (
            bool(stripped_user_name)
            and (not history_store.is_reserved_user_id(normalized_typed_user))
            and (not bootstrap_anonymous)
        )
        if (not stripped_user_name) or history_store.is_reserved_user_id(normalized_typed_user):
            if stripped_user_name and stripped_user_name != current_user_name:
                st.warning(
                    _tr(
                        "guest/debug/default/anonymous 为保留用户名，请使用其他用户名。",
                        "guest/debug/default/anonymous are reserved names. Please choose another username.",
                    )
                )
                st.session_state["sidebar_user_name_input"] = current_user_name
        elif normalized_typed_user != current_user_id or bootstrap_anonymous:
            if st.session_state.get("is_running", False):
                app_logic.request_stop_active_run()
            st.session_state["user_name"] = typed_user_name or normalized_typed_user
            st.session_state["user_id"] = normalized_typed_user
            history_store.ensure_user_profile(normalized_typed_user, typed_user_name or normalized_typed_user)
            known_threads = history_store.list_user_threads(normalized_typed_user, limit=100)
            if known_threads:
                app_state.set_active_thread(str(known_threads[0].get("thread_id")))
            else:
                new_thread_id = history_store.generate_thread_id(normalized_typed_user)
                app_state.set_active_thread(new_thread_id)
                history_store.bind_thread_to_user(normalized_typed_user, new_thread_id)
            st.rerun()

        if not username_ready:
            st.warning(
                _tr(
                    "请先创建一个用户名。",
                    "Please create an username first.",
                )
            )

        if username_ready:
            _render_gee_pipeline_panel(current_user_id)

            user_threads = history_store.list_user_threads(current_user_id, limit=100)
            thread_ids = [str(row.get("thread_id")) for row in user_threads if row.get("thread_id")]
            thread_ids = [tid for tid in thread_ids if tid]
            current_tid = str(st.session_state.get("thread_id", "debug") or "debug")

            if not thread_ids:
                new_thread_id = history_store.generate_thread_id(current_user_id)
                app_state.set_active_thread(new_thread_id)
                history_store.bind_thread_to_user(current_user_id, new_thread_id)
                st.rerun()

            if current_tid not in thread_ids:
                app_state.set_active_thread(thread_ids[0])
                st.rerun()

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
                    key="sidebar_thread_selector",
                    label_visibility="visible",
                )
            with thread_del_col:
                st.markdown("<div style='height: 1.78rem;'></div>", unsafe_allow_html=True)
                if st.button(
                    "🗑",
                    key="delete_selected_thread_inline_trigger",
                    help=_tr("删除当前选中线程", "Delete selected thread"),
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
                        else:
                            app_state.set_active_thread(remaining_ids[0])

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

        _render_sidebar_thread_history_list(current_user_id, current_tid, user_threads)

        status = _tr("已激活", "Active") if st.session_state.get("initialized") else _tr("未激活", "Inactive")
        status_class = "active" if st.session_state.get("initialized") else "inactive"
        limit_snapshot = app_logic.get_run_limit_snapshot(current_user_id)
        global_limit = int(limit_snapshot.get("global_limit", 0) or 0)
        user_limit = int(limit_snapshot.get("user_limit", 0) or 0)
        global_usage = f"{int(limit_snapshot.get('global_active', 0) or 0)}/{global_limit}" if global_limit else f"{int(limit_snapshot.get('global_active', 0) or 0)}/∞"
        user_usage = f"{int(limit_snapshot.get('user_active', 0) or 0)}/{user_limit}" if user_limit else f"{int(limit_snapshot.get('user_active', 0) or 0)}/∞"
        st.markdown(
            (
                "<div class='ntl-thread-status-row'>"
                f"<span class='ntl-thread-status-item'><span>{_tr('线程 ID', 'Thread ID')}:</span>"
                f"<span class='ntl-thread-status-value'>{st.session_state.thread_id}</span></span>"
                f"<span class='ntl-thread-status-item'><span>{_tr('状态', 'Status')}:</span>"
                f"<span class='ntl-status-text {status_class}'>{status}</span></span>"
                "</div>"
                "<div class='ntl-thread-quota-row'>"
                f"<span class='ntl-thread-quota-pill'><span>MAX_RUNS</span><strong>{global_usage}</strong></span>"
                f"<span class='ntl-thread-quota-pill'><span>MAX_RUNS_PER_USER</span><strong>{user_usage}</strong></span>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
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
                    if case_id in _EXCLUDED_TEST_CASE_IDS:
                        continue
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

def render_sidebar():
    """Render sidebar with authenticated user accounts."""
    with st.sidebar:
        st.subheader(_tr("地缘环境智能计算平台 控制台", "地缘环境智能计算平台 Console"))

        if not st.session_state.get("authenticated"):
            _render_auth_panel()
            return

        workspace = storage_manager.get_workspace(st.session_state.get("thread_id", "debug"))
        current_user_id = str(st.session_state.get("user_id", "") or "").strip()
        current_user_name = str(st.session_state.get("user_name", "") or "").strip()
        st.caption(f"{_tr('当前用户', 'Current user')}: {current_user_name}")
        if st.button(_tr("退出登录", "Logout"), key="logout_btn_v2", width="stretch"):
            _logout_sidebar_session()

        _render_gee_pipeline_panel(current_user_id)

        user_threads = history_store.list_user_threads(current_user_id, limit=100)
        thread_ids = [str(row.get("thread_id")) for row in user_threads if row.get("thread_id")]
        thread_ids = [tid for tid in thread_ids if tid]
        current_tid = str(st.session_state.get("thread_id", "debug") or "debug")

        if not thread_ids:
            new_thread_id = history_store.generate_thread_id(current_user_id)
            app_state.set_active_thread(new_thread_id)
            history_store.bind_thread_to_user(current_user_id, new_thread_id)
            st.rerun()

        if current_tid not in thread_ids:
            app_state.set_active_thread(thread_ids[0])
            st.rerun()

        thread_label_map = {}
        for row in user_threads:
            tid = str(row.get("thread_id", "")).strip()
            if not tid:
                continue
            question = str(row.get("last_question", "") or "").strip()
            question_short = (question[:28] + "...") if len(question) > 28 else question
            thread_label_map[tid] = f"{tid} | {question_short}" if question_short else tid

        default_idx = thread_ids.index(current_tid) if current_tid in thread_ids else 0
        thread_sel_col, thread_del_col = st.columns([0.84, 0.16], gap="small")
        with thread_sel_col:
            selected_tid = st.selectbox(
                _tr("历史线程", "History Threads"),
                options=thread_ids,
                index=default_idx,
                format_func=lambda tid: thread_label_map.get(tid, tid),
                key="sidebar_thread_selector_v2",
                label_visibility="visible",
            )
        with thread_del_col:
            st.markdown("<div style='height: 1.78rem;'></div>", unsafe_allow_html=True)
            if st.button(
                "🗑",
                key="delete_selected_thread_inline_trigger_v2",
                help=_tr("删除当前选中线程", "Delete selected thread"),
                width="stretch",
            ):
                st.session_state["confirm_delete_selected_thread_inline_v2"] = True

        if selected_tid != current_tid:
            if st.session_state.get("is_running", False):
                app_logic.request_stop_active_run(thread_id=current_tid)
            app_state.set_active_thread(selected_tid)
            history_store.bind_thread_to_user(current_user_id, selected_tid)
            st.rerun()

        if st.session_state.get("confirm_delete_selected_thread_inline_v2", False):
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
                    key="delete_selected_thread_inline_confirm_btn_v2",
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
                    else:
                        app_state.set_active_thread(remaining_ids[0])

                    st.session_state["confirm_delete_selected_thread_inline_v2"] = False
                    if delete_result.get("deleted"):
                        st.success(_tr("线程已删除。", "Thread deleted."))
                    else:
                        st.warning(_tr("未找到可删除的线程记录。", "No thread record was deleted."))
                    st.rerun()
            with confirm_cols[1]:
                if st.button(
                    _tr("取消", "Cancel"),
                    key="delete_selected_thread_inline_cancel_btn_v2",
                    width="stretch",
                ):
                    st.session_state["confirm_delete_selected_thread_inline_v2"] = False
                    st.rerun()

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
            key="model_selector",
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
                key="user_api_key_input",
            )

        action_cols = st.columns(3, gap="small")
        with action_cols[0]:
            if st.button(
                _tr("激活", "Activate"),
                key="activate_btn_v2",
                width="stretch",
                type="secondary",
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

        with action_cols[1]:
            if st.button(
                _tr("新建", "New"),
                key="reset_btn_v2",
                width="stretch",
                type="secondary",
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

                new_thread_id = history_store.generate_thread_id(current_user_id)
                app_state.set_active_thread(new_thread_id)
                history_store.bind_thread_to_user(current_user_id, new_thread_id)
                st.warning(_tr("已创建新会话。", "New session created."))
                st.rerun()

        with action_cols[2]:
            if st.button(
                _tr("中断", "Stop"),
                key="interrupt_current_run_btn_v2",
                width="stretch",
                type="secondary",
                help=_tr("请求立即中断当前回答。", "Request immediate interruption of the current run."),
            ):
                if st.session_state.get("is_running", False):
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
                st.success(_tr("待生效设置已应用。", "Queued settings applied."))
                st.rerun()

        _render_sidebar_thread_history_list(current_user_id, current_tid, user_threads)

        status = _tr("已激活", "Active") if st.session_state.get("initialized") else _tr("未激活", "Inactive")
        status_class = "active" if st.session_state.get("initialized") else "inactive"
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
        st.markdown(
            (
                "<div class='ntl-thread-status-row'>"
                f"<span class='ntl-thread-status-item'><span>{_tr('线程 ID', 'Thread ID')}:</span>"
                f"<span class='ntl-thread-status-value'>{st.session_state.thread_id}</span></span>"
                f"<span class='ntl-thread-status-item'><span>{_tr('状态', 'Status')}:</span>"
                f"<span class='ntl-status-text {status_class}'>{status}</span></span>"
                "</div>"
                "<div class='ntl-thread-quota-row'>"
                f"<span class='ntl-thread-quota-pill'><span>MAX_RUNS</span><strong>{global_usage}</strong></span>"
                f"<span class='ntl-thread-quota-pill'><span>MAX_RUNS_PER_USER</span><strong>{user_usage}</strong></span>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
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
                df_cases = df_cases.dropna(subset=["Case"])
                df_cases = df_cases.drop_duplicates(subset=["Case"], keep="first")
                df_cases["Category"] = df_cases["Category"].fillna("General").astype(str)
                df_cases["Label"] = df_cases["Label"].fillna("Unnamed Task").astype(str)
                categories = {}
                for _, row in df_cases.iterrows():
                    case_id = _test_case_id_from_row(row)
                    if case_id in _EXCLUDED_TEST_CASE_IDS:
                        continue
                    case = _localized_test_case(
                        row["Category"].strip(),
                        row["Label"].strip(),
                        str(row["Case"]).strip(),
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
                            if st.button(_tr("运行用例", "Run Case"), key=f"run_case_v2_{case_key}", width="stretch"):
                                st.session_state["pending_question"] = case["query"]
                                st.rerun()
            except Exception as e:
                st.error(f"Failed to load test case files: {e}")


def _render_sidebar_thread_history_list(current_user_id: str, current_tid: str, user_threads: list[dict]) -> None:
    st.session_state.setdefault("sidebar_threads_show_all", False)
    st.session_state.setdefault("sidebar_thread_rename_target", "")
    st.session_state.setdefault("sidebar_thread_delete_target", "")
    st.session_state.setdefault("sidebar_thread_menu_target", "")
    st.session_state.setdefault("sidebar_thread_rename_input", "")

    st.markdown(f"**{_tr('历史线程', 'History Threads')}**")
    show_all_threads = bool(st.session_state.get("sidebar_threads_show_all", False))
    rename_target = str(st.session_state.get("sidebar_thread_rename_target", "") or "").strip()
    delete_target = str(st.session_state.get("sidebar_thread_delete_target", "") or "").strip()
    menu_target = str(st.session_state.get("sidebar_thread_menu_target", "") or "").strip()
    visible_threads = user_threads if show_all_threads else user_threads[: history_store.DEFAULT_THREAD_LIST_LIMIT]

    for row in visible_threads:
        tid = str(row.get("thread_id", "")).strip()
        if not tid:
            continue
        title = history_store.resolve_thread_title(row)
        is_current = tid == current_tid
        button_key = f"thread_current_btn_v4_{tid}" if is_current else f"thread_item_btn_v4_{tid}"
        if st.button(
            title,
            key=button_key,
            width="stretch",
            type="secondary",
            help=title,
        ):
            if is_current:
                st.session_state["sidebar_thread_menu_target"] = "" if menu_target == tid else tid
                st.session_state["sidebar_thread_delete_target"] = ""
                st.session_state["sidebar_thread_rename_target"] = ""
                st.rerun()
            else:
                if st.session_state.get("is_running", False):
                    app_logic.request_stop_active_run(thread_id=current_tid)
                st.session_state["sidebar_thread_menu_target"] = ""
                st.session_state["sidebar_thread_delete_target"] = ""
                st.session_state["sidebar_thread_rename_target"] = ""
                app_state.set_active_thread(tid)
                history_store.bind_thread_to_user(current_user_id, tid)
                st.rerun()

        if is_current:
            menu_cols = st.columns([0.66, 0.15, 0.19], gap="small")
            with menu_cols[1]:
                if st.button(_tr("重命名", "Rename"), key=f"thread_rename_inline_v6_{tid}"):
                    st.session_state["sidebar_thread_rename_target"] = tid
                    st.session_state["sidebar_thread_delete_target"] = ""
                    st.session_state["sidebar_thread_rename_input"] = title
                    st.rerun()
            with menu_cols[2]:
                if st.button(_tr("删除", "Delete"), key=f"thread_delete_inline_v6_{tid}"):
                    st.session_state["sidebar_thread_delete_target"] = tid
                    st.session_state["sidebar_thread_rename_target"] = ""
                    st.rerun()

        if rename_target == tid:
            current_draft = str(st.session_state.get("sidebar_thread_rename_input", "") or title)
            with st.form(f"rename_thread_form_v7_{tid}", clear_on_submit=False):
                new_title = st.text_input(
                    _tr("线程标题", "Thread title"),
                    value=current_draft,
                    key=f"rename_thread_input_v7_{tid}",
                    label_visibility="collapsed",
                    placeholder=_tr("输入线程标题", "Enter thread title"),
                )
                st.session_state["sidebar_thread_rename_input"] = new_title
                rename_cols = st.columns([0.46, 0.25, 0.29], gap="small")
                with rename_cols[1]:
                    save_submitted = st.form_submit_button(
                        _tr("保存", "Save"),
                        key=f"rename_thread_save_v7_{tid}",
                    )
                with rename_cols[2]:
                    cancel_submitted = st.form_submit_button(
                        _tr("取消", "Cancel"),
                        key=f"rename_thread_cancel_v7_{tid}",
                    )

            if save_submitted:
                try:
                    history_store.rename_user_thread(current_user_id, tid, new_title)
                except ValueError as exc:
                    st.warning(str(exc))
                else:
                    st.session_state["sidebar_thread_rename_target"] = ""
                    st.session_state["sidebar_thread_rename_input"] = ""
                    st.session_state["sidebar_thread_menu_target"] = ""
                    st.rerun()

            if cancel_submitted:
                st.session_state["sidebar_thread_rename_target"] = ""
                st.session_state["sidebar_thread_rename_input"] = ""
                st.session_state["sidebar_thread_menu_target"] = ""
                st.rerun()

        if delete_target == tid:
            st.caption(
                _tr(
                    f"删除 `{title}`？该操作会移除历史和工作区文件。",
                    f"Delete `{title}`? This removes history and workspace files.",
                )
            )
            delete_cols = st.columns([0.44, 0.25, 0.31], gap="small")
            with delete_cols[1]:
                if st.button(_tr("删除", "Delete"), key=f"delete_thread_confirm_v8_{tid}"):
                    if st.session_state.get("is_running", False):
                        app_logic.request_stop_active_run(thread_id=tid, detach_session=True)
                    delete_result = history_store.delete_user_thread(
                        current_user_id,
                        tid,
                        delete_workspace=True,
                    )
                    remaining = history_store.list_user_threads(current_user_id, limit=100)
                    remaining_ids = [str(item.get("thread_id")) for item in remaining if item.get("thread_id")]
                    if not remaining_ids:
                        new_thread_id = history_store.generate_thread_id(current_user_id)
                        history_store.bind_thread_to_user(current_user_id, new_thread_id)
                        app_state.set_active_thread(new_thread_id)
                    elif tid == current_tid:
                        app_state.set_active_thread(remaining_ids[0])
                    st.session_state["sidebar_thread_delete_target"] = ""
                    st.session_state["sidebar_thread_menu_target"] = ""
                    if delete_result.get("deleted"):
                        st.success(_tr("线程已删除。", "Thread deleted."))
                    else:
                        st.warning(_tr("未找到可删除的线程记录。", "No thread record was deleted."))
                    st.rerun()
            with delete_cols[2]:
                if st.button(_tr("取消", "Cancel"), key=f"delete_thread_cancel_v8_{tid}"):
                    st.session_state["sidebar_thread_delete_target"] = ""
                    st.session_state["sidebar_thread_menu_target"] = ""
                    st.rerun()

    if len(user_threads) > history_store.DEFAULT_THREAD_LIST_LIMIT:
        toggle_label = _tr("收起", "Show less") if show_all_threads else _tr("查看更多", "Show more")
        if st.button(toggle_label, key="sidebar_threads_toggle_btn_v3", width="stretch"):
            st.session_state["sidebar_threads_show_all"] = not show_all_threads
            st.rerun()


def _validate_gee_project_for_current_runtime(project_id: str) -> tuple[bool, str]:
    value = str(project_id or "").strip()
    if not value:
        return False, "Project ID is empty."
    try:
        import ee

        ee.Initialize(project=value)
        return True, ""
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def _query_param_value(name: str) -> str:
    try:
        raw = st.query_params.get(name, "")
    except Exception:
        return ""
    if isinstance(raw, list):
        return str(raw[0] if raw else "").strip()
    return str(raw or "").strip()


def _clear_gee_oauth_query_params() -> None:
    try:
        if "gee_oauth" in st.query_params:
            del st.query_params["gee_oauth"]
        if "code" in st.query_params:
            del st.query_params["code"]
        if "state" in st.query_params:
            del st.query_params["state"]
        if "error" in st.query_params:
            del st.query_params["error"]
    except Exception:
        pass


def _handle_gee_oauth_callback(current_user_id: str) -> None:
    expected_state = str(st.session_state.get("gee_oauth_state") or "")
    state = _query_param_value("state")
    if _query_param_value("gee_oauth") != "1" and not expected_state and not state:
        return
    error = _query_param_value("error")
    if error:
        st.error(f"Google OAuth failed: {error}")
        _clear_gee_oauth_query_params()
        return
    code = _query_param_value("code")
    if not code:
        return
    try:
        profile = gee_auth.complete_oauth_callback(
            user_id=current_user_id,
            code=code,
            expected_state=expected_state,
            received_state=state,
            history_store_module=history_store,
        )
        connected_user_id = str(profile.get("user_id") or current_user_id)
        st.session_state["gee_oauth_state"] = ""
        st.session_state["gee_oauth_url"] = ""
        app_state.sync_gee_profile_state(connected_user_id)
        st.success("认证成功，请关闭此页面，回到原来的网页。")
        components.html(gee_auth.popup_close_html("认证成功，请关闭此页面，回到原来的网页。"), height=160)
    except Exception as exc:  # noqa: BLE001
        existing_profile = {}
        try:
            state_payload = gee_auth.verify_oauth_state(state) if state else {}
            existing_user_id = str(state_payload.get("user_id") or current_user_id or "").strip()
            if existing_user_id:
                existing_profile = history_store.get_user_gee_profile(existing_user_id)
        except Exception:
            existing_profile = {}
        message = gee_auth.oauth_failure_message(exc, existing_profile=existing_profile)
        st.error(message)
        components.html(gee_auth.popup_close_html(message), height=160)
    finally:
        _clear_gee_oauth_query_params()
        st.stop()


def _validate_gee_project_with_profile(project_id: str, profile: dict) -> tuple[bool, str]:
    value = str(project_id or "").strip()
    if not value:
        return False, "Project ID is empty."
    try:
        import ee

        encrypted = str(profile.get("encrypted_refresh_token") or "").strip()
        if encrypted:
            refresh_token = gee_auth.decrypt_refresh_token(encrypted)
            scopes = str(profile.get("token_scopes") or "").split() or None
            credentials = gee_auth.credentials_from_refresh_token(refresh_token, scopes=scopes)
            ee.Initialize(credentials=credentials, project=value)
        else:
            ee.Initialize(project=value)
        return True, ""
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def _render_gee_pipeline_panel(current_user_id: str) -> None:
    _handle_gee_oauth_callback(current_user_id)
    profile = app_state.sync_gee_profile_state(current_user_id)
    default_project = str(profile.get("default_project_id") or "").strip()
    current_project = str(profile.get("gee_project_id") or "").strip()
    current_mode = str(profile.get("mode") or "default").strip()
    mode_options = ["default", "user"]
    mode_labels = {
        "default": "Default pipeline",
        "user": "My GEE pipeline (recommended)",
    }

    with st.expander("GEE Pipeline", expanded=False):
        st.caption(f"Active: {mode_labels.get(current_mode, current_mode)}")
        st.caption(f"Project: {profile.get('effective_project_id') or 'not configured'}")
        last_error = str(profile.get("last_error") or "").strip()
        if last_error:
            st.caption(f"Last error: {last_error}")
        if profile.get("oauth_connected"):
            st.caption(f"Google account: {profile.get('google_email') or 'connected'}")
        elif current_mode == "user":
            st.caption("Google OAuth is required before user-owned GEE execution is fully isolated.")

        selected_mode = st.radio(
            "Pipeline",
            mode_options,
            index=mode_options.index(current_mode) if current_mode in mode_options else 0,
            format_func=lambda value: mode_labels.get(value, value),
            key="gee_pipeline_mode_input_v1",
            label_visibility="collapsed",
        )
        user_project = st.text_input(
            "GEE Project ID",
            value=current_project,
            placeholder="my-earthengine-project",
            key="gee_project_id_input_v1",
            help="Use your own Earth Engine enabled Google Cloud project. OAuth credentials will be added in the next stage.",
        )
        st.caption(f"Default project: {default_project or 'not configured'}")

        oauth_connected = bool(profile.get("oauth_connected"))
        oauth_label = gee_auth.oauth_action_label(oauth_connected, selected_mode)
        if not gee_auth.oauth_configured():
            st.caption("OAuth not configured. Set GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET, and NTL_TOKEN_ENCRYPTION_KEY.")
        elif oauth_label:
            if st.button(oauth_label, key="gee_oauth_connect_btn_v2", width="stretch"):
                state = gee_auth.generate_oauth_state(current_user_id)
                st.session_state["gee_oauth_state"] = state
                oauth_url = gee_auth.build_authorization_url(state)
                st.session_state["gee_oauth_url"] = oauth_url
                components.html(gee_auth.popup_authorization_html(oauth_url), height=0)

        if st.button("Save & Validate", key="gee_profile_save_validate_btn_v1", width="stretch"):
            target_project = user_project.strip() if selected_mode == "user" else ""
            if selected_mode == "user" and not target_project:
                history_store.save_user_gee_profile(
                    current_user_id,
                    mode=selected_mode,
                    gee_project_id=target_project,
                    status="default",
                    last_error="User pipeline needs a GEE Project ID.",
                )
                app_state.sync_gee_profile_state(current_user_id)
                st.warning("User pipeline needs a GEE Project ID. Falling back to default.")
                st.rerun()

            saved_profile = history_store.save_user_gee_profile(
                current_user_id,
                mode=selected_mode,
                gee_project_id=target_project,
                status="configured",
                last_error="",
            )
            project_to_validate = target_project if selected_mode == "user" else default_project
            validation_profile = saved_profile if selected_mode == "user" else {}
            ok, error = _validate_gee_project_with_profile(project_to_validate, validation_profile)
            history_store.save_user_gee_profile(
                current_user_id,
                mode=selected_mode,
                gee_project_id=target_project,
                status="validated" if ok else "error",
                last_error="" if ok else error[:500],
                validated_at=int(time.time()) if ok else 0,
            )
            app_state.sync_gee_profile_state(current_user_id)
            if ok:
                st.success("GEE pipeline saved and validated.")
            else:
                st.error("GEE validation failed. Check project ID, API enablement, and IAM.")
            st.rerun()


def render_sidebar():
    """Render sidebar with authenticated user accounts."""
    with st.sidebar:
        st.subheader(_tr("地缘环境智能计算平台 控制台", "地缘环境智能计算平台 Console"))

        if not st.session_state.get("authenticated"):
            _render_auth_panel()
            return

        workspace = storage_manager.get_workspace(st.session_state.get("thread_id", "debug"))
        current_user_id = str(st.session_state.get("user_id", "") or "").strip()
        current_user_name = str(st.session_state.get("user_name", "") or "").strip()
        st.caption(f"{_tr('当前用户', 'Current user')}: {current_user_name}")
        if st.button(_tr("退出登录", "Logout"), key="logout_btn_v3", width="stretch"):
            _logout_sidebar_session()

        _render_gee_pipeline_panel(current_user_id)

        user_threads = history_store.list_user_threads(current_user_id, limit=100)
        thread_ids = [str(row.get("thread_id")) for row in user_threads if row.get("thread_id")]
        thread_ids = [tid for tid in thread_ids if tid]
        current_tid = str(st.session_state.get("thread_id", "debug") or "debug")

        if not thread_ids:
            new_thread_id = history_store.generate_thread_id(current_user_id)
            app_state.set_active_thread(new_thread_id)
            history_store.bind_thread_to_user(current_user_id, new_thread_id)
            st.rerun()

        if current_tid not in thread_ids:
            app_state.set_active_thread(thread_ids[0])
            st.rerun()

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
            key="model_selector",
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
                key="user_api_key_input",
            )

        action_cols = st.columns(3, gap="small")
        with action_cols[0]:
            if st.button(
                _tr("激活", "Activate"),
                key="activate_btn_v3",
                width="stretch",
                type="secondary",
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

        with action_cols[1]:
            if st.button(
                _tr("新建", "New"),
                key="reset_btn_v3",
                width="stretch",
                type="secondary",
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

                new_thread_id = history_store.generate_thread_id(current_user_id)
                app_state.set_active_thread(new_thread_id)
                history_store.bind_thread_to_user(current_user_id, new_thread_id)
                st.warning(_tr("已创建新会话。", "New session created."))
                st.rerun()

        with action_cols[2]:
            if st.button(
                _tr("中断", "Stop"),
                key="interrupt_current_run_btn_v3",
                width="stretch",
                type="secondary",
                help=_tr("请求立即中断当前回答。", "Request immediate interruption of the current run."),
            ):
                if st.session_state.get("is_running", False):
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
                st.success(_tr("待生效设置已应用。", "Queued settings applied."))
                st.rerun()

        _render_sidebar_thread_history_list(current_user_id, current_tid, user_threads)

        status = _tr("已激活", "Active") if st.session_state.get("initialized") else _tr("未激活", "Inactive")
        status_class = "active" if st.session_state.get("initialized") else "inactive"
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
        st.markdown(
            (
                "<div class='ntl-thread-status-row'>"
                f"<span class='ntl-thread-status-item'><span>{_tr('线程 ID', 'Thread ID')}:</span>"
                f"<span class='ntl-thread-status-value'>{st.session_state.thread_id}</span></span>"
                f"<span class='ntl-thread-status-item'><span>{_tr('状态', 'Status')}:</span>"
                f"<span class='ntl-status-text {status_class}'>{status}</span></span>"
                "</div>"
                "<div class='ntl-thread-quota-row'>"
                f"<span class='ntl-thread-quota-pill'><span>MAX_RUNS</span><strong>{global_usage}</strong></span>"
                f"<span class='ntl-thread-quota-pill'><span>MAX_RUNS_PER_USER</span><strong>{user_usage}</strong></span>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
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
                df_cases = df_cases.dropna(subset=["Case"])
                df_cases = df_cases.drop_duplicates(subset=["Case"], keep="first")
                df_cases["Category"] = df_cases["Category"].fillna("General").astype(str)
                df_cases["Label"] = df_cases["Label"].fillna("Unnamed Task").astype(str)
                categories = {}
                for _, row in df_cases.iterrows():
                    case_id = _test_case_id_from_row(row)
                    if case_id in _EXCLUDED_TEST_CASE_IDS:
                        continue
                    case = _localized_test_case(
                        row["Category"].strip(),
                        row["Label"].strip(),
                        str(row["Case"]).strip(),
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
                            if st.button(_tr("运行用例", "Run Case"), key=f"run_case_v3_{case_key}", width="stretch"):
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
        thread_id = str(st.session_state.get("thread_id", "debug"))
        user_id = str(st.session_state.get("user_id", "guest"))
        workspace = storage_manager.get_workspace(thread_id)
        input_dir = workspace / "inputs"
        for uploaded_file in uploaded_files:
            target_path = input_dir / uploaded_file.name
            if not target_path.exists():
                try:
                    if uploaded_file.name.endswith(".zip"):
                        with zipfile.ZipFile(uploaded_file, "r") as zip_ref:
                            targets = _zip_member_target_paths(input_dir, zip_ref)
                            projected_bytes = sum(max(0, int(info.file_size or 0)) for info, _target in targets)
                            quota_rejection = app_logic._workspace_quota_rejection(
                                thread_id,
                                user_id,
                                additional_bytes=projected_bytes,
                            )
                            if quota_rejection:
                                st.sidebar.warning(_workspace_quota_error_message(quota_rejection))
                                continue
                            for info, member_target in targets:
                                member_target.parent.mkdir(parents=True, exist_ok=True)
                                with zip_ref.open(info, "r") as src, open(member_target, "wb") as dst:
                                    shutil.copyfileobj(src, dst)
                        st.sidebar.success(_tr(f"已解压：{uploaded_file.name}", f"Extracted: {uploaded_file.name}"))
                    else:
                        payload = uploaded_file.getbuffer()
                        quota_rejection = app_logic._workspace_quota_rejection(
                            thread_id,
                            user_id,
                            additional_bytes=len(payload),
                        )
                        if quota_rejection:
                            st.sidebar.warning(_workspace_quota_error_message(quota_rejection))
                            continue
                        with open(target_path, "wb") as f:
                            f.write(payload)
                    st.sidebar.success(_tr(f"已上传：{uploaded_file.name}", f"Uploaded: {uploaded_file.name}"))
                except Exception as exc:  # noqa: BLE001
                    st.sidebar.error(_sanitize_paths_in_text(str(exc)))


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
            file_name = Path(str(content)).name or os.path.basename(str(content))
            _render_image_artifact(
                content,
                key_prefix=f"chat_img_{i}",
                caption=_tr(f"图像结果: {file_name}", f"Plot: {file_name}"),
            )
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

    if overall_bounds:
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
    tool_meta_by_id: dict[str, dict] = {}
    seen_user_query = False
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
                tool_meta_by_id.update(_extract_tool_call_metadata(msg))
                agent = (msg.name or "AI")
                if "(streaming)" in agent.lower():
                    # Skip transient streaming markers; final messages are rendered below.
                    continue
                if grouped and grouped[-1]["kind"] == "ai" and grouped[-1]["agent"] == agent:
                    grouped[-1]["messages"].append(msg)
                else:
                    grouped.append({"kind": "ai", "agent": agent, "messages": [msg]})
            elif isinstance(msg, ToolMessage):
                tool_name = msg.name or "tool"
                tool_call_id = str(getattr(msg, "tool_call_id", "") or "").strip()
                tool_meta = tool_meta_by_id.get(tool_call_id) if tool_call_id else None
                if grouped and grouped[-1]["kind"] == "tool" and grouped[-1]["tool"] == tool_name:
                    grouped[-1]["messages"].append(msg)
                else:
                    grouped.append({"kind": "tool", "tool": tool_name, "messages": [msg], "tool_meta": {}})
                if tool_meta:
                    grouped[-1].setdefault("tool_meta", {})[tool_call_id] = tool_meta
            elif isinstance(msg, HumanMessage):
                kind = "human" if not seen_user_query else "instruction"
                seen_user_query = True
                if grouped and grouped[-1]["kind"] == kind:
                    grouped[-1]["messages"].append(msg)
                else:
                    grouped.append({"kind": kind, "messages": [msg]})
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


_PYTHON_SUFFIXES = {".py", ".pyw", ".ipynb"}
_CSV_SUFFIXES = {".csv", ".tsv"}
_IMAGE_SUFFIXES_FOR_REF = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tif", ".tiff"}
_VECTOR_SUFFIXES = {".shp", ".geojson", ".gpkg", ".kml"}


def _classify_file_reference(path: str) -> dict:
    """Return display metadata for a file path used by read_file/read_files."""
    raw_path = str(path or "").strip()
    normalized = raw_path.replace("\\", "/")
    lower = normalized.lower()
    suffix = Path(normalized).suffix.lower()

    is_skill_path = (
        "/skills/" in lower
        or lower.startswith("skills/")
        or lower.startswith("/skills/")
        or ".ntl-gpt/skills/" in lower
    )

    if is_skill_path and suffix in {".md", ".markdown"}:
        kind = "skill"
        label = _tr("Skill 文件", "Skill file")
        language = "markdown"
    elif is_skill_path and suffix in _PYTHON_SUFFIXES:
        kind = "python"
        label = _tr("Skill Python 脚本", "Skill Python script")
        language = "python"
    elif is_skill_path and suffix in _CSV_SUFFIXES:
        kind = "csv"
        label = _tr("Skill CSV/TSV 表格", "Skill CSV/TSV table")
        language = "csv"
    elif is_skill_path and suffix in _IMAGE_SUFFIXES_FOR_REF:
        kind = "image"
        label = _tr("Skill 影像/图片", "Skill image/raster")
        language = "text"
    elif is_skill_path and suffix in _VECTOR_SUFFIXES:
        kind = "vector"
        label = _tr("Skill 矢量地理数据", "Skill vector geodata")
        language = "json" if suffix == ".geojson" else "text"
    elif is_skill_path and suffix in {".json", ".jsonl", ".ndjson"}:
        kind = "json"
        label = _tr("Skill JSON 数据", "Skill JSON data")
        language = "json"
    elif is_skill_path:
        kind = "skill"
        label = _tr("Skill 文件", "Skill file")
        language = "text"
    elif suffix in _PYTHON_SUFFIXES:
        kind = "python"
        label = _tr("Python 脚本", "Python script")
        language = "python"
    elif suffix in _CSV_SUFFIXES:
        kind = "csv"
        label = _tr("CSV/TSV 表格", "CSV/TSV table")
        language = "csv"
    elif suffix in _IMAGE_SUFFIXES_FOR_REF:
        kind = "image"
        label = _tr("影像/图片", "Image/raster")
        language = "text"
    elif suffix in _VECTOR_SUFFIXES:
        kind = "vector"
        label = _tr("矢量地理数据", "Vector geodata")
        language = "json" if suffix == ".geojson" else "text"
    elif suffix in {".json", ".jsonl", ".ndjson"}:
        kind = "json"
        label = _tr("JSON 数据", "JSON data")
        language = "json"
    elif suffix in {".md", ".markdown"}:
        kind = "markdown"
        label = _tr("Markdown 文档", "Markdown document")
        language = "markdown"
    else:
        kind = "file"
        label = _tr("文件", "File")
        language = "text"

    return {
        "path": raw_path,
        "display_path": normalized,
        "name": Path(normalized).name or normalized,
        "kind": kind,
        "label": label,
        "language": language,
    }


def _looks_like_path(value: str) -> bool:
    text = str(value or "").strip()
    if not text or "\n" in text:
        return False
    suffix = Path(text.replace("\\", "/")).suffix.lower()
    return bool(suffix) or "/" in text or "\\" in text


def _extract_paths_from_tool_args(args) -> list[str]:
    paths: list[str] = []

    def add(value) -> None:
        if isinstance(value, str) and _looks_like_path(value):
            paths.append(value)
        elif isinstance(value, (list, tuple)):
            for item in value:
                add(item)

    if isinstance(args, dict):
        for key in (
            "file_path",
            "file_paths",
            "path",
            "paths",
            "files",
            "file",
            "target_file",
            "target_files",
        ):
            if key in args:
                add(args.get(key))
    else:
        add(args)

    seen = set()
    out = []
    for path in paths:
        normalized = str(path).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _extract_change_preview_from_tool_args(args) -> dict:
    if not isinstance(args, dict):
        return {}
    before = None
    after = None
    for key in ("old_string", "old_str", "old", "before", "search"):
        if isinstance(args.get(key), str):
            before = args.get(key)
            break
    for key in ("new_string", "new_str", "new", "after", "replace"):
        if isinstance(args.get(key), str):
            after = args.get(key)
            break
    preview = {}
    if before is not None:
        preview["before"] = str(before)
    if after is not None:
        preview["after"] = str(after)
    return preview


def _normalize_tool_call(raw_call) -> tuple[str, str, object] | None:
    if not isinstance(raw_call, dict):
        return None
    call_id = str(raw_call.get("id") or raw_call.get("tool_call_id") or "").strip()
    name = str(raw_call.get("name") or "").strip()
    args = raw_call.get("args")
    function = raw_call.get("function")
    if isinstance(function, dict):
        name = name or str(function.get("name") or "").strip()
        raw_args = function.get("arguments")
        if isinstance(raw_args, str):
            try:
                args = json.loads(raw_args)
            except Exception:
                args = raw_args
        elif raw_args is not None:
            args = raw_args
    if not call_id or not name:
        return None
    return call_id, name, args


def _extract_tool_call_metadata(msg: AIMessage) -> dict:
    """Extract display metadata from AI tool call arguments."""
    metadata: dict[str, dict] = {}
    raw_calls = []
    tool_calls = getattr(msg, "tool_calls", None)
    if isinstance(tool_calls, list):
        raw_calls.extend(tool_calls)
    additional = getattr(msg, "additional_kwargs", None)
    if isinstance(additional, dict) and isinstance(additional.get("tool_calls"), list):
        raw_calls.extend(additional.get("tool_calls") or [])

    for raw_call in raw_calls:
        normalized = _normalize_tool_call(raw_call)
        if normalized is None:
            continue
        call_id, name, args = normalized
        file_refs = []
        name_norm = str(name).strip().lower()
        if name_norm in {"read_file", "read_files", "edit_file", "write_file"}:
            file_refs = [_classify_file_reference(path) for path in _extract_paths_from_tool_args(args)]
        metadata[call_id] = {
            "name": name,
            "args": args,
            "file_refs": file_refs,
        }
        if name_norm == "edit_file":
            metadata[call_id]["change_preview"] = _extract_change_preview_from_tool_args(args)
    return metadata


def _looks_like_code_assistant_code(text: str) -> bool:
    """Keep real scripts in code blocks while rendering prose as readable Markdown."""
    raw = str(text or "").strip()
    if not raw:
        return False
    if raw.startswith("```"):
        return True
    code_markers = (
        "import ",
        "from ",
        "def ",
        "class ",
        "if __name__",
        "with ",
        "for ",
        "while ",
        "try:",
    )
    lines = [line for line in raw.splitlines() if line.strip()]
    if any(line.lstrip().startswith(code_markers) for line in lines):
        return True
    indented = sum(1 for line in lines if line.startswith(("    ", "\t")))
    assignments = sum(1 for line in lines if re.match(r"^[A-Za-z_][A-Za-z0-9_]*\s*=", line.strip()))
    return len(lines) >= 3 and (indented >= 1 or assignments >= 2)


def _render_read_file_tool_output(raw_content, metadata: dict | None = None) -> None:
    meta = metadata or {}
    refs = [ref for ref in meta.get("file_refs", []) if isinstance(ref, dict)]
    tool_name = str(meta.get("name") or "read_file")
    title = _tr("读取文件", "Read file") if tool_name == "read_file" else _tr("读取多个文件", "Read files")
    _render_tool_file_card(title, refs)
    language = "text"
    if refs:
        language = str(refs[0].get("language") or "text")
    title_ref = ""
    if refs:
        title_ref = str(refs[0].get("display_path") or refs[0].get("path") or "")
    _render_static_code_block(
        _sanitize_paths_in_text(str(raw_content or "")),
        language=language,
        title=title_ref,
    )


def _render_tool_file_card(title: str, refs: list[dict], *, message: str = "") -> None:
    safe_title = html.escape(str(title or "Tool output"))
    count = len(refs)
    count_text = _tr(f"{count} 个文件", f"{count} file{'s' if count != 1 else ''}") if count else _tr("未识别文件", "No file detected")
    rows = []
    if refs:
        for ref in refs:
            label = html.escape(str(ref.get("label") or "File"))
            path = html.escape(str(ref.get("display_path") or ref.get("path") or ""))
            rows.append(
                f"<div class='ntl-file-row'><span class='ntl-file-kind'>{label}</span><span class='ntl-file-path'>{path}</span></div>"
            )
    else:
        rows.append(
            "<div class='ntl-file-row'><span class='ntl-file-kind'>File</span>"
            f"<span class='ntl-file-path'>{html.escape(_tr('未从工具参数中找到文件路径。', 'No file path was found in the tool arguments.'))}</span></div>"
        )
    safe_message = html.escape(message.strip()) if isinstance(message, str) and message.strip() else ""
    st.markdown(
        f"""
        <div class="ntl-tool-card">
          <div class="ntl-tool-card-head">
            <span class="ntl-tool-title">{safe_title}</span>
            <span class="ntl-tool-count">{html.escape(count_text)}</span>
          </div>
          <div class="ntl-file-list">{''.join(rows)}</div>
          {f'<div class="ntl-tool-message">{safe_message}</div>' if safe_message else ''}
        </div>
        """,
        unsafe_allow_html=True,
    )


def _truncate_tool_preview(text: str, max_chars: int = 520) -> str:
    value = str(text or "")
    if len(value) <= max_chars:
        return value
    return value[: max_chars - 1].rstrip() + "..."


def _render_edit_file_tool_output(raw_content, metadata: dict | None = None) -> None:
    meta = metadata or {}
    refs = [ref for ref in meta.get("file_refs", []) if isinstance(ref, dict)]
    _render_tool_file_card(_tr("修改文件", "Edit file"), refs, message=str(raw_content or ""))
    change = meta.get("change_preview") if isinstance(meta.get("change_preview"), dict) else {}
    before = change.get("before")
    after = change.get("after")
    if before is None and after is None:
        return
    before_html = html.escape(_truncate_tool_preview(str(before or "")))
    after_html = html.escape(_truncate_tool_preview(str(after or "")))
    st.markdown(
        f"""
        <div class="ntl-change-grid">
          <div class="ntl-change-pane">
            <span class="ntl-change-label">{html.escape(_tr('修改前', 'Before'))}</span>
            <pre class="ntl-change-text">{before_html}</pre>
          </div>
          <div class="ntl-change-pane">
            <span class="ntl-change-label">{html.escape(_tr('修改后', 'After'))}</span>
            <pre class="ntl-change-text">{after_html}</pre>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


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
        return "Engineer"
    m = re.search(r"transfer_to_([a-z0-9_]+)", name)
    if m:
        suffix = m.group(1)
        return "_".join(part.capitalize() for part in suffix.split("_"))
    return None


def _build_reasoning_graph_payload(events, show_sub_steps: bool = False):
    grouped = _build_reasoning_sections(events)
    if not grouped:
        return None

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

        if kind == "instruction":
            human_idx += 1
            node_id = f"task_{human_idx}"
            add_node(node_id, "Agent Instruction", "instruction")
            add_edge(last_anchor, node_id, "handoff_edge")
            last_anchor = node_id
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
        'graph [bgcolor="#071021", splines=true, overlap=false];',
        'node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=10, fontcolor="#eaf1ff", color="#6b86bd", penwidth=1.4];',
        'edge [color="#7da7e8", fontcolor="#eaf1ff"];',
    ]
    for node in nodes:
        data = node.get("data", {})
        node_id = str(data.get("id", "n")).replace("-", "_")
        label = _escape_dot_label(data.get("label") or node_id)
        kind = str(data.get("kind", "default"))
        fill = "#162b52"
        if kind == "human":
            fill = "#1f4f7a"
        elif kind == "ai":
            fill = "#17635b"
        elif kind == "tool":
            fill = "#29476f"
        elif kind == "tool_kb":
            fill = "#215f68"
        elif kind == "instruction":
            fill = "#3f3170"
        elif kind == "system":
            fill = "#26384f"
        lines.append(f'{node_id} [label="{label}", fillcolor="{fill}", fontcolor="#eaf1ff"];')
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


_STREAMING_CHAT_SIG_KEY = "_streaming_chat_last_sig"
_STREAMING_LIFECYCLE_SIG_KEY = "_streaming_lifecycle_last_sig"
_STREAMING_REASONING_SIG_KEY = "_streaming_reasoning_last_sig"


def _normalize_streaming_signature_value(value):
    if isinstance(value, (HumanMessage, AIMessage, ToolMessage)):
        normalized = {
            "type": value.__class__.__name__,
            "content": getattr(value, "content", None),
            "name": getattr(value, "name", None),
            "id": getattr(value, "id", None),
        }
        if isinstance(value, AIMessage):
            normalized["tool_calls"] = getattr(value, "tool_calls", None)
        if isinstance(value, ToolMessage):
            normalized["tool_call_id"] = getattr(value, "tool_call_id", None)
        return normalized
    if isinstance(value, dict):
        return {
            str(k): _normalize_streaming_signature_value(v)
            for k, v in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_normalize_streaming_signature_value(v) for v in value]
    return value


def _stable_streaming_signature(payload) -> str:
    normalized = _normalize_streaming_signature_value(payload)
    try:
        text = json.dumps(normalized, ensure_ascii=False, sort_keys=True, default=str)
    except Exception:
        text = str(normalized)
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()


def _compute_streaming_chat_signature(chat_history, *, is_running: bool, run_last_terminal_kind: str) -> str:
    return _stable_streaming_signature(
        {
            "chat_history": chat_history or [],
            "is_running": bool(is_running),
            "run_last_terminal_kind": str(run_last_terminal_kind or ""),
        }
    )


def _compute_streaming_lifecycle_signature(analysis_logs, *, is_running: bool, run_last_terminal_kind: str) -> str:
    return _stable_streaming_signature(
        {
            "analysis_logs": analysis_logs or [],
            "is_running": bool(is_running),
            "run_last_terminal_kind": str(run_last_terminal_kind or ""),
        }
    )


def _compute_streaming_reasoning_signature(analysis_logs) -> str:
    return _stable_streaming_signature({"analysis_logs": analysis_logs or []})


def _state_get(state, key: str, default=None):
    try:
        return state.get(key, default)
    except AttributeError:
        try:
            return state[key]
        except Exception:
            return default


def _state_set(state, key: str, value) -> None:
    try:
        state[key] = value
    except Exception:
        setattr(state, key, value)


def _current_streaming_live_signatures(state) -> dict:
    logs = _state_get(state, "analysis_logs", []) or []
    is_running = bool(_state_get(state, "is_running", False))
    terminal_kind = str(_state_get(state, "run_last_terminal_kind", "") or "")
    return {
        "chat": _compute_streaming_chat_signature(
            _state_get(state, "chat_history", []) or [],
            is_running=is_running,
            run_last_terminal_kind=terminal_kind,
        ),
        "lifecycle": _compute_streaming_lifecycle_signature(
            logs,
            is_running=is_running,
            run_last_terminal_kind=terminal_kind,
        ),
        "reasoning": _compute_streaming_reasoning_signature(logs),
    }


def _remember_streaming_live_signatures(state=None) -> dict:
    state = st.session_state if state is None else state
    signatures = _current_streaming_live_signatures(state)
    _state_set(state, _STREAMING_CHAT_SIG_KEY, signatures["chat"])
    _state_set(state, _STREAMING_LIFECYCLE_SIG_KEY, signatures["lifecycle"])
    _state_set(state, _STREAMING_REASONING_SIG_KEY, signatures["reasoning"])
    return signatures


def _compute_streaming_render_flags(state=None, *, events_consumed: bool) -> dict:
    state = st.session_state if state is None else state
    signatures = _current_streaming_live_signatures(state)
    flags = {
        "chat": signatures["chat"] != str(_state_get(state, _STREAMING_CHAT_SIG_KEY, "") or ""),
        "lifecycle": signatures["lifecycle"] != str(_state_get(state, _STREAMING_LIFECYCLE_SIG_KEY, "") or ""),
        "reasoning": signatures["reasoning"] != str(_state_get(state, _STREAMING_REASONING_SIG_KEY, "") or ""),
    }
    flags["any"] = bool(flags["chat"] or flags["lifecycle"] or flags["reasoning"])
    if (not events_consumed) and not flags["any"]:
        return flags
    _state_set(state, _STREAMING_CHAT_SIG_KEY, signatures["chat"])
    _state_set(state, _STREAMING_LIFECYCLE_SIG_KEY, signatures["lifecycle"])
    _state_set(state, _STREAMING_REASONING_SIG_KEY, signatures["reasoning"])
    return flags


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
            {{ selector: "node", style: {{ "label": "data(label)", "font-size": 11, "font-weight": 700, "color": "#eaf1ff", "text-outline-color": "#071021", "text-outline-width": 2, "text-wrap": "wrap", "text-max-width": 150, "shape": "round-rectangle", "background-color": "#162b52", "border-width": 1.4, "border-color": "#7da7e8" }} }},
            {{ selector: "node.human", style: {{ "background-color": "#1f4f7a" }} }},
            {{ selector: "node.ai", style: {{ "background-color": "#17635b" }} }},
            {{ selector: "node.tool", style: {{ "background-color": "#29476f" }} }},
            {{ selector: "node.tool_kb", style: {{ "background-color": "#215f68" }} }},
            {{ selector: "node.instruction", style: {{ "background-color": "#3f3170" }} }},
            {{ selector: "edge", style: {{ "curve-style": "bezier", "target-arrow-shape": "triangle", "line-color": "#7da7e8", "target-arrow-color": "#7da7e8", "width": 1.6 }} }},
            {{ selector: "edge.handoff_edge", style: {{ "line-color": "#60a5fa", "target-arrow-color": "#60a5fa", "width": 2.2 }} }},
            {{ selector: "edge.return_edge", style: {{ "line-style": "dashed", "line-color": "#2dd4bf", "target-arrow-color": "#2dd4bf" }} }}
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
    components.html(html, height=graph_height + 8, scrolling=False)


def _render_code_assistant_message(raw_content: str) -> None:
    """Render Code_Assistant message by content shape (JSON vs non-JSON)."""
    if isinstance(raw_content, (dict, list)):
        _render_static_json_block(raw_content, title="Tool JSON")
        return

    raw_text = raw_content if isinstance(raw_content, str) else str(raw_content)
    parsed, rest = _extract_json(raw_text)
    if isinstance(parsed, (dict, list)):
        if isinstance(rest, str) and rest.strip():
            _render_reasoning_text(rest, accent="#7dd3fc")
        _render_static_json_block(parsed, title="Tool JSON")
        return

    sanitized = _sanitize_paths_in_text(raw_text)
    if _looks_like_code_assistant_code(sanitized):
        _render_static_code_block(sanitized, language="python", title="Code_Assistant")
    else:
        _render_reasoning_text(sanitized, accent="#7dd3fc")


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

    for step in grouped:
        if step["kind"] == "human":
            for msg in step["messages"]:
                render_event_human(msg.content)
                st.markdown(_reasoning_divider_html(), unsafe_allow_html=True)
        elif step["kind"] == "instruction":
            for msg in step["messages"]:
                render_event_instruction(msg.content)
                st.markdown(_reasoning_divider_html(), unsafe_allow_html=True)
        elif step["kind"] == "ai":
            agent_name = step["agent"]
            agent_meta = _reasoning_agent_meta(agent_name)
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
                st.markdown(_reasoning_divider_html(), unsafe_allow_html=True)
                continue

            for msg_content in effective_messages:
                if agent_name.lower() == "data_searcher":
                    render_data_searcher_output(msg_content)
                elif agent_name.lower() == "code_assistant":
                    _render_code_assistant_message(msg_content)
                else:
                    _render_reasoning_text(msg_content, accent=agent_meta["accent"])
            st.markdown(_reasoning_divider_html(), unsafe_allow_html=True)
        elif step["kind"] == "tool":
            tool_messages = _dedupe_tool_messages([m for m in step["messages"] if isinstance(m, ToolMessage)])
            for msg in tool_messages:
                exp_title = _tr(f"工具输出 · {msg.name}", f"Tool Output · {msg.name}")
                with st.expander(exp_title, expanded=False):
                    render_label_tool(str(msg.name or "tool"))
                    tool_name_norm = str(msg.name or "").strip().lower()
                    tool_call_id = str(getattr(msg, "tool_call_id", "") or "").strip()
                    tool_meta = (step.get("tool_meta") or {}).get(tool_call_id, {})
                    if tool_name_norm == "write_todos":
                        render_write_todos_output(msg.content)
                    elif tool_name_norm in {"read_file", "read_files"}:
                        _render_read_file_tool_output(msg.content, tool_meta)
                    elif tool_name_norm == "edit_file":
                        _render_edit_file_tool_output(msg.content, tool_meta)
                    elif tool_name_norm in {
                        "uploaded_pdf_understanding_tool",
                        "uploaded_image_understanding_tool",
                        "uploaded_file_understanding_tool",
                    }:
                        render_uploaded_understanding_output(msg.content, tool_name=str(msg.name or ""))
                    else:
                        try:
                            _render_static_json_block(json.loads(msg.content), title="Tool JSON")
                        except Exception:
                            _render_reasoning_text(str(msg.content), accent="#93c5fd")
            st.markdown(_reasoning_divider_html(), unsafe_allow_html=True)
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
            st.markdown(_reasoning_divider_html(), unsafe_allow_html=True)


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
            _render_static_code_block(p, language="text", title="Recovered file")
        note = str(latest.get("recovery_note") or "").strip()
        if note:
            st.caption(_sanitize_paths_in_text(note))
        return

    if out_paths:
        st.caption(_tr("实际落盘路径：", "Actual output paths:"))
        for p in out_paths:
            _render_static_code_block(p, language="text", title="Output path")
        dst = workspace or f"user_data/{st.session_state.get('thread_id', 'debug')}/outputs"
        copy_cmd = "\n".join(
            [f"Copy-Item \"{p}\" \"{dst}\" -Force" for p in out_paths]
        )
        st.caption(_tr("建议修复命令（可复制）：", "Suggested recovery commands (copy):"))
        _render_static_code_block(copy_cmd, language="powershell", title="Recovery command")


def _render_output_preview():
    workspace = storage_manager.get_workspace(st.session_state.get("thread_id", "debug"))
    output_dir = workspace / "outputs"
    files = [f for f in output_dir.rglob("*") if f.is_file()] if output_dir.exists() else []
    files = sorted(files, key=lambda p: (p.parent.as_posix(), p.name.lower()))
    if not files:
        st.info(_tr("暂无输出文件。", "No output files available."))
        return

    selected = st.selectbox(_tr("预览输出文件", "Preview Output"), options=files, format_func=lambda p: p.name)
    st.caption(_artifact_display_ref(selected))
    suffix = selected.suffix.lower()
    if suffix == ".csv":
        try:
            df = pd.read_csv(selected)
            st.dataframe(df, width="stretch", height=360)
        except Exception as e:
            st.error(_tr(f"预览失败 {selected.name}: {e}", f"Failed to preview {selected.name}: {e}"))
    elif suffix in IMAGE_PREVIEW_SUFFIXES:
        _render_image_artifact(
            selected,
            key_prefix="output_preview",
            caption=_tr(f"预览: {selected.name}", f"Preview: {selected.name}"),
        )
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
            _render_static_code_block(code_text, language="python", title=selected.name)
        except Exception:
            try:
                code_text = selected.read_text(encoding="gbk")
                _render_static_code_block(code_text, language="python", title=selected.name)
            except Exception as e:
                st.error(_tr(f"预览失败 {selected.name}: {e}", f"Failed to preview {selected.name}: {e}"))
    elif suffix in [".json", ".geojson"]:
        try:
            payload = json.loads(selected.read_text(encoding="utf-8"))
            _render_static_json_block(payload, title=selected.name)
        except Exception:
            try:
                text = selected.read_text(encoding="utf-8", errors="replace")
                _render_static_code_block(text, language="json", title=selected.name)
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
    "Engineer",
]


def _normalize_subagent_name(raw_name: str) -> str:
    name = str(raw_name or "").strip().lower()
    if not name:
        return ""
    if "data_searcher" in name:
        return "Data_Searcher"
    if "code_assistant" in name:
        return "Code_Assistant"
    if "engineer" in name or name == "ntl-gpt":
        return "Engineer"
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
        "Data_Searcher": "Data Searcher",
        "Code_Assistant": "Code Assistant",
        "Engineer": "Engineer",
    }
    return mapping.get(str(agent_name or "").strip(), str(agent_name or "").replace("_", " "))


def _build_subagent_lifecycle_state(logs: list, is_running: bool, last_terminal_kind: str = "") -> dict:
    state = {
        name: {"status": "pending", "summary": ""}
        for name in _SUBAGENT_CARD_ORDER
    }

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
            state["Engineer"]["status"] = "running"
            state["Engineer"]["summary"] = _tr("任务已启动", "Run started")
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
        state["Engineer"]["status"] = "error"
    elif terminal == "interrupted":
        state["Engineer"]["status"] = "interrupted"

    return state


def _render_subagent_lifecycle_cards(logs: list, is_running: bool, last_terminal_kind: str = "") -> None:
    lifecycle = _build_subagent_lifecycle_state(logs, is_running=is_running, last_terminal_kind=last_terminal_kind)
    status_map = {
        "pending": ("#64748b", _tr("待命", "Pending")),
        "running": ("#2563eb", _tr("运行中", "Running")),
        "done": ("#059669", _tr("完成", "Done")),
        "error": ("#dc2626", _tr("错误", "Error")),
        "interrupted": ("#f59e0b", _tr("中断", "Interrupted")),
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
            with chat_live_placeholder.container():
                _render_chat_history_with_run_notice()
        current_thread_running_for_input = _is_current_thread_running()
        chat_input_value = get_user_input(disabled=False)
        user_question, chat_files = _extract_chat_input_text_and_files(chat_input_value)
        if current_thread_running_for_input and (user_question or chat_files):
            st.warning(
                _tr(
                    "当前回答尚未结束，本次输入未发送。请等待系统回答完成，或点击 Stop 后再发送。",
                    "The current response is still running, so this input was not sent. Wait for the answer to finish or click Stop before sending.",
                )
            )
            user_question = ""
            chat_files = []
        if chat_files:
            thread_id = str(st.session_state.get("thread_id", "debug"))
            user_id = str(st.session_state.get("user_id", "guest"))
            save_result = _save_chat_input_files_to_workspace(chat_files, thread_id, user_id)
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
        if pending_question and current_thread_running_for_input:
            st.warning(
                _tr(
                    "当前回答尚未结束，示例问题未发送。请等待系统回答完成，或点击 Stop 后再发送。",
                    "The current response is still running, so the example prompt was not sent. Wait for the answer to finish or click Stop before sending.",
                )
            )
            pending_question = None
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
                    reasoning_live_placeholder = st.empty()
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
                        st.session_state["reasoning_graph_last_sig"] = _compute_reasoning_graph_signature(
                            st.session_state.analysis_logs
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

        _remember_streaming_live_signatures()

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
                elif reason in {"global_run_limit_reached", "user_run_limit_reached"}:
                    limit = run_result.get("limit", "?")
                    active_runs = run_result.get("active_runs", "?")
                    scope_cn = "当前用户" if reason == "user_run_limit_reached" else "系统"
                    scope_en = "user" if reason == "user_run_limit_reached" else "system"
                    toast_text = _tr(
                        f"{scope_cn}并发任务数已达上限（{active_runs}/{limit}）。",
                        f"The {scope_en} active run limit is reached ({active_runs}/{limit}).",
                    )
                    if hasattr(st, "toast"):
                        st.toast(toast_text, icon="⚠️")
                    st.warning(
                        _tr(
                            f"{scope_cn}并发任务数已达上限（{active_runs}/{limit}），请等待运行中的任务完成。",
                            f"The {scope_en} active run limit is reached ({active_runs}/{limit}). Please wait for a running task to finish.",
                        )
                    )
                elif reason in {"thread_workspace_quota_reached", "user_workspace_quota_reached"}:
                    toast_text = _workspace_quota_error_message(run_result)
                    if hasattr(st, "toast"):
                        st.toast(toast_text, icon="💾")
                    st.warning(toast_text)
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
            @st.fragment(run_every=_STREAMING_MAIN_REFRESH_SECONDS)
            def _streaming_live_fragment_main():
                was_running = bool(st.session_state.get("_streaming_was_running", False))
                events_consumed = app_logic.consume_active_run_events()
                is_running_now = bool(st.session_state.get("is_running", False))
                if was_running and not is_running_now:
                    st.session_state["reasoning_graph_force_refresh_once"] = True
                st.session_state["_streaming_was_running"] = is_running_now

                render_flags = _compute_streaming_render_flags(
                    st.session_state,
                    events_consumed=events_consumed,
                )
                if (not render_flags["any"]) and not bool(st.session_state.get("ui_force_refresh_once", False)):
                    return

                if render_flags["chat"] and chat_live_placeholder is not None:
                    with chat_live_placeholder.container():
                        _render_chat_history_with_run_notice()
                if render_flags["lifecycle"] and lifecycle_placeholder is not None:
                    with lifecycle_placeholder.container():
                        _render_subagent_lifecycle_cards(
                            st.session_state.get("analysis_logs", []),
                            is_running=is_running_now,
                            last_terminal_kind=str(st.session_state.get("run_last_terminal_kind", "") or ""),
                        )
                if render_flags["reasoning"] and reasoning_live_placeholder is not None and st.session_state.get("analysis_logs"):
                    with reasoning_live_placeholder.container():
                        with st.expander(_tr("本轮推理过程", "Reasoning Flow"), expanded=True):
                            render_reasoning_content(st.session_state.analysis_logs)
                if bool(st.session_state.get("ui_force_refresh_once", False)):
                    _rerun_app()

            @st.fragment(run_every=_STREAMING_GRAPH_REFRESH_SECONDS)
            def _streaming_live_fragment_graph():
                force_refresh = bool(st.session_state.get("reasoning_graph_force_refresh_once", False))
                if reasoning_graph_live_placeholder is None:
                    if force_refresh:
                        st.session_state["reasoning_graph_force_refresh_once"] = False
                    return
                if not bool(st.session_state.get("reasoning_graph_refresh_enabled", True)):
                    return
                logs = st.session_state.get("analysis_logs") or []
                if not logs:
                    if force_refresh:
                        st.session_state["reasoning_graph_force_refresh_once"] = False
                    return

                current_sig = _compute_reasoning_graph_signature(logs)
                last_sig = str(st.session_state.get("reasoning_graph_last_sig", "") or "")
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



