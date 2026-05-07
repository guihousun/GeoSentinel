from __future__ import annotations

import base64
import math
import os
import re
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from PIL import Image
from pydantic import SecretStr

import history_store
from storage_manager import storage_manager
from langchain_core.messages import HumanMessage

try:
    import rasterio  # type: ignore
except Exception:  # noqa: BLE001
    rasterio = None

try:
    from langchain_community.document_loaders import PyPDFLoader
except Exception:  # noqa: BLE001
    PyPDFLoader = None

try:
    from langchain_text_splitters import RecursiveCharacterTextSplitter
except Exception:  # noqa: BLE001
    RecursiveCharacterTextSplitter = None

try:
    from langchain_openai import ChatOpenAI
except Exception:  # noqa: BLE001
    ChatOpenAI = None


SUPPORTED_EXTS = {".pdf", ".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff"}
IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".bmp"}
TIFF_EXTS = {".tif", ".tiff"}
PDF_EXTS = {".pdf"}


def is_supported_file(path: Path) -> bool:
    return path.suffix.lower() in SUPPORTED_EXTS


def _split_text(text: str, chunk_size: int = 900, chunk_overlap: int = 150) -> List[str]:
    body = (text or "").strip()
    if not body:
        return []
    if RecursiveCharacterTextSplitter is not None:
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=max(200, int(chunk_size)),
            chunk_overlap=max(0, int(chunk_overlap)),
        )
        chunks = [c.strip() for c in splitter.split_text(body) if str(c).strip()]
        if chunks:
            return chunks
    chunks: List[str] = []
    start = 0
    n = len(body)
    step = max(100, chunk_size - chunk_overlap)
    while start < n:
        end = min(n, start + chunk_size)
        chunks.append(body[start:end].strip())
        start += step
    return [c for c in chunks if c]


def _safe_float(v: Any) -> Optional[float]:
    try:
        return float(v)
    except Exception:
        return None


def _image_mime(path: Path) -> str:
    ext = path.suffix.lower()
    if ext in {".jpg", ".jpeg"}:
        return "image/jpeg"
    if ext == ".png":
        return "image/png"
    if ext == ".webp":
        return "image/webp"
    if ext == ".bmp":
        return "image/bmp"
    return "application/octet-stream"


def _encode_image_data_url(path: Path) -> str:
    data = path.read_bytes()
    b64 = base64.b64encode(data).decode("ascii")
    return f"data:{_image_mime(path)};base64,{b64}"


def _content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: List[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict) and item.get("text"):
                parts.append(str(item.get("text")))
        return "\n".join([p for p in parts if str(p).strip()])
    return str(content or "")


def _image_vlm_summary(
    path: Path,
    model_name: str = "deepseek-v4-pro",
    api_key: Optional[str] = None,
    timeout_s: int = 90,
) -> Tuple[Optional[str], Dict[str, Any], Optional[str]]:
    """
    End-to-end VLM understanding for image files.
    Returns: (summary_text, meta, warning)
    """
    if ChatOpenAI is None:
        return None, {"model": model_name}, "langchain_openai is unavailable for VLM."

    key = (api_key or os.getenv("DEEPSEEK_API_KEY") or "").strip()
    if not key:
        return None, {"model": model_name}, "DEEPSEEK_API_KEY is missing for VLM image understanding."

    data_url = _encode_image_data_url(path)
    prompt = (
        "You are a geospatial-vision analyst. Analyze this image and return concise factual understanding. "
        "Include: observed objects/patterns, likely geospatial relevance, potential quality risks, "
        "and what downstream GIS/NTL conflict-analysis tasks this image can support."
    )
    try:
        llm = ChatOpenAI(
            api_key=SecretStr(key),
            base_url="https://api.deepseek.com",
            model=model_name,
            timeout=timeout_s,
        )

        msg = HumanMessage(
            content=[
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": data_url}},
            ]
        )
        resp = llm.invoke([msg])
        text = _content_to_text(getattr(resp, "content", ""))
        text = str(text or "").strip()
        if not text:
            return None, {"model": model_name}, f"VLM returned empty content for {path.name}."
        meta = {
            "model": model_name,
            "understanding_mode": "vlm_e2e",
            "mime": _image_mime(path),
        }
        return text, meta, None
    except Exception as exc:  # noqa: BLE001
        return None, {"model": model_name}, f"VLM parse failed for {path.name}: {exc}"


def _sample_raster_band(arr: np.ndarray, target_pixels: int = 300000) -> np.ndarray:
    if arr.size <= target_pixels:
        return arr
    stride = max(1, int(math.sqrt(arr.size / max(1, target_pixels))))
    return arr[::stride, ::stride]


def _tif_summary(path: Path) -> Tuple[str, Dict[str, Any]]:
    if rasterio is None:
        return (
            f"TIFF file `{path.name}` is present, but rasterio is unavailable for metadata extraction.",
            {"warning": "rasterio_not_available"},
        )
    with rasterio.open(path) as ds:
        meta: Dict[str, Any] = {
            "width": ds.width,
            "height": ds.height,
            "bands": ds.count,
            "crs": str(ds.crs) if ds.crs else None,
            "dtype": ds.dtypes[0] if ds.count else None,
            "nodata": _safe_float(ds.nodata),
            "bounds": [float(ds.bounds.left), float(ds.bounds.bottom), float(ds.bounds.right), float(ds.bounds.top)],
            "resolution": [abs(float(ds.transform.a)), abs(float(ds.transform.e))],
        }
        band = ds.read(1, masked=True)
        band = _sample_raster_band(np.ma.filled(band, np.nan))
        valid = band[np.isfinite(band)]
        if valid.size:
            stats = {
                "min": float(np.nanmin(valid)),
                "max": float(np.nanmax(valid)),
                "mean": float(np.nanmean(valid)),
                "std": float(np.nanstd(valid)),
            }
        else:
            stats = {"min": None, "max": None, "mean": None, "std": None}
        meta["band1_stats"] = stats
        summary = (
            f"TIFF file `{path.name}` has size {ds.width}x{ds.height}, bands={ds.count}, dtype={meta['dtype']}, "
            f"CRS={meta['crs'] or 'unknown'}, resolution={meta['resolution']}. "
            f"Band-1 statistics: min={stats['min']}, max={stats['max']}, mean={stats['mean']}, std={stats['std']}."
        )
        return summary, meta


def _pdf_chunks(path: Path, max_pages: int = 120) -> Tuple[List[Dict[str, Any]], List[str]]:
    warnings: List[str] = []
    if PyPDFLoader is None:
        return [], ["PyPDFLoader is unavailable; PDF parsing skipped."]
    try:
        pages = PyPDFLoader(str(path)).load()
    except Exception as exc:  # noqa: BLE001
        return [], [f"Failed to parse PDF {path.name}: {exc}"]
    page_docs = pages[: max(1, int(max_pages))]
    items: List[Dict[str, Any]] = []
    for page_doc in page_docs:
        page_idx = int(page_doc.metadata.get("page", 0)) + 1
        text = str(page_doc.page_content or "").strip()
        if not text:
            continue
        chunks = _split_text(text, chunk_size=900, chunk_overlap=150)
        for chunk_idx, chunk in enumerate(chunks):
            items.append(
                {
                    "file_type": "pdf",
                    "source_file": path.name,
                    "page": page_idx,
                    "chunk_idx": chunk_idx,
                    "text": chunk,
                    "tags": ["pdf", "user_upload", "context_injection"],
                }
            )
    if not items:
        warnings.append(f"No extractable text from PDF {path.name}.")
    return items, warnings


def _resolve_existing_input_file(filename: str, thread_id: str) -> Optional[Path]:
    return _resolve_existing_workspace_file(filename, thread_id=thread_id, workspace_lookup="inputs")


def _resolve_existing_workspace_file(
    filename: str,
    thread_id: str,
    workspace_lookup: str = "auto",
) -> Optional[Path]:
    raw = str(filename or "").strip().replace("\\", "/")
    if not raw:
        return None

    explicit: Optional[str] = None
    relative = raw
    lowered = raw.lower()
    if lowered.startswith("inputs/"):
        explicit = "inputs"
        relative = raw.split("/", 1)[1]
    elif lowered.startswith("outputs/"):
        explicit = "outputs"
        relative = raw.split("/", 1)[1]

    # Logical filename protocol: keep basename only to prevent traversal/nested absolute usage.
    safe_name = os.path.basename(relative.strip())
    if not safe_name:
        return None

    if explicit == "inputs":
        lookup_order = ("inputs", "outputs")
    elif explicit == "outputs":
        lookup_order = ("outputs", "inputs")
    elif str(workspace_lookup or "auto").strip().lower() == "outputs":
        lookup_order = ("outputs", "inputs")
    else:
        # Backward-compatible default: prefer inputs.
        lookup_order = ("inputs", "outputs")

    for loc in lookup_order:
        if loc == "inputs":
            abs_path = storage_manager.resolve_input_path(safe_name, thread_id=thread_id)
        else:
            abs_path = storage_manager.resolve_output_path(safe_name, thread_id=thread_id)
        p = Path(abs_path)
        if p.exists() and p.is_file():
            return p
    return None


def build_context_items_for_files(
    thread_id: str,
    file_names: List[str],
    max_pages: int = 120,
    vlm_model_name: str = "deepseek-v4-pro",
    vlm_timeout_s: int = 90,
    workspace_lookup: str = "auto",
) -> Dict[str, Any]:
    items: List[Dict[str, Any]] = []
    warnings: List[str] = []
    unsupported_files: List[str] = []
    missing_files: List[str] = []
    processed_files: List[str] = []

    now = int(time.time())
    for raw_name in file_names:
        name = os.path.basename(str(raw_name or "").strip())
        if not name:
            continue
        p = _resolve_existing_workspace_file(name, thread_id=thread_id, workspace_lookup=workspace_lookup)
        if p is None:
            missing_files.append(name)
            continue
        ext = p.suffix.lower()
        if ext not in SUPPORTED_EXTS:
            unsupported_files.append(name)
            continue
        processed_files.append(name)
        signature = history_store.file_signature(p)
        if ext in PDF_EXTS:
            pdf_items, pdf_warnings = _pdf_chunks(p, max_pages=max_pages)
            warnings.extend(pdf_warnings)
            for row in pdf_items:
                row.update(
                    {
                        "context_id": uuid.uuid4().hex,
                        "signature": signature,
                        "created_at": now,
                    }
                )
                items.append(row)
            continue
        if ext in IMAGE_EXTS:
            text, meta, warning = _image_vlm_summary(
                p,
                model_name=vlm_model_name,
                timeout_s=vlm_timeout_s,
            )
            if warning:
                warnings.append(warning)
            if text:
                items.append(
                    {
                        "context_id": uuid.uuid4().hex,
                        "file_type": "image",
                        "source_file": p.name,
                        "chunk_idx": 0,
                        "page": None,
                        "text": text,
                        "meta": meta,
                        "tags": ["image", "vlm", "user_upload", "context_injection"],
                        "signature": signature,
                        "created_at": now,
                    }
                )
            continue
        if ext in TIFF_EXTS:
            try:
                text, meta = _tif_summary(p)
                items.append(
                    {
                        "context_id": uuid.uuid4().hex,
                        "file_type": "tif",
                        "source_file": p.name,
                        "chunk_idx": 0,
                        "page": None,
                        "text": text,
                        "meta": meta,
                        "tags": ["tif", "raster", "user_upload", "context_injection"],
                        "signature": signature,
                        "created_at": now,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"TIFF parse failed for {p.name}: {exc}")
            continue
        unsupported_files.append(name)

    status = "success"
    if not items and (unsupported_files or missing_files):
        status = "unsupported" if unsupported_files and not missing_files else "partial"
    elif not items:
        status = "empty"

    return {
        "status": status,
        "thread_id": thread_id,
        "processed_files": processed_files,
        "unsupported_files": unsupported_files,
        "missing_files": missing_files,
        "items": items,
        "warnings": warnings,
    }
