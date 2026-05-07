from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional

from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field

import file_context_service
import history_store
from storage_manager import current_thread_id, storage_manager


_FILE_MENTION_PATTERN = re.compile(
    r"([a-z0-9_\-\u4e00-\u9fff]+\.(?:pdf|png|jpg|jpeg|webp|bmp|tif|tiff))",
    re.IGNORECASE,
)
_IMAGE_UNDERSTANDING_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".bmp"}


class UploadedFileUnderstandingInput(BaseModel):
    query: str = Field(..., description="User question about uploaded PDF/images.")
    file_names: Optional[str] = Field(
        default=None,
        description="Optional comma-separated filenames in inputs/ or outputs/. Example: a.pdf,b.png",
    )
    workspace_lookup: str = Field(
        default="auto",
        description="File lookup scope: auto|inputs|outputs. auto prefers inputs then outputs.",
    )
    top_n: int = Field(default=4, ge=1, le=8, description="Top-N snippets returned.")


class UploadedPdfUnderstandingInput(BaseModel):
    query: str = Field(..., description="Question about uploaded PDF files.")
    file_names: Optional[str] = Field(
        default=None,
        description="Optional comma-separated PDF filenames in inputs/ or outputs/. Example: a.pdf,b.pdf",
    )
    workspace_lookup: str = Field(
        default="auto",
        description="File lookup scope: auto|inputs|outputs. auto prefers inputs then outputs.",
    )
    top_n: int = Field(default=4, ge=1, le=8, description="Top-N snippets returned.")


class UploadedImageUnderstandingInput(BaseModel):
    query: str = Field(..., description="Question about uploaded image files.")
    file_names: Optional[str] = Field(
        default=None,
        description="Optional comma-separated image filenames in inputs/ or outputs/. Example: a.png,b.jpg",
    )
    workspace_lookup: str = Field(
        default="auto",
        description="File lookup scope: auto|inputs|outputs. auto prefers inputs then outputs.",
    )
    top_n: int = Field(default=4, ge=1, le=8, description="Top-N snippets returned.")


def _parse_file_list(value: Optional[str]) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    out = []
    for part in text.split(","):
        name = os.path.basename(part.strip())
        if name:
            out.append(name)
    return out


def _extract_mentions(query: str) -> list[str]:
    seen = set()
    out = []
    for m in _FILE_MENTION_PATTERN.findall(str(query or "").lower()):
        key = str(m).strip().lower()
        if key and key not in seen:
            seen.add(key)
            out.append(key)
    return out


def _normalize_for_filename_match(text: str) -> str:
    t = str(text or "").strip().lower()
    # Remove all whitespace to tolerate variants like "name ) .pdf".
    return re.sub(r"\s+", "", t)


def _pick_files_by_normalized_query(query: str, candidate_names: list[str]) -> list[str]:
    q_norm = _normalize_for_filename_match(query)
    if not q_norm:
        return []
    picked: list[str] = []
    for name in candidate_names:
        n_norm = _normalize_for_filename_match(name)
        if n_norm and n_norm in q_norm:
            picked.append(name)
    return picked


def _infer_requested_image_count(query: str, default_count: int = 1, max_count: int = 4) -> int:
    q = str(query or "").strip().lower()
    if not q:
        return default_count

    m = re.search(r"(\d+)\s*(?:张|幅|个)?\s*(?:图|图片|照片|image|images|photo|photos|picture|pictures)", q, flags=re.IGNORECASE)
    if m:
        n = int(m.group(1))
        return max(1, min(max_count, n))

    zh_map = {
        "一": 1,
        "二": 2,
        "两": 2,
        "俩": 2,
        "三": 3,
        "四": 4,
    }
    m2 = re.search(r"(一|二|两|俩|三|四)\s*(?:张|幅|个)?\s*(?:图|图片|照片)", q)
    if m2:
        return max(1, min(max_count, zh_map.get(m2.group(1), default_count)))

    if any(tok in q for tok in ("all images", "all photos", "multiple images", "multiple photos", "所有图片", "全部图片", "多张图片", "多图")):
        return max(2, min(max_count, 3))

    if any(tok in q for tok in ("these images", "these photos", "these pictures", "这些图片")):
        return max(2, min(max_count, 2))
    return default_count


def _pick_default_files(
    thread_id: str,
    query: str,
    allowed_exts: Optional[set[str]] = None,
    desired_count: int = 1,
    workspace_lookup: str = "auto",
) -> list[str]:
    workspace = storage_manager.get_workspace(thread_id)
    lookup = str(workspace_lookup or "auto").strip().lower()
    if lookup == "outputs":
        candidate_dirs = [workspace / "outputs", workspace / "inputs"]
    elif lookup == "inputs":
        candidate_dirs = [workspace / "inputs", workspace / "outputs"]
    else:
        candidate_dirs = [workspace / "inputs", workspace / "outputs"]

    ranked_files = []
    for pref_idx, d in enumerate(candidate_dirs):
        if not d.exists():
            continue
        for p in d.glob("*.*"):
            if p.suffix.lower() in file_context_service.SUPPORTED_EXTS:
                ranked_files.append((pref_idx, -p.stat().st_mtime, p))

    # Keep deterministic order: preferred directory first, then latest mtime.
    ranked_files.sort(key=lambda x: (x[0], x[1], x[2].name.lower()))
    all_files = []
    seen_names = set()
    for _, __, p in ranked_files:
        k = p.name.lower()
        if k in seen_names:
            continue
        seen_names.add(k)
        all_files.append(p)
    if allowed_exts:
        all_files = [p for p in all_files if p.suffix.lower() in allowed_exts]
    if not all_files:
        return []
    mentions = set(_extract_mentions(query))
    if mentions:
        picked = [p.name for p in all_files if p.name.lower() in mentions]
        if picked:
            return picked[: max(1, int(desired_count))]
    # Robust fallback for filenames containing spaces/brackets/CJK punctuation.
    normalized_hits = _pick_files_by_normalized_query(query, [p.name for p in all_files])
    if normalized_hits:
        return normalized_hits[: max(1, int(desired_count))]
    # Prefer PDF when question is document-centric; otherwise latest supported file.
    q = str(query or "").lower()
    pdf_like = any(tok in q for tok in ("pdf", "paper", "document", "文档", "论文", "报告"))
    if pdf_like:
        pdfs = [p.name for p in all_files if p.suffix.lower() == ".pdf"]
        if pdfs:
            return pdfs[:1]
    return [p.name for p in all_files[: max(1, int(desired_count))]]


def _run_understanding(
    query: str,
    file_names: Optional[str],
    top_n: int,
    allowed_exts: Optional[set[str]],
    workspace_lookup: str = "auto",
) -> dict:
    """
    Understand uploaded PDF/images in current thread workspace and return relevant snippets.
    Also upserts extracted context into thread-level context store for subsequent QA turns.
    """
    thread_id = str(current_thread_id.get() or "debug")
    explicit_files = _parse_file_list(file_names)
    if allowed_exts:
        explicit_files = [x for x in explicit_files if Path(x).suffix.lower() in allowed_exts]
    desired_count = 1
    if allowed_exts and set(allowed_exts) == _IMAGE_UNDERSTANDING_EXTS and not explicit_files:
        desired_count = _infer_requested_image_count(query, default_count=1, max_count=4)
    targets = explicit_files or _pick_default_files(
        thread_id,
        query,
        allowed_exts=allowed_exts,
        desired_count=desired_count,
        workspace_lookup=workspace_lookup,
    )
    if not targets:
        return {
            "status": "no_input_files",
            "thread_id": thread_id,
            "message": "No supported files found in inputs/ or outputs/ for current thread.",
            "targets": [],
            "snippets": [],
        }

    result = file_context_service.build_context_items_for_files(
        thread_id=thread_id,
        file_names=targets,
        max_pages=120,
        vlm_model_name="deepseek-v4-pro",
        vlm_timeout_s=90,
        workspace_lookup=workspace_lookup,
    )
    items = result.get("items", []) or []
    merge_stats = {"inserted": 0, "updated": 0, "total": 0}
    if items:
        merge_stats = history_store.upsert_injected_context_items(thread_id, items)

    snippets = history_store.retrieve_relevant_context(
        thread_id=thread_id,
        query=query,
        top_n=max(1, min(int(top_n or 4), 8)),
        max_chars=6000,
    )

    compact = []
    for s in snippets:
        compact.append(
            {
                "source_file": s.get("source_file"),
                "file_type": s.get("file_type"),
                "page": s.get("page"),
                "score": s.get("score"),
                "text": str(s.get("text", ""))[:1200],
            }
        )
    total_injected = int((merge_stats or {}).get("total", 0) or 0)
    inserted = int((merge_stats or {}).get("inserted", 0) or 0)
    updated = int((merge_stats or {}).get("updated", 0) or 0)
    if compact:
        status = "success"
    elif total_injected > 0 or inserted > 0 or updated > 0:
        # Context has been parsed and injected, but current query has no direct hit.
        status = "context_injected_no_match"
    else:
        status = "no_relevant_snippet"

    return {
        "status": status,
        "thread_id": thread_id,
        "targets": targets,
        "merge_stats": merge_stats,
        "warnings": result.get("warnings", []),
        "snippets": compact,
    }


def uploaded_file_understanding_tool_fn(
    query: str,
    file_names: Optional[str] = None,
    workspace_lookup: str = "auto",
    top_n: int = 4,
) -> dict:
    return _run_understanding(
        query=query,
        file_names=file_names,
        top_n=top_n,
        allowed_exts=None,
        workspace_lookup=workspace_lookup,
    )


def uploaded_pdf_understanding_tool_fn(
    query: str,
    file_names: Optional[str] = None,
    workspace_lookup: str = "auto",
    top_n: int = 4,
) -> dict:
    return _run_understanding(
        query=query,
        file_names=file_names,
        top_n=top_n,
        allowed_exts={".pdf"},
        workspace_lookup=workspace_lookup,
    )


def uploaded_image_understanding_tool_fn(
    query: str,
    file_names: Optional[str] = None,
    workspace_lookup: str = "auto",
    top_n: int = 4,
) -> dict:
    return _run_understanding(
        query=query,
        file_names=file_names,
        top_n=top_n,
        allowed_exts=_IMAGE_UNDERSTANDING_EXTS,
        workspace_lookup=workspace_lookup,
    )


uploaded_file_understanding_tool = StructuredTool.from_function(
    func=uploaded_file_understanding_tool_fn,
    name="uploaded_file_understanding_tool",
    description=(
        "Read and understand uploaded PDF/images in current workspace (inputs/outputs), "
        "inject context, and return relevant snippets for current query."
    ),
    args_schema=UploadedFileUnderstandingInput,
)


uploaded_pdf_understanding_tool = StructuredTool.from_function(
    func=uploaded_pdf_understanding_tool_fn,
    name="uploaded_pdf_understanding_tool",
    description=(
        "Understand uploaded PDF files in current workspace (inputs/outputs), inject context, "
        "and return relevant snippets for current query. "
        "Use when user asks to summarize/read/extract information from uploaded PDF(s)."
    ),
    args_schema=UploadedPdfUnderstandingInput,
)


uploaded_image_understanding_tool = StructuredTool.from_function(
    func=uploaded_image_understanding_tool_fn,
    name="uploaded_image_understanding_tool",
    description=(
        "Understand uploaded image files in current workspace (inputs/outputs) with VLM, inject context, "
        "and return relevant snippets for current query. "
        "Use when user asks to describe/explain an uploaded image/photo/screenshot."
    ),
    args_schema=UploadedImageUnderstandingInput,
)
