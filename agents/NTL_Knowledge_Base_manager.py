"""NTL Knowledge Base (Chroma) ingestion and management utilities."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from langchain_chroma import Chroma
from langchain_community.document_loaders import (
    PyPDFLoader,
    PythonLoader,
    TextLoader,
    WebBaseLoader,
)
from langchain_core.documents import Document
from langchain_openai import OpenAIEmbeddings
from langchain_text_splitters import RecursiveCharacterTextSplitter

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from utils.ntl_kb_aliases import flatten_records, normalize_tool_name, normalize_workflow_task


load_dotenv()


CODE_PROFILE = "code"
SOLUTION_PROFILE = "solution"
LITERATURE_PROFILE = "literature"

DEFAULT_SOLUTION_PERSIST_DIR = r".\RAG\Solution_RAG"
DEFAULT_SOLUTION_COLLECTION_NAME = "Solution_RAG"
DEFAULT_SOLUTION_REPORT_PATH = r".\RAG\Solution_RAG\rebuild_report.json"

DEFAULT_LITERATURE_DIR = r".\RAG\literature base"
DEFAULT_LITERATURE_PERSIST_DIR = r".\RAG\Literature_RAG"
DEFAULT_LITERATURE_COLLECTION_NAME = "Literature_RAG"
DEFAULT_LITERATURE_REPORT_PATH = r".\RAG\Literature_RAG\rebuild_report.json"

CODE_GUIDE_DATASET_JSON = Path("GEE_dataset") / "dataset_documents.json"
CODE_GUIDE_GEE_DIR = Path("Geospatial_Code_GEE")
CODE_GUIDE_GIS_DIRS = [
    Path("Geospatial_Code_geopanda_rasterio"),
    Path("geopandas"),
    Path("rasterio"),
]
CODE_GIS_SUFFIX = ".txt"

CODE_EXCLUDE_DIRECTORIES = {"earthengine-api-master", "pytest", "txttest"}

TOOL_TEMPLATE_TARGETS: dict[str, list[str]] = {
    "GEE_download.py": ["ntl_download_tool"],
    "VNP46A2_angular_correction.py": ["VNP46A2_angular_correction_tool"],
    "NTL_raster_stats.py": ["NTL_raster_statistics"],
    "NTL_trend_detection_tool.py": ["analyze_ntl_trend_masked_logic"],
    "NTL_anomaly_detection_tool.py": ["detect_ntl_anomaly"],
    "NPP_viirs_index_tool.py": ["compute_vnci_index"],
}


def _ensure_openai_api_key() -> None:
    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError(
            "OPENAI_API_KEY is required to build Chroma embeddings. "
            "Set it in environment variables or .env."
        )


def _safe_read_text_loader(file_path: str) -> list[Document]:
    try:
        return TextLoader(file_path, encoding="utf-8").load()
    except Exception:
        return TextLoader(file_path, encoding="gbk").load()


def _read_text_with_fallback(path: Path) -> str:
    for encoding in ("utf-8", "gbk", "latin-1"):
        try:
            return path.read_text(encoding=encoding)
        except Exception:
            continue
    raise UnicodeDecodeError("unknown", b"", 0, 1, f"Failed to decode {path}")


def _normalize_for_hash(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _doc_hash(page_content: str, metadata: dict[str, Any]) -> str:
    key = json.dumps(
        {
            "content": _normalize_for_hash(page_content),
            "doc_type": str(metadata.get("doc_type", "")),
            "symbol": str(metadata.get("symbol", "")),
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _python_file_is_test(file_path: Path) -> bool:
    file_name = file_path.name.lower()
    if file_name.endswith("_test.py") or file_name.startswith("test_"):
        return True
    parent_names = {p.name.lower() for p in file_path.parents}
    return "tests" in parent_names


def _topic_tags_from_text(text: str, base_tags: set[str] | None = None) -> str:
    tags = set(base_tags or set())
    low = text.lower()
    rules = {
        "ntl": [r"\bntl\b", r"nighttime light", r"night light"],
        "viirs": [r"\bviirs\b", r"npp-viirs"],
        "vnp46a2": [r"\bvnp46a2\b"],
        "vnci": [r"\bvnci\b"],
        "dmsp": [r"\bdmsp\b"],
        "sdgsat": [r"\bsdgsat\b"],
        "geopandas": [r"\bgeopandas\b"],
        "rasterio": [r"\brasterio\b"],
        "gee": [r"earth engine", r"\bee\."],
    }
    for tag, patterns in rules.items():
        if any(re.search(pattern, low) for pattern in patterns):
            tags.add(tag)
    if not tags:
        tags.add("general_code")
    return ",".join(sorted(tags))


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT_DIR))
    except Exception:
        return str(path)


def _extract_year(text: str) -> str:
    match = re.search(r"(19|20)\d{2}", text or "")
    return match.group(0) if match else ""


def _extract_literature_title(file_path: Path) -> str:
    stem = re.sub(r"\s+", " ", file_path.stem.replace("_", " ")).strip()
    parts = [part.strip() for part in re.split(r"\s+-\s+", stem) if part.strip()]
    if len(parts) >= 3 and _extract_year(parts[1]):
        title = " - ".join(parts[2:])
        return title.strip() or stem
    if len(parts) >= 2 and _extract_year(parts[0]):
        title = " - ".join(parts[1:])
        return title.strip() or stem
    return stem


def _infer_language(text: str) -> str:
    if re.search(r"[\u4e00-\u9fff]", text or ""):
        return "zh"
    if re.search(r"[A-Za-z]", text or ""):
        return "en"
    return "unknown"


def _infer_literature_quality_tier(title: str, text: str) -> str:
    low = f"{title}\n{text}".lower()
    if "review" in low or "综述" in low:
        return "high"
    if "conference" in low or "proceedings" in low:
        return "medium"
    return "high"


def _infer_literature_section_type(text: str) -> str:
    low = (text or "").lower()
    if any(key in low for key in ("method", "materials and methods", "methodology", "方法")):
        return "methods"
    if any(key in low for key in ("equation", "formula", "model", "公式", "模型")):
        return "equations"
    if any(key in low for key in ("dataset", "data source", "study area", "数据集", "研究区")):
        return "data"
    if any(key in low for key in ("result", "results", "实验结果", "结果")):
        return "results"
    if any(key in low for key in ("discussion", "conclusion", "讨论", "结论")):
        return "discussion"
    return "general"


def _is_low_signal_literature_chunk(text: str) -> bool:
    low = (text or "").lower().strip()
    if not low:
        return True
    noise_markers = (
        "references",
        "bibliography",
        "acknowledg",
        "funding",
        "conflict of interest",
        "参考文献",
        "致谢",
    )
    return any(marker in low for marker in noise_markers)


def _strip_references_tail(text: str) -> tuple[str, bool]:
    """Return (main_text, references_started). If references heading appears, trim tail."""
    raw = str(text or "")
    if not raw.strip():
        return "", False

    # Match common standalone section headings to avoid false positives in body sentences.
    heading_pattern = re.compile(
        r"(?im)^\s*(references|bibliography|参考文献|致谢|acknowledg(?:e)?ments?)\s*[:：]?\s*$"
    )
    match = heading_pattern.search(raw)
    if not match:
        return raw.strip(), False

    main = raw[: match.start()].strip()
    return main, True


def _read_literature_text_file(path: Path) -> str:
    for enc in ("utf-8", "utf-8-sig", "gb18030"):
        try:
            return path.read_text(encoding=enc)
        except Exception:
            continue
    return path.read_text(errors="ignore")


class RAGDatabase:
    def __init__(
        self,
        persistent_directory: str,
        collection_name: str = "knowledge-chroma",
        embedding_model: str = "text-embedding-3-small",
    ):
        _ensure_openai_api_key()
        self.persistent_directory = persistent_directory
        self.collection_name = collection_name
        self.embedding_model = embedding_model
        os.makedirs(persistent_directory, exist_ok=True)

        self.text_splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
            chunk_size=2048,
            chunk_overlap=512,
        )
        self.embeddings = OpenAIEmbeddings(model=self.embedding_model)
        self.vector_store = Chroma(
            collection_name=self.collection_name,
            persist_directory=self.persistent_directory,
            embedding_function=self.embeddings,
        )

    @staticmethod
    def _extract_tool_names(steps: list[dict[str, Any]]) -> list[str]:
        out: list[str] = []
        for step in steps:
            if not isinstance(step, dict):
                continue
            if step.get("type") != "builtin_tool":
                continue
            name = normalize_tool_name(str(step.get("name", "")).strip())
            if name:
                out.append(name)
        seen = set()
        uniq: list[str] = []
        for name in out:
            if name not in seen:
                seen.add(name)
                uniq.append(name)
        return uniq

    @staticmethod
    def _init_report(profile: str, reset: bool) -> dict[str, Any]:
        return {
            "collection_name": "",
            "persistent_directory": "",
            "embedding_model": "",
            "profile": profile,
            "reset": reset,
            "deleted_before_rebuild": 0,
            "records_seen": 0,
            "records_ingested": 0,
            "records_skipped": 0,
            "dedupe_removed_count": 0,
            "errors": [],
            "non_json_docs_loaded": 0,
            "chunks_written": 0,
            "final_collection_count": 0,
            "doc_type_counts": {},
            "source_bucket_counts": {},
            "language_counts": {},
        }

    @staticmethod
    def _bump_count(counter: dict[str, int], key: str) -> None:
        if not key:
            return
        counter[key] = counter.get(key, 0) + 1

    def _update_distribution_counts(self, report: dict[str, Any], metadata: dict[str, Any]) -> None:
        self._bump_count(report["doc_type_counts"], str(metadata.get("doc_type", "")))
        self._bump_count(report["source_bucket_counts"], str(metadata.get("source_bucket", "")))
        self._bump_count(report["language_counts"], str(metadata.get("language", "")))

    def _register_document(
        self,
        docs: list[Document],
        report: dict[str, Any],
        dedupe_hashes: set[str],
        page_content: str,
        metadata: dict[str, Any],
        *,
        count_seen: bool = True,
    ) -> None:
        if count_seen:
            report["records_seen"] += 1

        hash_content = str(metadata.pop("_dedupe_content", page_content))
        hash_symbol = str(metadata.pop("_dedupe_symbol", metadata.get("symbol", "")))
        hash_metadata = dict(metadata)
        hash_metadata["symbol"] = hash_symbol
        digest = _doc_hash(hash_content, hash_metadata)
        if digest in dedupe_hashes:
            report["records_skipped"] += 1
            report["dedupe_removed_count"] += 1
            return
        dedupe_hashes.add(digest)

        docs.append(Document(page_content=page_content, metadata=metadata))
        report["records_ingested"] += 1
        self._update_distribution_counts(report, metadata)

    def _build_json_documents(
        self,
        json_path: Path,
        report: dict[str, Any],
        dedupe_hashes: set[str],
    ) -> list[Document]:
        docs: list[Document] = []
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception as exc:
            report["errors"].append({"file": str(json_path), "error": f"load_failed: {exc}"})
            return docs

        records = flatten_records(data)
        name_lower = json_path.name.lower()

        for idx, record in enumerate(records):
            if not isinstance(record, dict):
                report["records_seen"] += 1
                report["records_skipped"] += 1
                report["errors"].append(
                    {"file": str(json_path), "index": idx, "error": "record_not_dict"}
                )
                continue

            if "workflow" in name_lower:
                normalized = normalize_workflow_task(record)
                missing = [k for k in ("task_id", "task_name", "steps") if not normalized.get(k)]
                if missing:
                    report["records_seen"] += 1
                    report["records_skipped"] += 1
                    report["errors"].append(
                        {
                            "file": str(json_path),
                            "index": idx,
                            "task_id": normalized.get("task_id"),
                            "error": f"workflow_schema_missing:{','.join(missing)}",
                        }
                    )
                    continue

                metadata = {
                    "source": str(json_path),
                    "source_file": str(json_path),
                    "doc_type": "workflow_task",
                    "task_id": str(normalized.get("task_id", "")),
                    "category": str(normalized.get("category", "")),
                    "tool_names": json.dumps(
                        self._extract_tool_names(normalized.get("steps", [])),
                        ensure_ascii=False,
                    ),
                    "language": "json",
                    "source_bucket": "guidence_json",
                    "topic_tags": "",
                    "symbol": "",
                    "quality_tier": "medium",
                }
                page_content = json.dumps(normalized, ensure_ascii=False, indent=2)
            elif "tools" in name_lower:
                normalized = dict(record)
                normalized["tool_name"] = normalize_tool_name(str(normalized.get("tool_name", "")).strip())
                tool_name = str(normalized.get("tool_name", ""))
                metadata = {
                    "source": str(json_path),
                    "source_file": str(json_path),
                    "doc_type": "tool_spec",
                    "task_id": "",
                    "category": str(normalized.get("category", "")),
                    "tool_names": json.dumps([tool_name] if tool_name else [], ensure_ascii=False),
                    "language": "json",
                    "source_bucket": "guidence_json",
                    "topic_tags": "",
                    "symbol": tool_name,
                    "quality_tier": "medium",
                }
                page_content = json.dumps(normalized, ensure_ascii=False, indent=2)
            else:
                metadata = {
                    "source": str(json_path),
                    "source_file": str(json_path),
                    "doc_type": "json_record",
                    "task_id": str(record.get("task_id", "")),
                    "category": str(record.get("category", "")),
                    "tool_names": "[]",
                    "language": "json",
                    "source_bucket": "generic_json",
                    "topic_tags": "",
                    "symbol": "",
                    "quality_tier": "medium",
                }
                page_content = json.dumps(record, ensure_ascii=False, indent=2)

            self._register_document(
                docs,
                report,
                dedupe_hashes,
                page_content,
                metadata,
                count_seen=True,
            )

        return docs
    def _extract_python_symbols(self, file_path: Path) -> list[dict[str, str]]:
        source = _read_text_with_fallback(file_path)
        tree = ast.parse(source)
        lines = source.splitlines()
        symbols: list[dict[str, str]] = []

        for node in tree.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue

            start = max(node.lineno - 1, 0)
            end = getattr(node, "end_lineno", node.lineno)
            code = "\n".join(lines[start:end]).strip()
            if not code:
                continue

            kind = "class" if isinstance(node, ast.ClassDef) else "function"
            signature = ""
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                try:
                    signature = f"{node.name}({ast.unparse(node.args)})"
                except Exception:
                    signature = f"{node.name}(...)"
            docstring = ast.get_docstring(node) or ""

            symbols.append(
                {
                    "name": node.name,
                    "kind": kind,
                    "signature": signature,
                    "docstring": docstring,
                    "code": code,
                }
            )

        return symbols

    def _build_dataset_card_docs(
        self,
        code_guide_dir: Path,
        docs: list[Document],
        report: dict[str, Any],
        dedupe_hashes: set[str],
    ) -> None:
        dataset_json_path = code_guide_dir / CODE_GUIDE_DATASET_JSON
        if not dataset_json_path.exists():
            report["errors"].append(
                {"file": str(dataset_json_path), "error": "dataset_documents_missing"}
            )
            return

        try:
            dataset_docs = json.loads(dataset_json_path.read_text(encoding="utf-8"))
        except Exception as exc:
            report["errors"].append(
                {"file": str(dataset_json_path), "error": f"dataset_load_failed:{exc}"}
            )
            return

        if not isinstance(dataset_docs, list):
            report["errors"].append(
                {"file": str(dataset_json_path), "error": "dataset_documents_not_list"}
            )
            return

        for idx, item in enumerate(dataset_docs):
            if not isinstance(item, dict):
                report["records_seen"] += 1
                report["records_skipped"] += 1
                report["errors"].append(
                    {
                        "file": str(dataset_json_path),
                        "index": idx,
                        "error": "dataset_item_not_dict",
                    }
                )
                continue

            dataset_id = str(item.get("Dataset_id") or item.get("dataset_id") or f"dataset_{idx + 1}")
            name = str(item.get("Name") or item.get("name") or "")
            provider = str(item.get("Provider") or item.get("provider") or "")
            snippet = str(item.get("Snippet") or item.get("snippet") or "")
            tags = str(item.get("Tags") or item.get("tags") or "")
            data_type = str(item.get("Type") or item.get("type") or "")
            description = str(item.get("Description") or item.get("description") or "")
            website = str(item.get("Website") or item.get("website") or "")
            doi = str(item.get("DOI") or item.get("doi") or "")

            payload = {
                "dataset_id": dataset_id,
                "name": name,
                "provider": provider,
                "snippet": snippet,
                "tags": tags,
                "type": data_type,
                "description": description,
                "website": website,
                "doi": doi,
            }
            page_content = (
                f"Dataset ID: {dataset_id}\n"
                f"Name: {name}\n"
                f"Provider: {provider}\n"
                f"Snippet: {snippet}\n"
                f"Tags: {tags}\n"
                f"Type: {data_type}\n"
                f"Website: {website}\n"
                f"DOI: {doi}\n"
                f"Description: {description}\n\n"
                f"Normalized JSON:\n{json.dumps(payload, ensure_ascii=False, indent=2)}"
            )

            metadata = {
                "source": str(dataset_json_path),
                "source_file": str(dataset_json_path),
                "doc_type": "dataset_card",
                "language": "json",
                "source_bucket": "code_guide",
                "topic_tags": _topic_tags_from_text(" ".join(payload.values())),
                "symbol": dataset_id,
                "quality_tier": "medium",
                "task_id": "",
                "category": "dataset",
                "tool_names": "[]",
            }
            self._register_document(docs, report, dedupe_hashes, page_content, metadata)

    def _build_code_symbol_docs(
        self,
        code_guide_dir: Path,
        docs: list[Document],
        report: dict[str, Any],
        dedupe_hashes: set[str],
    ) -> None:
        source_dir = code_guide_dir / CODE_GUIDE_GEE_DIR
        if not source_dir.exists():
            report["errors"].append({"file": str(source_dir), "error": "code_symbol_dir_missing"})
            return

        for file_path in sorted(source_dir.rglob("*.py")):
            if _python_file_is_test(file_path):
                continue
            if any(part.lower() in CODE_EXCLUDE_DIRECTORIES for part in file_path.parts):
                continue

            try:
                source = _read_text_with_fallback(file_path)
            except Exception as exc:
                report["records_seen"] += 1
                report["records_skipped"] += 1
                report["errors"].append(
                    {"file": str(file_path), "error": f"python_read_failed:{exc}"}
                )
                continue

            try:
                symbols = self._extract_python_symbols(file_path)
            except Exception as exc:
                report["records_seen"] += 1
                report["records_skipped"] += 1
                report["errors"].append(
                    {"file": str(file_path), "error": f"python_parse_failed:{exc}"}
                )
                continue

            rel_path = _display_path(file_path)
            if symbols:
                for symbol in symbols:
                    docstring = symbol["docstring"] or "No docstring."
                    signature = symbol["signature"] or f"{symbol['name']}(...)"
                    page_content = (
                        f"Source: {rel_path}\n"
                        f"Symbol: {symbol['name']}\n"
                        f"Kind: {symbol['kind']}\n"
                        f"Signature: {signature}\n"
                        f"Docstring: {docstring}\n\n"
                        f"Code:\n```python\n{symbol['code']}\n```"
                    )
                    metadata = {
                        "source": str(file_path),
                        "source_file": str(file_path),
                        "doc_type": "code_symbol",
                        "language": "python",
                        "source_bucket": "code_guide",
                        "topic_tags": _topic_tags_from_text(page_content),
                        "symbol": symbol["name"],
                        "quality_tier": "medium",
                        "task_id": "",
                        "category": "code",
                        "tool_names": "[]",
                    }
                    self._register_document(docs, report, dedupe_hashes, page_content, metadata)
            else:
                page_content = f"Source: {rel_path}\n\n```python\n{source}\n```"
                metadata = {
                    "source": str(file_path),
                    "source_file": str(file_path),
                    "doc_type": "code_module",
                    "language": "python",
                    "source_bucket": "code_guide",
                    "topic_tags": _topic_tags_from_text(page_content),
                    "symbol": "",
                    "quality_tier": "medium",
                    "task_id": "",
                    "category": "code",
                    "tool_names": "[]",
                }
                self._register_document(docs, report, dedupe_hashes, page_content, metadata)

    def _build_gis_guide_docs(
        self,
        code_guide_dir: Path,
        docs: list[Document],
        report: dict[str, Any],
        dedupe_hashes: set[str],
        include_gis_docs: bool,
    ) -> None:
        if not include_gis_docs:
            return

        for rel_dir in CODE_GUIDE_GIS_DIRS:
            guide_dir = code_guide_dir / rel_dir
            if not guide_dir.exists():
                continue

            for txt_path in sorted(guide_dir.rglob(f"*{CODE_GIS_SUFFIX}")):
                try:
                    raw = _read_text_with_fallback(txt_path)
                except Exception as exc:
                    report["records_seen"] += 1
                    report["records_skipped"] += 1
                    report["errors"].append(
                        {"file": str(txt_path), "error": f"gis_text_load_failed:{exc}"}
                    )
                    continue

                chunks = self.text_splitter.split_text(raw)
                if not chunks:
                    continue

                base_tags = {"geopandas"} if "geopandas" in str(rel_dir).lower() else {"rasterio"}
                if "geospatial_code_geopanda_rasterio" in str(rel_dir).lower():
                    base_tags = {"geopandas", "rasterio"}

                for idx, chunk in enumerate(chunks, start=1):
                    page_content = (
                        f"Guide: {txt_path.name}\n"
                        f"Source: {_display_path(txt_path)}\n"
                        f"Chunk: {idx}/{len(chunks)}\n\n"
                        f"{chunk}"
                    )
                    metadata = {
                        "source": str(txt_path),
                        "source_file": str(txt_path),
                        "doc_type": "gis_guide",
                        "language": "text",
                        "source_bucket": "code_guide",
                        "topic_tags": _topic_tags_from_text(page_content, base_tags=base_tags),
                        "symbol": f"{txt_path.stem}_chunk_{idx}",
                        "quality_tier": "medium",
                        "task_id": "",
                        "category": "code_guide",
                        "tool_names": "[]",
                        "_dedupe_content": chunk,
                        "_dedupe_symbol": "",
                    }
                    self._register_document(docs, report, dedupe_hashes, page_content, metadata)
    def _build_tool_template_docs(
        self,
        tool_dir: Path,
        docs: list[Document],
        report: dict[str, Any],
        dedupe_hashes: set[str],
    ) -> None:
        for file_name, target_symbols in TOOL_TEMPLATE_TARGETS.items():
            file_path = tool_dir / file_name
            if not file_path.exists():
                report["errors"].append(
                    {"file": str(file_path), "error": "tool_template_file_missing"}
                )
                continue

            try:
                symbols = self._extract_python_symbols(file_path)
            except Exception as exc:
                report["errors"].append(
                    {"file": str(file_path), "error": f"tool_template_parse_failed:{exc}"}
                )
                continue
            by_name = {item["name"]: item for item in symbols}

            for symbol_name in target_symbols:
                symbol = by_name.get(symbol_name)
                if not symbol:
                    report["records_seen"] += 1
                    report["records_skipped"] += 1
                    report["errors"].append(
                        {
                            "file": str(file_path),
                            "symbol": symbol_name,
                            "error": "tool_template_symbol_missing",
                        }
                    )
                    continue

                signature = symbol["signature"] or f"{symbol['name']}(...)"
                page_content = (
                    f"Template Source: {_display_path(file_path)}\n"
                    f"Template Symbol: {symbol['name']}\n"
                    f"Signature: {signature}\n"
                    f"Docstring: {symbol['docstring'] or 'No docstring.'}\n\n"
                    f"Code:\n```python\n{symbol['code']}\n```"
                )
                metadata = {
                    "source": str(file_path),
                    "source_file": str(file_path),
                    "doc_type": "tool_template",
                    "language": "python",
                    "source_bucket": "tools_latest",
                    "topic_tags": _topic_tags_from_text(page_content, base_tags={"ntl"}),
                    "symbol": symbol_name,
                    "quality_tier": "high",
                    "task_id": "",
                    "category": "tool_template",
                    "tool_names": "[]",
                }
                self._register_document(docs, report, dedupe_hashes, page_content, metadata)

    def build_code_documents(
        self,
        code_guide_dir: str,
        tool_dir: str,
        include_gis_docs: bool = True,
        report: dict[str, Any] | None = None,
        dedupe_hashes: set[str] | None = None,
    ) -> list[Document]:
        local_report = report if report is not None else self._init_report(profile=CODE_PROFILE, reset=False)
        local_hashes = dedupe_hashes if dedupe_hashes is not None else set()
        docs: list[Document] = []

        code_guide_path = Path(code_guide_dir)
        tool_path = Path(tool_dir)

        self._build_dataset_card_docs(code_guide_path, docs, local_report, local_hashes)
        self._build_code_symbol_docs(code_guide_path, docs, local_report, local_hashes)
        self._build_gis_guide_docs(code_guide_path, docs, local_report, local_hashes, include_gis_docs)
        self._build_tool_template_docs(tool_path, docs, local_report, local_hashes)

        return docs

    def build_literature_documents(
        self,
        literature_dir: str,
        report: dict[str, Any] | None = None,
        dedupe_hashes: set[str] | None = None,
    ) -> list[Document]:
        local_report = (
            report if report is not None else self._init_report(profile=LITERATURE_PROFILE, reset=False)
        )
        local_hashes = dedupe_hashes if dedupe_hashes is not None else set()
        docs: list[Document] = []

        literature_path = Path(literature_dir)
        if not literature_path.exists():
            local_report["errors"].append(
                {"file": str(literature_path), "error": "literature_dir_missing"}
            )
            return docs

        candidates: list[Path] = []
        for pattern in ("*.pdf", "*.md", "*.txt"):
            candidates.extend(literature_path.rglob(pattern))

        for file_path in sorted(set(candidates)):
            source_file = str(file_path)
            suffix = file_path.suffix.lower()
            source_bucket = "literature_pdf" if suffix == ".pdf" else "literature_text"
            try:
                if suffix == ".pdf":
                    pages = PyPDFLoader(source_file).load()
                else:
                    text = _read_literature_text_file(file_path)
                    pages = [Document(page_content=text, metadata={"source": source_file, "page": 0})]
            except Exception as exc:
                local_report["errors"].append(
                    {"file": source_file, "error": f"literature_load_failed:{exc}"}
                )
                continue

            title = _extract_literature_title(file_path)
            year = _extract_year(file_path.name)
            rel_source = _display_path(file_path)
            folder_hint = _display_path(file_path.parent)
            references_started = False

            for page_doc in pages:
                if references_started:
                    local_report["records_seen"] += 1
                    local_report["records_skipped"] += 1
                    continue

                page_text = str(getattr(page_doc, "page_content", "") or "").strip()
                if not page_text:
                    local_report["records_seen"] += 1
                    local_report["records_skipped"] += 1
                    continue
                page_text, references_started = _strip_references_tail(page_text)
                if not page_text:
                    local_report["records_seen"] += 1
                    local_report["records_skipped"] += 1
                    continue

                page_value = page_doc.metadata.get("page", "")
                page_index = str(page_value) if page_value != "" else ""
                chunks = self.text_splitter.split_text(page_text)
                if not chunks:
                    continue

                language = _infer_language(f"{title}\n{page_text}")
                quality_tier = _infer_literature_quality_tier(title, page_text)
                topic_tags = _topic_tags_from_text(
                    f"{title}\n{folder_hint}\n{page_text}",
                    base_tags={"literature", "ntl"},
                )

                for idx, chunk in enumerate(chunks, start=1):
                    if _is_low_signal_literature_chunk(chunk):
                        local_report["records_seen"] += 1
                        local_report["records_skipped"] += 1
                        continue

                    section_type = _infer_literature_section_type(chunk)
                    page_content = (
                        f"Title: {title or pdf_path.stem}\n"
                        f"Year: {year}\n"
                        f"Source: {rel_source}\n"
                        f"Page: {page_index}\n"
                        f"Chunk: {idx}/{len(chunks)}\n\n"
                        f"{chunk}"
                    )
                    metadata = {
                        "source": source_file,
                        "source_file": source_file,
                        "doc_type": "literature_paper",
                        "language": language,
                        "title": title or pdf_path.stem,
                        "year": year,
                        "section_type": section_type,
                        "topic_tags": topic_tags,
                        "quality_tier": quality_tier,
                        "source_bucket": source_bucket,
                        "task_id": "",
                        "category": "literature",
                        "tool_names": "[]",
                        "symbol": f"{file_path.stem}_p{page_index or 'na'}_c{idx}",
                        "_dedupe_content": chunk,
                        "_dedupe_symbol": "",
                    }
                    self._register_document(docs, local_report, local_hashes, page_content, metadata)

        return docs

    def _clear_collection(self) -> int:
        collection_info = self.vector_store._collection.get(include=["metadatas"])
        ids = collection_info.get("ids", [])
        if not ids:
            return 0
        self.vector_store._collection.delete(ids=ids)
        return len(ids)

    def create_database(
        self,
        url_list: list[str] | None = None,
        pdf_folder: str | None = None,
        json_folder: str | None = None,
        py_folder: str | None = None,
        txt_folder: str | None = None,
        literature_dir: str | None = None,
        report_path: str | None = None,
        reset: bool = False,
        profile: str = SOLUTION_PROFILE,
        code_guide_dir: str | None = None,
        tool_dir: str | None = None,
        include_gis_docs: bool = True,
    ) -> dict[str, Any]:
        profile = (profile or SOLUTION_PROFILE).lower()
        if profile not in {SOLUTION_PROFILE, CODE_PROFILE, LITERATURE_PROFILE}:
            raise ValueError(
                f"Unsupported profile '{profile}'. "
                f"Use '{SOLUTION_PROFILE}', '{CODE_PROFILE}', or '{LITERATURE_PROFILE}'."
            )

        report = self._init_report(profile=profile, reset=reset)
        report["collection_name"] = self.collection_name
        report["persistent_directory"] = self.persistent_directory
        report["embedding_model"] = self.embedding_model

        if reset:
            report["deleted_before_rebuild"] = self._clear_collection()

        dedupe_hashes: set[str] = set()
        all_docs: list[Document] = []

        if profile == CODE_PROFILE:
            code_guide_dir = code_guide_dir or r".\RAG\code_guide"
            tool_dir = tool_dir or r".\tools"
            all_docs = self.build_code_documents(
                code_guide_dir=code_guide_dir,
                tool_dir=tool_dir,
                include_gis_docs=include_gis_docs,
                report=report,
                dedupe_hashes=dedupe_hashes,
            )
        elif profile == LITERATURE_PROFILE:
            literature_dir = literature_dir or DEFAULT_LITERATURE_DIR
            all_docs = self.build_literature_documents(
                literature_dir=literature_dir,
                report=report,
                dedupe_hashes=dedupe_hashes,
            )
        else:
            json_docs: list[Document] = []
            non_json_docs: list[Document] = []

            if url_list:
                for url in url_list:
                    try:
                        non_json_docs.extend(WebBaseLoader(url).load())
                    except Exception as exc:
                        report["errors"].append({"file": url, "error": f"url_load_failed:{exc}"})

            if pdf_folder and os.path.isdir(pdf_folder):
                for file_name in os.listdir(pdf_folder):
                    if not file_name.lower().endswith(".pdf"):
                        continue
                    file_path = os.path.join(pdf_folder, file_name)
                    try:
                        non_json_docs.extend(PyPDFLoader(file_path).load())
                    except Exception as exc:
                        report["errors"].append(
                            {"file": file_path, "error": f"pdf_load_failed:{exc}"}
                        )

            if py_folder and os.path.isdir(py_folder):
                for file_name in os.listdir(py_folder):
                    if not file_name.lower().endswith(".py"):
                        continue
                    file_path = os.path.join(py_folder, file_name)
                    try:
                        non_json_docs.extend(PythonLoader(file_path).load())
                    except Exception as exc:
                        report["errors"].append(
                            {"file": file_path, "error": f"python_load_failed:{exc}"}
                        )

            if txt_folder and os.path.isdir(txt_folder):
                for file_name in os.listdir(txt_folder):
                    if not file_name.lower().endswith(".txt"):
                        continue
                    file_path = os.path.join(txt_folder, file_name)
                    try:
                        non_json_docs.extend(_safe_read_text_loader(file_path))
                    except Exception as exc:
                        report["errors"].append(
                            {"file": file_path, "error": f"text_load_failed:{exc}"}
                        )

            report["non_json_docs_loaded"] = len(non_json_docs)

            if json_folder and os.path.isdir(json_folder):
                for file_name in sorted(os.listdir(json_folder)):
                    if not file_name.lower().endswith(".json"):
                        continue
                    file_path = Path(json_folder) / file_name
                    json_docs.extend(self._build_json_documents(file_path, report, dedupe_hashes))

            split_non_json_docs = self.text_splitter.split_documents(non_json_docs)
            all_docs = split_non_json_docs + json_docs

        if not all_docs:
            report["final_collection_count"] = self.vector_store._collection.count()
            if report_path:
                Path(report_path).write_text(
                    json.dumps(report, ensure_ascii=False, indent=2) + "\n",
                    encoding="utf-8",
                )
            return report

        batch_size = 200
        for i in range(0, len(all_docs), batch_size):
            batch = all_docs[i : i + batch_size]
            self.vector_store.add_documents(batch)
            report["chunks_written"] += len(batch)

        report["final_collection_count"] = self.vector_store._collection.count()
        if report_path:
            Path(report_path).write_text(
                json.dumps(report, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
        return report

    def add_documents(self, folder_path: str, report_path: str | None = None) -> dict[str, Any]:
        return self.create_database(
            pdf_folder=folder_path,
            py_folder=folder_path,
            txt_folder=folder_path,
            json_folder=folder_path,
            report_path=report_path,
            reset=False,
            profile=SOLUTION_PROFILE,
        )


class ChromaManager:
    def __init__(
        self,
        persistent_directory: str,
        collection_name: str = "knowledge-chroma",
        embedding_model: str = "text-embedding-3-small",
    ):
        _ensure_openai_api_key()
        self.persistent_directory = persistent_directory
        self.collection_name = collection_name
        self.embeddings = OpenAIEmbeddings(model=embedding_model)
        os.makedirs(persistent_directory, exist_ok=True)
        self.vector_store = Chroma(
            collection_name=collection_name,
            persist_directory=persistent_directory,
            embedding_function=self.embeddings,
        )

    def count_documents(self) -> int:
        return self.vector_store._collection.count()

    def list_documents(self, limit: int | None = None) -> dict[str, Any]:
        docs = self.vector_store._collection.get(include=["documents", "metadatas"])
        if limit:
            docs["ids"] = docs["ids"][:limit]
            docs["documents"] = docs["documents"][:limit]
            docs["metadatas"] = docs["metadatas"][:limit]
        return docs

    def search_documents(self, query: str, k: int = 5) -> list[Document]:
        return self.vector_store.similarity_search(query, k=k)

    def delete_documents(
        self,
        ids: list[str] | None = None,
        filter_metadata: dict[str, Any] | None = None,
    ) -> int:
        if ids:
            self.vector_store._collection.delete(ids=ids)
            return len(ids)
        if filter_metadata:
            delete_ids = self.vector_store._collection.get(
                where=filter_metadata,
                include=["ids"],
            )["ids"]
            if delete_ids:
                self.vector_store._collection.delete(ids=delete_ids)
            return len(delete_ids)
        return 0

    def delete_all_documents(self) -> int:
        docs = self.vector_store._collection.get(include=["metadatas"])
        ids = docs.get("ids", [])
        if not ids:
            return 0
        self.vector_store._collection.delete(ids=ids)
        return len(ids)
def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Rebuild NTL RAG collections (Solution, Code, or Literature)."
    )
    parser.add_argument(
        "--profile",
        default=SOLUTION_PROFILE,
        choices=[SOLUTION_PROFILE, CODE_PROFILE, LITERATURE_PROFILE],
        help="Ingestion profile: solution | code | literature.",
    )
    parser.add_argument(
        "--json-dir",
        default=r".\RAG\guidence_json",
        help="Folder containing guidance JSON files (solution profile).",
    )
    parser.add_argument(
        "--code-guide-dir",
        default=r".\RAG\code_guide",
        help="Folder containing code_guide corpus (code profile).",
    )
    parser.add_argument(
        "--tool-dir",
        default=r".\tools",
        help="Folder containing latest local tool implementations (code profile).",
    )
    parser.add_argument(
        "--literature-dir",
        default=DEFAULT_LITERATURE_DIR,
        help="Folder containing literature PDF corpus (literature profile).",
    )
    parser.add_argument(
        "--include-gis-docs",
        action="store_true",
        default=True,
        help="Include GIS text guides from code_guide (code profile).",
    )
    parser.add_argument(
        "--no-include-gis-docs",
        action="store_false",
        dest="include_gis_docs",
        help="Exclude GIS text guides from code_guide (code profile).",
    )
    parser.add_argument(
        "--persist-dir",
        default=DEFAULT_SOLUTION_PERSIST_DIR,
        help="Chroma persist directory.",
    )
    parser.add_argument(
        "--collection-name",
        default=DEFAULT_SOLUTION_COLLECTION_NAME,
        help="Chroma collection name.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete current collection documents before rebuild.",
    )
    parser.add_argument(
        "--report-path",
        default=DEFAULT_SOLUTION_REPORT_PATH,
        help="Where to write rebuild report JSON.",
    )
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()
    persist_dir = args.persist_dir
    collection_name = args.collection_name
    report_path = args.report_path

    if args.profile == LITERATURE_PROFILE:
        if persist_dir == DEFAULT_SOLUTION_PERSIST_DIR:
            persist_dir = DEFAULT_LITERATURE_PERSIST_DIR
        if collection_name == DEFAULT_SOLUTION_COLLECTION_NAME:
            collection_name = DEFAULT_LITERATURE_COLLECTION_NAME
        if report_path == DEFAULT_SOLUTION_REPORT_PATH:
            report_path = DEFAULT_LITERATURE_REPORT_PATH

    rag_db = RAGDatabase(
        persistent_directory=persist_dir,
        collection_name=collection_name,
    )
    report = rag_db.create_database(
        profile=args.profile,
        json_folder=args.json_dir,
        code_guide_dir=args.code_guide_dir,
        tool_dir=args.tool_dir,
        literature_dir=args.literature_dir,
        include_gis_docs=args.include_gis_docs,
        reset=args.reset,
        report_path=report_path,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
