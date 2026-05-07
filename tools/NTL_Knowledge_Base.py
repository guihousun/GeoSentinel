"""Retriever tools for NTL knowledge stores."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Callable

from dotenv import load_dotenv
from langchain_chroma import Chroma
from langchain_core.tools import StructuredTool, create_retriever_tool
from langchain_openai import OpenAIEmbeddings


load_dotenv()


def _resolve_rag_persist_dir(store_name: str) -> str:
    """
    Resolve RAG store path in a cwd-independent way.

    Priority:
    1) NTL_RAG_ROOT env (absolute recommended), e.g. D:\\地缘环境智能计算平台\\地缘环境智能计算平台\\RAG
    2) repository-root/RAG (derived from this file location)
    """
    env_root = str(os.getenv("NTL_RAG_ROOT", "") or "").strip()
    if env_root:
        root = Path(env_root).expanduser().resolve()
    else:
        root = Path(__file__).resolve().parents[1] / "RAG"
    return str((root / store_name).resolve())


def _ensure_openai_api_key() -> None:
    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError(
            "OPENAI_API_KEY is required for NTL Knowledge Base retrieval. "
            "Set it in environment variables or .env."
        )


def _make_empty_store_tool(tool_name: str, description: str, store_name: str) -> StructuredTool:
    def _empty_store(query: str) -> str:
        return json.dumps(
            {
                "status": "empty_store",
                "store": store_name,
                "reason": f"{store_name} currently has no indexed documents.",
                "query": query,
            },
            ensure_ascii=False,
        )

    return StructuredTool.from_function(
        func=_empty_store,
        name=tool_name,
        description=description,
    )


def _build_retriever_tool(
    *,
    collection_name: str,
    persist_directory: str,
    tool_name: str,
    description: str,
    embeddings: OpenAIEmbeddings,
    k: int,
    score_threshold: float,
) -> StructuredTool:
    vector_store = Chroma(
        collection_name=collection_name,
        persist_directory=persist_directory,
        embedding_function=embeddings,
    )
    count = vector_store._collection.count()
    if count == 0:
        return _make_empty_store_tool(tool_name, description, collection_name)

    retriever = vector_store.as_retriever(
        search_type="similarity_score_threshold",
        search_kwargs={"k": k, "score_threshold": score_threshold},
    )
    return create_retriever_tool(retriever, name=tool_name, description=description)


_ensure_openai_api_key()
_EMBEDDINGS = OpenAIEmbeddings(model="text-embedding-3-small")


NTL_Literature_Knowledge = _build_retriever_tool(
    collection_name="Literature_RAG",
    persist_directory=_resolve_rag_persist_dir("Literature_RAG"),
    tool_name="NTL_Literature_Knowledge",
    description=(
        "Use this tool to retrieve peer-reviewed academic literature related to "
        "Nighttime Light (NTL) remote sensing. Includes theory, equations, and "
        "scientific definitions."
    ),
    embeddings=_EMBEDDINGS,
    k=2,
    score_threshold=0.33,
)


NTL_Solution_Knowledge = _build_retriever_tool(
    collection_name="Solution_RAG",
    persist_directory=_resolve_rag_persist_dir("Solution_RAG"),
    tool_name="NTL_Solution_Knowledge",
    description=(
        "Use this tool to retrieve structured workflows, tool usage guides, "
        "dataset access instructions, and end-to-end NTL application solutions."
    ),
    embeddings=_EMBEDDINGS,
    k=3,
    score_threshold=0.3,
)


NTL_Code_Knowledge = _build_retriever_tool(
    collection_name="Code_RAG",
    persist_directory=_resolve_rag_persist_dir("Code_RAG"),
    tool_name="NTL_Code_Knowledge",
    description=(
        "Use this tool to retrieve Python and GEE code snippets relevant to NTL tasks. "
        "Focused on executable logic."
    ),
    embeddings=_EMBEDDINGS,
    k=5,
    score_threshold=0.3,
)

