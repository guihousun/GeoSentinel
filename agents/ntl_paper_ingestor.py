"""NTL Paper Ingestor - Add preprocessed papers to knowledge base."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from langchain_chroma import Chroma
from langchain_core.documents import Document
from langchain_openai import OpenAIEmbeddings
from langchain_text_splitters import RecursiveCharacterTextSplitter

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


load_dotenv()


DEFAULT_LITERATURE_PERSIST_DIR = ROOT_DIR / "RAG" / "Literature_RAG"
DEFAULT_LITERATURE_COLLECTION_NAME = "Literature_RAG"
DEFAULT_LITERATURE_REPORT_PATH = ROOT_DIR / "RAG" / "Literature_RAG" / "rebuild_report.json"


def _ensure_openai_api_key() -> None:
    import os
    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError(
            "OPENAI_API_KEY is required to build Chroma embeddings. "
            "Set it in environment variables or .env."
        )


def _load_processed_papers(processed_dir: Path) -> list[dict]:
    results_file = processed_dir / "processing_results.json"
    if not results_file.exists():
        print(f"No processing results found in {results_file}")
        return []

    with open(results_file, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_metadata(metadata_dir: Path) -> list[dict]:
    metadata_file = metadata_dir / "metadata.json"
    if not metadata_file.exists():
        return []

    with open(metadata_file, "r", encoding="utf-8") as f:
        return json.load(f)


def _create_documents_from_papers(
    papers: list[dict],
    processed_dir: Path,
    metadata_list: list[dict],
) -> list[Document]:
    docs = []
    metadata_map = {p.get("paper_hash", ""): p for p in metadata_list}

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=200,
        length_function=len,
    )

    for paper in papers:
        paper_hash = paper.get("paper_hash", "")
        processed_file = paper.get("processed_file", "")

        if not processed_file:
            continue

        text_path = Path(processed_file)
        if not text_path.exists():
            continue

        try:
            text = text_path.read_text(encoding="utf-8")
        except Exception as e:
            print(f"Error reading {text_path}: {e}")
            continue

        if not text.strip():
            continue

        metadata = metadata_map.get(paper_hash, {})
        title = metadata.get("title", paper.get("title", ""))
        year = metadata.get("year", paper.get("year", ""))
        source = metadata.get("source", "")
        doi = metadata.get("doi", "")

        chunks = text_splitter.split_text(text)

        for idx, chunk in enumerate(chunks, start=1):
            page_content = (
                f"Title: {title}\n"
                f"Year: {year}\n"
                f"Source: {source}\n"
                f"DOI: {doi}\n"
                f"Chunk: {idx}/{len(chunks)}\n\n"
                f"{chunk}"
            )

            doc_metadata = {
                "source": processed_file,
                "source_file": processed_file,
                "doc_type": "literature_paper",
                "language": "en",
                "title": title,
                "year": year,
                "section_type": "body",
                "topic_tags": "ntl,literature",
                "quality_tier": "high",
                "doi": doi,
                "chunk_index": idx,
                "total_chunks": len(chunks),
            }

            docs.append(Document(page_content=page_content, metadata=doc_metadata))

    return docs


def ingest_papers_to_kb(
    processed_dir: str | Path,
    persist_dir: str | Path | None = None,
    collection_name: str | None = None,
    reset: bool = False,
) -> dict[str, Any]:
    _ensure_openai_api_key()

    processed_path = Path(processed_dir)
    metadata_dir = processed_path.parent / "metadata"

    papers = _load_processed_papers(processed_path)
    if not papers:
        print("No processed papers found.")
        return {"status": "no_papers", "ingested": 0}

    metadata_list = _load_metadata(metadata_dir)

    print(f"Creating documents from {len(papers)} papers...")
    docs = _create_documents_from_papers(papers, processed_path, metadata_list)
    print(f"Created {len(docs)} document chunks.")

    if not docs:
        print("No documents created.")
        return {"status": "no_documents", "ingested": 0}

    persist_path = Path(persist_dir) if persist_dir else DEFAULT_LITERATURE_PERSIST_DIR
    collection = collection_name or DEFAULT_LITERATURE_COLLECTION_NAME

    embeddings = OpenAIEmbeddings(
        model="text-embedding-3-small",
        api_key=None,
    )

    if reset:
        import shutil
        if persist_path.exists():
            shutil.rmtree(persist_path)
            print(f"Cleared existing collection at {persist_path}")

    persist_path.mkdir(parents=True, exist_ok=True)

    vectorstore = Chroma(
        persist_directory=str(persist_path),
        embedding_function=embeddings,
        collection_name=collection,
    )

    print(f"Adding documents to collection '{collection}'...")
    vectorstore.add_documents(docs)

    print(f"Successfully ingested {len(docs)} document chunks.")

    return {
        "status": "success",
        "ingested": len(docs),
        "papers": len(papers),
        "persist_dir": str(persist_path),
        "collection_name": collection,
    }


def main():
    parser = argparse.ArgumentParser(
        description="NTL Paper Ingestor - Add preprocessed papers to knowledge base",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python agents/ntl_paper_ingestor.py --processed-dir RAG/paper_retrieval/processed
  python agents/ntl_paper_ingestor.py --processed-dir RAG/paper_retrieval/processed --reset
        """
    )
    parser.add_argument(
        "--processed-dir",
        required=True,
        help="Directory containing processed text files (from ntl_paper_preprocessor.py)"
    )
    parser.add_argument(
        "--persist-dir",
        default=str(DEFAULT_LITERATURE_PERSIST_DIR),
        help="Chroma persist directory"
    )
    parser.add_argument(
        "--collection-name",
        default=DEFAULT_LITERATURE_COLLECTION_NAME,
        help="Chroma collection name"
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete current collection documents before adding new ones"
    )

    args = parser.parse_args()

    result = ingest_papers_to_kb(
        processed_dir=args.processed_dir,
        persist_dir=args.persist_dir,
        collection_name=args.collection_name,
        reset=args.reset,
    )

    print(f"\nIngestion complete: {result}")


if __name__ == "__main__":
    main()
