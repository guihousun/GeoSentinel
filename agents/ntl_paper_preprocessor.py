"""NTL Paper Preprocessor - Paper preprocessing for RAG ingestion."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from langchain_community.document_loaders import PyPDFLoader
from langchain_core.documents import Document

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


load_dotenv()


def load_config(config_path: str = "config/paper_retrieval_config.json") -> dict:
    config_file = ROOT_DIR / config_path
    if not config_file.exists():
        return {}
    with open(config_file, "r", encoding="utf-8") as f:
        return json.load(f)


def load_metadata(metadata_dir: Path) -> list[dict]:
    metadata_file = metadata_dir / "metadata.json"
    if not metadata_file.exists():
        return []
    with open(metadata_file, "r", encoding="utf-8") as f:
        return json.load(f)


def remove_header_footer(text: str) -> str:
    lines = text.split("\n")
    cleaned_lines = []

    for line in lines:
        line = line.strip()
        if not line:
            continue

        if len(line) < 3:
            continue

        if re.match(r"^(doi:|DOI:|ISSN|Volume|Issue|Page)", line, re.IGNORECASE):
            continue

        if re.match(r"^\d+\s*$", line):
            continue

        if re.match(r"^(Received|Accepted|Published|Copyright)", line):
            continue

        cleaned_lines.append(line)

    return "\n".join(cleaned_lines)


def remove_references(text: str) -> str:
    reference_patterns = [
        r"(?i)^\s*references\s*$",
        r"(?i)^\s*bibliography\s*$",
        r"(?i)^\s*literature\s+cited\s*$",
    ]

    for pattern in reference_patterns:
        match = re.search(pattern, text, re.MULTILINE)
        if match:
            text = text[:match.start()]

    return text


def remove_blank_pages(text: str) -> str:
    lines = text.split("\n")
    non_blank_lines = []

    for line in lines:
        stripped = line.strip()
        if stripped:
            non_blank_lines.append(line)

    return "\n".join(non_blank_lines)


def remove_figure_captions(text: str) -> str:
    fig_pattern = r"(?i)(figure\s*\d+[\s:.-]*.*?|fig\.\s*\d+[\s:.-]*.*?|图\s*\d+[\s:.-]*)"
    text = re.sub(fig_pattern, "", text)

    table_pattern = r"(?i)(table\s*\d+[\s:.-]*.*?|表\s*\d+[\s:.-]*)"
    text = re.sub(table_pattern, "", text)

    return text


def clean_text(text: str, config: dict) -> str:
    preproc_config = config.get("preprocessing", {})

    text = remove_blank_pages(text)

    if preproc_config.get("remove_references", True):
        text = remove_references(text)

    if preproc_config.get("remove_header_footer", True):
        text = remove_header_footer(text)

    if preproc_config.get("remove_figure_captions", False):
        text = remove_figure_captions(text)

    text = re.sub(r"\n{3,}", "\n\n", text)

    text = re.sub(r" {2,}", " ", text)

    min_length = preproc_config.get("min_page_length", 100)
    if len(text.strip()) < min_length:
        return ""

    return text.strip()


def extract_text_from_pdf(pdf_path: Path) -> str:
    try:
        loader = PyPDFLoader(str(pdf_path))
        pages = loader.load()

        full_text = ""
        for page in pages:
            full_text += page.page_content + "\n\n"

        return full_text

    except Exception as e:
        print(f"Error extracting text from PDF {pdf_path}: {e}")
        return ""


def process_paper(paper: dict, raw_dir: Path, processed_dir: Path, config: dict) -> dict | None:
    pdf_path = paper.get("local_pdf")
    if not pdf_path:
        pdf_path_local = raw_dir / f"{paper.get('filename', paper.get('paper_hash', ''))}.pdf"
        if pdf_path_local.exists():
            pdf_path = str(pdf_path_local)

    if not pdf_path:
        if paper.get("abstract"):
            cleaned_text = paper.get("abstract", "")
        else:
            print(f"No PDF or abstract for paper: {paper.get('title', 'Unknown')}")
            return None
    else:
        pdf_file = Path(pdf_path)
        if not pdf_file.exists():
            print(f"PDF file not found: {pdf_path}")
            return None

        print(f"Processing PDF: {pdf_file.name}")
        raw_text = extract_text_from_pdf(pdf_file)

        if not raw_text:
            print(f"Failed to extract text from: {pdf_file.name}")
            return None

        cleaned_text = clean_text(raw_text, config)

        if not cleaned_text:
            print(f"Text too short after cleaning: {pdf_file.name}")
            return None

    output_filename = f"{paper.get('paper_hash', paper.get('filename', 'unknown'))}.txt"
    output_path = processed_dir / output_filename

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(cleaned_text)

    result = {
        "paper_hash": paper.get("paper_hash", ""),
        "title": paper.get("title", ""),
        "year": paper.get("year", ""),
        "source": paper.get("source", ""),
        "doi": paper.get("doi", ""),
        "processed_file": str(output_path),
        "word_count": len(cleaned_text.split()),
        "char_count": len(cleaned_text)
    }

    return result


def preprocess_papers(
    input_dir: str | None = None,
    output_dir: str | None = None,
    config_path: str = "config/paper_retrieval_config.json",
) -> list[dict]:
    config = load_config(config_path)

    base_dir = ROOT_DIR / (input_dir or config.get("output", {}).get("base_dir", "RAG/paper_retrieval"))
    metadata_dir = base_dir / "metadata"
    raw_dir = base_dir / "raw"
    processed_dir = base_dir / "processed"

    processed_dir.mkdir(parents=True, exist_ok=True)

    papers = load_metadata(metadata_dir)
    if not papers:
        print(f"No metadata found in {metadata_dir}")
        return []

    processed_results = []

    for paper in papers:
        result = process_paper(paper, raw_dir, processed_dir, config)
        if result:
            processed_results.append(result)

    results_file = processed_dir / "processing_results.json"
    with open(results_file, "w", encoding="utf-8") as f:
        json.dump(processed_results, f, ensure_ascii=False, indent=2)

    print(f"\nPreprocessing complete! Processed {len(processed_results)} papers.")
    print(f"Results saved to: {results_file}")

    return processed_results


def main():
    parser = argparse.ArgumentParser(
        description="NTL Paper Preprocessor - Clean papers for RAG ingestion",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python agents/ntl_paper_preprocessor.py
  python agents/ntl_paper_preprocessor.py --input-dir RAG/paper_retrieval
  python agents/ntl_paper_preprocessor.py --output-dir RAG/paper_retrieval/processed
        """
    )
    parser.add_argument(
        "--input-dir",
        help="Input directory containing metadata.json and raw PDFs"
    )
    parser.add_argument(
        "--output-dir",
        help="Output directory for processed text files"
    )
    parser.add_argument(
        "--config",
        default="config/paper_retrieval_config.json",
        help="Path to configuration file"
    )

    args = parser.parse_args()

    results = preprocess_papers(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        config_path=args.config
    )


if __name__ == "__main__":
    main()
