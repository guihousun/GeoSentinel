"""NTL Paper Retriever - Automated paper retrieval from academic sources."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

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


def sanitize_filename(title: str) -> str:
    title = re.sub(r"[^\w\s\-]", "", title)
    title = re.sub(r"\s+", "_", title)
    return title[:100]


def compute_paper_hash(paper: dict) -> str:
    key = f"{paper.get('doi', '')}{paper.get('title', '')}"
    return hashlib.md5(key.encode()).hexdigest()[:12]


def normalize_title(title: str) -> str:
    return re.sub(r"\s+", " ", title.lower().strip())


def is_duplicate(new_paper: dict, existing_papers: list[dict]) -> bool:
    new_norm = normalize_title(new_paper.get("title", ""))
    new_doi = new_paper.get("doi", "").lower().strip()

    for paper in existing_papers:
        if new_doi and paper.get("doi", "").lower().strip() == new_doi:
            return True
        existing_norm = normalize_title(paper.get("title", ""))
        if existing_norm and new_norm:
            if existing_norm == new_norm:
                return True
            if new_norm in existing_norm or existing_norm in new_norm:
                if len(new_norm) > 20 and len(existing_norm) > 20:
                    return True
    return False


def search_arxiv(keywords: list[str], config: dict) -> list[dict]:
    results = []
    arxiv_config = config.get("arxiv", {})
    max_results = arxiv_config.get("max_results", 50)

    headers = {
        "User-Agent": "NTL-Paper-Retriever/1.0 (mailto:research@example.com)"
    }

    for keyword in keywords:
        query = f"all:{keyword}"
        url = (
            f"http://export.arxiv.org/api/query?search_query={query}"
            f"&start=0&max_results={max_results}&sortBy=relevance&sortOrder=descending"
        )

        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()

            entries = re.findall(
                r"<entry>.*?<title>(.*?)</title>.*?<summary>(.*?)</summary>.*?"
                r"<link.*?href=\"(.*?)\".*?</link>.*?"
                r"<published>(.*?)</published>.*?"
                r"(?:<arxiv:doi>(.*?)</arxiv:doi>)?.*?"
                r"</entry>",
                response.text,
                re.DOTALL
            )

            for entry in entries:
                title, summary, link, published, doi = entry
                title = title.strip()
                summary = summary.strip()
                pdf_link = link.replace("abs", "pdf")

                paper = {
                    "title": title,
                    "abstract": summary,
                    "year": published[:4] if published else "",
                    "source": "arxiv",
                    "url": link,
                    "pdf_url": pdf_link,
                    "doi": doi.strip() if doi else "",
                    "authors": [],
                    "venue": ""
                }
                results.append(paper)

        except Exception as e:
            print(f"Error searching arXiv for '{keyword}': {e}")

        time.sleep(3)

    return results


def search_semantic_scholar(keywords: list[str], config: dict) -> list[dict]:
    results = []
    ss_config = config.get("semantic_scholar", {})
    api_key = os.getenv(ss_config.get("api_key_env", "SEMANTIC_SCHOLAR_API_KEY"))
    fields = ss_config.get("fields", "title,authors,year,abstract,url,doi,venue")
    max_results = config.get("retrieval", {}).get("max_results_per_keyword", 50)

    headers = {
        "User-Agent": "NTL-Paper-Retriever/1.0"
    }
    if api_key:
        headers["x-api-key"] = api_key

    for keyword in keywords:
        url = (
            f"https://api.semanticscholar.org/graph/v1/paper/search"
            f"?query={urllib.parse.quote(keyword)}"
            f"&fields={fields}"
            f"&limit={max_results}"
            f"&offset=0"
        )

        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()

            for item in data.get("data", []):
                authors = []
                for author in item.get("authors", []):
                    authors.append(author.get("name", ""))

                pdf_url = ""
                if item.get("url"):
                    pdf_url = item["url"].replace("semantic scholar", "arxiv.org/pdf")

                paper = {
                    "title": item.get("title", ""),
                    "abstract": item.get("abstract", ""),
                    "year": str(item.get("year", "")),
                    "source": "semantic_scholar",
                    "url": item.get("url", ""),
                    "pdf_url": pdf_url,
                    "doi": item.get("doi", ""),
                    "authors": authors,
                    "venue": item.get("venue", "")
                }
                results.append(paper)

        except Exception as e:
            print(f"Error searching Semantic Scholar for '{keyword}': {e}")

        time.sleep(1)

    return results


def deduplicate_papers(papers: list[dict]) -> list[dict]:
    unique_papers = []
    for paper in papers:
        if not is_duplicate(paper, unique_papers):
            unique_papers.append(paper)
    return unique_papers


def download_pdf(pdf_url: str, output_dir: Path, filename: str) -> str | None:
    if not pdf_url:
        return None

    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / f"{filename}.pdf"

    if filepath.exists():
        print(f"PDF already exists: {filepath}")
        return str(filepath)

    try:
        response = requests.get(pdf_url, timeout=60, stream=True)
        response.raise_for_status()

        with open(filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        print(f"Downloaded PDF: {filepath}")
        return str(filepath)

    except Exception as e:
        print(f"Error downloading PDF from {pdf_url}: {e}")
        return None


def save_metadata(papers: list[dict], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata_file = output_dir / "metadata.json"

    existing = []
    if metadata_file.exists():
        with open(metadata_file, "r", encoding="utf-8") as f:
            existing = json.load(f)

    existing.extend(papers)

    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)

    print(f"Saved metadata for {len(papers)} papers to {metadata_file}")


def retrieve_papers(
    keywords: list[str] | None = None,
    sources: list[str] | None = None,
    output_dir: str | None = None,
    config_path: str = "config/paper_retrieval_config.json",
    download_pdf_flag: bool = True,
) -> list[dict]:
    config = load_config(config_path)

    if keywords is None:
        keywords = config.get("retrieval", {}).get("keywords", [])
    if sources is None:
        sources = config.get("retrieval", {}).get("sources", ["arxiv", "semantic_scholar"])

    base_dir = ROOT_DIR / (output_dir or config.get("output", {}).get("base_dir", "RAG/paper_retrieval"))
    raw_dir = base_dir / "raw"
    metadata_dir = base_dir / "metadata"

    all_papers = []

    if "arxiv" in sources:
        print(f"Searching arXiv for keywords: {keywords}")
        arxiv_results = search_arxiv(keywords, config)
        all_papers.extend(arxiv_results)
        print(f"Found {len(arxiv_results)} papers from arXiv")

    if "semantic_scholar" in sources:
        print(f"Searching Semantic Scholar for keywords: {keywords}")
        ss_results = search_semantic_scholar(keywords, config)
        all_papers.extend(ss_results)
        print(f"Found {len(ss_results)} papers from Semantic Scholar")

    unique_papers = deduplicate_papers(all_papers)
    print(f"Total papers after deduplication: {len(unique_papers)}")

    for paper in unique_papers:
        paper["paper_hash"] = compute_paper_hash(paper)
        paper["filename"] = sanitize_filename(paper["title"])

    if download_pdf_flag:
        for paper in unique_papers:
            if paper.get("pdf_url"):
                filename = paper.get("filename", paper.get("paper_hash", ""))
                pdf_path = download_pdf(paper["pdf_url"], raw_dir, filename)
                paper["local_pdf"] = pdf_path

    save_metadata(unique_papers, metadata_dir)

    return unique_papers


def main():
    parser = argparse.ArgumentParser(
        description="NTL Paper Retriever - Automated paper retrieval from academic sources",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python agents/ntl_paper_retriever.py --keywords "nighttime light" --sources arxiv
  python agents/ntl_paper_retriever.py --keywords "VIIRS" "DMSP-OLS" --sources semantic_scholar
  python agents/ntl_paper_retriever.py --no-download
        """
    )
    parser.add_argument(
        "--keywords",
        nargs="+",
        help="Keywords to search for (space separated)"
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        choices=["arxiv", "semantic_scholar"],
        help="Sources to search from"
    )
    parser.add_argument(
        "--output-dir",
        help="Output directory for retrieved papers"
    )
    parser.add_argument(
        "--config",
        default="config/paper_retrieval_config.json",
        help="Path to configuration file"
    )
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="Skip PDF download"
    )

    args = parser.parse_args()

    papers = retrieve_papers(
        keywords=args.keywords,
        sources=args.sources,
        output_dir=args.output_dir,
        config_path=args.config,
        download_pdf_flag=not args.no_download
    )

    print(f"\nRetrieval complete! Found {len(papers)} unique papers.")


if __name__ == "__main__":
    main()
