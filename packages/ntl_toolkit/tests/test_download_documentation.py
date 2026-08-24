from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]


def test_local_download_docs_configure_env_file_without_secret_value() -> None:
    text = (REPO_ROOT / "docs" / "mcp" / "ntl-download.md").read_text(encoding="utf-8")

    assert "ntl-download" in text
    assert "NTL_MCP_ENV_FILE" in text
    assert "EARTHDATA_TOKEN=<" not in text
    assert "mcp_servers/download_server.py" in text


def test_repository_and_package_readmes_link_the_download_mcp() -> None:
    root_readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    package_readme = (REPO_ROOT / "packages" / "ntl_toolkit" / "README.md").read_text(
        encoding="utf-8"
    )

    assert "ntl-download" in root_readme
    assert "ntl-download" in package_readme
