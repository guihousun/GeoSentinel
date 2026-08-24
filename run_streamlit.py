from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from ssl_compat import configure_outbound_ssl


ROOT = Path(__file__).resolve().parent


def _load_streamlit_cli() -> Any:
    from streamlit.web import cli as streamlit_cli

    return streamlit_cli


def main(argv: list[str] | None = None) -> int:
    configure_outbound_ssl()
    streamlit_cli = _load_streamlit_cli()
    extra_args = list(sys.argv[1:] if argv is None else argv)
    sys.argv = [
        "streamlit",
        "run",
        str(ROOT / "Streamlit.py"),
        "--server.address",
        "127.0.0.1",
        "--server.port",
        "8501",
        *extra_args,
    ]
    return int(streamlit_cli.main())


if __name__ == "__main__":
    raise SystemExit(main())
