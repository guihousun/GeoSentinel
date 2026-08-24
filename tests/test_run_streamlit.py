from __future__ import annotations

import sys

import run_streamlit


def test_launcher_configures_ssl_before_loading_streamlit(monkeypatch) -> None:
    events = []

    class FakeCli:
        @staticmethod
        def main() -> int:
            events.append("main")
            return 0

    monkeypatch.setattr(
        run_streamlit,
        "configure_outbound_ssl",
        lambda: events.append("ssl") or "CERTIFI_FALLBACK",
    )
    monkeypatch.setattr(
        run_streamlit,
        "_load_streamlit_cli",
        lambda: events.append("import") or FakeCli,
    )

    result = run_streamlit.main(["--server.headless", "true"])

    assert result == 0
    assert events == ["ssl", "import", "main"]
    assert sys.argv == [
        "streamlit",
        "run",
        str(run_streamlit.ROOT / "Streamlit.py"),
        "--server.address",
        "127.0.0.1",
        "--server.port",
        "8501",
        "--server.headless",
        "true",
    ]
