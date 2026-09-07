from __future__ import annotations

import os

import run_web


def test_launcher_configures_ssl_before_loading_uvicorn(monkeypatch) -> None:
    events = []
    calls = []

    class FakeUvicorn:
        @staticmethod
        def run(app: str, **kwargs) -> None:
            events.append("run")
            calls.append((app, kwargs))

    monkeypatch.setattr(
        run_web,
        "configure_outbound_ssl",
        lambda: events.append("ssl") or "CERTIFI_FALLBACK",
    )
    monkeypatch.setattr(
        run_web,
        "_load_uvicorn",
        lambda: events.append("import") or FakeUvicorn,
    )

    run_web.main(["--host", "127.0.0.1", "--port", "8502"])

    assert events == ["ssl", "import", "run"]
    assert calls == [
        (
            "web_api:app",
            {
                "host": "127.0.0.1",
                "port": 8502,
                "reload": False,
                "proxy_headers": True,
                "forwarded_allow_ips": "127.0.0.1",
                "log_level": "info",
            },
        )
    ]


def test_dev_http_flag_sets_process_override(monkeypatch) -> None:
    class FakeUvicorn:
        @staticmethod
        def run(_app: str, **_kwargs) -> None:
            return None

    monkeypatch.delenv("NTL_WEB_FORCE_DEV_HTTP", raising=False)
    monkeypatch.setattr(run_web, "configure_outbound_ssl", lambda: "SYSTEM_DEFAULT")
    monkeypatch.setattr(run_web, "_load_uvicorn", lambda: FakeUvicorn)

    run_web.main(["--dev-http"])

    assert os.environ["NTL_WEB_FORCE_DEV_HTTP"] == "1"
