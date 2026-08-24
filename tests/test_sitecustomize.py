from __future__ import annotations

import importlib

import sitecustomize
import ssl_compat


def test_sitecustomize_configures_ssl_during_import(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(
        ssl_compat,
        "configure_outbound_ssl",
        lambda: calls.append(True) or ssl_compat.SSL_MODE_SYSTEM,
    )

    reloaded = importlib.reload(sitecustomize)

    assert calls == [True]
    assert reloaded.SSL_MODE == ssl_compat.SSL_MODE_SYSTEM
