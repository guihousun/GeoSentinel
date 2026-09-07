from __future__ import annotations

from types import SimpleNamespace

import pytest
import requests

from monitoring import sources


def test_eonet_uses_bounded_curl_fallback_for_unexpected_tls_eof(monkeypatch) -> None:
    def fail_requests(*args, **kwargs):
        raise requests.exceptions.SSLError("[SSL: UNEXPECTED_EOF_WHILE_READING]")

    captured: dict[str, object] = {}

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        return SimpleNamespace(returncode=0, stdout=b'{"events": []}\n200', stderr=b"")

    monkeypatch.setattr(sources.requests, "get", fail_requests)
    monkeypatch.setattr(sources.subprocess, "run", fake_run)

    response = sources._request(
        "https://eonet.gsfc.nasa.gov/api/v3/events",
        params={"status": "open", "limit": 1},
    )

    assert response.status_code == 200
    assert response.json() == {"events": []}
    assert response.headers["X-GeoSentinel-Transport"] == "curl-http1.1"
    assert captured["command"][0] == "curl.exe"
    assert "--http1.1" in captured["command"]
    assert all("Authorization" not in str(value) for value in captured["command"])


def test_non_eonet_tls_failure_is_not_downgraded(monkeypatch) -> None:
    def fail_requests(*args, **kwargs):
        raise requests.exceptions.SSLError("[SSL: UNEXPECTED_EOF_WHILE_READING]")

    monkeypatch.setattr(sources.requests, "get", fail_requests)
    with pytest.raises(requests.exceptions.SSLError):
        sources._request("https://example.org/events")
