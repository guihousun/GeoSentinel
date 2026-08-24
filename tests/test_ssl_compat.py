from __future__ import annotations

import os
import ssl

import pytest

import ssl_compat


@pytest.fixture(autouse=True)
def _reset_ssl_compat(monkeypatch):
    monkeypatch.setattr(ssl_compat, "_fallback_active", False)
    monkeypatch.setattr(ssl_compat, "_certifi_cafile", None)
    monkeypatch.setattr(ssl, "create_default_context", ssl_compat._SYSTEM_CREATE_DEFAULT_CONTEXT)
    monkeypatch.setattr(ssl, "_create_default_https_context", ssl_compat._SYSTEM_CREATE_DEFAULT_CONTEXT)


def test_healthy_windows_store_keeps_system_default(monkeypatch):
    calls = []

    def healthy_factory(*args, **kwargs):
        calls.append((args, kwargs))
        return object()

    monkeypatch.setattr(ssl_compat, "_SYSTEM_CREATE_DEFAULT_CONTEXT", healthy_factory)

    mode = ssl_compat.configure_outbound_ssl(platform_name="win32")

    assert mode == ssl_compat.SSL_MODE_SYSTEM
    assert calls == [((), {})]
    assert ssl.create_default_context is not healthy_factory


def test_windows_asn1_failure_uses_certifi_with_verification(monkeypatch, tmp_path):
    ca_bundle = tmp_path / "cacert.pem"
    ca_bundle.write_text("test-ca", encoding="ascii")
    calls = []

    def failing_system_factory(*args, **kwargs):
        calls.append((args, kwargs))
        if not kwargs.get("cafile"):
            raise ssl.SSLError("[ASN1: NOT_ENOUGH_DATA] not enough data")
        return kwargs

    monkeypatch.setattr(ssl_compat, "_SYSTEM_CREATE_DEFAULT_CONTEXT", failing_system_factory)
    monkeypatch.setattr(ssl_compat, "_get_certifi_cafile", lambda: str(ca_bundle))
    monkeypatch.setenv("SSL_CERT_DIR", "broken-store")

    mode = ssl_compat.configure_outbound_ssl(platform_name="win32")
    context = ssl.create_default_context()

    assert mode == ssl_compat.SSL_MODE_CERTIFI
    assert context["cafile"] == str(ca_bundle)
    assert os.environ["SSL_CERT_FILE"] == str(ca_bundle)
    assert "SSL_CERT_DIR" not in os.environ
    assert calls[0] == ((), {})
    assert calls[1][1]["cafile"] == str(ca_bundle)

    monkeypatch.setenv("SSL_CERT_FILE", "overridden-after-startup")
    monkeypatch.setenv("SSL_CERT_DIR", "broken-store-again")
    assert ssl_compat.configure_outbound_ssl(platform_name="win32") == ssl_compat.SSL_MODE_CERTIFI
    assert os.environ["SSL_CERT_FILE"] == str(ca_bundle)
    assert "SSL_CERT_DIR" not in os.environ


def test_certifi_fallback_preserves_explicit_ca_settings(monkeypatch, tmp_path):
    ca_bundle = tmp_path / "cacert.pem"
    ca_bundle.write_text("test-ca", encoding="ascii")
    custom_bundle = tmp_path / "custom.pem"
    custom_bundle.write_text("custom-ca", encoding="ascii")

    def factory(*args, **kwargs):
        if not kwargs.get("cafile"):
            raise ssl.SSLError("[ASN1: NOT_ENOUGH_DATA] not enough data")
        return kwargs

    monkeypatch.setattr(ssl_compat, "_SYSTEM_CREATE_DEFAULT_CONTEXT", factory)
    monkeypatch.setattr(ssl_compat, "_get_certifi_cafile", lambda: str(ca_bundle))
    ssl_compat.configure_outbound_ssl(platform_name="win32")

    context = ssl.create_default_context(cafile=str(custom_bundle))

    assert context["cafile"] == str(custom_bundle)


def test_unrelated_ssl_error_is_not_hidden(monkeypatch):
    def failing_factory(*args, **kwargs):
        raise ssl.SSLError("certificate verify failed")

    monkeypatch.setattr(ssl_compat, "_SYSTEM_CREATE_DEFAULT_CONTEXT", failing_factory)

    with pytest.raises(ssl.SSLError, match="certificate verify failed"):
        ssl_compat.configure_outbound_ssl(platform_name="win32")


def test_non_windows_does_not_probe_certificate_store(monkeypatch):
    def unexpected_factory(*args, **kwargs):
        raise AssertionError("certificate store should not be probed")

    monkeypatch.setattr(ssl_compat, "_SYSTEM_CREATE_DEFAULT_CONTEXT", unexpected_factory)

    assert ssl_compat.configure_outbound_ssl(platform_name="linux") == ssl_compat.SSL_MODE_SYSTEM
