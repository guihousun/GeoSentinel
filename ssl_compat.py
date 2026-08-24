from __future__ import annotations

import os
import ssl
import sys
from collections.abc import Callable
from pathlib import Path


SSL_MODE_SYSTEM = "SYSTEM_DEFAULT"
SSL_MODE_CERTIFI = "CERTIFI_FALLBACK"

_SYSTEM_CREATE_DEFAULT_CONTEXT = ssl.create_default_context
_fallback_active = False
_certifi_cafile: str | None = None


def _is_windows_store_asn1_error(exc: BaseException) -> bool:
    message = str(exc).upper()
    return isinstance(exc, ssl.SSLError) and "ASN1" in message and "NOT_ENOUGH_DATA" in message


def _get_certifi_cafile() -> str:
    import certifi

    path = Path(certifi.where()).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"certifi CA bundle not found: {path}")
    return str(path)


def _make_certifi_context_factory(
    factory: Callable[..., ssl.SSLContext],
    cafile: str,
) -> Callable[..., ssl.SSLContext]:
    def create_default_context(
        purpose: ssl.Purpose = ssl.Purpose.SERVER_AUTH,
        *,
        cafile: str | None = None,
        capath: str | None = None,
        cadata: str | bytes | None = None,
    ) -> ssl.SSLContext:
        if cafile is None and capath is None and cadata is None:
            cafile = _certifi_cafile or cafile_default
        return factory(purpose=purpose, cafile=cafile, capath=capath, cadata=cadata)

    cafile_default = cafile
    create_default_context.__name__ = "create_default_context"
    create_default_context.__doc__ = factory.__doc__
    return create_default_context


def configure_outbound_ssl(*, platform_name: str | None = None) -> str:
    """Use certifi only when Windows cannot parse its certificate store.

    TLS verification remains enabled. Non-Windows platforms, healthy Windows
    certificate stores, and unrelated SSL failures keep their normal behavior.
    """
    global _certifi_cafile, _fallback_active

    if _fallback_active:
        if _certifi_cafile:
            os.environ["SSL_CERT_FILE"] = _certifi_cafile
            os.environ.pop("SSL_CERT_DIR", None)
        return SSL_MODE_CERTIFI

    current_platform = platform_name or sys.platform
    if current_platform != "win32":
        return SSL_MODE_SYSTEM

    try:
        _SYSTEM_CREATE_DEFAULT_CONTEXT()
    except ssl.SSLError as exc:
        if not _is_windows_store_asn1_error(exc):
            raise
    else:
        return SSL_MODE_SYSTEM

    certifi_cafile = _get_certifi_cafile()
    _SYSTEM_CREATE_DEFAULT_CONTEXT(cafile=certifi_cafile)

    _certifi_cafile = certifi_cafile
    fallback_factory = _make_certifi_context_factory(
        _SYSTEM_CREATE_DEFAULT_CONTEXT,
        certifi_cafile,
    )
    ssl.create_default_context = fallback_factory
    ssl._create_default_https_context = fallback_factory
    os.environ["SSL_CERT_FILE"] = certifi_cafile
    os.environ.pop("SSL_CERT_DIR", None)
    _fallback_active = True
    return SSL_MODE_CERTIFI
