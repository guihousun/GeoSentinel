from __future__ import annotations

import os
import base64
import hashlib
import hmac
import json
import html
import secrets
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import requests
from cryptography.fernet import Fernet
from google.oauth2.credentials import Credentials


GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo"
DEFAULT_REDIRECT_URI = "http://localhost:8501"
DEFAULT_SCOPES = (
    "openid",
    "email",
    "profile",
    "https://www.googleapis.com/auth/earthengine",
    "https://www.googleapis.com/auth/cloud-platform",
)


@dataclass(frozen=True)
class GoogleOAuthConfig:
    client_id: str
    client_secret: str
    redirect_uri: str
    scopes: tuple[str, ...]


def _env_text(name: str) -> str:
    return str(os.getenv(name, "") or "").strip()


def oauth_config() -> GoogleOAuthConfig:
    scopes_raw = _env_text("GOOGLE_OAUTH_SCOPES")
    scopes = tuple(scopes_raw.split()) if scopes_raw else DEFAULT_SCOPES
    return GoogleOAuthConfig(
        client_id=_env_text("GOOGLE_OAUTH_CLIENT_ID"),
        client_secret=_env_text("GOOGLE_OAUTH_CLIENT_SECRET"),
        redirect_uri=_env_text("GOOGLE_OAUTH_REDIRECT_URI") or DEFAULT_REDIRECT_URI,
        scopes=scopes,
    )


def oauth_configured() -> bool:
    config = oauth_config()
    return bool(config.client_id and config.client_secret and _env_text("NTL_TOKEN_ENCRYPTION_KEY"))


def oauth_action_label(oauth_connected: bool, mode: str = "user") -> str:
    if str(mode or "").strip() != "user":
        return ""
    return "Reconnect Google" if oauth_connected else "Connect Google"


def _state_signing_key() -> bytes:
    key = _env_text("NTL_TOKEN_ENCRYPTION_KEY") or _env_text("GOOGLE_OAUTH_CLIENT_SECRET")
    if not key:
        raise RuntimeError("NTL_TOKEN_ENCRYPTION_KEY or GOOGLE_OAUTH_CLIENT_SECRET is required for OAuth state.")
    return key.encode("utf-8")


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _b64url_decode(data: str) -> bytes:
    padded = str(data or "") + "=" * (-len(str(data or "")) % 4)
    return base64.urlsafe_b64decode(padded.encode("utf-8"))


def generate_oauth_state(user_id: str = "") -> str:
    payload = {
        "user_id": str(user_id or "").strip(),
        "nonce": secrets.token_urlsafe(18),
        "ts": int(time.time()),
    }
    payload_blob = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signature = hmac.new(_state_signing_key(), payload_blob.encode("utf-8"), hashlib.sha256).digest()
    return f"{payload_blob}.{_b64url_encode(signature)}"


def verify_oauth_state(state: str, max_age_seconds: int = 1800) -> dict[str, Any]:
    try:
        payload_blob, signature_blob = str(state or "").split(".", 1)
    except ValueError as exc:
        raise ValueError("Invalid Google OAuth state.") from exc
    expected = hmac.new(_state_signing_key(), payload_blob.encode("utf-8"), hashlib.sha256).digest()
    received = _b64url_decode(signature_blob)
    if not hmac.compare_digest(expected, received):
        raise ValueError("Invalid Google OAuth state signature.")
    payload = json.loads(_b64url_decode(payload_blob).decode("utf-8"))
    ts = int(payload.get("ts") or 0)
    if max_age_seconds and ts and int(time.time()) - ts > max_age_seconds:
        raise ValueError("Google OAuth state expired.")
    return dict(payload)


def gee_project_input_hint(project_id: str) -> str:
    value = str(project_id or "").strip()
    return ""


def build_authorization_url(state: str) -> str:
    config = oauth_config()
    params = {
        "client_id": config.client_id,
        "redirect_uri": config.redirect_uri,
        "response_type": "code",
        "scope": " ".join(config.scopes),
        "state": str(state or ""),
        "access_type": "offline",
        "prompt": "consent",
        "include_granted_scopes": "true",
    }
    return f"{GOOGLE_AUTH_URL}?{urlencode(params)}"


def same_tab_redirect_html(url: str) -> str:
    safe_url = html.escape(str(url or ""), quote=True)
    return f"""
<script>
try {{
  window.top.location.assign("{safe_url}");
}} catch (err) {{
  window.location.assign("{safe_url}");
}}
</script>
<div style="font-family:sans-serif;font-size:14px;color:#dbe7ff;">
Redirecting to Google authorization...
<br>
<a href="{safe_url}" style="color:#cfe2ff;">Continue to Google authorization</a>
</div>
"""


def popup_authorization_html(url: str) -> str:
    safe_url = html.escape(str(url or ""), quote=True)
    return f"""
<script>
const oauthUrl = "{safe_url}";
const popup = window.open(
  oauthUrl,
  "ntl_gee_oauth",
  "popup=yes,width=720,height=780,noopener=no,noreferrer=no"
);
if (popup) {{
  popup.focus();
}}
</script>
<div style="font-family:sans-serif;font-size:14px;color:#dbe7ff;">
Google authorization opened in a popup.
<br>
<a href="{safe_url}" target="ntl_gee_oauth" style="color:#cfe2ff;">Open Google authorization</a>
</div>
"""


def popup_close_html(message: str = "Google Earth Engine connected.") -> str:
    safe_message = html.escape(str(message or "Google Earth Engine connected."), quote=False)
    return f"""
<script>
setTimeout(() => window.close(), 500);
</script>
<div style="font-family:sans-serif;padding:18px;color:#0f172a;">
{safe_message}
<br>
You can close this window and return to 地缘环境智能计算平台.
</div>
"""


def _fernet() -> Fernet:
    key = _env_text("NTL_TOKEN_ENCRYPTION_KEY")
    if not key:
        raise RuntimeError("NTL_TOKEN_ENCRYPTION_KEY is required for Google OAuth token storage.")
    return Fernet(key.encode("utf-8"))


def encrypt_refresh_token(refresh_token: str) -> str:
    token = str(refresh_token or "").strip()
    if not token:
        raise ValueError("refresh_token is empty.")
    return _fernet().encrypt(token.encode("utf-8")).decode("utf-8")


def decrypt_refresh_token(encrypted_refresh_token: str) -> str:
    token = str(encrypted_refresh_token or "").strip()
    if not token:
        return ""
    return _fernet().decrypt(token.encode("utf-8")).decode("utf-8")


def exchange_code_for_token(code: str) -> dict[str, Any]:
    config = oauth_config()
    payload = {
        "code": str(code or "").strip(),
        "client_id": config.client_id,
        "client_secret": config.client_secret,
        "redirect_uri": config.redirect_uri,
        "grant_type": "authorization_code",
    }
    last_exc: Exception | None = None
    for attempt in range(2):
        try:
            response = requests.post(GOOGLE_TOKEN_URL, data=payload, timeout=30)
            break
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            if attempt == 1:
                raise
            time.sleep(0.5)
    else:
        raise last_exc or RuntimeError("Google OAuth token exchange failed.")
    response.raise_for_status()
    return dict(response.json())


def oauth_failure_message(error: Any, existing_profile: dict[str, Any] | None = None) -> str:
    text = str(error or "").strip()
    profile = existing_profile or {}
    if profile.get("oauth_connected"):
        email = str(profile.get("google_email") or "Google account").strip()
        return f"认证重试失败，但已有 Google 连接仍然保留：{email}。你可以关闭此页面，回到原页面继续操作。错误：{text}"
    return f"认证失败：{text}"


def fetch_google_user_email(access_token: str) -> str:
    response = requests.get(
        GOOGLE_USERINFO_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    return str(payload.get("email") or "").strip()


def credentials_from_refresh_token(refresh_token: str, scopes: list[str] | tuple[str, ...] | None = None) -> Credentials:
    config = oauth_config()
    return Credentials(
        token=None,
        refresh_token=str(refresh_token or "").strip(),
        token_uri=GOOGLE_TOKEN_URL,
        client_id=config.client_id,
        client_secret=config.client_secret,
        scopes=list(scopes or config.scopes),
    )


def complete_oauth_callback(
    *,
    user_id: str,
    code: str,
    expected_state: str,
    received_state: str,
    history_store_module: Any,
) -> dict[str, Any]:
    state_user_id = ""
    if received_state and "." in str(received_state):
        state_payload = verify_oauth_state(received_state)
        state_user_id = str(state_payload.get("user_id") or "").strip()
    elif not expected_state or not received_state or str(expected_state) != str(received_state):
        raise ValueError("Google OAuth state mismatch.")
    effective_user_id = state_user_id or str(user_id or "").strip()
    token_payload = exchange_code_for_token(code)
    refresh_token = str(token_payload.get("refresh_token") or "").strip()
    if not refresh_token:
        raise RuntimeError("Google OAuth did not return a refresh_token. Reconnect with consent prompt.")
    access_token = str(token_payload.get("access_token") or "").strip()
    google_email = fetch_google_user_email(access_token) if access_token else ""
    scopes = str(token_payload.get("scope") or " ".join(oauth_config().scopes)).split()
    encrypted = encrypt_refresh_token(refresh_token)
    return history_store_module.save_user_gee_oauth_token(
        effective_user_id,
        google_email=google_email,
        encrypted_refresh_token=encrypted,
        scopes=scopes,
    )

