from __future__ import annotations

import importlib
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock


FERNET_KEY = "1z-yLJFzfZtuWktx3fylUoFZtakFy0AOBbQ-B_WVB8o="


class GeeOAuthTests(unittest.TestCase):
    def setUp(self) -> None:
        self._old_env = {
            key: os.environ.get(key)
            for key in (
                "NTL_USER_DATA_DIR",
                "NTL_HISTORY_DB_URL",
                "NTL_LANGGRAPH_POSTGRES_URL",
                "GEE_DEFAULT_PROJECT_ID",
                "GOOGLE_OAUTH_CLIENT_ID",
                "GOOGLE_OAUTH_CLIENT_SECRET",
                "GOOGLE_OAUTH_REDIRECT_URI",
                "GOOGLE_OAUTH_SCOPES",
                "NTL_TOKEN_ENCRYPTION_KEY",
            )
        }
        self.tempdir = tempfile.TemporaryDirectory()
        base_dir = Path(self.tempdir.name) / "user_data"
        self.db_path = Path(self.tempdir.name) / "history_store_oauth.db"
        os.environ["NTL_USER_DATA_DIR"] = str(base_dir)
        os.environ["NTL_HISTORY_DB_URL"] = f"sqlite:///{self.db_path.as_posix()}"
        os.environ.pop("NTL_LANGGRAPH_POSTGRES_URL", None)
        os.environ["GEE_DEFAULT_PROJECT_ID"] = "default-gee-project"
        os.environ["GOOGLE_OAUTH_CLIENT_ID"] = "client-id"
        os.environ["GOOGLE_OAUTH_CLIENT_SECRET"] = "client-secret"
        os.environ.pop("GOOGLE_OAUTH_REDIRECT_URI", None)
        os.environ["NTL_TOKEN_ENCRYPTION_KEY"] = FERNET_KEY

        import runtime_governance
        import storage_manager
        import history_store
        import gee_auth

        self.runtime_governance = importlib.reload(runtime_governance)
        self.storage_manager = importlib.reload(storage_manager)
        self.history_store = importlib.reload(history_store)
        self.gee_auth = importlib.reload(gee_auth)

    def tearDown(self) -> None:
        for key, value in self._old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        try:
            self.tempdir.cleanup()
        except PermissionError:
            pass

    def _db_row(self, sql: str, params: tuple = ()) -> tuple | None:
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute(sql, params).fetchone()

    def test_oauth_config_defaults_to_localhost_redirect(self) -> None:
        config = self.gee_auth.oauth_config()

        self.assertEqual(config.redirect_uri, "http://localhost:8501")
        self.assertIn("https://www.googleapis.com/auth/earthengine", config.scopes)
        self.assertTrue(self.gee_auth.oauth_configured())

    def test_authorization_url_includes_offline_access_and_state(self) -> None:
        url = self.gee_auth.build_authorization_url("state-123")

        self.assertIn("https://accounts.google.com/o/oauth2/v2/auth", url)
        self.assertIn("access_type=offline", url)
        self.assertIn("prompt=consent", url)
        self.assertIn("state=state-123", url)

    def test_same_tab_redirect_html_uses_location_href(self) -> None:
        html = self.gee_auth.same_tab_redirect_html("https://example.com/oauth?state=abc")

        self.assertIn("window.top.location.assign", html)
        self.assertIn("https://example.com/oauth?state=abc", html)
        self.assertIn("Continue to Google authorization", html)
        self.assertNotIn('target="_blank"', html)

    def test_popup_authorization_html_opens_named_popup(self) -> None:
        html = self.gee_auth.popup_authorization_html("https://example.com/oauth?state=abc")

        self.assertIn("window.open", html)
        self.assertIn("ntl_gee_oauth", html)
        self.assertIn("https://example.com/oauth?state=abc", html)

    def test_oauth_action_label_only_shows_for_user_pipeline(self) -> None:
        self.assertEqual(self.gee_auth.oauth_action_label(False, "user"), "Connect Google")
        self.assertEqual(self.gee_auth.oauth_action_label(True, "user"), "Reconnect Google")
        self.assertEqual(self.gee_auth.oauth_action_label(False, "default"), "")

    def test_popup_close_html_closes_callback_page(self) -> None:
        html = self.gee_auth.popup_close_html("认证成功，请关闭此页面，回到原来的网页。")

        self.assertIn("window.close()", html)
        self.assertIn("认证成功，请关闭此页面，回到原来的网页。", html)

    def test_signed_oauth_state_round_trips_user_id_without_session_state(self) -> None:
        state = self.gee_auth.generate_oauth_state("user-123")

        payload = self.gee_auth.verify_oauth_state(state)

        self.assertEqual(payload["user_id"], "user-123")
        self.assertTrue(payload["nonce"])

    def test_short_project_ids_are_allowed(self) -> None:
        hint = self.gee_auth.gee_project_input_hint("ntlagent")

        self.assertEqual(hint, "")

    def test_encrypts_and_decrypts_refresh_token(self) -> None:
        encrypted = self.gee_auth.encrypt_refresh_token("refresh-token")

        self.assertNotEqual(encrypted, "refresh-token")
        self.assertEqual(self.gee_auth.decrypt_refresh_token(encrypted), "refresh-token")

    def test_stores_encrypted_google_token_on_gee_profile(self) -> None:
        account = self.history_store.register_user("GEEUser", "secure-pass-123")
        encrypted = self.gee_auth.encrypt_refresh_token("refresh-token")

        profile = self.history_store.save_user_gee_oauth_token(
            account["user_id"],
            google_email="user@example.com",
            encrypted_refresh_token=encrypted,
            scopes=["https://www.googleapis.com/auth/earthengine", "email"],
        )

        self.assertEqual(profile["google_email"], "user@example.com")
        self.assertTrue(profile["oauth_connected"])
        self.assertEqual(profile["token_scopes"], "https://www.googleapis.com/auth/earthengine email")
        row = self._db_row(
            "SELECT encrypted_refresh_token FROM user_gee_profiles WHERE user_id = ?",
            (account["user_id"],),
        )
        self.assertNotEqual(row[0], "refresh-token")

    def test_exchange_callback_persists_token_and_email(self) -> None:
        account = self.history_store.register_user("GEEUser", "secure-pass-123")

        with mock.patch.object(self.gee_auth.requests, "post") as mocked_post:
            with mock.patch.object(self.gee_auth.requests, "get") as mocked_get:
                mocked_post.return_value.json.return_value = {
                    "refresh_token": "refresh-token",
                    "access_token": "access-token",
                    "scope": "https://www.googleapis.com/auth/earthengine email",
                }
                mocked_post.return_value.raise_for_status.return_value = None
                mocked_get.return_value.json.return_value = {"email": "user@example.com"}
                mocked_get.return_value.raise_for_status.return_value = None

                state = self.gee_auth.generate_oauth_state(account["user_id"])
                profile = self.gee_auth.complete_oauth_callback(
                    user_id=account["user_id"],
                    code="auth-code",
                    expected_state="",
                    received_state=state,
                    history_store_module=self.history_store,
                )

        self.assertEqual(profile["google_email"], "user@example.com")
        self.assertTrue(profile["oauth_connected"])
        self.assertEqual(self.gee_auth.decrypt_refresh_token(profile["encrypted_refresh_token"]), "refresh-token")

    def test_exchange_code_retries_transient_ssl_failure(self) -> None:
        failed_once = self.gee_auth.requests.exceptions.SSLError("temporary EOF")
        ok_response = mock.Mock()
        ok_response.json.return_value = {"refresh_token": "refresh-token"}
        ok_response.raise_for_status.return_value = None

        with mock.patch.object(self.gee_auth.requests, "post", side_effect=[failed_once, ok_response]) as mocked_post:
            payload = self.gee_auth.exchange_code_for_token("auth-code")

        self.assertEqual(payload["refresh_token"], "refresh-token")
        self.assertEqual(mocked_post.call_count, 2)

    def test_oauth_failure_message_preserves_existing_connection(self) -> None:
        message = self.gee_auth.oauth_failure_message(
            "SSL EOF",
            existing_profile={"oauth_connected": True, "google_email": "user@example.com"},
        )

        self.assertIn("已有 Google 连接仍然保留", message)
        self.assertIn("user@example.com", message)

    def test_code_generation_patches_ee_initialize_when_oauth_token_active(self) -> None:
        from storage_manager import current_gee_encrypted_refresh_token
        from tools import NTL_Code_generation

        encrypted = self.gee_auth.encrypt_refresh_token("refresh-token")
        token = current_gee_encrypted_refresh_token.set(encrypted)
        try:
            patched = NTL_Code_generation._patch_ee_initialize_for_active_credentials(
                "import ee\nee.Initialize(project=project_id)\n"
            )
        finally:
            current_gee_encrypted_refresh_token.reset(token)

        self.assertIn("credentials=ntl_ee_credentials", patched)


if __name__ == "__main__":
    unittest.main()
