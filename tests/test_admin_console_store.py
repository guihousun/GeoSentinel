from __future__ import annotations

import importlib
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path


class AdminConsoleStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self._old_env = {
            key: os.environ.get(key)
            for key in (
                "NTL_USER_DATA_DIR",
                "NTL_HISTORY_DB_URL",
                "NTL_LANGGRAPH_POSTGRES_URL",
                "NTL_ADMIN_USERNAMES",
                "GEE_DEFAULT_PROJECT_ID",
            )
        }
        self.tempdir = tempfile.TemporaryDirectory()
        base_dir = Path(self.tempdir.name) / "user_data"
        self.db_path = Path(self.tempdir.name) / "history_store_admin.db"
        os.environ["NTL_USER_DATA_DIR"] = str(base_dir)
        os.environ["NTL_HISTORY_DB_URL"] = f"sqlite:///{self.db_path.as_posix()}"
        os.environ.pop("NTL_LANGGRAPH_POSTGRES_URL", None)
        os.environ["NTL_ADMIN_USERNAMES"] = "AdminUser"
        os.environ["GEE_DEFAULT_PROJECT_ID"] = "default-project"

        import storage_manager
        import history_store

        self.storage_manager = importlib.reload(storage_manager)
        self.history_store = importlib.reload(history_store)

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

    def _count_audit_rows(self) -> int:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT COUNT(*) FROM admin_audit_logs").fetchone()
        return int(row[0] or 0)

    def test_admin_bootstrap_from_env_marks_registered_user_admin(self) -> None:
        account = self.history_store.register_user("AdminUser", "secure-pass-123")
        authenticated = self.history_store.authenticate_user("adminuser", "secure-pass-123")

        self.assertEqual(account["role"], "admin")
        self.assertTrue(account["is_admin"])
        self.assertEqual(authenticated["role"], "admin")
        self.assertTrue(self.history_store.is_admin_user(account["user_id"]))

    def test_existing_user_is_promoted_by_admin_env_on_setup(self) -> None:
        os.environ["NTL_ADMIN_USERNAMES"] = ""
        user = self.history_store.register_user("LaterAdmin", "secure-pass-123")
        self.assertEqual(user["role"], "user")

        os.environ["NTL_ADMIN_USERNAMES"] = "LaterAdmin"
        self.history_store._DB_READY.clear()
        self.history_store._db_setup()

        promoted = self.history_store.get_registered_user(user["user_id"])
        self.assertEqual(promoted["role"], "admin")
        self.assertTrue(self.history_store.is_admin_user(user["user_id"]))

    def test_disabled_user_cannot_authenticate(self) -> None:
        admin = self.history_store.register_user("AdminUser", "secure-pass-123")
        user = self.history_store.register_user("BetaUser", "secure-pass-123")

        updated = self.history_store.set_user_disabled(
            user["user_id"],
            disabled=True,
            reason="quota abuse",
            admin_user_id=admin["user_id"],
        )

        self.assertFalse(updated["is_active"])
        self.assertEqual(updated["disabled_reason"], "quota abuse")
        self.assertIsNone(self.history_store.authenticate_user("BetaUser", "secure-pass-123"))
        self.assertEqual(self._count_audit_rows(), 1)

    def test_admin_user_list_includes_thread_and_gee_status(self) -> None:
        self.history_store.register_user("AdminUser", "secure-pass-123")
        user = self.history_store.register_user("BetaUser", "secure-pass-123")
        self.history_store.bind_thread_to_user(user["user_id"], "beta-thread-1")
        self.history_store.bind_thread_to_user(user["user_id"], "beta-thread-2")
        self.history_store.save_user_gee_profile(
            user["user_id"],
            mode="user",
            gee_project_id="user-gee-project",
            status="validated",
        )

        rows = self.history_store.list_admin_users(limit=20)
        beta = next(row for row in rows if row["username"] == "BetaUser")

        self.assertEqual(beta["thread_count"], 2)
        self.assertEqual(beta["gee_mode"], "user")
        self.assertEqual(beta["gee_project_id"], "user-gee-project")
        self.assertEqual(beta["gee_status"], "validated")
        self.assertFalse(beta["oauth_connected"])

    def test_admin_can_reset_user_gee_pipeline_and_audit_it(self) -> None:
        admin = self.history_store.register_user("AdminUser", "secure-pass-123")
        user = self.history_store.register_user("BetaUser", "secure-pass-123")
        self.history_store.save_user_gee_oauth_token(
            user["user_id"],
            google_email="beta@example.com",
            encrypted_refresh_token="encrypted-token",
            scopes=["email", "https://www.googleapis.com/auth/earthengine"],
        )
        self.history_store.save_user_gee_profile(
            user["user_id"],
            mode="user",
            gee_project_id="user-gee-project",
            status="validated",
        )

        profile = self.history_store.reset_user_gee_pipeline(
            user["user_id"],
            admin_user_id=admin["user_id"],
            reason="user requested reset",
        )

        self.assertEqual(profile["source"], "default")
        self.assertEqual(profile["gee_project_id"], "")
        self.assertEqual(profile["google_email"], "")
        self.assertFalse(profile["oauth_connected"])
        self.assertEqual(self._count_audit_rows(), 1)


if __name__ == "__main__":
    unittest.main()
