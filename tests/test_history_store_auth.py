from __future__ import annotations

import importlib
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path


class HistoryStoreAuthTests(unittest.TestCase):
    def setUp(self) -> None:
        self._old_env = {
            key: os.environ.get(key)
            for key in (
                "NTL_USER_DATA_DIR",
                "NTL_HISTORY_DB_URL",
                "NTL_LANGGRAPH_POSTGRES_URL",
            )
        }
        self.tempdir = tempfile.TemporaryDirectory()
        os.environ["NTL_USER_DATA_DIR"] = str(Path(self.tempdir.name) / "user_data")
        self.db_path = Path(self.tempdir.name) / "history_store_auth.db"
        os.environ["NTL_HISTORY_DB_URL"] = f"sqlite:///{self.db_path.as_posix()}"
        os.environ.pop("NTL_LANGGRAPH_POSTGRES_URL", None)

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

    def _db_row(self, sql: str, params: tuple = ()) -> tuple | None:
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute(sql, params).fetchone()

    def test_register_and_authenticate_user_with_hashed_password(self) -> None:
        account = self.history_store.register_user("TestUser", "secure-pass-123")
        authenticated = self.history_store.authenticate_user("testuser", "secure-pass-123")

        self.assertTrue(account["user_id"])
        self.assertEqual(account["username"], "TestUser")
        self.assertEqual(authenticated["user_id"], account["user_id"])
        self.assertEqual(authenticated["username"], "TestUser")
        self.assertIsNone(self.history_store.authenticate_user("TestUser", "wrong-pass"))

        stored = self._db_row(
            "SELECT username, username_key, password_hash, last_login_at FROM users WHERE user_id = ?",
            (account["user_id"],),
        )
        self.assertEqual(stored[0], "TestUser")
        self.assertEqual(stored[1], "testuser")
        self.assertNotEqual(stored[2], "secure-pass-123")
        self.assertTrue(stored[3])

    def test_duplicate_username_registration_is_rejected_case_insensitively(self) -> None:
        self.history_store.register_user("test-user", "secure-pass-123")

        with self.assertRaises(ValueError):
            self.history_store.register_user("TEST-user", "secure-pass-123")

    def test_registration_rejects_reserved_or_short_credentials(self) -> None:
        with self.assertRaises(ValueError):
            self.history_store.register_user("guest", "secure-pass-123")

        with self.assertRaises(ValueError):
            self.history_store.register_user("ok-user", "short")


if __name__ == "__main__":
    unittest.main()
