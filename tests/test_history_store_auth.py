from __future__ import annotations

import importlib
import json
import os
import sqlite3
import tempfile
import unittest
from unittest import mock
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
        base_dir = Path(self.tempdir.name) / "user_data"
        self.db_path = Path(self.tempdir.name) / "history_store_auth.db"
        os.environ["NTL_USER_DATA_DIR"] = str(base_dir)
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

    def test_register_and_authenticate_user_with_unique_username(self) -> None:
        account = self.history_store.register_user("TestUser", "secure-pass-123")
        authenticated = self.history_store.authenticate_user("testuser", "secure-pass-123")

        self.assertTrue(account["user_id"])
        self.assertEqual(account["username"], "TestUser")
        self.assertEqual(authenticated["user_id"], account["user_id"])
        self.assertEqual(authenticated["username"], "TestUser")
        self.assertIsNone(self.history_store.authenticate_user("TestUser", "wrong-pass"))
        stored = self._db_row(
            "SELECT username, username_key, password_hash, legacy_migrated_from FROM users WHERE user_id = ?",
            (account["user_id"],),
        )
        self.assertEqual(stored[0], "TestUser")
        self.assertEqual(stored[1], "testuser")
        self.assertNotEqual(stored[2], "secure-pass-123")
        self.assertEqual(stored[3], "")

    def test_duplicate_username_registration_is_rejected(self) -> None:
        self.history_store.register_user("test-user", "secure-pass-123")

        with self.assertRaises(ValueError):
            self.history_store.register_user("TEST-user", "secure-pass-123")

    def test_first_registration_migrates_legacy_history_to_real_user_id_once(self) -> None:
        legacy_user_id = self.history_store.normalize_user_id("TestUser")
        thread_id = self.history_store.generate_thread_id(legacy_user_id)
        self.history_store.ensure_user_profile(legacy_user_id, "TestUser")
        self.history_store.bind_thread_to_user(
            legacy_user_id,
            thread_id,
            meta={"last_question": "wildfire near port"},
        )
        self.history_store.append_chat_record(thread_id, "user", "legacy chat row")
        self.history_store.append_turn_summary(thread_id, {"status": "ok", "question": "legacy summary"})
        self.history_store.save_injected_context_items(
            thread_id,
            [
                {
                    "source_file": "legacy.txt",
                    "signature": "sig-legacy",
                    "chunk_idx": 0,
                    "text": "legacy context",
                    "created_at": 123,
                }
            ],
        )

        account = self.history_store.register_user("TestUser", "secure-pass-123")
        migrated_threads = self.history_store.list_user_threads(account["user_id"], limit=10)
        migrated_chat = self.history_store.load_chat_records(thread_id, limit=10)
        migrated_context = self.history_store.load_injected_context_items(thread_id)

        self.assertEqual(account["legacy_migrated_from"], legacy_user_id)
        self.assertEqual(len(migrated_threads), 1)
        self.assertEqual(migrated_threads[0]["thread_id"], thread_id)
        self.assertEqual(migrated_threads[0]["user_id"], account["user_id"])
        self.assertEqual(migrated_chat[0]["content"], "legacy chat row")
        self.assertEqual(migrated_context[0]["text"], "legacy context")

        legacy_threads = self.history_store.list_user_threads(legacy_user_id, limit=10)
        self.assertEqual(legacy_threads, [])

        users_row = self._db_row(
            "SELECT legacy_migrated_from FROM users WHERE user_id = ?",
            (account["user_id"],),
        )
        self.assertEqual(users_row[0], legacy_user_id)

        with self.assertRaises(ValueError):
            self.history_store.register_user("TestUser", "different-pass-123")

    def test_insert_user_uses_boolean_flag_for_is_active(self) -> None:
        with mock.patch.object(self.history_store, "_db_get_user_by_user_id", return_value={"user_id": "u-1"}):
            with mock.patch.object(self.history_store, "_db_execute") as mocked_execute:
                self.history_store._db_insert_user(
                    user_id="u-1",
                    username="TestUser",
                    username_key="testuser",
                    password_hash="hash",
        )

        params = mocked_execute.call_args.args[2]
        self.assertIs(params[8], True)


if __name__ == "__main__":
    unittest.main()
