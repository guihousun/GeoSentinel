from __future__ import annotations

import importlib
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path


class GeeProfileTests(unittest.TestCase):
    def setUp(self) -> None:
        self._old_env = {
            key: os.environ.get(key)
            for key in (
                "NTL_USER_DATA_DIR",
                "NTL_HISTORY_DB_URL",
                "NTL_LANGGRAPH_POSTGRES_URL",
                "GEE_DEFAULT_PROJECT_ID",
            )
        }
        self.tempdir = tempfile.TemporaryDirectory()
        base_dir = Path(self.tempdir.name) / "user_data"
        self.db_path = Path(self.tempdir.name) / "history_store_gee.db"
        os.environ["NTL_USER_DATA_DIR"] = str(base_dir)
        os.environ["NTL_HISTORY_DB_URL"] = f"sqlite:///{self.db_path.as_posix()}"
        os.environ.pop("NTL_LANGGRAPH_POSTGRES_URL", None)
        os.environ["GEE_DEFAULT_PROJECT_ID"] = "default-gee-project"

        import runtime_governance
        import storage_manager
        import history_store

        self.runtime_governance = importlib.reload(runtime_governance)
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

    def test_default_gee_profile_uses_hosted_project(self) -> None:
        account = self.history_store.register_user("GEEUser", "secure-pass-123")

        profile = self.history_store.get_user_gee_profile(account["user_id"])

        self.assertEqual(profile["mode"], "default")
        self.assertEqual(profile["effective_project_id"], "default-gee-project")
        self.assertEqual(profile["source"], "default")
        self.assertFalse(profile["user_project_configured"])

    def test_user_gee_profile_can_be_saved_and_selected(self) -> None:
        account = self.history_store.register_user("GEEUser", "secure-pass-123")

        saved = self.history_store.save_user_gee_profile(
            account["user_id"],
            mode="user",
            gee_project_id="my-user-project",
            status="validated",
            last_error="",
        )
        loaded = self.history_store.get_user_gee_profile(account["user_id"])

        self.assertEqual(saved["mode"], "user")
        self.assertEqual(loaded["mode"], "user")
        self.assertEqual(loaded["gee_project_id"], "my-user-project")
        self.assertEqual(loaded["effective_project_id"], "my-user-project")
        self.assertEqual(loaded["source"], "user")
        self.assertTrue(loaded["user_project_configured"])
        row = self._db_row(
            "SELECT mode, gee_project_id, status FROM user_gee_profiles WHERE user_id = ?",
            (account["user_id"],),
        )
        self.assertEqual(row, ("user", "my-user-project", "validated"))

    def test_user_mode_without_project_falls_back_to_default(self) -> None:
        account = self.history_store.register_user("GEEUser", "secure-pass-123")
        self.history_store.save_user_gee_profile(account["user_id"], mode="user", gee_project_id="")

        profile = self.history_store.get_user_gee_profile(account["user_id"])

        self.assertEqual(profile["mode"], "default")
        self.assertEqual(profile["effective_project_id"], "default-gee-project")
        self.assertEqual(profile["source"], "default")

    def test_run_metadata_includes_gee_profile(self) -> None:
        metadata = self.runtime_governance.build_runtime_metadata(
            user_id="u-1",
            thread_id="t-1",
            gee_pipeline_mode="user",
            gee_project_id="my-user-project",
            gee_profile_source="user",
        )

        self.assertEqual(metadata["gee_pipeline_mode"], "user")
        self.assertEqual(metadata["gee_project_id"], "my-user-project")
        self.assertEqual(metadata["gee_profile_source"], "user")

    def test_code_generation_prefers_thread_local_gee_project(self) -> None:
        from storage_manager import current_gee_project_id
        from tools import NTL_Code_generation

        NTL_Code_generation = importlib.reload(NTL_Code_generation)
        token = current_gee_project_id.set("thread-local-project")
        try:
            self.assertEqual(NTL_Code_generation._gee_project_id(), "thread-local-project")
        finally:
            current_gee_project_id.reset(token)


if __name__ == "__main__":
    unittest.main()
