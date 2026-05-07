from __future__ import annotations

import importlib
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path


class AdminLocalServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self._old_env = {
            key: os.environ.get(key)
            for key in (
                "NTL_USER_DATA_DIR",
                "NTL_SHARED_DATA_DIR",
                "NTL_HISTORY_DB_URL",
                "NTL_LANGGRAPH_POSTGRES_URL",
                "NTL_ADMIN_USERNAMES",
                "NTL_THREAD_WORKSPACE_QUOTA_MB",
                "NTL_USER_WORKSPACE_QUOTA_MB",
                "NTL_LOCAL_ADMIN_ACTOR",
            )
        }
        self.tempdir = tempfile.TemporaryDirectory()
        base_dir = Path(self.tempdir.name) / "user_data"
        shared_dir = Path(self.tempdir.name) / "base_data"
        self.db_path = Path(self.tempdir.name) / "admin_local.db"
        os.environ["NTL_USER_DATA_DIR"] = str(base_dir)
        os.environ["NTL_SHARED_DATA_DIR"] = str(shared_dir)
        os.environ["NTL_HISTORY_DB_URL"] = f"sqlite:///{self.db_path.as_posix()}"
        os.environ.pop("NTL_LANGGRAPH_POSTGRES_URL", None)
        os.environ["NTL_ADMIN_USERNAMES"] = "AdminUser"
        os.environ["NTL_THREAD_WORKSPACE_QUOTA_MB"] = "500"
        os.environ["NTL_USER_WORKSPACE_QUOTA_MB"] = "1024"
        os.environ["NTL_LOCAL_ADMIN_ACTOR"] = "local_admin"

        import runtime_governance
        import storage_manager
        import history_store
        import admin_local_service

        self.runtime_governance = importlib.reload(runtime_governance)
        self.storage_manager_module = importlib.reload(storage_manager)
        self.history_store = importlib.reload(history_store)
        self.admin_service = importlib.reload(admin_local_service)
        self.storage_manager = self.storage_manager_module.storage_manager

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

    def _write_bytes(self, path: Path, size_bytes: int) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x" * size_bytes)

    def _audit_count(self) -> int:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT COUNT(*) FROM admin_audit_logs").fetchone()
        return int(row[0] or 0)

    def test_user_detail_reports_workspace_usage(self) -> None:
        self.history_store.register_user("AdminUser", "secure-pass-123")
        user = self.history_store.register_user("BetaUser", "secure-pass-123")
        self.history_store.bind_thread_to_user(user["user_id"], "beta-thread-1")
        workspace = self.storage_manager.get_workspace("beta-thread-1")
        self._write_bytes(workspace / "outputs" / "result.bin", 128 * 1024)

        detail = self.admin_service.get_user_detail(user["user_id"])

        self.assertEqual(detail["user"]["username"], "BetaUser")
        self.assertEqual(len(detail["threads"]), 1)
        self.assertGreater(detail["workspace_usage_bytes"], 120 * 1024)
        self.assertTrue(detail["workspace_limit_label"].endswith("GB"))

    def test_clear_thread_section_removes_outputs_and_audits(self) -> None:
        self.history_store.register_user("AdminUser", "secure-pass-123")
        user = self.history_store.register_user("BetaUser", "secure-pass-123")
        self.history_store.bind_thread_to_user(user["user_id"], "beta-thread-1")
        workspace = self.storage_manager.get_workspace("beta-thread-1")
        self._write_bytes(workspace / "outputs" / "result.bin", 64 * 1024)
        self._write_bytes(workspace / "inputs" / "input.bin", 8 * 1024)

        result = self.admin_service.clear_thread_section(user["user_id"], "beta-thread-1", "outputs")

        self.assertEqual(result["section"], "outputs")
        self.assertTrue((workspace / "inputs" / "input.bin").exists())
        self.assertFalse((workspace / "outputs" / "result.bin").exists())
        self.assertEqual(self._audit_count(), 1)

    def test_delete_thread_as_admin_removes_workspace_and_index(self) -> None:
        self.history_store.register_user("AdminUser", "secure-pass-123")
        user = self.history_store.register_user("BetaUser", "secure-pass-123")
        self.history_store.bind_thread_to_user(user["user_id"], "beta-thread-1")
        workspace = self.storage_manager.get_workspace("beta-thread-1")
        self._write_bytes(workspace / "memory" / "note.txt", 1024)

        result = self.admin_service.delete_thread_as_admin(user["user_id"], "beta-thread-1")

        self.assertTrue(result["deleted"])
        self.assertFalse(workspace.exists())
        self.assertEqual(self.history_store.list_user_threads(user["user_id"], limit=0), [])
        self.assertEqual(self._audit_count(), 1)

    def test_reset_user_password_invalidates_old_password_and_audits_without_secret(self) -> None:
        admin = self.history_store.register_user("AdminUser", "secure-pass-123")
        user = self.history_store.register_user("BetaUser", "old-pass-123")

        updated = self.admin_service.reset_user_password_as_admin(
            user["user_id"],
            "new-pass-456",
            admin_user_id=admin["user_id"],
            reason="user forgot password",
        )

        self.assertEqual(updated["user_id"], user["user_id"])
        self.assertIsNone(self.history_store.authenticate_user("BetaUser", "old-pass-123"))
        self.assertIsNotNone(self.history_store.authenticate_user("BetaUser", "new-pass-456"))
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT action, reason, payload_json FROM admin_audit_logs").fetchone()
        self.assertEqual(row[0], "reset_user_password")
        self.assertEqual(row[1], "user forgot password")
        self.assertNotIn("new-pass-456", row[2])


if __name__ == "__main__":
    unittest.main()
