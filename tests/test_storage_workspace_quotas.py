import importlib
import os
import tempfile
import unittest
from pathlib import Path


class StorageWorkspaceQuotaTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self._old_env = {name: os.environ.get(name) for name in (
            "NTL_USER_DATA_DIR",
            "NTL_SHARED_DATA_DIR",
            "NTL_THREAD_WORKSPACE_QUOTA_MB",
            "NTL_USER_WORKSPACE_QUOTA_MB",
            "NTL_HISTORY_DB_URL",
        )}
        os.environ["NTL_USER_DATA_DIR"] = str(Path(self.tempdir.name) / "user_data")
        os.environ["NTL_SHARED_DATA_DIR"] = str(Path(self.tempdir.name) / "base_data")
        os.environ["NTL_THREAD_WORKSPACE_QUOTA_MB"] = "1"
        os.environ["NTL_USER_WORKSPACE_QUOTA_MB"] = "2"
        os.environ["NTL_HISTORY_DB_URL"] = ""
        self.addCleanup(self._restore_env)

        import runtime_governance
        import storage_manager
        import history_store

        self.runtime_governance = importlib.reload(runtime_governance)
        self.storage_manager_module = importlib.reload(storage_manager)
        self.history_store = importlib.reload(history_store)
        self.storage_manager = self.storage_manager_module.storage_manager

    def _restore_env(self):
        for name, value in self._old_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    def _write_bytes(self, path: Path, size_bytes: int) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"a" * size_bytes)

    def test_thread_quota_snapshot_rejects_projected_write(self):
        thread_id = "alice-a1"
        workspace = self.storage_manager.get_workspace(thread_id)
        self._write_bytes(workspace / "inputs" / "sample.bin", 800 * 1024)

        snapshot = self.storage_manager.thread_quota_snapshot(thread_id, additional_bytes=300 * 1024)

        self.assertEqual(snapshot["limit_bytes"], 1 * 1024 * 1024)
        self.assertFalse(snapshot["allowed"])
        self.assertGreater(snapshot["projected_bytes"], snapshot["limit_bytes"])

    def test_user_quota_snapshot_counts_multiple_threads(self):
        user_id = "alice"
        thread_ids = ["alice-a1", "alice-b2"]
        self.history_store.bind_thread_to_user(user_id, thread_ids[0])
        self.history_store.bind_thread_to_user(user_id, thread_ids[1])

        workspace_a = self.storage_manager.get_workspace(thread_ids[0])
        workspace_b = self.storage_manager.get_workspace(thread_ids[1])
        self._write_bytes(workspace_a / "outputs" / "a.bin", 700 * 1024)
        self._write_bytes(workspace_b / "outputs" / "b.bin", 900 * 1024)

        resolved_thread_ids = [row["thread_id"] for row in self.history_store.list_user_threads(user_id, limit=0)]
        snapshot = self.storage_manager.user_quota_snapshot(resolved_thread_ids, additional_bytes=500 * 1024)

        self.assertEqual(snapshot["limit_bytes"], 2 * 1024 * 1024)
        self.assertFalse(snapshot["allowed"])
        self.assertGreater(snapshot["usage_bytes"], 1_500 * 1024)
        self.assertGreater(snapshot["projected_bytes"], snapshot["limit_bytes"])


if __name__ == "__main__":
    unittest.main()
