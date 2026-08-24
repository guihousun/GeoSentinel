from __future__ import annotations

import importlib
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock


class AppLogicSafetyLimitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self._old_env = {
            name: os.environ.get(name)
            for name in (
                "NTL_USER_DATA_DIR",
                "NTL_SHARED_DATA_DIR",
                "NTL_MAX_ACTIVE_RUNS",
                "NTL_MAX_ACTIVE_RUNS_PER_USER",
                "NTL_THREAD_WORKSPACE_QUOTA_MB",
                "NTL_USER_WORKSPACE_QUOTA_MB",
            )
        }
        os.environ["NTL_USER_DATA_DIR"] = str(Path(self.tempdir.name) / "user_data")
        os.environ["NTL_SHARED_DATA_DIR"] = str(Path(self.tempdir.name) / "base_data")
        os.environ["NTL_MAX_ACTIVE_RUNS"] = "2"
        os.environ["NTL_MAX_ACTIVE_RUNS_PER_USER"] = "1"
        os.environ["NTL_THREAD_WORKSPACE_QUOTA_MB"] = "1"
        os.environ["NTL_USER_WORKSPACE_QUOTA_MB"] = "2"
        self.addCleanup(self._restore_env)

        import runtime_governance
        import storage_manager
        import history_store
        import app_logic

        self.runtime_governance = importlib.reload(runtime_governance)
        self.storage_manager_module = importlib.reload(storage_manager)
        self.history_store = importlib.reload(history_store)
        self.app_logic = importlib.reload(app_logic)
        self.storage_manager = self.storage_manager_module.storage_manager

    def _restore_env(self) -> None:
        for name, value in self._old_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    def test_user_run_limit_rejects_when_user_already_has_active_run(self) -> None:
        with self.app_logic._RUN_REGISTRY_LOCK:
            self.app_logic._RUN_REGISTRY.clear()
            self.app_logic._RUN_REGISTRY["run-1"] = {"state": "running", "user_id": "alice"}

            with mock.patch.object(
                self.app_logic,
                "build_run_limit_snapshot",
                return_value={"global_active": 1, "global_limit": 2, "user_active": 1, "user_limit": 1},
            ):
                rejection = self.app_logic._run_limit_rejection_locked("alice")

        self.assertEqual(rejection["reason"], "user_run_limit_reached")
        self.assertEqual(rejection["active_runs"], 1)
        self.assertEqual(rejection["limit"], 1)

    def test_global_run_limit_rejects_when_system_is_full(self) -> None:
        with self.app_logic._RUN_REGISTRY_LOCK:
            self.app_logic._RUN_REGISTRY.clear()
            self.app_logic._RUN_REGISTRY["run-1"] = {"state": "running", "user_id": "alice"}
            self.app_logic._RUN_REGISTRY["run-2"] = {"state": "running", "user_id": "bob"}

            with mock.patch.object(
                self.app_logic,
                "build_run_limit_snapshot",
                return_value={"global_active": 2, "global_limit": 2, "user_active": 0, "user_limit": 1},
            ):
                rejection = self.app_logic._run_limit_rejection_locked("carol")

        self.assertEqual(rejection["reason"], "global_run_limit_reached")
        self.assertEqual(rejection["active_runs"], 2)
        self.assertEqual(rejection["limit"], 2)

    def test_workspace_quota_rejects_thread_before_run(self) -> None:
        workspace = self.storage_manager.get_workspace("alice-a1")
        sample = workspace / "inputs" / "large.bin"
        sample.parent.mkdir(parents=True, exist_ok=True)
        sample.write_bytes(b"a" * (2 * 1024 * 1024))

        rejection = self.app_logic._workspace_quota_rejection("alice-a1", "alice")

        self.assertEqual(rejection["reason"], "thread_workspace_quota_reached")
        self.assertFalse(rejection["started"])
        self.assertGreater(rejection["usage_bytes"], rejection["limit_bytes"])


if __name__ == "__main__":
    unittest.main()
