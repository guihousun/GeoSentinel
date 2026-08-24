from __future__ import annotations

import importlib
import json
import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


class StorageWorkspaceQuotaTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self._old_env = {
            name: os.environ.get(name)
            for name in (
                "NTL_USER_DATA_DIR",
                "NTL_SHARED_DATA_DIR",
                "NTL_THREAD_WORKSPACE_QUOTA_MB",
                "NTL_USER_WORKSPACE_QUOTA_MB",
            )
        }
        os.environ["NTL_USER_DATA_DIR"] = str(Path(self.tempdir.name) / "user_data")
        os.environ["NTL_SHARED_DATA_DIR"] = str(Path(self.tempdir.name) / "base_data")
        os.environ["NTL_THREAD_WORKSPACE_QUOTA_MB"] = "1"
        os.environ["NTL_USER_WORKSPACE_QUOTA_MB"] = "2"
        self.addCleanup(self._restore_env)

        import runtime_governance
        import storage_manager
        import history_store

        self.runtime_governance = importlib.reload(runtime_governance)
        self.storage_manager_module = importlib.reload(storage_manager)
        self.history_store = importlib.reload(history_store)
        self.storage_manager = self.storage_manager_module.storage_manager

    def _restore_env(self) -> None:
        for name, value in self._old_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    def _write_bytes(self, path: Path, size_bytes: int) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"a" * size_bytes)

    def test_thread_quota_snapshot_rejects_projected_write(self) -> None:
        thread_id = "alice-a1"
        workspace = self.storage_manager.get_workspace(thread_id)
        self._write_bytes(workspace / "inputs" / "sample.bin", 800 * 1024)

        snapshot = self.storage_manager.thread_quota_snapshot(thread_id, additional_bytes=300 * 1024)

        self.assertEqual(snapshot["limit_bytes"], 1 * 1024 * 1024)
        self.assertFalse(snapshot["allowed"])
        self.assertGreater(snapshot["projected_bytes"], snapshot["limit_bytes"])

    def test_user_quota_snapshot_counts_multiple_threads(self) -> None:
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

    def test_nested_paths_are_preserved_and_output_roots_are_enforced(self) -> None:
        thread_id = "alice-nested"
        workspace = self.storage_manager.get_workspace(thread_id)
        nested_input = workspace / "inputs" / "boundaries" / "districts.shp"
        nested_input.parent.mkdir(parents=True, exist_ok=True)
        nested_input.write_bytes(b"shape")

        self.assertEqual(
            Path(self.storage_manager.resolve_input_path("boundaries/districts.shp", thread_id)),
            nested_input.resolve(),
        )
        self.assertEqual(
            Path(self.storage_manager.resolve_output_path("tables/result.csv", thread_id)),
            (workspace / "outputs" / "tables" / "result.csv").resolve(),
        )
        self.assertEqual(
            Path(self.storage_manager.resolve_input_path("", thread_id)),
            (workspace / "inputs").resolve(),
        )
        with self.assertRaises(PermissionError):
            self.storage_manager.resolve_output_path("inputs/not-an-output.txt", thread_id)
        with self.assertRaises(PermissionError):
            self.storage_manager.resolve_output_path("/data/raw/not-an-output.txt", thread_id)

    def test_thread_ids_and_windows_absolute_paths_cannot_escape_workspace(self) -> None:
        with self.assertRaises(ValueError):
            self.storage_manager.get_workspace("../outside")
        with self.assertRaises(ValueError):
            self.storage_manager.get_workspace("nested/thread")
        with self.assertRaises(ValueError):
            self.storage_manager.resolve_output_path(r"C:\outside\result.txt", "alice-safe")

    def test_atomic_write_rejects_quota_overflow_without_partial_file(self) -> None:
        thread_id = "alice-atomic-quota"
        workspace = self.storage_manager.get_workspace(thread_id)
        self._write_bytes(workspace / "inputs" / "existing.bin", 900 * 1024)

        with self.assertRaises(self.storage_manager_module.StorageQuotaExceededError):
            self.storage_manager.atomic_write_text(
                "too-large.txt",
                "x" * (200 * 1024),
                thread_id=thread_id,
            )

        self.assertFalse((workspace / "outputs" / "too-large.txt").exists())
        self.assertEqual(list((workspace / "outputs").glob(".too-large.txt.*.tmp")), [])

    def test_jsonl_appends_are_valid_under_thread_concurrency(self) -> None:
        thread_id = "alice-jsonl"

        def append_event(index: int) -> None:
            self.storage_manager.append_jsonl(
                "events.jsonl",
                {"index": index},
                thread_id=thread_id,
            )

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(append_event, range(40)))

        history_path = self.storage_manager.get_workspace(thread_id) / "outputs" / "events.jsonl"
        records = [json.loads(line) for line in history_path.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(len(records), 40)
        self.assertEqual({record["index"] for record in records}, set(range(40)))


if __name__ == "__main__":
    unittest.main()
