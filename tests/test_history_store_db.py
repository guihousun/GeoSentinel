from __future__ import annotations

import importlib
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path


class HistoryStoreDbTests(unittest.TestCase):
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
        self.db_path = Path(self.tempdir.name) / "history_store.db"
        os.environ["NTL_USER_DATA_DIR"] = str(base_dir)
        os.environ["NTL_HISTORY_DB_URL"] = f"sqlite:///{self.db_path.as_posix()}"
        os.environ.pop("NTL_LANGGRAPH_POSTGRES_URL", None)

        import storage_manager
        import history_store

        self.storage_manager = importlib.reload(storage_manager)
        self.history_store = importlib.reload(history_store)

    def _db_count(self, table_name: str) -> int:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        return int(row[0] or 0)

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

    def test_migrates_existing_thread_index_and_chat_history_into_database(self) -> None:
        user_id = "alice"
        thread_id = "alice-flood"
        user_dir = self.history_store.USERS_DIR / user_id
        user_dir.mkdir(parents=True, exist_ok=True)
        (user_dir / "threads_index.json").write_text(
            json.dumps(
                {
                    "threads": [
                        {
                            "thread_id": thread_id,
                            "created_at": 100,
                            "updated_at": 120,
                            "workspace": "user_data/alice-flood",
                            "last_question": "wildfire",
                        }
                    ]
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        history_dir = self.history_store.BASE_DIR / thread_id / "history"
        history_dir.mkdir(parents=True, exist_ok=True)
        (history_dir / "chat_history.jsonl").write_text(
            json.dumps({"ts": 111, "role": "user", "kind": "text", "content": "flood prompt"}, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

        rows = self.history_store.list_user_threads(user_id, limit=10)
        chat = self.history_store.load_chat_records(thread_id, limit=10)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["thread_id"], thread_id)
        self.assertEqual(chat[0]["content"], "flood prompt")
        self.assertTrue(self.db_path.exists())
        self.assertEqual(self._db_count("user_threads"), 1)
        self.assertEqual(self._db_count("chat_records"), 1)

    def test_db_backed_history_round_trip_for_context_and_turn_summary(self) -> None:
        thread_id = "alice-conflict"
        self.history_store.append_turn_summary(thread_id, {"status": "success", "question": "conflict"})
        merged = self.history_store.upsert_injected_context_items(
            thread_id,
            [
                {
                    "source_file": "damage_map.png",
                    "signature": "sig-1",
                    "chunk_idx": 0,
                    "text": "port damage visible",
                    "file_type": "image",
                    "created_at": 123,
                }
            ],
        )

        context_rows = self.history_store.load_injected_context_items(thread_id)
        overview = self.history_store.injected_file_overview(thread_id)

        self.assertEqual(merged["inserted"], 1)
        self.assertEqual(context_rows[0]["text"], "port damage visible")
        self.assertEqual(overview[0]["source_file"], "damage_map.png")
        self.assertEqual(self._db_count("turn_summaries"), 1)
        self.assertEqual(self._db_count("injected_context"), 1)

    def test_load_turn_summaries_restores_reasoning_logs(self) -> None:
        thread_id = "alice-reasoning"
        self.history_store.append_turn_summary(
            thread_id,
            {
                "status": "success",
                "question": "nighttime light trend",
                "analysis_logs": [{"custom": [{"event_type": "kb_progress", "step": "search"}]}],
            },
        )

        rows = self.history_store.load_turn_summaries(thread_id, limit=12)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["question"], "nighttime light trend")
        self.assertEqual(rows[0]["analysis_logs"][0]["custom"][0]["step"], "search")

    def test_first_user_question_generates_thread_title_once(self) -> None:
        user_id = "alice"
        thread_id = "alice-flood"
        self.history_store.bind_thread_to_user(user_id, thread_id)

        self.history_store.touch_thread_activity(
            user_id,
            thread_id,
            last_question="Wildfire intensity analysis for Los Angeles urban corridor",
        )
        self.history_store.touch_thread_activity(
            user_id,
            thread_id,
            last_question="Second question should not replace the first title",
        )

        rows = self.history_store.list_user_threads(user_id, limit=10)
        self.assertEqual(rows[0]["thread_title"], "Wildfire intensity analysis for Los Angeles urban corridor")
        self.assertEqual(rows[0]["thread_title_manual"], 0)

    def test_manual_thread_rename_persists_and_blocks_auto_title_overwrite(self) -> None:
        user_id = "alice"
        thread_id = "alice-eq"
        self.history_store.bind_thread_to_user(user_id, thread_id)
        self.history_store.touch_thread_activity(user_id, thread_id, last_question="Earthquake baseline title")

        self.history_store.rename_user_thread(user_id, thread_id, "Custom incident review")
        self.history_store.touch_thread_activity(
            user_id,
            thread_id,
            last_question="Later question should not overwrite custom title",
        )

        rows = self.history_store.list_user_threads(user_id, limit=10)
        self.assertEqual(rows[0]["thread_title"], "Custom incident review")
        self.assertEqual(rows[0]["thread_title_manual"], 1)

    def test_binding_existing_thread_without_meta_preserves_manual_title(self) -> None:
        user_id = "alice"
        thread_id = "alice-ntl"
        self.history_store.bind_thread_to_user(user_id, thread_id)
        self.history_store.rename_user_thread(user_id, thread_id, "Manual NTL title")

        self.history_store.bind_thread_to_user(user_id, thread_id)

        rows = self.history_store.list_user_threads(user_id, limit=10)
        self.assertEqual(rows[0]["thread_title"], "Manual NTL title")
        self.assertEqual(rows[0]["thread_title_manual"], 1)


if __name__ == "__main__":
    unittest.main()
