from __future__ import annotations

import importlib
import os
import tempfile
import unittest
from pathlib import Path

from langchain_core.messages import AIMessage, message_to_dict


class AppStateReasoningTests(unittest.TestCase):
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
        import app_state

        self.storage_manager = importlib.reload(storage_manager)
        self.history_store = importlib.reload(history_store)
        self.app_state = importlib.reload(app_state)

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

    def test_load_analysis_history_for_thread_rehydrates_messages(self) -> None:
        thread_id = "alice-reasoning"
        self.history_store.append_turn_summary(
            thread_id,
            {
                "question": "Analyze flood exposure",
                "analysis_logs": [
                    {
                        "messages": [
                            message_to_dict(AIMessage(content="Use VIIRS first", name="Data_Searcher"))
                        ]
                    }
                ],
            },
        )

        rows = self.app_state._load_analysis_history_for_thread(thread_id)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["question"], "Analyze flood exposure")
        self.assertEqual(rows[0]["logs"][0]["messages"][0].content, "Use VIIRS first")
        self.assertEqual(rows[0]["logs"][0]["messages"][0].name, "Data_Searcher")


if __name__ == "__main__":
    unittest.main()
