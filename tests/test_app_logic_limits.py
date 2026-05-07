from __future__ import annotations

import importlib
import os
import unittest


class AppLogicLimitTests(unittest.TestCase):
    def setUp(self) -> None:
        self._old_env = {
            "NTL_MAX_ACTIVE_RUNS": os.environ.get("NTL_MAX_ACTIVE_RUNS"),
            "NTL_MAX_ACTIVE_RUNS_PER_USER": os.environ.get("NTL_MAX_ACTIVE_RUNS_PER_USER"),
        }
        os.environ["NTL_MAX_ACTIVE_RUNS"] = "10"
        os.environ["NTL_MAX_ACTIVE_RUNS_PER_USER"] = "2"

        import runtime_governance
        self.runtime_governance = importlib.reload(runtime_governance)

    def tearDown(self) -> None:
        for key, value in self._old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_run_limit_snapshot_reports_global_and_user_counts(self) -> None:
        snapshot = self.runtime_governance.build_run_limit_snapshot(
            [
                {"state": "running", "user_id": "alice"},
                {"state": "running", "user_id": "alice"},
                {"state": "running", "user_id": "bob"},
                {"state": "done", "user_id": "alice"},
            ],
            user_id="alice",
        )

        self.assertEqual(snapshot["global_active"], 3)
        self.assertEqual(snapshot["global_limit"], 10)
        self.assertEqual(snapshot["user_active"], 2)
        self.assertEqual(snapshot["user_limit"], 2)


if __name__ == "__main__":
    unittest.main()
