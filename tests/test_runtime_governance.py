from __future__ import annotations

import os
import unittest

from runtime_governance import (
    ASSISTANT_ID,
    build_runtime_metadata,
    deepagents_memory_namespace,
    max_active_runs,
    max_active_runs_per_user,
)


class _Runtime:
    def __init__(self, config):
        self.config = config
        self.context = None


class RuntimeGovernanceTests(unittest.TestCase):
    def tearDown(self) -> None:
        for name in (
            "NTL_MEMORY_NAMESPACE_SCOPE",
            "NTL_MAX_ACTIVE_RUNS",
            "NTL_MAX_ACTIVE_RUNS_PER_USER",
        ):
            os.environ.pop(name, None)

    def test_thread_scoped_namespace_isolates_users_and_threads(self) -> None:
        first = _Runtime(
            {
                "metadata": build_runtime_metadata(
                    assistant_id=ASSISTANT_ID,
                    user_id="alice",
                    thread_id="alice-flood",
                )
            }
        )
        second = _Runtime(
            {
                "metadata": build_runtime_metadata(
                    assistant_id=ASSISTANT_ID,
                    user_id="alice",
                    thread_id="alice-wildfire",
                )
            }
        )

        self.assertEqual(
            deepagents_memory_namespace(first),
            ("NTL_Engineer", "alice", "alice-flood"),
        )
        self.assertEqual(
            deepagents_memory_namespace(second),
            ("NTL_Engineer", "alice", "alice-wildfire"),
        )
        self.assertNotEqual(
            deepagents_memory_namespace(first),
            deepagents_memory_namespace(second),
        )

    def test_user_scoped_namespace_can_share_memory_across_threads(self) -> None:
        os.environ["NTL_MEMORY_NAMESPACE_SCOPE"] = "user"
        runtime = _Runtime(
            {
                "metadata": build_runtime_metadata(
                    assistant_id=ASSISTANT_ID,
                    user_id="alice",
                    thread_id="alice-conflict",
                )
            }
        )

        self.assertEqual(deepagents_memory_namespace(runtime), ("NTL_Engineer", "alice"))

    def test_active_run_limits_have_safe_defaults_and_env_overrides(self) -> None:
        self.assertEqual(max_active_runs(), 10)
        self.assertEqual(max_active_runs_per_user(), 2)

        os.environ["NTL_MAX_ACTIVE_RUNS"] = "0"
        os.environ["NTL_MAX_ACTIVE_RUNS_PER_USER"] = "9"

        self.assertEqual(max_active_runs(), 0)
        self.assertEqual(max_active_runs_per_user(), 9)


if __name__ == "__main__":
    unittest.main()
