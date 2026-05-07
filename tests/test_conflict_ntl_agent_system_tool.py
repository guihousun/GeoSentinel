from __future__ import annotations

import csv
import importlib
import json
import os
import tempfile
import unittest
from pathlib import Path


class ConflictNTLAgentSystemToolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self._old_env = {
            "NTL_USER_DATA_DIR": os.environ.get("NTL_USER_DATA_DIR"),
            "NTL_SHARED_DATA_DIR": os.environ.get("NTL_SHARED_DATA_DIR"),
        }
        os.environ["NTL_USER_DATA_DIR"] = str(Path(self.tempdir.name) / "user_data")
        os.environ["NTL_SHARED_DATA_DIR"] = str(Path(self.tempdir.name) / "base_data")
        self.addCleanup(self._restore_env)

        import storage_manager
        from tools import conflict_ntl

        self.storage_manager_module = importlib.reload(storage_manager)
        self.conflict_ntl = importlib.reload(conflict_ntl)
        self.storage_manager = self.storage_manager_module.storage_manager
        self.thread_id = "conflict-agent-system"
        self.workspace = self.storage_manager.get_workspace(self.thread_id)

    def _restore_env(self) -> None:
        for name, value in self._old_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    def _write_events(self) -> str:
        path = self.workspace / "inputs" / "events.csv"
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "event_id",
                    "event_date_utc",
                    "latitude",
                    "longitude",
                    "coord_type",
                    "event_type",
                    "site_type",
                    "site_subtype",
                    "source_1",
                ],
            )
            writer.writeheader()
            writer.writerows(
                [
                    {
                        "event_id": "e1",
                        "event_date_utc": "2026-03-01",
                        "latitude": "35.7",
                        "longitude": "51.4",
                        "coord_type": "exact",
                        "event_type": "Confirmed Airstrike",
                        "site_type": "energy",
                        "site_subtype": "oil infrastructure",
                        "source_1": "https://www.reuters.com/world/middle-east/example",
                    },
                    {
                        "event_id": "e2",
                        "event_date_utc": "2026-03-01",
                        "latitude": "35.705",
                        "longitude": "51.405",
                        "coord_type": "exact",
                        "event_type": "Reported Airstrike",
                        "site_type": "military",
                        "site_subtype": "airbase",
                        "source_1": "https://www.bbc.com/news/example",
                    },
                    {
                        "event_id": "e3",
                        "event_date_utc": "2026-03-01",
                        "latitude": "35.9",
                        "longitude": "51.9",
                        "coord_type": "exact",
                        "event_type": "Air Defense Activity",
                        "site_type": "unknown",
                        "site_subtype": "unknown target",
                        "source_1": "https://x.com/example/status/1",
                    },
                ]
            )
        return "inputs/events.csv"

    def test_agent_system_tool_chains_core_conflict_ntl_stages(self) -> None:
        result = self.conflict_ntl.run_conflict_ntl_agent_system(
            events_path=self._write_events(),
            case_name="Demo conflict agent run",
            run_label="agent_case",
            buffer_radii_m="2000,5000",
            config={"configurable": {"thread_id": self.thread_id}},
        )

        self.assertEqual(result["schema"], "conflict_ntl.agent_system_run.v1")
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["summary"]["input_records"], 3)
        self.assertEqual(result["summary"]["top_candidates"], 2)
        self.assertEqual(result["summary"]["analysis_units"], 2)
        self.assertIn("ConflictNTL-Commander", result["agent_roles"])
        self.assertIn("Conflict-Searcher", result["agent_roles"])
        self.assertIn("Data-Searcher", result["agent_roles"])
        self.assertIn("Conflict-Analyst", result["agent_roles"])
        self.assertTrue(result["stages"]["screen_events"]["output_files"]["screened_events"].endswith(".csv"))
        self.assertNotIn("compare_case_buffers", result["stages"])
        self.assertNotIn("source_freshness", result["stages"])

        manifest_path = Path(result["output_files"]["agent_system_manifest"])
        runbook_path = Path(result["output_files"]["agent_system_runbook"])
        self.assertTrue(manifest_path.exists())
        self.assertTrue(runbook_path.exists())
        self.assertIn(str(self.workspace / "outputs"), str(manifest_path))
        self.assertNotIn("confirmed damage", runbook_path.read_text(encoding="utf-8").lower())

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(manifest["handoff_contract"]["ntl_product"], "NASA/VIIRS/002/VNP46A2")
        self.assertIn("requires_independent_validation", manifest["interpretation_guardrails"]["required_caveats"])

    def test_registered_as_lazy_ntl_gpt_tool_export(self) -> None:
        import tools

        self.assertIn("conflict_ntl_agent_system_tool", tools.__all__)
        tool = getattr(tools, "conflict_ntl_agent_system_tool")
        self.assertEqual(tool.name, "conflict_ntl_agent_system_tool")


if __name__ == "__main__":
    unittest.main()
