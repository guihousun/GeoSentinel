from __future__ import annotations

import csv
import importlib
import json
import os
import tempfile
import unittest
from pathlib import Path


class ConflictNTLCaseBufferComparisonTests(unittest.TestCase):
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
        self.thread_id = "conflict-buffer-compare"
        self.workspace = self.storage_manager.get_workspace(self.thread_id)

    def _restore_env(self) -> None:
        for name, value in self._old_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    def test_compare_case_buffers_reports_radius_density_and_risk(self) -> None:
        top_path = self.workspace / "inputs" / "top_candidates.csv"
        with top_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "event_id",
                    "event_date_utc",
                    "country",
                    "city",
                    "event_type",
                    "site_type",
                    "site_subtype",
                    "source_quality",
                    "latitude",
                    "longitude",
                ],
            )
            writer.writeheader()
            writer.writerows(
                [
                    {
                        "event_id": "a",
                        "event_date_utc": "2026-03-01",
                        "country": "Iran",
                        "city": "Demo",
                        "event_type": "Confirmed Airstrike",
                        "site_type": "civilian",
                        "site_subtype": "civilian : oil infrastructure",
                        "source_quality": "strong",
                        "latitude": "27.0",
                        "longitude": "52.0",
                    },
                    {
                        "event_id": "b",
                        "event_date_utc": "2026-03-01",
                        "country": "Iran",
                        "city": "Demo",
                        "event_type": "Explosion",
                        "site_type": "energy",
                        "site_subtype": "civilian : oil infrastructure",
                        "source_quality": "reference_plus_leads",
                        "latitude": "27.01",
                        "longitude": "52.0",
                    },
                    {
                        "event_id": "far",
                        "event_date_utc": "2026-03-01",
                        "country": "Iran",
                        "city": "Demo",
                        "event_type": "Explosion",
                        "site_type": "military",
                        "site_subtype": "military : airbase",
                        "source_quality": "weak_lead",
                        "latitude": "27.15",
                        "longitude": "52.0",
                    },
                ]
            )

        result = self.conflict_ntl.run_conflict_ntl_compare_case_buffers(
            top_candidates_csv_path="inputs/top_candidates.csv",
            cases_json=json.dumps([{"case_id": "energy_demo", "event_ids": ["a", "b"]}]),
            buffer_radii_m="2000,5000,20000",
            run_label="buffer_case",
            config={"configurable": {"thread_id": self.thread_id}},
        )

        self.assertEqual(result["status"], "complete")
        with open(result["output_files"]["buffer_comparison_csv"], encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
        self.assertEqual([int(r["radius_m"]) for r in rows], [2000, 5000, 20000])
        self.assertEqual(rows[0]["included_event_count"], "2")
        self.assertEqual(rows[2]["included_event_count"], "3")
        self.assertGreater(float(rows[0]["event_density_per_km2"]), float(rows[2]["event_density_per_km2"]))
        self.assertEqual(rows[0]["dominant_signal_hypothesis"], "industrial_energy_signal")
        self.assertEqual(rows[2]["background_dilution_risk"], "high")

        md = Path(result["output_files"]["buffer_comparison_md"]).read_text(encoding="utf-8")
        self.assertIn("energy_demo", md)
        self.assertIn("industrial_energy_signal", md)


if __name__ == "__main__":
    unittest.main()
