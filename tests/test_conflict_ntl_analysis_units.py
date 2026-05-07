from __future__ import annotations

import csv
import importlib
import json
import os
import tempfile
import unittest
from pathlib import Path


class ConflictNTLAnalysisUnitsTests(unittest.TestCase):
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
        self.thread_id = "conflict-units"
        self.workspace = self.storage_manager.get_workspace(self.thread_id)

    def _restore_env(self) -> None:
        for name, value in self._old_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    def _write_screened(self, rows: list[dict[str, object]]) -> str:
        path = self.workspace / "inputs" / "screened.csv"
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=sorted({k for row in rows for k in row}))
            writer.writeheader()
            writer.writerows(rows)
        return "inputs/screened.csv"

    def _generate(self, rows: list[dict[str, object]]) -> dict[str, object]:
        return self.conflict_ntl.run_conflict_ntl_generate_analysis_units(
            screened_events_path=self._write_screened(rows),
            run_label="units_case",
            config={"configurable": {"thread_id": self.thread_id}},
        )

    def _read_csv(self, path: str) -> list[dict[str, str]]:
        with open(path, encoding="utf-8", newline="") as f:
            return list(csv.DictReader(f))

    def test_exact_point_generates_two_and_five_km_buffer_aois(self) -> None:
        result = self._generate(
            [
                {
                    "event_id": "e1",
                    "event_date_utc": "2026-03-01",
                    "latitude": "35.7",
                    "longitude": "51.4",
                    "coord_type": "exact",
                    "round1_event_candidate_status": "event_candidate",
                    "ntl_relevance_level": "ntl_applicable",
                }
            ]
        )

        self.assertEqual(result["status"], "complete")
        queue = json.loads(Path(result["output_files"]["task_queue"]).read_text(encoding="utf-8"))
        buffer_tasks = [t for t in queue["tasks"] if t["aoi_type"] == "buffer"]
        self.assertEqual([t["radius_m"] for t in buffer_tasks], [2000, 5000])

        aois = json.loads(Path(result["output_files"]["candidate_aois"]).read_text(encoding="utf-8"))
        self.assertEqual(len(aois["features"]), 2)

    def test_same_day_same_radius_overlapping_buffers_merge(self) -> None:
        result = self._generate(
            [
                {
                    "event_id": "e1",
                    "event_date_utc": "2026-03-01",
                    "latitude": "35.7000",
                    "longitude": "51.4000",
                    "coord_type": "exact",
                    "round1_event_candidate_status": "event_candidate",
                    "ntl_relevance_level": "ntl_applicable",
                },
                {
                    "event_id": "e2",
                    "event_date_utc": "2026-03-01",
                    "latitude": "35.7005",
                    "longitude": "51.4005",
                    "coord_type": "exact",
                    "round1_event_candidate_status": "event_candidate",
                    "ntl_relevance_level": "ntl_applicable",
                },
            ]
        )

        units = self._read_csv(result["output_files"]["analysis_units_csv"])
        buffer_units = [u for u in units if u["unit_type"] == "buffer_overlap_day"]
        self.assertEqual(len(buffer_units), 2)
        self.assertTrue(all(int(u["source_event_count"]) == 2 for u in buffer_units))

    def test_different_dates_do_not_merge(self) -> None:
        result = self._generate(
            [
                {
                    "event_id": "e1",
                    "event_date_utc": "2026-03-01",
                    "latitude": "35.7",
                    "longitude": "51.4",
                    "coord_type": "exact",
                    "round1_event_candidate_status": "event_candidate",
                    "ntl_relevance_level": "ntl_applicable",
                },
                {
                    "event_id": "e2",
                    "event_date_utc": "2026-03-02",
                    "latitude": "35.7005",
                    "longitude": "51.4005",
                    "coord_type": "exact",
                    "round1_event_candidate_status": "event_candidate",
                    "ntl_relevance_level": "ntl_applicable",
                },
            ]
        )

        units = self._read_csv(result["output_files"]["analysis_units_csv"])
        buffer_units = [u for u in units if u["unit_type"] == "buffer_overlap_day"]
        self.assertEqual(len(buffer_units), 4)
        self.assertTrue(all(int(u["source_event_count"]) == 1 for u in buffer_units))

    def test_non_exact_precision_enters_admin_aoi_queue(self) -> None:
        result = self._generate(
            [
                {
                    "event_id": "e1",
                    "event_date_utc": "2026-03-01",
                    "latitude": "35.7",
                    "longitude": "51.4",
                    "coord_type": "pov",
                    "admin_iso3": "IRN",
                    "admin_level": "ADM2",
                    "admin_id": "tehran",
                    "round1_event_candidate_status": "event_candidate",
                    "ntl_relevance_level": "ntl_applicable",
                },
                {
                    "event_id": "e2",
                    "event_date_utc": "2026-03-01",
                    "latitude": "35.8",
                    "longitude": "51.5",
                    "coord_type": "general town",
                    "admin_iso3": "IRN",
                    "admin_level": "ADM2",
                    "admin_id": "tehran",
                    "round1_event_candidate_status": "event_candidate",
                    "ntl_relevance_level": "ntl_applicable",
                },
            ]
        )

        queue = json.loads(Path(result["output_files"]["task_queue"]).read_text(encoding="utf-8"))
        admin_tasks = [t for t in queue["tasks"] if t["aoi_type"] == "admin"]
        self.assertEqual(len(admin_tasks), 2)

        units = self._read_csv(result["output_files"]["analysis_units_csv"])
        admin_units = [u for u in units if u["unit_type"] == "admin_day"]
        self.assertEqual(len(admin_units), 1)
        self.assertEqual(admin_units[0]["source_event_ids"], "e1;e2")

    def test_analysis_units_accepts_screening_tool_absolute_output_path(self) -> None:
        raw_events = self.workspace / "inputs" / "events.csv"
        with raw_events.open("w", encoding="utf-8", newline="") as f:
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
                    "source_1",
                ],
            )
            writer.writeheader()
            writer.writerow(
                {
                    "event_id": "chain1",
                    "event_date_utc": "2026-03-01",
                    "latitude": "35.7",
                    "longitude": "51.4",
                    "coord_type": "exact",
                    "event_type": "Confirmed Airstrike",
                    "site_type": "refinery",
                    "source_1": "https://www.reuters.com/world/example",
                }
            )
        screened = self.conflict_ntl.run_conflict_ntl_screen_events(
            events_path="inputs/events.csv",
            run_label="chain_screen",
            config={"configurable": {"thread_id": self.thread_id}},
        )

        result = self.conflict_ntl.run_conflict_ntl_generate_analysis_units(
            screened_events_path=screened["output_files"]["screened_events"],
            run_label="chain_units",
            config={"configurable": {"thread_id": self.thread_id}},
        )

        self.assertEqual(result["summary"]["eligible_event_count"], 1)


if __name__ == "__main__":
    unittest.main()
