from __future__ import annotations

import csv
import importlib
import json
import os
import tempfile
import unittest
from pathlib import Path


class ConflictNTLScreeningTests(unittest.TestCase):
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
        self.thread_id = "conflict-screening"
        self.workspace = self.storage_manager.get_workspace(self.thread_id)

    def _restore_env(self) -> None:
        for name, value in self._old_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    def _write_events(self, rows: list[dict[str, object]]) -> str:
        path = self.workspace / "inputs" / "events.csv"
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=sorted({k for row in rows for k in row}))
            writer.writeheader()
            writer.writerows(rows)
        return "inputs/events.csv"

    def _screen(self, rows: list[dict[str, object]]) -> dict[str, object]:
        return self.conflict_ntl.run_conflict_ntl_screen_events(
            events_path=self._write_events(rows),
            run_label="screening_case",
            config={"configurable": {"thread_id": self.thread_id}},
        )

    def _read_csv(self, path: str) -> list[dict[str, str]]:
        with open(path, encoding="utf-8", newline="") as f:
            return list(csv.DictReader(f))

    def test_strong_source_exact_fixed_target_is_ntl_applicable_candidate(self) -> None:
        result = self._screen(
            [
                {
                    "event_id": "e1",
                    "event_date_utc": "2026-03-01",
                    "latitude": 35.7,
                    "longitude": 51.4,
                    "coord_type": "exact",
                    "event_type": "Confirmed Airstrike",
                    "site_type": "refinery",
                    "source_1": "https://www.reuters.com/world/middle-east/example",
                }
            ]
        )

        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["summary"]["round1_status_counts"]["event_candidate"], 1)
        self.assertEqual(result["summary"]["ntl_relevance_counts"]["ntl_applicable"], 1)

        rows = self._read_csv(result["output_files"]["screened_events"])
        self.assertEqual(rows[0]["round1_event_candidate_status"], "event_candidate")
        self.assertEqual(rows[0]["ntl_relevance_level"], "ntl_applicable")
        self.assertEqual(rows[0]["conflict_ntl_candidate"], "true")

    def test_missing_coordinates_requires_geocoding(self) -> None:
        result = self._screen(
            [
                {
                    "event_id": "e2",
                    "event_date_utc": "2026-03-02",
                    "event_type": "Confirmed Airstrike",
                    "site_type": "airport",
                    "source_1": "https://www.bbc.com/news/example",
                }
            ]
        )

        rows = self._read_csv(result["output_files"]["screened_events"])
        self.assertEqual(rows[0]["round1_event_candidate_status"], "needs_geocoding")
        self.assertEqual(rows[0]["coord_quality"], "missing_coordinates")

    def test_weak_source_gets_caveat_without_confirming_event(self) -> None:
        result = self._screen(
            [
                {
                    "event_id": "e3",
                    "event_date_utc": "2026-03-03",
                    "latitude": 35.7,
                    "longitude": 51.4,
                    "coord_type": "exact",
                    "event_type": "Explosion report",
                    "site_type": "fuel depot",
                    "source_1": "https://x.com/example/status/1",
                }
            ]
        )

        rows = self._read_csv(result["output_files"]["screened_events"])
        self.assertEqual(rows[0]["source_quality"], "weak_lead")
        self.assertEqual(rows[0]["round1_event_candidate_status"], "event_candidate")
        self.assertEqual(rows[0]["event_confirmation_status"], "not_confirmed_by_screening")
        self.assertIn("not confirmation", rows[0]["verification_notes"])

    def test_air_defense_unknown_target_is_ntl_uncertain(self) -> None:
        result = self._screen(
            [
                {
                    "event_id": "e4",
                    "event_date_utc": "2026-03-04",
                    "latitude": 35.7,
                    "longitude": 51.4,
                    "coord_type": "exact",
                    "event_type": "Air Defense Activity",
                    "site_type": "unknown",
                    "source_1": "https://apnews.com/article/example",
                }
            ]
        )

        rows = self._read_csv(result["output_files"]["screened_events"])
        self.assertEqual(rows[0]["ntl_relevance_level"], "ntl_uncertain")
        self.assertEqual(rows[0]["conflict_ntl_candidate"], "false")

    def test_flood_and_wildfire_variations_do_not_use_conflict_fixed_target_rule(self) -> None:
        result = self._screen(
            [
                {
                    "event_id": "flood1",
                    "event_date_utc": "2026-03-05",
                    "latitude": 30.1,
                    "longitude": 31.2,
                    "coord_type": "exact",
                    "event_type": "flood",
                    "site_type": "urban fixed target",
                    "source_1": "https://reliefweb.int/report/example",
                },
                {
                    "event_id": "fire1",
                    "event_date_utc": "2026-03-06",
                    "latitude": 30.1,
                    "longitude": 31.2,
                    "coord_type": "exact",
                    "event_type": "wildfire",
                    "site_type": "power station",
                    "source_1": "https://firms.modaps.eosdis.nasa.gov/example",
                },
            ]
        )

        rows = self._read_csv(result["output_files"]["screened_events"])
        self.assertEqual(rows[0]["ntl_relevance_level"], "out_of_scope_non_conflict")
        self.assertEqual(rows[1]["ntl_relevance_level"], "out_of_scope_non_conflict")
        self.assertEqual(result["summary"]["ntl_relevance_counts"]["out_of_scope_non_conflict"], 2)

    def test_conflict_fixed_security_military_political_and_transport_targets_are_applicable(self) -> None:
        result = self._screen(
            [
                {
                    "event_id": "sec1",
                    "event_date_utc": "2026-03-07",
                    "latitude": 35.7,
                    "longitude": 51.4,
                    "coord_type": "exact",
                    "event_type": "Confirmed Airstrike",
                    "site_type": "internal security",
                    "site_subtype": "internal security : police",
                    "source_1": "https://www.reuters.com/world/example",
                },
                {
                    "event_id": "mil1",
                    "event_date_utc": "2026-03-07",
                    "latitude": 35.8,
                    "longitude": 51.5,
                    "coord_type": "exact",
                    "event_type": "Missile strike",
                    "site_type": "military",
                    "site_subtype": "military : airbase",
                    "source_1": "https://apnews.com/article/example",
                },
                {
                    "event_id": "pol1",
                    "event_date_utc": "2026-03-07",
                    "latitude": 35.9,
                    "longitude": 51.6,
                    "coord_type": "exact",
                    "event_type": "Reported Airstrike",
                    "site_type": "political",
                    "site_subtype": "political : administrative",
                    "source_1": "https://www.bbc.com/news/example",
                },
                {
                    "event_id": "rail1",
                    "event_date_utc": "2026-03-07",
                    "latitude": 36.0,
                    "longitude": 51.7,
                    "coord_type": "exact",
                    "event_type": "Drone attack",
                    "site_type": "civilian",
                    "site_subtype": "civilian : railway infrastructure",
                    "source_1": "https://www.cnn.com/world/example",
                },
            ]
        )

        rows = self._read_csv(result["output_files"]["screened_events"])
        self.assertEqual({r["event_id"]: r["ntl_relevance_level"] for r in rows}, {
            "sec1": "ntl_applicable",
            "mil1": "ntl_applicable",
            "pol1": "ntl_applicable",
            "rail1": "ntl_applicable",
        })

    def test_json_input_is_supported(self) -> None:
        events_path = self.workspace / "inputs" / "events.json"
        events_path.write_text(
            json.dumps(
                [
                    {
                        "event_id": "json1",
                        "event_date_utc": "2026-03-07",
                        "latitude": 35.7,
                        "longitude": 51.4,
                        "coord_type": "general neighborhood",
                        "event_type": "Missile strike",
                        "site_type": "port",
                        "source_1": "https://www.reuters.com/world/example",
                    }
                ]
            ),
            encoding="utf-8",
        )

        result = self.conflict_ntl.run_conflict_ntl_screen_events(
            events_path="inputs/events.json",
            run_label="json_case",
            config={"configurable": {"thread_id": self.thread_id}},
        )

        self.assertEqual(result["summary"]["round1_status_counts"]["event_candidate"], 1)


if __name__ == "__main__":
    unittest.main()
