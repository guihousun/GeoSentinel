from __future__ import annotations

import importlib
import json
import os
import tempfile
import unittest
from pathlib import Path


class ConflictNTLCaseReportTests(unittest.TestCase):
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
        self.thread_id = "conflict-report"
        self.workspace = self.storage_manager.get_workspace(self.thread_id)

    def _restore_env(self) -> None:
        for name, value in self._old_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    def test_case_report_summarizes_screening_units_and_freshness(self) -> None:
        run_dir = self.workspace / "outputs" / "case_inputs"
        run_dir.mkdir(parents=True)
        screening_summary = run_dir / "screening_summary.json"
        analysis_units = run_dir / "analysis_units.csv"
        top_candidates = run_dir / "top_candidates.csv"
        freshness = run_dir / "freshness.json"

        screening_summary.write_text(
            json.dumps(
                {
                    "total_input_records": 10,
                    "top_candidate_count": 4,
                    "round1_status_counts": {"event_candidate": 8, "needs_geocoding": 2},
                    "ntl_relevance_counts": {"ntl_applicable": 4, "ntl_uncertain": 6},
                }
            ),
            encoding="utf-8",
        )
        analysis_units.write_text(
            "analysis_unit_id,unit_type,event_date_utc,source_event_ids,source_event_count,aoi_count\n"
            "u1,buffer_overlap_day,2026-03-01,e1,1,1\n"
            "u2,admin_day,2026-03-02,e2;e3,2,2\n",
            encoding="utf-8",
        )
        top_candidates.write_text(
            "event_id,event_date_utc,country,city,site_type,site_subtype,source_quality\n"
            "e1,2026-03-01,Iran,Tehran,military,military : airbase,strong\n",
            encoding="utf-8",
        )
        freshness.write_text(
            json.dumps(
                {
                    "status": "complete",
                    "source": "isw_storymap",
                    "source_modified_utc": "2026-05-01T18:49:48Z",
                    "age_hours": 48.2,
                }
            ),
            encoding="utf-8",
        )

        result = self.conflict_ntl.run_conflict_ntl_build_case_report(
            case_name="US-Israel-Iran smoke test",
            screening_summary_path=str(screening_summary),
            analysis_units_csv_path=str(analysis_units),
            top_candidates_csv_path=str(top_candidates),
            freshness_json_path=str(freshness),
            run_label="report_case",
            config={"configurable": {"thread_id": self.thread_id}},
        )

        self.assertEqual(result["status"], "complete")
        report_json = json.loads(Path(result["output_files"]["case_report_json"]).read_text(encoding="utf-8"))
        self.assertEqual(report_json["screening"]["top_candidate_count"], 4)
        self.assertEqual(report_json["analysis_units"]["total_units"], 2)
        self.assertEqual(report_json["analysis_units"]["unit_type_counts"]["buffer_overlap_day"], 1)
        report_md = Path(result["output_files"]["case_report_md"]).read_text(encoding="utf-8")
        self.assertIn("US-Israel-Iran smoke test", report_md)
        self.assertIn("top candidates: 4", report_md)


if __name__ == "__main__":
    unittest.main()
