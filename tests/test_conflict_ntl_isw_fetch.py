from __future__ import annotations

import csv
import importlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlparse


class ConflictNTLISWFetchTests(unittest.TestCase):
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
        self.thread_id = "conflict-isw-fetch"
        self.workspace = self.storage_manager.get_workspace(self.thread_id)

    def _restore_env(self) -> None:
        for name, value in self._old_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    def test_fetch_isw_events_normalizes_features_to_csv_geojson_and_metadata(self) -> None:
        seen_time_filters: list[str] = []

        def fake_fetch(
            url: str,
            *,
            timeout_seconds: int = 30,
            page_size: int = 1000,
            time_extent_ms: str = "",
        ) -> list[dict[str, object]]:
            self.assertIn("FeatureServer/0", url)
            seen_time_filters.append(time_extent_ms)
            return [
                {
                    "attributes": {
                        "OBJECTID": 1,
                        "event_id": 10,
                        "strikedate": 1772236800000,
                        "post_date": 1772236800000,
                        "pub_date": 1772323200000,
                        "time": -2209155240000,
                        "event_type": "Confirmed Airstrike",
                        "actor": "UNK",
                        "site_type": "energy",
                        "siteStype": "oil infrastructure",
                        "city": "Demo",
                        "country": "Iran",
                        "coord_type": "exact",
                        "source_1": "https://www.reuters.com/example",
                        "source_2": "",
                        "sources": "https://www.bbc.com/example",
                    },
                    "geometry": {"x": 52.0, "y": 27.0},
                },
                {
                    "attributes": {
                        "OBJECTID": 2,
                        "event_id": 11,
                        "strikedate": 1773360000000,
                        "event_type": "Air Defense Activity",
                        "site_type": "unknown",
                        "siteStype": "unknown",
                        "city": "Outside",
                        "country": "Iran",
                    },
                    "geometry": {"x": 53.0, "y": 28.0},
                },
            ]

        self.conflict_ntl._fetch_arcgis_feature_layer = fake_fetch

        result = self.conflict_ntl.run_conflict_ntl_fetch_isw_events(
            layer_urls_json=json.dumps(
                [
                    {
                        "name": "demo_layer",
                        "label": "Demo ISW Layer",
                        "url": "https://example.com/arcgis/rest/services/demo/FeatureServer/0",
                        "event_family": "demo_family",
                    }
                ]
            ),
            event_window_start="2026-02-27",
            event_window_end="2026-03-05",
            run_label="fetch_case",
            config={"configurable": {"thread_id": self.thread_id}},
        )

        self.assertEqual(result["schema"], "conflict_ntl.isw_event_fetch.v1")
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["summary"]["total_features_raw"], 2)
        self.assertEqual(result["summary"]["total_records"], 1)

        with open(result["output_files"]["events_csv"], encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["source_layer"], "Demo ISW Layer")
        self.assertEqual(rows[0]["event_family"], "demo_family")
        self.assertEqual(rows[0]["event_date_utc"], "2026-02-28T00:00:00Z")
        self.assertEqual(rows[0]["site_subtype"], "oil infrastructure")
        self.assertEqual(rows[0]["latitude"], "27.0")
        self.assertEqual(rows[0]["longitude"], "52.0")

        geojson = json.loads(Path(result["output_files"]["events_geojson"]).read_text(encoding="utf-8"))
        self.assertEqual(geojson["type"], "FeatureCollection")
        self.assertEqual(len(geojson["features"]), 1)

        metadata = json.loads(Path(result["output_files"]["metadata_json"]).read_text(encoding="utf-8"))
        self.assertEqual(metadata["layers"][0]["feature_count_raw"], 2)
        self.assertEqual(metadata["layers"][0]["feature_count_in_window"], 1)
        self.assertTrue(seen_time_filters[0].startswith("1772150400000,"))
        self.assertIn(str(self.workspace / "outputs"), result["output_files"]["events_csv"])

    def test_fetch_isw_events_keeps_csv_header_on_source_error(self) -> None:
        def failing_fetch(
            url: str,
            *,
            timeout_seconds: int = 30,
            page_size: int = 1000,
            time_extent_ms: str = "",
        ) -> list[dict[str, object]]:
            raise RuntimeError("ArcGIS rate limited")

        self.conflict_ntl._fetch_arcgis_feature_layer = failing_fetch

        result = self.conflict_ntl.run_conflict_ntl_fetch_isw_events(
            layer_urls_json=json.dumps(
                [
                    {
                        "name": "demo_layer",
                        "label": "Demo ISW Layer",
                        "url": "https://example.com/arcgis/rest/services/demo/FeatureServer/0",
                        "event_family": "demo_family",
                    }
                ]
            ),
            run_label="fetch_error_case",
            config={"configurable": {"thread_id": self.thread_id}},
        )

        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["summary"]["error_count"], 1)
        header = Path(result["output_files"]["events_csv"]).read_text(encoding="utf-8").splitlines()[0]
        self.assertIn("event_date_utc", header)
        self.assertIn("source_layer_url", header)

    def test_isw_normalization_prefers_attribute_lon_lat_over_projected_geometry(self) -> None:
        row = self.conflict_ntl._normalize_isw_feature(
            {
                "attributes": {
                    "OBJECTID": 2084,
                    "event_id": 2225,
                    "post_date": 1777939200000,
                    "city": "Tahlu",
                    "country": "Iran",
                    "latitude": 27.37918,
                    "longitude": 56.32825,
                    "coord_type": "exact",
                },
                "geometry": {"x": 6270432.1072762115, "y": 3170925.6786001367},
            },
            {
                "name": "demo_layer",
                "label": "Demo ISW Layer",
                "url": "https://example.com/arcgis/rest/services/demo/FeatureServer/0",
                "event_family": "demo_family",
            },
        )

        self.assertEqual(row["longitude"], 56.32825)
        self.assertEqual(row["latitude"], 27.37918)

    def test_isw_normalization_converts_projected_geometry_when_attributes_missing(self) -> None:
        row = self.conflict_ntl._normalize_isw_feature(
            {
                "attributes": {"OBJECTID": 2085, "post_date": 1777939200000},
                "geometry": {"x": 6270432.1072762115, "y": 3170925.6786001367},
            },
            {
                "name": "demo_layer",
                "label": "Demo ISW Layer",
                "url": "https://example.com/arcgis/rest/services/demo/FeatureServer/0",
                "event_family": "demo_family",
            },
        )

        self.assertAlmostEqual(float(row["longitude"]), 56.32825, places=4)
        self.assertAlmostEqual(float(row["latitude"]), 27.37918, places=4)

    def test_time_filtered_arcgis_fetch_uses_object_id_detail_query(self) -> None:
        calls: list[dict[str, list[str]]] = []

        class FakeResponse:
            def __init__(self, payload: dict[str, object]) -> None:
                self.payload = payload

            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, *args: object) -> None:
                return None

            def read(self) -> bytes:
                return json.dumps(self.payload).encode("utf-8")

        def fake_urlopen(request: object, timeout: int = 30) -> FakeResponse:
            url = getattr(request, "full_url")
            query = parse_qs(urlparse(url).query)
            calls.append(query)
            if query.get("returnIdsOnly") == ["true"]:
                self.assertEqual(query["time"], ["1777939200000,1778025599999"])
                return FakeResponse({"objectIds": [99]})
            self.assertEqual(query["objectIds"], ["99"])
            return FakeResponse(
                {
                    "features": [
                        {
                            "attributes": {"OBJECTID": 99, "strikedate": 1777939200000},
                            "geometry": {"x": 51.0, "y": 32.0},
                        }
                    ]
                }
            )

        old_urlopen = self.conflict_ntl.urlopen
        self.conflict_ntl.urlopen = fake_urlopen
        self.addCleanup(lambda: setattr(self.conflict_ntl, "urlopen", old_urlopen))

        features = self.conflict_ntl._fetch_arcgis_feature_layer(
            "https://example.com/arcgis/rest/services/demo/FeatureServer/0",
            page_size=10,
            time_extent_ms="1777939200000,1778025599999",
        )

        self.assertEqual(len(features), 1)
        self.assertEqual(features[0]["attributes"]["OBJECTID"], 99)
        self.assertEqual(len(calls), 2)

    def test_arcgis_query_retries_429_error_payload_once(self) -> None:
        calls = 0
        sleeps: list[int] = []

        class FakeResponse:
            def __init__(self, payload: dict[str, object]) -> None:
                self.payload = payload

            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, *args: object) -> None:
                return None

            def read(self) -> bytes:
                return json.dumps(self.payload).encode("utf-8")

        def fake_urlopen(request: object, timeout: int = 30) -> FakeResponse:
            nonlocal calls
            calls += 1
            if calls == 1:
                return FakeResponse(
                    {
                        "error": {
                            "code": 429,
                            "message": "Unable to perform query. Too many requests.",
                            "details": ["Retry after 60 sec."],
                        }
                    }
                )
            return FakeResponse({"features": []})

        old_urlopen = self.conflict_ntl.urlopen
        old_sleep = self.conflict_ntl.time.sleep
        self.conflict_ntl.urlopen = fake_urlopen
        self.conflict_ntl.time.sleep = lambda seconds: sleeps.append(seconds)
        self.addCleanup(lambda: setattr(self.conflict_ntl, "urlopen", old_urlopen))
        self.addCleanup(lambda: setattr(self.conflict_ntl.time, "sleep", old_sleep))

        payload = self.conflict_ntl._arcgis_query_json(
            "https://example.com/arcgis/rest/services/demo/FeatureServer/0",
            {"f": "json"},
            30,
        )

        self.assertEqual(payload, {"features": []})
        self.assertEqual(calls, 2)
        self.assertEqual(sleeps, [62])

    def test_registered_as_lazy_ntl_gpt_tool_export(self) -> None:
        import tools

        self.assertIn("conflict_ntl_fetch_isw_events_tool", tools.__all__)
        tool = getattr(tools, "conflict_ntl_fetch_isw_events_tool")
        self.assertEqual(tool.name, "conflict_ntl_fetch_isw_events_tool")


if __name__ == "__main__":
    unittest.main()
