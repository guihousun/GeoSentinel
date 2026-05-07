from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock


class CountryGDPToolTests(unittest.TestCase):
    def setUp(self) -> None:
        self._old_env = {"NTL_USER_DATA_DIR": os.environ.get("NTL_USER_DATA_DIR")}
        self.tempdir = tempfile.TemporaryDirectory()
        os.environ["NTL_USER_DATA_DIR"] = str(Path(self.tempdir.name) / "user_data")
        import importlib
        import storage_manager
        from tools import country_gdp_tool

        importlib.reload(storage_manager)
        self.tool = importlib.reload(country_gdp_tool)

    def tearDown(self) -> None:
        for key, value in self._old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.tempdir.cleanup()

    def test_world_bank_country_gdp_range(self) -> None:
        payload = [
            {"page": 1},
            [
                {
                    "countryiso3code": "CHN",
                    "country": {"value": "China"},
                    "date": "2024",
                    "value": 18743803170827.2,
                },
                {
                    "countryiso3code": "CHN",
                    "country": {"value": "China"},
                    "date": "2023",
                    "value": 18270356654533.2,
                },
            ],
        ]
        with mock.patch.object(self.tool, "_run_curl_json_request", return_value=payload):
            report = json.loads(
                self.tool.country_gdp_search_tool(
                    countries="China",
                    start_year=2023,
                    end_year=2024,
                )
            )

        self.assertEqual(report["status"], "success")
        self.assertEqual(report["coverage"]["record_count"], 2)
        self.assertEqual(report["records"][0]["country_code"], "CHN")
        self.assertEqual(report["records"][0]["source_status"], "official_world_bank")

    def test_worldometers_latest_fallback(self) -> None:
        html = """
        <table>
          <tr><th>#</th><th>Country</th><th>GDP</th><th>GDP (Full Value)</th><th>GDP Growth</th><th>GDP per Capita</th></tr>
          <tr><td>1</td><td>China</td><td>$20.85 trillion</td><td>$20,851,593,000,000</td><td>4.41%</td><td>$14,874</td></tr>
        </table>
        """
        response = mock.Mock()
        response.raise_for_status.return_value = None
        response.text = html
        with mock.patch.object(self.tool, "_run_curl_json_request", side_effect=RuntimeError("curl_failed:test")):
            with mock.patch.object(self.tool.requests, "get", return_value=response):
                report = json.loads(
                    self.tool.country_gdp_search_tool(
                        countries="China",
                        start_year=2024,
                        end_year=2024,
                        source_preference="auto",
                    )
                )

        self.assertEqual(report["status"], "success")
        self.assertEqual(report["records"][0]["source_status"], "public_worldometers_latest_snapshot")
        self.assertEqual(report["records"][0]["value"], 20851593000000.0)


if __name__ == "__main__":
    unittest.main()
