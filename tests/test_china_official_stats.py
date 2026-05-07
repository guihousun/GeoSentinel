from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock


class ChinaOfficialStatsTests(unittest.TestCase):
    def setUp(self) -> None:
        self._old_env = {
            "NTL_USER_DATA_DIR": os.environ.get("NTL_USER_DATA_DIR"),
        }
        self.tempdir = tempfile.TemporaryDirectory()
        os.environ["NTL_USER_DATA_DIR"] = str(Path(self.tempdir.name) / "user_data")
        import importlib
        import storage_manager
        from tools import China_official_stats

        self.storage_manager = importlib.reload(storage_manager)
        self.stats = importlib.reload(China_official_stats)

    def tearDown(self) -> None:
        for key, value in self._old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.tempdir.cleanup()

    def test_all_34_census_population_returns_hmt_and_mainland(self) -> None:
        payload = json.loads(
            self.stats.china_official_stats_tool(
                regions="all_34",
                indicators="census_population",
                start_year=2020,
                end_year=2020,
                census_year=2020,
            )
        )

        self.assertEqual(payload["status"], "success")
        self.assertEqual(payload["coverage"]["record_count"], 34)
        by_code = {row["region_code"]: row for row in payload["records"]}
        self.assertEqual(by_code["440000"]["value"], 126012510)
        self.assertEqual(by_code["710000"]["value"], 23561236)
        self.assertEqual(by_code["810000"]["value"], 7474200)
        self.assertEqual(by_code["820000"]["value"], 683218)
        self.assertTrue(payload["output_file"].endswith(".csv"))

    def test_mainland_gdp_uses_nbs_query_and_legacy_wrapper(self) -> None:
        with mock.patch.object(self.stats, "_query_nbs_region_gdp", return_value=(38700.58, None)) as mocked:
            payload = json.loads(
                self.stats.china_official_gdp_tool(
                    region="上海",
                    start_year=2020,
                    end_year=2020,
                )
            )

        self.assertEqual(payload["status"], "success")
        self.assertEqual(payload["records"][0]["region_code"], "310000")
        self.assertEqual(payload["records"][0]["value"], 38700.58)
        mocked.assert_called_once_with("310000", 2020, timeout_s=20)

    def test_all_34_gdp_uses_special_region_official_caches(self) -> None:
        def fake_nbs(region_code: str, year: int, timeout_s: int = 20):
            return (100.0, None)

        with mock.patch.object(self.stats, "_query_nbs_region_gdp", side_effect=fake_nbs):
            payload = json.loads(
                self.stats.china_official_stats_tool(
                    regions="all_34",
                    indicators="gdp",
                    start_year=2020,
                    end_year=2020,
                )
            )

        self.assertEqual(payload["status"], "success")
        self.assertEqual(payload["coverage"]["record_count"], 34)
        self.assertEqual(payload["coverage"]["value_count"], 34)
        by_code = {row["region_code"]: row for row in payload["records"]}
        self.assertEqual(by_code["710000"]["unit"], "million NT$")
        self.assertEqual(by_code["810000"]["value"], 2710730)
        self.assertEqual(by_code["820000"]["source_status"], "official_local_cache")

    def test_gdp_falls_back_to_official_local_cache_when_live_sources_blocked(self) -> None:
        with mock.patch.object(self.stats, "_query_nbs_region_gdp", return_value=(None, "nbs_403_forbidden")):
            with mock.patch.object(
                self.stats,
                "_query_yearbook_region_gdp",
                return_value=(None, "yearbook_xls_403_forbidden", "https://www.stats.gov.cn/sj/ndsj/2021/html/C03-09.xls"),
            ):
                payload = json.loads(
                    self.stats.china_official_stats_tool(
                        regions="上海",
                        indicators="gdp",
                        start_year=2020,
                        end_year=2020,
                    )
                )

        self.assertEqual(payload["status"], "success")
        self.assertEqual(payload["records"][0]["value"], 38700.58)
        self.assertEqual(payload["records"][0]["source_status"], "official_yearbook_xls")
        self.assertIn("stats.gov.cn", payload["records"][0]["source_url"])

    def test_gdp_can_read_shared_base_data_cache(self) -> None:
        with mock.patch.object(self.stats, "_query_nbs_region_gdp", return_value=(None, "nbs_403_forbidden")):
            with mock.patch.object(self.stats, "_query_yearbook_region_gdp", return_value=(None, "yearbook_xls_not_registered_for_year", "")):
                with mock.patch.object(self.stats, "_query_local_cached_gdp", return_value=(None, "official_local_cache_not_registered_for_year", "")):
                    payload = json.loads(
                        self.stats.china_official_stats_tool(
                            regions="广东",
                            indicators="gdp",
                            start_year=2019,
                            end_year=2019,
                        )
                    )

        self.assertEqual(payload["status"], "success")
        self.assertEqual(payload["records"][0]["value"], 107986.9)
        self.assertIn("public_github_dataset", payload["records"][0]["source_status"])

    def test_all_34_2020_gdp_can_return_mainland_cache_when_nbs_is_blocked(self) -> None:
        with mock.patch.object(self.stats, "_query_nbs_region_gdp", return_value=(None, "nbs_403_forbidden")):
            with mock.patch.object(
                self.stats,
                "_query_yearbook_region_gdp",
                return_value=(None, "yearbook_xls_403_forbidden", "https://www.stats.gov.cn/sj/ndsj/2021/html/C03-09.xls"),
            ):
                payload = json.loads(
                    self.stats.china_official_stats_tool(
                        regions="all_34",
                        indicators="gdp",
                        start_year=2020,
                        end_year=2020,
                    )
                )

        self.assertEqual(payload["status"], "success")
        self.assertEqual(payload["coverage"]["record_count"], 34)
        self.assertEqual(payload["coverage"]["value_count"], 34)
        by_code = {row["region_code"]: row for row in payload["records"]}
        self.assertEqual(by_code["440000"]["value"], 110760.94)
        self.assertEqual(by_code["310000"]["source_status"], "official_yearbook_xls")
        self.assertEqual(by_code["810000"]["value"], 2710730)

    def test_gdp_falls_back_to_public_github_dataset_for_older_years(self) -> None:
        csv_text = (
            ",北京市,上海市,广东省\n"
            "2019,35445.1,37987.6,107986.9\n"
            "2018,33106.0,36011.8,99945.2\n"
        )
        with mock.patch.object(self.stats, "_query_nbs_region_gdp", return_value=(None, "nbs_403_forbidden")):
            with mock.patch.object(
                self.stats,
                "_query_yearbook_region_gdp",
                return_value=(None, "yearbook_xls_not_registered_for_year", ""),
            ):
                with mock.patch.object(
                    self.stats,
                    "_get_public_github_gdp_cache",
                    return_value=(self.stats._parse_public_github_gdp_csv(csv_text), None),
                ):
                    payload = json.loads(
                        self.stats.china_official_stats_tool(
                            regions="广东",
                            indicators="gdp",
                            start_year=2019,
                            end_year=2019,
                        )
                    )

        self.assertEqual(payload["status"], "success")
        self.assertEqual(payload["records"][0]["value"], 107986.9)
        self.assertEqual(payload["records"][0]["source_status"], "public_github_dataset")

    def test_gdp_falls_back_to_public_wikipedia_dataset_for_recent_years(self) -> None:
        html = """
        <table class="wikitable">
          <tr><th>主要年份</th><th>2024 (p)</th><th>2023</th></tr>
          <tr><th>广东</th><td>14,163,380</td><td>13,790,540</td></tr>
          <tr><th>上海</th><td>5,390,000</td><td>4,720,000</td></tr>
        </table>
        中国大陆31个省级行政区主要年份现价名义GDP（百万人民币）
        """
        with mock.patch.object(self.stats, "_query_nbs_region_gdp", return_value=(None, "nbs_403_forbidden")):
            with mock.patch.object(
                self.stats,
                "_query_yearbook_region_gdp",
                return_value=(None, "yearbook_xls_not_registered_for_year", ""),
            ):
                with mock.patch.object(
                    self.stats,
                    "_get_public_github_gdp_cache",
                    return_value=({}, "public_github_gdp_no_region_year_value"),
                ):
                    with mock.patch.object(
                        self.stats,
                        "_get_public_wikipedia_gdp_cache",
                        return_value=(self.stats._parse_public_wikipedia_gdp_html(html), None),
                    ):
                        payload = json.loads(
                            self.stats.china_official_stats_tool(
                                regions="广东",
                                indicators="gdp",
                                start_year=2024,
                                end_year=2024,
                            )
                        )

        self.assertEqual(payload["status"], "success")
        self.assertEqual(payload["records"][0]["value"], 141633.8)
        self.assertEqual(payload["records"][0]["source_status"], "public_wikipedia_dataset")

    def test_unknown_region_is_reported_without_crash(self) -> None:
        payload = json.loads(
            self.stats.china_official_stats_tool(
                regions="not-a-region",
                indicators="census_population",
            )
        )

        self.assertEqual(payload["status"], "no_data")
        self.assertEqual(payload["coverage"]["unsupported_regions"], ["not-a-region"])

    def test_new_tool_is_registered_for_lazy_import(self) -> None:
        import tools

        tool = tools.China_Official_Stats_tool

        self.assertEqual(tool.name, "China_Official_Stats_tool")

    def test_shared_base_data_indicators_are_queryable(self) -> None:
        population_payload = json.loads(
            self.stats.china_official_stats_tool(
                regions="广东",
                indicators="resident_population",
                start_year=2020,
                end_year=2020,
            )
        )
        electricity_payload = json.loads(
            self.stats.china_official_stats_tool(
                regions="广东",
                indicators="electricity_consumption",
                start_year=2020,
                end_year=2020,
            )
        )
        co2_payload = json.loads(
            self.stats.china_official_stats_tool(
                regions="广东",
                indicators="co2_emissions",
                start_year=2022,
                end_year=2022,
            )
        )

        self.assertEqual(population_payload["records"][0]["value"], 12624.0)
        self.assertEqual(population_payload["records"][0]["source_url"], "省总人口_万人.xlsx")
        self.assertEqual(electricity_payload["records"][0]["value"], 6926.0)
        self.assertEqual(electricity_payload["records"][0]["unit"], "100 million kWh")
        self.assertAlmostEqual(co2_payload["records"][0]["value"], 659.071528733333)
        self.assertEqual(co2_payload["records"][0]["source_status"], "shared_base_data")


if __name__ == "__main__":
    unittest.main()
