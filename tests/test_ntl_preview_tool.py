from __future__ import annotations

import importlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from PIL import Image


class NTLPreviewToolTests(unittest.TestCase):
    def setUp(self) -> None:
        self._old_env = {key: os.environ.get(key) for key in ("NTL_USER_DATA_DIR", "GEE_DEFAULT_PROJECT_ID")}
        self.tempdir = tempfile.TemporaryDirectory()
        os.environ["NTL_USER_DATA_DIR"] = str(Path(self.tempdir.name) / "user_data")
        os.environ["GEE_DEFAULT_PROJECT_ID"] = "empyrean-caster-430308-m2"

        import storage_manager
        import tools.ntl_preview_tool as ntl_preview_tool

        self.storage_manager = importlib.reload(storage_manager)
        self.tool = importlib.reload(ntl_preview_tool)

    def tearDown(self) -> None:
        for key, value in self._old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        try:
            self.tempdir.cleanup()
        except PermissionError:
            pass

    def test_normalize_years_deduplicates_and_sorts(self) -> None:
        self.assertEqual(self.tool._normalize_years([2020, 2000, 2010, 2000]), [2000, 2010, 2020])

    def test_dataset_specs_include_expected_ranges(self) -> None:
        spec = self.tool._dataset_spec("NPP-VIIRS-Like")
        self.assertEqual(spec.collection_id, "projects/sat-io/open-datasets/npp-viirs-ntl")
        self.assertEqual(spec.band, "b1")
        self.assertEqual(spec.year_min, 2000)
        self.assertEqual(spec.year_max, 2024)

        dmsp = self.tool._dataset_spec("DMSP-OLS")
        self.assertEqual(dmsp.collection_id, "NOAA/DMSP-OLS/NIGHTTIME_LIGHTS")
        self.assertEqual(dmsp.band, "avg_vis")
        self.assertEqual(dmsp.year_min, 1992)
        self.assertEqual(dmsp.year_max, 2013)

    def test_region_name_candidates_include_taiwan_for_china(self) -> None:
        self.assertEqual(
            self.tool._region_name_candidates("China"),
            ["China", "Taiwan", "Hong Kong", "Macao"],
        )
        self.assertEqual(self.tool._region_name_candidates("France"), ["France"])

    def test_preview_run_writes_pngs_and_gif_by_default(self) -> None:
        with mock.patch.object(self.tool, "_initialize_earth_engine", return_value=None), mock.patch.object(
            self.tool, "_resolve_country_geometry", return_value=(object(), ["China", "Taiwan", "Hong Kong", "Macao"])
        ), mock.patch.object(self.tool, "_build_annual_image", side_effect=lambda year, *args, **kwargs: f"image-{year}"), mock.patch.object(
            self.tool, "_render_thumbnail", side_effect=lambda *args, **kwargs: args[1].write_text("png", encoding="utf-8")
        ), mock.patch.object(self.tool, "_render_gif", side_effect=lambda *args, **kwargs: args[1].write_text("gif", encoding="utf-8")):
            result = self.tool.run_annual_ntl_preview(
                years=[2020, 2000, 2010],
                country_name="China",
                dataset_name="NPP-VIIRS-Like",
                generate_gif=True,
                output_root="preview_runs",
                run_label="china_preview",
            )

        summary_path = Path(result["summary_path"])
        self.assertTrue(summary_path.exists())
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["years"], [2000, 2010, 2020])
        self.assertEqual(len(payload["preview_items"]), 3)
        self.assertTrue(Path(result["gif_path"]).exists())
        for item in payload["preview_items"]:
            self.assertTrue(Path(item["png_path"]).exists())

    def test_preview_run_can_skip_gif(self) -> None:
        with mock.patch.object(self.tool, "_initialize_earth_engine", return_value=None), mock.patch.object(
            self.tool, "_resolve_country_geometry", return_value=(object(), ["China", "Taiwan", "Hong Kong", "Macao"])
        ), mock.patch.object(self.tool, "_build_annual_image", side_effect=lambda year, *args, **kwargs: f"image-{year}"), mock.patch.object(
            self.tool, "_render_thumbnail", side_effect=lambda *args, **kwargs: args[1].write_text("png", encoding="utf-8")
        ), mock.patch.object(self.tool, "_render_gif") as render_gif:
            result = self.tool.run_annual_ntl_preview(
                years=[2020, 2010],
                country_name="China",
                dataset_name="NPP-VIIRS-Like",
                generate_gif=False,
                output_root="preview_runs",
                run_label="china_preview_no_gif",
            )

        self.assertIsNone(result["gif_path"])
        render_gif.assert_not_called()

    def test_compose_gif_from_pngs_turns_transparency_white(self) -> None:
        source = Path(self.tempdir.name) / "source.gif"
        target = Path(self.tempdir.name) / "flattened.gif"

        frame = Image.new("RGBA", (4, 4), (0, 0, 0, 0))
        frame.putpixel((1, 1), (255, 0, 0, 255))
        frame.save(source, format="PNG")

        self.tool._compose_gif_from_pngs([source], target, fps=1.0)

        with Image.open(target) as gif:
            rgba = gif.convert("RGBA")
            self.assertEqual(rgba.getpixel((0, 0))[:3], (255, 255, 255))
            self.assertEqual(rgba.getpixel((1, 1))[:3], (255, 0, 0))


if __name__ == "__main__":
    unittest.main()
