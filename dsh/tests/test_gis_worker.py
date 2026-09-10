import unittest
from unittest.mock import patch
from gis_dispatch import scoped_path, run_gis
from boundaries import datav, gcj02_to_wgs84, fetch_json

class WorkerContracts(unittest.TestCase):
    def test_input_and_output_namespaces(self):
        self.assertEqual(scoped_path("outputs/abc/file.tif"), "previous/abc/file.tif")
        self.assertEqual(scoped_path("outputs/result.csv", output=True), "outputs/result.csv")
        for value in ("/etc/passwd", "../secret", "inputs/../../secret", "C:/secret", "inputs\\x"):
            with self.assertRaises(ValueError):
                scoped_path(value)
        with self.assertRaises(ValueError):
            scoped_path("inputs/result.csv", output=True)

    def test_no_parent_substitution(self):
        with patch("boundaries.fetch_json", return_value={"features": [{"properties": {"adcode": 310000}}]}):
            with self.assertRaisesRegex(ValueError, "parent boundary"):
                datav({"adcode": "310000", "scope": "children"})

    def test_source_allowlist(self):
        for url in ("http://geo.datav.aliyun.com/a", "https://127.0.0.1/", "https://evil.example/"):
            with self.assertRaises(ValueError):
                fetch_json(url)

    def test_unregistered_operation(self):
        with self.assertRaises(ValueError):
            run_gis({"operation": "eval", "parameters": {}})

    def test_coordinate_transform_scope(self):
        self.assertEqual(gcj02_to_wgs84(0, 0), (0, 0))
        x, y = gcj02_to_wgs84(121.47, 31.23)
        self.assertLess(abs(x-121.47), .02)
        self.assertLess(abs(y-31.23), .02)
        self.assertNotEqual(x, 121.47)

if __name__ == "__main__":
    unittest.main()
