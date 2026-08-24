from __future__ import annotations

import ast
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
FASTPATH_DIR = ROOT / "experiments" / "official_daily_ntl_fastpath"


class NTLAvailabilityScanAssetsTests(unittest.TestCase):
    def test_official_availability_scan_script_and_gee_baseline_exist(self) -> None:
        self.assertTrue((FASTPATH_DIR / "scan_official_ntl_availability.py").is_file())
        self.assertTrue((FASTPATH_DIR / "gee_baseline.py").is_file())

    def test_app_ui_accepts_current_and_legacy_scan_script_names(self) -> None:
        source = (ROOT / "app_ui.py").read_text(encoding="utf-8")
        tree = ast.parse(source)

        values: list[str] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue
            if not any(isinstance(t, ast.Name) and t.id == "_NTL_SCAN_SCRIPT_CANDIDATES" for t in node.targets):
                continue
            if isinstance(node.value, ast.List):
                for item in node.value.elts:
                    values.extend(
                        arg.value
                        for arg in getattr(item, "args", [])
                        if isinstance(arg, ast.Constant) and isinstance(arg.value, str)
                    )

        self.assertIn("scan_ntl_availability.py", values)
        self.assertIn("scan_official_ntl_availability.py", values)


if __name__ == "__main__":
    unittest.main()
