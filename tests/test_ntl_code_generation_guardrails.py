import unittest

from tools import NTL_Code_generation as codegen


class NTLCodeGenerationGuardrailTests(unittest.TestCase):
    def test_stdout_quality_audit_rejects_zero_region_success_logs(self):
        audit = codegen._build_stdout_quality_audit("Done. 0 regions processed")

        self.assertFalse(audit["pass"])
        self.assertIn("0 regions", " ".join(audit["warnings"]))

    def test_preflight_warns_on_sat_io_annual_ntl_wrong_band(self):
        code = """
import ee
ee.Initialize(project='demo-project')
img = ee.ImageCollection('projects/sat-io/open-datasets/npp-viirs-ntl').select('avg_rad').mean()
"""
        report = codegen._preflight_checks(code, strict_mode=False)

        self.assertTrue(
            any("Expected one of ['b1']" in warning for warning in report["warnings"]),
            report["warnings"],
        )


if __name__ == "__main__":
    unittest.main()
