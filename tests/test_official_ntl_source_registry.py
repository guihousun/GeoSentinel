from __future__ import annotations

import unittest

from experiments.official_daily_ntl_fastpath.source_registry import (
    get_default_sources,
    get_nrt_priority_sources,
    parse_sources_arg,
)


class OfficialNTLSourceRegistryTests(unittest.TestCase):
    def test_default_sources_use_vnp46_products(self) -> None:
        self.assertEqual(get_default_sources(), ["VNP46A1", "VNP46A2", "VNP46A3", "VNP46A4"])

    def test_nrt_priority_sources_use_vnp46_products(self) -> None:
        self.assertEqual(get_nrt_priority_sources(), ["VNP46A1", "VNP46A2", "VNP46A3", "VNP46A4"])

    def test_parse_sources_arg_supports_vnp46_series(self) -> None:
        self.assertEqual(
            parse_sources_arg("VNP46A4,VNP46A2,VNP46A1"),
            ["VNP46A4", "VNP46A2", "VNP46A1"],
        )


if __name__ == "__main__":
    unittest.main()
