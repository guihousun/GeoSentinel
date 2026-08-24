from __future__ import annotations

import json

from tools.official_vj_dnb_pipeline_tool import run_official_vj_dnb_fullchain


def main() -> None:
    result = run_official_vj_dnb_fullchain(
        start_date="2026-02-22",
        end_date="2026-03-01",
        bbox="44.03,25.08,63.33,39.77",
        output_root="official_vj_dnb_pipeline_runs",
        run_label="iran_20260222_20260301",
        composite="mean",
        resolution_m=500.0,
        radius_m=2000.0,
        radiance_scale=1e9,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
