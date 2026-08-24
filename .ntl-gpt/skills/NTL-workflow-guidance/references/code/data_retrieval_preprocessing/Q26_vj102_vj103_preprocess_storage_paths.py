from __future__ import annotations

import json

from tools.official_vj_dnb_preprocess_tool import run_official_vj_dnb_preprocess


def main() -> None:
    """
    Example: preprocess already-downloaded VJ102DNB/VJ103DNB files using
    storage_manager-compatible virtual paths.
    """
    result = run_official_vj_dnb_preprocess(
        input_dir="/data/processed/official_vj_dnb_pipeline_runs/iran_20260222_20260301/raw_nc",
        output_root="/data/processed/official_vj_dnb_preprocess_runs",
        run_label="iran_20260222_20260301_preprocess_only",
        start_date="2026-02-22",
        end_date="2026-03-01",
        bbox="44.03,25.08,63.33,39.77",
        composite="mean",
        resolution_m=500.0,
        radius_m=2000.0,
        radiance_scale=1e9,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

