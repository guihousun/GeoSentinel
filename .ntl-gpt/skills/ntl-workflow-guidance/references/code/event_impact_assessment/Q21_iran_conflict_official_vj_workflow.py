from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from tools.official_vj_dnb_pipeline_tool import run_official_vj_dnb_fullchain


def _run(cmd: list[str], cwd: Path) -> dict:
    p = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True, encoding="utf-8", errors="ignore")
    return {"cmd": cmd, "returncode": p.returncode, "stdout": p.stdout, "stderr": p.stderr}


def main() -> None:
    repo_root = Path(__file__).resolve().parents[7]

    fullchain = run_official_vj_dnb_fullchain(
        start_date="2026-02-28",
        end_date="2026-03-02",
        bbox="44.03,25.08,63.33,39.77",
        output_root="official_vj_dnb_pipeline_runs",
        run_label="iran_conflict_20260228_20260302",
        composite="mean",
        resolution_m=500.0,
        radius_m=2000.0,
        radiance_scale=1e9,
    )

    fetch = _run(
        [
            sys.executable,
            "tools/fetch_inss_arcgis_strikes.py",
            "--out-dir",
            "base_data/Iran_War/analysis/inss_arcgis_strikes_latest",
            "--start-date",
            "2026-02-28",
            "--end-date",
            "2026-03-02",
        ],
        repo_root,
    )
    if fetch["returncode"] != 0:
        raise RuntimeError(fetch["stderr"][-2000:])

    rebuild = _run(
        [
            sys.executable,
            "base_data/Iran_War/analysis/scripts/modular_refactor/rebuild_iran_event_report.py",
            "--base-dir",
            "base_data/Iran_War",
            "--event-key",
            "iran_war",
            "--pre-date",
            "2026-02-28",
            "--post-date",
            "2026-03-02",
            "--event-start",
            "2026-02-28",
            "--event-end",
            "2026-03-02",
        ],
        repo_root,
    )
    if rebuild["returncode"] != 0:
        raise RuntimeError(rebuild["stderr"][-2000:])

    print(json.dumps({"fullchain": fullchain, "fetch": fetch, "rebuild": rebuild}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

