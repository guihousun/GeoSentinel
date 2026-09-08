"""Fixed entrypoints. Networked acquisition never executes model-authored code."""
import json
import os
from pathlib import Path
import subprocess
import sys
import signal


def safe_file(root, relative):
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise ValueError("A workspace-relative file is required")
    target = (Path(root) / relative).resolve()
    target.relative_to(Path(root).resolve())
    return target


def inspect_raster(request):
    import rasterio
    import numpy as np
    root = "/workspace/inputs" if request["source"] == "inputs" else "/workspace/previous"
    target = safe_file(root, request["path"])
    with rasterio.open(target) as src:
        # Windowed accumulation avoids loading country-scale rasters into memory.
        count, total, low, high = 0, 0.0, float("inf"), float("-inf")
        for _, window in src.block_windows(1):
            data = src.read(1, window=window, masked=True).compressed()
            data = data[np.isfinite(data)]
            if len(data):
                count += len(data)
                total += float(data.sum(dtype=np.float64))
                low, high = min(low, float(data.min())), max(high, float(data.max()))
        return {"width": src.width, "height": src.height, "bands": src.count,
                "crs": str(src.crs), "valid_pixels": count,
                "mean": total / count if count else None,
                "min": low if count else None, "max": high if count else None}


def download(request):
    from ntl_toolkit.core.gee_download import GeeDownloadRequest, download_gee_raster
    payload = dict(request["parameters"])
    payload["project"] = os.environ["GEE_DEFAULT_PROJECT_ID"]
    payload["output"] = "outputs/imagery.tif"
    value = download_gee_raster(GeeDownloadRequest(**payload), progress=lambda current, total, message: print(
        json.dumps({"type": "progress", "current": current, "total": total, "message": message}), flush=True))
    result = value.model_dump(mode="json")
    if result.get("status") not in ("success", "succeeded", "ok"):
        # ToolResult uses success boolean in some revisions; preserve its envelope.
        if result.get("error"):
            raise RuntimeError(json.dumps(result["error"], ensure_ascii=False))
    return result


def main():
    # The worker also expires when the host service is interrupted or crashes.
    seconds = int(os.environ.get("GEO_JOB_TIMEOUT_SECONDS", "1800"))
    if not 1 <= seconds <= 86400:
        raise ValueError("Invalid worker timeout")
    signal.alarm(seconds)
    request = json.loads(Path("/request/request.json").read_text())
    kind = request["kind"]
    if kind == "inspect":
        value = inspect_raster(request)
    elif kind == "execute":
        result = subprocess.run([sys.executable, "/request/script.py"], cwd="/workspace", check=False)
        if result.returncode:
            raise RuntimeError(f"Analysis script exited with code {result.returncode}")
        value = {"status": "completed"}
    elif kind == "gee-download":
        value = download(request)
    else:
        raise ValueError("Unknown worker operation")
    Path("/workspace/outputs/result.json").write_text(json.dumps(value, ensure_ascii=False, indent=2))
    print(json.dumps({"status": "completed", "operation": kind}))


if __name__ == "__main__":
    main()
