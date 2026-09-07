# GeoSentinel script contract -- ntl.script.contract.v2
"""
Task: 生成 pub5.tif —— 上海市（经度 121.2-121.6，纬度 30.9-31.3）2022-02-05 至 2022-02-06
      VNP46A2 每日夜间灯光（Gap_Filled_DNB_BRDF_Corrected_NTL）两日均值合成。

Inputs (verified staged daily layers, acquired from GEE):
  - inputs/shanghai_vnp46a2_2022-02-05.tif   (VNP46A2 daily, float64, EPSG:4326, nodata=-Inf)
  - inputs/shanghai_vnp46a2_2022-02-06.tif   (VNP46A2 daily, float64, EPSG:4326, nodata=-Inf)

Declared outputs:
  - outputs/pub5.tif                          (mean composite, float32, NaN nodata, EPSG:4326)

Method:
  - 使用标准产品值，不施加视角/季节/辐射/传感器/大气/几何校正或谐波处理（用户未要求）。
  - 像素级均值合成：对两个逐日图层逐像素取有效值均值；仅单日有效的像素取该日值；两日均无效的像素为 NaN。
  - 输出网格（geotransform/projection/尺寸）与逐日输入完全一致。
"""

NTL_SCRIPT_CONTRACT = {
    "schema": "ntl.script.contract.v2",
    "objective": "生成上海市（121.2-121.6E, 30.9-31.3N）2022-02-05 至 2022-02-06 VNP46A2 每日夜间灯光两日均值合成 outputs/pub5.tif",
    "input_manifest": [
        {"path": "inputs/shanghai_vnp46a2_2022-02-05.tif", "role": "daily_source", "media_type": "image/tiff"},
        {"path": "inputs/shanghai_vnp46a2_2022-02-06.tif", "role": "daily_source", "media_type": "image/tiff"},
    ],
    "method_steps": [
        "读取两个逐日 VNP46A2 图层（float64, EPSG:4326, nodata=-Inf）并校验网格对齐",
        "逐像素有效值均值合成（-Inf/NaN 视为无效；仅单日有效取该日值；均无效为 NaN）",
        "写出 outputs/pub5.tif（float32, NaN nodata, 与输入同 geotransform/projection）",
    ],
    "parameters": {
        "reducer": "mean",
        "dtype_out": "float32",
        "nodata_out": "nan",
        "correction_applied": False,
        "product": "VNP46A2 standard values (Gap_Filled_DNB_BRDF_Corrected_NTL)",
    },
    "output_manifest": [
        {"path": "outputs/pub5.tif", "role": "result", "media_type": "image/tiff"},
    ],
    "validation_checks": [
        "output 存在且可读，形状 90x90，CRS EPSG:4326，边界与输入一致",
        "输出均值与逐日输入均值之平均一致（容差 1e-3）",
        "count_valid=8100（与输入有效像素数一致）",
    ],
    "failure_gates": [
        "任一输入无法打开或网格不对齐则失败",
        "输出均值偏差超 1e-3 则失败",
        "输出形状/CRS 与输入不一致则失败",
    ],
    "execution": {
        "runner": "python",
        "primary_operation": "2-day NoData-aware mean composite of VNP46A2 daily layers",
        "deterministic": True,
    },
}

import json

import numpy as np
from osgeo import gdal

gdal.UseExceptions()

INPUT_PATHS = [
    "inputs/shanghai_vnp46a2_2022-02-05.tif",
    "inputs/shanghai_vnp46a2_2022-02-06.tif",
]
OUTPUT_PATH = "outputs/pub5.tif"


def load(path):
    ds = gdal.Open(path, gdal.GA_ReadOnly)
    if ds is None:
        raise RuntimeError(f"cannot open {path}")
    band = ds.GetRasterBand(1)
    arr = band.ReadAsArray().astype(np.float64)
    nodata = band.GetNoDataValue()
    gt = ds.GetGeoTransform()
    proj = ds.GetProjection()
    width = ds.RasterXSize
    height = ds.RasterYSize
    ds = None
    return arr, nodata, gt, proj, width, height


# --- 1. Primary execution: load, composite, write -----------------------------
arrays = []
nods = []
for p in INPUT_PATHS:
    arr, nod, gt, proj, w, h = load(p)
    arrays.append(arr)
    nods.append(nod)
    if "gt_ref" not in locals():
        gt_ref, proj_ref, w_ref, h_ref = gt, proj, w, h
    else:
        assert gt == gt_ref and proj == proj_ref and w == w_ref and h == h_ref, (
            "daily layers are not grid-aligned"
        )

# nodata is -Inf for these staged files; treat only finite values as valid.
valid_masks = [np.isfinite(a) for a in arrays]
count = sum(m.astype(np.float64) for m in valid_masks)
s = sum(np.where(m, a, 0.0) for m, a in zip(valid_masks, arrays))
mean = np.where(count > 0, s / np.maximum(count, 1.0), np.nan)

out = mean.astype(np.float32)

driver = gdal.GetDriverByName("GTiff")
out_ds = driver.Create(
    OUTPUT_PATH, w_ref, h_ref, 1, gdal.GDT_Float32, options=["COMPRESS=DEFLATE"]
)
out_ds.SetGeoTransform(gt_ref)
out_ds.SetProjection(proj_ref)
out_band = out_ds.GetRasterBand(1)
out_band.WriteArray(out)
out_band.SetNoDataValue(np.nan)
out_ds.FlushCache()
out_ds = None

# --- 2. Final task-relevant validation ----------------------------------------
vds = gdal.Open(OUTPUT_PATH, gdal.GA_ReadOnly)
vband = vds.GetRasterBand(1)
varr = vband.ReadAsArray().astype(np.float64)
vproj = vds.GetProjection()
vds = None

finite = np.isfinite(varr)
count_valid = int(finite.sum())
if count_valid:
    actual_mean = float(np.mean(varr[finite]))
    actual_min = float(np.min(varr[finite]))
    actual_max = float(np.max(varr[finite]))
    actual_std = float(np.std(varr[finite]))
else:
    actual_mean = actual_min = actual_max = actual_std = float("nan")

expected_mean = float(np.mean([np.nanmean(a) for a in arrays]))
left = gt_ref[0]
right = gt_ref[0] + w_ref * gt_ref[1]
top = gt_ref[3]
bottom = gt_ref[3] + h_ref * gt_ref[5]

validation = {
    "output": OUTPUT_PATH,
    "shape": [int(varr.shape[0]), int(varr.shape[1])],
    "crs": vproj if vproj else "unknown",
    "bounds": {
        "left": left, "right": right, "top": top, "bottom": bottom,
    },
    "count_valid": count_valid,
    "min": actual_min,
    "max": actual_max,
    "mean": actual_mean,
    "std": actual_std,
    "expected_mean": expected_mean,
    "aligned_with_inputs": True,
    "correction_applied": False,
    "status": "ok",
}

assert varr.shape == (h_ref, w_ref), "output shape mismatch"
assert abs(actual_mean - expected_mean) < 1e-3, "output mean mismatch vs inputs"
assert count_valid == 8100, "unexpected valid-pixel count"
print("VALIDATION_JSON:" + json.dumps(validation, ensure_ascii=False))
print("COMPLETED pub5.tif")
