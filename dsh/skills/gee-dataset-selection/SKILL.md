---
name: gee-dataset-selection
description: Use when choosing Earth Engine datasets, bands, date ranges, scale, reducers, or auxiliary layers. Carries the curated GeoSentinel dataset table (NTL, boundaries, population, optical, environmental covariates, event context) with coverage, bands and default reducers.
---

# GEE 数据集选择

在制定 GEE 计划或调用 `geo_download_gee` 之前使用本技能。**不要猜数据集标识或波段名**——先在本表里查。

## 平台工具映射

| 需要做的事 | 本平台 |
| --- | --- |
| 下载栅格 | `geo_download_gee`（必须给出 `dataset_id`、`bands`、`bbox`、`scale`、`asset_type`、日期与 reducer） |
| 验证覆盖/波段 | 无独立元数据工具。用 `geo_download_gee` 的返回值核实：选错数据集/波段/日期会返回 `GEE_DOWNLOAD_FAILED: No images found for the requested dataset, dates, and AOI` |
| 行政区边界 | `geo_download_boundary`（国内 DataV/adcode，国外 geoBoundaries 或已核实 GEE FeatureCollection） |
| 后续统计 | `geo_calculate_zonal_statistics`（均值指标 ANTL）、`geo_inspect_raster`、`geo_validate_geodata` |

更细的筛选规则见 `references/dataset-selection-rules.md`，参数术语见 `references/gee-parameter-glossary.md`（用 `read` 读取）。

## 选择规则

1. 先确定**时间窗**：年度产品的时间锚点是"年度合成"，不是"只到某天"。例如 `NOAA/VIIRS/DNB/ANNUAL_V21` 覆盖 2013–2021，`ANNUAL_V22` 覆盖 2022–2025；要 2020 年数据必须用 V21。
2. 再确定**波段**：年度集合用 `average` / `average_masked`；`avg_rad` 属于月度集合 `NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG`。两者不可混用。
3. 夜间灯光是证据之一，不是平台身份；不要因为任务带"灯光"就跳过其他证据族。
4. 校正（角度/季节/辐射/跨传感器/大气/几何）是**显式选择**：用户没有明确要求，或所选方法本身不要求时，保持产品标准值。
5. 元数据是证据范围内的信息：优先 `validation_status: task_tested` 的条目，但仍需针对本次 AOI/日期核实覆盖。
6. 覆盖不足时：换有记录的替代产品、在有可见理由下调整时间窗，或明确报告延迟/覆盖错误——不要静默替换。

## 数据集表

| dataset_id | asset_type | bands | 时间分辨率 | 覆盖 | scale_m |
| --- | --- | --- | --- | --- | --- |
projects/sat-io/open-datasets/npp-viirs-ntl | ImageCollection | b1 | annual | 2000-01-01 .. 2024-01-01（须实测覆盖） | 500
NOAA/VIIRS/DNB/ANNUAL_V22 | ImageCollection | average, average_masked, cf_cvg, cvg, maximum, median, median_masked, minimum | annual | 2022-01-01 .. 2025-01-01（须实测覆盖） | 464
NOAA/VIIRS/DNB/ANNUAL_V21 | ImageCollection | average, average_masked, cf_cvg, cvg, maximum, median, median_masked, minimum | annual | 2013-01-01 .. 2021-01-01（须实测覆盖） | 464
NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG | ImageCollection | avg_rad, cf_cvg | monthly | 2014-01-01 .. 2026-03-01（须实测覆盖） | 464
NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG | ImageCollection | avg_rad, cf_cvg | monthly | 2012-04-01 .. 2026-03-01（须实测覆盖） | 464
NASA/VIIRS/002/VNP46A2 | ImageCollection | DNB_BRDF_Corrected_NTL, DNB_Lunar_Irradiance, Gap_Filled_DNB_BRDF_Corrected_NTL, Latest_High_Quality_Retrieval, Mandatory_Quality_Flag, QF_Cloud_Mask, Snow_Flag | daily | 2012-01-19 .. 2026-04-08（须实测覆盖） | 464
NOAA/VIIRS/001/VNP46A1 | ImageCollection | BrightnessTemperature_M12, BrightnessTemperature_M13, BrightnessTemperature_M15, BrightnessTemperature_M16, DNB_At_Sensor_Radiance_500m, Glint_Angle, Granule, Lunar_Zenith, Lunar_Azimuth, Moon_Illumination_Fraction, Moon_Phase_Angle, QF_Cloud_Mask, QF_DNB, QF_VIIRS_M10, QF_VIIRS_M11, QF_VIIRS_M12, QF_VIIRS_M13, QF_VIIRS_M15, QF_VIIRS_M16, Radiance_M10, Radiance_M11, Sensor_Zenith, Sensor_Azimuth, Solar_Zenith, Solar_Azimuth, UTC_Time | daily | 2012-01-19 .. 2025-01-02（须实测覆盖） | 464
NOAA/DMSP-OLS/NIGHTTIME_LIGHTS | ImageCollection | avg_vis, stable_lights, cf_cvg, avg_lights_x_pct | annual | 1992-01-01 .. 2013-12-31（须实测覆盖） | 928
NOAA/DMSP-OLS/CALIBRATED_LIGHTS_V4 | ImageCollection | avg_vis, cf_cvg | annual | 1996-03-16 .. 2011-07-31（须实测覆盖） | 928
 | FeatureCollection |  |  |  | 
 | FeatureCollection |  |  |  | 
 | FeatureCollection |  |  |  | 
USDOS/LSIB_SIMPLE/2017 | FeatureCollection |  |  |  | 
projects/sat-io/open-datasets/ORNL/LANDSCAN_GLOBAL | ImageCollection | b1 | annual | 2000-01-01 .. 2024-01-01（须实测覆盖） | 928
WorldPop/GP/100m/pop | ImageCollection | population | annual | 2000-01-01 .. 2020-01-01（须实测覆盖） | 93
CIESIN/GPWv411/GPW_Population_Count | ImageCollection | population_count | 5-year | 2000-01-01 .. 2020-01-01（须实测覆盖） | 928
JRC/GHSL/P2023A/GHS_POP | ImageCollection | population_count | 5-year | 1975-01-01 .. 2030-01-01（须实测覆盖） | 100
MODIS/061/MOD13A1 | ImageCollection | NDVI, EVI | 16-day |  | 500
MODIS/061/MOD13A2 | ImageCollection | NDVI, EVI | 16-day |  | 1000
MODIS/061/MCD12Q1 | ImageCollection | LC_Type1 | annual |  | 500
ESA/WorldCover/v200 | ImageCollection | Map | annual/static | 2021-01-01 .. 2021-01-01（须实测覆盖） | 10
MODIS/061/MOD17A3HGF | ImageCollection | Npp | annual |  | 500
USGS/SRTMGL1_003 | Image | elevation | static |  | 30
NASA/NASADEM_HGT/001 | Image | elevation | static |  | 30
FIRMS | ImageCollection | T21, confidence, line_number | daily | 2000-11-01 .. 2026-04-22（须实测覆盖） | 927
MODIS/061/MCD64A1 | ImageCollection | BurnDate, Uncertainty, QA, FirstDay, LastDay | monthly |  | 500
GLOBAL_FLOOD_DB/MODIS_EVENTS/V1 | ImageCollection | flooded, duration, clear_views, clear_perc, jrc_perm_water | event | 2000-02-17 .. 2018-12-05（须实测覆盖） | 250
COPERNICUS/S2_SR_HARMONIZED | ImageCollection | B2, B3, B4, B8, B11, B12, SCL | subdaily | 2017-03-28（须实测覆盖） | 10
GOOGLE/CLOUD_SCORE_PLUS/V1/S2_HARMONIZED | ImageCollection | cs, cs_cdf | subdaily | （须实测覆盖） | 10
 | ImageCollection | SR_B2, SR_B3, SR_B4, SR_B5, ST_B10, QA_PIXEL | subdaily | 2013-03-18（须实测覆盖） | 30
MODIS/061/MOD13Q1 | ImageCollection | NDVI, EVI, DetailedQA, SummaryQA | 16-day | 2000-02-18（须实测覆盖） | 250

表由 `gee-dataset-registry.json`（31 条）生成；完整字段（`use_when` / `avoid_when` / `notes` / `live_validation`）在仓库 `.ntl-gpt/skills/gee-dataset-selection/references/` 内，属于开发期参考资料。
