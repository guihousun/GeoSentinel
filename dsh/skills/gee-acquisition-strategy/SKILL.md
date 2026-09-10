---
name: gee-acquisition-strategy
description: Use when planning how to obtain observations: Earth Engine raster download, administrative boundaries, or user-supplied inputs, and which product semantics must be recorded.
---

# 观测获取策略

## 可用通道

| 通道 | 工具 | 必须给出 |
| --- | --- | --- |
| GEE 栅格下载 | `geo_download_gee` | `dataset_id`、`bands`、`bbox`、`scale`、`asset_type`、`start_date`/`end_date`、`reducer` |
| 行政区边界 | `geo_download_boundary` | 国内 DataV 的 `place_name` 或已核实六位 `adcode`（`scope=children` 取下级区划）；国外 geoBoundaries 的 ISO3 + ADM 等级，或已核实的 GEE FeatureCollection |
| 用户文件 | — | 上传文件在 `inputs/`，先 `geo_inspect_raster` / `geo_inspect_vector` 核验 |

## 不可用

- **无 GEE 服务端 Python**：不能 `ee.Initialize`、`reduceRegions`、Drive 导出或批处理导出任务。
- **无 Earthdata/LAADS VNP46A1/A2 HDF5 下载管线**。

## 必须记录

数据集标识、波段、产品时间语义（年度合成 vs 单日/月度）、空间分辨率、reducer、AOI 与边界来源、检索时间、有效像元与覆盖。

## 纪律

- 不猜数据集或波段；先查 `gee-dataset-selection` 技能里的表。
- 覆盖不足时报告，不要静默替换产品或日期。
- 边界区数与名称必须核验；`expected_count` 与实际不一致时先查原因，不要直接继续统计。
- 下载成功不等于数据可用：继续做有效像元与覆盖检查，再交给下游。
