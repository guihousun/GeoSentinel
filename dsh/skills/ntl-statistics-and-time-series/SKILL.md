---
name: ntl-statistics-and-time-series
description: Use for nighttime-light metrics, zonal statistics, time-series trend and anomaly detection with the platform's registered tools, including their unit and coverage requirements.
---

# 夜间灯光统计与时间序列

## 指标

`geo_calculate_ntl_metrics_for_raster` 的 `selected` 支持：`MaxNTL`、`MinNTL`、`SDNTL`、`TNTL`、`LArea`、`3DPLand`、`3DED`、`3DLPI`、`ANTL`。**均值用 `ANTL`**。指标定义、单位与空间支持必须随结果报告。

## 分区统计

`geo_calculate_zonal_statistics` 逐行政区统计，`selected_indices` 选指标（均值用 `ANTL`）。已有分区统计需求不要另写脚本。执行前：

- `geo_inspect_vector` 核验边界字段、CRS、要素数；
- `geo_validate_geodata` 确认栅格与边界空间重叠、网格兼容；
- 记录区数与名称，说明是否使用下级区划。

## 时间序列

`geo_analyze_ntl_trend` 需要**有序**时序栅格，输出斜率与显著性。执行前必须核实输入顺序、日期标签与网格一致性；顺序错乱会得到无意义的结果。

## 异常

`geo_detect_ntl_anomaly` 基于基线检测异常，可设 `k_sigma` 与最小基线观测数。**异常不等于事件因果证明**：要写出基线窗口、基线观测数、阈值与替代解释。

## 合成

`geo_composite_ntl_rasters` 用于已对齐网格的多期合成，方法必须写明。

## 纪律

- 缺测、云覆盖或 QA 不合格的时相按缺测处理，不要插值成趋势。
- 重复计算同一栅格是计算交叉校验，不是独立科学验证。
- 不同产品（年度/月度、不同传感器）不可混入同一条时间序列。
