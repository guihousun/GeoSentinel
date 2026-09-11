---
name: ntl-statistics-and-time-series
description: Use for nighttime-light metrics, zonal statistics, time-series trend and anomaly detection with the platform's registered tools, including their unit and coverage requirements.
---

# 夜间灯光统计与时间序列

## 指标

`geo_calculate_ntl_metrics_for_raster` 的 `selected` 支持：`MaxNTL`、`MinNTL`、`SDNTL`、`TNTL`、`LArea`、`3DPLand`、`3DED`、`3DLPI`、`ANTL`。**均值用 `ANTL`**。指标定义、单位与空间支持必须随结果报告；下面就是平台实现的口径，照此报告，不要另立定义，也**不要为了解释口径去下载影像或跑容器作业**：

- `TNTL`（总强度）= 有效像元值之和，**不按像元面积加权**。单位与输入一致（VIIRS 夜间灯光为 nW·cm⁻²·sr⁻¹）。它随像元数与分辨率变化，只在**同一网格、同一产品**内比较总量。
- `ANTL`（平均强度）= 有效像元值之和 ÷ 有效像元数（NaN 不计入分母）。单位同上，是强度均值，**可用于不同区域之间比较**（行政区大小、像元数不影响它）。
- `MaxNTL` / `MinNTL` / `SDNTL` = 有效像元的最大值 / 最小值 / 标准差，单位同上。
- `LArea` = 亮像元数（值 > 0）× 像元面积。**只有它使用像元面积**；地理坐标系（经纬度）下像元面积不是常数，结果会带 `GEOGRAPHIC_PIXEL_AREA` 警告，报告时必须说明该值是近似。
- `3DPLand` = 总强度 ÷（最大强度 × 有效像元数）；`3DED` = 亮像元边界周长 ÷ 总强度；`3DLPI` = 最大连通亮区强度 ÷ 总强度。三者是无量纲指数，只在同一网格与同一预处理下比较。
- 所有指标都只统计**有效像元**（非 NaN）；`0` 是有效观测（无灯），与缺测（NaN）含义不同，报告时不要混为一谈。

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
