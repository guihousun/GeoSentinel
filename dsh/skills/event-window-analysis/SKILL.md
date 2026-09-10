---
name: event-window-analysis
description: Use for pre-event / event / post-event nighttime-light window comparison while preserving temporal mapping and non-attribution limits.
---

# 事件窗口分析

- 先有来源有界的事件上下文与分析就绪的观测；不要自己重建事件事实，也不要另找替代观测。
- 记录本地夜间/UTC 映射、基线与比较窗口、对照或参考区、有效日判据与覆盖。
- 比较绝对与百分比变化时，同时检查：低辐射底噪、云与质量标记、背景活动（季节、节假日、经济周期）、缓冲区稀释。
- 结论只能表述为"与给定事件上下文一致或不一致的候选信号"，**不是**因果、损失、停运或责任的证明。
- 保留替代解释（天气、供电调度、数据质量、人口或经济活动变化）；上游缺口通过主管请求修订。

## 工具映射

| 步骤 | 工具 |
| --- | --- |
| 事件缓冲区 | `geo_buffer_points_aeqd`（半径单位为公里，局部等距投影） |
| 窗口裁剪 | `geo_clip_raster` |
| 窗口合成 | `geo_composite_ntl_rasters` |
| 窗口内分区统计 | `geo_calculate_zonal_statistics`（均值 ANTL） |
| 基线异常 | `geo_detect_ntl_anomaly`（写明 `k_sigma` 与最小基线观测数） |
| 时序趋势 | `geo_analyze_ntl_trend`（仅当窗口足够长且输入有序） |
