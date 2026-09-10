---
name: analysis-ready-preprocessing
description: Use when turning downloads into analysis-ready rasters or vectors: clipping, reprojection, mosaicking, compositing, grid alignment, QA and boundary checks. Not the place to choose task-specific baselines or thresholds.
---

# 分析就绪预处理

- 记录每个输入、变换、参数、输出、有效像元检查与覆盖检查。
- **先检查再处理**：`geo_validate_geodata` 确认栅格/矢量有效性与网格兼容；`geo_inspect_raster` 看 CRS、范围、NoData 与统计；`geo_inspect_vector` 看要素数、字段与几何类型。
- 按矢量范围裁剪用 `geo_clip_raster`（`all_touched` 要写明，默认与选择会影响边缘像元）。
- 重投影用 `geo_reproject_raster`；分类数据显式用 `nearest`，连续数据才考虑插值，并记录方法。
- 对齐网格的多期合成用 `geo_composite_ntl_rasters`（写明方法）；镶嵌兼容栅格用 `geo_mosaic_rasters`。
- 行政区边界用 `geo_download_boundary` 获取，并核验区数与名称；中国城市请求 `scope=children` 取下级区划，不能拿整市面替代区级边界。国外用 geoBoundaries（ISO3+ADM）或已核实的 GEE FeatureCollection。
- 不在此决定任务特有的基线、阈值、模型结构或事件比较策略——那些属于分析助手。
- 默认不做校正/协调；只有用户明确要求或所选方法本身要求时才做。
- 观测不合格时明确报错（数据契约无效、观测质量不足），不要带着含糊数据往下走；缺测/云覆盖/QA 不合格的时相按缺测处理，不要插值。
