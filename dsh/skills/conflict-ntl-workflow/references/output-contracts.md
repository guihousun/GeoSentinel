# 冲突事件夜间灯光工作流：产物结构

本平台不提供旧的运行信封字段（运行目录、线程标识、代理系统清单）。产物是工作区里的文件，路径用工作区相对路径，通常写 `outputs/<作业ID>/`。

## 1. 事件筛选表 `event_screening.csv`

| 字段 | 含义 |
| --- | --- |
| `event_id` | 来源内唯一标识（或规范化后的稳定标识） |
| `event_date_utc` | 事件日期（UTC；仅日期语义要注明） |
| `time_quality` / `time_score` | 时间可信度等级与分数 |
| `coord_quality` / `coord_score` | 坐标精度等级与分数 |
| `source_quality` / `source_score` | 来源可信度等级与分数 |
| `round1_score` | 可追溯性总分 |
| `round1_event_candidate_status` | `event_candidate` / `insufficient` 等 |
| `ntl_relevance_level` | `ntl_applicable` / `ntl_uncertain` / `ntl_not_applicable` |
| `conflict_ntl_candidate` | 是否进入夜间灯光核查 |
| `event_confirmation_status` | 来源加固状态（官方/运营方/通讯社/暂无） |
| `verification_notes` | 备注与未决项 |

字段与阈值见 `event-screening-criteria.md`。

## 2. 分析单元 `analysis_units.geojson`

每个要素是一个可核查单元，至少带：`analysis_unit_id`、`unit_type`（`buffer` / `admin`）、`event_date_utc`、`source_event_ids`、`source_event_count`、`aoi_count`、`radius_km`（缓冲区）。

同日重叠缓冲区先融合（`geo_dissolve_intersections`）再送分析，避免同一像元被重复计入。

## 3. 夜间灯光核查表 `ntl_verification.csv`

至少带：`aoi_id`、`event_id`、`event_date_utc`、`aoi_type`、`metric`（如 `ANTL`）、`baseline_window`、`event_window`、`valid_days`、`valid_pixels`、`change_absolute`、`change_percent`、`quality_notes`。

指标定义、单位与空间支持必须写明；缺测夜次按缺测处理。

## 4. 案例报告 `case_report.md`

用 `geo_write_report` 生成，`source_paths` 引用上面真实存在的文件。报告结构：

1. 事件与来源（含检索时间与 `as_of`）；
2. 筛选结果与未通过项；
3. AOI 与聚合规则；
4. 观测产品、日期语义与质量；
5. 夜间灯光结果（数值、单位、覆盖）；
6. 非归因表述与替代解释；
7. 限制与未决项。
