---
name: gee-ntl-date-boundary-handling
description: "Use for daily/event NTL timing and AOI handling: VIIRS first-night selection, filterDate end-exclusive windows, timezone conversion, event buffers, and no-data guards."
---

# GEE NTL 日期与边界处理

## 目的

避免夜间灯光按日/按事件分析时的常见错误：

- `filterDate` 结束端排他导致的空窗口；
- 白天地震等事件后的首夜影像取错；
- 把「当地首夜日期」当成「UTC 索引的文件日期」；
- AOI 未核验（拿硬编码 bbox 代替行政区）；
- reduce 参数缺失导致的失败或不可复现。

核心区分：

- `local_first_night_date`：用于解释的当地夜间日期。
- `utc_file_date`：UTC 索引产品、GEE `system:time_start` 或官方日文件实际使用的日期。
- 当地采集时刻不固定；没有元数据时按 `00:30–02:30` 当地作为候选区间。

## 何时使用

- `VNP46A2` / VIIRS 日尺度分析；
- 事件影响评估（灾前 / 首夜 / 灾后）；
- 震中缓冲区 ANTL 统计；
- 事件时间对时区敏感、或存在同日/无数据风险的场景。

普通的年度/月度省级统计不触发本技能，除非同时涉及事件窗口、首夜逻辑、日尺度影像或时间不确定。

## 关键规则

1. `filterDate(start, end)` 结束端不含 `end`；单日查询用 `end = start + 1 天`。
2. 事件发生在当地过境（常见 `00:30–02:30` 当地）之后时，`local_first_night_date = D+1`。
3. 官方日文件若按 UTC 采集/文件日期索引，先把当地首夜采集时刻换算到 UTC，再选文件。
4. UTC 日期接近午夜或本身有歧义时，用可信依据核实：像元级 `UTC_Time`（仅当源产品覆盖目标日期）、官方产品元数据、LAADS/CMR 粒度时间。核实不了就返回 `needs_verification`，不要编造具体时刻。
5. AOI 优先用已核验的行政边界，不用无依据的 bbox 代替命名区域。
6. reduce 显式给出 `scale`、`maxPixels`，必要时加 `tileScale`；返回影像数与无数据区间。

## 执行清单

1. 明确事件 UTC 时间、当地时区、当地首夜标签。
2. 确定候选采集窗口并换算到 UTC，检查是否跨越 UTC 日期边界。
3. 按产品日期约定选择影像/文件，并记录 `local_first_night_date` 与 `utc_file_date`。
4. 典型窗口：灾前 `event-7d … event`，首夜 `event … event+1d`，灾后 `event+7d … event+14d`。
5. 汇总影像数、无数据时段与边界来源。

## 歧义核验流程

当地首夜标签与 UTC 索引文件日期可能不一致时：

1. 确认事件 UTC 时间、当地时区、当地首夜标签。
2. 构造候选当地采集窗口（通常 `00:30–02:30` 当地，除非元数据更窄）。
3. 换算到 UTC，判断是否跨 UTC 日期边界。
4. 若日期选择会改变查询结果，用下列之一核实：像元级 `UTC_Time`（VNP46A1，且覆盖目标日期）、官方产品元数据、LAADS/CMR 粒度时间。
5. 同时记录 `local_first_night_date` 与 `utc_file_date`；无法安全区分时返回 `needs_verification`。

## 边界策略

- 首选：获取阶段已核验的行政区几何。
- 允许回退：震中缓冲区（25/50/100 km）做影响分析。
- 避免：给命名城市/省份用无依据的硬编码 bbox。
- AOI 不确定时向用户确认，不要猜。

## 输出契约（建议）

至少返回：`period_name`、`start_date`、`end_date`、`image_count`、`status`；日期对 UTC 敏感时再加 `local_first_night_date`、`local_acquisition_time`、`utc_acquisition_time`、`utc_file_date`；以及 `buffer_name` 或 `region_id`、`antl_mean`（可选 `antl_std`）与 `notes`（首夜规则、时区决策、边界来源）。

## 案例要点

- 缅甸 2025-03-28 地震：当地首夜采集约 2025-03-29 00:30–02:30 MMT，对应 UTC 2025-03-28 18:00–20:00；UTC 索引文件取 2025-03-28，当地夜间日期另记。
- 伊朗官方 VJ/DNB：当地 02-29 00:30–02:30 可对应 UTC 02-28 深夜；按产品日期约定选文件，不按当地日历日期直接选。
- 缓冲区 ANTL（25/50/100 km）在缺少行政边界时可用；`scale=500 + maxPixels + tileScale` 能显著降低 reduce 失败率。

## 反模式

- `filterDate("2025-03-29", "2025-03-29")`（必然为空）。
- 仅按当地首夜日历日期选 UTC 索引的官方日文件。
- 在可服务端 map/reduce 时仍写大量 `getInfo()` 循环。
- 无数据时无界重试而不调整窗口。
- 用硬编码绝对路径写产物。

## 参考

实现片段（日期助手、时区换算、VNP46A1 UTC_Time 核验、集合构建、稳健 reduce、无数据保护）见 `references/snippets.md`，按需读取，不必全量套用。
