---
name: external-geo-services
description: Use when a task needs an address↔coordinate lookup, POI search, distance or route planning (Amap `mcp__amap__*`), or official dataset/coverage metadata (NASA CMR `mcp__cmr__*`). Explains what each remote service answers, how to cite it as an external source, and what must never be presented as platform observation.
---

# 外部地理服务（远程 MCP）

平台接了两台**远程** MCP 服务（无本地安装、无本地进程，只是出站 HTTPS 查询）。它们的返回值是
**外部来源**，与 `web_search`/`web_fetch` 同级：可以引用，但**不是平台观测**，也不能当已核实事实。

| 服务 | 工具 | 回答什么 | 典型用途 |
| --- | --- | --- | --- |
| 高德 LBS（`mcp__amap__*`） | `maps_geo` | 结构化地址 → 经纬度（含地标/建筑名） | 把"缅甸仰光港""上海外滩"这类地名转成坐标 |
| | `maps_regeocode` | 经纬度 → 行政区划地址 | 把事件坐标落到省/市/区，做行政区归属核对 |
| | `maps_text_search` / `maps_around_search` / `maps_search_detail` | 关键字/周边 POI 与详情 | 找港口、机场、医院、学校等设施，作为背景或核验线索 |
| | `maps_distance` | 两点距离（驾车/步行/球面） | 量级估算（不是路网测距） |
| | `maps_direction_driving` / `_walking` / `_transit_integrated` | 路径规划（驾车/步行/公交） | 通行距离与时长的量级参考 |
| NASA CMR（`mcp__cmr__*`） | `get_collections` | 官方数据集条目（含摘要、空间/时间范围） | **首次使用某数据集前查明来源与覆盖**，不许猜 |
| | `get_granules` | 某个数据集下的具体影像/文件条目 | 核对某时段是否真有产品、覆盖到不到 AOI |
| | `get_variables` | 科学变量与测量定义 | 弄清楚波段/变量到底代表什么 |
| | `get_citations` / `get_keywords` / `get_services` / `get_tools` | 引用与 DOI、官方词表、处理服务与工具 | 引用出处、统一术语、找官方处理链 |

CMR 每次最多返回 10 条，适合"确认存在与口径"，不适合当批量下载接口。

## 怎么用

1. **先判断是不是平台自己就能答**：观测数值、分区统计、栅格/矢量处理一律用平台工具与
   `geo_*`；只有"地名↔坐标""设施检索""数据集来源与覆盖"这类**目录/定位问题**才用外部服务。
2. **空间分析前先把坐标落成产物**：把外部返回的坐标写成 GeoJSON/CSV 放进 `outputs/`（自己用 `write`
   写文件时注明来源与检索时间），再交给 `geo_*` 工具或容器计算。不要直接在正文里心算距离。
3. **引用规则**：写清"服务名 + 查询内容 + 检索时间"，例如
   `高德 maps_geo（查询"仰光港"，2026-09-10 21:40）`；CMR 要写集合名与 DOI/ID、查询时间。
   `geo_write_evidence` 里这类证据的 `kind` 用 `web`，`source` 写服务与查询串。
4. **限制必须写明**（按需选择）：
   - 高德：国内 POI 与地址覆盖好，境外地址解析能力有限；返回的行政区划按高德口径，不等于官方边界；
     距离/路径是服务商按自有路网计算，与平台栅格/矢量测距不是同一口径。
   - CMR：只覆盖 NASA/合作方的目录；条目存在不等于数据已下载或可用于本任务；覆盖范围与投影要在下载后
     用 `geo_inspect_raster`/`geo_validate_geodata` 复核。
5. **不得做的事**：把地名解析结果当成"事件发生地已确认"；把 POI 名称当成官方名称；用外部服务替代平台观测；
   不做交叉核验就引用单一来源的坐标。

## 工具可用性（不要当成理所当然）

远程 MCP 工具由管理员在部署配置里开启，其中高德这组需要 `AMAP_API_KEY`。未配置该密钥时，对应插件行
会加载失败而被平台容忍（`failOnStartupError: false`），表现为 **`mcp__amap__*` 根本不出现在工具清单里**，
而不是报错。

- 工具不在清单里：直接说明"本部署未提供该服务"，改用 `web_search`/`web_fetch` 或平台自身数据；
- **绝不**凭记忆编造坐标、距离或路径，也不要在回答里写"已查询高德"；
- CMR 一行不需要密钥，所以 `mcp__cmr__*` 通常可用；两组互不影响，不要因为一组缺失就放弃另一组。

## 与既有能力的分工

- 事件线索的地理定位：`事件助手` 用 `maps_geo`/`maps_regeocode`/`maps_around_search` 给出候选坐标，
  **仍须**用来源报道与平台数据交叉核对，并在回答里区分"来源给出"与"平台核实"。
- 可达性/路网分析：`分析助手` 用 `maps_distance`/`maps_direction_*` 做量级参考，再用共享数据里的路网
  （`share/缅甸地理/…/图1-3.mdb` → `ogr2ogr` 转 GPKG）做平台侧的距离密度与覆盖率计算，两者口径分开写。
- 数据集来源核查：`数据助手` 用 CMR 确认数据集存在、时段覆盖与变量定义，再决定是否用
  `geo_download_gee` 获取；CMR 结论不等于数据已在平台上可用。
