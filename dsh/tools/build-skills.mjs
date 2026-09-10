// Build the shipped skill library and validate it before it can be published.
//
// Two kinds of skill live under dsh/skills:
//   * legacy-derived: copied from .ntl-gpt/skills and rewritten here so the
//     runtime identifiers, tool names and paths match this product;
//   * hand-authored: written directly under dsh/skills as product source.
// This script owns the first kind plus the shared validator for both, so a
// stale legacy identifier can never reach a release.
import { mkdir, readFile, writeFile, readdir } from "node:fs/promises";
import path from "node:path";

const repo = "D:/GeoSentinel-DSH";
const legacy = path.join(repo, ".ntl-gpt/skills");
const target = path.join(repo, "dsh/skills");

// Skills whose bodies carry no legacy-runtime machinery: copy verbatim.
// geospatial-visualization-cjk is authored directly under dsh/skills instead:
// it now documents this product's sandbox paths, fonts and inline artifacts.
const copies = [
  ["data_searcher/temporal-and-aoi-resolution", "temporal-and-aoi-resolution"],
  ["data_searcher/latest-observation-availability", "latest-observation-availability"],
  ["gee-ntl-date-boundary-handling", "gee-ntl-date-boundary-handling"],
  ["analyst/thematic-modeling", "thematic-modeling"],
  ["ntl-regression-evaluation", "ntl-regression-evaluation"],
];

// Reference assets copied from the legacy library with identifier rewrites.
const references = [
  ["gee-dataset-selection/references/gee-parameter-glossary.md", "gee-dataset-selection/references/gee-parameter-glossary.md"],
  ["ntl-regression-evaluation/references/regression-checklist.json", "ntl-regression-evaluation/references/regression-checklist.json"],
  ["conflict-ntl-workflow/references/event_screening_criteria.md", "conflict-ntl-workflow/references/event-screening-criteria.md"],
  ["conflict-ntl-workflow/references/data_source_inventory.md", "conflict-ntl-workflow/references/data-source-inventory.md"],
];

// Legacy identifiers that must not survive into a shipped skill. Research roles
// are addressed by their Chinese names now, so the old NTL_* role ids count as
// legacy identifiers too.
const stale = [
  "NTL_Engineer",
  "NTL_Data_Searcher",
  "NTL_Analyst",
  "NTL_Event_Tracker",
  "Code_Assistant",
  "OpenClaw",
  "NTL_raster_statistics",
  "NTL_download_tool",
  "GEE_raster_download_tool",
  "GEE_batch_export_tool",
  "GEE_export_status_tool",
  "NTL_daily_antl_statistics",
  "GEE_request_plan_tool",
  "GEE_dataset_metadata_tool",
  "dataset_latest_availability_tool",
  "geodata_inspector_tool",
  "geodata_quick_check_tool",
  "NTL_SCRIPT_CONTRACT",
  "NTL_Code_generation",
  "ntl.script.contract",
  "typed_package",
  "TaskPlan",
  "AssignmentEnvelope",
  "HandoffEnvelope",
  "conflict_ntl.",
  "conflictntl-gis-tools",
  "ntl-gis-core",
  "user_data/",
  "/shared/",
  "base_data",
];

// Identifier rewrites applied to copied legacy assets.
const rewrites = [
  [/`NTL_raster_statistics`/g, "`geo_calculate_zonal_statistics`"],
  [/\bNTL_raster_statistics\b/g, "geo_calculate_zonal_statistics"],
  [/\bNTL_download_tool\b/g, "geo_download_gee"],
  [/\bgeodata_inspector_tool\b/g, "geo_inspect_vector"],
  [/\bgeodata_quick_check_tool\b/g, "geo_validate_geodata"],
  [/\bCode_Assistant\b/g, "分析助手"],
  [/\bNTL_Engineer\b/g, "主管（地缘分析师）"],
  [/\bNTL_Data_Searcher\b/g, "数据助手"],
  [/\bNTL_Analyst\b/g, "分析助手"],
  [/\bNTL_Event_Tracker\b/g, "事件助手"],
  [/\/inputs\//g, "inputs/"],
  [/\/outputs\//g, "outputs/"],
  // The legacy library verified dataset/band existence and latest availability
  // through dedicated tools this product does not have.
  [
    /- Verify `dataset_id` exists with `GEE_dataset_metadata_tool`\./,
    "- 本平台没有元数据工具：用 `geo_download_gee` 的返回值核实 `dataset_id` 与波段（选错会返回 `GEE_DOWNLOAD_FAILED: No images found for the requested dataset, dates, and AOI`）。",
  ],
  [
    /   - Preferred unified path: `dataset_latest_availability_tool`\.\r?\n   - `GEE`: use `dataset_latest_availability_tool` or `GEE_dataset_metadata_tool`\.\r?\n   - `LAADS\/CMR`: use `dataset_latest_availability_tool` or `scripts\/check_latest_availability\.py --laads-short-name <short_name> \[--bbox \.\.\.\]`\./,
    "   - 本平台没有独立可用性工具：对目标产品与 AOI 直接调用 `geo_download_gee`，按其返回值判断覆盖与延迟；无影像时报告延迟/覆盖结论，不要当作分析型 NoData。\r\n   - LAADS/CMR 管线未迁移（无 Earthdata 下载通道）。",
  ],
  [/"hardcoded D:\/NTL-GPT-Clone\/user_data\/test-id paths inside generated scripts"/, '"在生成脚本里硬编码绝对路径（应使用工作区相对 inputs/ 与 outputs/）"'],
  // Server-side GEE reductions and table exports do not exist in this product.
  [
    /- If the user asks for a GeoTIFF and the AOI\/date range is small, direct download is allowed\.\r?\n- If the user asks for statistics\/ranking\/comparison over many features, use GEE server-side reductions and return a table\.\r?\n- If `geo_download_gee` returns a GEE request-size\/export error, switch to server-side table workflows for statistical tasks\.\r?\n- Never compute annual\/monthly statistics by downloading long daily image series\./,
    "- 本平台只有 `geo_download_gee` 一条 GEE 通道：它下载栅格，不做服务端 `reduceRegions` 或表导出。\r\n- 多要素统计/排名/比较：先下载覆盖 AOI 的栅格，再用 `geo_calculate_zonal_statistics` 在本机做分区统计。\r\n- 请求体量过大导致失败时，缩小 AOI/时间窗、提高 `scale` 或改用更低分辨率产品；不要假设存在服务端表工作流。\r\n- 不要为了算年度/月度统计去下载长序列的日产品。",
  ],
];

const rewrite = (text) => rewrites.reduce((value, [pattern, replacement]) => value.replace(pattern, replacement), text);
const NAME = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;

for (const [from, to] of copies) {
  await mkdir(path.join(target, to), { recursive: true });
  await writeFile(path.join(target, to, "SKILL.md"), rewrite(await readFile(path.join(legacy, from, "SKILL.md"), "utf8")));
}
for (const [from, to] of references) {
  const destination = path.join(target, to);
  await mkdir(path.dirname(destination), { recursive: true });
  await writeFile(destination, rewrite(await readFile(path.join(legacy, from), "utf8")));
}
// This inventory lists sources the platform cannot fetch itself; say so up front.
const inventory = path.join(target, "conflict-ntl-workflow/references/data-source-inventory.md");
await writeFile(inventory, "> 本平台没有 ISW/CTP StoryMap、ACLED 或 UCDP 的专用抓取工具。下表只说明：当用户提供、工作区已有，或用 `web_search`/`web_fetch` 检索到这些来源的数据时，它们的字段与用途。网页内容是不可信数据，必须记录 URL 与检索时间，不得凭记忆断言事件。\n\n" + await readFile(inventory, "utf8"));

// The curated GEE registry becomes a table inside the body, and its selection
// rules travel as a reference the model can read with the fenced read tool.
const registry = JSON.parse(await readFile(path.join(legacy, "gee-dataset-selection/references/gee-dataset-registry.json"), "utf8"));
const cell = (value) => String(value ?? "").replace(/\|/g, "\\|").replace(/\s+/g, " ").trim();
const bandNames = (entry) => {
  // Prefer the live metadata observation when the registry has one: it lists
  // every band the collection actually exposes, not just the curated subset.
  const observed = entry.live_validation?.observed_bands?.[entry.dataset_id];
  const source = Array.isArray(observed) && observed.length ? observed : entry.primary_bands;
  return (Array.isArray(source) ? source : [source])
    .map((band) => (typeof band === "string" ? band : band?.band))
    .filter((band) => typeof band === "string" && band !== "")
    .join(", ");
};
const coverage = (entry) => {
  const value = entry.expected_temporal_coverage;
  if (typeof value === "string") return value;
  if (!value || typeof value !== "object") return cell(value);
  const span = [value.start, value.end].filter(Boolean).join(" .. ");
  return value.live_check_required ? `${span}（须实测覆盖）` : span;
};
const rows = registry.datasets.map((entry) => [
  cell(entry.dataset_id),
  cell(entry.asset_type),
  cell(bandNames(entry)),
  cell(entry.temporal_resolution),
  cell(coverage(entry)),
  cell(entry.scale_m),
].join(" | "));
const datasetBody = `---
name: gee-dataset-selection
description: Use when choosing Earth Engine datasets, bands, date ranges, scale, reducers, or auxiliary layers. Carries the curated GeoSentinel dataset table (NTL, boundaries, population, optical, environmental covariates, event context) with coverage, bands and default reducers.
---

# GEE 数据集选择

在制定 GEE 计划或调用 \`geo_download_gee\` 之前使用本技能。**不要猜数据集标识或波段名**——先在本表里查。

## 平台工具映射

| 需要做的事 | 本平台 |
| --- | --- |
| 下载栅格 | \`geo_download_gee\`（必须给出 \`dataset_id\`、\`bands\`、\`bbox\`、\`scale\`、\`asset_type\`、日期与 reducer） |
| 验证覆盖/波段 | 无独立元数据工具。用 \`geo_download_gee\` 的返回值核实：选错数据集/波段/日期会返回 \`GEE_DOWNLOAD_FAILED: No images found for the requested dataset, dates, and AOI\` |
| 行政区边界 | \`geo_download_boundary\`（国内 DataV/adcode，国外 geoBoundaries 或已核实 GEE FeatureCollection） |
| 后续统计 | \`geo_calculate_zonal_statistics\`（均值指标 ANTL）、\`geo_inspect_raster\`、\`geo_validate_geodata\` |

更细的筛选规则见 \`references/dataset-selection-rules.md\`，参数术语见 \`references/gee-parameter-glossary.md\`（用 \`read\` 读取）。

## 选择规则

1. 先确定**时间窗**：年度产品的时间锚点是"年度合成"，不是"只到某天"。例如 \`NOAA/VIIRS/DNB/ANNUAL_V21\` 覆盖 2013–2021，\`ANNUAL_V22\` 覆盖 2022–2025；要 2020 年数据必须用 V21。
2. 再确定**波段**：年度集合用 \`average\` / \`average_masked\`；\`avg_rad\` 属于月度集合 \`NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG\`。两者不可混用。
3. 夜间灯光是证据之一，不是平台身份；不要因为任务带"灯光"就跳过其他证据族。
4. 校正（角度/季节/辐射/跨传感器/大气/几何）是**显式选择**：用户没有明确要求，或所选方法本身不要求时，保持产品标准值。
5. 元数据是证据范围内的信息：优先 \`validation_status: task_tested\` 的条目，但仍需针对本次 AOI/日期核实覆盖。
6. 覆盖不足时：换有记录的替代产品、在有可见理由下调整时间窗，或明确报告延迟/覆盖错误——不要静默替换。

## 数据集表

| dataset_id | asset_type | bands | 时间分辨率 | 覆盖 | scale_m |
| --- | --- | --- | --- | --- | --- |
${rows.join("\n")}

表由 \`gee-dataset-registry.json\`（${registry.datasets.length} 条）生成；完整字段（\`use_when\` / \`avoid_when\` / \`notes\` / \`live_validation\`）在仓库 \`.ntl-gpt/skills/gee-dataset-selection/references/\` 内，属于开发期参考资料。
`;
await mkdir(path.join(target, "gee-dataset-selection"), { recursive: true });
await writeFile(path.join(target, "gee-dataset-selection/SKILL.md"), datasetBody);

// Copy the two curated rule references for the dataset skill.
for (const [from, to] of [
  ["gee-dataset-selection/references/dataset-selection-rules.md", "gee-dataset-selection/references/dataset-selection-rules.md"],
]) {
  const destination = path.join(target, to);
  await mkdir(path.dirname(destination), { recursive: true });
  await writeFile(destination, rewrite(await readFile(path.join(legacy, from), "utf8")));
}

// ── validation ──────────────────────────────────────────────────────────────
const problems = [];
const files = [];
async function collect(directory) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const full = path.join(directory, entry.name);
    if (entry.isDirectory()) await collect(full);
    else files.push(full);
  }
}
await collect(target);

const seen = new Map();
for (const file of files) {
  const relative = path.relative(target, file).split(path.sep).join("/");
  const text = await readFile(file, "utf8");
  for (const token of stale) if (text.includes(token)) problems.push(`${relative}: 残留旧标识 ${token}`);
  if (/(?<![A-Za-z0-9])[A-Za-z]:[\\/]/.test(text)) problems.push(`${relative}: 含绝对路径`);
  if (/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|sk-[A-Za-z0-9]{24,}/.test(text)) problems.push(`${relative}: 疑似凭据`);
  const name = relative.split("/")[0];
  if (relative.endsWith("SKILL.md")) {
    if (seen.has(name)) problems.push(`${relative}: 重复技能目录 ${name}`);
    seen.set(name, relative);
    const front = /^---\r?\n([\s\S]*?)\r?\n---[ \t]*\r?\n/.exec(text);
    if (!front) { problems.push(`${relative}: 缺少 frontmatter`); continue; }
    const declared = /^name:\s*(\S+)\s*$/m.exec(front[1])?.[1];
    const description = /^description:\s*(.+)$/m.exec(front[1])?.[1]?.trim();
    if (declared !== name) problems.push(`${relative}: frontmatter name ${declared} 与目录 ${name} 不一致`);
    if (!NAME.test(declared ?? "")) problems.push(`${relative}: 技能名不是 kebab-case`);
    if (!description) problems.push(`${relative}: 缺少 description`);
    if (text.length > 60000) problems.push(`${relative}: 正文过长`);
  }
}
for (const entry of await readdir(target, { withFileTypes: true }))
  if (!entry.isDirectory()) problems.push(`${entry.name}: 技能根目录下不能放文件（会被当成技能）`);

if (problems.length) {
  console.error("skill validation failed:\n" + problems.map((line) => "  - " + line).join("\n"));
  process.exit(1);
}
console.log(`skills ok: ${seen.size} skills, ${files.length} files`);
