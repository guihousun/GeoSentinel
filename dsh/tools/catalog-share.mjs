#!/usr/bin/env node
/**
 * Generate the shared-data catalog (`CATALOG.md` + `catalog.json`).
 *
 * A research agent should be able to read ONE document to know what the shared
 * library holds, instead of opening every file and guessing how to parse it.
 * The catalog is generated from the real files, so it cannot drift the way a
 * hand-written description does; regenerate it whenever the library changes.
 *
 *   node dsh/tools/catalog-share.mjs                 # 预览（不写文件）
 *   node dsh/tools/catalog-share.mjs --write         # 写入每个根目录的 CATALOG.md
 *   node dsh/tools/catalog-share.mjs --write --no-docker   # 跳过 .mdb 的容器内探查
 *
 * Configuration is the same as the product: `GEO_SHARE_DIR` (one root) and
 * `GEO_SHARE_DIRS` (名称=路径 pairs). When neither is in the environment the
 * tool falls back to `dsh/.env`, so it can run from a plain shell.
 */
import { readFile, readdir, stat, writeFile } from "node:fs/promises";
import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { parseShareDirs } from "../plugins/platform/share.mjs";
import { readShapefilePreview, readDbf } from "../plugins/platform/shapefile.mjs";
import { readGeoTiffPreview } from "../plugins/platform/tiff.mjs";

const here = path.dirname(fileURLToPath(import.meta.url));
const argv = process.argv.slice(2);
const has = (name) => argv.includes("--" + name);
const value = (name, fallback) => {
  const index = argv.indexOf("--" + name);
  return index === -1 ? fallback : argv[index + 1];
};
const WRITE = has("write");
const USE_DOCKER = !has("no-docker");
const MAX_FILES = Number(value("max-files", 600)) || 600;
const TEXTUAL = new Set([".csv", ".tsv", ".xlsx", ".xls"]);
const SHEET_EXT = new Set([".xlsx", ".xls", ".csv", ".tsv"]);
const RASTER_EXT = new Set([".tif", ".tiff"]);
const VECTOR_EXT = new Set([".shp", ".geojson", ".json", ".gpkg"]);
const ARCHIVE_EXT = new Set([".rar", ".zip", ".7z"]);
const IMAGE_EXT = new Set([".jpg", ".jpeg", ".png", ".webp"]);
const DOC_EXT = new Set([".pdf", ".doc", ".docx", ".ppt", ".pptx", ".txt", ".md"]);

const bytes = (value) => value >= 1048576 ? (value / 1048576).toFixed(1) + " MiB" : value >= 1024 ? (value / 1024).toFixed(0) + " KiB" : value + " B";

/** Read GEO_SHARE_DIR / GEO_SHARE_DIRS from `dsh/.env` when the shell has none. */
async function shareEntries() {
  const fromEnv = parseShareDirs();
  if (fromEnv.length) return fromEnv;
  try {
    const text = await readFile(path.join(here, "..", ".env"), "utf8");
    const env = {};
    for (const line of text.split(/\r?\n/)) {
      const match = /^(GEO_SHARE_DIR|GEO_SHARE_DIRS)\s*=\s*(.*)$/.exec(line.trim());
      if (match) env[match[1]] = match[2].trim().replace(/^"(.*)"$/, "$1");
    }
    return parseShareDirs(env);
  } catch {
    return [];
  }
}

async function walk(root, prefix = "", files = []) {
  let entries;
  try {
    entries = await readdir(path.join(root, prefix), { withFileTypes: true });
  } catch {
    return files;
  }
  for (const entry of entries) {
    if (files.length >= MAX_FILES) return files;
    if (entry.name.startsWith(".") || entry.isSymbolicLink()) continue;
    // The catalog itself lives in the root: listing it would be noise.
    if (!prefix && (entry.name === "CATALOG.md" || entry.name === "catalog.json")) continue;
    const relative = prefix ? `${prefix}/${entry.name}` : entry.name;
    if (entry.isDirectory()) {
      await walk(root, relative, files);
      continue;
    }
    const info = await stat(path.join(root, relative)).catch(() => null);
    if (info?.isFile()) files.push({ relative, real: path.join(root, relative), size: info.size, mtime: info.mtime });
  }
  return files;
}

/** One python process for every workbook, so the catalog stays fast. */
function inspectSheets(file) {
  const script = `import json,sys
import pandas as pd
out = {"sheets": []}
p = sys.argv[1]
try:
    book = pd.ExcelFile(p) if p.lower().endswith((".xlsx", ".xls")) else None
    names = book.sheet_names if book else [None]
    for name in names:
        frame = book.parse(name, nrows=200) if book else pd.read_csv(p, nrows=200)
        columns = [str(c) for c in frame.columns][:24]
        out["sheets"].append({"name": name, "columns": columns, "sample_rows": int(len(frame))})
except Exception as error:
    out["error"] = str(error)[:200]
print(json.dumps(out, ensure_ascii=False))
`;
  return new Promise((resolve) => {
    // Windows Python defaults to the ANSI code page when its stdout is a pipe;
    // force UTF-8 or sheet names arrive as mojibake.
    const child = spawn("python", ["-X", "utf8", "-c", script, file], { windowsHide: true, env: { ...process.env, PYTHONIOENCODING: "utf-8" } });
    let output = "";
    child.stdout.on("data", (chunk) => { output += chunk.toString(); });
    child.stderr.on("data", () => {});
    child.on("error", () => resolve(null));
    child.on("close", () => {
      try { resolve(JSON.parse(output.trim().split(/\r?\n/).pop())); } catch { resolve(null); }
    });
  });
}

/**
 * List layers of an ESRI Personal Geodatabase with the shipped GIS image.
 * Host Node has no GDAL and the ODBC driver is Linux-only, so this mirrors what
 * the sandbox does: mount the share root read-only and run `ogrinfo` as root
 * (the driver config is baked into the image).
 */
function inspectGeodatabase(root, relative, image) {
  const containerPath = "/workspace/share/" + relative;
  const script = [
    "set -e",
    `ogrinfo -so "${containerPath}" 2>&1 | sed -n '2,40p'`,
  ].join("; ");
  return new Promise((resolve) => {
    const child = spawn("docker", ["run", "--rm", "--mount", `type=bind,source=${root},target=/workspace/share,readonly`, "--entrypoint", "sh", image, "-c", script], { windowsHide: true });
    let output = "";
    child.stdout.on("data", (chunk) => { output += chunk.toString(); });
    child.stderr.on("data", (chunk) => { output += chunk.toString(); });
    child.on("error", () => resolve(null));
    child.on("close", (code) => {
      const text = output.trim();
      if (/MDB Tools driver not found|Unable to initialize ODBC connection/.test(text))
        return resolve(`镜像 ${image} 缺少 MDB ODBC 驱动：请用 geosentinel-gis:0.1，或用 --image 指定含 mdbtools 的镜像`);
      resolve(code === 0 ? text : text || null);
    });
  });
}

function classify(file) {
  const extension = path.extname(file).toLowerCase();
  if (VECTOR_EXT.has(extension)) return "矢量";
  if (RASTER_EXT.has(extension)) return "栅格/影像";
  if (SHEET_EXT.has(extension)) return "表格";
  if (extension === ".mdb") return "地理数据库";
  if (ARCHIVE_EXT.has(extension)) return "压缩包";
  if (IMAGE_EXT.has(extension)) return "图片";
  if (DOC_EXT.has(extension)) return "文档";
  return "其他";
}

async function describe(file, entry, options) {
  const extension = path.extname(file.real).toLowerCase();
  const record = { path: file.relative, size: file.size, kind: classify(file.real), mtime: file.mtime.toISOString().slice(0, 10) };
  try {
    if (extension === ".shp") {
      const preview = await readShapefilePreview(file.real, 1);
      const table = await readDbf(file.real);
      const prj = await readFile(file.real.replace(/\.shp$/i, ".prj"), "utf8").catch(() => "");
      record.vector = {
        geometry: preview.shapeType ?? "未知",
        features: preview.featureCount,
        crs: /PROJCS/.test(prj) ? "投影坐标系（见 .prj）" : /GEOGCS/.test(prj) ? "地理坐标系（见 .prj）" : "未提供 .prj",
        fields: (preview.attributeFields ?? table?.fields ?? []).slice(0, 24),
        note: preview.kind === "unsupported" ? preview.reason : "",
      };
    } else if (RASTER_EXT.has(extension)) {
      const preview = await readGeoTiffPreview(file.real, file.size);
      record.raster = {
        size: preview.width && preview.height ? `${preview.width}x${preview.height}` : undefined,
        bands: preview.bands,
        sampleFormat: preview.sampleFormat,
        bits: preview.bitsPerSample,
        crs: preview.crs,
        valueRange: preview.valueRange,
        note: preview.note,
      };
    } else if (TEXTUAL.has(extension)) {
      record.table = await inspectSheets(file.real);
    } else if (extension === ".mdb" && options.docker) {
      record.geodatabase = await inspectGeodatabase(entry.root, file.relative, options.image);
    }
  } catch (error) {
    record.note = "无法解析：" + String(error.message).slice(0, 120);
  }
  return record;
}

function renderMarkdown(entry, records, generatedAt) {
  const totals = records.reduce((sum, record) => sum + record.size, 0);
  const byKind = new Map();
  for (const record of records) byKind.set(record.kind, (byKind.get(record.kind) ?? 0) + 1);
  const lines = [
    `# 共享数据目录：${entry.name || "(默认根)"}`,
    "",
    `> 由 \`dsh/tools/catalog-share.mjs\` 于 ${generatedAt} 从真实文件生成，共 ${records.length} 个文件（${bytes(totals)}）。`,
    `> 数据是**只读**的：宿主侧用 \`share/<相对路径>\` 读取，分析容器里同一路径为 \`/workspace/share/<相对路径>\`（只读挂载）。`,
    `> 读取方式：文本/表格用 \`read\`/\`read_document\`；矢量与栅格用容器内的 GDAL（\`ogrinfo\`/\`gdalinfo\`）或先转 GeoPackage 再用 geopandas；\`.mdb\` 必须先转换（见下方“调用方式”）。`,
    "",
    `分类统计：${[...byKind].map(([kind, count]) => `${kind} ${count}`).join("、")}`,
    "",
    "| 相对路径 | 类型 | 大小 | 关键信息 |",
    "| --- | --- | --- | --- |",
  ];
  for (const record of records) {
    const bits = [];
    if (record.vector) bits.push(`几何 ${record.vector.geometry}`, `要素 ${record.vector.features ?? "?"}`, record.vector.crs, record.vector.fields?.length ? `字段：${record.vector.fields.slice(0, 8).join(", ")}` : "");
    if (record.raster) bits.push(record.raster.size ? `${record.raster.size} 像元` : "", record.raster.bands ? `${record.raster.bands} 波段` : "", record.raster.sampleFormat ? `${record.raster.sampleFormat}${record.raster.bits ? " " + record.raster.bits + " 位" : ""}` : "", record.raster.crs ? `CRS ${record.raster.crs}` : "", record.raster.valueRange ? `取值 ${record.raster.valueRange[0]}–${record.raster.valueRange[1]}` : "", (record.raster.note ?? "").replace(/；[^；]*浏览器[^；]*/u, ""));
    if (record.table?.sheets?.length) bits.push(record.table.sheets.map((sheet) => `${sheet.name ?? "(单表)"} ${sheet.sample_rows} 行样本｜列：${sheet.columns.slice(0, 8).join(", ")}`).join("；"));
    if (record.table?.error) bits.push("表格解析失败：" + record.table.error);
    if (record.geodatabase) bits.push(record.geodatabase.split(/\r?\n/).slice(1, 12).map((line) => line.trim()).filter(Boolean).join(" / "));
    if (record.note) bits.push(record.note);
    if (entry.name === "" && record.path.includes("/")) bits.push("");
    lines.push(`| \`${record.path}\` | ${record.kind} | ${bytes(record.size)} | ${bits.filter(Boolean).join("；")} |`);
  }
  lines.push("", "## 调用方式（按类型）", "",
    "| 类型 | 怎么做 |", "| --- | --- |",
    "| 表格 `.xlsx/.csv` | `read_document` 直接读（宿主侧），或容器内 `pandas.read_excel/read_csv` |",
    "| 矢量 `.shp/.geojson/.gpkg` | 容器内 `ogrinfo`/`ogr2ogr`，或先 `ogr2ogr -f GPKG` 转换后用 geopandas；平台注册工具只认容器里的 `inputs/ outputs/ previous/ share/` 相对路径 |",
    "| 影像 `.tif` | 容器内 `gdalinfo`/rasterio；平台侧 `geo_inspect_raster` 支持已放进 `inputs/` 的栅格 |",
    "| 地理数据库 `.mdb` | **先转换**：容器内 `ogr2ogr -f GPKG out.gpkg \"<mdb>\" <图层名> [-spat ...]`；geopandas/pyogrio 自带 GDAL 不含 PGeo，直接读会报 “not recognized as being in a supported file format” |",
    "| 压缩包 `.rar/.zip` | 宿主侧解压（Windows 自带 `tar` 可解 zip；rar 需 WinRAR），解出的文件放入 `inputs/` 或共享根目录 |",
    "| 扫描 PDF | 需 OCR（本机 RapidOCR/其他 OCR），页与印刷页可能相差固定偏移，引用数字前必须核对原页 |",
    "| 图片 `.jpg/.png` | 只作对照，不作为计算输入 |",
    "");
  return lines.join("\n");
}

async function main() {
  const entries = await shareEntries();
  if (!entries.length) {
    console.error("未配置共享数据：请在环境或 dsh/.env 设置 GEO_SHARE_DIR / GEO_SHARE_DIRS");
    process.exitCode = 1;
    return;
  }
  // The environment may carry a stale pinned digest (the release flow sets one),
  // so an explicit flag wins and the chosen image is always printed.
  const image = String(value("image", process.env.GEO_GIS_IMAGE || "geosentinel-gis:0.1"));
  const generatedAt = new Date().toISOString().slice(0, 10);
  console.log(`容器镜像（用于 .mdb 探查）：${image}`);
  for (const entry of entries) {
    const files = await walk(entry.root);
    const records = [];
    for (const file of files) records.push(await describe(file, entry, { docker: USE_DOCKER, image }));
    records.sort((left, right) => left.path.localeCompare(right.path, "zh-Hans"));
    const markdown = renderMarkdown(entry, records, generatedAt);
    const label = entry.name || "(默认根)";
    console.log(`\n=== ${label} → ${entry.root}（${records.length} 个文件）===`);
    for (const record of records) console.log(`  ${record.kind}\t${bytes(record.size)}\t${record.path}`);
    if (!WRITE) {
      console.log("  （预览模式：加 --write 才会写入 CATALOG.md / catalog.json）");
      console.log(markdown.split("\n").slice(0, 12).join("\n"));
      continue;
    }
    await writeFile(path.join(entry.root, "CATALOG.md"), markdown);
    await writeFile(path.join(entry.root, "catalog.json"), JSON.stringify({ root: entry.root, name: entry.name, generatedAt, files: records }, null, 2));
    console.log(`  已写入 ${path.join(entry.root, "CATALOG.md")} 与 catalog.json`);
  }
}

await main();
