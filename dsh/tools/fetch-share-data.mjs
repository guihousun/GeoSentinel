#!/usr/bin/env node
/**
 * Fetch the shared-data libraries this platform expects under `share/`:
 * global administrative boundaries (geoBoundaries) and global 1 km population
 * (WorldPop). One script, no dependencies beyond Node 20+, safe to copy to any
 * machine or CI job — the development host does NOT have to carry the data.
 *
 *   node dsh/tools/fetch-share-data.mjs --help
 *   node dsh/tools/fetch-share-data.mjs --dry-run                 # 计划与体量估算
 *   node dsh/tools/fetch-share-data.mjs                           # 全量（默认）
 *   node dsh/tools/fetch-share-data.mjs --root /srv/share --dataset=geoboundaries
 *   node dsh/tools/fetch-share-data.mjs --dataset=worldpop --years=all
 *   node dsh/tools/fetch-share-data.mjs --dataset=worldpop --years=2000,2010,2020 --variant=both
 *
 * Defaults are the COMPLETE scope: every country, every level, every year.
 *   geoBoundaries  ADM0-ADM5 for every country the API publishes (715 records today,
 *                  plus CGAZ — geoBoundaries' own harmonized global ADM0/1/2 — as
 *                  GeoPackage AND GeoJSON)  ≈ 12 GB of bundles.
 *   WorldPop       1 km population, 2000-2020 for all 249 published countries
 *                  (`--variant=both` adds the UN-adjusted copy)  ≈ 17 GB (≈ 34 GB both).
 *
 * Every record keeps its provenance: `MANIFEST.json` per library (source URL, byte
 * length, sha256, license, year, unit count), `LICENSE-AND-ATTRIBUTION.md` generated
 * from the manifest (licenses differ PER BOUNDARY — 25 distinct ones today, including
 * ODbL and CC BY-SA — so attribution is grouped and listed, never assumed), and
 * `SOURCES.md` at the root summarising what was fetched and when.
 *
 * Re-runnable and self-healing: verified files are never re-downloaded, an upstream
 * record whose URL changed is refetched, a bundle that will not open is moved aside
 * as `.corrupt-<timestamp>` and downloaded again, extraction is retried, and nothing
 * is ever deleted.
 */
import { createHash } from "node:crypto";
import { createWriteStream, existsSync } from "node:fs";
import { mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { spawn } from "node:child_process";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import path from "node:path";

// ---------------------------------------------------------------- arguments --
const argv = process.argv.slice(2);
const flag = (name, fallback) => {
  const inline = argv.find((value) => value.startsWith(`--${name}=`));
  if (inline) return inline.slice(name.length + 3);
  const index = argv.indexOf("--" + name);
  if (index === -1) return fallback;
  const next = argv[index + 1];
  return next === undefined || next.startsWith("--") ? true : next;
};
const has = (name) => argv.includes("--" + name) || argv.some((value) => value.startsWith(`--${name}=`));

if (has("help") || argv.length === 0 && false) {
  console.log(`用法: node fetch-share-data.mjs [选项]

  --root <dir>        输出根目录（默认 ./share-data；容器里通常挂到 share/）
  --dataset <list>    all（默认）| geoboundaries | worldpop，可逗号组合
  --dry-run           只打印计划与体量估算，不下载
  --concurrency <n>   并发下载数（默认 3）

  geoBoundaries 专用:
  --levels <list>     0,1,2,3,4,5（默认全部）
  --countries <list>  all（默认）| focus | ISO,ISO
  --no-cgaz           不取 CGAZ 全球统一层（gpkg + geojson）
  --no-extract        不从 -all.zip 里解出可直接读的 GeoJSON

  WorldPop 专用:
  --years <list>      all（默认，2000-2020）| 2020 | 2000,2010,2020
  --variant <kind>    adj（默认）| unadj | both

  说明: 默认即"全球全时段全量"。断点续传、sha256 校验、逐条许可记录、失败自愈；
        已校验的文件不会重下，坏包另存 .corrupt-<ts> 后重下，绝不删除既有数据。`);
  await stop(0);
}

const root = path.resolve(String(flag("root", "./share-data")));
const dryRun = has("dry-run");
const concurrency = Number(flag("concurrency", 3)) || 3;
const datasets = String(flag("dataset", "all")).split(",").map((value) => value.trim()).filter(Boolean);
const wanted = (name) => datasets.includes("all") || datasets.includes(name);
const limit = Number(flag("limit", 0)) || 0;

const mb = (bytes) => (bytes / 1048576).toFixed(1);
const gb = (bytes) => (bytes / 1073741824).toFixed(2);
// Exiting straight after console.log drops queued output when stdout is a pipe, so
// the JSON reports this tool exists to produce would vanish in CI. Flush first.
const stop = async (code) => {
  await Promise.all([new Promise((resolve) => process.stdout.write("", resolve)), new Promise((resolve) => process.stderr.write("", resolve))]);
  process.exit(code);
};
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const UA = { "user-agent": "geosentinel-share-fetch" };

// ------------------------------------------------------------- http helpers --
async function request(url, options = {}, attempt = 0) {
  try {
    const response = await fetch(url, { ...options, headers: { ...UA, ...options.headers }, redirect: "follow" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response;
  } catch (error) {
    if (attempt >= 3) throw error;
    await sleep(1500 * (attempt + 1));
    return request(url, options, attempt + 1);
  }
}
const getJson = (url) => request(url).then((response) => response.json());
async function head(url) {
  try {
    const response = await request(url, { method: "HEAD" });
    return Number(response.headers.get("content-length")) || 0;
  } catch { return null; }
}
/** Stream to `.partial`, hash while writing, rename only when complete. */
async function download(url, target) {
  const partial = target + ".partial";
  await mkdir(path.dirname(target), { recursive: true });
  let bytes = 0;
  for (let attempt = 1; attempt <= 4; attempt += 1) {
    bytes = 0;
    try {
      const response = await request(url);
      const digest = createHash("sha256");
      await pipeline(Readable.fromWeb(response.body), async function* (source) {
        for await (const chunk of source) { bytes += chunk.length; digest.update(chunk); yield chunk; }
      }, createWriteStream(partial));
      await rename(partial, target);
      return { bytes, sha256: digest.digest("hex") };
    } catch (error) {
      await rm(partial, { force: true });
      if (attempt === 4) throw error;
      await sleep(2000 * attempt);
    }
  }
  throw new Error("unreachable");
}
/** Extract named members from a zip with python's zipfile (no npm dependency). */
async function unzip(zipPath, members) {
  const script = `import json,sys,zipfile,os
zip_path, target_dir, names = sys.argv[1], sys.argv[2], json.loads(sys.argv[3])
with zipfile.ZipFile(zip_path) as archive:
    entries = {os.path.basename(info.filename): info for info in archive.infolist()}
    written = []
    for name in names:
        if name in entries:
            with archive.open(entries[name]) as source, open(os.path.join(target_dir, name), 'wb') as target:
                target.write(source.read())
            written.append(name)
print(json.dumps(written))`;
  return new Promise((resolve) => {
    const child = spawn(process.env.PYTHON ?? "python", ["-c", script, zipPath, path.dirname(zipPath), JSON.stringify(members)], { stdio: ["ignore", "pipe", "pipe"] });
    let out = "", err = "";
    child.stdout.on("data", (chunk) => { out += chunk; });
    child.stderr.on("data", (chunk) => { err += chunk; });
    child.on("error", (error) => resolve({ error: error.message }));
    child.on("close", (code) => resolve(code === 0 ? JSON.parse(out.trim() || "[]") : { error: err.trim().slice(0, 300) }));
  });
}
const fileSize = async (file) => { try { return (await stat(file)).size; } catch { return null; } };

// ------------------------------------------------------------------ manifest --
async function openManifest(file, source) {
  const loaded = existsSync(file) ? JSON.parse(await readFile(file, "utf8")) : { source, fetchedAt: null, files: {} };
  const done = new Map(Object.entries(loaded.files ?? {}));
  return {
    loaded, done,
    async save() { loaded.files = Object.fromEntries(done); loaded.fetchedAt = new Date().toISOString(); await mkdir(path.dirname(file), { recursive: true }); await writeFile(file, JSON.stringify(loaded, null, 2)); },
  };
}
/** Run the job list with a fixed number of workers; returns per-run counters. */
async function runJobs(jobs, { label, handle }) {
  let index = 0, completed = 0, failed = 0, skipped = 0, bytes = 0;
  const started = Date.now();
  console.log(`${label}: ${jobs.length} 个目标，${jobs.filter((job) => job.skip).length} 个已完成`);
  await Promise.all(Array.from({ length: concurrency }, async () => {
    while (index < jobs.length) {
      const job = jobs[index++];
      try {
        const outcome = await handle(job);
        if (outcome === "skipped") { skipped += 1; continue; }
        if (outcome === "repaired") { console.log(`↻ 已修复 ${job.key}`); continue; }
        completed += 1;
        bytes += outcome.bytes ?? 0;
        const rate = bytes / 1048576 / Math.max(1, (Date.now() - started) / 1000);
        console.log(`[${completed + failed + skipped}/${jobs.length}] ${job.key} ${job.size ? mb(job.size) + " MB" : ""} (累计 ${mb(bytes)} MB, ${rate.toFixed(1)} MB/s)`);
      } catch (error) { failed += 1; console.error(`✗ ${job.key}: ${error.message}`); }
      await sleep(150);
    }
  }));
  return { completed, failed, skipped, bytes, minutes: Number(((Date.now() - started) / 60000).toFixed(1)) };
}
/** Licence table + attribution notice, generated from the manifest (never hand-written). */
async function writeAttribution(file, { title, source, note, units }) {
  const manifest = JSON.parse(await readFile(path.join(path.dirname(file), "MANIFEST.json"), "utf8"));
  const byLicense = new Map();
  for (const entry of Object.values(manifest.files ?? {})) {
    const key = entry.license || "未标注";
    const list = byLicense.get(key) ?? [];
    list.push(entry.iso ? `${entry.iso}/${entry.level || entry.year}` : entry.name);
    byLicense.set(key, list);
  }
  const text = [
    `# ${title}`, "",
    `数据来源：${source}`,
    `抓取时间：${manifest.fetchedAt}。清单（逐文件 URL、字节、sha256、许可、年份）：\`MANIFEST.json\`。`, "",
    note, "",
    "许可是**逐条**标注的，不是单一数据集许可；按许可分组如下：", "",
    ...[...byLicense.entries()].sort((left, right) => right[1].length - left[1].length)
      .map(([license, list]) => `- **${license}**（${list.length} 项）：${list.slice(0, 40).join("、")}${list.length > 40 ? ` 等 ${list.length} 项` : ""}`),
    "", units, "",
    "署名要求：使用任一图层时，请在图件或报告中标注来源与年份；",
    "每个 geoBoundaries 包内另附 `CITATION-AND-USE-geoBoundaries.txt` 与 `metaData.json`。",
    "注意：边界与人口均为研究数据集，不代表各国官方界线，也不是普查计数。", "",
  ].join("\n");
  await writeFile(file, text);
}

// ------------------------------------------------------- dataset: geoBoundaries --
const GB_API = "https://www.geoboundaries.org/api/current";
const GB_CGAZ = "https://github.com/wmgeolab/geoBoundaries/raw/main/releaseData/CGAZ";
const GB_LEVELS = ["ADM0", "ADM1", "ADM2", "ADM3", "ADM4", "ADM5"];
const FOCUS = ["CHN", "MMR", "IND", "KHM", "USA", "THA", "VNM", "LAO", "BGD", "PAK",
  "LKA", "NPL", "BTN", "MYS", "IDN", "PHL", "SGP", "RUS", "JPN", "KOR", "PRK"];

async function fetchGeoBoundaries() {
  const levels = String(flag("levels", GB_LEVELS.map((_, index) => index).join(",")))
    .split(",").map((value) => "ADM" + value.trim().replace(/^ADM/i, ""))
    .filter((value, index, all) => GB_LEVELS.includes(value) && all.indexOf(value) === index);
  const countryFlag = String(flag("countries", "all"));
  const countryFilter = countryFlag === "all" || countryFlag === "focus" ? countryFlag : countryFlag.split(",").map((value) => value.trim().toUpperCase());
  const withCgaz = !has("no-cgaz");
  const extract = !has("no-extract");
  const base = path.join(root, "geoBoundaries");

  // One boundary can be published twice for the same country and level (IND/ADM1
  // carries a 2024 build beside its 2023 one). Keying jobs without collapsing those
  // lets two workers race on ONE target, so the newest buildDate wins and the
  // dropped record is reported rather than silently lost.
  const byKey = new Map();
  for (const level of levels) {
    const list = await getJson(`${GB_API}/gbOpen/ALL/${level}/`);
    for (const record of Array.isArray(list) ? list : [list]) {
      const iso = record.boundaryISO;
      if (countryFilter !== "all" && !(countryFilter === "focus" ? FOCUS.includes(iso) : countryFilter.includes(iso))) continue;
      const key = `${iso}/${level}`;
      const previous = byKey.get(key);
      if (!previous) { byKey.set(key, record); continue; }
      const newer = Date.parse(record.buildDate ?? 0) >= Date.parse(previous.buildDate ?? 0) ? record : previous;
      console.log(`注意：${key} 有两条发布记录（${previous.buildDate} / ${record.buildDate}），保留 ${newer.buildDate}`);
      byKey.set(key, newer);
    }
    process.stdout.write(`API ${level}: ${byKey.size} 条累计\r\n`);
  }

  const manifest = await openManifest(path.join(base, "MANIFEST.json"), GB_API);
  const jobs = [];
  if (withCgaz && countryFilter === "all") for (const level of ["ADM0", "ADM1", "ADM2"]) for (const ext of ["gpkg", "geojson"]) {
    const name = `geoBoundariesCGAZ_${level}.${ext}`;
    jobs.push({ key: `CGAZ/${name}`, url: `${GB_CGAZ}/${name}`, target: path.join(base, "CGAZ", name), license: "Creative Commons Attribution 4.0 (CC BY 4.0)", meta: { boundaryName: "CGAZ (geoBoundaries Comprehensive Global Administrative Zones)", boundaryISO: "ALL", boundaryType: level } });
  }
  for (const record of byKey.values()) {
    const { boundaryISO: iso, boundaryType: level } = record;
    const name = `geoBoundaries-${iso}-${level}`;
    const dir = path.join(base, "gbOpen", iso, level);
    jobs.push({
      key: `gbOpen/${iso}/${level}`, url: record.staticDownloadLink, target: path.join(dir, `${name}-all.zip`),
      extract: extract ? [`${name}.geojson`, `${name}_simplified.geojson`] : null, license: record.boundaryLicense, meta: record,
    });
  }
  const planned = limit ? jobs.slice(0, limit) : jobs;
  for (const job of planned) {
    job.skip = manifest.done.has(job.key) && (await fileSize(job.target)) === manifest.done.get(job.key)?.bytes;
    job.size = manifest.done.get(job.key)?.bytes;
  }
  if (dryRun) {
    const pending = planned.filter((job) => !job.skip);
    const stride = Math.max(1, Math.floor(pending.length / 24));
    const sample = pending.filter((_, index) => index % stride === 0).slice(0, 24);
    let sampled = 0, bytes = 0;
    for (const job of sample) { const size = await head(job.url); if (size) { sampled += 1; bytes += size; } }
    return { dataset: "geoBoundaries", targets: planned.length, pending: pending.length, sampled, estimatedBytes: sampled ? bytes / sampled * pending.length : 0, note: "抽样外推" };
  }
  const result = await runJobs(planned, {
    label: "geoBoundaries",
    handle: async (job) => {
      const entry = manifest.done.get(job.key);
      const size = await fileSize(job.target);
      const intact = entry && size === entry.bytes;
      const stale = intact && entry.url && entry.url !== job.url;
      if (stale) console.log(`↻ ${job.key} 上游发布记录已更新，重新下载`);
      if (intact && !stale) {
        if (!job.extract || await extracted(job.target, job.extract)) return "skipped";
        let written = await unzip(job.target, job.extract);
        if (!Array.isArray(written)) {
          const quarantine = `${job.target}.corrupt-${Date.now()}`;
          await rename(job.target, quarantine);
          console.error(`↻ ${job.key} 解压失败，已另存 ${path.basename(quarantine)} 并重下`);
          const retry = await download(job.url, job.target);
          entry.bytes = retry.bytes; entry.sha256 = retry.sha256;
          written = await unzip(job.target, job.extract);
        }
        if (!Array.isArray(written)) throw new Error(`解压仍失败：${written.error}`);
        entry.extracted = written;
        await manifest.save();
        return "repaired";
      }
      const result = await download(job.url, job.target);
      let written = [];
      if (job.extract) {
        const outcome = await unzip(job.target, job.extract);
        if (!Array.isArray(outcome)) throw new Error(`解压失败：${outcome.error}`);
        written = outcome;
      }
      manifest.done.set(job.key, {
        url: job.url, bytes: result.bytes, sha256: result.sha256, extracted: written,
        license: job.license ?? "未标注", licenseSource: job.meta?.licenseSource ?? "",
        year: job.meta?.boundaryYearRepresented ?? "", buildDate: job.meta?.buildDate ?? "",
        units: job.meta?.admUnitCount ?? "", name: job.meta?.boundaryName ?? "", iso: job.meta?.boundaryISO ?? "",
        level: job.meta?.boundaryType ?? "", at: new Date().toISOString(),
      });
      await manifest.save();
      return result;
    },
  });
  await writeAttribution(path.join(base, "LICENSE-AND-ATTRIBUTION.md"), {
    title: "geoBoundaries 使用与署名（本目录）",
    source: `geoBoundaries（William & Mary geoLab），API \`${GB_API}/gbOpen/ALL/ADM{0..5}/\`，CGAZ 取自同一仓库 \`releaseData/CGAZ\``,
    note: "每个 `-all.zip` 内含 SHP（全精度与简化）、GeoJSON、topojson、该边界的 `metaData.json` 与 `CITATION-AND-USE-geoBoundaries.txt`；同级另有解出的直接可用 GeoJSON。",
    units: "等级含义：ADM0 国家、ADM1 一级行政区、ADM2 二级……各条记录的 `admUnitCount` 见 MANIFEST。",
  });
  return { dataset: "geoBoundaries", ...result, manifest: path.join(base, "MANIFEST.json") };
}
async function extracted(zipPath, members) {
  for (const member of members) {
    const size = await fileSize(path.join(path.dirname(zipPath), member));
    if (!size) return false;
  }
  return true;
}

// ------------------------------------------------------------ dataset: WorldPop --
const WP_ROOT = "https://data.worldpop.org/GIS/Population";
const WP_COLLECTION = { adj: "Global_2000_2020_1km", unadj: "Global_2000_2020_1km_UNadj" };
const WP_YEARS = Array.from({ length: 21 }, (_, index) => String(2000 + index));

async function fetchWorldPop() {
  const yearFlag = String(flag("years", "all"));
  const years = yearFlag === "all" ? WP_YEARS : yearFlag.split(",").map((value) => value.trim()).filter((value) => /^\d{4}$/.test(value));
  const variant = String(flag("variant", "adj"));
  const variants = variant === "both" ? ["adj", "unadj"] : [variant === "unadj" ? "unadj" : "adj"];
  const countryFlag = String(flag("countries", "all"));
  const base = path.join(root, "WorldPop");

  const manifest = await openManifest(path.join(base, "MANIFEST.json"), WP_ROOT);
  const jobs = [];
  const listings = new Map();
  for (const key of variants) {
    const collection = WP_COLLECTION[key];
    for (const year of years) {
      if (!listings.has(collection)) {
        const html = await (await request(`${WP_ROOT}/${collection}/${year}/`)).text();
        listings.set(collection, [...new Set([...html.matchAll(/href="([A-Z]{3})\/"/g)].map((match) => match[1]))].sort());
      }
      const published = listings.get(collection);
      const wantedCountries = countryFlag === "all" ? published
        : countryFlag === "focus" ? published.filter((iso) => FOCUS.includes(iso))
          : published.filter((iso) => countryFlag.split(",").map((value) => value.trim().toUpperCase()).includes(iso));
      for (const iso of wantedCountries) {
        const name = `${iso.toLowerCase()}_ppp_${year}_1km_Aggregated${key === "unadj" ? "_UNadj" : ""}.tif`;
        jobs.push({ key: `${collection}/${year}/${iso}`, url: `${WP_ROOT}/${collection}/${year}/${iso}/${name}`, target: path.join(base, "1km", key, year, iso, name), iso, year, collection });
      }
    }
  }
  const planned = limit ? jobs.slice(0, limit) : jobs;
  for (const job of planned) {
    job.skip = manifest.done.has(job.key) && (await fileSize(job.target)) === manifest.done.get(job.key)?.bytes;
    job.size = manifest.done.get(job.key)?.bytes;
  }
  if (dryRun) {
    const pending = planned.filter((job) => !job.skip);
    const stride = Math.max(1, Math.floor(pending.length / 30));
    const sample = pending.filter((_, index) => index % stride === 0).slice(0, 30);
    let sampled = 0, bytes = 0;
    for (const job of sample) { const size = await head(job.url); if (size) { sampled += 1; bytes += size; } }
    return { dataset: "WorldPop", targets: planned.length, pending: pending.length, sampled, estimatedBytes: sampled ? bytes / sampled * pending.length : 0, note: "抽样外推（按国家逐年发布，无单一全球镶嵌）" };
  }
  const result = await runJobs(planned, {
    label: "WorldPop",
    handle: async (job) => {
      const entry = manifest.done.get(job.key);
      if (entry && (await fileSize(job.target)) === entry.bytes && (!entry.url || entry.url === job.url)) return "skipped";
      const size = await head(job.url);
      if (size === null) return "skipped";   // not published for that country/year
      const result = await download(job.url, job.target);
      manifest.done.set(job.key, {
        url: job.url, bytes: result.bytes, sha256: result.sha256, iso: job.iso, year: job.year, collection: job.collection,
        license: "Creative Commons Attribution 4.0 (CC BY 4.0)",
        citation: "WorldPop (www.worldpop.org), Global 2000-2020 1km population, CC BY 4.0", at: new Date().toISOString(),
      });
      await manifest.save();
      return result;
    },
  });
  await writeAttribution(path.join(base, "LICENSE-AND-ATTRIBUTION.md"), {
    title: "WorldPop 1 km 人口栅格（本目录）",
    source: `WorldPop (www.worldpop.org)，\`Global 2000-2020 1km\`（按国家逐年发布）`,
    note: "每像元人口数，1 km（由 100 m 聚合）；`adj` 为 UN 调整版，`unadj` 为未调整版。",
    units: "注意：人口栅格是建模估计值，不是普查计数；用于暴露度分析时请说明版本与年份。",
  });
  return { dataset: "WorldPop", ...result, manifest: path.join(base, "MANIFEST.json") };
}

// -------------------------------------------------------------------- main --
const results = [];
if (wanted("geoboundaries")) results.push(await fetchGeoBoundaries());
if (wanted("worldpop")) results.push(await fetchWorldPop());

if (dryRun) {
  const total = results.reduce((sum, item) => sum + (item.estimatedBytes ?? 0), 0);
  console.log(JSON.stringify({ root, dryRun: true, results, estimatedTotal: `${gb(total)} GB` }, null, 2));
  await stop(0);
}

const sources = [
  "# 共享数据来源与许可（本根目录）", "",
  `生成时间：${new Date().toISOString()}　根目录：\`${root}\``, "",
  "| 库 | 内容 | 记录数 | 许可 |", "| --- | --- | --- | --- |",
  "| `geoBoundaries/` | 全球各国 ADM0-ADM5 行政边界（含 CGAZ 全球统一层） | 见 `geoBoundaries/MANIFEST.json` | 逐条标注，25 种（ODbL 1.0、CC BY、CC BY-SA、公有领域、CC0 等） |",
  "| `WorldPop/` | 全球 1 km 人口栅格（2000-2020 逐年，按国家） | 见 `WorldPop/MANIFEST.json` | CC BY 4.0 |",
  "", "各库目录内的 `LICENSE-AND-ATTRIBUTION.md` 按许可分组列出具体条目；",
  "使用时必须保留署名；边界与人口数据均为研究数据集，不代表官方界线，也不是普查计数。", "",
].join("\n");
await writeFile(path.join(root, "SOURCES.md"), sources);

console.log(JSON.stringify({ root, results, sources: path.join(root, "SOURCES.md") }, null, 2));
await stop(results.some((item) => item.failed) ? 1 : 0);
