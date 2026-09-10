import { readdir, stat } from "node:fs/promises";
import path from "node:path";
import { isShapefileSidecar } from "./shapefile.mjs";
import { SHARE_LABEL, listShare, shareChildren } from "./share.mjs";

// The workspace on disk stays `inputs/` + `outputs/<作业ID>/` because the tools,
// the artifact routes and the container mounts are built on it (and the storage
// module is release-gated). Users should not have to read that layout, so the
// explorer shows a small, everyday view instead:
//
//   上传的文件/     what the user handed in (project inputs, chat inputs, uploads)
//   分析结果/       the produced files, grouped by kind, job folders hidden
//   共享数据（只读）/ administrator-curated libraries outside the account data
//   过程记录/       the raw per-job folders, kept for traceability
//
// Every virtual path resolves back to a real, ownership-checked file.

export const VIEW = Object.freeze({
  uploads: "上传的文件",
  results: "分析结果",
  share: SHARE_LABEL,
  history: "过程记录",
});

export const RESULT_GROUPS = Object.freeze([
  { id: "图表", extensions: [".png", ".jpg", ".jpeg", ".webp", ".gif", ".svg"] },
  { id: "表格", extensions: [".csv", ".tsv", ".xlsx", ".xls"] },
  { id: "报告", extensions: [".md", ".txt", ".pdf", ".docx", ".html"] },
  { id: "空间数据", extensions: [".geojson", ".shp", ".gpkg", ".tif", ".tiff", ".zip"] },
  { id: "其他文件", extensions: [] },
]);

// Tool envelopes are machine plumbing, not user results: they stay visible in
// 过程记录 but not in 分析结果.
const MACHINE_FILES = new Set(["result.json"]);

// A pathological workspace must not make the panel hang: cap what one listing
// materializes, and say so through the entry count rather than timing out.
const MAX_SCANNED_FILES = 2000;
const MAX_GROUP_ENTRIES = 200;

const groupOf = (name) => {
  const extension = path.extname(name).toLowerCase();
  return RESULT_GROUPS.find((group) => group.extensions.includes(extension))?.id ?? "其他文件";
};

/** Add ` (2)`, ` (3)` … so two results with the same name stay distinguishable. */
function uniqueName(name, used) {
  if (!used.has(name)) {
    used.add(name);
    return name;
  }
  const extension = path.extname(name);
  const stem = name.slice(0, name.length - extension.length);
  for (let index = 2; index < 1000; index++) {
    const candidate = `${stem} (${index})${extension}`;
    if (!used.has(candidate)) {
      used.add(candidate);
      return candidate;
    }
  }
  used.add(name);
  return name;
}

/** Recursively list regular files (relative POSIX paths) with size, skipping dot paths. */
async function listFiles(directory, prefix = "", budget = { remaining: MAX_SCANNED_FILES }) {
  const files = [];
  let entries;
  try {
    entries = await readdir(directory, { withFileTypes: true });
  } catch {
    return files;
  }
  for (const entry of entries) {
    if (budget.remaining <= 0) break;
    if (entry.name.startsWith(".") || entry.isSymbolicLink()) continue;
    // Shapefile companions (.shx/.dbf/.prj…) are part of the .shp entry: they
    // stay hidden and download together with it.
    if (isShapefileSidecar(entry.name)) continue;
    const relative = prefix ? `${prefix}/${entry.name}` : entry.name;
    const full = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      files.push(...(await listFiles(full, relative, budget)));
      continue;
    }
    const info = await stat(full).catch(() => null);
    if (info?.isFile()) {
      budget.remaining -= 1;
      files.push({ relative, real: full, name: entry.name, size: info.size });
    }
  }
  return files;
}

/**
 * Build the friendly view for one chat.
 * `roots` are absolute, ownership-checked directories:
 * `{ chatRoot, inputsRoot, projectInputsRoot }`.
 */
export async function workspaceView({ chatRoot, inputsRoot, projectInputsRoot, share = [] }) {
  const outputsRoot = path.join(chatRoot, "outputs");
  const uploads = [];
  const usedUploads = new Set();
  const uploadSources = [
    { root: projectInputsRoot, label: "项目资料" },
    { root: inputsRoot, label: "对话资料" },
    { root: path.join(chatRoot, ".dsh-uploads"), label: "上传" },
  ];
  for (const source of uploadSources) {
    for (const file of await listFiles(source.root)) {
      uploads.push({
        // Uploaded files are stored with a session prefix; the user knows them
        // by the name they chose.
        name: uniqueName(file.name.replace(/^[0-9a-f-]{36}-/i, ""), usedUploads),
        real: file.real,
        relative: file.relative,
        size: file.size,
        label: source.label,
      });
    }
  }
  uploads.sort((left, right) => left.name.localeCompare(right.name, "zh-Hans"));

  const grouped = new Map(RESULT_GROUPS.map((group) => [group.id, []]));
  const usedResults = new Map();
  const jobs = [];
  let jobEntries = [];
  try {
    jobEntries = (await readdir(outputsRoot, { withFileTypes: true }))
      .filter((entry) => entry.isDirectory() && !entry.name.startsWith(".") && !entry.isSymbolicLink());
  } catch {
    jobEntries = [];
  }
  for (const job of jobEntries) {
    const jobRoot = path.join(outputsRoot, job.name);
    const files = await listFiles(jobRoot);
    jobs.push({ name: job.name, files });
    for (const file of files) {
      if (MACHINE_FILES.has(file.name)) continue;
      const group = groupOf(file.name);
      const used = usedResults.get(group) ?? new Set();
      usedResults.set(group, used);
      if (grouped.get(group).length >= MAX_GROUP_ENTRIES) continue;
      grouped.get(group).push({ ...file, job: job.name, display: uniqueName(file.name, used) });
    }
  }
  for (const list of grouped.values())
    list.sort((left, right) => left.display.localeCompare(right.display, "zh-Hans"));
  jobs.sort((left, right) => right.name.localeCompare(left.name, "zh-Hans"));

  // Shared data library (public use-case data, boundaries, imagery, GDP, books).
  // Read-only, and listed with its own budget so a large library cannot slow the
  // whole explorer down.
  const shareFiles = share.length ? (await listShare(share).catch(() => ({ files: [] }))).files : [];

  return { outputsRoot, uploads, grouped, jobs, shareFiles };
}

/**
 * List one directory of the virtual tree.
 * @param view - result of {@link workspaceView}
 * @param virtualRoot - the explorer's session root (`/工作区/<title>`)
 * @param virtualPath - absolute virtual path to list
 */
export function viewEntries(view, virtualRoot, virtualPath) {
  const relative = virtualPath === virtualRoot ? "" : virtualPath.slice(virtualRoot.length + 1);
  const parts = relative ? relative.split("/") : [];
  const base = virtualPath.endsWith("/") ? virtualPath.slice(0, -1) : virtualPath;
  const dir = (name) => ({ name, path: `${base}/${name}`, isDir: true });
  const file = (name, real, size) => ({ name, path: `${base}/${name}`, isDir: false, real, size });

  if (!relative) return [dir(VIEW.uploads), dir(VIEW.results),
    ...(view.shareFiles?.length ? [dir(VIEW.share)] : []), dir(VIEW.history)];
  if (parts[0] === VIEW.uploads && parts.length === 1)
    return view.uploads.map((entry) => file(entry.name, entry.real, entry.size));
  if (parts[0] === VIEW.results && parts.length === 1)
    return RESULT_GROUPS.filter((group) => view.grouped.get(group.id).length > 0).map((group) => dir(group.id));
  if (parts[0] === VIEW.results && parts.length === 2) {
    const group = view.grouped.get(parts[1]) ?? [];
    return group.map((entry) => file(entry.display, entry.real, entry.size));
  }
  if (parts[0] === VIEW.history && parts.length === 1) return view.jobs.map((job) => dir(job.name));
  if (parts[0] === VIEW.history && parts.length === 2) {
    const job = view.jobs.find((item) => item.name === parts[1]);
    if (!job) return [];
    return job.files.map((entry) => file(entry.relative, entry.real, entry.size));
  }
  if (parts[0] === VIEW.share) {
    const children = shareChildren(view.shareFiles ?? [], parts.slice(1).join("/"));
    return [
      ...children.directories.map((child) => dir(child.name)),
      ...children.files.map((entry) => file(entry.name, entry.real, entry.size)),
    ];
  }
  return [];
}

/**
 * Resolve any virtual path to its real file, or `undefined` when the path is
 * not part of the view. Callers still re-check containment with `workspacePath`.
 */
export function resolveViewPath(view, virtualRoot, virtualPath) {
  const parent = virtualPath.slice(0, virtualPath.lastIndexOf("/"));
  if (!parent || !parent.startsWith(virtualRoot)) return undefined;
  return viewEntries(view, virtualRoot, parent).find((item) => item.path === virtualPath)?.real;
}

/** Recursive filename search inside the virtual view, returning virtual paths. */
export function searchView(view, virtualRoot, needle) {
  const query = needle.trim().toLowerCase();
  if (!query) return [];
  const matches = [];
  const walk = (virtualPath, depth) => {
    // Reference libraries nest deeper than the workspace view (library → 章节 →
    // 数据 → file), so they get a larger, still bounded, budget.
    const limit = virtualPath.includes(VIEW.share) ? 8 : 3;
    if (depth > limit || matches.length >= 200) return;
    for (const entry of viewEntries(view, virtualRoot, virtualPath)) {
      if (entry.isDir) {
        walk(entry.path, depth + 1);
        continue;
      }
      if (entry.name.toLowerCase().includes(query) && matches.length < 200) matches.push(entry.path);
    }
  };
  walk(virtualRoot, 0);
  return matches;
}
