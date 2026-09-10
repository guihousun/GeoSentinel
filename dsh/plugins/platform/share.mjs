import { readdir, stat } from "node:fs/promises";
import path from "node:path";
import { isShapefileSidecar } from "./shapefile.mjs";

// Shared data library: public use-case data, downloaded global vector boundaries,
// imagery, national GDP, and reference packs such as 《缅甸地理》. It lives
// OUTSIDE the account data directory, because it is (a) far larger than the
// upload limits (16 MiB per file, 512 MiB per project) and (b) shared by every
// account instead of belonging to one project.
//
// Writes are impossible by construction: the host fence only ever allows a
// model-authored write inside the chat's own `outputs/`, and the analysis
// container mounts every root read-only.
//
// Configuration in `dsh/.env` — a single root, or a few named roots:
//
//   GEO_SHARE_DIR=E:/DSH/share                     → share/<相对路径>
//   GEO_SHARE_DIRS="缅甸地理=E:/DSH/缅甸地理;GDP=E:/DSH/gdp"
//                                                  → share/缅甸地理/<相对路径>
//
// One virtual namespace, identical on both sides:
//   host (read/glob/grep/read_document, geo_list_files, explorer): share/<…>
//   analysis container (registered GIS tools and model-authored Python):
//   /workspace/share/<…>

export const SHARE_PREFIX = "share";
export const SHARE_LABEL = "共享数据（只读）";
export const SHARE_MOUNT_ROOT = "/workspace/" + SHARE_PREFIX;
const MAX_ROOTS = 8;
const MAX_NAME = 40;
const MAX_ENTRIES = 600;
const MAX_SCANNED = 4000;

const insideOrEqual = (root, target) =>
  target === root || target.startsWith(root.endsWith(path.sep) ? root : root + path.sep);

/** A root name has to survive a virtual path, so it may not carry separators. */
function cleanName(value) {
  const name = String(value ?? "").replace(/[\u0000-\u001f]/g, "").trim();
  if (!name) return "";
  if (name.length > MAX_NAME || /[/\\=;]/.test(name)) return null;
  return name === "." || name === ".." ? null : name;
}

/**
 * Parse the shared-data configuration.
 * `GEO_SHARE_DIR` names one root that is addressed directly (`share/<子路径>`);
 * `GEO_SHARE_DIRS` adds `名称=路径` pairs addressed as `share/<名称>/<子路径>`.
 * Malformed pairs are skipped: a broken deployment variable must never take the
 * whole platform down.
 * @returns {Array<{ name: string, root: string }>} `name` is "" for GEO_SHARE_DIR
 */
export function parseShareDirs(env = process.env) {
  const entries = [];
  const seen = new Set();
  const push = (name, target) => {
    if (!target || entries.length >= MAX_ROOTS) return;
    const key = path.resolve(target);
    if (seen.has(key)) return;
    seen.add(key);
    entries.push({ name, root: key });
  };
  const single = cleanName("");
  if (typeof env.GEO_SHARE_DIR === "string" && env.GEO_SHARE_DIR.trim()) push("", env.GEO_SHARE_DIR.trim());
  void single;
  if (typeof env.GEO_SHARE_DIRS === "string" && env.GEO_SHARE_DIRS.trim()) {
    for (const chunk of env.GEO_SHARE_DIRS.split(";")) {
      const pair = chunk.trim();
      if (!pair) continue;
      const separator = pair.indexOf("=");
      if (separator <= 0) continue;
      const name = cleanName(pair.slice(0, separator));
      const target = pair.slice(separator + 1).trim();
      if (name === null || !name || !target || entries.some((entry) => entry.name === name)) continue;
      push(name, target);
    }
  }
  return entries;
}

/** The virtual alias prefix of one root: `share` or `share/<名称>`. */
export function shareRootPath(entry) {
  return entry.name ? `${SHARE_PREFIX}/${entry.name}` : SHARE_PREFIX;
}

/**
 * Resolve a `share/<…>` alias to an absolute path inside a configured root.
 * Returns null when the value is not a share path or tries to escape through
 * `.`/`..`; the caller then falls back to the normal workspace fence instead of
 * trusting anything here.
 */
export function shareTarget(entries, value) {
  if (typeof value !== "string" || value.length > 4096 || value.includes("\0")) return null;
  const normalised = value.replace(/\\/g, "/");
  if (normalised !== SHARE_PREFIX && !normalised.startsWith(SHARE_PREFIX + "/")) return null;
  const rest = normalised.slice(SHARE_PREFIX.length).replace(/^\/+/, "");
  if (!rest) return null;
  const segments = rest.split("/");
  if (segments.some((segment) => segment === "." || segment === ".." || segment === "")) return null;
  const named = entries.find((entry) => entry.name && entry.name === segments[0]);
  const entry = named ?? entries.find((item) => !item.name);
  if (!entry) return null;
  const tail = named ? segments.slice(1) : segments;
  if (!tail.length) return null;
  const target = path.resolve(entry.root, ...tail);
  return insideOrEqual(entry.root, target) ? target : null;
}

/** True when an absolute path already lives inside one of the roots. */
export function shareContained(entries, target) {
  return typeof target === "string" && entries.some((entry) => insideOrEqual(entry.root, path.resolve(target)));
}

/** True when an absolute path is one of the roots itself. */
export function shareRootFor(entries, target) {
  const resolved = typeof target === "string" ? path.resolve(target) : "";
  return entries.find((entry) => resolved === entry.root) ?? null;
}

/**
 * Bounded listing of every root, expressed in the virtual namespace
 * (`relative` is the `share/<…>` tail, `real` the host path). Shapefile
 * companions stay hidden (they belong to the `.shp` entry) and dot paths are
 * skipped, exactly like the workspace explorer.
 * @returns {Promise<{ files: Array<{relative: string, real: string, name: string, size: number}>, truncated: boolean }>}
 */
export async function listShare(entries = parseShareDirs(), budget = { remaining: MAX_ENTRIES, scanned: MAX_SCANNED }) {
  const files = [];
  for (const entry of entries) {
    const walk = async (directory, prefix) => {
      let children;
      try {
        children = await readdir(directory, { withFileTypes: true });
      } catch {
        return;
      }
      children.sort((left, right) => left.name.localeCompare(right.name, "zh-Hans"));
      for (const child of children) {
        if (budget.remaining <= 0 || budget.scanned <= 0) return;
        budget.scanned -= 1;
        if (child.name.startsWith(".") || child.isSymbolicLink()) continue;
        if (isShapefileSidecar(child.name)) continue;
        const relative = prefix ? `${prefix}/${child.name}` : child.name;
        const full = path.join(directory, child.name);
        if (child.isDirectory()) {
          await walk(full, relative);
          continue;
        }
        const info = await stat(full).catch(() => null);
        if (!info?.isFile()) continue;
        budget.remaining -= 1;
        files.push({ relative, real: full, name: child.name, size: info.size });
      }
    };
    await walk(entry.root, entry.name);
  }
  return { files, truncated: budget.remaining <= 0 || budget.scanned <= 0 };
}

/** Immediate children (directories + files) of one directory in the namespace. */
export function shareChildren(files, relative = "") {
  const prefix = relative ? relative + "/" : "";
  const directories = new Map();
  const leaves = [];
  for (const file of files) {
    if (!file.relative.startsWith(prefix)) continue;
    const rest = file.relative.slice(prefix.length);
    if (!rest) continue;
    const slash = rest.indexOf("/");
    if (slash === -1) {
      leaves.push(file);
      continue;
    }
    const name = rest.slice(0, slash);
    const entry = directories.get(name) ?? { name, count: 0, size: 0 };
    entry.count += 1;
    entry.size += file.size;
    directories.set(name, entry);
  }
  return {
    directories: [...directories.values()].sort((left, right) => left.name.localeCompare(right.name, "zh-Hans")),
    files: leaves.sort((left, right) => left.name.localeCompare(right.name, "zh-Hans")),
  };
}

/** Container mount target for one root: `/workspace/share` or `/workspace/share/<名称>`. */
export function shareMountTarget(entry) {
  return entry.name ? path.posix.join(SHARE_MOUNT_ROOT, entry.name) : SHARE_MOUNT_ROOT;
}
