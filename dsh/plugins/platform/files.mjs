import path from "node:path";
import { realpathSync } from "node:fs";
import { readdir, lstat, writeFile, statfs } from "node:fs/promises";
import { PlatformError, workspacePath } from "./store.mjs";

/** Resolve a path to its real location, tolerating a tail that does not exist yet. */
function canonicalOf(target) {
  let current = path.resolve(target), suffix = [];
  for (;;) {
    try {
      const real = realpathSync(current);
      return suffix.length ? path.join(real, ...suffix) : real;
    } catch {
      const parent = path.dirname(current);
      if (parent === current) return path.resolve(target);
      suffix.unshift(path.basename(current));
      current = parent;
    }
  }
}

/**
 * Containment fence for model-facing filesystem tools.
 *
 * DSH fences fs WRITES with the sandbox but resolves reads against the session
 * cwd, so an absolute path can reach anything the host process may read. The
 * product therefore checks every path argument itself: relative paths resolve
 * against the session workspace, and the result must stay inside one of the
 * allowed roots (the chat workspace and the shipped skill assets). Symlinks are
 * followed before the comparison, so a link cannot escape the fence.
 */
export function containedPath(roots, value, base) {
  if (value === undefined || value === null || value === "") return true;
  if (typeof value !== "string" || value.includes("\0") || value.length > 4096) return false;
  const target = canonicalOf(path.isAbsolute(value) ? value : path.resolve(base, value));
  return roots.some((root) => {
    const canonical = canonicalOf(root);
    return target === canonical || target.startsWith(canonical + path.sep);
  });
}

/** The only directory a model-authored write may target, relative to a chat workspace. */
export const WRITE_DIR = "outputs";

/**
 * Write fence for `write`/`edit`.
 *
 * Reading may span the chat workspace, the project inputs and the skill library;
 * writing must not. Uploaded files, project-material `inputs/`, the runtime
 * `memory/` directory and the shipped skills stay read-only, and every artifact
 * belongs to `outputs/<作业ID>/`. The path is resolved and canonicalised exactly
 * like a read, so a symlink or `..` cannot escape the single writable root, and a
 * missing or non-string path is refused instead of being treated as unrestricted.
 */
export function containedWrite(root, value) {
  if (typeof value !== "string" || value === "") return false;
  return containedPath([path.join(root, WRITE_DIR)], value, root);
}

export async function directoryUsage(root, { skipLinks = false } = {}) {
  let bytes = 0, files = 0, entries = 0;
  async function walk(dir) {
    const info = await lstat(dir).catch((e) => { if (e.code === "ENOENT") return null; throw e; });
    if (!info) return;
    if (info.isSymbolicLink()) {
      // The platform itself links the read-only share roots into chat workspaces
      // (see `ensureShareLinks`), so usage counting must skip a link: the linked data
      // belongs to the administrator, not to the account, and `userUsage()` would
      // otherwise refuse the whole tree with 403 for every user who ever opened a
      // chat. Cleanup deliberately keeps the strict default — removing THROUGH a link
      // could delete data outside the fence (tests/storage-runtime.test.mjs pins that).
      if (skipLinks) return;
      throw new PlatformError(403, "存储目录包含符号链接，拒绝操作");
    }
    if (info.isFile()) { bytes += info.size; files++; return; }
    if (!info.isDirectory()) throw new PlatformError(403, "存储目录含不支持的文件类型");
    for (const entry of await readdir(dir)) {
      if (++entries > 100000) throw new PlatformError(413, "存储目录过大，请管理员离线检查");
      await walk(path.join(dir, entry));
    }
  }
  await walk(root); return { bytes, files };
}

export class WorkspaceFiles {
  constructor(store, runtime, { minFreeMiB = Number(process.env.GEO_MIN_FREE_DISK_MIB ?? 1024), projectBytes = 512 * 1024 * 1024, stat = statfs } = {}) {
    if (!Number.isSafeInteger(minFreeMiB) || minFreeMiB < 0) throw new Error("Invalid GEO_MIN_FREE_DISK_MIB");
    Object.assign(this, { store, runtime, minFreeMiB, projectBytes, stat }); this.locks = new Map();
  }
  async checkSpace(bytes = 0) {
    const info = await this.stat(this.store.root);
    if (Number(info.bavail) * Number(info.bsize) < this.minFreeMiB * 1024 * 1024 + bytes)
      throw new PlatformError(507, "磁盘可用空间不足，请管理员清理后重试");
  }
  async writeInput(user, projectId, name, bytes, { reuse = false } = {}) {
    if (typeof name !== "string" || name.includes("/") || name.startsWith(".")) throw new PlatformError(400, "文件名无效");
    if (bytes.length > 16 * 1024 * 1024) throw new PlatformError(413, "单个上传文件上限为 16 MiB");
    const previous = this.locks.get(projectId) ?? Promise.resolve();
    const action = previous.catch(() => {}).then(async () => {
      const project = this.store.project(user, projectId);
      if (project.archived) throw new PlatformError(409, "项目已归档");
      const root = path.join(this.store.projectRoot(user, projectId), "inputs"), target = workspacePath(root, name);
      if (reuse && await lstat(target).then(() => true, (e) => { if (e.code === "ENOENT") return false; throw e; })) return;
      const usage = await directoryUsage(root);
      if (usage.bytes + bytes.length > this.projectBytes) throw new PlatformError(413, "项目资料超出配额");
      await this.checkSpace(bytes.length);
      // Ownership can change while scanning a large directory.
      if (this.store.project(user, projectId).archived) throw new PlatformError(409, "项目已归档");
      if (!this.store.db.prepare("SELECT id FROM users WHERE id=? AND disabled=0").get(user.id)) throw new PlatformError(403, "账号已停用");
      try { await writeFile(target, bytes, { flag: "wx" }); }
      catch (e) { if (e.code === "EEXIST") throw new PlatformError(409, "同名文件已存在"); throw e; }
      this.runtime?.usage(user.id, "upload_bytes", bytes.length);
    });
    this.locks.set(projectId, action);
    try { await action; } finally { if (this.locks.get(projectId) === action) this.locks.delete(projectId); }
  }
  async userUsage(user) { return directoryUsage(workspacePath(this.store.root, `users/${user.id}`), { skipLinks: true }); }
}
