import path from "node:path";
import { readdir, lstat, writeFile, statfs } from "node:fs/promises";
import { PlatformError, workspacePath } from "./store.mjs";

export async function directoryUsage(root) {
  let bytes = 0, files = 0, entries = 0;
  async function walk(dir) {
    const info = await lstat(dir).catch((e) => { if (e.code === "ENOENT") return null; throw e; });
    if (!info) return;
    if (info.isSymbolicLink()) throw new PlatformError(403, "存储目录包含符号链接，拒绝操作");
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
  async userUsage(user) { return directoryUsage(workspacePath(this.store.root, `users/${user.id}`)); }
}
