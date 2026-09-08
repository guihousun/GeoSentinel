import path from "node:path";
import { rm } from "node:fs/promises";
import { workspacePath } from "./store.mjs";
import { directoryUsage } from "./files.mjs";

/** Offline-only cleanup: never traverses DSH sessions, monitor or active projects. */
export async function cleanupDeleted(store, runtime, { days = 30, apply = false, now = Date.now() } = {}) {
  if (!Number.isSafeInteger(days) || days < 1) throw new Error("Retention must be at least one day");
  runtime.assertOwner();
  const cutoff = now - days * 86400000, candidates = [];
  const projects = store.db.prepare("SELECT * FROM projects WHERE deleted=1 AND deleted_at<=? AND purged_at IS NULL").all(cutoff);
  const chats = store.db.prepare("SELECT c.*,p.user_id FROM chats c JOIN projects p ON p.id=c.project_id WHERE c.deleted=1 AND c.deleted_at<=? AND c.purged_at IS NULL AND p.deleted=0").all(cutoff);
  for (const [kind, rows] of [["project", projects], ["chat", chats]]) for (const row of rows) {
    const projectId = kind === "project" ? row.id : row.project_id;
    const chatIds = kind === "project" ? store.db.prepare("SELECT id FROM chats WHERE project_id=?").all(row.id).map((c) => c.id) : [row.id];
    if ([row.id, row.user_id, projectId, ...chatIds].some((id) => !/^[0-9a-f-]{36}$/.test(id))) throw new Error("Invalid persisted workspace identity");
    const allJobs = chatIds.flatMap((id) => store.db.prepare("SELECT id,status FROM runtime_jobs WHERE chat_id=?").all(id));
    if (allJobs.some((job) => ["queued", "running", "cancelling"].includes(job.status))) continue;
    const relative = `users/${row.user_id}/projects/${projectId}${kind === "chat" ? `/chats/${row.id}` : ""}`;
    const relatives = [relative, ...allJobs.map((job) => {
      if (!/^[0-9a-f-]{36}$/.test(job.id)) throw new Error("Invalid persisted job identity");
      return `jobs/${job.id}`;
    })];
    let bytes = 0, files = 0;
    // Validate every descendant before any recursive removal, including junctions.
    for (const item of relatives) { const usage = await directoryUsage(workspacePath(store.root, item)); bytes += usage.bytes; files += usage.files; }
    candidates.push({ kind, id: row.id, relative, bytes, files });
    if (apply) {
      runtime.assertOwner();
      for (const item of relatives) {
        const target = workspacePath(store.root, item);
        if (!path.isAbsolute(target) || !target.startsWith(store.root + path.sep)) throw new Error("Cleanup escaped platform workspace");
        await directoryUsage(target);
        await rm(target, { recursive: true, force: true });
      }
      store.db.prepare(`UPDATE ${kind === "project" ? "projects" : "chats"} SET purged_at=? WHERE id=?`).run(now, row.id);
      store.audit(null, "storage.purge", `${kind}:${row.id}`);
    }
  }
  return { apply, retentionDays: days, candidates, bytes: candidates.reduce((sum, row) => sum + row.bytes, 0), note: "DSH 会话记录、公共监测、正常与仅归档项目不在清理范围内" };
}
