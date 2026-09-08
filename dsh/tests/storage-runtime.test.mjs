import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, existsSync, symlinkSync } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import { PlatformStore } from "../plugins/platform/store.mjs";
import { RuntimeLedger } from "../plugins/platform/runtime.mjs";
import { WorkspaceFiles } from "../plugins/platform/files.mjs";
import { cleanupDeleted } from "../plugins/platform/maintenance.mjs";
import { DockerRunner, dockerMemoryMiB } from "../plugins/research/docker.mjs";

function fixture(t) {
  const root = mkdtempSync(path.join(tmpdir(), "geo-storage-")), store = new PlatformStore(root), runtime = new RuntimeLedger(store);
  const user = store.createUser("alice", "valid-password"), project = store.createProject(user, "Study"), chat = store.createChat(user, project.id);
  const storage = new WorkspaceFiles(store, runtime, { minFreeMiB: 0, projectBytes: 10 });
  t.after(() => { runtime.close(); store.close(); rmSync(root, { recursive: true, force: true }); });
  return { root, store, runtime, user, project, chat, storage };
}
test("Docker jobs default to thirty minutes while explicit test timeouts remain supported", (t) => {
  const f = fixture(t);
  assert.equal(new DockerRunner({ store: f.store, runtime: f.runtime }).timeoutMs, 1800000);
  assert.equal(new DockerRunner({ store: f.store, runtime: f.runtime, timeoutMs: 100 }).timeoutMs, 100);
});
test("Docker memory defaults to three GiB and supports a bounded four-GiB override", (t) => {
  const f = fixture(t);
  assert.equal(new DockerRunner({ store: f.store, runtime: f.runtime }).memoryMiB, 3072);
  assert.equal(new DockerRunner({ store: f.store, runtime: f.runtime, memoryMiB: 4096 }).memoryMiB, 4096);
  for (const value of [0, -1, 1.5, "4g", 100000, NaN]) assert.throws(() => dockerMemoryMiB(value));
});
test("GEE readiness fails before allocating jobs and names the administrator configuration", async (t) => {
  const f = fixture(t), identity = { user: f.user, chatId: f.chat.id, projectId: f.project.id, root: f.store.chatRoot(f.user, f.chat.id) };
  const missing = new DockerRunner({ store: f.store, runtime: f.runtime, geeProject: "test-project" });
  await assert.rejects(() => missing.run(identity, { kind: "gee-download" }), (e) => e.status === 503 && /GEO_GEE_CREDENTIALS/.test(e.message));
  const invalid = new DockerRunner({ store: f.store, runtime: f.runtime, geeProject: "test-project", geeCredentials: f.root });
  await assert.rejects(() => invalid.run(identity, { kind: "gee-download" }), (e) => e.status === 503 && /凭据文件/.test(e.message));
  assert.equal(f.runtime.queued("docker").length, 0);
  assert.equal(f.runtime.running("docker").length, 0);
});
test("concurrent uploads serialize quota checks and imported evidence uses the same quota", async (t) => {
  const f = fixture(t);
  const results = await Promise.allSettled(["a.txt", "b.txt"].map((name) => f.storage.writeInput(f.user, f.project.id, name, Buffer.alloc(6))));
  assert.equal(results.filter((r) => r.status === "fulfilled").length, 1);
  await assert.rejects(() => f.storage.writeInput(f.user, f.project.id, "monitor.json", Buffer.alloc(6), { reuse: true }), /配额/);
  assert.equal(f.runtime.summary(f.user.id).usage[0].upload_bytes, 6);
  assert.equal((await f.storage.userUsage(f.user)).bytes, 6);
});
test("disk reserve blocks writes and traversal remains rejected", async (t) => {
  const f = fixture(t), full = new WorkspaceFiles(f.store, f.runtime, { minFreeMiB: 1, stat: async () => ({ bavail: 0, bsize: 4096 }) });
  await assert.rejects(() => full.writeInput(f.user, f.project.id, "a.txt", Buffer.from("a")), /磁盘/);
  await assert.rejects(() => f.storage.writeInput(f.user, f.project.id, "../a", Buffer.from("a")), /文件名/);
});
test("cleanup is dry-run by default, retention-bound and leaves active and archived data intact", async (t) => {
  const f = fixture(t); await f.storage.writeInput(f.user, f.project.id, "a.txt", Buffer.from("a"));
  const kept = f.store.createProject(f.user, "Keep"); f.store.updateProject(f.user, kept.id, { archived: true });
  const target = f.store.projectRoot(f.user, f.project.id), keptRoot = f.store.projectRoot(f.user, kept.id);
  f.store.updateProject(f.user, f.project.id, { deleted: true });
  assert.equal((await cleanupDeleted(f.store, f.runtime)).candidates.length, 0);
  const now = Date.now() + 31 * 86400000;
  assert.equal((await cleanupDeleted(f.store, f.runtime, { now })).candidates.length, 1);
  assert.equal(existsSync(target), true);
  await cleanupDeleted(f.store, f.runtime, { now, apply: true });
  assert.equal(existsSync(target), false); assert.equal(existsSync(keptRoot), true);
  assert.equal((await cleanupDeleted(f.store, f.runtime, { now })).candidates.length, 0);
});
test("cleanup refuses junctions and will not purge a pending workspace", async (t) => {
  const f = fixture(t), job = f.runtime.enqueue({ kind: "research", operation: "prompt", user: f.user, chatId: f.chat.id, payload: { text: "test" } });
  const root = f.store.projectRoot(f.user, f.project.id);
  f.store.updateProject(f.user, f.project.id, { deleted: true });
  const now = Date.now() + 31 * 86400000;
  assert.equal((await cleanupDeleted(f.store, f.runtime, { now })).candidates.length, 0);
  f.runtime.finish(job.id, "cancelled");
  const outside = mkdtempSync(path.join(tmpdir(), "geo-outside-"));
  t.after(() => rmSync(outside, { recursive: true, force: true }));
  const link = path.join(root, "outside"); symlinkSync(outside, link, process.platform === "win32" ? "junction" : "dir");
  await assert.rejects(() => cleanupDeleted(f.store, f.runtime, { now, apply: true }), /符号链接/);
  assert.equal(existsSync(outside), true);
  rmSync(link);
});
