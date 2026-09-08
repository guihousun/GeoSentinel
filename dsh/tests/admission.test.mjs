import test from "node:test";
import assert from "node:assert/strict";
import { ResearchQueue } from "../plugins/platform/admission.mjs";
import { RuntimeLedger, runtimeLimits } from "../plugins/platform/runtime.mjs";
import { PlatformStore } from "../plugins/platform/store.mjs";
import { mkdtempSync, rmSync } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";

function setup(t, limits = runtimeLimits({})) {
  const root = mkdtempSync(path.join(tmpdir(), "geo-queue-")), store = new PlatformStore(root);
  let now = 10000;
  const ledger = new RuntimeLedger(store, { limits, clock: () => now });
  t.after(() => { ledger.close(); store.close(); rmSync(root, { recursive: true, force: true }); });
  const users = Array.from({ length: 6 }, (_, i) => store.createUser(`user${i}`, "test-password"));
  const chats = users.map((user) => { const p = store.createProject(user, "Study"); return Array.from({ length: 4 }, () => store.createChat(user, p.id)); });
  const add = (u, c, kind = "research") => ledger.enqueue({ kind, operation: "prompt", user: users[u], chatId: chats[u][c].id, payload: { text: `study ${u} ${c}` } });
  return { root, store, ledger, users, chats, add, advance: () => { now += 2000; }, clock: () => now };
}

test("research queue enforces 2/10, eligible FIFO, same-chat serialization and drains automatically", async (t) => {
  const f = setup(t), active = new Set(), dispatched = [];
  const queue = new ResearchQueue(f.ledger, { clock: f.clock, recover: async () => {}, isActive: async (id) => active.has(id), dispatch: async (job) => { active.add(job.chat_id); dispatched.push(job.id); } });
  t.after(() => queue.close());
  const jobs = [f.add(0, 0), f.add(0, 1), f.add(0, 2), f.add(1, 0), f.add(1, 1)];
  for (let u = 2; u < 6; u++) { jobs.push(f.add(u, 0), f.add(u, 1)); }
  const same = f.add(0, 0);
  await queue.tick(); await new Promise(setImmediate);
  assert.equal(f.ledger.running("research").length, 10);
  assert.equal(f.ledger.get(jobs[2].id).status, "queued");
  assert.equal(f.ledger.get(same.id).status, "queued");
  assert.equal(f.ledger.get(jobs[3].id).status, "running");
  active.delete(f.chats[0][0].id); f.advance();
  await queue.tick(); await new Promise(setImmediate);
  assert.equal(f.ledger.get(jobs[2].id).status, "running");
  assert.equal(f.ledger.get(same.id).status, "queued");
  assert.equal(new Set(dispatched).size, dispatched.length);
});

test("Docker queue has independent 2/10 capacity and never blocks an eligible other account", (t) => {
  const f = setup(t), jobs = [];
  for (let u = 0; u < 6; u++) for (let c = 0; c < 3; c++) jobs.push(f.add(u, c, "docker"));
  for (const job of f.ledger.queued("docker")) f.ledger.start(job.id);
  assert.equal(f.ledger.running("docker").length, 10);
  assert.equal(f.ledger.running("docker").filter((j) => j.user_id === f.users[0].id).length, 2);
  assert.equal(f.ledger.get(jobs[2].id).status, "queued");
  f.ledger.finish(jobs[0].id, "completed");
  assert.equal(f.ledger.start(jobs[2].id), true);
});

test("restart preserves undispatched research, interrupts in-flight work and serializes owners", (t) => {
  const f = setup(t), waiting = f.add(0, 0), running = f.add(1, 0), docker = f.add(2, 0, "docker");
  f.ledger.start(running.id); f.ledger.start(docker.id);
  assert.throws(() => new RuntimeLedger(f.store), /占用/);
  f.ledger.close();
  const next = new RuntimeLedger(f.store); t.after(() => next.close());
  assert.equal(next.get(waiting.id).status, "queued");
  assert.equal(next.get(running.id).status, "interrupted");
  assert.equal(next.needsRecovery("docker").length, 1);
  assert.throws(() => f.ledger.start(waiting.id), /未就绪/);
});

test("cancel, disable, archive and queue caps are enforced again before dispatch", (t) => {
  const limits = runtimeLimits({}); limits.pending.user = 2;
  const f = setup(t, limits), a = f.add(0, 0), b = f.add(0, 1);
  assert.throws(() => f.add(0, 2), /队列已满/);
  f.ledger.cancelQueued(a.chat_id); assert.equal(f.ledger.start(a.id), false);
  f.store.updateProject(f.users[0], f.chats[0][1].project_id, { archived: true });
  assert.equal(f.ledger.start(b.id), false); assert.equal(f.ledger.get(b.id).status, "cancelled");
  const c = f.add(1, 0); f.store.db.prepare("UPDATE users SET disabled=1 WHERE id=?").run(f.users[1].id);
  assert.equal(f.ledger.start(c.id), false);
});

test("per-user 20/minute rate limit survives restart, usage is isolated", (t) => {
  const f = setup(t);
  for (let i = 0; i < 20; i++) f.ledger.throttle(f.users[0].id);
  assert.throws(() => f.ledger.throttle(f.users[0].id), /频繁/);
  f.ledger.close(); const next = new RuntimeLedger(f.store, { clock: f.clock }); t.after(() => next.close());
  assert.throws(() => next.throttle(f.users[0].id), /频繁/);
  next.throttle(f.users[1].id); next.usage(f.users[0].id, "upload_bytes", 100);
  assert.equal(next.summary(f.users[1].id).usage.length, 0);
  assert.equal(next.summary(f.users[0].id).usage[0].upload_bytes, 100);
});

test("recovery failure pauses dispatch, stale approval failure releases capacity", async (t) => {
  const f = setup(t), job = f.add(0, 0);
  f.ledger.start(job.id); f.ledger.close();
  const ledger = new RuntimeLedger(f.store, { clock: f.clock }); t.after(() => ledger.close());
  const later = ledger.enqueue({ kind: "research", operation: "approve", user: f.users[1], chatId: f.chats[1][0].id, payload: { revision: "old" } });
  let broken = true;
  const q = new ResearchQueue(ledger, { clock: f.clock, isActive: async () => false, recover: async () => { if (broken) throw new Error("offline"); }, dispatch: async () => { throw Object.assign(new Error("方案已变化"), { status: 409 }); } });
  t.after(() => q.close());
  await q.tick(); assert.equal(ledger.get(later.id).status, "queued");
  broken = false; await q.tick(); await new Promise(setImmediate);
  assert.equal(ledger.get(later.id).status, "failed");
  assert.equal(ledger.running("research").length, 0);
});

test("invalid operator concurrency configuration fails closed", () => {
  assert.deepEqual(runtimeLimits({}).docker, { global: 10, user: 2 });
  assert.equal(runtimeLimits({ GEO_DOCKER_CONCURRENCY: "5" }).docker.global, 5);
  assert.throws(() => runtimeLimits({ GEO_DOCKER_CONCURRENCY: "0" }));
});
