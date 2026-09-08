import assert from "node:assert/strict";
import { mkdtemp, writeFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import path from "node:path";
import { PlatformStore } from "../plugins/platform/store.mjs";
import { RuntimeLedger } from "../plugins/platform/runtime.mjs";
import { DockerRunner } from "../plugins/research/docker.mjs";

const root = await mkdtemp(path.resolve(".runtime/recovery-")), store = new PlatformStore(root);
const docker = (...args) => { const r = spawnSync("docker", args, { encoding: "utf8", windowsHide: true, timeout: 30000 }); if (r.status !== 0) throw new Error(r.stderr || "Docker failed"); return r.stdout.trim(); };
let runtime = new RuntimeLedger(store);
const user = store.createUser("recovery_qa", "isolated-recovery-password"), project = store.createProject(user, "Recovery QA"), chat = store.createChat(user, project.id);
const job = runtime.enqueue({ kind: "docker", operation: "execute", user, chatId: chat.id, payload: {} }); runtime.start(job.id);
const own = `geosentinel-${job.id}`, foreign = `geosentinel-${randomUUID()}`, healthy = `geosentinel-${randomUUID()}`;
try {
  for (const [name, scope] of [[own, runtime.scope], [foreign, "other-platform-qa"], [healthy, runtime.scope]])
    docker("create", "--name", name, "--label", `geosentinel.scope=${scope}`, "--network", "none", "geosentinel-gis:0.1");
  runtime.close(); runtime = new RuntimeLedger(store);
  assert.equal(runtime.get(job.id).status, "interrupted");
  await new DockerRunner({ store, runtime }).ensureRecovered();
  assert.equal(docker("ps", "-a", "--filter", `name=${own}`, "--format", "{{.Names}}"), "");
  assert.equal(docker("ps", "-a", "--filter", `name=${foreign}`, "--format", "{{.Names}}"), foreign);
  assert.equal(docker("ps", "-a", "--filter", `name=${healthy}`, "--format", "{{.Names}}"), healthy);
  assert.equal(runtime.needsRecovery("docker").length, 0);
  const report = { passed: true, checks: ["persisted-interruption", "owned-orphan-removed", "other-platform-container-preserved", "non-interrupted-same-platform-container-preserved"], at: new Date().toISOString() };
  await writeFile(".runtime/recovery-acceptance.json", JSON.stringify(report, null, 2)); console.log(JSON.stringify(report));
} finally {
  for (const name of [own, foreign, healthy]) { try { docker("rm", "-f", name); } catch {} }
  runtime.close(); store.close();
}
