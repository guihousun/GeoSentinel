import assert from "node:assert/strict";
import { readFile, readdir, writeFile } from "node:fs/promises";
import path from "node:path";
const base = "http://127.0.0.1:8510/geo/api";
const account = JSON.parse(await readFile(".runtime/qa-account.json", "utf8"));
const login = await fetch(base + "/auth/login", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify(account),
});
const cookie = login.headers.get("set-cookie").split(";")[0];
async function api(route, method = "GET", data) {
  const res = await fetch(base + route, {
    method,
    headers: { cookie, "content-type": "application/json" },
    body: data ? JSON.stringify(data) : undefined,
  });
  const value = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(value));
  return value;
}
const { project } = await api("/projects", "POST", { title: "任务取消验收" });
const { chat } = await api(`/projects/${project.id}/chats`, "POST", {
  title: "受控长任务取消",
});
await api(`/chats/${chat.id}/prompt`, "POST", {
  text: "请制定一个软件验收任务，交由分析助手用 geo_execute_python 执行 import time; time.sleep(90)，用于测试取消功能。这不是科研计算，不需要假装有研究发现，不要访问网络或资料。先等待我的方案批准。",
});
const pause = () => new Promise((resolve) => setTimeout(resolve, 1000));
let plan;
try {
  for (let i = 0; i < 60; i++) {
    plan = await api(`/chats/${chat.id}/plan`);
    if (plan.team?.phase === "staged" && plan.team.tasks.length) break;
    await pause();
  }
  assert.ok(plan.team?.tasks.length, "plan was not staged");
  // Wait until the planning turn is idle so no in-flight edit changes the approved revision.
  for (let i = 0; i < 20; i++) {
    const h = await api(`/chats/${chat.id}/history`);
    if (h.events.at(-1)?.type === "turn/end") break;
    await pause();
  }
  plan = await api(`/chats/${chat.id}/plan`);
  await api(`/chats/${chat.id}/approve`, "POST", {
    teamId: plan.team.id,
    revision: plan.team.revision,
  });
  const jobsRoot = path.resolve(".runtime/home/geosentinel/jobs");
  let job;
  for (let i = 0; i < 60 && !job; i++) {
    for (const id of await readdir(jobsRoot)) {
      try {
        const value = JSON.parse(
          await readFile(path.join(jobsRoot, id, "manifest.json"), "utf8"),
        );
        if (value.chatId === chat.id && value.status === "running") {
          job = value;
          break;
        }
      } catch {}
    }
    if (!job) await pause();
  }
  assert.ok(job, "real Docker task did not start");
  await pause();
  const started = Date.now();
  await api(`/chats/${chat.id}/cancel`, "POST", {});
  const persisted = JSON.parse(
    await readFile(path.join(jobsRoot, job.id, "manifest.json"), "utf8"),
  );
  assert.equal(persisted.status, "cancelled");
  const after = await api(`/chats/${chat.id}/plan`);
  assert.equal(after.team?.halted, true);
  const report = {
    passed: true,
    chatId: chat.id,
    jobId: job.id,
    cancelLatencyMs: Date.now() - started,
    at: new Date().toISOString(),
  };
  await writeFile(
    ".runtime/cancellation-acceptance.json",
    JSON.stringify(report, null, 2),
  );
  console.log(JSON.stringify(report));
} finally {
  await api(`/chats/${chat.id}/cancel`, "POST", {}).catch(() => {});
}
