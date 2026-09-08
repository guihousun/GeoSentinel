import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { PlatformStore } from "../plugins/platform/store.mjs";

// Deliberately separate from the live service's home and 8510 listener.
const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const base = "http://127.0.0.1:8511/geo/api";
const dataRoot = path.join(root, ".runtime/upgrade-rc1-home/geosentinel");
if (!existsSync(path.join(dataRoot, "platform.sqlite"))) throw new Error("Start the isolated upgrade server first");
const store = new PlatformStore(dataRoot);
const account = { username: "upgrade_" + randomBytes(4).toString("hex"), password: randomBytes(18).toString("base64url") };
let admin = store.db.prepare("SELECT id FROM users WHERE admin=1 AND disabled=0 LIMIT 1").get();
if (!admin) admin = store.bootstrapAdmin("upgrade_admin", randomBytes(24).toString("base64url"));
store.acceptInvite(store.invite(admin), account.username, account.password);
store.close();
let cookie;
async function api(route, method = "GET", body) {
  const response = await fetch(base + route, { method, headers: { "content-type": "application/json", ...(cookie ? { cookie } : {}) },
    body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(20000) });
  cookie = response.headers.get("set-cookie")?.split(";")[0] ?? cookie;
  const result = await response.json();
  assert.equal(response.ok, true, `${route} ${response.status}: ${JSON.stringify(result)}`);
  return result;
}
async function until(fn, seconds = 180) {
  const end = Date.now() + seconds * 1000;
  while (Date.now() < end) { const result = await fn(); if (result) return result; await new Promise((resolve) => setTimeout(resolve, 2000)); }
  throw new Error("Upgrade smoke timed out");
}
const receipt = {};
let chatId;
try {
  await api("/auth/login", "POST", account); receipt.login = true;
  const { project } = await api("/projects", "POST", { title: "版本升级隔离验收" });
  const { chat } = await api(`/projects/${project.id}/chats`, "POST", { title: "问答与团队兼容" });
  chatId = chat.id;
  receipt.project = project.id; receipt.chat = chat.id;
  await api(`/chats/${chatId}/prompt`, "POST", { text: "你好。软件版本验收：只回答一句中文，不调用工具。" });
  await until(async () => {
    const history = await api(`/chats/${chatId}/history`);
    return history.events.some((e) => e.type === "assistant/message") && history.events.some((e) => e.type === "turn/end");
  });
  receipt.greeting = true; console.log("Login, project, chat and actual model response passed.");
  await api(`/chats/${chatId}/prompt`, "POST", { text: "这是软件多智能体验收，不是实际科研结论。请创建研究团队，只添加 NTL_Event_Tracker 事件助手，分配一个任务：调用 geo_list_files 查看当前资料列表，说明资料是否足以核验冲突事件，不访问外网，不编造事件，不运行 Python 或 GEE。先建立 staged 方案等待我的确认。完成后向主管汇报，主管给一句结果。" });
  const team = await until(async () => { const { team } = await api(`/chats/${chatId}/plan`); return team?.phase === "staged" && team.tasks?.length && team.members?.length ? team : null; });
  const settle = await until(async () => { const history = await api(`/chats/${chatId}/history`); const starts = history.events.filter((e) => e.type === "turn/start").length; const ends = history.events.filter((e) => e.type === "turn/end").length; return starts === ends; });
  assert.ok(settle);
  const current = (await api(`/chats/${chatId}/plan`)).team;
  await api(`/chats/${chatId}/approve`, "POST", { teamId: current.id, revision: current.revision }); receipt.approval = true;
  await until(async () => {
    const history = await api(`/chats/${chatId}/history`);
    const plan = (await api(`/chats/${chatId}/plan`)).team;
    receipt.childVisible = history.events.some((e) => e.agentRole === "NTL_Event_Tracker" && e.type === "assistant/message");
    receipt.specialistTool = history.events.some((e) => e.agentRole === "NTL_Event_Tracker" && e.type === "tool/call" && e.data.name === "geo_list_files");
    return receipt.childVisible && receipt.specialistTool && plan.tasks.every((task) => task.status === "completed");
  });
  receipt.passed = true; console.log(JSON.stringify(receipt));
} finally {
  if (chatId) await api(`/chats/${chatId}/cancel`, "POST").catch(() => {});
  await mkdir(path.join(root, ".runtime"), { recursive: true });
  await writeFile(path.join(root, ".runtime/upgrade-acceptance.json"), JSON.stringify(receipt, null, 2));
}
