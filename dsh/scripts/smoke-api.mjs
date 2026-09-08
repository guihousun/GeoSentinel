import { randomBytes } from "node:crypto";
import { readFileSync, writeFileSync, existsSync } from "node:fs";
import { PlatformStore } from "../plugins/platform/store.mjs";
const base = "http://127.0.0.1:8510/geo/api";
const store = new PlatformStore(".runtime/home/geosentinel");
const file = ".runtime/qa-account.json";
let account;
if (existsSync(file)) account = JSON.parse(readFileSync(file));
else {
  account = {
    username: "acceptance_admin",
    password: randomBytes(24).toString("base64url"),
  };
  store.bootstrapAdmin(account.username, account.password);
  writeFileSync(file, JSON.stringify(account));
}
store.close();
let cookie = "";
async function api(route, method = "GET", data) {
  const r = await fetch(base + route, {
    method,
    headers: { cookie, "content-type": "application/json" },
    body: data ? JSON.stringify(data) : undefined,
  });
  if (r.headers.get("set-cookie"))
    cookie = r.headers.get("set-cookie").split(";")[0];
  const value = await r.json();
  if (!r.ok) throw new Error(`${route}: ${r.status} ${JSON.stringify(value)}`);
  return value;
}
await api("/auth/login", "POST", account);
const { project } = await api("/projects", "POST", { title: "DSH 接入验收" });
const { chat } = await api(`/projects/${project.id}/chats`, "POST", {
  title: "基础对话验收",
});
await api(`/chats/${chat.id}/prompt`, "POST", {
  text: "你好。请用一句中文介绍你的职责，不调用工具。",
});
writeFileSync(
  ".runtime/smoke-session.json",
  JSON.stringify({ projectId: project.id, chatId: chat.id }),
);
console.log(
  JSON.stringify({
    login: true,
    projectCreated: true,
    chatCreated: true,
    promptAccepted: true,
    chatId: chat.id,
  }),
);
