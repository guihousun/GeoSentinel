import { unlink } from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { randomBytes } from "node:crypto";
import path from "node:path";
import { atomicJson } from "./manager.mjs";
/** Fixed preview origin only. This probe must never create production data. */
export async function acceptPreview(manager, id, password, { fetcher = fetch, delay = (ms) => new Promise(r => setTimeout(r, ms)) } = {}) {
  const base = "http://127.0.0.1:8513";
  let cookie = "";
  const request = async (url, body, raw = false) => {
    const response = await fetcher(base + url, { method: body === undefined ? "GET" : "POST", redirect: "manual", signal: AbortSignal.timeout(15000), headers: { origin: base, cookie, ...(body === undefined ? {} : { "content-type": raw ? "application/octet-stream" : "application/json" }) }, ...(body === undefined ? {} : { body: raw ? body : JSON.stringify(body) }) });
    if (!response.ok) throw new Error(`预览验收 ${url}: HTTP ${response.status}`);
    const cookies = response.headers.getSetCookie(); if (cookies.length) cookie = cookies.map(c => c.split(";")[0]).join("; ");
    return response.json();
  };
  const health = await request("/geo/api/health");
  if (health.preview !== true || health.release !== id) throw new Error("拒绝验收：8513 不是指定候选的预览实例");
  await unlink(path.join(manager.releaseRoot(id), "acceptance.json")).catch(error => { if (error.code !== "ENOENT") throw error; });
  await request("/geo/api/auth/login", { username: "preview_user", password });
  const { project } = await request("/geo/api/projects", { title: "自动发布验收" });
  const { chat } = await request(`/geo/api/projects/${project.id}/chats`, { title: "上传读取验收" });
  const nonce = randomBytes(12).toString("hex");
  await request(`/api/upload/native?sessionId=${chat.id}&name=release-check.txt`, nonce, true);
  await request(`/geo/api/chats/${chat.id}/prompt`, { text: "读取本会话上传的 release-check.txt，仅回复文件中的口令。" });
  for (let i = 0; i < 30; i++) {
    await delay(4000);
    const history = await request(`/geo/api/chats/${chat.id}/native-history`);
    const quoted = (history.events ?? []).some(e => e.type === "assistant/message" && (e.data?.message?.content ?? []).some(p => p.type === "text" && p.text?.includes(nonce)));
    if (quoted) { await manager.verify(id); const evidence = { id, check: "preview-upload-v1", passed: true, at: Date.now(), chatId: chat.id }; await atomicJson(path.join(manager.releaseRoot(id), "acceptance.json"), evidence); return evidence; }
  }
  throw new Error("预览上传读取验收失败：智能体未返回随机口令，禁止发布");
}

export async function checkEnvironment(root) {
  await promisify(execFile)(process.execPath, ["scripts/check-env.mjs"], { cwd: root, env: process.env, windowsHide: true, timeout: 120000, maxBuffer: 4 * 1024 * 1024 });
}
