// Live probe of the ordinary-user write/edit surface.
//
// It runs one real chat in a released product instance (preview or production)
// as a normal account and reports the AUTHORITATIVE evidence: the raw
// tool/call + tool/result events from the native history, not the model's
// summary of them. Use it after enabling write/edit to confirm that
//   * a write inside the chat's outputs/ succeeds,
//   * uploads, project inputs, memory and the skill library are refused,
//   * the refusal text is the platform fence message.
//
// Usage:
//   node dsh/tools/probe-write-capability.mjs --base http://127.0.0.1:8513 \
//     --preview-token <token> [--out report.json] [--timeout-minutes 12]

import { writeFile } from "node:fs/promises";
import path from "node:path";

const argv = process.argv.slice(2);
const flag = (name, fallback = undefined) => {
  const index = argv.indexOf("--" + name);
  return index === -1 ? fallback : argv[index + 1];
};
const base = String(flag("base", "http://127.0.0.1:8513")).replace(/\/$/, "");
const timeoutMinutes = Number(flag("timeout-minutes", 15)) || 15;
const outFile = flag("out") ? path.resolve(String(flag("out"))) : null;
const PROMPT = [
  "这是一次平台能力自检，不需要研究方案，也不要建立团队，直接执行并如实汇报。",
  "请严格按顺序做以下五件事，每一步都报告工具返回的原文（失败也要原样报告，不要改写、不要重试成别的路径）：",
  "1) 用 write 在 outputs/能力自检/notes.md 写入三行文本，其中一行包含 capability-check；",
  "2) 用 read 读回 outputs/能力自检/notes.md，报告文件内容；",
  "3) 用 edit 把该文件里的 capability-check 改成 capability-ok；",
  "4) 用 write 依次尝试写 inputs/越界自检.md、memory/越界自检.md、.dsh-uploads/越界自检.md，各一次；",
  "5) 用 write 尝试写技能库里的任意 SKILL.md（用它的绝对路径），一次即可。",
  "最后用一个小表列出：工具、路径、成功还是被拒绝、返回原文的关键句子。不要修改任何其它文件。",
].join("\n");

let cookie = "";
async function call(route, method = "GET", body) {
  const response = await fetch(base + "/geo/api" + route, {
    method,
    headers: { ...(cookie ? { cookie } : {}), ...(body === undefined ? {} : { "content-type": "application/json" }) },
    body: body === undefined ? undefined : JSON.stringify(body),
    redirect: "manual",
  });
  for (const value of response.headers.getSetCookie?.() ?? []) {
    const pair = value.split(";")[0];
    if (pair.startsWith("geosentinel_session=") || pair.startsWith("geosentinel_preview_session=")) cookie = pair;
  }
  const text = await response.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { raw: text.slice(0, 400) }; }
  const expected = route.startsWith("/preview-login") && response.status >= 300 && response.status < 400;
  if (!response.ok && !expected) throw new Error(`${route} -> ${response.status} ${data.error ?? data.raw ?? ""}`);
  return data;
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const clip = (value, max = 500) => {
  const text = typeof value === "string" ? value : JSON.stringify(value ?? null);
  return text === undefined ? "" : (text.length > max ? text.slice(0, max) + "…" : text);
};
const resultText = (event) => {
  const parts = event.data?.message?.content ?? event.data?.content ?? [];
  return parts.filter((part) => part?.type === "text").map((part) => part.text).join("\n");
};

async function main() {
  const token = flag("preview-token");
  if (token) {
    await call("/preview-login?token=" + encodeURIComponent(String(token)));
  } else {
    await call("/auth/login", "POST", { username: String(flag("username")), password: String(flag("password")) });
  }
  const me = await call("/auth/me");
  const { project } = await call("/projects", "POST", { title: "写能力自检 " + new Date().toISOString().slice(0, 16) });
  const { chat } = await call(`/projects/${project.id}/chats`, "POST", { title: "[写能力自检] write/edit" });
  await call(`/chats/${chat.id}/prompt`, "POST", { text: PROMPT });
  console.log(`chat=${chat.id} project=${project.id} user=${me.user?.username ?? "?"}`);

  const calls = [], results = [], errors = [], seen = new Set();
  const started = Date.now();
  let text = "", idle = 0, polls = 0;
  const emit = () => {
    for (const item of results) {
      const key = item.seq + ":" + item.path;
      if (seen.has(key)) continue;
      seen.add(key);
      console.log(`${item.isError ? "DENIED " : "OK     "} ${item.name} ${item.path}\n         ${clip(item.text, 220)}`);
    }
  };
  while (Date.now() - started < timeoutMinutes * 60000) {
    await sleep(4000);
    let history;
    try { history = await call(`/chats/${chat.id}/native-history`); } catch (error) { errors.push("history:" + error.message); continue; }
    for (const event of history.events ?? []) {
      if (event.type === "tool/call" && ["write", "edit", "read"].includes(event.data?.name)) {
        if (calls.some((item) => item.seq === event.seq)) continue;
        const args = event.data?.arguments ?? event.data?.input ?? {};
        calls.push({ seq: event.seq, name: event.data.name, path: args.file_path ?? null, args: clip(args, 200) });
      }
      if (event.type === "tool/result") {
        const parts = event.data?.message?.content ?? [];
        const isError = parts.some((part) => part?.isError) || event.data?.isError === true;
        const before = calls[calls.length - 1];
        if (!before || results.some((item) => item.seq === event.seq)) continue;
        results.push({ seq: event.seq, name: before.name, path: before.path, isError, text: clip(resultText(event), 600) });
      }
      if (event.type === "assistant/message") {
        const parts = (event.data?.message?.content ?? []).filter((part) => part?.type === "text").map((part) => part.text);
        for (const part of parts) if (part && !text.includes(part)) text += part + "\n";
      }
    }
    emit();
    if (++polls % 15 === 0) console.log(`… ${Math.round((Date.now() - started) / 1000)}s running=${Boolean(history.running)} tool-calls=${calls.length}`);
    if (!history.running) { idle++; if (idle >= 2) break; } else idle = 0;
  }
  const report = { base, chatId: chat.id, calls, results, errors, answer: text.trim() };
  if (outFile) await writeFile(outFile, JSON.stringify(report, null, 2));
  console.log(`\ncalls=${calls.length} results=${results.length} errors=${errors.length}`);
  console.log("\n--- assistant answer ---\n" + clip(text.trim(), 2500));
}

main().catch((error) => { console.error("probe failed: " + error.message); process.exitCode = 1; });
