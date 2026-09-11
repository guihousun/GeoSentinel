#!/usr/bin/env node
/**
 * GeoSentinel 地缘环境智能计算基准（30 例）执行器。
 *
 * 只通过产品 HTTP 接口驱动一个普通用户账号：建项目、建对话、发问题、
 * 以用户身份回答方案审批、收集最终回答、工具名与产物，然后按 cases.json
 * 的期望项打分。它不调用任何内部插件或模型接口，也不写入产品源码。
 *
 * 用法：
 *   node dsh/benchmark/run.mjs --base http://127.0.0.1:8513 \
 *     --preview-token <预览登录 token> --concurrency 3 --out report.json
 *   node dsh/benchmark/run.mjs --base http://127.0.0.1:8511 \
 *     --username bench_user --password '...' --invite <邀请码>
 *
 * 选项：--only B01,B02  只跑指定用例；--timeout 覆盖单例超时分钟；
 *      --no-approve 不自动批准方案（默认以用户身份批准并记录）。
 */
import { readFile, writeFile, mkdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const argv = process.argv.slice(2);
const flag = (name, fallback = undefined) => {
  const index = argv.indexOf("--" + name);
  if (index < 0) return fallback;
  const value = argv[index + 1];
  return value === undefined || value.startsWith("--") ? true : value;
};

const base = String(flag("base", "http://127.0.0.1:8513")).replace(/\/$/, "");
const concurrency = Math.max(1, Math.min(4, Number(flag("concurrency", 3)) || 3));
const only = flag("only") ? String(flag("only")).split(",").map((id) => id.trim()).filter(Boolean) : null;
const timeoutOverride = Number(flag("timeout", 0)) || 0;
const autoApprove = flag("no-approve", false) !== true;
const maxUnblocks = Math.max(0, Math.min(5, Number(flag("unblocks", 2)) ?? 2));
const outFile = path.resolve(String(flag("out", path.join(here, "report.json"))));
const projectTitle = String(flag("project", "基准测试 " + new Date().toISOString().slice(0, 16)));

const APPROVE_LABELS = ["确认方案并开始", "开始执行", "确认"];
// Sent when the model blocks on a question written in prose instead of raising a
// question. The operator's standing decision is "do not wait for me, deliver the
// result together with its limits" — it does not supply any method answer.
const UNBLOCK_PROMPT = "请按平台默认口径继续完成本次任务，不必再等我确认；需要选择时采用你列出的第一个选项。不要再向我提问，直接给出结论、产物与限制说明。";
let cookie = "";

async function call(route, method = "GET", body) {
  const response = await fetch(base + "/geo/api" + route, {
    method,
    headers: { ...(cookie ? { cookie } : {}), ...(body === undefined ? {} : { "content-type": "application/json" }) },
    body: body === undefined ? undefined : JSON.stringify(body),
    redirect: "manual",
  });
  const setCookie = response.headers.getSetCookie?.() ?? [];
  for (const value of setCookie) {
    const pair = value.split(";")[0];
    if (pair.startsWith("geosentinel_session=") || pair.startsWith("geosentinel_preview_session=")) cookie = pair;
  }
  const text = await response.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { raw: text.slice(0, 400) }; }
  // The preview login is a deliberate 302 that only carries the session cookie.
  const expected = route.startsWith("/preview-login") && response.status >= 300 && response.status < 400;
  if (!response.ok && !expected) throw new Error(`${route} -> ${response.status} ${data.error ?? ""}`);
  return data;
}

async function login() {
  const token = flag("preview-token");
  if (token) {
    await call("/preview-login?token=" + encodeURIComponent(String(token)));
    const me = await call("/auth/me");
    return me.user;
  }
  const username = flag("username"), password = flag("password"), invite = flag("invite");
  if (invite) await call("/auth/join", "POST", { invite: String(invite), username: String(username), password: String(password) }).catch(() => {});
  const result = await call("/auth/login", "POST", { username: String(username), password: String(password) });
  return result.user;
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** Choose one offered option label for a question, preferring a declared answer. */
function selectOption(question, wanted) {
  const options = (question?.options ?? []).map((option) => option.label);
  if (!wanted) return options[0] ?? null;
  return options.find((candidate) => candidate.includes(wanted)) ?? (options.includes(wanted) ? wanted : options[0] ?? null);
}

function answerOf(pending, wanted) {
  if (!pending) return null;
  const questions = pending.questions ?? [];
  if (!questions.length) return null;
  // The platform requires EVERY question in the request to be answered
  // ("请完整回答问题"); answering only the first one was rejected with 400, which
  // silently left the case waiting until its timeout.
  const preferences = Array.isArray(wanted) ? wanted : [wanted];
  const answers = [];
  for (const [index, question] of questions.entries()) {
    const preference = preferences[index] ?? preferences[0];
    const options = (question.options ?? []).map((option) => option.label);
    // A declared preference wins when it names (or is contained in) an offered
    // option; otherwise the standing benchmark decision is the first option, which
    // is what a user clicking through the review would pick.
    const label = preference
      ? options.find((candidate) => candidate.includes(preference)) ?? (options.includes(preference) ? preference : options[0])
      : APPROVE_LABELS.find((candidate) => options.includes(candidate)) ?? options[0];
    if (label) answers.push({ id: question.id, selected: [label] });
  }
  if (answers.length !== questions.length) return null;
  return { requestId: pending.id, answer: { answers } };
}

async function collectSubagentTools(chatId) {
  const names = new Set();
  const members = [];
  try {
    const catalog = await call(`/chats/${chatId}/subagents`);
    for (const entry of catalog.entries ?? []) {
      if (entry.kind !== "child" || !entry.id) continue;
      members.push({ id: entry.id, label: entry.label, activity: entry.activity });
      const history = await call(`/chats/${chatId}/subagents/${entry.id}/history`).catch(() => null);
      for (const event of history?.events ?? [])
        if (event.type === "tool/call" && event.data?.name) names.add(event.data.name);
    }
  } catch {}
  return { tools: [...names].sort(), members };
}

async function runCase(item, projectId) {
  const started = Date.now();
  const { chat } = await call(`/projects/${projectId}/chats`, "POST", { title: `[${item.id}] ${item.title}` });
  const chatId = chat.id;
  await call(`/chats/${chatId}/prompt`, "POST", { text: item.prompt });
  const timeoutMs = (timeoutOverride || item.timeoutMinutes || 30) * 60000;
  // Admission control is a platform feature, not a case failure: while a case
  // is still queued (no events, not running) it consumes no case budget. The
  // queue itself is capped so a starved case still ends with a clear reason.
  const queueCapMs = 45 * 60000;
  const queuedSince = Date.now();
  let activeStart = 0;
  const tools = new Set();
  const callNames = new Map();
  const errors = new Set();
  const unanswered = new Set();
  const answered = new Set();
  const answerAttempts = new Map();
  let unblocks = 0;
  let text = "", approvals = 0, idle = 0, lastLength = -1, planSeen = false;
  let turns = 0, steps = 0;
  // A staged plan ends its turn before the approved execution begins, so an
  // idle poll right after approval must not be read as completion.
  let lastSeq = 0, lastAssistantSeq = 0, approvedSeq = -1, scannedSeq = -1;
  while (true) {
    await sleep(5000);
    let history;
    try { history = await call(`/chats/${chatId}/native-history`); } catch (error) { errors.add("history:" + error.message); continue; }
    const hasEvents = (history.events ?? []).length > 0;
    if (!hasEvents && !history.running) {
      if (Date.now() - queuedSince > queueCapMs) { errors.add("queue-timeout"); break; }
      continue;
    }
    if (!activeStart) activeStart = Date.now();
    if (Date.now() - activeStart > timeoutMs) { errors.add("case-timeout"); break; }
    for (const event of history.events ?? []) {
      if (typeof event.seq === "number") lastSeq = Math.max(lastSeq, event.seq);
      // Each poll returns the whole history: only events newer than the last scan
      // may be recorded, or one tool error would be counted on every poll.
      if (typeof event.seq === "number" && event.seq <= scannedSeq) continue;
      if (event.type === "tool/call" && event.data?.name) {
        tools.add(event.data.name);
        // native-history replaces a failing tool's message with a fixed sentence, so
        // the failing tool can only be named by correlating the result's callId with
        // the call event that preceded it.
        if (event.data.callId) callNames.set(event.data.callId, event.data.name);
      }
      if (event.type === "tool/result" && event.data?.message?.content?.[0]?.isError) {
        const callId = event.data?.message?.source?.callId;
        errors.add("tool-error:" + (callNames.get(callId) ?? callId ?? "未知"));
      }
      if (event.type === "step/end") steps++;
      if (event.type === "turn/end") turns++;
      if (event.type === "assistant/message") {
        if (typeof event.seq === "number") lastAssistantSeq = Math.max(lastAssistantSeq, event.seq);
        const parts = (event.data?.message?.content ?? []).filter((part) => part.type === "text").map((part) => part.text);
        // Accumulate every assistant message: the closing bookkeeping note is
        // not the research answer, and a keyword must match anywhere in the
        // session's assistant output, not only in its last message.
        for (const part of parts) if (part && !text.includes(part)) text += part + "\n";
      }
    }
    scannedSeq = Math.max(scannedSeq, lastSeq);
    const { pending } = await call(`/chats/${chatId}/questions`).catch(() => ({ pending: null }));
    // A pending request is answered a bounded number of times. Repeating the POST
    // on every poll (the id stays pending until the platform retires it) produced
    // hundreds of failed attempts, but never retrying one transient failure leaves
    // the case waiting until its timeout — both are wrong.
    const attempts = pending ? answerAttempts.get(pending.id) ?? 0 : 0;
    if (pending && !answered.has(pending.id) && attempts < 3) {
      planSeen = planSeen || pending.kind === "plan-review";
      // The plan-approval question is answered automatically, because that is the
      // user's standing decision for a benchmark run. Method clarifications are
      // answered the same way by default (the operator's standing decision is
      // "accept the platform's first offered option"); a case sets
      // `clarify: false` to leave them unanswered, and `answers` still forces a
      // specific choice. Leaving them unanswered by default burned whole case
      // budgets in B10, C03, C04 and C10 without measuring anything.
      const wantsClarify = pending.kind !== "plan-review" && item.clarify !== false;
      const declared = Array.isArray(item.answers) && item.answers.length ? item.answers : null;
      const reply = pending.kind === "plan-review" && autoApprove
        ? answerOf(pending, declared?.[0])
        : declared
          ? { requestId: pending.id, answer: { answers: pending.questions.map((question, index) => ({ id: question.id, selected: [selectOption(question, declared[index] ?? declared[0])].filter(Boolean) })) } }
          : wantsClarify
            ? answerOf(pending)
            : null;
      if (reply) {
        answerAttempts.set(pending.id, attempts + 1);
        await call(`/chats/${chatId}/questions`, "POST", reply)
          .then(() => answered.add(pending.id))
          .catch((error) => errors.add("approve:" + error.message));
        approvals++;
        if (pending.kind === "plan-review") approvedSeq = lastSeq;
      } else if (!unanswered.has(pending.id)) {
        unanswered.add(pending.id);
        errors.add("unanswered-question:" + (pending.questions?.[0]?.question ?? "").slice(0, 80));
      }
    }
    // A delegated turn ends while the specialists are still working, so "main
    // session idle" is not "case finished": a case that only looks idle while a
    // member is running would be scored (and cancelled) halfway through.
    const members = await call(`/chats/${chatId}/subagents`).catch(() => null);
    const membersRunning = (members?.entries ?? []).some((entry) => entry.kind === "child" && entry.activity === "running");
    if (!history.running && !membersRunning) {
      const answeredAfterPlan = approvedSeq < 0 || lastAssistantSeq > approvedSeq;
      if (text.length > 0 && text.length === lastLength && answeredAfterPlan) idle++; else idle = 0;
      lastLength = text.length;
      if (idle >= 3) {
        // The model sometimes blocks by ASKING IN PROSE ("需要你确认三件事")
        // instead of raising a question, so there is nothing for the harness to
        // answer. A real user would say "just proceed" — that is a bounded,
        // recorded user decision, not a fabricated method answer.
        if (unblocks < maxUnblocks) {
          unblocks++;
          errors.add("unblock-prompt:" + unblocks + "@seq" + lastSeq);
          await call(`/chats/${chatId}/prompt`, "POST", { text: UNBLOCK_PROMPT }).catch((error) => errors.add("unblock:" + error.message));
          idle = 0;
          continue;
        }
        break;
      }
    } else idle = 0;
  }
  const files = await call(`/chats/${chatId}/files`).catch(() => ({ files: [] }));
  // Release the chat's admission slot: an abandoned case (timeout) can still
  // hold a running team, which would starve the next queued case.
  await call(`/chats/${chatId}/cancel`, "POST", {}).catch(() => {});
  // Specialists do the domain work in a team run, so their tool calls count
  // too; the supervisor's own session alone would under-report the platform.
  const delegated = await collectSubagentTools(chatId);
  return { chatId, tools: [...new Set([...tools, ...delegated.tools])].sort(), ownTools: [...tools].sort(), subagents: delegated.members,
    // Turns/steps and the seq of each nudge let a reader separate "the model stopped
    // early and a real user would have said continue" from "the case hit its budget".
    turns, steps,
    // Machine-readable counterparts of the two budget errors: a reader must be able
    // to tell "ran out of budget" from "finished and missed a check" without parsing
    // the error strings (summarize.mjs renders them as 预算用尽 / 排队超时).
    budgetExhausted: errors.has("case-timeout"), queueStarved: errors.has("queue-timeout"),
    text, files: (files.files ?? []).map((file) => file.name), errors: [...errors], approvals, unanswered: [...unanswered], planSeen, approvedSeq, lastAssistantSeq, elapsedSeconds: Math.round((Date.now() - started) / 1000) };
}

function score(item, result) {
  const expect = item.expect ?? {};
  // Keywords and forbidden terms are regular expressions: several cases accept
  // synonyms, and a forbidden term must describe a *claim* ("证明了因果"),
  // not the bare word — otherwise an honest disclaimer ("这不是因果证明")
  // would be scored as a violation.
  const matches = (pattern, text) => {
    try {
      return new RegExp(pattern, "s").test(text);
    } catch {
      return text.includes(pattern);
    }
  };
  const checks = [];
  for (const tool of expect.tools ?? []) checks.push({ name: "tool:" + tool, passed: result.tools.includes(tool) });
  for (const extension of expect.artifacts ?? []) checks.push({ name: "artifact:" + extension, passed: result.files.some((file) => file.endsWith(extension)) });
  for (const keyword of expect.keywords ?? []) checks.push({ name: "keyword:" + keyword, passed: matches(keyword, result.text) });
  for (const keyword of expect.forbidden ?? []) checks.push({ name: "forbidden:" + keyword, passed: !matches(keyword, result.text) });
  const passed = checks.filter((check) => check.passed).length;
  return { checks, passed, total: checks.length, score: checks.length ? passed / checks.length : 1, pass: checks.length === 0 || passed === checks.length };
}

async function main() {
  const suite = JSON.parse(await readFile(path.join(here, "cases.json"), "utf8"));
  const cases = (only ? suite.cases.filter((item) => only.includes(item.id)) : suite.cases);
  const user = await login();
  if (!user) throw new Error("登录失败");
  if (user.admin) console.warn("警告：该账号是管理员，本次结果不能当作普通用户模式证据");
  const { project } = await call("/projects", "POST", { title: projectTitle });
  console.log(`基准：${cases.length} 例 · 并发 ${concurrency} · 账号 ${user.username}${user.admin ? "（管理员）" : "（普通用户）"} · 实例 ${base}`);
  const results = [];
  let cursor = 0;
  const worker = async () => {
    while (cursor < cases.length) {
      const item = cases[cursor++];
      const started = Date.now();
      let record;
      try {
        const result = await runCase(item, project.id);
        record = { id: item.id, tier: item.tier, title: item.title, prompt: item.prompt, ...result, ...score(item, result) };
      } catch (error) {
        record = { id: item.id, tier: item.tier, title: item.title, prompt: item.prompt, error: String(error.message), tools: [], text: "", files: [], checks: [], passed: 0, total: 0, score: 0, pass: false, elapsedSeconds: Math.round((Date.now() - started) / 1000) };
      }
      results.push(record);
      console.log(`${record.pass ? "PASS" : "FAIL"} ${record.id} ${record.tier} ${record.passed}/${record.total} ${record.elapsedSeconds}s ${record.title}`);
      await writeFile(outFile, JSON.stringify({ schema: "geosentinel.benchmark.report.v1", base, user: { username: user.username, admin: Boolean(user.admin) }, project: project.id, started: new Date().toISOString(), results }, null, 2));
    }
  };
  await Promise.all(Array.from({ length: concurrency }, worker));
  const summary = results.reduce((total, record) => { const tier = (total[record.tier] ??= { pass: 0, count: 0, score: 0 }); tier.count++; tier.score += record.score; if (record.pass) tier.pass++; return total; }, {});
  console.log("\n分档汇总：");
  for (const [tier, value] of Object.entries(summary)) console.log(`  ${tier}: ${value.pass}/${value.count} 全通过，平均得分 ${(value.score / value.count).toFixed(2)}`);
  console.log(`报告：${outFile}`);
}

main().catch((error) => { console.error("基准执行失败：" + error.stack); process.exitCode = 1; });
