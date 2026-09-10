import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { PlatformStore } from "../plugins/platform/store.mjs";
import { createRoleBinder, parentSessionIdOf } from "../plugins/platform/role-binding.mjs";

const ROLE_TOOLS = { "数据助手": ["geo_list_files", "read"], "分析助手": ["geo_calculate_zonal_statistics"] };
const stored = { "数据助手": "NTL_Data_Searcher", "分析助手": "NTL_Analyst" };

function harness(bodies = {}) {
  const dir = mkdtempSync(path.join(tmpdir(), "geo-role-bind-"));
  const store = new PlatformStore(dir);
  const admin = store.bootstrapAdmin("admin", "valid-admin-password");
  const alice = store.acceptInvite(store.invite(admin), "alice", "alice-valid-password");
  const chat = store.createChat(alice, store.createProject(alice, "Study").id);
  const restricted = [], reports = [], agents = new Map();
  const messageBody = (id) => (bodies[id] === undefined ? []
    : [{ type: "user/message", data: { content: [{ type: "text", text: bodies[id] }] } }]);
  const binder = createRoleBinder({
    store,
    agents: { get: (id) => agents.get(id) },
    sessionController: { inspect: async (id) => ({ events: messageBody(id) }) },
    roles: ["数据助手", "分析助手", "事件助手"],
    roleTools: ROLE_TOOLS,
    toStored: (role) => stored[role] ?? role,
    report: (message) => reports.push(message),
    retries: 2,
    wait: 1,
    reapplyMs: [0],
  });
  return { dir, store, alice, chat, binder, restricted, reports, agents, messageBody,
    cleanup: () => { store.close(); rmSync(dir, { recursive: true, force: true }); } };
}

test("a delegated child is bound to its role and narrowed to that role's tools", async (t) => {
  const h = harness();
  t.after(h.cleanup);
  const child = { ctx: { tools: { restrict: (options) => h.restricted.push(options.allow) } } };
  h.agents.set("child-1", child);

  // The supervision label the supervisor is told to write is the primary source.
  assert.equal(await h.binder.bind(h.chat.id, "child-1", "数据助手：核对 inputs 目录"), "数据助手");
  assert.deepEqual(h.restricted, [["geo_list_files", "read"]]);
  assert.deepEqual(h.store.listChildren(h.alice, h.chat.id).map((member) => [member.id, member.role]), [["child-1", "NTL_Data_Searcher"]]);

  // Binding is idempotent: a second observation must not restrict twice.
  assert.equal(await h.binder.bind(h.chat.id, "child-1", "数据助手：核对 inputs 目录"), undefined);
  assert.equal(h.restricted.length, 1);

  // When the supervisor names the role only in the prompt body, the child's own first
  // user message still resolves it.
  const bodyCase = harness({ "child-2": "请分析助手计算上海与重庆的 ANTL。" });
  t.after(batchCleanup(bodyCase));
  bodyCase.agents.set("child-2", { ctx: { tools: { restrict: (options) => bodyCase.restricted.push(options.allow) } } });
  assert.equal(await bodyCase.binder.bind(bodyCase.chat.id, "child-2", undefined), "分析助手");
  assert.deepEqual(bodyCase.restricted, [["geo_calculate_zonal_statistics"]]);

  // An unattributable child is left unbound and reported, never silently bound to
  // something else.
  const unknown = await h.binder.bindListed(h.chat.id, [{ kind: "child", id: "child-3", label: "随便写点什么" }]);
  assert.equal(unknown?.id, "child-3");
  assert.equal(h.store.listChildren(h.alice, h.chat.id).some((member) => member.id === "child-3"), false);

  // A child that is already recorded is skipped by the catalog pass as well.
  const again = await h.binder.bindListed(h.chat.id, [{ kind: "child", id: "child-1", label: "数据助手：再来一次" }]);
  assert.equal(again, undefined);
  assert.equal(h.restricted.length, 1);
});

test("the spawn event's parent resolves to the chat however the runtime hands it over", () => {
  assert.equal(parentSessionIdOf("chat-1"), "chat-1");
  assert.equal(parentSessionIdOf({ id: "chat-2" }), "chat-2");
  assert.equal(parentSessionIdOf({ sessionId: "chat-3" }), "chat-3");
  assert.equal(parentSessionIdOf({ session: { id: "chat-4" } }), "chat-4");
  assert.equal(parentSessionIdOf(undefined), undefined);
  assert.equal(parentSessionIdOf({}), undefined);
});

test("the role comes from the delegation descriptor, with a bounded retry while the child is not inspectable yet", async (t) => {
  const h = harness();
  t.after(h.cleanup);
  const child = { ctx: { tools: { restrict: (options) => h.restricted.push(options.allow) } } };
  h.agents.set("child-9", child);
  // The runtime appends `subagent/descriptor` (carrying the supervisor's description as
  // `label`) to the child session; the spawn event itself carries no label.
  const inspectCalls = [];
  const descriptorBinder = createRoleBinder({
    store: h.store, agents: { get: (id) => h.agents.get(id) },
    sessionController: { inspect: async (id) => {
      inspectCalls.push(id);
      if (inspectCalls.length < 3) throw new Error("session not available yet");
      return { events: [{ type: "subagent/descriptor", data: { mode: "one-shot", provider: "in-process", label: "数据助手：核对 inputs" } },
        { type: "user/message", data: { content: [{ type: "text", text: "你的任务：列出工作区文件" }] } }] };
    } },
    roles: ["数据助手", "分析助手", "事件助手"], roleTools: ROLE_TOOLS,
    toStored: (role) => stored[role] ?? role, report: () => {},
    retries: 4, wait: 1, reapplyMs: [0],
  });
  assert.equal(await descriptorBinder.bind(h.chat.id, "child-9"), "数据助手");
  assert.equal(inspectCalls.length, 3, "应先重试到子会话可读取为止");
  assert.deepEqual(h.restricted, [["geo_list_files", "read"]]);
  // A child that never becomes inspectable stays unbound instead of being guessed.
  const never = createRoleBinder({
    store: h.store, agents: { get: () => undefined },
    sessionController: { inspect: async () => { throw new Error("still nothing"); } },
    roles: ["数据助手"], roleTools: ROLE_TOOLS, toStored: (role) => role, report: () => {}, retries: 1, wait: 1, reapplyMs: [0],
  });
  assert.equal(await never.bind(h.chat.id, "child-10"), undefined);
  assert.equal(h.store.listChildren(h.alice, h.chat.id).some((member) => member.id === "child-10"), false);
});

test("a spawned child is bound from its own session header, since the event carries no parent", async (t) => {
  // Measured on 0.1.5: `subagent/start` delivers `{ runId, provider, id, local }` and the
  // parent agent (the emit's second argument) never reaches the listener, so the parent
  // must come from the child's session header once the agent exists.
  const h = harness();
  t.after(h.cleanup);
  const child = { session: { header: { parentSession: h.chat.id } },
    ctx: { tools: { restrict: (options) => h.restricted.push(options.allow) } } };
  let present = false;
  h.agents.set("child-7", undefined);
  const binder = createRoleBinder({
    store: h.store,
    agents: { get: (id) => (present && id === "child-7" ? child : undefined) },
    sessionController: { inspect: async () => ({ events: [{ type: "subagent/descriptor", data: { label: "数据助手：核对" } }] }) },
    roles: ["数据助手", "分析助手", "事件助手"], roleTools: ROLE_TOOLS,
    toStored: (role) => stored[role] ?? role, report: () => {}, retries: 4, wait: 1, reapplyMs: [0],
  });
  setTimeout(() => { present = true; }, 5);
  assert.equal(await binder.bindSpawned("child-7"), "数据助手");
  assert.deepEqual(h.restricted, [["geo_list_files", "read"]]);
  assert.deepEqual(h.store.listChildren(h.alice, h.chat.id).map((member) => member.id), ["child-7"]);
});

test("the role restriction is re-applied across the creation window", async (t) => {
  // The agent-preset layer composes after binding and can add its delegation rows back
  // (measured: a restricted child still held `subagent` after the first restriction), so
  // the binder re-applies the role table a few times. Each application is identical.
  const h = harness();
  t.after(h.cleanup);
  const child = { ctx: { tools: { restrict: (options) => h.restricted.push(options.allow) } } };
  h.agents.set("child-5", child);
  const binder = createRoleBinder({
    store: h.store, agents: { get: (id) => (id === "child-5" ? child : undefined) },
    sessionController: { inspect: async () => ({ events: [{ type: "subagent/descriptor", data: { label: "数据助手：核对" } }] }) },
    roles: ["数据助手"], roleTools: ROLE_TOOLS, toStored: (role) => stored[role] ?? role, report: () => {},
    retries: 1, wait: 1, reapplyMs: [0, 5, 10],
  });
  assert.equal(await binder.bind(h.chat.id, "child-5"), "数据助手");
  assert.equal(h.restricted.length, 3);
  assert.equal(new Set(h.restricted.map((table) => table.join(","))).size, 1);
});

function batchCleanup(h) { return () => h.cleanup(); }
