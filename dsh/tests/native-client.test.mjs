import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import vm from "node:vm";

test("native client restores remembered history and keeps project/file ownership aligned", async () => {
  const source = await readFile(new URL("../plugins/workbench/native/client.js", import.meta.url), "utf8");
  const stores = [], requests = [], services = {}, slots = new Map();
  const snapshot = (initial) => {
    let state = initial;
    const result = { getSnapshot: () => state, subscribe: () => () => {}, set: (next) => { state = next; } };
    stores.push(result); return result;
  };
  const projects = [{ id: "p1", title: "First" }, { id: "p2", title: "Second" }];
  const ctx = { provide: (key, value) => { services[key] = value; ctx[key] = value; },
    get: (key) => key === "layout" ? { closeDetails() {} } : services[key],
    plugin: () => ({ dispose() {} }), inject() {}, on() {}, slots: { provideRoot() {}, inject(name, register) { register(); }, register(config, component) { slots.set(config.id ?? config.name, { config, component }); } } };
  const responses = {
    "/auth/status": { user: { id: "u1", username: "alice" } },
    "/projects": { projects },
    "/projects/p1/chats": { chats: [{ id: "c1", title: "One" }] },
    "/projects/p2/chats": { chats: [{ id: "c2", title: "Two" }] },
    "/projects/p1/files": { files: [{ name: "first.txt" }] },
    "/projects/p2/files": { files: [{ name: "second.txt" }] },
    "/chats/c1/native-history": { events: [], running: false },
    "/chats/c2/native-history": { events: [], running: false },
    "/chats/c1/subagents": { parentAvailable: true, entries: [{ kind: "child", id: "child1", mode: "continuable", activity: "inactive", label: "数据助手", hasChildren: false }] },
    "/chats/c2/subagents": { parentAvailable: true, entries: [] },
    "/chats/c1/subagents/child1/history": { events: [], running: false, parentAvailable: true, readOnly: true },
    "/chats/c1/plan": { team: { phase: "running", tasks: [
      { id: "t1", subject: "读取资料", status: "completed" },
      { id: "t2", subject: "分析", status: "in_progress" },
      { id: "t3", subject: "下载", status: "failed" },
      { id: "t4", subject: "报告", status: "cancelled" },
    ] } },
    "/chats/c2/plan": { team: { phase: "running", halted: true, tasks: [{ id: "t1", subject: "暂停分析", status: "in_progress" }] } },
    "/chats/c1/questions": { pending: null }, "/chats/c2/questions": { pending: null },
    "/chats/c1/files": { files: [{ name: "job/result.md" }] }, "/chats/c2/files": { files: [] },
  };
  const storage = new Map([["geosentinel:selection:u1", "c1"]]);
  const context = vm.createContext({
    window: { __ModuleLoader__: { load({ factory }) {
      const plugin = factory((id) => {
        if (id === "react") return { createElement: (type, props, ...children) => ({ type, props, children }) };
        if (id.includes("client-store")) return { createSnapshotStore: snapshot };
        if (id.includes("api-session-controller")) return { createScope: () => ({ ctx: {}, fiber: { dispose() {} } }), scopeOf() {}, MutableSessionEventSource: class { replace() {} } };
        return {};
      }); plugin.apply(ctx);
    } } },
    localStorage: { getItem: (key) => storage.get(key), setItem: (key, value) => storage.set(key, value), removeItem: (key) => storage.delete(key) },
    EventSource: class { close() {} }, setInterval: () => 1, clearInterval() {}, setTimeout, clearTimeout,
    fetch: async (url) => { const route = url.replace("/geo/api", ""); requests.push(route); assert.ok(responses[route], route); return { ok: true, json: async () => responses[route] }; },
  });
  vm.runInContext(source, context);
  for (let i = 0; i < 8; i++) await new Promise(setImmediate);
  assert.equal(services.sessions.list.getSnapshot().current, "c1");
  assert.ok(requests.includes("/chats/c1/native-history"));
  assert.equal(stores[0].getSnapshot().files[0].path, "first.txt");
  assert.equal(stores[0].getSnapshot().files[1].path, "job/result.md");
  const todos = services.sessions.binding("c1").session.projections.faceOf("todos").getSnapshot();
  assert.equal(todos[0].status, "completed");
  assert.equal(todos[1].status, "in_progress");
  assert.equal(todos[2].status, "pending");
  assert.match(todos[2].content, /\[失败\]/);
  assert.match(todos[3].content, /\[已取消\]/);
  const address = { parentSessionId: "c1", childSessionId: "child1", mode: "continuable" };
  await services.sessions.openSubagent(address);
  for (let i = 0; i < 8; i++) await new Promise(setImmediate);
  assert.equal(services.sessions.list.getSnapshot().current, "child1");
  assert.equal(services.sessions.list.getSnapshot().ids.includes("child1"), false);
  assert.equal(services.sessions.binding("child1").session.getSnapshot().subagent.address.parentSessionId, "c1");
  assert.equal(stores[0].getSnapshot().team, null);
  const child = services.sessions.binding("child1").session;
  assert.equal((await child.prompt([{ type: "text", text: "run" }])).ok, false);
  assert.equal((await child.cancel()).ok, false);
  assert.equal((await child.rename("changed")).ok, false);
  assert.throws(() => child.beginSubmission({ text: "run" }), /只读/);
  assert.equal(requests.some((route) => route.startsWith("/chats/child1")), false);
  assert.equal(stores[0].getSnapshot().files[1].path, "job/result.md");
  await services.sessions.refresh();
  assert.equal(services.sessions.list.getSnapshot().current, "child1");
  await assert.rejects(services.sessions.openSubagent({ ...address, parentSessionId: "c2" }), /不可访问/);
  const readonly = slots.get("geo-child-readonly");
  assert.equal(readonly.config.select({ session: { subagent: null } }), null);
  const matched = readonly.config.select({ session: child.getSnapshot() });
  const composer = readonly.component({ matched });
  await composer.children[1].props.onClick();
  assert.equal(services.sessions.list.getSnapshot().current, "c1");
  assert.equal(services.sessions.list.getSnapshot().currentAddress, undefined);
  await services.uiWorkspace.connectWorkspace("p2");
  for (let i = 0; i < 8; i++) await new Promise(setImmediate);
  assert.equal(services.sessions.list.getSnapshot().current, "c2");
  assert.equal(stores[0].getSnapshot().activeProject, "p2");
  assert.equal(stores[0].getSnapshot().files[0].path, "second.txt");
  assert.equal(stores[0].getSnapshot().files.length, 1);
  const secondTodos = services.sessions.binding("c2").session.projections.faceOf("todos").getSnapshot();
  assert.equal(secondTodos.length, 1);
  assert.equal(secondTodos[0].status, "pending");
  assert.match(secondTodos[0].content, /\[已停止\]/);
  responses["/chats/c2/plan"] = { team: null };
  services.sessions.open("c2");
  for (let i = 0; i < 8; i++) await new Promise(setImmediate);
  assert.equal(services.sessions.binding("c2").session.projections.faceOf("todos").getSnapshot().length, 0);
  await assert.rejects(services.sessions.fork());
  assert.match(stores[0].getSnapshot().error, /暂不支持/);
});
