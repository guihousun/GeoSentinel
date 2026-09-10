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
  // The layout face differs between lines: 0.1.2 exposes `closeDetails`, 0.1.5 exposes
  // `closeRightbar`/`selectPanel`. Every overlay call goes through one guarded helper.
  let layoutFace = { closeDetails() {} };
  const ctx = { provide: (key, value) => { services[key] = value; ctx[key] = value; },
    get: (key) => key === "layout" ? layoutFace : services[key],
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
  const posts = [];
  let uuid = 0;
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
    crypto: { randomUUID: () => `id-${++uuid}` },
    EventSource: class { close() {} }, setInterval: () => 1, clearInterval() {}, setTimeout, clearTimeout,
    fetch: async (url, init = {}) => {
      const route = url.replace("/geo/api", ""), method = init.method ?? "GET";
      requests.push(route);
      if (method === "POST") {
        posts.push(route);
        const created = { id: "c9", title: "新研究对话" };
        if (route === "/projects/p2/chats") {
          responses[route] = { chats: [...responses[route].chats, created] };
          responses["/chats/c9/native-history"] = { events: [], running: false };
          responses["/chats/c9/plan"] = { team: null };
          responses["/chats/c9/questions"] = { pending: null };
          responses["/chats/c9/subagents"] = { parentAvailable: true, entries: [] };
        }
        return { ok: true, json: async () => ({ chat: created }) };
      }
      assert.ok(responses[route], route);
      return { ok: true, json: async () => responses[route] };
    },
  });
  vm.runInContext(source, context);
  for (let i = 0; i < 8; i++) await new Promise(setImmediate);
  assert.equal(services.sessions.list.getSnapshot().current, "c1");
  assert.ok(requests.includes("/chats/c1/native-history"));
  // The retired 资料与产出 panel is gone: opening a chat no longer fetches the
  // project/file lists (uploads live in the session workspace and the upload
  // dock owns them).
  assert.equal(requests.some((route) => route.endsWith("/files")), false);
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
  // The native provider of `uiWorkspace` (`dsh-client-ui-workspace`) cannot activate
  // on the closed host plane: it waits for `workspaces` and `remote.directoryPicker`.
  // `ui-conversation` injects `connectWorkspace` and `ui-sidebar` injects
  // `startSession`, so the product implements the whole face against its own model.
  // Guard every method the native service exposes: a native upgrade that starts
  // calling another one must fail here instead of blanking the browser.
  for (const name of ["connectWorkspace", "openSession", "openWorkspace", "startSession", "forkSession", "archiveSession", "pickDirectory", "listDirectory", "createDirectory"])
    assert.equal(typeof services.uiWorkspace[name], "function", name);
  services.uiWorkspace.openSession("c1");
  assert.equal(services.sessions.list.getSnapshot().current, "c1");
  await services.uiWorkspace.openWorkspace("p2");
  for (let i = 0; i < 8; i++) await new Promise(setImmediate);
  assert.equal(services.sessions.list.getSnapshot().current, "c2");
  await services.uiWorkspace.startSession("p2");
  for (let i = 0; i < 8; i++) await new Promise(setImmediate);
  assert.deepEqual(posts, ["/projects/p2/chats"]);
  assert.equal(services.sessions.list.getSnapshot().current, "c9");
  assert.equal(stores[0].getSnapshot().activeProject, "p2");
  // No product equivalent on this plane: they must fail loudly, not pretend.
  for (const [name, args, pattern] of [["archiveSession", ["c9"], /归档/], ["pickDirectory", [], /目录/],
    ["listDirectory", ["D:\\"], /目录/], ["createDirectory", ["D:\\", "x"], /目录/]])
    await assert.rejects(services.uiWorkspace[name](...args), pattern, name);
  // 0.1.5 dropped `layout.closeDetails`: opening a session must use the new face
  // (`closeRightbar`) instead of throwing inside open(), which used to leave the
  // project dialog stuck and the chat unopened.
  let closed = 0;
  layoutFace = { selectPanel() {}, closeRightbar() { closed++; }, toggleSidebar() {} };
  services.sessions.open("c1");
  for (let i = 0; i < 4; i++) await new Promise(setImmediate);
  assert.equal(closed, 1);
  assert.equal(services.sessions.list.getSnapshot().current, "c1");
  // An unrecognised layout face must not break the same path either.
  layoutFace = {};
  services.sessions.open("c2");
  for (let i = 0; i < 4; i++) await new Promise(setImmediate);
  assert.equal(services.sessions.list.getSnapshot().current, "c2");
  // The native chat bubble renders `submission.attachments.map(...)`, so the product's
  // submission echo must carry that field (0.1.5) as well as the older `images`, and
  // the placement the native controller derives.
  const submission = services.sessions.binding("c2").session.beginSubmission({ mode: "send", text: "你好" });
  const echo = services.sessions.binding("c2").session.getSnapshot().pendingSubmissions[0];
  // (the vm realm has its own Array prototype, so assert by shape, not deep equality)
  assert.ok(Array.isArray(echo.attachments) && echo.attachments.length === 0);
  assert.ok(Array.isArray(echo.images) && echo.images.length === 0);
  assert.equal(echo.placement, "transcript");
  assert.equal(typeof submission.requestId, "string");
  assert.equal((await services.sessions.binding("c2").session.prompt([{ type: "text", text: "你好" }])).ok, true);
  assert.ok(requests.includes("/chats/c2/prompt"), "prompt 未发送到平台接口");
});

// From 0.1.5 the native sidebar family owns the single `sidebar` slot, so the overlay
// must contribute at the two positions that shell declares instead: `sidebar.workspaces`
// for the project/chat navigation (its native filler `ui-workspace` cannot activate —
// it waits for the closed host directory picker) and `sidebar.footer.action` for the
// product entries that the retired better-sidebar tabs used to carry. On the older line
// it must keep registering its own sidebar and neither native slot.
async function loadOverlay(nativeSidebar) {
  const source = await readFile(new URL("../plugins/workbench/native/client.js", import.meta.url), "utf8");
  const slots = new Map();
  const ctx = { provide: (key, value) => { ctx[key] = value; }, get: () => undefined, plugin: () => ({ dispose() {} }), inject() {}, on() {},
    slots: { provideRoot() {}, inject(name, register) { register(); }, register(config, component) { slots.set(config.name, { config, component }); } } };
  const snapshot = (initial) => { let state = initial; return { getSnapshot: () => state, subscribe: () => () => {}, set: (next) => { state = next; } }; };
  vm.runInNewContext(source, {
    __GEOSENTINEL_NATIVE_SIDEBAR__: nativeSidebar,
    window: { __ModuleLoader__: { load({ factory }) { factory((id) => {
      if (id === "react") return { createElement: () => null, Fragment: {},
        useSyncExternalStore: (_subscribe, getSnapshot) => getSnapshot(), useRef: () => ({}), useEffect() {}, useLayoutEffect() {} };
      if (id.includes("client-store")) return { createSnapshotStore: snapshot };
      if (id.includes("api-session-controller")) return { createScope: () => ({ ctx: {}, fiber: { dispose() {} } }), scopeOf() {}, MutableSessionEventSource: class { replace() {} } };
      return {};
    }).apply(ctx); } } },
    localStorage: { getItem: () => null, setItem() {}, removeItem() {} },
    EventSource: class { close() {} }, setInterval: () => 1, clearInterval() {}, setTimeout, clearTimeout,
    crypto: { randomUUID: () => "id" },
    fetch: async () => ({ ok: true, json: async () => ({ user: null }) }),
  });
  for (let i = 0; i < 4; i++) await new Promise(setImmediate);
  return slots;
}

test("the overlay fills the native sidebar's declared positions on 0.1.5 and keeps its own sidebar before it", async () => {
  const native = await loadOverlay(true);
  assert.ok(native.has("sidebar.workspaces"), "未注册原生侧栏的会话列表区域");
  assert.ok(native.has("sidebar.footer.action"), "未注册原生侧栏的工具入口区域");
  assert.equal(native.has("sidebar"), false, "原生侧栏存在时不应再抢占 sidebar 槽位");
  // The registered components must survive a render with no signed-in user instead of
  // throwing inside the native shell.
  const state = { user: null };
  for (const name of ["sidebar.workspaces", "sidebar.footer.action"]) {
    assert.equal(native.get(name).component({ wide: true }), null, name);
    assert.equal(native.get(name).component({ wide: false }), null, name);
  }
  assert.equal(state.user, null);

  const legacy = await loadOverlay(false);
  assert.ok(legacy.has("sidebar"), "旧版内核上必须继续注册产品自己的侧栏");
  assert.equal(legacy.has("sidebar.workspaces"), false);
  assert.equal(legacy.has("sidebar.footer.action"), false);
});
