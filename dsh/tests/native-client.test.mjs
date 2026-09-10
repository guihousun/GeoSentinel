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
      if (id === "react") return { createElement: (type, props, ...children) => ({ type, props, children }), Fragment: {},
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

// The native right sidebar's file tabs read the session workspace through the Host
// Remote namespace `remote.workspaceFiles`; the product answers it from its own
// explorer surface. These are the four methods those tabs call, exercised against a
// stubbed explorer so the mapping (virtual paths, line pages, base64, refusals) is
// fixed by a test instead of by a browser session.
async function loadOverlayForFiles({ files, tree }) {
  const source = await readFile(new URL("../plugins/workbench/native/client.js", import.meta.url), "utf8");
  const services = {};
  const requests = [];
  const ctx = { provide: (key, value) => { services[key] = value; ctx[key] = value; }, get: () => undefined,
    plugin: () => ({ dispose() {} }), inject() {}, on() {}, slots: { provideRoot() {}, inject() {}, register() {} } };
  const snapshot = (initial) => { let state = initial; return { getSnapshot: () => state, subscribe: () => () => {}, set: (next) => { state = next; } }; };
  vm.runInNewContext(source, {
    window: { __ModuleLoader__: { load({ factory }) { factory((id) => {
      if (id === "react") return { createElement: () => null, Fragment: {} };
      if (id.includes("client-store")) return { createSnapshotStore: snapshot };
      if (id.includes("api-session-controller")) return { createScope: () => ({ ctx: {}, fiber: { dispose() {} } }), scopeOf() {}, MutableSessionEventSource: class { replace() {} } };
      return {};
    }).apply(ctx); } } },
    localStorage: { getItem: () => null, setItem() {}, removeItem() {} },
    EventSource: class { close() {} }, setInterval: () => 1, clearInterval() {}, setTimeout, clearTimeout,
    crypto: { randomUUID: () => "id" }, TextDecoder, TextEncoder, URLSearchParams, btoa,
    fetch: async (url, init = {}) => {
      requests.push(`${init.method ?? "GET"} ${url}`);
      if (url.startsWith("/geo/api/auth/status")) return { ok: true, json: async () => ({ user: null }) };
      if (url === "/sidebar/api/session.cwd") return { ok: true, json: async () => ({ ok: true, value: { sessionId: "c1", cwd: "/工作区/甲", root: "/工作区/甲", parent: null } }) };
      if (url === "/sidebar/api/fs.tree") {
        const body = JSON.parse(init.body);
        const entries = tree[body.path] ?? [];
        return { ok: true, json: async () => ({ ok: true, value: { entries } }) };
      }
      if (url.startsWith("/sidebar/file?")) {
        const parsed = new URLSearchParams(url.slice("/sidebar/file?".length));
        const path = parsed.get("path");
        if (!(path in files)) return { ok: false, status: 404, json: async () => ({ ok: false, error: { message: "文件不存在" } }) };
        return { ok: true, arrayBuffer: async () => new TextEncoder().encode(files[path]).buffer, status: 200 };
      }
      throw new Error(`unexpected request ${url}`);
    },
  });
  for (let i = 0; i < 4; i++) await new Promise(setImmediate);
  return { services, requests };
}

test("the product answers the native file namespace from its own explorer", async () => {
  const root = "/工作区/甲";
  const state = await loadOverlayForFiles({
    tree: { [root]: [{ name: "上传的文件", path: `${root}/上传的文件`, isDir: true },
      { name: "分析结果", path: `${root}/分析结果`, isDir: true }],
      [`${root}/上传的文件`]: [{ name: "报告.md", path: `${root}/上传的文件/报告.md`, isDir: false, size: 12 }] },
    files: { [`${root}/上传的文件/报告.md`]: "第一行\n第二行\n第三行" },
  });
  const face = state.services["remote.workspaceFiles"];
  assert.equal(typeof face.list, "function");
  for (const name of ["list", "stat", "read", "readAll", "readRelated", "changes"])
    assert.equal(typeof face[name], "function", name);

  // The root listing arrives through the session's own virtual root, and entries are
  // translated to the native lstat shape ("directory"/"file").
  const listing = await face.list("c1", "");
  assert.equal(listing.ok, true);
  assert.equal(listing.value.path, root);
  assert.equal(listing.value.entries[0].type, "directory");
  assert.equal(listing.value.truncated, false);

  // The native tree descends with RELATIVE paths (`parent + "/" + name`), so a relative
  // address must resolve against the same virtual root instead of the host filesystem.
  const child = await face.list("c1", "上传的文件");
  assert.equal(child.ok, true);
  assert.equal(child.value.entries[0].name, "报告.md");
  assert.equal(child.value.entries[0].type, "file");
  assert.equal(child.value.entries[0].size, 12);
  assert.ok(state.requests.includes(`POST /sidebar/api/fs.tree`));
  assert.equal(JSON.stringify(state.requests).includes("/geo/api/chats"), false, "文件读取不应绕过 explorer 接口");

  // Line pages: 1-based offset, `lines` counts the page, `eof` says whether it reached
  // the last line, and the address stays the virtual path.
  const firstPage = await face.read("c1", "上传的文件/报告.md", { offset: 1, limit: 2 });
  assert.equal(firstPage.ok, true);
  assert.equal(firstPage.value.text, "第一行\n第二行");
  assert.equal(firstPage.value.lines, 2);
  assert.equal(firstPage.value.eof, false);
  assert.equal(firstPage.value.absolutePath, `${root}/上传的文件/报告.md`);
  const lastPage = await face.read("c1", "上传的文件/报告.md", { offset: 3, limit: 2 });
  assert.equal(lastPage.value.text, "第三行");
  assert.equal(lastPage.value.eof, true);

  // Complete read: base64 of the raw bytes, with the stat metadata the tabs expect.
  const complete = await face.readAll("c1", "上传的文件/报告.md");
  assert.equal(complete.ok, true);
  assert.equal(complete.value.offset, 0);
  assert.equal(complete.value.eof, true);
  const decoded = new TextDecoder().decode(Uint8Array.from(atob(complete.value.data), (character) => character.charCodeAt(0)));
  assert.equal(decoded, "第一行\n第二行\n第三行");

  // Related reads join a sibling path; anything that could leave the view is refused
  // with a message instead of reaching the host.
  const related = await face.readRelated("c1", "上传的文件/报告.md", "./报告.md");
  assert.equal(related.ok, true);
  const escaped = await face.readRelated("c1", "上传的文件/报告.md", "../../etc/passwd");
  assert.equal(escaped.ok, false);
  assert.match(escaped.error.message, /路径无效/);
  const absolute = await face.readRelated("c1", "上传的文件/报告.md", "/etc/passwd");
  assert.equal(absolute.ok, false);
  const traversal = await face.list("c1", "../../etc");
  assert.equal(traversal.ok, false);
  assert.match(traversal.error.message, /路径无效/);

  // A missing file surfaces as a failed envelope (the tree renders the message), never
  // as an exception that would take the whole client down.
  const missing = await face.read("c1", "上传的文件/缺失.md");
  assert.equal(missing.ok, false);
  assert.match(missing.error.message, /文件不存在/);
  // No live change stream exists for an ordinary user: say so instead of pretending.
  const changes = await face.changes("c1");
  assert.equal(changes.ok, false);
  assert.match(changes.error.message, /不可用/);
});

test("the overlay stays inert in a host that has no product shell", async () => {
  // The administrator development instance mounts this package as its own client entry,
  // but only the product shell injects the `@geosentinel/dsh-theme` module. A failed
  // require there used to take down the whole loader entry ("failed to import loader
  // entry … require(\"@geosentinel/dsh-theme\") missed the module table"), blanking that
  // GUI; the overlay must instead recognise a foreign host and do nothing.
  const source = await readFile(new URL("../plugins/workbench/native/client.js", import.meta.url), "utf8");
  const slots = new Map();
  let plugin = null;
  const ctx = { provide() {}, get: () => undefined, plugin: () => ({ dispose() {} }), inject() {}, on() {},
    slots: { provideRoot() {}, inject(name, register) { register(); }, register(config) { slots.set(config.name, config); } } };
  vm.runInNewContext(source, {
    window: { __ModuleLoader__: { load(definition) {
      plugin = definition.factory((id) => {
        if (id === "@geosentinel/dsh-theme") throw new Error("client-modules: missed the module table");
        return {};
      });
      plugin.apply(ctx);
    } } },
    localStorage: { getItem: () => null, setItem() {}, removeItem() {} },
    EventSource: class { close() {} }, setInterval: () => 1, clearInterval() {}, setTimeout, clearTimeout,
  });
  assert.deepEqual(Object.keys(plugin), ["apply"], "外来宿主里应返回惰性插件");
  assert.equal(slots.size, 0, "外来宿主里不应注册任何槽位");
  plugin.apply(ctx);
  assert.equal(slots.size, 0, "惰性插件的 apply 必须是空操作");
});

test("the overlay fills the native sidebar's declared positions on 0.1.5 and keeps its own sidebar before it", async () => {
  const native = await loadOverlay(true);
  assert.ok(native.has("sidebar.workspaces"), "未注册原生侧栏的会话列表区域");
  assert.ok(native.has("sidebar.footer.action"), "未注册原生侧栏的工具入口区域");
  assert.ok(native.has("sidebar.brand.name"), "未接回产品名，原生侧栏会显示厂商构建标签");
  assert.ok(native.has("sidebar.brand.mark"), "未接回产品标识");
  assert.equal(native.has("sidebar"), false, "原生侧栏存在时不应再抢占 sidebar 槽位");
  // The brand components render the product identity and honour the size the native
  // sidebar hands the mark.
  const mark = native.get("sidebar.brand.mark").component({ size: 18 });
  assert.equal(mark.type, "svg");
  assert.equal(mark.props.width, 18);
  const name = native.get("sidebar.brand.name").component({});
  assert.equal(name.children[0], "地缘环境智能计算平台");
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
  assert.equal(legacy.has("sidebar.brand.name"), false, "旧版内核的品牌由产品侧栏自己渲染");
});
