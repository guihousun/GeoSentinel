import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import vm from "node:vm";

// The native file panel and the document preview subscribe to the workspace through
// `remote.workspaceFiles.changes(sessionId, signal)`, whose contract is an async iterable of
// frames:
//   {kind:"ready"}                                   once, acknowledged by the consumer
//   {kind:"change", change:{absolutePath, version}}  a file appeared or changed
//   {kind:"change", change:{absolutePath, absent:true}}  a file is gone
// The wire protocol wraps each item as `{value, accept}`; the product answers the namespace
// itself, so it has to wrap them too. This test drives the real provided namespace with a fake
// listing, the way the native consumer does — it does not reach into the overlay's internals.
const source = await readFile(new URL("../plugins/workbench/native/client.js", import.meta.url), "utf8");

// The overlay skips the administrator's read-only library when it walks the view; that label
// lives in the host module, so the two must not drift apart.
test("the watch skips the same read-only group the host labels", async () => {
  const share = await readFile(new URL("../plugins/platform/share.mjs", import.meta.url), "utf8");
  const label = /SHARE_LABEL\s*=\s*"([^"]+)"/.exec(share)?.[1];
  assert.ok(label, "share.mjs 里找不到 SHARE_LABEL");
  assert.ok(source.includes(`const WATCH_SHARE_LABEL = "${label}"`),
    `native/client.js 的 WATCH_SHARE_LABEL 必须等于 SHARE_LABEL（${label}）`);
});

test("the workspace change feed reports ready, then additions, edits and removals", async (t) => {
  const services = {};
  const requests = [];
  const listings = [];
  const streams = [];
  const root = "/工作区/验收";
  const share = "/工作区/验收/共享数据（只读）";
  // One mutable listing per directory, so a change is a change of the served tree.
  const tree = new Map([
    [root, [dir(root, "上传的文件"), dir(root, "分析结果"), dir(root, "共享数据（只读）")]],
    [`${root}/上传的文件`, [file(root, "上传的文件", "kept.md", 10), file(root, "上传的文件", "edited.md", 20)]],
    [`${root}/分析结果`, []],
    [share, [file(share.slice(0, share.lastIndexOf("/")), "共享数据（只读）", "library.tif", 999999)]],
  ]);
  function dir(base, name) { return { name, path: `${base}/${name}`, isDir: true }; }
  function file(base, group, name, size) { return { name, path: `${base}/${group}/${name}`, isDir: false, size }; }

  const ctx = {
    provide: (key, value) => { services[key] = value; ctx[key] = value; },
    get: (key) => services[key],
    plugin: () => ({ dispose() {} }), inject() {}, on() {},
    slots: { provideRoot() {}, inject(name, register) { register(); }, register() {} },
  };
  const context = vm.createContext({
    window: { __ModuleLoader__: { load({ factory }) {
      const plugin = factory((id) => {
        if (id === "react") return { createElement: (type, props, ...children) => ({ type, props, children }) };
        if (id.includes("client-store")) return { createSnapshotStore: () => ({ getSnapshot: () => ({}), subscribe: () => () => {}, set() {} }) };
        if (id.includes("api-session-controller")) return { createScope: () => ({ ctx: {}, fiber: { dispose() {} } }), scopeOf() {}, MutableSessionEventSource: class { replace() {} } };
        return {};
      });
      plugin.apply(ctx);
    } } },
    localStorage: { getItem: () => null, setItem() {}, removeItem() {} },
    crypto: { randomUUID: () => "id-watch" },
    EventSource: class { constructor(url) { this.url = url; streams.push(this); } close() { this.closed = true; } },
    setInterval: () => 1, clearInterval() {},
    setTimeout, clearTimeout, AbortController,
    fetch: async (url, init = {}) => {
      const route = String(url).replace("/geo/api", "");
      requests.push(route);
      if (route === "/sidebar/api/session.cwd") return { ok: true, json: async () => ({ ok: true, value: { root } }) };
      if (route === "/sidebar/api/fs.tree") {
        const { path } = JSON.parse(init.body);
        listings.push(path);
        return { ok: true, json: async () => ({ ok: true, value: { entries: tree.get(path) ?? [] } }) };
      }
      return { ok: false, status: 404, json: async () => ({ ok: false, error: { message: route } }) };
    },
  });
  vm.runInContext(source, context);
  for (let index = 0; index < 8; index += 1) await new Promise(setImmediate);

  const face = services["remote.workspaceFiles"];
  assert.equal(typeof face.changes, "function", "叠加层必须提供 remote.workspaceFiles.changes");
  const controller = new AbortController();
  // A feed left running would keep a pending timer and hold the test process open, so it is
  // always cancelled — including when an assertion below fails.
  t.after(() => controller.abort());
  const frames = face.changes("s1", controller.signal)[Symbol.asyncIterator]();

  const ready = await frames.next();
  // The consumer reads `item.value` (the frame); frame objects come from the vm realm, so
  // compare fields rather than prototypes.
  assert.equal(ready.value.value.kind, "ready", "第一帧必须是 ready");
  assert.equal(typeof ready.value.accept, "function", "帧要带 accept（消费者在 ready 上调用它）");
  assert.equal(ready.done, false);

  // The baseline listing is taken right after readiness; changing the tree before it lands
  // would fold the change into that baseline and test nothing.
  for (let attempt = 0; attempt < 200 && listings.length === 0; attempt += 1) await new Promise(setImmediate);
  assert.ok(listings.length > 0, "ready 之后应立即取基线");

  // The agent writes inside the chat: one file appears, one grows, one disappears.
  tree.set(`${root}/上传的文件`, [file(root, "上传的文件", "edited.md", 55), file(root, "上传的文件", "fresh.md", 7)]);
  streams[0].onmessage?.();
  await new Promise((resolve) => setTimeout(resolve, 900));

  const change = await frames.next();
  const batch = [change.value.value];
  while (true) {
    const next = await Promise.race([frames.next(), new Promise((resolve) => setTimeout(() => resolve(undefined), 150))]);
    if (next === undefined) break;
    if (next.done) break;
    batch.push(next.value.value);
  }
  const byPath = new Map(batch.map((frame) => [frame.change.absolutePath, frame.change]));
  assert.deepEqual([...byPath.keys()].sort(), [
    `${root}/上传的文件/edited.md`,
    `${root}/上传的文件/fresh.md`,
    `${root}/上传的文件/kept.md`,
  ].sort(), "应当报告新增、改动与删除");
  assert.equal(byPath.get(`${root}/上传的文件/fresh.md`).version, "7");
  assert.equal(byPath.get(`${root}/上传的文件/edited.md`).version, "55");
  assert.equal(byPath.get(`${root}/上传的文件/kept.md`).absent, true);
  assert.ok(batch.every((frame) => frame.kind === "change"), "除 ready 外只能是 change 帧");

  // Cancelling the consumer's signal ends the feed and closes the stream it opened.
  controller.abort();
  const finished = await Promise.race([frames.next(), new Promise((resolve) => setTimeout(() => resolve("timeout"), 1500))]);
  assert.notEqual(finished, "timeout", "取消后订阅必须结束");
  assert.equal(finished.done, true);
  assert.equal(streams[0].closed, true, "结束时要关掉自己开的事件流");

  // The read-only administrator library cannot change during a session, so walking it would
  // only cost listings.
  assert.ok(listings.length >= 2, "至少走过根与一个可写分组");
  assert.equal(listings.includes(share), false, "只读的共享数据分组不应被遍历");
  assert.ok(listings.includes(`${root}/上传的文件`), "可写分组要被遍历");
});

// A listing that keeps failing (an expired preview link answers 403, a closed session 404)
// must not be retried every beat forever: measured on a preview instance whose login link had
// expired, the feed produced a console error and a wasted request every 15 seconds, with no way
// to stop. It now backs off and gives up.
test("a feed whose listings keep failing backs off and gives up", async () => {
  const services = {};
  let attempts = 0;
  const ctx = {
    provide: (key, value) => { services[key] = value; ctx[key] = value; },
    get: (key) => services[key],
    plugin: () => ({ dispose() {} }), inject() {}, on() {},
    slots: { provideRoot() {}, inject(name, register) { register(); }, register() {} },
  };
  const context = vm.createContext({
    window: { __ModuleLoader__: { load({ factory }) {
      const plugin = factory((id) => {
        if (id === "react") return { createElement: (type, props, ...children) => ({ type, props, children }) };
        if (id.includes("client-store")) return { createSnapshotStore: () => ({ getSnapshot: () => ({}), subscribe: () => () => {}, set() {} }) };
        if (id.includes("api-session-controller")) return { createScope: () => ({ ctx: {}, fiber: { dispose() {} } }), scopeOf() {}, MutableSessionEventSource: class { replace() {} } };
        return {};
      });
      plugin.apply(ctx);
    } } },
    localStorage: { getItem: () => null, setItem() {}, removeItem() {} },
    crypto: { randomUUID: () => "id-fail" },
    EventSource: class { constructor(url) { this.url = url; } close() {} },
    setInterval: () => 1, clearInterval() {},
    // Compress the feed's beats so a minute of backoff runs in milliseconds here.
    setTimeout: (callback, ms) => setTimeout(callback, Math.min(ms ?? 0, 20)), clearTimeout: (timer) => clearTimeout(timer),
    AbortController,
    fetch: async (url, init = {}) => {
      const route = String(url).replace("/geo/api", "");
      if (route === "/sidebar/api/session.cwd") return { ok: true, json: async () => ({ ok: true, value: { root: "/工作区/失效" } }) };
      if (route === "/sidebar/api/fs.tree") { attempts += 1; return { ok: false, status: 403, json: async () => ({ ok: false, error: { message: "forbidden" } }) }; }
      return { ok: false, status: 404, json: async () => ({ ok: false, error: { message: route } }) };
    },
  });
  vm.runInContext(source, context);
  for (let index = 0; index < 8; index += 1) await new Promise(setImmediate);

  const controller = new AbortController();
  const feed = services["remote"].workspaceFiles.changes("s1", controller.signal);
  assert.equal((await feed.next()).value.value.kind, "ready");
  const finished = await Promise.race([feed.next(), new Promise((resolve) => setTimeout(() => resolve("timeout"), 3000))]);
  assert.notEqual(finished, "timeout", "连续失败后应当自己结束，而不是无限重试");
  assert.equal(finished.done, true);
  assert.ok(attempts >= 2, `至少要重试一次（实际 ${attempts}）`);
  assert.ok(attempts <= 8, `重试次数必须有上限（实际 ${attempts}）`);
  controller.abort();
});

// The session's virtual root is `/工作区/<会话标题>`, and the product re-titles a chat from its
// first message — so the root changes mid-session. Measured on a preview instance: the feed's
// cached root went stale the moment the first turn renamed the chat and every later listing was
// answered 403 "文件路径超出工作区", three console errors per beat. The feed re-reads the root
// every beat and the panel's calls retry once with a fresh one.
test("a re-titled chat does not stale the feed or the panel", async () => {
  const services = {};
  const listings = [];
  const streams = [];
  let root = "/工作区/变更流";
  const tree = new Map();
  const ctx = {
    provide: (key, value) => { services[key] = value; ctx[key] = value; },
    get: (key) => services[key],
    plugin: () => ({ dispose() {} }), inject() {}, on() {},
    slots: { provideRoot() {}, inject(name, register) { register(); }, register() {} },
  };
  const context = vm.createContext({
    window: { __ModuleLoader__: { load({ factory }) {
      const plugin = factory((id) => {
        if (id === "react") return { createElement: (type, props, ...children) => ({ type, props, children }) };
        if (id.includes("client-store")) return { createSnapshotStore: () => ({ getSnapshot: () => ({}), subscribe: () => () => {}, set() {} }) };
        if (id.includes("api-session-controller")) return { createScope: () => ({ ctx: {}, fiber: { dispose() {} } }), scopeOf() {}, MutableSessionEventSource: class { replace() {} } };
        return {};
      });
      plugin.apply(ctx);
    } } },
    localStorage: { getItem: () => null, setItem() {}, removeItem() {} },
    crypto: { randomUUID: () => "id-retitle" },
    EventSource: class { constructor(url) { this.url = url; streams.push(this); } close() {} },
    setInterval: () => 1, clearInterval() {},
    setTimeout: (callback, ms) => setTimeout(callback, Math.min(ms ?? 0, 20)), clearTimeout: (timer) => clearTimeout(timer),
    AbortController,
    fetch: async (url, init = {}) => {
      const route = String(url).replace("/geo/api", "");
      if (route === "/sidebar/api/session.cwd") return { ok: true, json: async () => ({ ok: true, value: { root } }) };
      if (route === "/sidebar/api/fs.tree") {
        const { path } = JSON.parse(init.body);
        // The host refuses any path outside the session's CURRENT root — 403, like the product.
        if (path !== root && !path.startsWith(root + "/"))
          return { ok: false, status: 403, json: async () => ({ ok: false, error: { message: "文件路径超出工作区" } }) };
        listings.push(path);
        return { ok: true, json: async () => ({ ok: true, value: { entries: tree.get(path) ?? [] } }) };
      }
      return { ok: false, status: 404, json: async () => ({ ok: false, error: { message: route } }) };
    },
  });
  vm.runInContext(source, context);
  for (let index = 0; index < 8; index += 1) await new Promise(setImmediate);

  tree.set(root, [{ name: "报告.md", path: `${root}/报告.md`, isDir: false, size: 5 }]);
  const controller = new AbortController();
  const feed = services["remote"].workspaceFiles.changes("s1", controller.signal);
  assert.equal((await feed.next()).value.value.kind, "ready");
  for (let attempt = 0; attempt < 200 && listings.length === 0; attempt += 1) await new Promise(setImmediate);

  // The first turn renames the chat: new root, and the file moves with it.
  root = "/工作区/请在工作区写入一个文件";
  tree.set(root, [{ name: "报告.md", path: `${root}/报告.md`, isDir: false, size: 5 },
    { name: "新文件.md", path: `${root}/新文件.md`, isDir: false, size: 9 }]);
  streams[0].onmessage?.();

  const frame = await Promise.race([feed.next(), new Promise((resolve) => setTimeout(() => resolve("timeout"), 2000))]);
  assert.notEqual(frame, "timeout", "改标题后变更流必须仍然给出帧");
  const changes = [frame.value.value];
  while (true) {
    const next = await Promise.race([feed.next(), new Promise((resolve) => setTimeout(() => resolve(undefined), 30))]);
    if (next === undefined || next.done) break;
    changes.push(next.value.value);
  }
  const added = changes.find((change) => change.change.absolutePath === `${root}/新文件.md`);
  assert.ok(added, `变更帧要用新根下的路径（实际 ${JSON.stringify(changes.map((change) => change.change.absolutePath))}）`);

  // The panel's own calls recover too: `list` retries with a re-read root.
  const listed = await services["remote"].workspaceFiles.list("s1", "/");
  assert.equal(listed.ok, true, `改标题后 list 必须自愈（实际 ${JSON.stringify(listed)}）`);
  assert.ok(listings.some((path) => path === root), "list 必须用新根重新列目录");
  controller.abort();
});

// The native resource provider never calls `workspaceFiles.changes` directly: it wraps it in the
// transport's streaming primitive, `remote.$stream({name, open, ended})`, and iterates the handle
// (`for await (const item of this.stream)` → `item.value`, `item.accept()`); when its last
// follower leaves it calls `dispose()`, and the session's next feed awaits that result before
// opening. Without `$stream` the feed was unreachable — an open preview with no change traffic.
test("the transport streaming primitive exposes the feed the native provider iterates", async (t) => {
  const services = {};
  const streams = [];
  const root = "/工作区/流";
  const tree = new Map([[root, [{ name: "报告.md", path: `${root}/报告.md`, isDir: false, size: 12 }]]]);
  const ctx = {
    provide: (key, value) => { services[key] = value; ctx[key] = value; },
    get: (key) => services[key],
    plugin: () => ({ dispose() {} }), inject() {}, on() {},
    slots: { provideRoot() {}, inject(name, register) { register(); }, register() {} },
  };
  const context = vm.createContext({
    window: { __ModuleLoader__: { load({ factory }) {
      const plugin = factory((id) => {
        if (id === "react") return { createElement: (type, props, ...children) => ({ type, props, children }) };
        if (id.includes("client-store")) return { createSnapshotStore: () => ({ getSnapshot: () => ({}), subscribe: () => () => {}, set() {} }) };
        if (id.includes("api-session-controller")) return { createScope: () => ({ ctx: {}, fiber: { dispose() {} } }), scopeOf() {}, MutableSessionEventSource: class { replace() {} } };
        return {};
      });
      plugin.apply(ctx);
    } } },
    localStorage: { getItem: () => null, setItem() {}, removeItem() {} },
    crypto: { randomUUID: () => "id-stream" },
    EventSource: class { constructor(url) { this.url = url; streams.push(this); } close() { this.closed = true; } },
    setInterval: () => 1, clearInterval() {},
    setTimeout, clearTimeout, AbortController,
    fetch: async (url, init = {}) => {
      const route = String(url).replace("/geo/api", "");
      if (route === "/sidebar/api/session.cwd") return { ok: true, json: async () => ({ ok: true, value: { root } }) };
      if (route === "/sidebar/api/fs.tree") {
        const { path } = JSON.parse(init.body);
        return { ok: true, json: async () => ({ ok: true, value: { entries: tree.get(path) ?? [] } }) };
      }
      return { ok: false, status: 404, json: async () => ({ ok: false, error: { message: route } }) };
    },
  });
  vm.runInContext(source, context);
  for (let index = 0; index < 8; index += 1) await new Promise(setImmediate);

  const remote = services["remote"];
  assert.equal(typeof remote.$stream, "function", "叠加层必须提供 remote.$stream");
  // Exactly the shape the native provider builds.
  const stream = remote.$stream({
    name: "workspace file changes of s1",
    open: (signal) => remote.workspaceFiles.changes("s1", signal),
    ended: () => new Error("ended"),
  });
  assert.equal(typeof stream[Symbol.asyncIterator], "function");
  assert.equal(typeof stream.dispose, "function");
  t.after(() => stream.dispose());

  const ready = await stream.next();
  assert.equal(ready.value.value.kind, "ready");
  assert.equal(typeof ready.value.accept, "function");
  assert.equal(streams.length > 0 && streams[0].closed !== true, true, "订阅打开时事件流是活的");

  await stream.dispose();
  const ended = await stream.next();
  assert.equal(ended.done, true, "dispose 之后不再产出帧");
  assert.equal(streams[0].closed, true, "dispose 要关掉底层订阅");
});
