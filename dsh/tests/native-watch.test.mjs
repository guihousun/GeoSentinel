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
    setTimeout, clearTimeout,
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
