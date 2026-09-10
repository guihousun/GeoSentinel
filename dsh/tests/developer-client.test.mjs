import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import vm from "node:vm";

const code = await readFile(new URL("../plugins/developer/client.js", import.meta.url), "utf8");
async function setup(current, options = {}) {
  const source = "D:\\GeoSentinel-DSH", calls = [], disposers = [], messages = [];
  const snapshot = { ids: current ? [current.id] : [], current: current?.id, byId: current ? { [current.id]: current } : {} };
  let plugin;
  const parent = { postMessage: (message) => messages.push(message) };
  const originalList = async (path) => { calls.push(["list", path]); return path; };
  const inner = {
    sessions: { list: { getSnapshot: () => snapshot, subscribe: () => () => {} }, refresh: async () => {}, open: (id) => calls.push(["open", id]) },
    remote: { workspace: { create: async (args) => { calls.push(["workspace", args.path]); return { ok: true, value: { workspace: { workspaceId: "source" } } }; } },
      session: { create: async (args) => { calls.push(["create", args.workspaceId, args.agentPreset]); return { ok: true, value: { sessionId: "new" } }; } } },
    uiWorkspace: { listDirectory: originalList }, get: () => undefined,
    effect: (effect) => disposers.push(effect()),
  };
  const root = options.root ?? null;
  vm.runInNewContext(code, {
    window: { parent, __GEOSENTINEL_SOURCE__: source, __ModuleLoader__: { load: (definition) => { plugin = definition.factory(() => ({})); } }, addEventListener() {}, removeEventListener() {} },
    location: { origin: "http://localhost" },
    document: { body: {}, querySelector: () => root },
    getComputedStyle: () => ({ gridTemplateColumns: options.tracks ?? "0px" }),
    MutationObserver: class { observe() {} disconnect() {} },
    ResizeObserver: class { observe() {} disconnect() {} },
    requestAnimationFrame: (callback) => callback(),
    setInterval: () => 1, clearInterval() {},
  });
  plugin.apply({ inject: (_names, callback) => callback(inner), slots: { inject() {} } });
  for (let i = 0; i < 20 && !messages.some((message) => message.type === "geo:development-ready"); i++) await new Promise(setImmediate);
  return { calls, inner, disposers, originalList, source, messages };
}

test("native creator defaults to product workspace instead of a recent unrelated blank", async () => {
  const state = await setup({ id: "old", blank: true, cwd: "D:\\other" });
  assert.deepEqual(state.calls.slice(0, 3), [["workspace", state.source], ["create", "source", "cordis"], ["open", "new"]]);
  assert.equal(await state.inner.uiWorkspace.listDirectory(), state.source);
  assert.equal(await state.inner.uiWorkspace.listDirectory("D:\\other"), "D:\\other");
  state.disposers.forEach((dispose) => dispose?.());
  assert.equal(state.inner.uiWorkspace.listDirectory, state.originalList);
});

test("opening an existing nonblank task never reassigns its workspace", async () => {
  const state = await setup({ id: "existing", blank: false, cwd: "D:\\other" });
  assert.deepEqual(state.calls, []);
  state.disposers.forEach((dispose) => dispose?.());
});

test("embedded layout tracks the native grid by observation instead of polling", async () => {
  const written = {};
  const root = { isConnected: true, hasAttribute: () => false, querySelector: () => null,
    style: { setProperty: (name, value) => { written[name] = value; } } };
  const state = await setup(null, { root, tracks: "0px 800px 360px" });
  assert.equal(written["--geo-embedded-details"], "360px");
  assert.ok(state.messages.some((message) => message.type === "geo:development-ready"));
  state.disposers.forEach((dispose) => dispose?.());
});
