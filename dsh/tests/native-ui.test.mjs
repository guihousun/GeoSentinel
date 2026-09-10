import test from "node:test";
import assert from "node:assert/strict";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { createServer } from "node:http";
import { PlatformStore } from "../plugins/platform/store.mjs";
import { nativeEvent } from "../plugins/platform/native-events.mjs";
import { sidebarPolicy, createSidebarHandler, createSidebarFileHandler, createSidebarBundleHandler } from "../plugins/platform/sidebar-adapter.mjs";
import { dreamSkinTheme } from "../plugins/workbench/skin-theme.mjs";

test("managed Dream Skin uses real midnight tokens with accessible contrast", async (t) => {
  // The theme folds in the administrator's stored appearance, so the assertion is
  // pinned to an empty home instead of whatever this machine happens to have
  // deployed (running with GEO_DSH_HOME pointing at a live home used to fail here).
  const previousHome = process.env.GEO_DSH_HOME;
  const root = await mkdtemp(path.join(tmpdir(), "geo-theme-"));
  process.env.GEO_DSH_HOME = root;
  t.after(async () => {
    if (previousHome === undefined) delete process.env.GEO_DSH_HOME; else process.env.GEO_DSH_HOME = previousHome;
    await rm(root, { recursive: true, force: true });
  });
  const theme = await dreamSkinTheme();
  assert.equal(theme.id, "geosentinel-midnight");
  assert.equal(theme.colorScheme, "dark");
  assert.equal(theme.tokens["--dsw-alias-bg-base"], "#0b0b0e");
  const luminance = (hex) => hex.slice(1).match(/../g).map((value) => {
    const channel = parseInt(value, 16) / 255;
    return channel <= .04045 ? channel / 12.92 : ((channel + .055) / 1.055) ** 2.4;
  }).reduce((sum, channel, i) => sum + channel * [.2126, .7152, .0722][i], 0);
  for (const background of ["--dsw-alias-bg-base", "--dsw-alias-bg-layer-1"])
    for (const foreground of ["--dsw-alias-label-primary", "--dsw-alias-label-secondary", "--dsw-alias-label-tertiary", "--dsw-alias-brand-primary"])
      assert.ok((luminance(theme.tokens[foreground]) + .05) / (luminance(theme.tokens[background]) + .05) >= 4.5, foreground);
});

test("native events preserve renderer identity while excluding model request secrets", () => {
  assert.equal(nativeEvent({ type: "request/header", data: { token: "secret" } }), null);
  const event = nativeEvent({ type: "assistant/message", seq: 2, time: 123,
    data: { turn: 1, step: 1, message: { id: "reply", role: "assistant", source: { kind: "model", provider: "deepseek", model: "test", apiKey: "secret" }, content: [{ type: "text", text: "回复" }] } } });
  assert.equal(event.data.message.source.provider, "deepseek");
  assert.equal(event.surfaceOp, "append");
  // 0.1.5's token meter calls `streamUsage(event.data.stream)` and throws on an
  // undefined `stream`, which killed the whole event feed in the browser. The product
  // forwards no raw records, so the honest value is an empty array — never a count.
  assert.deepEqual(event.data.stream, []);
  assert.equal("usage" in event.data, false);
  assert.equal(JSON.stringify(event).includes("secret"), false);
  assert.equal(nativeEvent({ type: "user/message", data: { source: { kind: "plugin" }, content: [] } }), null);
});

test("native tool history keeps call identity and reports business failures without exposing payloads", () => {
  const call = nativeEvent({ type: "assistant/message", data: { message: { content: [{ type: "tool-call", id: "c1", name: "geo_list_files", arguments: "secret" }] } } });
  assert.equal(call.data.message.content[0].id, "c1");
  assert.equal(call.data.message.content[0].arguments, "{}");
  const result = nativeEvent({ type: "tool/result", data: { message: { source: { callId: "c1" }, content: [{ type: "tool-result", isError: true, content: [{ type: "text", text: "secret" }] }] } } });
  assert.equal(result.data.message.content[0].isError, true);
  assert.ok(!JSON.stringify(result).includes("secret"));
});

test("research shell follows the administrator development appearance and falls back safely", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "geo-appearance-"));
  const admin = path.join(root, "development", "admin-1");
  await mkdir(admin, { recursive: true });
  const png = "data:image/png;base64,iVBORw0KGgo=";
  await writeFile(path.join(admin, "dream-skin.json"), JSON.stringify({
    "dsh-dream-skin:skin": "abyss",
    "dsh-dream-skin:wallpaper-kind": "image",
    "dsh-dream-skin:wallpaper": png,
    "dsh-dream-skin:wallpaper-opacity": 0.5,
    "dsh-dream-skin:wallpaper-blur": 8,
  }));
  await writeFile(path.join(admin, "settings.yaml"), "ui-theme:\n  preference: light\n  fontSize: 18\n");
  const previousHome = process.env.GEO_DSH_HOME, previousSource = process.env.GEO_APPEARANCE_SOURCE;
  process.env.GEO_DSH_HOME = root;
  delete process.env.GEO_APPEARANCE_SOURCE;
  t.after(async () => {
    if (previousHome === undefined) delete process.env.GEO_DSH_HOME; else process.env.GEO_DSH_HOME = previousHome;
    if (previousSource === undefined) delete process.env.GEO_APPEARANCE_SOURCE; else process.env.GEO_APPEARANCE_SOURCE = previousSource;
    await rm(root, { recursive: true, force: true });
  });
  const live = await dreamSkinTheme();
  assert.equal(live.id, "geosentinel-midnight");
  assert.equal(live.wallpaper, png);
  assert.equal(live.wallpaperBlur, 8);
  assert.equal(live.fontSize, 18);
  // colorScheme comes from the skin itself; abyss is a dark skin.
  assert.equal(live.colorScheme, "dark");
  // 0.5 opacity is the development value; the draft importer's 0.65 floor must not apply here.
  assert.match(live.tokens["--dsw-alias-bg-base"], /^rgba\(20,20,24,0\.5\)$/);
  await writeFile(path.join(admin, "dream-skin.json"), JSON.stringify({
    "dsh-dream-skin:skin": "system",
    "dsh-dream-skin:wallpaper-kind": "image",
    "dsh-dream-skin:wallpaper": png,
    "dsh-dream-skin:wallpaper-opacity": 0.8,
  }));
  const light = await dreamSkinTheme();
  assert.equal(light.colorScheme, "light");
  assert.match(light.tokens["--dsw-alias-bg-base"], /^rgba\(255,255,255,0\.8\)$/);
  process.env.GEO_APPEARANCE_SOURCE = "product";
  assert.equal((await dreamSkinTheme()).wallpaper, "");
});

// The pinned-native-distribution test is gone with the re-hosting layer: on the
// 0.1.5 line the native server serves the client, and the product only adds its
// basemap assets and appearance (see tests/workbench-routes.test.mjs).
test("the product shell bundles the 0.1.5 surfaces it reuses", async () => {
  const { createRequire } = await import("node:module");
  const { nativeAssets } = await import("../plugins/workbench/native-host.mjs");
  const requireWeb = createRequire(import.meta.resolve("@deepseek-ai/dsh-web-app"));
  const installed = (name) => { try { requireWeb.resolve(name + "/package.json"); return true; } catch { return false; } };
  const assets = await nativeAssets();
  // Core surfaces must always be in the bundle; the 0.1.5-only ones are asserted
  // only when the installed client actually ships them (the product's pins are
  // upgraded separately from this code).
  for (const name of ["@deepseek-ai/dsh-client-modules", "@deepseek-ai/dsh-client-ui-chat"])
    assert.ok(assets.bundle.includes(name), `外壳未包含核心包 ${name}`);
  for (const name of ["@deepseek-ai/dsh-client-ui-deliverables",
    "@deepseek-ai/dsh-client-resources"])
    if (installed(name)) assert.ok(assets.bundle.includes(name), `外壳未包含 ${name}`);
  // `open-in-app` is dropped on purpose: its host API sits behind the closed browser
  // API plane, so an ordinary user's page load would only log a failed request.
  assert.ok(!assets.bundle.includes("@deepseek-ai/dsh-client-ui-open-in-app"), "open-in-app 需要封闭通道，不应打包");
  // The native plan panel is excluded by design: it needs the client-side
  // `remote.commands` Host Remote, which only `api-remotes` publishes — and the
  // product keeps that row closed so a user's browser gets no host-service channel.
  assert.ok(!assets.bundle.includes("@deepseek-ai/dsh-client-ui-plan"), "方案面板需要 api-remotes，产品保持关闭");
  // `ui-workspace` provides the client `uiWorkspace` service, but activating it waits
  // for `workspaces` + `remote.directoryPicker` (closed plane), and the native loader
  // reports the ENTIRE bundle as failed when one booted entry stays pending. Nothing
  // else requires its module, so it is neither bundled nor booted; the product overlay
  // implements the `uiWorkspace` face itself (see workbench/native/client.js).
  assert.ok(!assets.bundle.includes("@deepseek-ai/dsh-client-ui-workspace"), "ui-workspace 会因封闭通道挂起，不应打包");
  assert.match(assets.html, /地缘环境智能计算平台/);
  assert.match(assets.html, /MutationObserver/, "产品标题需要在原生客户端改写后恢复");
});

test("sidebar adapter enforces identity and project ownership; privileged upstream APIs stay denied", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "geo-sidebar-"));
  const store = new PlatformStore(root);
  const admin = store.bootstrapAdmin("admin", "admin-password");
  const user = store.acceptInvite(store.invite(admin), "alice", "user-password");
  const other = store.acceptInvite(store.invite(admin), "other", "other-password");
  const chat = store.createChat(user, store.createProject(user, "private").id);
  const chatRoot = store.chatRoot(user, chat.id);
  await mkdir(path.join(chatRoot, "outputs", "job-1"), { recursive: true });
  await writeFile(path.join(chatRoot, "outputs", "job-1", "report.md"), "# 报告\n结论");
  await writeFile(path.join(chatRoot, "memory", "notes.md"), "private runtime memory");
  const cookies = {};
  for (const [key, name, password] of [["admin", "admin", "admin-password"], ["user", "alice", "user-password"], ["other", "other", "other-password"]]) cookies[key] = "geosentinel_session=" + store.login(name, password).token;
  let handler;
  const server = createServer((req, res) => handler(req, res));
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const host = `127.0.0.1:${server.address().port}`;
  handler = createSidebarHandler({ store, hosts: [host] });
  t.after(async () => { server.closeAllConnections(); await new Promise((resolve) => server.close(resolve)); store.close(); await rm(root, { recursive: true, force: true }); });
  const call = (method, who, body = {}, headers = {}) => fetch(`http://${host}/sidebar/api/${method}`, { method: "POST", headers: { "content-type": "application/json", ...(who ? { cookie: cookies[who] } : {}), ...headers }, body: JSON.stringify(body) });
  assert.equal((await call("settings.get")).status, 401);
  assert.equal((await call("settings.get", "user", {}, { origin: "https://evil.example" })).status, 403);
  const own = await (await call("session.cwd", "user", { sessionId: chat.id, cwd: "C:/Windows" })).json();
  assert.match(own.value.cwd, /^\/工作区\//);
  // An absolute path outside the session view is refused instead of being read
  // through a stale token.
  assert.equal((await call("fs.tree", "user", { sessionId: chat.id, path: "/projects/legacy-id/outputs" })).status, 403);
  assert.equal((await call("session.cwd", "other", { sessionId: chat.id })).status, 404);
  for (const method of ["fs.write", "pty.create", "git.commit", "settings.update", "sidechat.start"])
    for (const who of ["user", "admin"]) assert.equal((await call(method, who, { sessionId: chat.id })).status, 403);
  assert.equal(sidebarPolicy.tabsEnabled.terminal, false);
  assert.equal(sidebarPolicy.tabsEnabled.explorer, true);
  assert.equal(sidebarPolicy.tabsEnabled.subagent, true);
  assert.equal(sidebarPolicy.workspaceFence, true);

  // The 任务管理 tab authorises its root session and answers without an error,
  // so the client does not poll a forbidden endpoint while it is open.
  const live = await (await call("subagents.live", "user", { rootSessionId: chat.id })).json();
  assert.deepEqual(live.value, { live: {} });
  assert.equal((await call("subagents.live", "other", { rootSessionId: chat.id })).status, 404);

  // The explorer surface is read-only, ownership-fenced and blind to runtime
  // memory. It presents the friendly view (上传的文件 / 分析结果 / 过程记录)
  // instead of the raw inputs/outputs layout.
  const tree = await (await call("fs.tree", "user", { sessionId: chat.id, path: own.value.cwd })).json();
  assert.deepEqual(tree.value.entries.map((entry) => entry.name), ["上传的文件", "分析结果", "过程记录"]);
  assert.equal(tree.value.entries.every((entry) => entry.isDir), true);
  // A produced file appears under its kind, not under its job folder.
  const groups = await (await call("fs.tree", "user", { sessionId: chat.id, path: `${own.value.cwd}/分析结果` })).json();
  assert.deepEqual(groups.value.entries.map((entry) => entry.name), ["报告"]);
  const group = await (await call("fs.tree", "user", { sessionId: chat.id, path: `${own.value.cwd}/分析结果/报告` })).json();
  assert.deepEqual(group.value.entries.map((entry) => entry.name), ["report.md"]);
  assert.equal(group.value.entries[0].isDir, false);
  const file = await (await call("fs.read", "user", { sessionId: chat.id, path: `${own.value.cwd}/分析结果/报告/report.md` })).json();
  assert.equal(file.value.kind, "text");
  assert.match(file.value.content, /报告/);
  // The raw job folder stays reachable for traceability.
  const history = await (await call("fs.tree", "user", { sessionId: chat.id, path: `${own.value.cwd}/过程记录` })).json();
  assert.deepEqual(history.value.entries.map((entry) => entry.name), ["job-1"]);
  const nested = await (await call("fs.tree", "user", { sessionId: chat.id, path: `${own.value.cwd}/过程记录/job-1` })).json();
  assert.deepEqual(nested.value.entries.map((entry) => entry.name), ["report.md"]);
  const found = await (await call("fs.search", "user", { sessionId: chat.id, query: "report" })).json();
  assert.ok(found.value.includes(`${own.value.cwd}/分析结果/报告/report.md`), JSON.stringify(found.value));
  assert.equal((await call("fs.tree", "user", { sessionId: chat.id, path: "../../../Windows" })).status, 400);
  assert.equal((await call("fs.tree", "user", { sessionId: chat.id, path: `${own.value.cwd}/../../../Windows` })).status, 400);
  assert.equal((await call("fs.read", "user", { sessionId: chat.id, path: `${own.value.cwd}/memory/notes.md` })).status, 403);
  assert.equal((await call("fs.read", "user", { sessionId: chat.id, path: "/projects/legacy/outputs/job-1/report.md" })).status, 403);
  assert.equal((await call("fs.tree", "other", { sessionId: chat.id, path: own.value.cwd })).status, 404);

  let fileHandler;
  const files = createServer((req, res) => fileHandler(req, res));
  await new Promise((resolve) => files.listen(0, "127.0.0.1", resolve));
  const fileHost = `127.0.0.1:${files.address().port}`;
  fileHandler = createSidebarFileHandler({ store, hosts: [fileHost] });
  t.after(async () => { files.closeAllConnections(); await new Promise((resolve) => files.close(resolve)); });
  const download = await fetch(`http://${fileHost}/sidebar/file?sessionId=${chat.id}&path=${encodeURIComponent(`${own.value.cwd}/分析结果/报告/report.md`)}&download=1`, { headers: { cookie: cookies.user } });
  assert.equal(download.status, 200);
  assert.match(download.headers.get("content-disposition"), /attachment/);
  assert.match(await download.text(), /报告/);
  assert.equal((await fetch(`http://${fileHost}/sidebar/file?sessionId=${chat.id}&path=${encodeURIComponent(`${own.value.cwd}/memory/notes.md`)}`, { headers: { cookie: cookies.user } })).status, 403);
  assert.equal((await fetch(`http://${fileHost}/sidebar/file?sessionId=${chat.id}&path=x`, {})).status, 401);

  let bundleHandler;
  const bundles = createServer((req, res) => bundleHandler(req, res));
  await new Promise((resolve) => bundles.listen(0, "127.0.0.1", resolve));
  const bundleHost = `127.0.0.1:${bundles.address().port}`;
  bundleHandler = createSidebarBundleHandler({ store, hosts: [bundleHost] });
  t.after(async () => { bundles.closeAllConnections(); await new Promise((resolve) => bundles.close(resolve)); });
  const chunk = await fetch(`http://${bundleHost}/sidebar/bundle/editor.js`, { headers: { cookie: cookies.user } });
  assert.equal(chunk.status, 200);
  assert.match(chunk.headers.get("content-type"), /javascript/);
  assert.ok((await chunk.text()).length > 1000);
  assert.equal((await fetch(`http://${bundleHost}/sidebar/bundle/editor.js`, {})).status, 401);
  assert.equal((await fetch(`http://${bundleHost}/sidebar/bundle/../../package.json`, { headers: { cookie: cookies.user } })).status, 404);
  store.disableUser(admin, other.id, true);
  assert.equal((await call("settings.get", "other")).status, 401);
});
