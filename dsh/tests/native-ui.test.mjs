import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { createServer } from "node:http";
import { PlatformStore } from "../plugins/platform/store.mjs";
import { nativeEvent } from "../plugins/platform/native-events.mjs";
import { bundleImports, nativeAssets, nativePlugins } from "../plugins/workbench/native-host.mjs";
import { sidebarPolicy, createSidebarHandler } from "../plugins/platform/sidebar-adapter.mjs";
import { dreamSkinTheme } from "../plugins/workbench/skin-theme.mjs";

test("managed Dream Skin uses real midnight tokens with accessible contrast", async () => {
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
  assert.equal(JSON.stringify(event).includes("secret"), false);
  assert.equal(nativeEvent({ type: "user/message", data: { source: { kind: "plugin" }, content: [] } }), null);
});

test("native bundle dependency scanning ignores comments and string examples", () => {
  assert.deepEqual(bundleImports('/* require("bad") */ const example = `require("bad")`; const x = require("react");'), ["react"]);
});

test("native tool history keeps call identity and reports business failures without exposing payloads", () => {
  const call = nativeEvent({ type: "assistant/message", data: { message: { content: [{ type: "tool-call", id: "c1", name: "geo_list_files", arguments: "secret" }] } } });
  assert.equal(call.data.message.content[0].id, "c1");
  assert.equal(call.data.message.content[0].arguments, "{}");
  const result = nativeEvent({ type: "tool/result", data: { message: { source: { callId: "c1" }, content: [{ type: "tool-result", isError: true, content: [{ type: "text", text: "secret" }] }] } } });
  assert.equal(result.data.message.content[0].isError, true);
  assert.ok(!JSON.stringify(result).includes("secret"));
});

test("pinned native distribution and extension load without activating unrestricted host clients", async () => {
  const assets = await nativeAssets();
  assert.equal(assets.version, "0.1.2-rc.1");
  assert.match(assets.html, /geo\/native\/bundle\.js/);
  assert.match(assets.bundle, /dsh-better-sidebar/);
  assert.ok(nativePlugins.includes("@deepseek-ai/dsh-client-ui-user-questions"));
  assert.match(assets.bundle, /geosentinel-midnight/);
  assert.ok(!assets.bundle.includes("dsh-dream-skin-nav-icon"));
  assert.ok(!assets.bundle.includes("/dream-skin/api"));
  assert.ok(!nativePlugins.includes("@deepseek-ai/dsh-api-session-controller"));
  assert.ok(!nativePlugins.includes("@deepseek-ai/dsh-api-remotes"));
});

test("sidebar adapter enforces identity and project ownership; privileged upstream APIs stay denied", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "geo-sidebar-"));
  const store = new PlatformStore(root);
  const admin = store.bootstrapAdmin("admin", "admin-password");
  const user = store.acceptInvite(store.invite(admin), "alice", "user-password");
  const other = store.acceptInvite(store.invite(admin), "other", "other-password");
  const chat = store.createChat(user, store.createProject(user, "private").id);
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
  assert.match(own.value.cwd, /^\/projects\//);
  assert.equal((await call("session.cwd", "other", { sessionId: chat.id })).status, 404);
  for (const method of ["fs.read", "fs.write", "pty.create", "git.commit", "settings.update", "sidechat.start"])
    for (const who of ["user", "admin"]) assert.equal((await call(method, who, { sessionId: chat.id })).status, 403);
  assert.equal(sidebarPolicy.tabsEnabled.terminal, false);
  assert.equal(sidebarPolicy.workspaceFence, true);
  store.disableUser(admin, other.id, true);
  assert.equal((await call("settings.get", "other")).status, 401);
});
