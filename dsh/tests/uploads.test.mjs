import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import { PlatformStore } from "../plugins/platform/store.mjs";
import { createUploadProxy, createNativeUploadProxy, uploadChat } from "../plugins/platform/uploads.mjs";

function harness() {
  const dir = mkdtempSync(path.join(tmpdir(), "geo-upload-"));
  const store = new PlatformStore(dir);
  const admin = store.bootstrapAdmin("admin", "valid-admin-password");
  const alice = store.acceptInvite(store.invite(admin), "alice", "alice-valid-password");
  const bob = store.acceptInvite(store.invite(admin), "bob", "bob-valid-password");
  const project = store.createProject(alice, "Study");
  const chat = store.createChat(alice, project.id);
  const cookies = {
    alice: "geosentinel_session=" + store.login("alice", "alice-valid-password").token,
    bob: "geosentinel_session=" + store.login("bob", "bob-valid-password").token,
  };
  return { dir, store, alice, bob, chat, cookies };
}

const request = (overrides = {}) => {
  const { headers = {}, ...rest } = overrides;
  return { method: "POST", ...rest, headers: { host: "127.0.0.1:8511", "content-length": "64", ...headers } };
};

function response() {
  const captured = {};
  return {
    captured,
    writeHead(status, headers) { captured.status = status; captured.headers = headers; },
    end(body) { captured.body = body; },
  };
}

test("the upload proxy authenticates, checks ownership and limits, then forwards", async (t) => {
  const h = harness();
  t.after(() => { h.store.close(); rmSync(h.dir, { recursive: true, force: true }); });
  const forwarded = [];
  const proxy = createUploadProxy({
    store: h.store,
    hosts: ["127.0.0.1:8511"],
    forward: async (req, port) => {
      forwarded.push({ sessionId: req.headers["x-session-id"], method: req.method, port });
      return new Response(JSON.stringify({ path: "/x/.dsh-uploads/y.pdf", relativePath: ".dsh-uploads/y.pdf", name: "y.pdf" }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    },
  });
  const call = async (req) => { const res = response(); await proxy(req, res, 8511); return res.captured; };

  assert.equal((await call(request({ headers: { "x-session-id": h.chat.id } }))).status, 401);
  assert.equal((await call(request({ headers: { cookie: h.cookies.bob, "x-session-id": h.chat.id } }))).status, 404);
  assert.equal((await call(request({ headers: { host: "evil.example", cookie: h.cookies.alice, "x-session-id": h.chat.id } }))).status, 403);
  assert.equal((await call(request({ headers: { origin: "http://evil.example", cookie: h.cookies.alice, "x-session-id": h.chat.id } }))).status, 403);
  assert.equal((await call(request({ method: "GET", headers: { cookie: h.cookies.alice, "x-session-id": h.chat.id } }))).status, 405);
  assert.equal((await call(request({ headers: { cookie: h.cookies.alice, "x-session-id": h.chat.id, "content-length": String(17 * 1024 * 1024) } }))).status, 413);

  const ok = await call(request({ headers: { cookie: h.cookies.alice, "x-session-id": h.chat.id } }));
  assert.equal(ok.status, 200);
  assert.match(ok.body, /relativePath/);
  assert.equal(forwarded.length, 1);
  assert.equal(forwarded[0].sessionId, h.chat.id);
  assert.equal(forwarded[0].port, 8511);

  const removed = await call(request({ method: "DELETE", headers: { cookie: h.cookies.alice, "x-session-id": h.chat.id } }));
  assert.equal(removed.status, 200);
  assert.equal(forwarded[1].method, "DELETE");
});

test("the native attach control uploads through the product bridge, in its own envelope", async (t) => {
  // On the 0.1.5 line the visible attach control belongs to the native file-upload
  // plugin and posts to its own route behind DSH's token auth (measured live: 401 and the
  // file never arrived). The shell points that constant here instead, so this handler has
  // to authenticate, keep ownership and limits, store through the same handler, and
  // answer in the envelope the native client parses.
  const h = harness();
  t.after(() => { h.store.close(); rmSync(h.dir, { recursive: true, force: true }); });
  const forwarded = [];
  let upstream = new Response(JSON.stringify({ path: "/x/.dsh-uploads/y.md", relativePath: ".dsh-uploads/y.md", name: "y.md" }),
    { status: 200, headers: { "content-type": "application/json" } });
  const proxy = createNativeUploadProxy({
    store: h.store,
    hosts: ["127.0.0.1:8511"],
    forward: async (req, port, name, chatId) => {
      forwarded.push({ name, chatId, port, method: req.method });
      return upstream;
    },
  });
  const call = async ({ url, method = "POST", cookies = h.cookies.alice, headers = {} }) => {
    const res = response();
    await proxy({ method, url, headers: { host: "127.0.0.1:8511", "content-length": "10", cookie: cookies, ...headers } }, res, 8511);
    res.captured.json = (() => { try { return JSON.parse(res.captured.body); } catch { return undefined; } })();
    return res.captured;
  };
  const path = (name = "y.md") => `/api/upload/native?sessionId=${h.chat.id}&name=${name}`;

  assert.equal((await call({ url: path(), cookies: "" })).status, 401);
  assert.equal((await call({ url: path(), cookies: h.cookies.bob })).status, 404);
  assert.equal((await call({ url: path("a/b.md") })).status, 400);
  assert.equal((await call({ url: path(), method: "GET" })).status, 405);
  assert.equal((await call({ url: path(), headers: { "content-length": String(17 * 1024 * 1024) } })).status, 413);

  const ok = await call({ url: path() });
  assert.equal(ok.status, 200);
  assert.equal(ok.json.ok, true);
  assert.equal(ok.json.value.file.name, "y.md");
  assert.equal(ok.json.value.file.bytes, 10);
  // The handles are the stored file's own workspace path, not invented ids.
  assert.equal(ok.json.value.receiptId, ".dsh-uploads/y.md");
  assert.equal(ok.json.value.file.attachmentId, ".dsh-uploads/y.md");
  assert.deepEqual(forwarded, [{ name: "y.md", chatId: h.chat.id, port: 8511, method: "POST" }]);

  upstream = new Response(JSON.stringify({ error: "空间不足" }), { status: 507, headers: { "content-type": "application/json" } });
  const failed = await call({ url: path() });
  assert.equal(failed.status, 507);
  assert.equal(failed.json.ok, false);
  assert.equal(failed.json.error.message, "空间不足");
  assert.equal(typeof failed.json.error.details, "object");
});

test("upload sessions resolve member sessions to their owning chat", (t) => {
  const h = harness();
  t.after(() => { h.store.close(); rmSync(h.dir, { recursive: true, force: true }); });
  assert.equal(uploadChat(h.store, h.alice, h.chat.id).id, h.chat.id);
  h.store.recordChild(h.chat.id, "child-session", "NTL_Data_Searcher");
  assert.equal(uploadChat(h.store, h.alice, "child-session").id, h.chat.id);
  assert.throws(() => uploadChat(h.store, h.bob, h.chat.id), { status: 404 });
  assert.throws(() => uploadChat(h.store, h.alice, ""), { status: 400 });
});
