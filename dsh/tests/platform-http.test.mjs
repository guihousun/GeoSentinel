import test from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdtempSync, rmSync } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import { PlatformStore } from "../plugins/platform/store.mjs";
import { createPlatformHandler } from "../plugins/platform/http.mjs";
import { RuntimeLedger } from "../plugins/platform/runtime.mjs";

test("HTTP boundary rejects unowned resources, privileged parameters and forged origins", async (t) => {
  const dir = mkdtempSync(path.join(tmpdir(), "geosentinel-http-")),
    store = new PlatformStore(dir), runtime = new RuntimeLedger(store);
  const admin = store.bootstrapAdmin("admin", "valid-admin-password");
  const users = ["alice", "bob"].map((name) =>
    store.acceptInvite(store.invite(admin), name, `${name}-valid-password`),
  );
  const cookies = users.map(
    (u) =>
      "geosentinel_session=" +
      store.login(u.username, `${u.username}-valid-password`).token,
  );
  const projects = users.map((u) => store.createProject(u, "Private"));
  const chat = store.createChat(users[0], projects[0].id);
  const calls = [];
  const handler = createPlatformHandler({
    store,
    runtime,
    hosts: ["127.0.0.1:0"],
    secureCookies: false,
    bridge: {
      create: async () => {},
      prompt: async (...args) => { calls.push(args); const [user, chatId, text, id] = args; runtime.enqueue({ id, kind: "research", operation: "prompt", user, chatId, payload: { text } }); runtime.usage(user.id, "prompts"); return { queued: true }; },
      history: async () => ({ messages: [] }),
      subagentCatalog: async () => ({ entries: [] }),
    },
  });
  const server = createServer((req, res) => {
    req.headers.host = "127.0.0.1:0";
    return handler(req, res);
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(async () => {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    runtime.close(); store.close();
    rmSync(dir, { recursive: true, force: true });
  });
  const base = `http://127.0.0.1:${server.address().port}/geo/api`;
  const call = (route, index = 0, method = "GET", data, extra = {}) =>
    fetch(base + route, {
      method,
      headers: {
        cookie: cookies[index],
        "content-type": "application/json",
        ...extra,
      },
      body: data ? JSON.stringify(data) : undefined,
    });
  assert.equal((await fetch(base + "/projects")).status, 401);
  assert.equal((await call("/admin/users")).status, 403);
  assert.equal((await call(`/chats/${chat.id}/history`, 1)).status, 404);
  assert.equal((await call(`/chats/${chat.id}/subagents`, 1)).status, 404);
  assert.equal((await call(`/chats/${chat.id}/subagents`)).status, 200);
  for (const method of ["POST", "PATCH", "DELETE"]) assert.equal((await call(`/chats/${chat.id}/subagents/child/prompt`, 0, method)).status, 405);
  assert.equal((await call(`/chats/${chat.id}/subagents/child/unknown`)).status, 404);
  assert.equal(
    (await call(`/projects/${projects[0].id}/files`, 1)).status,
    404,
  );
  assert.equal(
    (
      await call(`/projects/${projects[0].id}/chats`, 0, "POST", {
        title: "Test",
        cwd: "C:/",
      })
    ).status,
    400,
  );
  assert.equal(
    (
      await call(`/chats/${chat.id}/prompt`, 0, "POST", {
        text: "Hello",
        agentPreset: "cordis",
      })
    ).status,
    400,
  );
  assert.equal(
    (
      await call(
        `/chats/${chat.id}/prompt`,
        0,
        "POST",
        { text: "Hello" },
        { origin: "http://evil.example" },
      )
    ).status,
    403,
  );
  assert.equal(
    (await call(`/chats/${chat.id}/prompt`, 0, "POST", { text: "Hello" }))
      .status,
    202,
  );
  assert.equal(calls.length, 1);
  assert.equal((await call(`/chats/${chat.id}/queue`, 1)).status, 404);
  assert.equal((await (await call(`/chats/${chat.id}/queue`)).json()).waiting.length, 1);
  assert.equal((await call("/admin/usage")).status, 403);
  assert.equal((await (await call("/account/usage", 1)).json()).usage.length, 0);
  for (let i = 0; i < 19; i++) assert.equal((await call(`/chats/${chat.id}/prompt`, 0, "POST", { text: "Next" })).status, 202);
  assert.equal((await call(`/chats/${chat.id}/prompt`, 0, "POST", { text: "Too many" })).status, 429);
  assert.equal(
    (
      await call(`/projects/${projects[0].id}/files`, 0, "POST", {
        name: "../outside",
        base64: "aGk=",
      })
    ).status,
    400,
  );
  assert.equal(
    (
      await call(`/projects/${projects[0].id}/files`, 0, "POST", {
        name: "notes.txt",
        base64: "aGk=",
      })
    ).status,
    201,
  );
  assert.equal(
    (await call(`/projects/${projects[1].id}/files`, 1)).status,
    200,
  );
  assert.deepEqual(
    (await (await call(`/projects/${projects[1].id}/files`, 1)).json()).files,
    [],
  );
});
