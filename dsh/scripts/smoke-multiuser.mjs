import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
import { randomBytes } from "node:crypto";

const base = "http://127.0.0.1:8510/geo/api";
async function login(account) {
  const res = await fetch(base + "/auth/login", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(account),
  });
  assert.equal(res.status, 200);
  return res.headers.get("set-cookie").split(";")[0];
}
async function api(cookie, route, method = "GET", data, expected = 200) {
  const res = await fetch(base + route, {
    method,
    headers: { cookie, "content-type": "application/json" },
    body: data ? JSON.stringify(data) : undefined,
  });
  assert.equal(
    res.status,
    expected,
    `${method} ${route}: ${await (res.status !== expected ? res.text() : Promise.resolve(""))}`,
  );
  return res.json();
}
const adminCookie = await login(
  JSON.parse(await readFile(".runtime/qa-account.json", "utf8")),
);
const stamp = Date.now().toString(36),
  users = [];
for (const name of ["alpha", "beta"]) {
  const { invite } = await api(adminCookie, "/admin/invites", "POST", {}, 201);
  const account = {
    username: `qa_${name}_${stamp}`,
    password: randomBytes(24).toString("hex"),
  };
  await api("", "/auth/join", "POST", { ...account, invite }, 201);
  const cookie = await login(account);
  const { project } = await api(
    cookie,
    "/projects",
    "POST",
    { title: `隔离验收 ${name}` },
    201,
  );
  const { chat } = await api(
    cookie,
    `/projects/${project.id}/chats`,
    "POST",
    { title: "并发问答" },
    201,
  );
  const marker = `PRIVATE_${name}_${stamp}`;
  await api(
    cookie,
    `/projects/${project.id}/files`,
    "POST",
    { name: `${name}.txt`, base64: Buffer.from(marker).toString("base64") },
    201,
  );
  users.push({ cookie, project, chat, marker });
}
const traces = users.map(() => []),
  controllers = users.map(() => new AbortController());
const watchers = users.map(async (u, i) => {
  try {
    const res = await fetch(base + `/chats/${u.chat.id}/events`, {
      headers: { cookie: u.cookie },
      signal: controllers[i].signal,
    });
    assert.equal(res.status, 200);
    const reader = res.body.getReader(),
      decoder = new TextDecoder();
    let buffered = "";
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffered += decoder.decode(value, { stream: true });
      let boundary;
      while ((boundary = buffered.indexOf("\n\n")) >= 0) {
        const frame = buffered.slice(0, boundary);
        buffered = buffered.slice(boundary + 2);
        if (frame.startsWith("data: "))
          traces[i].push(JSON.parse(frame.slice(6)));
      }
    }
  } catch (error) {
    if (error.name !== "AbortError") throw error;
  }
});
try {
  await Promise.all(
    users.map((u) =>
      api(
        u.cookie,
        `/chats/${u.chat.id}/prompt`,
        "POST",
        {
          text: `这是隔离验收，请只用一句中文回答，包含我的标识 ${u.marker}，不要调用工具。`,
        },
        202,
      ),
    ),
  );
  const histories = [];
  for (let attempt = 0; attempt < 45; attempt++) {
    histories.splice(
      0,
      histories.length,
      ...(await Promise.all(
        users.map((u) => api(u.cookie, `/chats/${u.chat.id}/history`)),
      )),
    );
    if (histories.every((h) => h.status === "completed")) break;
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  for (let i = 0; i < 2; i++) {
    const own = users[i],
      other = users[1 - i];
    assert.equal(histories[i].status, "completed");
    assert.ok(JSON.stringify(histories[i]).includes(own.marker));
    assert.ok(!JSON.stringify(histories[i]).includes(other.marker));
    assert.ok(!JSON.stringify(traces[i]).includes(other.marker));
    assert.ok(
      traces[i].some((e) => e.type === "assistant/chunk"),
      "real assistant token stream absent",
    );
    await api(
      own.cookie,
      `/chats/${other.chat.id}/history`,
      "GET",
      undefined,
      404,
    );
    await api(
      own.cookie,
      `/chats/${other.chat.id}/events`,
      "GET",
      undefined,
      404,
    );
    await api(
      own.cookie,
      `/projects/${other.project.id}/files`,
      "GET",
      undefined,
      404,
    );
    await api(own.cookie, "/admin/users", "GET", undefined, 403);
  }
  const report = {
    passed: true,
    realModel: true,
    users: users.map((u, i) => ({
      projectId: u.project.id,
      chatId: u.chat.id,
      streamedTokens: traces[i].filter((e) => e.type === "assistant/chunk")
        .length,
    })),
    at: new Date().toISOString(),
  };
  await writeFile(
    ".runtime/multiuser-acceptance.json",
    JSON.stringify(report, null, 2),
  );
  console.log(JSON.stringify(report));
} finally {
  controllers.forEach((c) => c.abort());
  await Promise.all(watchers);
}
