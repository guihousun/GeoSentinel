import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import path from "node:path";
import { tmpdir } from "node:os";
import {
  normalizeCandidate,
  mergeEvents,
  readSnapshot,
  safeUrl,
} from "../monitoring/snapshot.mjs";
import { createPlatformHandler } from "../plugins/platform/http.mjs";
import { PlatformStore } from "../plugins/platform/store.mjs";
import { createServer } from "node:http";
import {unwrapRing} from '../monitoring/basemap.mjs';

test('overview geometry normalizes dateline jumps without changing ordinary rings',()=>{
  const ordinary=[[10,10],[20,20],[15,15],[10,10]];assert.deepEqual(unwrapRing(ordinary),ordinary);
  const crossing=[[179,50],[-179,50],[-178,52],[179,50]];
  const result=unwrapRing(crossing);assert.equal(result[1][0],181);assert.equal(crossing[1][0],-179);
  assert.ok(result.slice(1).every((p,i)=>Math.abs(p[0]-result[i][0])<=180));
});

const candidate = {
  event_id: "test-event",
  title: "Source headline",
  source_name: "GDACS",
  source_url: "https://example.org/event",
  severity: "high",
  event_type: "eq",
  published_at: 1700000000,
  longitude: 0,
  latitude: 0,
  country: "Example country",
};
test("monitor keeps source geometry and never geocodes publisher country", () => {
  const e = normalizeCandidate({
    ...candidate,
    raw: { private: "not public" },
  });
  assert.equal(e.longitude, 0);
  assert.equal(e.latitude, 0);
  assert.equal(e.raw, undefined);
  const news = normalizeCandidate({
    ...candidate,
    source_name: "GDELT",
    longitude: undefined,
    latitude: undefined,
  });
  assert.equal(news.longitude, null);
  assert.equal(news.location, "");
  assert.equal(news.severity, "low");
  const bad = normalizeCandidate({ ...candidate, latitude: 91 });
  assert.equal(bad.latitude, null);
  assert.equal(
    normalizeCandidate({ ...candidate, source_url: "javascript:alert(1)" }),
    null,
  );
  assert.equal(safeUrl("https://user:pass@example.org/"), "");
});
test("monitor deduplicates, retains last good data and expires old records", () => {
  const now = Date.now(),
    e = normalizeCandidate(candidate, now);
  assert.equal(mergeEvents([e], [{ ...e, title: "updated" }], now).length, 1);
  assert.equal(mergeEvents([e], [], now).length, 1);
  assert.equal(
    mergeEvents([{ ...e, seenAt: now - 8 * 86400000 }], [], now).length,
    0,
  );
});
test("monitor snapshot reports missing, stale and offline states without inventing data", async (t) => {
  const directory = await mkdtemp(path.join(tmpdir(), "geo-monitor-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  assert.deepEqual((await readSnapshot(directory)).items, []);
  const now = Date.now();
  await writeFile(
    path.join(directory, "snapshot.json"),
    JSON.stringify({
      items: [normalizeCandidate(candidate, now)],
      state: "degraded",
      heartbeatAt: now - 100000,
      lastSuccessAt: now,
      intervalMs: 1800000,
    }),
  );
  const snapshot = await readSnapshot(directory, now);
  assert.equal(snapshot.workerOnline, false);
  assert.equal(snapshot.stale, true);
  assert.equal(snapshot.items.length, 1);
});
test("public snapshot is shared, read-only, and import is isolated to the owner project", async (t) => {
  const directory = await mkdtemp(path.join(tmpdir(), "geo-monitor-http-"));
  const store = new PlatformStore(directory);
  const admin = store.bootstrapAdmin("admin", "test-admin-password"),
    alice = store.acceptInvite(
      store.invite(admin),
      "alice",
      "test-alice-password",
    ),
    bob = store.acceptInvite(store.invite(admin), "bob", "test-bob-password");
  const project = store.createProject(alice, "test"),
    chat = store.createChat(alice, project.id);
  const a =
      "geosentinel_session=" +
      store.login("alice", "test-alice-password").token,
    b = "geosentinel_session=" + store.login("bob", "test-bob-password").token;
  const snapshot = {
    items: [normalizeCandidate(candidate)],
    enabled: true,
    state: "ok",
  };
  const handler = createPlatformHandler({
    store,
    bridge: {},
    hosts: ["127.0.0.1:0"],
    secureCookies: false,
    monitor: async () => snapshot,
  });
  const server = createServer((req, res) => {
    req.headers.host = "127.0.0.1:0";
    return handler(req, res);
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(async () => {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    store.close();
    await rm(directory, { recursive: true, force: true });
  });
  const base = `http://127.0.0.1:${server.address().port}/geo/api`;
  const call = (route, cookie = "", method = "GET", data) =>
    fetch(base + route, {
      method,
      headers: { cookie, "content-type": "application/json" },
      body: data ? JSON.stringify(data) : undefined,
    });
  for (const cookie of ["", a, b])
    assert.deepEqual(
      await (await call("/monitor/events", cookie)).json(),
      snapshot,
    );
  assert.equal((await call("/monitor/events", a, "POST", {})).status, 404);
  const imported = await call(`/chats/${chat.id}/monitor-context`, a, "POST", {
    eventId: "test-event",
  });
  assert.equal(imported.status, 201);
  assert.match(
    (await imported.json()).path,
    /^inputs\/monitor-[a-f0-9]+\.json$/,
  );
  assert.equal(
    (
      await call(`/chats/${chat.id}/monitor-context`, b, "POST", {
        eventId: "test-event",
      })
    ).status,
    404,
  );
});
