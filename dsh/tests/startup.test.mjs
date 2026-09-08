import test from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { startupOptions, inspectEndpoint } from "../scripts/startup-check.mjs";

test("startup defaults and validates fixed port arguments", () => {
  assert.deepEqual(startupOptions(["--no-open"]), {
    host: "127.0.0.1", port: 8510, args: ["--no-open", "--port", "8510"],
  });
  assert.equal(startupOptions(["--port=8511"]).port, 8511);
  assert.equal(startupOptions(["--port", "8512"]).port, 8512);
  for (const value of ["0", "-1", "65536", "abc", "1.5", ""])
    assert.throws(() => startupOptions(["--port", value]));
  assert.throws(() => startupOptions(["--port"]));
});

async function listener(t, handler) {
  const server = createServer(handler);
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise((resolve) => { server.closeAllConnections(); server.close(resolve); }));
  return { host: "127.0.0.1", port: server.address().port };
}

test("healthy GeoSentinel is recognized without stopping the listener", async (t) => {
  const endpoint = await listener(t, (req, res) => {
    assert.equal(req.url, "/geo/api/health");
    res.end(JSON.stringify({ status: "ok", service: "geosentinel-dsh" }));
  });
  assert.equal((await inspectEndpoint(endpoint)).state, "running");
  assert.equal((await inspectEndpoint(endpoint)).state, "running");
});

test("foreign, unhealthy and stalled listeners remain occupied", async (t) => {
  for (const body of [{ status: "ok", service: "other" }, { status: "error", service: "geosentinel-dsh" }, "not json"]) {
    const endpoint = await listener(t, (_, res) => res.end(typeof body === "string" ? body : JSON.stringify(body)));
    assert.equal((await inspectEndpoint(endpoint)).state, "occupied");
  }
  const stalled = await listener(t, () => {});
  assert.equal((await inspectEndpoint(stalled, 50)).state, "occupied");
});

test("released port is available and probe releases its socket", async () => {
  const server = createServer();
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const endpoint = { host: "127.0.0.1", port: server.address().port };
  await new Promise((resolve) => server.close(resolve));
  assert.equal((await inspectEndpoint(endpoint)).state, "free");
  assert.equal((await inspectEndpoint(endpoint)).state, "free");
});
