import test from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { startupOptions, inspectEndpoint } from "../scripts/startup-check.mjs";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";

test("trusted product bootstrap is inherited without re-reading product .env as a DSH project", (t) => {
  const root = mkdtempSync(path.join(tmpdir(), "geo-bootstrap-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const home = path.join(root, "home"), profile = path.join(home, "profiles", "geosentinel");
  mkdirSync(profile, { recursive: true });
  writeFileSync(path.join(root, ".env"), "DEEPSEEK_BASE_URL=https://example.invalid\n");
  const require = createRequire(new URL("../node_modules/@deepseek-ai/dsh/lib/bin.js", import.meta.url));
  // From the 0.1.5 line the boot package is no longer linked under `@deepseek-ai/dsh`,
  // so a top-level resolve can fail while the package is present in the pnpm store.
  let module;
  try { module = pathToFileURL(require.resolve("@deepseek-ai/dsh-app-boot")).href; }
  catch {
    const store = new URL("../node_modules/.pnpm/", import.meta.url);
    const entry = readdirSync(store, { withFileTypes: true })
      .filter((candidate) => candidate.isDirectory() && candidate.name.startsWith("@deepseek-ai+dsh-app-boot@"))
      .map((candidate) => candidate.name).sort()[0];
    assert.ok(entry, "依赖树缺少 @deepseek-ai/dsh-app-boot");
    module = new URL(`${entry}/node_modules/@deepseek-ai/dsh-app-boot/lib/index.js`, store).href;
  }
  const code = `import {loadLayeredEnv} from ${JSON.stringify(module)};loadLayeredEnv('dsh');`;
  const options = { encoding: "utf8", windowsHide: true, env: { ...process.env, DSH_HOME: home, DEEPSEEK_BASE_URL: "https://example.invalid" } };
  assert.notEqual(spawnSync(process.execPath, ["--input-type=module", "-e", code], { ...options, cwd: root }).status, 0);
  assert.equal(spawnSync(process.execPath, ["--input-type=module", "-e", code], { ...options, cwd: profile }).status, 0);
});

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
