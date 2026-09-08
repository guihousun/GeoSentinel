import test from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { access, readFile } from "node:fs/promises";
import { workbenchHandler } from "../plugins/workbench/index.mjs";

test("native workbench is the sole entry and retired assets are unavailable", async (t) => {
  const handler = workbenchHandler();
  const server = createServer((req, res) => { void handler(req, res).catch(() => { res.writeHead(500); res.end(); }); });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(async () => { server.closeAllConnections(); await new Promise((resolve) => server.close(resolve)); });
  const base = `http://127.0.0.1:${server.address().port}`;
  for (const path of ["/", "/geo", "/geo/?old=1", "/geo/native"]) {
    const response = await fetch(base + path, { redirect: "manual" });
    assert.equal(response.status, 302);
    assert.equal(response.headers.get("location"), "/geo/native/");
  }
  assert.equal((await fetch(base + "/geo/native/")).status, 200);
  for (const path of ["/geo/app.js", "/geo/style.css", "/geo/monitor.js", "/geo/monitor.css", "/geo/vendor/marked.js", "/geo/vendor/dompurify.js", "/geo/vendor/lucide.js"])
    assert.equal((await fetch(base + path)).status, 404, path);
  for (const path of ["/geo/vendor/leaflet.js", "/geo/vendor/leaflet.css", "/geo/vendor/world.json"])
    assert.equal((await fetch(base + path)).status, 200, path);
  assert.equal((await fetch(base + "/geo/", { method: "POST" })).status, 405);
  assert.equal(await (await fetch(base + "/geo/vendor/world.json", { method: "HEAD" })).text(), "");
  const client = await readFile(new URL("../plugins/workbench/native/client.js", import.meta.url), "utf8");
  assert.ok(!client.includes("返回现有工作台"));
  for (const name of ["index.html", "app.js", "style.css", "monitor.js", "monitor.css"])
    await assert.rejects(access(new URL(`../plugins/workbench/public/${name}`, import.meta.url)), { code: "ENOENT" });
});
