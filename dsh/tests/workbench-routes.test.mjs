import test from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { vendorHandler } from "../plugins/workbench/index.mjs";

// On the 0.1.5 line the native server owns the app shell and the single fallback
// seat, so the product plugin serves only its own assets: the basemap files its
// map surfaces load from the same origin.
test("the product web surface serves basemap assets and nothing else", async (t) => {
  const handler = vendorHandler();
  const server = createServer((req, res) => { void handler(req, res).catch(() => { res.writeHead(500); res.end(); }); });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(async () => { server.closeAllConnections(); await new Promise((resolve) => server.close(resolve)); });
  const base = `http://127.0.0.1:${server.address().port}`;
  for (const path of ["/geo/vendor/leaflet.js", "/geo/vendor/leaflet.css", "/geo/vendor/world.json"]) {
    const response = await fetch(base + path);
    assert.equal(response.status, 200, path);
    assert.equal(response.headers.get("cache-control"), "no-store", path);
  }
  // The retired shell assets must stay gone: the native server serves the app.
  for (const path of ["/", "/geo", "/geo/native/", "/geo/vendor/marked.js", "/geo/app.js", "/geo/monitor.js"])
    assert.equal((await fetch(base + path)).status, 404, path);
  assert.equal((await fetch(base + "/geo/vendor/leaflet.js", { method: "POST" })).status, 405);
  assert.equal(await (await fetch(base + "/geo/vendor/world.json", { method: "HEAD" })).text(), "");
  const world = await (await fetch(base + "/geo/vendor/world.json")).json();
  assert.ok(Array.isArray(world.features) && world.features.length > 0, "世界底图瓦片应有要素");
});
