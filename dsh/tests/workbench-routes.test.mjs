import test from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { vendorHandler } from "../plugins/workbench/index.mjs";
import { nativeHandler } from "../plugins/workbench/native-host.mjs";

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

test("the shell revalidates its multi-megabyte client instead of re-downloading it", async () => {
  // `no-store` cost a full bundle download on every visit (9 MB on the 0.1.5 line, which
  // the remote HTTPS proxy carries). The validator must identify a build exactly: same
  // build revalidates, changed appearance or a different path does not.
  const handler = nativeHandler();
  const shell = async (pathname, headers = {}) => {
    const captured = {};
    const res = { writeHead: (status, value) => { captured.status = status; Object.assign(captured, value); }, end: (body) => { captured.body = body; } };
    assert.equal(await handler({ method: "GET", headers }, res, pathname), true);
    return captured;
  };
  const first = await shell("/geo/native/bundle.js");
  assert.equal(first.status, 200);
  assert.equal(first["cache-control"], "no-cache");
  assert.match(first.etag ?? "", /^"[a-f0-9]{20}"$/);
  const revalidated = await shell("/geo/native/bundle.js", { "if-none-match": first.etag });
  assert.equal(revalidated.status, 304);
  assert.equal(revalidated.body, undefined, "304 不应带正文");
  assert.equal(revalidated.etag, first.etag);
  // A different asset must not reuse the bundle's validator.
  const css = await shell("/geo/native/custom.css", { "if-none-match": first.etag });
  assert.equal(css.status, 200);
  assert.notEqual(css.etag, first.etag);
  // A stale validator gets the body again.
  const stale = await shell("/geo/native/bundle.js", { "if-none-match": '"0000000000000000000"' });
  assert.equal(stale.status, 200);
  assert.ok(stale.body);
});

test("the product shell allows exactly the blob workers its own clients build", async () => {
  // The native upload client runs its background transport in a Worker created from a
  // blob URL. The shell's CSP governs workers through `default-src 'self'` unless
  // `worker-src` says otherwise, and the missing directive was measured live: the attach
  // control accepted a file, showed 上传失败，点击重试 and sent ZERO upload requests.
  const handler = nativeHandler();
  const captured = {};
  const res = { writeHead: (status, headers) => { captured.status = status; Object.assign(captured, headers); }, end: () => {} };
  assert.equal(await handler({ method: "GET" }, res, "/geo/native/bundle.js"), true);
  assert.equal(captured.status, 200);
  const csp = captured["content-security-policy"];
  assert.ok(csp.includes("worker-src 'self' blob:"), "原生上传客户端需要 blob Worker，缺这条会让上传在发请求前失败");
  assert.ok(csp.includes("script-src 'self'"), csp);
  assert.ok(!csp.includes("script-src 'self' blob:"), "不该把 blob 放宽到 script-src");
  assert.ok(csp.includes("frame-ancestors 'none'") && csp.includes("base-uri 'self'"), csp);
});
