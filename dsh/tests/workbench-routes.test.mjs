import test from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { vendorHandler, countryIndex } from "../plugins/workbench/index.mjs";
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
  for (const path of ["/geo/vendor/leaflet.js", "/geo/vendor/leaflet.css", "/geo/vendor/world.json", "/geo/vendor/countries.json"]) {
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

// The region dropdown zooms to a country, so its box must describe the mainland a
// user means — not every island and overseas territory the feature happens to carry.
test("the country index boxes the largest polygon and labels the lead regions", async (t) => {
  const synthetic = countryIndex({ features: [
    { id: 1, properties: { name: "Big" }, geometry: { type: "MultiPolygon", coordinates: [
      [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
      [[[100, 60], [101, 60], [101, 61], [100, 61], [100, 60]]],
    ] } },
    { id: 2, properties: { name: "Ring" }, geometry: { type: "Polygon", coordinates: [[[20, 20], [30, 20], [30, 30], [20, 30], [20, 20]]] } },
    { properties: { name: "No id" }, geometry: { type: "Polygon", coordinates: [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]] } },
  ] });
  assert.deepEqual(synthetic.countries.map((country) => country.name), ["Big", "Ring"], "无 id 的要素应跳过，其余按名称排序");
  assert.deepEqual(synthetic.countries[0].bbox, [0, 0, 10, 10], "范围框应只看最大多边形，不被远处小岛拉伸");

  const handler = vendorHandler();
  const server = createServer((req, res) => { void handler(req, res).catch(() => { res.writeHead(500); res.end(); }); });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(async () => { server.closeAllConnections(); await new Promise((resolve) => server.close(resolve)); });
  const base = `http://127.0.0.1:${server.address().port}`;
  const index = await (await fetch(base + "/geo/vendor/countries.json")).json();
  assert.ok(index.countries.length > 150, `国家索引过小：${index.countries.length}`);
  const byId = new Map(index.countries.map((country) => [country.id, country]));
  // The five regions the panel leads with, each checked against a point inside the
  // country: the box has to contain it, or "zoom to this country" lands elsewhere.
  for (const [id, cn, point] of [["156", "中国", [116.4, 39.9]], ["840", "美国", [-95, 39]],
    ["104", "缅甸", [96.1, 19.7]], ["356", "印度", [77.2, 28.6]], ["116", "柬埔寨", [104.9, 11.6]]]) {
    const country = byId.get(id);
    assert.ok(country, `缺少重点区域 ${id}`);
    assert.equal(country.cn, cn);
    const [west, south, east, north] = country.bbox;
    assert.ok(west < point[0] && point[0] < east, `${cn} 范围框未包含参考点经度`);
    assert.ok(south < point[1] && point[1] < north, `${cn} 范围框未包含参考点纬度`);
    assert.ok(north - south < 90 && east - west < 180, `${cn} 范围框过大，无法定位到国家`);
  }
  assert.deepEqual(byId.get("840").bbox.map(Math.round), [-125, 25, -67, 49], "美国范围框应是本土（不含阿拉斯加）");
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
