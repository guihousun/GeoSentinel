import { readFile } from "node:fs/promises";
import geojsonvt from "geojson-vt";
import { feature } from "topojson-client";
import { unwrapWorld } from "../../monitoring/basemap.mjs";
import { nativeHandler } from "./native-host.mjs";
import { appearanceStamp, dreamSkinTheme } from "./skin-theme.mjs";

// Product web surface on the DSH 0.1.5 line.
//
// The previous version took the web server's single FALLBACK seat and re-hosted
// the native client bundles itself (branding, patched client plugins, appearance
// bundle). On 0.1.5 the native server owns the shell and that seat, so re-hosting
// is gone. What stays is what the product actually owns:
//
//   - the basemap assets its map surfaces load from the same origin
//     (`/geo/vendor/leaflet.{js,css}`, `/geo/vendor/world.json`), served through a
//     native `prefix` route;
//   - the product's identity and appearance, contributed with the native
//     `tapIndex` transform (the documented escape hatch for markup no structured
//     injection row expresses) instead of a hand-built html.
export const name = "geosentinel-workbench";
export const inject = ["webServer", "geosentinelPlatform"];

// Chinese labels for the countries this product's users actually pick, keyed by the
// ISO 3166-1 numeric id the vendored topology carries. Anything outside this table
// keeps the topology's own English name — the dropdown stays complete either way,
// and no name is invented for a country nobody labelled here.
const COUNTRY_LABELS = {
  4: "阿富汗", 32: "阿根廷", 36: "澳大利亚", 76: "巴西", 104: "缅甸", 112: "白俄罗斯",
  116: "柬埔寨", 124: "加拿大", 156: "中国", 231: "埃塞俄比亚", 250: "法国", 276: "德国",
  356: "印度", 360: "印度尼西亚", 364: "伊朗", 368: "伊拉克", 376: "以色列", 380: "意大利",
  392: "日本", 404: "肯尼亚", 408: "朝鲜", 410: "韩国", 418: "老挝", 458: "马来西亚",
  484: "墨西哥", 566: "尼日利亚", 586: "巴基斯坦", 608: "菲律宾", 616: "波兰", 643: "俄罗斯",
  682: "沙特阿拉伯", 704: "越南", 710: "南非", 724: "西班牙", 729: "苏丹", 760: "叙利亚",
  764: "泰国", 784: "阿联酋", 792: "土耳其", 804: "乌克兰", 818: "埃及", 826: "英国",
  840: "美国",
};

/**
 * Country dropdown index derived from the SAME vendored topology the basemap is cut
 * from: `[{ id, name, cn, bbox }]`, bbox as `[west, south, east, north]`.
 *
 * The box is taken from the country's LARGEST polygon (by outer-ring area) rather
 * than the whole feature: overseas territories and far-flung islands would otherwise
 * stretch the box far past the mainland a user means by "zoom to this country"
 * (France with Guiana, the United States with Alaska, Russia across the antimeridian).
 */
export function countryIndex(collection) {
  const countries = [];
  for (const item of collection?.features ?? []) {
    const name = item.properties?.name;
    const id = String(item.id ?? item.properties?.id ?? "");
    if (!name || !id) continue;
    const polygons = item.geometry?.type === "Polygon" ? [item.geometry.coordinates]
      : item.geometry?.type === "MultiPolygon" ? item.geometry.coordinates : [];
    let outer = null, largest = -1;
    for (const polygon of polygons) {
      const ring = polygon?.[0];
      if (!Array.isArray(ring) || ring.length < 3) continue;
      let area = 0;
      for (let index = 0; index < ring.length; index += 1) {
        const [x, y] = ring[index], [px, py] = ring[(index + ring.length - 1) % ring.length];
        area += px * y - x * py;
      }
      if (Math.abs(area) / 2 > largest) { largest = Math.abs(area) / 2; outer = ring; }
    }
    if (!outer) continue;
    let west = 180, south = 90, east = -180, north = -90;
    for (const [lng, lat] of outer) {
      if (!Number.isFinite(lng) || !Number.isFinite(lat)) continue;
      if (lng < west) west = lng;
      if (lng > east) east = lng;
      if (lat < south) south = lat;
      if (lat > north) north = lat;
    }
    if (west > east || south > north) continue;
    countries.push({ id, name, cn: COUNTRY_LABELS[Number(id)] ?? "", bbox: [west, south, east, north] });
  }
  countries.sort((left, right) => left.name.localeCompare(right.name, "en"));
  return { countries };
}

export function vendorHandler() {
  const topology = readFile(
    new URL(import.meta.resolve("world-atlas/countries-110m.json")),
    "utf8",
  ).then((text) => JSON.parse(text));
  const world = topology.then((parsed) =>
    geojsonvt(
      unwrapWorld(feature(parsed, parsed.objects.countries)),
      { maxZoom: 0, indexMaxZoom: 0, tolerance: 0, buffer: 0, extent: 4096 },
    ).getTile(0, 0, 0),
  );
  const countries = topology.then((parsed) => countryIndex(feature(parsed, parsed.objects.countries)));
  const vendors = new Map([
    ["/geo/vendor/leaflet.js", import.meta.resolve("leaflet")],
    [
      "/geo/vendor/leaflet.css",
      new URL("leaflet.css", import.meta.resolve("leaflet")).href,
    ],
  ]);
  return async (req, res) => {
    const pathname = new URL(req.url, "http://localhost").pathname;
    if (!["GET", "HEAD"].includes(req.method)) { res.writeHead(405, { allow: "GET, HEAD" }); res.end(); return; }
    const json = pathname === "/geo/vendor/world.json" ? world : pathname === "/geo/vendor/countries.json" ? countries : null;
    if (json) {
      res.writeHead(200, { "content-type": "application/json", "cache-control": "no-store" });
      res.end(req.method === "HEAD" ? undefined : JSON.stringify(await json));
      return;
    }
    const vendor = vendors.get(pathname);
    if (vendor) {
      res.writeHead(200, {
        "content-type": vendor.endsWith(".css") ? "text/css" : "text/javascript",
        "x-content-type-options": "nosniff",
        "cache-control": "no-store",
      });
      res.end(req.method === "HEAD" ? undefined : await readFile(new URL(vendor)));
      return;
    }
    res.writeHead(404); res.end();
  };
}

export function apply(ctx) {
  if (ctx.geosentinelPlatform.development) return;
  ctx.effect(() =>
    ctx.webServer.register({
      kind: "prefix",
      path: "/geo/vendor",
      handler: vendorHandler(),
    }),
  );

  // The product shell. 0.1.5 serves its own single-user shell at `/` behind DSH's
  // own token auth, which cannot be handed to ordinary users without breaking the
  // platform's per-account isolation, so the product keeps serving its own gated
  // shell here (as the 0.1.2 line did). `/` is redirected onto it for anyone who
  // lands on the host root; the platform API is what actually enforces accounts.
  const serveShell = nativeHandler();
  ctx.effect(() =>
    ctx.webServer.register({
      kind: "prefix",
      path: "/geo/native",
      handler: async (req, res) => {
        const pathname = new URL(req.url, "http://localhost").pathname;
        if (pathname === "/geo/native") {
          res.writeHead(302, { location: "/geo/native/", "cache-control": "no-store" });
          res.end();
          return;
        }
        try {
          if (!(await serveShell(req, res, pathname))) { res.writeHead(404); res.end(); }
        } catch (error) {
          console.error("GeoSentinel: 工作台资源服务失败：" + error.message);
          if (!res.headersSent) res.writeHead(500);
          res.end();
        }
      },
    }),
  );
  ctx.effect(() =>
    ctx.webServer.register({
      kind: "exact",
      path: "/",
      handler: (req, res) => {
        if (!["GET", "HEAD"].includes(req.method)) { res.writeHead(405, { allow: "GET, HEAD" }); res.end(); return; }
        res.writeHead(302, { location: "/geo/native/", "cache-control": "no-store", "referrer-policy": "no-referrer" });
        res.end();
      },
    }),
  );

  // The appearance can change from the creation view at any time, while the
  // transform itself is synchronous: cache the rendered injection and rebuild it
  // only when its inputs change.
  let stamp = null, injection = "";
  const refresh = async () => {
    try {
      const current = await appearanceStamp();
      if (current === stamp) return;
      const theme = await dreamSkinTheme();
      injection = `<script>window.__ModuleLoader__.load({id:"@geosentinel/dsh-theme",factory:()=>(${JSON.stringify(theme)})});</script>`;
      stamp = current;
    } catch (error) {
      console.error("GeoSentinel: 外观注入未生效：" + error.message);
    }
  };
  void refresh();
  const timer = setInterval(() => void refresh(), 10000);
  timer.unref?.();
  ctx.on("dispose", () => clearInterval(timer));

  ctx.effect(() =>
    ctx.webServer.tapIndex((html) => {
      let out = html
        .replace('<html lang="en">', '<html lang="zh-CN">')
        .replace("<title>DeepSeek Harness</title>", "<title>地缘环境智能计算平台</title>");
      if (injection && out.includes("</head>")) out = out.replace("</head>", `${injection}</head>`);
      return out;
    }),
  );
}
