import { readFile } from "node:fs/promises";
import geojsonvt from "geojson-vt";
import { feature } from "topojson-client";
import { unwrapWorld } from "../../monitoring/basemap.mjs";
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

export function vendorHandler() {
  const world = readFile(
    new URL(import.meta.resolve("world-atlas/countries-110m.json")),
    "utf8",
  ).then((text) => {
    const topology = JSON.parse(text);
    return geojsonvt(
      unwrapWorld(feature(topology, topology.objects.countries)),
      { maxZoom: 0, indexMaxZoom: 0, tolerance: 0, buffer: 0, extent: 4096 },
    ).getTile(0, 0, 0);
  });
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
    if (pathname === "/geo/vendor/world.json") {
      res.writeHead(200, { "content-type": "application/json", "cache-control": "no-store" });
      res.end(req.method === "HEAD" ? undefined : JSON.stringify(await world));
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
