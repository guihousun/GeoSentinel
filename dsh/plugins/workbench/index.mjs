import { readFile } from "node:fs/promises";
import geojsonvt from "geojson-vt";
import { feature } from "topojson-client";
import { unwrapWorld } from "../../monitoring/basemap.mjs";
import { nativeHandler } from "./native-host.mjs";
export const name = "geosentinel-workbench";
export const inject = ["webServer", "geosentinelPlatform"];
export function workbenchHandler() {
  const serveNative = nativeHandler();
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
      if (["/", "/geo", "/geo/", "/geo/native"].includes(pathname)) {
        res.writeHead(302, { location: "/geo/native/", "cache-control": "no-store" }); res.end(); return;
      }
      if (await serveNative(req, res, pathname)) return;
      if (pathname === "/geo/vendor/world.json") {
        res.writeHead(200, {
          "content-type": "application/json",
          "cache-control": "no-store",
        });
        res.end(req.method === "HEAD" ? undefined : JSON.stringify(await world));
        return;
      }
      if (pathname === "/favicon.ico") {
        res.writeHead(204);
        res.end();
        return;
      }
      const vendor = vendors.get(pathname);
      if (vendor) {
        res.writeHead(200, {
          "content-type": vendor.endsWith(".css")
            ? "text/css"
            : vendor.endsWith(".json")
              ? "application/json"
              : "text/javascript",
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
  ctx.effect(() => ctx.webServer.registerFallback(workbenchHandler()));
}
