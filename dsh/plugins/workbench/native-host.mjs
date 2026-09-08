import { createRequire } from "node:module";
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import path from "node:path";
import { parse } from "acorn";
import { dreamSkinTheme } from "./skin-theme.mjs";

// Resolve from the running Web bundle, never from unrelated top-level links.
const requireWeb = createRequire(import.meta.resolve("@deepseek-ai/dsh-web-app"));
export const nativePlugins = [
  "dsh-client-modules", "dsh-client-locale", "dsh-client-ui-theme",
  "dsh-client-ui-layout", "dsh-client-ui-renderer", "dsh-client-ui-session",
  "dsh-client-ui-conversation", "dsh-client-ui-chat", "dsh-client-ui-tool",
  "dsh-client-ui-user-questions",
].map((name) => "@deepseek-ai/" + name);
const baseline = new Set(["react", "react/jsx-runtime", "react-dom", "react-dom/client",
  "@deepseek-ai/cordis", "@deepseek-ai/dsh-client-store",
  "@deepseek-ai/dsh-client-ui-slots", "@deepseek-ai/dsh-client-ui-primitives"]);

export async function nativeAssets() {
  const version = requireWeb("@deepseek-ai/dsh-web-app/package.json").version;
  const sources = new Map();
  async function collect(name, resolver = requireWeb) {
    name = name.replace(/\/client$/, "");
    if (baseline.has(name) || sources.has(name)) return;
    const manifestPath = resolver.resolve(name + "/package.json");
    const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
    if (name.startsWith("@deepseek-ai/dsh-") && manifest.version !== version)
      throw new Error(`Native UI dependency mismatch: ${name} ${manifest.version} != ${version}`);
    const source = await readFile(path.join(path.dirname(manifestPath), "lib/client.js"), "utf8");
    sources.set(name, source);
    for (const dependency of bundleImports(source))
      await collect(dependency, createRequire(manifestPath));
  }
  for (const name of nativePlugins) await collect(name);
  // API helpers are library-only: their unrestricted transport plugins never activate.
  await collect("@deepseek-ai/dsh-api-session-controller");
  await collect("dsh-better-sidebar", createRequire(import.meta.url));
  const id = "@geosentinel/dsh-workbench";
  const entries = [...nativePlugins, id].map((name) => ({
    id: name, url: "/geo/native/bundle.js", rev: version,
    immediately: name === "@deepseek-ai/dsh-client-modules",
  }));
  const graph = { rev: version, entries,
    batches: [{ phase: "bootstrap", url: "/geo/native/bundle.js", rev: version, entries: entries.map((e) => e.id) }] };
  const { bootInjections } = await import(pathToFileURL(requireWeb.resolve("@deepseek-ai/dsh-client-modules")));
  const injections = bootInjections(graph).map((row) => {
    if (row.kind === "script") return `<script>${row.text}</script>`;
    if (row.kind === "script-src") return `<script src="${row.src}"></script>`;
    if (row.kind === "script-preload") return `<link rel="preload" as="script" href="${row.src}">`;
    if (row.kind === "global") return `<script>window.${row.name}=${JSON.stringify(row.value).replaceAll("<", "\\u003c")}</script>`;
    if (row.kind === "link") return "";
    throw new Error(`Unsupported native boot injection: ${row.kind}`);
  }).join("\n");
  const dist = path.join(path.dirname(requireWeb.resolve("@deepseek-ai/dsh-web-frontend/package.json")), "dist");
  let html = await readFile(path.join(dist, "index.html"), "utf8");
  html = html.replace('<html lang="en">', '<html lang="zh-CN">')
    .replace("<head>", `<head><base href="/geo/native/">${injections}<link rel="stylesheet" href="/geo/native/custom.css"><link rel="stylesheet" href="/geo/vendor/leaflet.css"><script src="/geo/vendor/leaflet.js"></script>`)
    .replace("<title>DeepSeek Harness</title>", "<title>地缘环境智能计算平台</title>")
    .replace("<body>", '<body style="--dsh-content-font-size:16px">');
  const theme = await dreamSkinTheme();
  const bundle = [...sources.values()].join("\n") + `\nwindow.__ModuleLoader__.load({id:"@geosentinel/dsh-theme",factory:()=>(${JSON.stringify(theme)})});`;
  return { dist, html, bundle, version };
}

export function bundleImports(source) {
  const names = new Set(), pending = [parse(source, { ecmaVersion: "latest", sourceType: "script" })];
  while (pending.length) {
    const node = pending.pop();
    if (node.type === "CallExpression" && node.callee?.type === "Identifier" && node.callee.name === "require" && typeof node.arguments[0]?.value === "string") names.add(node.arguments[0].value);
    for (const value of Object.values(node)) {
      if (Array.isArray(value)) for (const child of value) { if (child?.type) pending.push(child); }
      else if (value?.type) pending.push(value);
    }
  }
  return [...names];
}

export function nativeHandler() {
  let assets;
  return async (req, res, pathname) => {
    if (!pathname.startsWith("/geo/native/")) return false;
    if (req.method !== "GET" && req.method !== "HEAD") { res.writeHead(405); res.end(); return true; }
    assets ??= nativeAssets();
    const data = await assets;
    let body, mime;
    if (pathname === "/geo/native/") { body = data.html; mime = "text/html; charset=utf-8"; }
    else if (pathname === "/geo/native/bundle.js") {
      body = data.bundle + "\n" + await readFile(new URL("./native/client.js", import.meta.url), "utf8");
      mime = "text/javascript";
    } else if (pathname === "/geo/native/custom.css") {
      body = await readFile(new URL("./native/style.css", import.meta.url)); mime = "text/css";
    } else {
      const relative = decodeURIComponent(pathname.slice("/geo/native/".length));
      const file = path.resolve(data.dist, relative);
      if (!file.startsWith(data.dist + path.sep) || !/^(assets\/|favicon\.svg$|manifest\.webmanifest$)/.test(relative)) {
        res.writeHead(404); res.end(); return true;
      }
      try { body = await readFile(file); } catch { res.writeHead(404); res.end(); return true; }
      mime = ({ ".js": "text/javascript", ".css": "text/css", ".svg": "image/svg+xml", ".woff2": "font/woff2", ".webmanifest": "application/manifest+json" })[path.extname(file)] ?? "application/octet-stream";
    }
    res.writeHead(200, { "content-type": mime, "cache-control": "no-store", "x-content-type-options": "nosniff",
      "content-security-policy": "default-src 'self'; script-src 'self' 'unsafe-inline' 'unsafe-eval'; style-src 'self' 'unsafe-inline'; font-src 'self' data:; img-src 'self' data: blob:; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'" });
    res.end(req.method === "HEAD" ? undefined : body);
    return true;
  };
}
