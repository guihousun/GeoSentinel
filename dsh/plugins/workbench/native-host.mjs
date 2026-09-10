import { createRequire } from "node:module";
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import path from "node:path";
import { parse } from "acorn";
import { dreamSkinTheme, appearanceStamp } from "./skin-theme.mjs";

// Resolve from the running Web bundle, never from unrelated top-level links.
const requireWeb = createRequire(import.meta.resolve("@deepseek-ai/dsh-web-app"));
const baseline = new Set(["react", "react/jsx-runtime", "react-dom", "react-dom/client",
  "@deepseek-ai/cordis", "@deepseek-ai/dsh-client-store",
  "@deepseek-ai/dsh-client-ui-slots", "@deepseek-ai/dsh-client-ui-primitives"]);
export const nativePlugins = [
  "dsh-client-modules", "dsh-client-locale", "dsh-client-ui-theme",
  "dsh-client-ui-layout", "dsh-client-ui-renderer", "dsh-client-ui-session",
  "dsh-client-ui-conversation", "dsh-client-ui-chat", "dsh-client-ui-tool",
  "dsh-client-ui-user-questions", "dsh-client-ui-subagent", "dsh-client-ui-input-trigger",
  // 0.1.5 surfaces the product reuses instead of re-implementing: the deliverables
  // panel, the resources/files surface and "open in app".
  //
  // NOT included yet: `dsh-client-ui-plan` (and `ui-commands`, which exists only to
  // satisfy it). Both wait for the client-side `remote.commands` service, which the
  // host does not expose in this composition, so adding them makes the whole client
  // bundle fail to activate ("web boot: 2 entries did not activate") and the product
  // shell renders nothing. Prerequisite: expose the host `commands` service to the
  // client, then the native plan panel can replace our own plan surface.
  "dsh-client-ui-deliverables", "dsh-client-resources",
  "dsh-client-ui-open-in-app",
].map((name) => "@deepseek-ai/" + name);
// Surfaces that only exist from the 0.1.5 line on. They are optional so the shell
// still builds against an older installed client (the product upgrades its pins
// separately); the bundle then simply lacks those panels.
export const optionalPlugins = new Set([
  "@deepseek-ai/dsh-client-ui-deliverables",
  "@deepseek-ai/dsh-client-resources",
  "@deepseek-ai/dsh-client-ui-open-in-app",
]);
const missingModule = (error) => error?.code === "MODULE_NOT_FOUND" || error?.code === "ERR_MODULE_NOT_FOUND" || /Cannot find (module|package)/.test(error?.message ?? "");

export async function nativeAssets() {
  const version = requireWeb("@deepseek-ai/dsh-web-app/package.json").version;
  const sources = new Map();
  // dsh-file-upload 0.4.3 ships a client bug: `subscribeErrors` pokes listeners
  // without the current value, so UploadDock stores `undefined` and then reads
  // `error.text`, which crashes the dock after every upload. Patch the bundled
  // source (the installed package stays untouched) and fail loudly if the
  // pinned version ever changes so the patch is re-checked.
  function patchUploadClient(name, packageVersion, source) {
    if (name !== "dsh-file-upload") return source;
    if (packageVersion !== "0.4.3") throw new Error(`dsh-file-upload ${packageVersion} is not the patched version`);
    const fixed = source
      .replaceAll("for (const listener of errorListeners) listener();", "for (const listener of errorListeners) listener(uploadError);")
      .replace("error !== null &&", "error != null &&");
    if (fixed === source) throw new Error("dsh-file-upload client patch no longer matches the installed source");
    return fixed;
  }
  async function collect(name, resolver = requireWeb) {
    name = name.replace(/\/client$/, "");
    if (baseline.has(name) || sources.has(name)) return;
    const manifestPath = resolver.resolve(name + "/package.json");
    const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
    if (name.startsWith("@deepseek-ai/dsh-") && manifest.version !== version)
      throw new Error(`Native UI dependency mismatch: ${name} ${manifest.version} != ${version}`);
    const source = patchUploadClient(name, manifest.version, await readFile(path.join(path.dirname(manifestPath), "lib/client.js"), "utf8"));
    sources.set(name, source);
    for (const dependency of bundleImports(source))
      await collect(dependency, createRequire(manifestPath));
  }
  // Core surfaces are mandatory; the 0.1.5-only ones are skipped (with a note)
  // when the installed client predates them, so the same shell code serves both.
  const collected = new Set();
  for (const name of nativePlugins) {
    try { await collect(name); collected.add(name); }
    catch (error) {
      if (!optionalPlugins.has(name) || !missingModule(error)) throw error;
      console.warn(`GeoSentinel: 外壳跳过未安装的原生界面包 ${name}`);
    }
  }
  // API helpers are library-only: their unrestricted transport plugins never activate.
  await collect("@deepseek-ai/dsh-api-session-controller");
  await collect("dsh-better-sidebar", createRequire(import.meta.url));
  // Data-visualisation client half: renders the dsh-ui fence and render_ui cards.
  await collect("@changfenhuang/dsh-genui", createRequire(import.meta.url));
  // Upload UI (paperclip, drag & drop, preview cards). Its host route is
  // authenticated by the platform's proxy; the microphone button is hidden by
  // the product stylesheet because voice input is not offered.
  await collect("dsh-file-upload", createRequire(import.meta.url));
  const id = "@geosentinel/dsh-workbench";
  // Third-party client halves that mount as plugins in the product UI: the
  // visualisation renderer and the upload dock.
  const thirdParty = ["@changfenhuang/dsh-genui", "dsh-file-upload"];
  const entries = [...collected, ...thirdParty, id].map((name) => ({
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
    .replace("<body>", '<body style="--dsh-content-font-size:16px">')
    // The native client rewrites document.title after boot, so the product name is
    // restored whenever it is replaced.
    .replace("</head>", `<script>(function(){var t=document.querySelector("title");if(!t)return;var n="地缘环境智能计算平台";var f=function(){if(document.title!==n)document.title=n};new MutationObserver(f).observe(t,{childList:true});f()})()</script></head>`);
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
  let assets, stamp;
  return async (req, res, pathname) => {
    if (!pathname.startsWith("/geo/native/")) return false;
    if (req.method !== "GET" && req.method !== "HEAD") { res.writeHead(405); res.end(); return true; }
    // The appearance can be changed from the creation view at any time, so the
    // shell bundle is rebuilt only when its appearance inputs actually change.
    const current = await appearanceStamp();
    if (!assets || current !== stamp) {
      stamp = current;
      assets = nativeAssets().catch((error) => { assets = undefined; stamp = undefined; throw error; });
    }
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
