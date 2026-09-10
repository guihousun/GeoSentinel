import { createRequire } from "node:module";
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import path from "node:path";
import { parse } from "acorn";
import { dreamSkinTheme, appearanceStamp } from "./skin-theme.mjs";

// Resolve from the running Web bundle, never from unrelated top-level links.
const requireWeb = createRequire(import.meta.resolve("@deepseek-ai/dsh-web-app"));
// The product's own root, for client packages the product declares as direct
// dependencies (see `collect`): those are linked beside `dsh/package.json`, not inside
// the web-app's dependency closure.
const requireProduct = createRequire(new URL("../../package.json", import.meta.url));
const baseline = new Set(["react", "react/jsx-runtime", "react-dom", "react-dom/client",
  "@deepseek-ai/cordis", "@deepseek-ai/dsh-client-store",
  "@deepseek-ai/dsh-client-ui-slots", "@deepseek-ai/dsh-client-ui-primitives"]);
// The client packages the product shell boots. This is the SAME core set the pre-0.1.5
// shell bundled (checked against the 0.1.2-era module: modules, locale, theme, layout,
// renderer, session, conversation, chat, tool, user-questions, subagent, input-trigger),
// followed by the surfaces that only exist from 0.1.5 on. Anything else — trajectory,
// jobs, goal, skill, settings, model selection, permission presets, reference,
// message-feedback, workflow-run, directory pickers, api-* — stays out until a product
// need justifies it AND the closed-plane check below passes; that is not a migration
// regression, it is the product's long-standing narrow surface. `tests/native-ui.test.mjs`
// pins this list against the served bundle and boot payload, so a silent drop fails.
export const nativePlugins = [
  "dsh-client-modules", "dsh-client-locale", "dsh-client-ui-theme",
  "dsh-client-ui-layout", "dsh-client-ui-renderer", "dsh-client-ui-session",
  "dsh-client-ui-conversation", "dsh-client-ui-chat", "dsh-client-ui-tool",
  "dsh-client-ui-user-questions", "dsh-client-ui-subagent", "dsh-client-ui-input-trigger",
  // NOT included on purpose: `dsh-client-ui-plan` (and `ui-commands`, which exists
  // only to satisfy it). Both wait for the client-side `remote.commands` service,
  // i.e. the Host Remote channel published by `@deepseek-ai/dsh-typert-protocol`,
  // which `api-remotes` exposes to the browser. The product keeps `api-remotes`
  // closed so an ordinary user's browser never gets a direct host-service channel,
  // so the plan UI stays OURS (the ask_user_question review flow) and bundling the
  // native panel is not just unnecessary but breaks the whole client bundle
  // ("web boot: 2 entries did not activate", nothing renders).
  // 0.1.5's native upload dock. Its `fileUpload` client service is what the native
  // conversation/chat plugins wait for, so without it the whole client bundle stays
  // pending and the shell renders nothing (verified on a release candidate:
  // "web boot: 3 entries did not activate"). It travels with them, and only exists
  // from 0.1.5 on, so it is optional for older installed clients.
  "dsh-client-file-upload",
  // The native sidebar family is REQUIRED from 0.1.5 on: `ui-chat` itself waits for
  // the `sidebarRight` service ("@deepseek-ai/dsh-client-ui-chat: pending (waiting
  // for service: sidebarRight)"), so the product cannot ship the native chat without
  // it. It claims the single `sidebar` slot, which means the third-party
  // `dsh-better-sidebar` the product used before has to step aside — the product's
  // three-group file panel is retired in favour of the native one here (see the
  // note in plugins/platform/sidebar-adapter.mjs).
  "dsh-client-ui-sidebar", "dsh-client-ui-sidebar-right",
  "dsh-client-ui-sidebar-files", "dsh-client-ui-sidebar-documentpreview",
  // The file-resource provider behind those two tabs. The document preview resolves
  // `dsh-resource://file/…` addresses through `ctx.resources`, and without this
  // provider the pane answers "文件资源服务不可用。" It needs exactly the three services
  // the product already publishes (`resources`, `remote`, `remote.workspaceFiles`), and
  // its live change feed is opened lazily, so the absent host change stream only
  // matters if something actually follows it.
  "dsh-api-workspace-files",
  // NOT bundled: `dsh-client-ui-workspace`. It is the native provider of the client
  // `uiWorkspace` service (`connectWorkspace`/`startSession`/`openWorkspace`/
  // `forkSession`/`archiveSession`/`pickDirectory`), which `ui-conversation` and
  // `ui-sidebar` inject. Its own activation waits for `workspaces` and
  // `remote.directoryPicker`, and the picker lives on the host plane the product
  // keeps closed — booting it leaves the entry pending and the native loader then
  // reports the WHOLE client bundle as failed. No other module requires it either,
  // so it is neither bundled nor booted; the product overlay implements the
  // `uiWorkspace` face against its own project/chat model instead. The package stays a
  // declared dependency so its pin tracks the rest of the 0.1.5 client and the native
  // service can be re-enabled if that plane ever opens; it is just not bundled.
  "dsh-client-ui-attachment", "dsh-client-ui-approval",
  "dsh-client-ui-deliverables", "dsh-client-resources",
  // NOT bundled: `dsh-client-ui-open-in-app`. It polls the host's `/open-in-app/apps`
  // API, which sits behind the closed browser API plane and answers 401 for an
  // ordinary user, so the button would be inert and every page load would log a
  // failed request. The feature needs that channel; it is dropped instead.
].map((name) => "@deepseek-ai/" + name);
// Surfaces that only exist from the 0.1.5 line on. They are optional so the shell
// still builds against an older installed client (the product upgrades its pins
// separately); the bundle then simply lacks those panels.
export const optionalPlugins = new Set([
  "@deepseek-ai/dsh-client-file-upload",
  "@deepseek-ai/dsh-client-ui-sidebar",
  "@deepseek-ai/dsh-client-ui-sidebar-right",
  "@deepseek-ai/dsh-client-ui-sidebar-files",
  "@deepseek-ai/dsh-client-ui-sidebar-documentpreview",
  "@deepseek-ai/dsh-client-ui-attachment",
  "@deepseek-ai/dsh-client-ui-approval",
  "@deepseek-ai/dsh-client-ui-deliverables",
  "@deepseek-ai/dsh-client-resources",
  "@deepseek-ai/dsh-api-workspace-files",
]);
// Bundled but never booted as plugins: their activation waits for the browser-facing
// API plane (`typert` + `api-gateway` + `api-remotes`), which the product keeps
// closed so a user's browser gets no direct host-service channel. The product's own
// overlay talks to /geo/api/* instead and only needs the session-controller client
// MODULE (not its plugin activation), so that code stays in the bundle while these
// entries are left out of the boot payload — otherwise the native loader reports the
// whole client bundle as failed and nothing renders at all.
export const noBoot = new Set([
  "@deepseek-ai/dsh-api-session-controller",
  "@deepseek-ai/dsh-api-gateway",
]);
const missingModule = (error) => error?.code === "MODULE_NOT_FOUND" || error?.code === "ERR_MODULE_NOT_FOUND" || /Cannot find (module|package)/.test(error?.message ?? "");

export async function nativeAssets() {
  const version = requireWeb("@deepseek-ai/dsh-web-app/package.json").version;
  const sources = new Map();
  // Every module the native loader must BOOT (in collection order), not just the
  // named plugins: a self-registering transitive dependency is a plugin as well.
  const bootable = [];
  // dsh-file-upload 0.4.3 ships a client bug: `subscribeErrors` pokes listeners
  // without the current value, so UploadDock stores `undefined` and then reads
  // `error.text`, which crashes the dock after every upload. Patch the bundled
  // source (the installed package stays untouched) and fail loudly if the
  // pinned version ever changes so the patch is re-checked.
  function patchUploadClient(name, packageVersion, source) {
    if (name === "dsh-file-upload") {
      if (packageVersion !== "0.4.3") throw new Error(`dsh-file-upload ${packageVersion} is not the patched version`);
      const fixed = source
        .replaceAll("for (const listener of errorListeners) listener();", "for (const listener of errorListeners) listener(uploadError);")
        .replace("error !== null &&", "error != null &&");
      if (fixed === source) throw new Error("dsh-file-upload client patch no longer matches the installed source");
      return fixed;
    }
    if (name === "@deepseek-ai/dsh-client-file-upload") return patchNativeUploadClient(packageVersion, source);
    return source;
  }
  // The native attach control (0.1.5) posts to its own `/api/session/uploadFileBinary`,
  // a route behind DSH's single-user token auth — an ordinary product user gets 401 and
  // the file never reaches the workspace (measured: 上传失败，点击重试, one 401, nothing
  // stored). Point that one constant at the product's authenticated bridge instead
  // (platform/uploads.mjs `createNativeUploadProxy`), which stores through the same
  // handler the product already uses. The installed package stays untouched; a version
  // change fails loudly so the rewrite is re-checked.
  function patchNativeUploadClient(packageVersion, source) {
    const original = 'const FILE_UPLOAD_PATH = "/api/session/uploadFileBinary";';
    if (!source.includes(original))
      throw new Error(`@deepseek-ai/dsh-client-file-upload ${packageVersion} no longer carries ${original}`);
    return source.replace(original, 'const FILE_UPLOAD_PATH = "/api/upload/native";');
  }
  async function collect(name, resolver = requireWeb, required = true) {
    name = name.replace(/\/client$/, "");
    if (baseline.has(name) || sources.has(name)) return;
    let manifestPath;
    try { manifestPath = resolver.resolve(name + "/package.json"); }
    catch (error) {
      if (!missingModule(error)) throw error;
      // A transitive dependency that is simply absent from its parent's own scope must
      // be skipped quietly: throwing here aborted the REST of that package's collection
      // and the caller then reported the parent as "not installed", which is exactly the
      // misleading line the 0.1.5 sidebar family produced.
      if (resolver !== requireWeb) { if (required) throw error; return; }
      // A client package the product declares as its OWN dependency is linked at the
      // product root, which the web-app's dependency closure cannot see — the 0.1.5
      // sidebar-right family is exactly that case. Fall back to the product's own root
      // once, so declaring the dependency is enough to make it resolvable.
      try { manifestPath = requireProduct.resolve(name + "/package.json"); }
      catch (fallback) { if (!required && missingModule(fallback)) return; throw error; }
    }
    const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
    if (name.startsWith("@deepseek-ai/dsh-") && manifest.version !== version)
      throw new Error(`Native UI dependency mismatch: ${name} ${manifest.version} != ${version}`);
    // A transitive dependency is often a plain library rather than a client plugin:
    // only packages that ship a client factory belong in the bundle. The entry file
    // is NOT always `lib/client.js` — several 0.1.5 packages ship `lib/index.js` —
    // so probe the usual candidates (and the manifest's own entry) before giving up.
    const base = path.dirname(manifestPath);
    const shipped = manifest.exports?.["./client"] ?? manifest.exports?.["./client.js"];
    const fromExports = typeof shipped === "string" ? shipped : shipped?.default ?? shipped?.import;
    // `lib/client.js` first: it is the file that registers the package with the
    // native module loader. Only when it is absent do we fall back to the manifest's
    // own entry, which may be a helper module that registers nothing.
    const candidates = ["lib/client.js", typeof fromExports === "string" ? fromExports : undefined,
      "lib/index.js", "index.js", manifest.module, manifest.main];
    let source;
    for (const candidate of candidates) {
      if (typeof candidate !== "string" || !candidate) continue;
      let text;
      try { text = await readFile(path.join(base, candidate), "utf8"); } catch { continue; }
      // The bundle is loaded through the native module loader: a file that never
      // calls it is not a client bundle, so keep looking instead of shipping it.
      if (!text.includes("__ModuleLoader__.load") && !text.includes("__ModuleLoader__")) continue;
      source = text;
      break;
    }
    if (source === undefined) {
      if (required && !optionalPlugins.has(name)) throw new Error(`${name} 缺少客户端入口（试过 ${candidates.filter(Boolean).join(", ")}）`);
      if (!required) return;
      console.warn(`GeoSentinel: 外壳跳过缺少客户端入口的界面包 ${name}`);
      return;
    }
    sources.set(name, patchUploadClient(name, manifest.version, source));
    // The boot payload decides which modules the native loader actually STARTS.
    // A transitive dependency that registers itself is a plugin too, so record it:
    // bundling without booting was why several client services (e.g. sidebarRight)
    // never appeared even though their package was in the bundle.
    if (/__ModuleLoader__\s*\.\s*load\s*\(/.test(source)) bootable.push(name);
    for (const dependency of bundleImports(source))
      await collect(dependency, createRequire(manifestPath), false);
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
  // `dsh-better-sidebar` is deliberately NOT collected any more: the native sidebar
  // family above owns the single `sidebar` slot from 0.1.5 on, and the native chat
  // requires it. On the older line the native sidebar does not exist, so the shell
  // then has no file panel — the product's own panel comes back only if it is
  // re-expressed as a native sidebar contribution.
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
  const entries = [...new Set([...bootable, ...thirdParty, id])].filter((name) => !noBoot.has(name)).map((name) => ({
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
  // The client overlay must know whether the native sidebar family is part of this
  // build: the native one owns the single `sidebar` slot (and the native chat waits
  // for its `sidebarRight` service), so the overlay only registers its own sidebar
  // on the older line where no native sidebar exists.
  const nativeSidebar = sources.has("@deepseek-ai/dsh-client-ui-sidebar");
  const bundle = [...sources.values()].join("\n")
    + `\nglobalThis.__GEOSENTINEL_NATIVE_SIDEBAR__=${nativeSidebar};\n`
    + `window.__ModuleLoader__.load({id:"@geosentinel/dsh-theme",factory:()=>(${JSON.stringify(theme)})});`;
  return { dist, html, bundle, version, nativeSidebar, entries: entries.map((entry) => entry.id) };
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
      // `worker-src 'self' blob:` — the native upload client runs its background
      // transport in a Worker built from a blob URL (`new Worker(URL.createObjectURL(
      // new Blob(["(" + worker + ")()"], {type:"text/javascript"})))`). Without this
      // directive the shell's `default-src 'self'` blocks that worker, so the attach
      // control accepted a file and then failed before any request was sent
      // ("上传失败，点击重试" with zero upload traffic). The blob is created by our own
      // page from our own code, so allowing blob workers keeps script-src tight.
      "content-security-policy": "default-src 'self'; script-src 'self' 'unsafe-inline' 'unsafe-eval'; worker-src 'self' blob:; style-src 'self' 'unsafe-inline'; font-src 'self' data:; img-src 'self' data: blob:; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'" });
    res.end(req.method === "HEAD" ? undefined : body);
    return true;
  };
}




