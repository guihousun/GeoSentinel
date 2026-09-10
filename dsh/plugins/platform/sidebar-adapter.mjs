import { WebSocketServer } from "ws";
import { createReadStream } from "node:fs";
import { open, readdir, readFile, stat } from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";
import { PlatformError } from "./store.mjs";
import { VIEW, workspaceView, viewEntries, resolveViewPath, searchView } from "./workspace-view.mjs";
import { isShapefileSidecar } from "./shapefile.mjs";
import { buildZip } from "./zip.mjs";

export const sidebarPolicy = Object.freeze({
  openByDefault: false, defaultWidthPercent: 34, bottomPanelAutoTerminal: false,
  autoOpenSubagent: false, autoOpenJobs: false, agentTerminalTools: false,
  agentOpenTools: false, interceptOpenPath: false, browserInterceptLinks: false,
  workspaceFence: true, htmlViewerNoSandbox: false, browserNoSandbox: false,
  titleBarScheme: "web", tabsEnabled: { editor: true, explorer: true, git: false,
    subagent: true, sidechat: false, terminal: false, browser: false, diff: false },
  viewersEnabled: {}, pluginSettings: {},
});

// The explorer is the product's only file surface: read-only, ownership-fenced,
// blind to runtime memory, and presented through the friendly workspace view
// (上传的文件 / 分析结果 / 过程记录) instead of the raw inputs/outputs layout.
const TEXT_PREVIEW_BYTES = 1024 * 1024;
const BINARY_HEAD_BYTES = 512;
const SEARCH_LIMIT = 200;

// better-sidebar lazily loads its own client chunks from /sidebar/bundle/<name>.js;
// the product has no better-sidebar host half, so the adapter serves them.
const CHUNKS = new Set(["editor", "mermaid", "registry", "terminal"]);
const sidebarLib = path.join(path.dirname(createRequire(import.meta.url).resolve("dsh-better-sidebar/package.json")), "lib");

const CONTENT_TYPES = new Map([
  [".json", "application/json; charset=utf-8"], [".geojson", "application/geo+json"],
  [".md", "text/markdown; charset=utf-8"], [".txt", "text/plain; charset=utf-8"],
  [".csv", "text/csv; charset=utf-8"], [".html", "text/html; charset=utf-8"],
  [".png", "image/png"], [".jpg", "image/jpeg"], [".jpeg", "image/jpeg"], [".webp", "image/webp"],
  [".svg", "image/svg+xml"], [".tif", "image/tiff"], [".tiff", "image/tiff"],
  [".pdf", "application/pdf"], [".zip", "application/zip"],
]);

export function sidebarIdentity(req, store, hosts) {
  if (!hosts.includes(req.headers.host)) throw new PlatformError(403, "访问域名未获授权");
  if (req.headers.origin) {
    let origin;
    try { origin = new URL(req.headers.origin); } catch { throw new PlatformError(403, "来源无效"); }
    if (!["http:", "https:"].includes(origin.protocol) || origin.host !== req.headers.host) throw new PlatformError(403, "来源无效");
  }
  const cookie = process.env.GEO_PREVIEW_RELEASE ? "geosentinel_preview_session=" : "geosentinel_session=";
  const token = (req.headers.cookie ?? "").split(";").map((part) => part.trim()).find((part) => part.startsWith(cookie))?.slice(cookie.length);
  return store.authenticate(token);
}

export function sidebarChat(user, id, store) {
  const member = store.db.prepare("SELECT chat_id FROM agent_sessions WHERE id=?").get(id);
  return store.chat(user, member?.chat_id ?? id);
}

const safeSegment = (value) => String(value ?? "").replace(/[/\\\0]/g, " ").replace(/\s+/g, " ").trim().slice(0, 40);
/** UI-only virtual root. It is never a filesystem authority. */
export const virtualRoot = (chat) => `/工作区/${safeSegment(chat.title) || chat.project_id}`;

/** The friendly view roots for one chat: user uploads, produced files, raw jobs. */
async function viewFor(user, chat, store, share) {
  return workspaceView({
    chatRoot: store.chatRoot(user, chat.id),
    inputsRoot: path.join(store.chatRoot(user, chat.id), "inputs"),
    projectInputsRoot: path.join(store.projectRoot(user, chat.project_id), "inputs"),
    share,
  });
}

/**
 * Resolve a virtual explorer path to a real file. The friendly labels
 * (上传的文件/分析结果/过程记录) are the only vocabulary the user sees; the
 * chat inputs/outputs layout stays an implementation detail.
 */
async function resolveFriendly(user, chat, store, share, value) {
  const root = virtualRoot(chat);
  const raw = String(value ?? "");
  if (!raw || raw.includes("..") || raw.includes("\\") || raw.includes("\0"))
    throw new PlatformError(400, "文件路径无效");
  const virtual = raw.startsWith("/") ? raw : `${root}/${raw}`;
  if (virtual !== root && !virtual.startsWith(root + "/")) throw new PlatformError(403, "文件路径超出工作区");
  const relative = virtual === root ? "" : virtual.slice(root.length + 1);
  if (relative === "memory" || relative.startsWith("memory/"))
    throw new PlatformError(403, "该目录不开放浏览");
  const view = await viewFor(user, chat, store, share);
  const real = resolveViewPath(view, root, virtual);
  if (!real) throw new PlatformError(404, "文件不存在");
  return real;
}

/** Authenticated, ownership-checked real path for one explorer leaf. */
async function resolveExplorerFile(user, chat, store, share, value) {
  const real = await resolveFriendly(user, chat, store, share, value);
  const allowed = [
    store.chatRoot(user, chat.id),
    path.join(store.projectRoot(user, chat.project_id), "inputs"),
    ...share.map((entry) => entry.root),
  ];
  const canonical = real.split(path.sep).join("/");
  if (!allowed.some((base) => canonical.startsWith(base.split(path.sep).join("/") + "/") || canonical === base.split(path.sep).join("/")))
    throw new PlatformError(403, "文件路径超出工作区");
  return real;
}

async function readPreview(file, size) {
  const handle = await open(file, "r");
  try {
    const length = Math.min(size, TEXT_PREVIEW_BYTES);
    const buffer = Buffer.alloc(length);
    if (length) await handle.read(buffer, 0, length, 0);
    const truncated = size > length;
    const binary = buffer.subarray(0, 8192).includes(0);
    return binary
      ? { kind: "binary", head: buffer.subarray(0, BINARY_HEAD_BYTES).toString("base64"), truncated }
      : { kind: "text", content: buffer.toString("utf8"), truncated };
  } finally { await handle.close(); }
}

export function createSidebarHandler({ store, hosts, share = [] }) {
  return async (req, res) => {
    let status = 200, result;
    try {
      const user = sidebarIdentity(req, store, hosts);
      if (req.method !== "POST") throw new PlatformError(405, "请求方式不支持");
      const method = new URL(req.url, "http://localhost").pathname.slice("/sidebar/api/".length);
      const chunks = []; let size = 0;
      for await (const chunk of req) { size += chunk.length; if (size > 8192) throw new PlatformError(413, "请求内容过大"); chunks.push(chunk); }
      let body;
      try { body = JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}"); } catch { throw new PlatformError(400, "请求格式错误"); }
      if (!body || typeof body !== "object" || Array.isArray(body)) throw new PlatformError(400, "请求格式错误");
      if (method === "settings.get") result = { value: sidebarPolicy, revision: 1, externalDisable: false };
      else if (method === "shell.get") result = { shell: "", name: "未开放终端" };
      else if (method === "session.cwd") {
        const chat = sidebarChat(user, body.sessionId, store);
        // UI-only virtual path. Never accept client cwd as a filesystem authority.
        result = { sessionId: body.sessionId, cwd: virtualRoot(chat), root: virtualRoot(chat), parent: null };
      } else if (method === "fs.tree") {
        const chat = sidebarChat(user, body.sessionId, store);
        const view = await viewFor(user, chat, store, share);
        const root = virtualRoot(chat);
        const requested = String(body.path ?? "");
        if (requested.includes("..") || requested.includes("\\") || requested.includes("\0"))
          throw new PlatformError(400, "文件路径无效");
        const virtualPath = !requested || requested === "/" ? root : requested;
        if (virtualPath !== root && !virtualPath.startsWith(root + "/")) throw new PlatformError(403, "文件路径超出工作区");
        const entries = viewEntries(view, root, virtualPath);
        // Keep the view's own order (uploads → results → history, groups in
        // reading order); only lift directories above files for a familiar
        // file-manager shape.
        entries.sort((left, right) => (left.isDir === right.isDir ? 0 : left.isDir ? -1 : 1));
        result = { entries };
      } else if (method === "fs.read") {
        const chat = sidebarChat(user, body.sessionId, store);
        const file = await resolveExplorerFile(user, chat, store, share, body.path);
        const info = await stat(file).catch(() => null);
        if (!info?.isFile()) throw new PlatformError(404, "文件不存在");
        result = await readPreview(file, info.size);
      } else if (method === "fs.search") {
        const chat = sidebarChat(user, body.sessionId, store);
        const view = await viewFor(user, chat, store, share);
        result = searchView(view, virtualRoot(chat), String(body.query ?? "")).slice(0, SEARCH_LIMIT);
      } else if (method === "subagents.live") {
        // The 任务管理 tab renders member activity from the native session
        // store; this endpoint only needs an ownership check, so a member of
        // another account can never poll it. An empty map means "no extra
        // live detail", not an error, so the tab stays quiet.
        sidebarChat(user, body.rootSessionId, store);
        result = { live: {} };
      } else throw new PlatformError(403, "此操作未在 GeoSentinel 开放");
      result = { ok: true, value: result };
    } catch (error) { status = error.status ?? 500; result = { ok: false, error: { code: "geosentinel/forbidden", message: status === 500 ? "服务暂不可用" : error.message } }; }
    res.writeHead(status, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" }); res.end(JSON.stringify(result));
  };
}

/** `/sidebar/file` — raw bytes for the explorer's open and download actions. */
export function createSidebarFileHandler({ store, hosts, share = [] }) {
  return async (req, res) => {
    try {
      if (req.method !== "GET" && req.method !== "HEAD") throw new PlatformError(405, "请求方式不支持");
      const user = sidebarIdentity(req, store, hosts);
      const url = new URL(req.url, "http://localhost");
      const chat = sidebarChat(user, url.searchParams.get("sessionId"), store);
      const file = await resolveExplorerFile(user, chat, store, share, url.searchParams.get("path"));
      const info = await stat(file).catch(() => null);
      if (!info?.isFile()) throw new PlatformError(404, "文件不存在");
      // A shapefile is only usable with its companions, so downloading a .shp
      // packages the whole set; the sidecars never appear as separate files.
      if (url.searchParams.has("download") && path.extname(file).toLowerCase() === ".shp") {
        const directory = path.dirname(file);
        const stem = path.basename(file, path.extname(file));
        const entries = [];
        for (const name of await readdir(directory)) {
          const isMain = name.toLowerCase() === path.basename(file).toLowerCase();
          if (!isMain && !isShapefileSidecar(name)) continue;
          if (!name.toLowerCase().startsWith(stem.toLowerCase())) continue;
          const entryInfo = await stat(path.join(directory, name)).catch(() => null);
          if (!entryInfo?.isFile()) continue;
          entries.push({ name, data: await readFile(path.join(directory, name)), modifiedAt: entryInfo.mtime });
        }
        if (entries.length > 1) {
          const archive = buildZip(entries);
          res.writeHead(200, {
            "content-type": "application/zip",
            "content-length": archive.length,
            "content-disposition": `attachment; filename*=UTF-8''${encodeURIComponent(stem)}.zip`,
            "cache-control": "no-store",
            "x-content-type-options": "nosniff",
          });
          res.end(req.method === "HEAD" ? undefined : archive);
          return;
        }
      }
      res.writeHead(200, {
        "content-type": CONTENT_TYPES.get(path.extname(file).toLowerCase()) ?? "application/octet-stream",
        "content-length": info.size,
        "cache-control": "no-store",
        "x-content-type-options": "nosniff",
        ...(url.searchParams.has("download") ? { "content-disposition": `attachment; filename*=UTF-8''${encodeURIComponent(path.basename(file))}` } : {}),
      });
      if (req.method === "HEAD") { res.end(); return; }
      createReadStream(file).on("error", () => res.destroy()).pipe(res);
    } catch (error) {
      const status = error.status ?? (error.code === "ENOENT" ? 404 : 500);
      res.writeHead(status, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
      res.end(JSON.stringify({ ok: false, error: { code: "geosentinel/forbidden", message: status === 500 ? "服务暂不可用" : error.message } }));
    }
  };
}

/** `/sidebar/bundle/<name>.js` — the explorer's lazily loaded client chunks. */
export function createSidebarBundleHandler({ store, hosts }) {
  return async (req, res) => {
    try {
      if (req.method !== "GET" && req.method !== "HEAD") throw new PlatformError(405, "请求方式不支持");
      sidebarIdentity(req, store, hosts);
      const name = decodeURIComponent(new URL(req.url, "http://localhost").pathname.slice("/sidebar/bundle/".length)).replace(/\.js$/, "");
      if (!CHUNKS.has(name)) throw new PlatformError(404, "资源不存在");
      const body = await readFile(path.join(sidebarLib, `client-${name}.js`));
      res.writeHead(200, { "content-type": "text/javascript; charset=utf-8", "content-length": body.length, "cache-control": "no-store", "x-content-type-options": "nosniff" });
      res.end(req.method === "HEAD" ? undefined : body);
    } catch (error) {
      const status = error.status ?? (error.code === "ENOENT" ? 404 : 500);
      res.writeHead(status, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
      res.end(JSON.stringify({ ok: false, error: { code: "geosentinel/forbidden", message: status === 500 ? "服务暂不可用" : error.message } }));
    }
  };
}

export function registerSidebarAdapter(ctx, { store, hosts, share = [] }) {
  ctx.effect(() => ctx.webServer.register({ kind: "prefix", path: "/sidebar/api", handler: createSidebarHandler({ store, hosts, share }) }));
  ctx.effect(() => ctx.webServer.register({ kind: "exact", path: "/sidebar/file", handler: createSidebarFileHandler({ store, hosts, share }) }));
  ctx.effect(() => ctx.webServer.register({ kind: "prefix", path: "/sidebar/bundle", handler: createSidebarBundleHandler({ store, hosts }) }));
  const sockets = new WebSocketServer({ noServer: true });
  for (const endpoint of ["agent-terminals", "agent-opens"]) ctx.effect(() => ctx.webServer.registerUpgrade({
    path: `/sidebar/ws/${endpoint}`,
    handler(req, socket, head) {
      const check = () => { const user = sidebarIdentity(req, store, hosts); const id = new URL(req.url, "http://localhost").searchParams.get("sessionId"); sidebarChat(user, id, store); };
      try { check(); } catch { socket.write("HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n"); socket.destroy(); return; }
      sockets.handleUpgrade(req, socket, head, (ws) => {
        if (endpoint === "agent-terminals") ws.send("[]");
        // These optional upstream channels are explicitly capability-empty.
        // They never execute commands and still recheck ownership after login revocation.
        const timer = setInterval(() => { try { check(); ws.ping(); } catch { ws.close(1008, "Access revoked"); } }, 10000);
        ws.on("message", () => ws.close(1008, "Read only")); ws.on("close", () => clearInterval(timer));
      });
    },
  }));
  ctx.on("dispose", () => { for (const ws of sockets.clients) ws.terminate(); sockets.close(); });
}
