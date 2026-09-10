import { spawn } from "node:child_process";
import { request } from "node:http";
import path from "node:path";
import { readFile, stat } from "node:fs/promises";
import { WebSocket, WebSocketServer } from "ws";
import { developmentAccess } from "./access.mjs";
import { PlatformError } from "../plugins/platform/store.mjs";
import { developmentDraft } from "./product-draft.mjs";

export const developmentPrefix = "/geo/development";
export function rewriteDevelopmentHtml(html, bootstrap) {
  return html.replace(/\b(src|href)=(['"])\/(?!\/)(.*?)\2/g, (_all, attr, quote, value) => `${attr}=${quote}${developmentPrefix}/${value}${quote}`)
    .replace(/("(?:url|src)"\s*:\s*")\/(?!\/)/g, `$1${developmentPrefix}/`)
    .replace(/<head(?:\s[^>]*)?>/i, (head) => head + `<script>${bootstrap}</script>`);
}
export function developmentGateway({ store, hosts, source, home, spawnWorker = spawn, isPublishing = () => false }) {
  const access = developmentAccess(store, hosts), workers = new Map(), sockets = new Set();
  let closed = false, lastMutation = 0;
  const bootstrap = readFile(new URL("./bootstrap.js", import.meta.url), "utf8");
  async function worker(user) {
    if (closed) throw new PlatformError(503, "开发服务正在关闭");
    if (workers.has(user.id)) return workers.get(user.id).ready;
    const root = path.join(source, "dsh");
    const child = spawnWorker(process.execPath, [path.join(root, "development/worker.mjs")], {
      cwd: root, windowsHide: true, stdio: ["ignore", "ignore", "pipe", "ipc"],
      env: { ...process.env, GEO_ADMIN_DEV_HOME: path.join(home, "development", user.id), GEO_ADMIN_DEV_USER_ID: user.id, GEO_ADMIN_DEV_SOURCE: source },
    });
    const entry = { child, busy: true };
    entry.ready = new Promise((resolve, reject) => {
      const timer = setTimeout(() => { child.kill(); reject(new PlatformError(503, "开发环境启动超时，请检查服务日志")); }, 60000);
      let diagnostic = "";
      child.stderr?.on("data", (data) => { diagnostic = (diagnostic + data).slice(-2000); });
      child.once("error", () => { clearTimeout(timer); workers.delete(user.id); reject(new PlatformError(503, "开发环境无法启动")); });
      child.once("exit", () => { clearTimeout(timer); workers.delete(user.id); if (!entry.target) console.error("Development startup:", diagnostic.replace(/token=[^\s]+/g, "token=[redacted]")); reject(new PlatformError(503, "开发环境已退出，请重新进入")); });
      child.on("message", async (message) => {
        if (message?.type === "geosentinel:development-state") { entry.busy = message.running !== false; return; }
        if (message?.type !== "geosentinel:development-ready" || entry.target) return;
        try {
          const url = new URL(message.url);
          if (url.protocol !== "http:" || url.hostname !== "127.0.0.1" || !url.port) throw new Error("Invalid developer endpoint");
          const response = await fetch(url, { redirect: "manual", signal: AbortSignal.timeout(5000) });
          const cookie = response.headers.getSetCookie().map((value) => value.split(";")[0]).join("; ");
          if (response.status !== 303 || !cookie) throw new Error("Native authentication failed");
          entry.target = { origin: url.origin, cookie }; clearTimeout(timer); resolve(entry.target);
        } catch { clearTimeout(timer); child.kill(); reject(new PlatformError(503, "开发环境身份握手失败")); }
      });
    });
    workers.set(user.id, entry);
    const revokeCheck = setInterval(() => {
      if (closed) return;
      const current = store.db.prepare("SELECT admin,disabled FROM users WHERE id=?").get(user.id);
      if (!current?.admin || current.disabled) { if (child.connected) child.send({ type: "geosentinel:development-exit" }); }
    }, 2000); revokeCheck.unref(); entry.revokeTimer = revokeCheck;
    child.once("exit", () => clearInterval(revokeCheck));
    return entry.ready;
  }
  async function http(req, res) {
    try {
      if (req.method === "GET" && req.headers["sec-fetch-dest"] === "document") {
        access.identity(req); res.writeHead(302, { location: "/geo/native/#development", "cache-control": "no-store" }); res.end(); return;
      }
      const user = access.check(req), target = await worker(user);
      const parsed = new URL(req.url, "http://localhost"), suffix = parsed.pathname.slice(developmentPrefix.length) || "/";
      if (req.method !== "GET" && req.method !== "HEAD") {
        if (isPublishing() && ["/api/session/prompt", "/api/session/create", "/api/session/fork"].includes(suffix)) throw new PlatformError(503, "正在同步发布，请等待切换完成后开始新的开发任务");
        lastMutation = Date.now();
      }
      if (!parsed.pathname.startsWith(developmentPrefix + "/") && parsed.pathname !== developmentPrefix) throw new PlatformError(404, "页面不存在");
      if (parsed.searchParams.has("token")) throw new PlatformError(400, "开发入口不接受原生登录令牌");
      const headers = { host: new URL(target.origin).host, cookie: target.cookie, origin: target.origin, "accept-encoding": "identity" };
      for (const name of ["content-type", "accept", "content-length"]) if (req.headers[name]) headers[name] = req.headers[name];
      const upstream = request(target.origin + suffix + parsed.search, { method: req.method, headers }, async (response) => {
        const html = response.headers["content-type"]?.includes("text/html");
        const output = { ...response.headers, "cache-control": "no-store", "x-content-type-options": "nosniff", "referrer-policy": "same-origin", "content-security-policy": "frame-ancestors 'self'; base-uri 'self'" };
        for (const name of ["set-cookie", "connection", "transfer-encoding", "content-encoding"]) delete output[name];
        if (output.location) { const location = new URL(output.location, target.origin); if (location.origin !== target.origin) { res.writeHead(502); res.end(); return; } output.location = developmentPrefix + location.pathname + location.search; }
        if (html) {
          delete output["content-length"]; const chunks = []; let size = 0;
          for await (const chunk of response) { size += chunk.length; if (size > 8 * 1024 * 1024) { response.destroy(); res.destroy(); return; } chunks.push(chunk); }
          res.writeHead(response.statusCode, output); res.end(rewriteDevelopmentHtml(Buffer.concat(chunks).toString("utf8"), `window.__GEOSENTINEL_SOURCE__=${JSON.stringify(source).replaceAll("<", "\\u003c")};` + await bootstrap));
        } else { res.writeHead(response.statusCode, output); response.pipe(res); }
      });
      upstream.on("error", () => { if (!res.headersSent) res.writeHead(502); res.end("开发服务暂不可用"); });
      const check = setInterval(() => { try { access.check(req); } catch { upstream.destroy(); res.destroy(); } }, 2000); check.unref();
      res.once("close", () => { clearInterval(check); upstream.destroy(); });
      req.pipe(upstream);
    } catch (error) {
      res.writeHead(error.status || 503, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
      res.end(JSON.stringify({ error: error.status ? error.message : "开发服务暂不可用" }));
    }
  }
  const wss = new WebSocketServer({ noServer: true, maxPayload: 16 * 1024 * 1024 });
  async function upgrade(req, socket, head) {
    try {
      const user = access.check(req), target = await worker(user);
      const url = new URL(req.url, "http://localhost");
      const upstream = new WebSocket(target.origin.replace("http:", "ws:") + url.pathname.slice(developmentPrefix.length) + url.search, { headers: { cookie: target.cookie, origin: target.origin } });
      upstream.once("error", () => socket.destroy());
      upstream.once("open", () => {
        try { access.check(req); } catch { upstream.terminate(); socket.destroy(); return; }
        wss.handleUpgrade(req, socket, head, (ws) => {
          sockets.add(ws);
          ws.on("message", (data, binary) => { try { access.check(req); upstream.send(data, { binary }); } catch { ws.close(1008, "Access revoked"); } });
          upstream.on("message", (data, binary) => { try { access.check(req); if (ws.readyState === WebSocket.OPEN) ws.send(data, { binary }); } catch { ws.close(1008, "Access revoked"); } });
          const timer = setInterval(() => { try { access.check(req); } catch { ws.close(1008, "Access revoked"); } }, 2000); timer.unref();
          ws.once("close", () => { clearInterval(timer); sockets.delete(ws); upstream.terminate(); });
          ws.on("error", () => upstream.terminate());
          upstream.once("close", () => ws.close());
        });
      });
    } catch { socket.write("HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n"); socket.destroy(); }
  }
  return { access, http, upgrade,
    async roots(req) {
      access.check(req);
      const candidates = process.platform === "win32" ? Array.from({ length: 26 }, (_, i) => String.fromCharCode(65 + i) + ":\\") : ["/"];
      const roots = [];
      for (const root of candidates) { try { if ((await stat(root)).isDirectory()) roots.push(root); } catch {} }
      return { roots, source };
    },
    draft(req) { const user = access.check(req); return developmentDraft(home, user.id); },
    busy: () => Date.now() - lastMutation < 2500 || [...workers.values()].some((entry) => entry.busy),
    async start(req) { const user = access.check(req); await worker(user); store.audit(user.id, "development.enter", null); return { url: developmentPrefix + "/" }; },
    close() { closed = true; for (const ws of sockets) ws.terminate(); wss.close(); for (const { child, revokeTimer } of workers.values()) { clearInterval(revokeTimer); if (child.connected) child.send({ type: "geosentinel:development-exit" }); } },
  };
}
