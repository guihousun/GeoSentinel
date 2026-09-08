import { WebSocketServer } from "ws";
import { PlatformError } from "./store.mjs";

export const sidebarPolicy = Object.freeze({
  openByDefault: false, defaultWidthPercent: 34, bottomPanelAutoTerminal: false,
  autoOpenSubagent: false, autoOpenJobs: false, agentTerminalTools: false,
  agentOpenTools: false, interceptOpenPath: false, browserInterceptLinks: false,
  workspaceFence: true, htmlViewerNoSandbox: false, browserNoSandbox: false,
  titleBarScheme: "web", tabsEnabled: { editor: false, explorer: false, git: false,
    subagent: false, sidechat: false, terminal: false, browser: false, diff: false },
  viewersEnabled: {}, pluginSettings: {},
});

export function sidebarIdentity(req, store, hosts) {
  if (!hosts.includes(req.headers.host)) throw new PlatformError(403, "访问域名未获授权");
  if (req.headers.origin) {
    let origin;
    try { origin = new URL(req.headers.origin); } catch { throw new PlatformError(403, "来源无效"); }
    if (!["http:", "https:"].includes(origin.protocol) || origin.host !== req.headers.host) throw new PlatformError(403, "来源无效");
  }
  const token = (req.headers.cookie ?? "").split(";").map((part) => part.trim()).find((part) => part.startsWith("geosentinel_session="))?.slice("geosentinel_session=".length);
  return store.authenticate(token);
}

export function sidebarChat(user, id, store) {
  const member = store.db.prepare("SELECT chat_id FROM agent_sessions WHERE id=?").get(id);
  return store.chat(user, member?.chat_id ?? id);
}

export function createSidebarHandler({ store, hosts }) {
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
        result = { sessionId: body.sessionId, cwd: `/projects/${chat.project_id}`, root: `/projects/${chat.project_id}`, parent: null };
      } else throw new PlatformError(403, "此操作未在 GeoSentinel 开放");
      result = { ok: true, value: result };
    } catch (error) { status = error.status ?? 500; result = { ok: false, error: { code: "geosentinel/forbidden", message: status === 500 ? "服务暂不可用" : error.message } }; }
    res.writeHead(status, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" }); res.end(JSON.stringify(result));
  };
}

export function registerSidebarAdapter(ctx, { store, hosts }) {
  ctx.effect(() => ctx.webServer.register({ kind: "prefix", path: "/sidebar/api", handler: createSidebarHandler({ store, hosts }) }));
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
