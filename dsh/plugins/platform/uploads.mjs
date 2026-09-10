import path from "node:path";
import { readdir, stat } from "node:fs/promises";
import { PlatformError } from "./store.mjs";
import { WorkspaceFiles } from "./files.mjs";

// The installed dsh-file-upload plugin brings the upload UI (paperclip, drag &
// drop, preview cards) and its own `/api/upload` route. That route has no login
// check, so the product keeps it shadowed with an exact route of its own: this
// proxy authenticates and authorises the caller, enforces the product's size
// and disk limits, and only then forwards the request to the plugin's handler
// (which stays the single implementation of sniffing, naming and storage).
const cookieName = process.env.GEO_PREVIEW_RELEASE ? "geosentinel_preview_session" : "geosentinel_session";
const MAX_FILE_BYTES = 16 * 1024 * 1024;
const MAX_CHAT_UPLOAD_BYTES = 128 * 1024 * 1024;

const json = (res, status, value) => {
  res.writeHead(status, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
  res.end(JSON.stringify(value));
};

function cookieToken(req) {
  return (req.headers.cookie ?? "")
    .split(";")
    .map((part) => part.trim())
    .find((part) => part.startsWith(cookieName + "="))
    ?.slice(cookieName.length + 1);
}

async function directoryBytes(root) {
  let total = 0;
  let entries;
  try {
    entries = await readdir(root, { withFileTypes: true });
  } catch {
    return 0;
  }
  for (const entry of entries) {
    const child = path.join(root, entry.name);
    if (entry.isDirectory()) total += await directoryBytes(child);
    else if (entry.isFile()) total += (await stat(child).catch(() => ({ size: 0 }))).size;
  }
  return total;
}

/** Resolve the chat an upload session belongs to, re-checking account ownership. */
export function uploadChat(store, user, sessionId) {
  if (typeof sessionId !== "string" || sessionId === "") throw new PlatformError(400, "缺少会话标识");
  const member = store.db.prepare("SELECT chat_id FROM agent_sessions WHERE id=?").get(sessionId);
  return store.chat(user, member?.chat_id ?? sessionId);
}

/**
 * Build the handler the NATIVE upload client posts to.
 *
 * On the 0.1.5 line the visible attach control belongs to
 * `@deepseek-ai/dsh-client-file-upload`, which uploads to its own
 * `/api/session/uploadFileBinary` route — a route served behind DSH's single-user
 * token auth, so an ordinary product user gets 401 and the file never arrives
 * (measured: the dock showed 上传失败，点击重试 with one 401 and no stored file). The
 * shell therefore points that client at this path instead (see
 * `patchNativeUploadClient` in native-host.mjs), and this handler authenticates the
 * product session, re-checks chat ownership, enforces the same size and disk limits as
 * the third-party route, forwards the bytes to that same storage handler, and answers
 * in the envelope the native client parses:
 *
 *   { ok: true, value: { receiptId, file: { attachmentId, name, bytes } } }
 *
 * `receiptId`/`attachmentId` are the stored file's workspace-relative path, not invented
 * handles: anything that resolves them later resolves a real file.
 * @param options.store - platform store (identity, chats, roots).
 * @param options.hosts - allowed Host header values.
 * @param options.forward - test seam; defaults to the loopback request to the plugin.
 */
export function createNativeUploadProxy({ store, hosts, forward }) {
  const send = forward ?? (async (req, port, name, chatId) => {
    const headers = { "content-type": "application/octet-stream", "x-session-id": chatId, "x-file-name": name };
    if (typeof req.headers["content-length"] === "string") headers["content-length"] = req.headers["content-length"];
    return fetch(`http://127.0.0.1:${port}/api/upload/forward`, { method: "POST", headers, body: req, duplex: "half" });
  });
  const respond = (res, status, value) => {
    res.writeHead(status, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
    res.end(JSON.stringify(value));
  };
  return async (req, res, port) => {
    try {
      if (req.method !== "POST") throw new PlatformError(405, "请求方式不支持");
      if (!hosts.includes(req.headers.host)) throw new PlatformError(403, "访问域名未获授权");
      const url = new URL(req.url, "http://localhost");
      const page = url.pathname;
      const name = String(url.searchParams.get("name") ?? "").trim();
      if (name === "" || name.includes("/") || name.includes("\\") || name.includes("\0")) throw new PlatformError(400, "文件名称无效");
      const user = store.authenticate(cookieToken(req));
      const chat = uploadChat(store, user, url.searchParams.get("sessionId"));
      const declared = Number(req.headers["content-length"]);
      if (Number.isFinite(declared) && declared > MAX_FILE_BYTES) throw new PlatformError(413, "单个上传文件上限为 16 MiB");
      const used = await directoryBytes(path.join(store.chatRoot(user, chat.id), ".dsh-uploads"));
      if (used >= MAX_CHAT_UPLOAD_BYTES) throw new PlatformError(413, "本对话的上传文件已达上限，请先删除不再需要的文件");
      await new WorkspaceFiles(store, undefined).checkSpace(Number.isFinite(declared) ? declared : MAX_FILE_BYTES);
      const upstream = await send(req, port, name, chat.id);
      const body = await upstream.text();
      if (upstream.status !== 200) {
        respond(res, upstream.status, { ok: false, error: { code: "geosentinel/upload", message: statusMessage(upstream.status, body), details: { path: page } } });
        return;
      }
      let stored = {};
      try { stored = JSON.parse(body); } catch { stored = {}; }
      const relative = typeof stored.relativePath === "string" ? stored.relativePath : undefined;
      respond(res, 200, { ok: true, value: {
        receiptId: relative ?? stored.path ?? name,
        file: { attachmentId: relative ?? stored.path ?? name, name: typeof stored.name === "string" ? stored.name : name,
          bytes: Number.isFinite(declared) ? declared : 0 } } });
    } catch (error) {
      const status = error.status ?? 500;
      respond(res, status, { ok: false, error: { code: "geosentinel/upload", message: status === 500 ? "上传服务暂不可用" : error.message, details: {} } });
    }
  };
}

function statusMessage(status, body) {
  const parsed = (() => { try { return JSON.parse(body); } catch { return undefined; } })();
  return parsed?.error ?? `上传服务返回 ${status}`;
}

/**
 * Build the authenticated `/api/upload` handler.
 * @param options.store - platform store (identity, chats, roots).
 * @param options.hosts - allowed Host header values.
 * @param options.forward - test seam; defaults to the loopback request that
 *   reaches the plugin's own prefix handler under a non-shadowed sub-path.
 */
export function createUploadProxy({ store, hosts, forward }) {
  const send = forward ?? (async (req, port) => {
    const headers = {};
    for (const [key, value] of Object.entries(req.headers))
      if (["x-session-id", "x-file-name", "x-file-path", "content-type", "content-length"].includes(key) && typeof value === "string")
        headers[key] = value;
    return fetch(`http://127.0.0.1:${port}/api/upload/forward`, {
      method: req.method,
      headers,
      body: req.method === "POST" ? req : undefined,
      duplex: "half",
    });
  });
  return async (req, res, port) => {
    try {
      if (!["POST", "DELETE"].includes(req.method)) throw new PlatformError(405, "请求方式不支持");
      if (!hosts.includes(req.headers.host)) throw new PlatformError(403, "访问域名未获授权");
      if (req.headers.origin) {
        let origin;
        try {
          origin = new URL(req.headers.origin);
        } catch {
          throw new PlatformError(403, "来源无效");
        }
        if (!["http:", "https:"].includes(origin.protocol) || origin.host !== req.headers.host)
          throw new PlatformError(403, "来源无效");
      }
      const user = store.authenticate(cookieToken(req));
      const chat = uploadChat(store, user, req.headers["x-session-id"]);
      if (req.method === "POST") {
        const declared = Number(req.headers["content-length"]);
        if (Number.isFinite(declared) && declared > MAX_FILE_BYTES) throw new PlatformError(413, "单个上传文件上限为 16 MiB");
        const used = await directoryBytes(path.join(store.chatRoot(user, chat.id), ".dsh-uploads"));
        if (used >= MAX_CHAT_UPLOAD_BYTES) throw new PlatformError(413, "本对话的上传文件已达上限，请先删除不再需要的文件");
        await new WorkspaceFiles(store, undefined).checkSpace(Number.isFinite(declared) ? declared : MAX_FILE_BYTES);
      }
      const upstream = await send(req, port);
      const body = await upstream.text();
      res.writeHead(upstream.status, {
        "content-type": upstream.headers.get("content-type") ?? "application/json; charset=utf-8",
        "cache-control": "no-store",
      });
      res.end(body);
    } catch (error) {
      const status = error.status ?? 500;
      json(res, status, { error: status === 500 ? "上传服务暂不可用" : error.message });
    }
  };
}
