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
