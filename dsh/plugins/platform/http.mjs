import { PlatformError, workspacePath } from "./store.mjs";
import { createReadStream } from "node:fs";
import { readdir, stat, writeFile, mkdir } from "node:fs/promises";
import path from "node:path";
import { randomUUID, createHash } from "node:crypto";

const cookieName = "geosentinel_session";
const cookieToken = (req) =>
  (req.headers.cookie ?? "")
    .split(";")
    .map((x) => x.trim())
    .find((x) => x.startsWith(cookieName + "="))
    ?.slice(cookieName.length + 1);
const json = (res, code, data, headers = {}) => {
  res.writeHead(code, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
    ...headers,
  });
  res.end(JSON.stringify(data));
};
async function body(req) {
  let length = 0;
  const chunks = [];
  for await (const chunk of req) {
    length += chunk.length;
    if (length > 24 * 1024 * 1024) throw new PlatformError(413, "请求内容过大");
    chunks.push(chunk);
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}");
  } catch {
    throw new PlatformError(400, "请求格式错误");
  }
}
function only(data, fields) {
  if (
    !data ||
    typeof data !== "object" ||
    Array.isArray(data) ||
    Object.keys(data).some((key) => !fields.includes(key))
  )
    throw new PlatformError(400, "包含不允许的参数");
  return data;
}

export function createPlatformHandler({
  store,
  bridge,
  monitor,
  hosts = ["127.0.0.1:8510", "localhost:8510"],
  secureCookies = true,
  onError = () => {},
}) {
  const attempts = new Map(),
    promptAttempts = new Map();
  const cookie = (value) =>
    `${cookieName}=${value}; HttpOnly; SameSite=Strict; Path=/; Max-Age=${value ? 604800 : 0}${secureCookies ? "; Secure" : ""}`;
  function throttle(req) {
    const now = Date.now(),
      key = req.socket.remoteAddress;
    for (const [k, v] of attempts) if (v.until < now) attempts.delete(k);
    const state = attempts.get(key) ?? { count: 0, until: now + 60000 };
    state.count++;
    attempts.set(key, state);
    if (state.count > 12)
      throw new PlatformError(429, "尝试过于频繁，请稍后再试");
  }
  return async (req, res) => {
    try {
      if (!hosts.includes(req.headers.host))
        throw new PlatformError(403, "访问域名未获授权");
      if (req.headers.origin) {
        let origin;
        try {
          origin = new URL(req.headers.origin);
        } catch {
          throw new PlatformError(403, "来源无效");
        }
        if (
          !["http:", "https:"].includes(origin.protocol) ||
          origin.host !== req.headers.host
        )
          throw new PlatformError(403, "来源无效");
      }
      const url = new URL(req.url, "http://localhost");
      const parts = url.pathname
        .replace(/^\/geo\/api\/?/, "")
        .split("/")
        .filter(Boolean);
      const method = req.method;
      if (parts[0] === "monitor") {
        if (
          method !== "GET" ||
          parts.length !== 2 ||
          !["events", "status"].includes(parts[1])
        )
          throw new PlatformError(404, "接口不存在");
        const snapshot = monitor
          ? await monitor()
          : { enabled: false, items: [], state: "disabled" };
        if (parts[1] === "status") {
          const { items, ...status } = snapshot;
          return json(res, 200, status);
        }
        return json(res, 200, snapshot);
      }
      if (parts.join("/") === "health" && method === "GET")
        return json(res, 200, { status: "ok", service: "geosentinel-dsh" });
      if (parts[0] === "auth" && parts[1] === "login" && method === "POST") {
        throttle(req);
        const data = only(await body(req), ["username", "password"]);
        const result = store.login(data.username, data.password);
        return json(
          res,
          200,
          { user: result.user },
          { "set-cookie": cookie(result.token) },
        );
      }
      if (parts[0] === "auth" && parts[1] === "join" && method === "POST") {
        throttle(req);
        const data = only(await body(req), ["invite", "username", "password"]);
        store.acceptInvite(data.invite, data.username, data.password);
        return json(res, 201, { created: true });
      }
      const secret = cookieToken(req);
      if (parts.join("/") === "auth/status" && method === "GET") {
        try {
          return json(res, 200, { user: store.authenticate(secret) });
        } catch (error) {
          if (error.status === 401) return json(res, 200, { user: null });
          throw error;
        }
      }
      const user = store.authenticate(secret);
      if (parts.join("/") === "auth/me" && method === "GET")
        return json(res, 200, { user });
      if (parts.join("/") === "auth/logout" && method === "POST") {
        store.logout(secret);
        return json(res, 200, { ok: true }, { "set-cookie": cookie("") });
      }
      if (parts.join("/") === "auth/password" && method === "POST") {
        const data = only(await body(req), ["currentPassword", "newPassword"]);
        store.changePassword(user, data.currentPassword, data.newPassword);
        return json(res, 200, { ok: true }, { "set-cookie": cookie("") });
      }
      if (parts[0] === "admin") {
        store.requireAdmin(user);
        if (parts[1] === "invites" && method === "POST")
          return json(res, 201, { invite: store.invite(user) });
        if (parts[1] === "users" && method === "GET")
          return json(res, 200, { users: store.listUsers(user) });
        if (parts[1] === "users" && parts[2] && method === "PATCH") {
          const data = only(await body(req), ["disabled"]);
          if (typeof data.disabled !== "boolean")
            throw new PlatformError(400, "状态无效");
          if (data.disabled && parts[2] !== user.id)
            await bridge.cancelUser?.(parts[2]);
          store.disableUser(user, parts[2], data.disabled);
          return json(res, 200, { ok: true });
        }
      }
      if (parts[0] === "projects") {
        if (parts.length === 1 && method === "GET")
          return json(res, 200, { projects: store.listProjects(user) });
        if (parts.length === 1 && method === "POST") {
          const data = only(await body(req), ["title"]);
          return json(res, 201, {
            project: store.createProject(user, data.title),
          });
        }
        const project = store.project(user, parts[1]);
        if (parts.length === 2 && method === "PATCH") {
          store.updateProject(
            user,
            project.id,
            only(await body(req), ["title", "archived"]),
          );
          return json(res, 200, { ok: true });
        }
        if (parts.length === 2 && method === "DELETE") {
          await bridge.cancelProject?.(user, project.id);
          store.updateProject(user, project.id, { deleted: true });
          return json(res, 200, { ok: true });
        }
        if (parts[2] === "chats" && method === "GET")
          return json(res, 200, { chats: store.listChats(user, project.id) });
        if (parts[2] === "chats" && method === "POST") {
          const data = only(await body(req), ["title"]);
          const chat = store.createChat(user, project.id, data.title);
          try {
            await bridge.create(user, chat);
          } catch (error) {
            store.updateChat(user, chat.id, { deleted: true });
            throw error;
          }
          return json(res, 201, { chat });
        }
        if (parts[2] === "files") {
          const root = path.join(store.projectRoot(user, project.id), "inputs");
          if (method === "GET")
            return json(res, 200, { files: await listFiles(root) });
          if (method === "POST") {
            const data = only(await body(req), ["name", "base64"]);
            if (project.archived) throw new PlatformError(409, "项目已归档");
            if (
              typeof data.name !== "string" ||
              data.name.includes("/") ||
              data.name.startsWith(".")
            )
              throw new PlatformError(400, "文件名无效");
            if (
              typeof data.base64 !== "string" ||
              !/^[A-Za-z0-9+/]*={0,2}$/.test(data.base64)
            )
              throw new PlatformError(400, "文件内容无效");
            const bytes = Buffer.from(data.base64, "base64");
            if (bytes.length > 16 * 1024 * 1024)
              throw new PlatformError(413, "单个上传文件上限为 16 MiB");
            const files = await listFiles(root);
            if (
              files.reduce((sum, f) => sum + f.size, 0) + bytes.length >
              512 * 1024 * 1024
            )
              throw new PlatformError(413, "项目资料超出配额");
            try {
              await writeFile(workspacePath(root, data.name), bytes, {
                flag: "wx",
              });
            } catch (error) {
              if (error.code === "EEXIST")
                throw new PlatformError(409, "同名文件已存在");
              throw error;
            }
            return json(res, 201, { name: data.name, size: bytes.length });
          }
        }
      }
      if (parts[0] === "chats") {
        const chat = store.chat(user, parts[1]);
        if (parts.length === 3 && parts[2] === "questions") {
          if (store.project(user, chat.project_id).archived) throw new PlatformError(409, "项目已归档");
          if (method === "GET") return json(res, 200, await bridge.questions(user, chat.id));
          if (method === "POST") return json(res, 200, await bridge.answerQuestion(user, chat.id, only(await body(req), ["requestId", "answer", "cancel"])));
        }
        if (parts.length === 3 && parts[2] === "review" && method === "POST") {
          if (store.project(user, chat.project_id).archived) throw new PlatformError(409, "项目已归档");
          return json(res, 200, await bridge.questions(user, chat.id, true));
        }
        if (parts[2] === "monitor-context" && method === "POST") {
          const data = only(await body(req), ["eventId"]);
          if (store.project(user, chat.project_id).archived)
            throw new PlatformError(409, "项目已归档");
          const snapshot = monitor ? await monitor() : { items: [] };
          const event = snapshot.items.find((e) => e.id === data.eventId);
          if (!event)
            throw new PlatformError(404, "监测记录已过期，请刷新队列");
          const content = JSON.stringify(
            {
              kind: "public_monitor_evidence",
              trust: "unverified_external_source",
              event,
            },
            null,
            2,
          );
          const name =
            "monitor-" +
            createHash("sha256").update(content).digest("hex").slice(0, 20) +
            ".json";
          const root = path.join(
            store.projectRoot(user, chat.project_id),
            "inputs",
          );
          try {
            await writeFile(workspacePath(root, name), content, { flag: "wx" });
          } catch (error) {
            if (error.code !== "EEXIST") throw error;
          }
          store.audit(user.id, "monitor.import", event.id);
          return json(res, 201, { path: "inputs/" + name });
        }
        if (parts.length === 2 && method === "PATCH") {
          const data = only(await body(req), ["title"]);
          store.updateChat(user, chat.id, data);
          return json(res, 200, { ok: true });
        }
        if (parts.length === 2 && method === "DELETE") {
          await bridge.cancel(user, chat.id);
          store.updateChat(user, chat.id, { deleted: true });
          return json(res, 200, { ok: true });
        }
        if (parts[2] === "history" && method === "GET")
          return json(res, 200, await bridge.history(user, chat.id));
        if (parts.length === 3 && parts[2] === "native-history" && method === "GET")
          return json(res, 200, await bridge.nativeHistory(user, chat.id));
        if (parts[2] === "prompt" && method === "POST") {
          const data = only(await body(req), ["text"]);
          if (
            typeof data.text !== "string" ||
            !data.text.trim() ||
            data.text.length > 24000
          )
            throw new PlatformError(400, "问题内容无效");
          if (store.project(user, chat.project_id).archived)
            throw new PlatformError(409, "项目已归档");
          const now = Date.now();
          for (const [key, value] of promptAttempts)
            if (value.until < now) promptAttempts.delete(key);
          const recent = promptAttempts.get(user.id) ?? {
            until: now + 60000,
            count: 0,
          };
          if (++recent.count > 20)
            throw new PlatformError(429, "提交过于频繁，请稍后再试");
          promptAttempts.set(user.id, recent);
          await bridge.prompt(user, chat.id, data.text, randomUUID());
          return json(res, 202, { accepted: true });
        }
        if (parts[2] === "cancel" && method === "POST") {
          await bridge.cancel(user, chat.id);
          return json(res, 200, { accepted: true });
        }
        if (parts[2] === "plan" && method === "GET")
          return json(res, 200, await bridge.plan(user, chat.id));
        if (parts[2] === "approve" && method === "POST") {
          if (store.project(user, chat.project_id).archived)
            throw new PlatformError(409, "项目已归档");
          const data = only(await body(req), ["teamId", "revision"]);
          return json(
            res,
            200,
            await bridge.approve(user, chat.id, data.teamId, data.revision),
          );
        }
        if (parts[2] === "events" && method === "GET") {
          const controller = new AbortController();
          res.on("close", () => controller.abort());
          res.writeHead(200, {
            "content-type": "text/event-stream; charset=utf-8",
            "cache-control": "no-store",
            "x-accel-buffering": "no",
          });
          const timer = setInterval(() => {
            try {
              store.authenticate(secret);
              store.chat(user, chat.id);
              res.write(": heartbeat\n\n");
            } catch {
              controller.abort();
              res.end();
            }
          }, 10000);
          try {
            for await (const event of bridge.follow(
              user,
              chat.id,
              controller.signal,
            )) {
              store.authenticate(secret);
              store.chat(user, chat.id);
              res.write(`data: ${JSON.stringify(event)}\n\n`);
            }
          } finally {
            clearInterval(timer);
            res.end();
          }
          return;
        }
        if (parts[2] === "files" && method === "GET") {
          const root = path.join(store.chatRoot(user, chat.id), "outputs");
          if (!url.searchParams.has("path"))
            return json(res, 200, { files: await listFiles(root) });
          const filename = workspacePath(root, url.searchParams.get("path"));
          const info = await stat(filename);
          if (!info.isFile()) throw new PlatformError(404, "文件不存在");
          res.writeHead(200, {
            "content-type": "application/octet-stream",
            "content-length": info.size,
            "x-content-type-options": "nosniff",
            "content-disposition": `attachment; filename*=UTF-8''${encodeURIComponent(path.basename(filename))}`,
          });
          createReadStream(filename)
            .on("error", () => res.destroy())
            .pipe(res);
          return;
        }
      }
      throw new PlatformError(404, "接口不存在");
    } catch (error) {
      if (!(error instanceof PlatformError)) onError(error);
      if (res.headersSent) {
        res.end();
        return;
      }
      const status =
        error instanceof PlatformError
          ? error.status
          : error.code === "ENOENT"
            ? 404
            : 500;
      json(res, status, {
        error:
          error instanceof PlatformError
            ? error.message
            : status === 404
              ? "文件不存在"
              : "操作未完成，请稍后重试",
      });
    }
  };
}

async function listFiles(root, prefix = "") {
  const result = [];
  for (const entry of await readdir(root, { withFileTypes: true })) {
    if (entry.isSymbolicLink() || entry.name.startsWith(".")) continue;
    const relative = prefix + entry.name;
    if (entry.isDirectory())
      result.push(
        ...(await listFiles(path.join(root, entry.name), relative + "/")),
      );
    else if (entry.isFile())
      result.push({
        name: relative,
        size: (await stat(path.join(root, entry.name))).size,
      });
  }
  return result;
}
