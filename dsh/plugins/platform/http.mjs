import { PlatformError, workspacePath } from "./store.mjs";
import { createReadStream } from "node:fs";
import { readdir, stat } from "node:fs/promises";
import path from "node:path";
import { randomUUID, createHash } from "node:crypto";
import { WorkspaceFiles } from "./files.mjs";
import { monitorReport } from "../../monitoring/snapshot.mjs";
import { listSpatialFiles, readSpatialFile, PREVIEW_EXTENSIONS } from "./spatial.mjs";

const cookieName = process.env.GEO_PREVIEW_RELEASE ? "geosentinel_preview_session" : "geosentinel_session";
// Media types the conversation may render inline (`inline=1` on the artifact
// route). Everything else stays a download: SVG is excluded on purpose, since
// it can carry script and is served from the product origin.
const INLINE_TYPES = new Map([
  [".png", "image/png"], [".jpg", "image/jpeg"], [".jpeg", "image/jpeg"],
  [".webp", "image/webp"], [".gif", "image/gif"],
  [".csv", "text/csv; charset=utf-8"], [".txt", "text/plain; charset=utf-8"],
  [".md", "text/markdown; charset=utf-8"], [".json", "application/json; charset=utf-8"],
]);
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
    ...(process.env.GEO_RELEASE_ID ? { "x-geosentinel-release": process.env.GEO_RELEASE_ID } : {}),
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
  runtime,
  releases,
  development,
  monitor,
  hosts = ["127.0.0.1:8510", "localhost:8510"],
  secureCookies = true,
  onError = () => {},
}) {
  const attempts = new Map(),
    promptAttempts = new Map();
  const storage = new WorkspaceFiles(store, runtime);
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
    let finishMutation;
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
      if (!["GET", "HEAD"].includes(method) && !(parts[0] === "admin" && parts[1] === "releases")) finishMutation = releases?.beginMutation();
      if (parts.join("/") === "preview-login" && method === "GET" && process.env.GEO_PREVIEW_RELEASE) {
        if (!process.env.GEO_PREVIEW_TOKEN || url.searchParams.get("token") !== process.env.GEO_PREVIEW_TOKEN || Date.now() >= Number(process.env.GEO_PREVIEW_EXPIRES)) throw new PlatformError(403, "预览入口已过期，请重新打开预览");
        const login = store.login("preview_user", process.env.GEO_PREVIEW_PASSWORD);
        res.writeHead(302, { location: "/geo/native/", "set-cookie": cookie(login.token), "cache-control": "no-store", "referrer-policy": "no-referrer" }); res.end(); return;
      }
      if (parts[0] === "monitor" && ["events", "status"].includes(parts[1])) {
        if (method !== "GET" || parts.length !== 2)
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
        return json(res, 200, { status: "ok", service: "geosentinel-dsh", release: process.env.GEO_RELEASE_ID ?? null, preview: Boolean(process.env.GEO_PREVIEW_RELEASE) });
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
          return json(res, 200, { user: store.authenticate(secret), preview: Boolean(process.env.GEO_PREVIEW_RELEASE) });
        } catch (error) {
          if (error.status === 401) return json(res, 200, { user: null });
          throw error;
        }
      }
      const user = store.authenticate(secret);
      if (parts.join("/") === "monitor/report" && method === "GET") {
        const snapshot = monitor ? await monitor() : { items: [], sources: [] };
        const event = snapshot.items.find(
          (e) => e.id === url.searchParams.get("eventId"),
        );
        if (!event)
          throw new PlatformError(404, "监测记录已过期，请刷新队列");
        const report = monitorReport(event, snapshot);
        res.writeHead(200, {
          "content-type": "text/markdown; charset=utf-8",
          "content-disposition": `attachment; filename="monitor-${event.id}.md"`,
          "cache-control": "no-store",
        });
        res.end(report);
        return;
      }
      if (parts.join("/") === "auth/me" && method === "GET")
        return json(res, 200, { user });
      if (parts.join("/") === "account/usage" && method === "GET")
        return json(res, 200, { ...runtime?.summary(user.id), storage: await storage.userUsage(user) });
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
        if (parts[1] === "releases") {
          if (!releases) throw new PlatformError(404, "此实例没有发布管理");
          if (parts.length === 2 && method === "GET") return json(res, 200, await releases.status());
          if (method !== "POST") throw new PlatformError(405, "请求方式不支持");
          if (development) { try { development.access.check(req); } catch { throw new PlatformError(403, "请先确认管理员密码，解锁开发与发布"); } }
          if (parts[2] === "product") return json(res, 200, await releases.saveProduct(await body(req)));
          if (parts[2] === "prepare") return json(res, 202, releases.prepare(user.username));
          if (parts[2] === "cancel") return json(res, 200, await releases.cancel());
          const data = only(await body(req), ["id"]);
          if (parts[2] === "preview") return json(res, 200, await releases.preview(data.id));
          if (["publish", "rollback"].includes(parts[2])) {
            const result = await releases.publish(data.id, user.username, parts[2] === "rollback");
            store.audit(user.id, "release." + parts[2], data.id);
            return json(res, 202, result);
          }
          throw new PlatformError(404, "发布接口不存在");
        }
        if (parts[1] === "development") {
          if (!development) throw new PlatformError(404, "此实例不开放开发模式");
          if (method !== "POST") throw new PlatformError(405, "请求方式不支持");
          if (parts[2] === "unlock") { const data = only(await body(req), ["password"]); return json(res, 200, development.access.confirm(req, data.password)); }
          if (parts[2] === "start") return json(res, 200, await development.start(req));
          if (parts[2] === "draft") return json(res, 200, await development.draft(req));
          if (parts[2] === "roots") return json(res, 200, await development.roots(req));
          if (parts[2] === "lock") { development.access.revoke(req); return json(res, 200, { locked: true }); }
          throw new PlatformError(404, "开发接口不存在");
        }
        if (parts[1] === "usage" && method === "GET") return json(res, 200, runtime?.summary() ?? {});
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
            await storage.writeInput(user, project.id, data.name, bytes);
            return json(res, 201, { name: data.name, size: bytes.length });
          }
        }
      }
      if (parts[0] === "chats") {
        const chat = store.chat(user, parts[1]);
        if (parts[2] === "subagents") {
          if (method !== "GET") throw new PlatformError(405, "子智能体记录只读，请通过主智能体操作");
          if (parts.length === 3) return json(res, 200, await bridge.subagentCatalog(user, chat.id));
          if (parts.length === 5 && parts[4] === "history") return json(res, 200, await bridge.subagentHistory(user, chat.id, parts[3]));
          throw new PlatformError(404, "子智能体接口不存在");
        }
        if (parts.length === 3 && parts[2] === "queue" && method === "GET") return json(res, 200, runtime?.snapshot(user, chat.id) ?? {});
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
          await storage.writeInput(user, chat.project_id, name, Buffer.from(content), { reuse: true });
          store.audit(user.id, "monitor.import", event.id);
          return json(res, 201, { path: "inputs/" + name });
        }
        if (parts.length === 2 && method === "PATCH") {
          const data = only(await body(req), ["title"]);
          if (typeof data.title === "string" && bridge.renameChat)
            await bridge.renameChat(user, chat.id, data.title);
          else store.updateChat(user, chat.id, data);
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
          if (runtime) runtime.throttle(user.id);
          else {
            const now = Date.now();
            for (const [key, value] of promptAttempts)
              if (value.until < now) promptAttempts.delete(key);
            const recent = promptAttempts.get(user.id) ?? { until: now + 60000, count: 0 };
            if (++recent.count > 20) throw new PlatformError(429, "提交过于频繁，请稍后再试");
            promptAttempts.set(user.id, recent);
          }
          const result = await bridge.prompt(user, chat.id, data.text, randomUUID());
          return json(res, 202, { accepted: true, ...result });
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
        if (parts[2] === "spatial" && method === "GET") {
          const root = store.chatRoot(user, chat.id);
          // Uploaded project data is the most likely geospatial source, so the
          // viewer lists `inputs/` (project-scoped) next to the chat workspace.
          // Both roots are ownership-checked; `inputs/...` paths resolve against
          // the project inputs directory and never escape it.
          const inputsRoot = path.join(store.projectRoot(user, chat.project_id), "inputs");
          if (parts[3] === "list") {
            const files = (await listSpatialFiles(root)).map((file) => ({ ...file, scope: "workspace" }));
            const uploaded = (await listSpatialFiles(inputsRoot)).map((file) => ({ ...file, path: "inputs/" + file.path, scope: "inputs" }));
            return json(res, 200, { files: [...files, ...uploaded].sort((left, right) => left.path.localeCompare(right.path, "zh-Hans")) });
          }
          if (parts[3] === "read") {
            const relative = url.searchParams.get("path") ?? "";
            const uploaded = relative.startsWith("inputs/");
            const file = workspacePath(
              uploaded ? inputsRoot : root,
              uploaded ? relative.slice("inputs/".length) : relative,
            );
            const extension = path.extname(file).toLowerCase();
            if (!PREVIEW_EXTENSIONS.includes(extension))
              throw new PlatformError(400, "该文件类型不支持地图预览");
            return json(res, 200, await readSpatialFile(file, extension));
          }
          throw new PlatformError(404, "接口不存在");
        }
        if (parts[2] === "files" && method === "GET") {
          const root = path.join(store.chatRoot(user, chat.id), "outputs");
          if (!url.searchParams.has("path"))
            return json(res, 200, { files: await listFiles(root) });
          // Artifact links are emitted as chat-workspace paths (`outputs/<job>/<file>`),
          // while the directory listing returns paths relative to `outputs`. Accept
          // both, but always resolve inside this chat's own outputs directory.
          const requested = url.searchParams.get("path");
          const relative = requested.startsWith("outputs/")
            ? requested.slice("outputs/".length)
            : requested;
          const filename = workspacePath(root, relative);
          const info = await stat(filename);
          if (!info.isFile()) throw new PlatformError(404, "文件不存在");
          const mediaType = INLINE_TYPES.get(path.extname(filename).toLowerCase());
          // `inline=1` renders the artifact in the conversation (Markdown image
          // or table). It stays ownership-checked: the path is resolved inside
          // this chat's own outputs directory above.
          const inline = url.searchParams.get("inline") === "1" && mediaType !== undefined;
          res.writeHead(200, {
            "content-type": inline ? mediaType : "application/octet-stream",
            "content-length": info.size,
            "x-content-type-options": "nosniff",
            "cache-control": "no-store",
            ...(inline
              ? { "content-disposition": "inline" }
              : { "content-disposition": `attachment; filename*=UTF-8''${encodeURIComponent(path.basename(filename))}` }),
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
    } finally { finishMutation?.(); }
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
