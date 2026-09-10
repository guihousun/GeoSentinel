import { createHash } from "node:crypto";
import { PlatformError } from "../plugins/platform/store.mjs";
import { sidebarIdentity } from "../plugins/platform/sidebar-adapter.mjs";

const sessionToken = (req) => (req.headers.cookie ?? "").split(";").map((s) => s.trim()).find((s) => s.startsWith("geosentinel_session="))?.slice(20);
const key = (req) => createHash("sha256").update(sessionToken(req) || "").digest("hex");
export function developmentAccess(store, hosts, { clock = Date.now, duration = 30 * 60 * 1000 } = {}) {
  const grants = new Map(), attempts = new Map();
  function identity(req) {
    if ((!["GET", "HEAD"].includes(req.method) || req.headers.upgrade) && !req.headers.origin) throw new PlatformError(403, "管理员操作需要同源请求");
    if (req.headers["sec-fetch-site"] === "cross-site") throw new PlatformError(403, "来源无效");
    const user = sidebarIdentity(req, store, hosts); store.requireAdmin(user); return user;
  }
  function confirm(req, password) {
    const user = identity(req), id = key(req), now = clock();
    const rate = attempts.get(user.id) ?? { count: 0, until: now + 60000 };
    if (rate.until <= now) { rate.count = 0; rate.until = now + 60000; }
    attempts.set(user.id, rate);
    if (++rate.count > 5) throw new PlatformError(429, "密码确认过于频繁，请稍后再试");
    const login = store.login(user.username, password);
    store.logout(login.token);
    const expires = now + duration;
    for (const [entry, grant] of grants) if (grant.expires <= now) grants.delete(entry);
    grants.set(id, { userId: user.id, expires });
    store.audit(user.id, "development.unlock", null);
    return { expires };
  }
  function check(req) {
    const user = identity(req), grant = grants.get(key(req));
    if (!grant || grant.userId !== user.id || grant.expires <= clock()) throw new PlatformError(401, "请重新确认管理员密码后进入开发模式");
    return user;
  }
  return { identity, confirm, check, revoke(req) { const user = identity(req); grants.delete(key(req)); store.audit(user.id, "development.lock", null); } };
}
