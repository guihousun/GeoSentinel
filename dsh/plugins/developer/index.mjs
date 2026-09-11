import { announceDevelopmentReady } from "./readiness.mjs";
import path from "node:path";
import { readFileSync } from "node:fs";
import { capabilityCatalog } from "../platform/catalog.mjs";
import { policyPath, readPolicy, writePolicy } from "../platform/capability-policy.mjs";

export const name = "geosentinel-developer";
export const inject = ["connection", "webServer"];

// The administrator manages the ordinary-user capability ceiling from the
// admin-mode workbench. The panel reads the published catalog (product.json +
// shipped skills) and toggles a narrowing policy file the product instance
// re-reads within a few seconds. It can only switch OFF what a release already
// published; it can never add a capability.
export function capabilityHandler() {
  const source = process.env.GEO_ADMIN_DEV_SOURCE;
  const file = policyPath();
  const productFile = path.join(source, "dsh/profile/product.json");
  const skillRoot = path.join(source, "dsh/skills");
  const state = () => {
    const product = JSON.parse(readFileSync(productFile, "utf8"));
    const policy = readPolicy(file);
    const disabled = new Set([...(policy.main ?? []), ...(policy.skills ?? [])]);
    const groups = capabilityCatalog(product, skillRoot).map((group) => ({
      id: group.id,
      label: group.label,
      kind: group.kind,
      items: group.items.map((item) => {
        const name = typeof item === "string" ? item : item.name;
        const roleOff = group.kind === "tool" && group.id !== "main" && (policy.roles?.[group.id] ?? []).includes(name);
        return { name, ...(typeof item === "string" ? {} : { description: item.description }),
          enabled: !(disabled.has(name) || roleOff) };
      }),
    }));
    return { revision: policy.revision, file, groups };
  };
  const apply = (body) => {
    const group = typeof body?.group === "string" ? body.group : "";
    const name = typeof body?.name === "string" ? body.name : "";
    if (group === "" || name === "") throw new Error("缺少 group 或 name");
    const policy = readPolicy(file);
    const enabled = body.enabled !== false;
    const current = state();
    const target = current.groups.find((entry) => entry.id === group);
    if (!target || !target.items.some((item) => item.name === name)) throw new Error("该能力不在已发布范围内");
    if (target.kind === "skill") {
      policy.skills = toggle(policy.skills ?? [], name, enabled);
    } else if (group === "main") {
      policy.main = toggle(policy.main ?? [], name, enabled);
    } else {
      policy.roles = { ...(policy.roles ?? {}), [group]: toggle(policy.roles?.[group] ?? [], name, enabled) };
    }
    writePolicy(file, policy);
    return state();
  };
  return async (req, res) => {
    const host = String(req.headers.host ?? "");
    const send = (status, value) => {
      res.writeHead(status, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
      res.end(JSON.stringify(value));
    };
    if (!/^(127\.0\.0\.1|localhost|\[::1\])(:\d+)?$/.test(host)) return send(403, { error: "仅限本机访问" });
    try {
      if (req.method === "GET") return send(200, state());
      if (req.method !== "POST") return send(405, { error: "请求方式不支持" });
      const chunks = [];
      let size = 0;
      for await (const chunk of req) {
        size += chunk.length;
        if (size > 8192) return send(413, { error: "请求内容过大" });
        chunks.push(chunk);
      }
      const body = JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}");
      return send(200, apply(body));
    } catch (error) {
      return send(error.status ?? 400, { error: error.message });
    }
  };
}

function toggle(list, name, enabled) {
  const next = new Set(list);
  if (enabled) next.delete(name);
  else next.add(name);
  return [...next].sort();
}

export function apply(ctx) {
  if (process.env.GEO_ADMIN_DEVELOPMENT !== "true" || !process.send) return;
  const capabilities = capabilityHandler();
  ctx.effect(() => ctx.webServer.register({ kind: "prefix", path: "/geo/api/development/capabilities", handler: capabilities }));
  ctx.inject(["systemPrompt"], (inner) => inner.systemPrompt.section({ name: "geosentinel:development", order: 100,
    text: () => `You are developing GeoSentinel through its administrator mode. Product source: ${process.env.GEO_ADMIN_DEV_SOURCE}. Reuse the native DSH UI and plugin APIs. Edit GeoSentinel product source and dsh/profile for user-facing changes, not installed upstream dependencies or frozen release snapshots. Native settings belong to this administrator's development home. Read dsh/ADMIN-DEVELOPMENT.md and dsh/RELEASES.md before publishing. Do not copy administrator credentials or host privileges into the ordinary-user profile. Product changes require explicit validated publication; never claim that a source edit is already deployed.` }));
  announceDevelopmentReady(ctx, (message) => process.send?.(message));
  const status = setInterval(() => { if (process.connected) process.send({ type: "geosentinel:development-state", running: (ctx.get("agents")?.roots() ?? []).some((agent) => agent.status === "running") || Boolean(ctx.get("geosentinelResearch")?.running.size) }); }, 1000); status.unref();
  let ending = false;
  const stop = async () => {
    if (ending) return; ending = true; clearInterval(status);
    try {
      for (const agent of ctx.get("agents")?.roots() ?? []) await ctx.get("sessionController")?.cancel({ sessionId: agent.session.header.id });
      const runner = ctx.get("geosentinelResearch");
      for (const chatId of new Set([...(runner?.running.values() ?? [])].map((job) => job.chatId))) await runner.cancelChat(chatId);
    } finally { process.exit(0); }
  };
  process.on("message", (message) => { if (message?.type === "geosentinel:development-exit") void stop(); });
  process.once("disconnect", () => void stop());
}
