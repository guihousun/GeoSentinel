import path from "node:path";
import { randomBytes } from "node:crypto";
import { PlatformStore } from "../plugins/platform/store.mjs";
import { RuntimeLedger } from "../plugins/platform/runtime.mjs";

// This adapter is loaded only inside the gateway-owned, per-administrator process.
export function applyDevelopmentPlatform(ctx) {
  if (!process.send || !process.env.GEO_ADMIN_DEV_USER_ID) throw new Error("Missing authenticated development worker");
  const store = new PlatformStore(path.join(process.env.DSH_HOME, "geosentinel-development"));
  let user = store.db.prepare("SELECT id,username,admin FROM users LIMIT 1").get();
  if (!user) user = store.bootstrapAdmin("developer", randomBytes(32).toString("hex"));
  const project = store.listProjects(user)[0] ?? store.createProject(user, "开发验证");
  store.db.exec("CREATE TABLE IF NOT EXISTS development_sessions(native_id TEXT PRIMARY KEY,chat_id TEXT NOT NULL)");
  const runtime = new RuntimeLedger(store);
  const identityForAgent = (agent) => {
    const nativeId = agent?.session?.header?.id;
    if (!nativeId) throw new Error("Development session missing");
    let row = store.db.prepare("SELECT chat_id FROM development_sessions WHERE native_id=?").get(nativeId);
    if (!row) { const chat = store.createChat(user, project.id, "开发验证"); row = { chat_id: chat.id }; store.db.prepare("INSERT INTO development_sessions VALUES(?,?)").run(nativeId, chat.id); }
    return { user, chatId: row.chat_id, projectId: project.id, root: store.chatRoot(user, row.chat_id) };
  };
  ctx.provide("geosentinelPlatform", { development: true, store, runtime, identityForAgent, ensureResearchExecution: async () => {} });
  ctx.on("dispose", () => { runtime.close(); store.close(); });
}
