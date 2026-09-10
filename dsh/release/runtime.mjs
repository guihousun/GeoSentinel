import { mkdirSync, writeFileSync, symlinkSync, lstatSync, realpathSync, readdirSync } from "node:fs";
import { spawn } from "node:child_process";
import path from "node:path";
import { createRequire } from "node:module";
import { readJson, validateProfile } from "./manager.mjs";
import { stringifyProfile } from "./profile-schema.mjs";
import { readFile } from "node:fs/promises";
const require = createRequire(import.meta.resolve("@deepseek-ai/dsh-base"));

/**
 * Resolve one dependency inside a frozen snapshot.
 *
 * `pnpm install` links the profile's bundle closure at the top level, so a
 * package that the product profile declares but no bundle imports — the remote
 * MCP client is exactly that — only exists in the content-addressed store. Link
 * whichever form is present; fail loudly when neither is, because a silently
 * missing dependency turns into a plugin row that loads as nothing.
 */
function dependencyPath(dsh, name) {
  const direct = path.join(dsh, "node_modules", name);
  if (lstatSync(direct, { throwIfNoEntry: false })) return direct;
  const store = path.join(dsh, "node_modules/.pnpm");
  const flat = name.replace("/", "+");
  const entry = (readdirSync(store, { withFileTypes: true }) ?? [])
    .filter((candidate) => candidate.isDirectory() && (candidate.name.startsWith(flat + "@") || candidate.name.startsWith(flat + "_")))
    .map((candidate) => candidate.name)
    .sort()[0];
  if (!entry) throw new Error(`发布快照缺少依赖：${name}`);
  return path.join(store, entry, "node_modules", name);
}

export async function launchRelease(manager, id, { home, args, preview, env = process.env }) {
  const manifest = await manager.verify(id), root = manager.releaseRoot(id), app = path.join(root, "app"), dsh = path.join(app, "dsh");
  const validation = await readJson(path.join(root, "validation.json"));
  if (!validation?.passed) throw new Error("版本尚未通过验证");
  const profileName = "geosentinel-" + id, profile = path.join(home, "profiles", profileName), modules = path.join(profile, "node_modules");
  mkdirSync(modules, { recursive: true });
  for (const [name, target] of [["@deepseek-ai/dsh-base", path.join(dsh, "node_modules/@deepseek-ai/dsh-base")], ["@deepseek-ai/dsh-web-app", path.join(dsh, "node_modules/@deepseek-ai/dsh-web-app")], 
      // Remote MCP servers are declared as rows in the product profile, so the
      // client bridge has to be resolvable from the release profile; without
      // this link the rows load as nothing and their tools never appear.
      ["@deepseek-ai/dsh-mcp-client", dependencyPath(dsh, "@deepseek-ai/dsh-mcp-client")],
      ["@nanmicoder/dsh-agent-teams", path.join(app, "agentteams")], ["@changfenhuang/dsh-genui", path.join(dsh, "node_modules/@changfenhuang/dsh-genui")], ["dsh-file-upload", path.join(dsh, "node_modules/dsh-file-upload")], ...["platform", "research", "workbench"].map((name) => ["@geosentinel/dsh-" + name, path.join(dsh, "plugins", name)])]) {
    const destination = path.join(modules, name); mkdirSync(path.dirname(destination), { recursive: true });
    if (lstatSync(destination, { throwIfNoEntry: false })) { if (realpathSync(destination) !== realpathSync(target)) throw new Error("发布 profile 链接不匹配"); }
    else symlinkSync(target, destination, process.platform === "win32" ? "junction" : "dir");
  }
  const rows = validateProfile(await readFile(path.join(dsh, "profile/cordis.patch.yml"), "utf8"));
  if (manifest.product.roleTools) rows.find((row) => row.id === "agent-teams").config.roleTools = manifest.product.roleTools;
  rows.push({ id: "agent-default-model", config: manifest.product.defaultModel });
  writeFileSync(path.join(profile, "package.json"), JSON.stringify({ name: "geosentinel-release", private: true, dsh: { profile: { bundles: ["@deepseek-ai/dsh-base", "@deepseek-ai/dsh-web-app", "@nanmicoder/dsh-agent-teams"] } } }));
  writeFileSync(path.join(profile, "cordis.yml"), "[]\n");
  // Re-serialize in the entry-list dialect so a `!!js` row value — the remote
  // MCP rows read their credential from the environment this way — survives as
  // an expression instead of becoming a literal string. See profile-schema.mjs.
  writeFileSync(path.join(profile, "cordis.patch.yml"), stringifyProfile(rows));
  const childEnv = { ...env, DSH_HOME: home, GEO_DATA_DIR: preview ? path.join(home, "geosentinel") : env.GEO_DATA_DIR || path.join(home, "geosentinel"), GEO_RELEASE_ID: id,
    GEO_DEPLOY_SOURCE: manager.source, GEO_RELEASE_DIR: manager.directory, GEO_GIS_IMAGE: validation.image,
    GEO_MONITOR_ENABLED: String(manifest.product.monitorEnabled), GEO_AGENT_TEAMS_DIR: manager.fork };
  if (preview) Object.assign(childEnv, { GEO_PREVIEW_RELEASE: id, GEO_PREVIEW_PASSWORD: preview.password, GEO_PREVIEW_TOKEN: preview.token,
    GEO_PREVIEW_EXPIRES: String(preview.expires), GEO_ALLOWED_HOSTS: "127.0.0.1:8513,localhost:8513", GEO_MONITOR_ENABLED: "false", GEO_MONITOR_DIR: path.join(home, "monitor") });
  return spawn(process.execPath, [path.join(dsh, "node_modules/@deepseek-ai/dsh/lib/bin.js"), "--profile", profileName, ...args], { cwd: profile,
    env: childEnv, windowsHide: true, stdio: ["ignore", "inherit", "inherit", "ipc"] });
}
export async function healthy(port, id, timeout = 30000) {
  const until = Date.now() + timeout;
  while (Date.now() < until) {
    try { const response = await fetch("http://127.0.0.1:" + port + "/geo/api/health", { signal: AbortSignal.timeout(1500) }); const value = await response.json(); if (value.status === "ok" && value.release === id) return true; } catch {}
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  return false;
}
