import { mkdirSync, writeFileSync, existsSync, lstatSync, symlinkSync, realpathSync, readFileSync } from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { spawn } from "node:child_process";
import { usablePowerShell } from "./shell.mjs";
const source = process.env.GEO_ADMIN_DEV_SOURCE, userId = process.env.GEO_ADMIN_DEV_USER_ID, home = process.env.GEO_ADMIN_DEV_HOME;
if (!process.send || !source || !home || !/^[0-9a-f-]{36}$/.test(userId || "")) throw new Error("Development worker requires the authenticated platform launcher");
const root = path.join(source, "dsh"), profile = path.join(home, "profiles/geosentinel-development"), modules = path.join(profile, "node_modules");
const require = createRequire(path.join(root, "node_modules/@deepseek-ai/dsh-base/package.json")), YAML = require("yaml");
const fork = process.env.GEO_AGENT_TEAMS_DIR || path.join(source, "../GeoSentinel-AgentTeams");
mkdirSync(modules, { recursive: true });
const settingsFile = path.join(home, "settings.yaml");
if (!existsSync(settingsFile)) writeFileSync(settingsFile, YAML.stringify({ "ui-theme": { preference: "dark", fontSize: 16 } }), { flag: "wx" });
for (const [name, target] of [
  ...["@deepseek-ai/dsh-base", "@deepseek-ai/dsh-web-app", "dsh-better-sidebar", "dsh-dream-skin"].map((name) => [name, path.join(root, "node_modules", name)]),
  ["@nanmicoder/dsh-agent-teams", fork], ...["platform", "research", "workbench", "developer"].map((name) => ["@geosentinel/dsh-" + name, path.join(root, "plugins", name)]),
]) {
  const destination = path.join(modules, name); mkdirSync(path.dirname(destination), { recursive: true });
  if (lstatSync(destination, { throwIfNoEntry: false })) { if (realpathSync(destination) !== realpathSync(target)) throw new Error("Development dependency path changed"); }
  else symlinkSync(target, destination, process.platform === "win32" ? "junction" : "dir");
}
const productRows = YAML.parse(readFileSync(path.join(root, "profile/cordis.patch.yml"), "utf8"));
const product = JSON.parse(readFileSync(path.join(root, "profile/product.json"), "utf8"));
const seeds = {
  "package.json": JSON.stringify({ name: "geosentinel-development", private: true, dsh: { profile: { bundles: ["@deepseek-ai/dsh-base", "@deepseek-ai/dsh-web-app", "@nanmicoder/dsh-agent-teams"] } } }),
  "cordis.yml": "[]\n",
  "cordis.patch.yml": YAML.stringify([
    { id: "agent-default-model", config: product.defaultModel },
    { id: "agent-teams", config: { ...productRows.find((row) => row.id === "agent-teams").config, webSurface: true } },
    { insert: [
      { id: "geosentinel-platform", name: "@geosentinel/dsh-platform" },
      { id: "geosentinel-research", name: "@geosentinel/dsh-research" },
      { id: "geosentinel-workbench", name: "@geosentinel/dsh-workbench" },
      { id: "better-sidebar", name: "dsh-better-sidebar" },
      { id: "dream-skin", name: "dsh-dream-skin" },
    ] },
  ]),
};
for (const [name, content] of Object.entries(seeds)) if (!existsSync(path.join(profile, name))) writeFileSync(path.join(profile, name), content, { flag: "wx" });
const overlay = path.join(profile, "gateway.patch.yml");
writeFileSync(overlay, YAML.stringify([
  ...(process.platform === "win32" ? [{ id: "pwsh-sandbox", config: { pwshPath: usablePowerShell() } }] : []),
  { id: "directory-picker", disabled: true },
  { id: "web-runtime", config: { openBrowser: false, printUrl: false, surfaceContext: true, trustedHosts: [] } },
  { insert: [{ id: "geosentinel-developer", name: "@geosentinel/dsh-developer" }, { id: "development-directory-host", name: "@deepseek-ai/dsh-host-directory-picker-browse" }] },
]));
const env = { ...process.env, DSH_HOME: home };
for (const key of Object.keys(env)) if (key.startsWith("GEO_") && !["GEO_GEE_CREDENTIALS", "GEO_GEE_PROXY", "GEO_GIS_IMAGE"].includes(key)) delete env[key];
Object.assign(env, { GEO_ADMIN_DEVELOPMENT: "true", GEO_ADMIN_DEV_SOURCE: source, GEO_ADMIN_DEV_USER_ID: userId });
const child = spawn(process.execPath, [path.join(root, "node_modules/@deepseek-ai/dsh/lib/bin.js"), "--profile", "geosentinel-development", "--patch", overlay, "--host", "127.0.0.1", "--port", "0", "--no-open"], { cwd: profile, env, windowsHide: true, stdio: ["ignore", "ignore", "inherit", "ipc"] });
child.on("message", (message) => { if (process.connected) process.send(message); });
process.on("message", (message) => { if (message?.type === "geosentinel:development-exit" && child.connected) child.send(message); });
process.once("disconnect", () => { if (child.connected) child.send({ type: "geosentinel:development-exit" }); else child.kill(); });
child.once("exit", (code) => process.exit(code || 0));
child.once("error", () => process.exit(1));
