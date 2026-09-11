import { migrateBundleInserts } from "./bundle-compat.mjs";
import { migrateUploadBundle, migrateLegacyTeams } from "./upload-compat.mjs";
import { mkdirSync, writeFileSync, existsSync, lstatSync, symlinkSync, realpathSync, readFileSync } from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { spawn } from "node:child_process";
import { usablePowerShell } from "./shell.mjs";
const source = process.env.GEO_ADMIN_DEV_SOURCE, userId = process.env.GEO_ADMIN_DEV_USER_ID, home = process.env.GEO_ADMIN_DEV_HOME;
if (!process.send || !source || !home || !/^[0-9a-f-]{36}$/.test(userId || "")) throw new Error("Development worker requires the authenticated platform launcher");
const root = path.join(source, "dsh"), profile = path.join(home, "profiles/geosentinel-development"), modules = path.join(profile, "node_modules");
mkdirSync(modules, { recursive: true });
// Composition first: it carries the settings document too, and it is the only place
// that knows the entry-list dialect (see profile.mjs — the plain `yaml` package is not
// linked in a 0.1.5 install, so requiring it here used to fail the whole worker).
const product = JSON.parse(readFileSync(path.join(root, "profile/product.json"), "utf8"));
const { developmentFiles } = await import("./profile.mjs");
const { seeds, overlay: overlayText } = developmentFiles({ product, platform: process.platform,
  pwshPath: process.platform === "win32" ? usablePowerShell() : undefined });
const settingsFile = path.join(home, "settings.yaml");
if (!existsSync(settingsFile)) writeFileSync(settingsFile, seeds["settings.yaml"], { flag: "wx" });
for (const [name, target] of [
  ...["@deepseek-ai/dsh-base", "@deepseek-ai/dsh-web-app", "dsh-better-sidebar", "dsh-dream-skin"].map((name) => [name, path.join(root, "node_modules", name)]),
  ...["platform", "research", "workbench", "developer"].map((name) => ["@geosentinel/dsh-" + name, path.join(root, "plugins", name)]),
]) {
  const destination = path.join(modules, name); mkdirSync(path.dirname(destination), { recursive: true });
  if (lstatSync(destination, { throwIfNoEntry: false })) { if (realpathSync(destination) !== realpathSync(target)) throw new Error("Development dependency path changed"); }
  else symlinkSync(target, destination, process.platform === "win32" ? "junction" : "dir");
}
for (const [name, content] of Object.entries(seeds)) if (name !== "settings.yaml" && !existsSync(path.join(profile, name))) writeFileSync(path.join(profile, name), content, { flag: "wx" });
migrateLegacyTeams(profile);
migrateUploadBundle(profile, root);
migrateBundleInserts(profile, root);
const overlay = path.join(profile, "gateway.patch.yml");
writeFileSync(overlay, overlayText);
const env = { ...process.env, DSH_HOME: home };
for (const key of Object.keys(env)) if (key.startsWith("GEO_") && !["GEO_GEE_CREDENTIALS", "GEO_GEE_PROXY", "GEO_GIS_IMAGE"].includes(key)) delete env[key];
Object.assign(env, { GEO_ADMIN_DEVELOPMENT: "true", GEO_ADMIN_DEV_SOURCE: source, GEO_ADMIN_DEV_USER_ID: userId });
const child = spawn(process.execPath, [path.join(root, "node_modules/@deepseek-ai/dsh/lib/bin.js"), "--profile", "geosentinel-development", "--patch", overlay, "--host", "127.0.0.1", "--port", "0", "--no-open"], { cwd: profile, env, windowsHide: true, stdio: ["ignore", "ignore", "inherit", "ipc"] });
child.on("message", (message) => { if (process.connected) process.send(message); });
process.on("message", (message) => { if (message?.type === "geosentinel:development-exit" && child.connected) child.send(message); });
process.once("disconnect", () => { if (child.connected) child.send({ type: "geosentinel:development-exit" }); else child.kill(); });
child.once("exit", (code) => process.exit(code || 0));
child.once("error", () => process.exit(1));
