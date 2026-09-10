import { mkdirSync, existsSync, readFileSync, writeFileSync, lstatSync, realpathSync, symlinkSync } from "node:fs";
import { spawn } from "node:child_process";
import { parseEnv } from "node:util";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { inspectEndpoint } from "./startup-check.mjs";

export function adminOptions(args) {
  let port = 8514, noOpen = false;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--no-open") noOpen = true;
    else if (args[i] === "--port") port = Number(args[++i]);
    else if (args[i].startsWith("--port=")) port = Number(args[i].slice(7));
    else throw new Error("管理员入口只支持 --port 和 --no-open，不允许公网绑定或增加可信主机");
  }
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error("无效端口");
  return { host: "127.0.0.1", port, noOpen };
}

export async function startAdmin(args) {
  const options = adminOptions(args), endpoint = await inspectEndpoint(options);
  if (endpoint.state !== "free") throw new Error("管理员端口已占用；未停止已有服务。请使用启动终端中的原生登录链接。");
  const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
  const envFile = process.env.GEO_ENV_FILE ?? path.join(root, ".env");
  const config = existsSync(envFile) ? parseEnv(readFileSync(envFile, "utf8")) : {};
  const home = path.resolve(process.env.GEO_ADMIN_HOME || config.GEO_ADMIN_HOME || path.join(root, ".runtime/admin-home"));
  const productionHome = path.resolve(process.env.GEO_DSH_HOME || config.GEO_DSH_HOME || path.join(root, ".runtime/home"));
  const canonical = (value) => { const actual = existsSync(value) ? realpathSync(value) : value; return process.platform === "win32" ? actual.toLowerCase() : actual; };
  const dev = canonical(home), prod = canonical(productionHome);
  if (dev === prod || dev.startsWith(prod + path.sep) || prod.startsWith(dev + path.sep)) throw new Error("管理员开发必须使用独立 DSH home");
  const profile = path.join(home, "profiles/geosentinel-admin"), modules = path.join(profile, "node_modules");
  mkdirSync(modules, { recursive: true });
  for (const name of ["@deepseek-ai/dsh-base", "@deepseek-ai/dsh-web-app"]) {
    const destination = path.join(modules, name), target = path.join(root, "node_modules", name);
    mkdirSync(path.dirname(destination), { recursive: true });
    if (lstatSync(destination, { throwIfNoEntry: false })) {
      if (realpathSync(destination) !== realpathSync(target)) throw new Error("管理员 profile 依赖路径已变化，请核对迁移目录");
    } else symlinkSync(target, destination, process.platform === "win32" ? "junction" : "dir");
  }
  const developerLink = path.join(modules, "@geosentinel/dsh-developer");
  mkdirSync(path.dirname(developerLink), { recursive: true });
  if (!lstatSync(developerLink, { throwIfNoEntry: false })) symlinkSync(path.join(root, "plugins/developer"), developerLink, process.platform === "win32" ? "junction" : "dir");
  // Seed once: native Settings and creation mode own later profile edits.
  const seeds = { "package.json": JSON.stringify({ name: "geosentinel-local-admin", private: true, dsh: { profile: { bundles: ["@deepseek-ai/dsh-base", "@deepseek-ai/dsh-web-app"] } } }, null, 2), "cordis.yml": "[]\n", "cordis.patch.yml": "[]\n" };
  for (const [name, content] of Object.entries(seeds)) if (!existsSync(path.join(profile, name))) writeFileSync(path.join(profile, name), content, { flag: "wx" });
  // The managed extension is a separate overlay; preserve native administrator edits.
  const overlay = path.join(profile, "geosentinel-developer.patch.yml");
  writeFileSync(overlay, "- insert:\n    - id: geosentinel-developer\n      name: '@geosentinel/dsh-developer'\n");
  const env = { ...process.env, DSH_HOME: home };
  env.DEEPSEEK_API_KEY ||= config.DEEPSEEK_API_KEY || config.DeepSeek_API_KEY || "";
  env.DEEPSEEK_BASE_URL ||= config.DEEPSEEK_BASE_URL || config.DeepSeek_Coding_URL || "https://api.deepseek.com";
  for (const key of Object.keys(env)) if (key.startsWith("GEO_") || key === "DSH_WEB_URL") delete env[key];
  console.log("本机管理员原生 DSH：" + options.port + "。拥有当前系统账号的文件与执行权限，不受平台 Docker 配额限制。\n开发目录：" + path.dirname(root) + "\n配置与历史独立保存；请勿将此端口映射到公网。");
  const child = spawn(process.execPath, [path.join(root, "node_modules/@deepseek-ai/dsh/lib/bin.js"), "--profile", "geosentinel-admin", "--patch", overlay, "--host", "127.0.0.1", "--port", String(options.port), ...(options.noOpen ? ["--no-open"] : [])], { cwd: profile, env, stdio: "inherit", windowsHide: true });
  for (const signal of ["SIGINT", "SIGTERM"]) process.on(signal, () => child.kill(signal));
  child.on("error", (error) => { console.error(error.message); process.exitCode = 1; });
  child.on("exit", (code) => { process.exitCode = code ?? 1; });
}
