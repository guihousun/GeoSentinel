import {
  mkdirSync,
  existsSync,
  writeFileSync,
  readFileSync,
  symlinkSync,
  realpathSync,
  lstatSync,
} from "node:fs";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { startupOptions, inspectEndpoint } from "./startup-check.mjs";

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const envFile = process.env.GEO_ENV_FILE ?? path.join(root, ".env");
if (existsSync(envFile)) process.loadEnvFile(envFile);
let startup;
try {
  startup = startupOptions(process.argv.slice(2));
  if (!startup.args.some((arg) => ["--help", "-h", "--version", "-V", "--dump-config", "--dump-default-config"].includes(arg))) {
    const endpoint = await inspectEndpoint(startup);
    if (endpoint.state === "running") {
      console.log(`GeoSentinel 已在运行：${endpoint.url}\n无需重复启动，本次未修改或停止已有服务。\n需要重启本项目时：.\\scripts\\restart.ps1 -Port ${startup.port}`);
      process.exit(0);
    }
    if (endpoint.state === "occupied") {
      console.error(`地址 ${endpoint.url} 已被占用，或已有服务尚未就绪。\n未启动新实例，也未停止任何进程。请检查已有服务，或指定其他 --port；更换端口时同步配置 .env 的 GEO_ALLOWED_HOSTS。`);
      process.exit(1);
    }
  }
} catch (error) {
  console.error(`GeoSentinel 启动检查失败：${error.message}`);
  process.exit(1);
}
if (!process.env.GEO_ALLOWED_HOSTS)
  process.env.GEO_ALLOWED_HOSTS = `127.0.0.1:${startup.port},localhost:${startup.port}`;
if (!process.env.DEEPSEEK_API_KEY && process.env.DeepSeek_API_KEY)
  process.env.DEEPSEEK_API_KEY = process.env.DeepSeek_API_KEY;
if (!process.env.DEEPSEEK_BASE_URL && process.env.DeepSeek_Coding_URL)
  process.env.DEEPSEEK_BASE_URL = process.env.DeepSeek_Coding_URL;
const home = path.resolve(
  process.env.GEO_DSH_HOME ?? path.join(root, ".runtime/home"),
);
const profile = path.join(home, "profiles/geosentinel");
const fork = path.resolve(
  process.env.GEO_AGENT_TEAMS_DIR ??
    path.join(root, "../../GeoSentinel-AgentTeams"),
);
if (!existsSync(path.join(fork, "lib/index.js")))
  throw new Error(
    "Build the GeoSentinel AgentTeams fork first; set GEO_AGENT_TEAMS_DIR to its checkout.",
  );
mkdirSync(profile, { recursive: true });
const modules = path.join(profile, "node_modules");
mkdirSync(modules, { recursive: true });
function link(name, target) {
  const destination = path.join(modules, name);
  mkdirSync(path.dirname(destination), { recursive: true });
  if (lstatSync(destination, { throwIfNoEntry: false })) {
    if (realpathSync(destination) !== realpathSync(target))
      throw new Error(
        `Managed profile link is stale: ${name}. Use a new GEO_DSH_HOME or repair this administrator-owned link.`,
      );
  } else
    symlinkSync(
      target,
      destination,
      process.platform === "win32" ? "junction" : "dir",
    );
}
for (const name of ["@deepseek-ai/dsh-base", "@deepseek-ai/dsh-web-app"])
  link(name, path.join(root, "node_modules", name));
link("@nanmicoder/dsh-agent-teams", fork);
for (const name of ["platform", "research", "workbench"])
  link(`@geosentinel/dsh-${name}`, path.join(root, "plugins", name));
writeFileSync(
  path.join(profile, "package.json"),
  JSON.stringify(
    {
      name: "geosentinel-managed-profile",
      private: true,
      dsh: {
        profile: {
          bundles: [
            "@deepseek-ai/dsh-base",
            "@deepseek-ai/dsh-web-app",
            "@nanmicoder/dsh-agent-teams",
          ],
        },
      },
    },
    null,
    2,
  ),
);
writeFileSync(path.join(profile, "cordis.yml"), "[]\n");
writeFileSync(
  path.join(profile, "cordis.patch.yml"),
  readFileSync(path.join(root, "profile/cordis.patch.yml")),
);
const command = path.join(root, "node_modules/@deepseek-ai/dsh/lib/bin.js");
const child = spawn(
  process.execPath,
  [command, "--profile", "geosentinel", ...startup.args],
  {
    // The managed launcher supplies trusted bootstrap variables via inheritance.
    // DSH must not re-read the product .env as an ordinary research-project layer.
    cwd: profile,
    stdio: "inherit",
    env: {
      ...process.env,
      DSH_HOME: home,
      GEO_DATA_DIR: process.env.GEO_DATA_DIR ?? path.join(home, "geosentinel"),
    },
  },
);
for (const signal of ["SIGINT", "SIGTERM"])
  process.on(signal, () => child.kill(signal));
child.on("exit", (code) => {
  process.exitCode = code ?? 1;
});
