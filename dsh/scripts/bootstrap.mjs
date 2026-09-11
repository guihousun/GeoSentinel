#!/usr/bin/env node
/**
 * Install (or repair) this checkout on a fresh machine, then tell you the one
 * command that starts it. Idempotent: run it as often as you like — every step
 * checks first and skips what is already done.
 *
 *   git clone <repo> && cd <repo>
 *   node dsh/scripts/bootstrap.mjs            # 装到可启动；缺凭据会明确停下并告诉你填什么
 *   node dsh/scripts/bootstrap.mjs --check    # 只体检，不改任何文件（JSON 输出）
 *   node dsh/scripts/bootstrap.mjs --dry-run  # 打印将要执行的步骤
 *
 * Options: --skip-install --skip-env --skip-image --skip-prepare --skip-admin
 *          --username <name> --port <n> --with-share-data [--share-root <dir>] --json
 *
 * What it deliberately does NOT do: write credentials, download shared data unless
 * asked, or start the server. Publication (frozen snapshots, preview, switch) stays
 * with `dsh/tools/release.mjs` — see RELEASES.md. On a machine with no active
 * release, the first `scripts/start.mjs` creates the bootstrap snapshot itself
 * (release/supervisor.mjs), so there is no second install path to learn.
 */
import { spawnSync } from "node:child_process";
import { copyFileSync, existsSync, readFileSync } from "node:fs";
import { randomBytes } from "node:crypto";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));   // <repo>/dsh
const repo = path.dirname(root);
const argv = process.argv.slice(2);
const flag = (name, fallback) => {
  const inline = argv.find((value) => value.startsWith(`--${name}=`));
  if (inline) return inline.slice(name.length + 3);
  const index = argv.indexOf("--" + name);
  if (index === -1) return fallback;
  const next = argv[index + 1];
  return next === undefined || next.startsWith("--") ? true : next;
};
const has = (name) => argv.includes("--" + name) || argv.some((value) => value.startsWith(`--${name}=`));
if (has("help")) {
  // Write-through callback instead of console.log + exit: exiting first would drop
  // the text whenever stdout is a pipe rather than a terminal.
  process.stdout.write(readFileSync(fileURLToPath(import.meta.url), "utf8").split("*/")[0].replace(/^\/\*\*?/, "").trim() + "\n", () => process.exit(0));
}

const check = has("check");
const dryRun = has("dry-run");
const asJson = has("json") || check;
const username = String(flag("username", "admin"));
const port = Number(flag("port", 8511)) || 8511;
const envFile = process.env.GEO_ENV_FILE ?? path.join(root, ".env");
const steps = [];
const report = { platform: process.platform, root, check, dryRun, steps };

const say = (text) => { if (!asJson) console.log(text); };
// Exiting right after console.log truncates the output when stdout is a pipe (CI,
// `pnpm doctor | …`): the queued writes never land. Flush both streams first.
const stop = async (code) => {
  await Promise.all([new Promise((resolve) => process.stdout.write("", resolve)), new Promise((resolve) => process.stderr.write("", resolve))]);
  process.exit(code);
};
const step = (name, status, detail) => { steps.push({ name, status, detail }); if (!asJson) console.log(`  ${status === "ok" ? "✓" : status === "skip" ? "·" : status === "todo" ? "→" : "✗"} ${name}${detail ? " — " + detail : ""}`); };
const run = (command, args, { capture = false, optional = false, cwd = root } = {}) => {
  if (dryRun && !capture) { say(`  → 将执行: ${command} ${args.join(" ")}`); return { status: 0, stdout: "", dryRun: true }; }
  const result = spawnSync(command, args, { cwd, shell: process.platform === "win32", encoding: "utf8", windowsHide: true, stdio: capture ? ["ignore", "pipe", "pipe"] : "inherit", timeout: 30 * 60 * 1000 });
  if (result.status !== 0 && !optional) throw new Error(`${command} ${args.join(" ")} 失败（退出码 ${result.status ?? "未知"}）`);
  return result;
};
const version = (command, args) => {
  const result = spawnSync(command, args, { shell: process.platform === "win32", encoding: "utf8", windowsHide: true, timeout: 30000 });
  return result.status === 0 ? result.stdout.trim() : null;
};

if (!asJson) console.log(`GeoSentinel bootstrap${check ? "（体检模式，不修改任何文件）" : dryRun ? "（预演，不实际执行）" : ""}\n仓库: ${repo}`);

// ---------------------------------------------------------------- 1. 前置条件 --
const pkg = JSON.parse(readFileSync(path.join(root, "package.json"), "utf8"));
const wantedNode = Number(String(pkg.engines?.node ?? ">=24").replace(/[^0-9]/g, "")) || 24;
const nodeMajor = Number(process.versions.node.split(".")[0]);
step(`Node.js ≥ ${wantedNode}`, nodeMajor >= wantedNode ? "ok" : "fail", `当前 v${process.versions.node}${nodeMajor >= wantedNode ? "" : ` —— 请安装 Node ${wantedNode} LTS（engines 要求）`}`);

const wantedPnpm = String(pkg.packageManager ?? "").replace(/^pnpm@/, "");
const pnpmVersion = version("pnpm", ["--version"]);
step(`pnpm ${wantedPnpm || "(任意近期版本)"}`, pnpmVersion ? (wantedPnpm && pnpmVersion !== wantedPnpm ? "warn" : "ok") : "fail",
  pnpmVersion ? `当前 ${pnpmVersion}${wantedPnpm && pnpmVersion !== wantedPnpm ? ` —— packageManager 固定为 ${wantedPnpm}，版本不同可能改动 lockfile` : ""}` : "未找到 pnpm：npm i -g pnpm");

const gitVersion = version("git", ["--version"]);
step("git", gitVersion ? "ok" : "warn", gitVersion ?? "未找到 git（升级与补丁流程需要）");

const dockerVersion = version("docker", ["version", "--format", "{{.Server.Version}}"]);
step("Docker 引擎（Linux 容器）", dockerVersion ? "ok" : "fail", dockerVersion ?? "Docker Desktop 未运行或未安装 —— GIS 计算与镜像构建都需要它");

const envExists = existsSync(envFile);
step(".env 配置文件", envExists ? "ok" : "todo", envExists ? path.relative(repo, envFile) : `将从 .env.example 生成 ${path.relative(repo, envFile)}`);

if (envExists) {
  process.loadEnvFile(envFile);
  const apiKey = process.env.DEEPSEEK_API_KEY || process.env.DeepSeek_API_KEY;
  const geeProject = process.env.GEE_DEFAULT_PROJECT_ID;
  const credentials = process.env.GEO_GEE_CREDENTIALS;
  step("DEEPSEEK_API_KEY", apiKey ? "ok" : "todo", apiKey ? "已配置（不打印）" : "登录与管理功能所需的模型凭据");
  step("GEE_DEFAULT_PROJECT_ID", geeProject ? "ok" : "todo", geeProject ? "已配置" : "固定 GEE 获取容器需要");
  step("GEO_GEE_CREDENTIALS", credentials && existsSync(credentials) ? "ok" : "todo",
    credentials ? (existsSync(credentials) ? "凭据文件存在（不打印路径）" : "配置的凭据文件不存在") : "指向本机 Earth Engine 凭据文件");
}
const blockers = steps.filter((item) => item.status === "fail");
const todos = steps.filter((item) => item.status === "todo");

if (check) {
  console.log(JSON.stringify({ ok: blockers.length === 0, blockers: blockers.length, todos: todos.length, report }, null, 2));
  await stop(blockers.length ? 1 : 0);
}
if (blockers.length && !dryRun) {
  console.error(`\n无法继续：${blockers.map((item) => item.name).join("、")}。修好后重跑本命令。`);
  await stop(1);
}

// ------------------------------------------------------------------ 2. 依赖 --
say("\n[2/6] 依赖");
const lock = path.join(root, "pnpm-lock.yaml");
if (has("skip-install")) step("pnpm install", "skip", "按 --skip-install 跳过");
else if (dryRun || !existsSync(path.join(root, "node_modules"))) { step("pnpm install --frozen-lockfile", dryRun ? "todo" : "ok", "重建依赖树（首次较慢）"); run("pnpm", ["install", "--frozen-lockfile"]); }
else {
  const stamp = existsSync(path.join(root, "node_modules", ".modules.yaml")) ? path.join(root, "node_modules", ".modules.yaml") : lock;
  const stale = existsSync(lock) && existsSync(stamp) && readFileSync(stamp, "utf8").length < readFileSync(lock, "utf8").length;
  if (stale) { step("pnpm install --frozen-lockfile", "ok", "lockfile 比安装记录新，重新同步"); run("pnpm", ["install", "--frozen-lockfile"]); }
  else step("pnpm install", "skip", "node_modules 已存在（需要重建时加 --skip-install=false 或手动 pnpm install）");
}

// -------------------------------------------------------------------- 3. 配置 --
say("\n[3/6] 配置");
if (envExists || has("skip-env")) step(".env", "skip", envExists ? "已存在，不覆盖" : "按 --skip-env 跳过");
else if (dryRun) step(".env", "todo", "将从 .env.example 复制");
else {
  copyFileSync(path.join(root, ".env.example"), envFile);
  step(".env", "ok", "已从 .env.example 生成 —— 现在填写凭据，然后重跑本命令");
}

if (envExists && todos.length) {
  console.log(`\n还需要你手工填写的配置（${path.relative(repo, envFile)}）：`);
  for (const item of todos) console.log(`  - ${item.name}: ${item.detail}`);
  console.log("\n填好后重跑 node dsh/scripts/bootstrap.mjs。凭据只放这个文件，不要提交到 Git。");
  await stop(1);
}
if (!envExists && !dryRun) { console.log("\n已生成 .env：填好上面的必填项后重跑本命令即可继续。"); await stop(1); }

// ---------------------------------------------------------------- 4. GIS 镜像 --
say("\n[4/6] GIS 镜像与冻结快照");
const image = process.env.GEO_GIS_IMAGE ?? "geosentinel-gis:0.1";
const imageId = run("docker", ["image", "inspect", image, "--format", "{{.Id}}"], { capture: true, optional: true }).stdout?.trim();
if (has("skip-image")) step(`镜像 ${image}`, "skip", "按 --skip-image 跳过");
else if (imageId) step(`镜像 ${image}`, "skip", `已存在 ${imageId.slice(0, 19)}（重建：docker build --provenance=false -t ${image} docker）`);
else if (dryRun) step(`构建镜像 ${image}`, "todo", "docker build（约 4 GB，首次较慢）");
else { step(`构建镜像 ${image}`, "ok", "docker build --provenance=false"); run("docker", ["build", "--provenance=false", "-t", image, "docker"]); }

if (has("skip-prepare")) step("冻结并校验首个版本", "skip", "按 --skip-prepare 跳过");
else if (dryRun) step("冻结并校验首个版本", "todo", "node tools/release.mjs prepare（隔离安装 + 语法 + 测试 + 镜像）");
else {
  step("冻结并校验首个版本", "ok", "release.mjs prepare");
  run(process.execPath, [path.join(root, "tools", "release.mjs"), "prepare"]);
}

// ------------------------------------------------------------------ 5. 体检 --
say("\n[5/6] 环境体检（scripts/check-env.mjs）");
if (dryRun) step("check-env", "todo", "node scripts/check-env.mjs");
else {
  const result = run(process.execPath, [path.join(root, "scripts", "check-env.mjs")], { capture: true, optional: true });
  const parsed = (() => { try { return JSON.parse(result.stdout); } catch { return null; } })();
  const failed = parsed ? parsed.checks.filter((entry) => !entry.ok) : [];
  step("check-env", result.status === 0 ? "ok" : "warn", result.status === 0 ? "全部通过" : `未通过 ${failed.length} 项：${failed.map((entry) => entry.name).join("、")}`);
  if (result.status !== 0 && !parsed) console.error(result.stdout || result.stderr);
}

// ---------------------------------------------------------------- 6. 管理员 --
say("\n[6/6] 管理员账号");
const storeRoot = process.env.GEO_DATA_DIR ?? path.join(process.env.GEO_DSH_HOME ?? path.join(root, ".runtime/home"), "geosentinel");
if (has("skip-admin")) step("管理员账号", "skip", "按 --skip-admin 跳过");
else if (dryRun) step("管理员账号", "todo", `在 ${path.relative(repo, storeRoot)} 建立首个管理员`);
else {
  const { PlatformStore } = await import("../plugins/platform/store.mjs");
  const store = new PlatformStore(storeRoot);
  const existing = store.db.prepare("SELECT count(*) AS n FROM users").get().n;
  if (existing) step("管理员账号", "skip", `已有 ${existing} 个账号，不新建（重置口令：node scripts/admin.mjs <用户名> 前先自行清库）`);
  else {
    const password = process.env.GEO_BOOTSTRAP_PASSWORD || randomBytes(12).toString("base64url");
    const generated = !process.env.GEO_BOOTSTRAP_PASSWORD;
    store.bootstrapAdmin(username, password);
    step("管理员账号", "ok", `${username} 已建立`);
    if (generated && !asJson) {
      console.log(`\n  管理员初始口令（只显示这一次，请立刻保存到密码管理器）：${password}`);
      console.log("  首次登录后可在界面修改；本脚本不把它写进任何文件。");
    }
  }
}

// ------------------------------------------------------------ 7. 共享数据（可选） --
if (has("with-share-data")) {
  say("\n[可选] 共享数据");
  const shareRoot = String(flag("share-root", "")) || (process.env.GEO_SHARE_DIRS ?? "").split(";").map((chunk) => chunk.split("=")[1]).filter(Boolean)[0] || process.env.GEO_SHARE_DIR;
  if (!shareRoot) step("共享数据", "warn", "未配置 GEO_SHARE_DIR / GEO_SHARE_DIRS，也没有 --share-root");
  else if (dryRun) step("共享数据", "todo", `node tools/fetch-share-data.mjs --root ${shareRoot}`);
  else { step("共享数据", "ok", `下载到 ${shareRoot}`); run(process.execPath, [path.join(root, "tools", "fetch-share-data.mjs"), "--root", shareRoot]); }
}

// -------------------------------------------------------------------- 收尾 --
const next = [
  `启动：cd ${root} && pnpm start --port ${port} --no-open`,
  `浏览器打开：http://127.0.0.1:${port}/`,
  "重启（保留数据）：pwsh -NoProfile -File scripts/restart.ps1 -Port " + port,
  "可选注册全局命令：cd cli && npm link --ignore-scripts --package-lock=false（之后任意目录运行 geosentinel）",
  "发布与回滚：node tools/release.mjs status|prepare|preview|publish（详见 RELEASES.md）",
];
if (asJson) console.log(JSON.stringify({ ok: true, report, next }, null, 2));
else console.log(`\n完成。下一步：\n${next.map((line) => "  " + line).join("\n")}`);
