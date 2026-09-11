import { existsSync, renameSync, readFileSync } from "node:fs";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";

/**
 * Switch the installed dependency tree of a product checkout to the versions its
 * `package.json` pins (the "upgrade DSH kernel dependencies" step in RELEASES.md).
 *
 * The dangerous part of that step is not `pnpm install`; it is what is left behind when
 * it fails halfway, because the same tree may be serving a running administrator
 * session. So this tool never installs in place: it renames the current `node_modules`
 * aside first, verifies the new tree (installed version, environment checks) before
 * declaring success, and puts the old tree back verbatim when anything fails.
 *
 * Dry run by default; pass `--apply` to actually move things.
 *
 * Usage:
 *   node scripts/upgrade-deps.mjs [--apply] [--online] [--root <dir>]
 *
 * Environment seams used by the tests (both are whole command lines):
 *   GEO_UPGRADE_INSTALL  command that performs the install
 *                        (default `pnpm install --no-frozen-lockfile [--offline]`)
 *   GEO_UPGRADE_CHECK    command that verifies the tree (default `node scripts/check-env.mjs`)
 */
const argv = process.argv.slice(2);
const flag = (name) => argv.includes(`--${name}`);
const value = (name, fallback) => {
  const index = argv.indexOf(`--${name}`);
  return index >= 0 ? argv[index + 1] : fallback;
};

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(value("root", path.join(here, "..")));
const apply = flag("apply");
const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
const modules = path.join(root, "node_modules");
const backup = path.join(root, `node_modules.bak-${stamp}`);
const failed = path.join(root, `node_modules.failed-${stamp}`);

const say = (line) => console.log(line);
const run = (command, args = [], { shell = false } = {}) => new Promise((resolve) => {
  const child = spawn(command, args, { cwd: root, stdio: "inherit", shell });
  child.on("error", (error) => resolve({ code: 1, error }));
  child.on("exit", (code) => resolve({ code: code ?? 1 }));
});
const installCommand = process.env.GEO_UPGRADE_INSTALL
  ?? ["pnpm", "install", "--no-frozen-lockfile", flag("online") ? "" : "--offline"].filter(Boolean).join(" ");
const checkCommand = process.env.GEO_UPGRADE_CHECK ?? `${process.execPath} scripts/check-env.mjs`;

function installedVersion() {
  const manifest = path.join(root, "node_modules", "@deepseek-ai", "dsh-web-app", "package.json");
  if (!existsSync(manifest)) return undefined;
  try { return JSON.parse(readFileSync(manifest, "utf8")).version; } catch { return undefined; }
}
function pinnedVersion() {
  try { return JSON.parse(readFileSync(path.join(root, "package.json"), "utf8")).dependencies?.["@deepseek-ai/dsh-web-app"]; }
  catch { return undefined; }
}

const pinned = pinnedVersion(), installed = installedVersion();
say(`产品目录：${root}`);
say(`package.json pin：@deepseek-ai/dsh-web-app ${pinned ?? "（缺失）"}`);
say(`当前安装：${installed ?? "（没有 node_modules/@deepseek-ai/dsh-web-app）"}`);
if (!existsSync(path.join(root, "package.json")) || !existsSync(path.join(root, "pnpm-lock.yaml")))
  throw new Error("这不是产品目录：需要 package.json 与 pnpm-lock.yaml");
if (!existsSync(modules)) throw new Error("没有 node_modules 可升级；请先完成一次安装");
if (existsSync(backup)) throw new Error(`备份目录已存在：${path.basename(backup)}`);
if (installed === pinned) say("提示：pin 与已安装版本一致，本次升级可能无事可做。");

const installArgs = [];
if (!flag("online")) installArgs.push("--offline");
say(`\n将执行：`);
say(`  1. 把 node_modules 改名为 ${path.basename(backup)}（同盘改名，秒级且可逆）`);
say(`  2. ${installCommand}`);
say(`  3. 校验：${checkCommand}`);
say(`  4. 任一步失败：把新树改名到 ${path.basename(failed)}，并把备份改回 node_modules`);
if (!apply) { say("\n（干跑：加 --apply 才会真的执行）"); process.exit(0); }

say(`\n[1/4] 备份当前依赖树…`);
renameSync(modules, backup);
let restored = false;
try {
  say(`[2/4] 安装 pin 版本…`);
  const install = await run(installCommand, [], { shell: true });
  if (install.code !== 0) throw new Error(`安装失败（退出码 ${install.code}）`);
  say(`[3/4] 校验…`);
  const check = process.env.GEO_UPGRADE_CHECK
    ? await run(checkCommand, [], { shell: true })
    : await run(process.execPath, ["scripts/check-env.mjs"]);
  if (check.code !== 0) throw new Error(`环境校验失败（退出码 ${check.code}）`);
  const now = installedVersion();
  if (pinned !== undefined && now !== pinned) throw new Error(`安装后的版本 ${now ?? "未知"} 与 pin ${pinned} 不一致`);
  say(`[4/4] 完成：@deepseek-ai/dsh-web-app ${now}`);
  say(`备份保留在 ${backup}，确认无误后再删除。`);
} catch (error) {
  say(`\n升级失败：${error.message}`);
  if (existsSync(modules)) renameSync(modules, failed);
  if (existsSync(backup)) { renameSync(backup, modules); restored = true; }
  say(restored ? `已恢复原依赖树；失败的新树留在 ${failed}（可删除）。` : "未能恢复原依赖树，请人工检查备份目录。");
  process.exitCode = 1;
}
