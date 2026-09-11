#!/usr/bin/env node
import { acceptPreview, checkEnvironment } from "../release/acceptance.mjs";
/**
 * Operator release CLI — the same validated publication path the administrator
 * panel drives, run from a local shell instead of an authenticated browser
 * session.
 *
 *   node dsh/tools/release.mjs status
 *   node dsh/tools/release.mjs prepare [--by <name>]
 *   node dsh/tools/release.mjs preview [--id <id>] [--hold <seconds>]
 *   node dsh/tools/release.mjs publish --id <id> [--by <name>]
 *   node dsh/tools/release.mjs rollback --id <id> [--by <name>]
 *   node dsh/tools/release.mjs cancel
 *
 * It reuses ReleaseManager (freeze → offline install → syntax → tests → fixed
 * Docker image digest) and launchRelease (isolated 8513 preview). It does not
 * re-implement validation and it never touches credentials.
 *
 * Because a local operator invokes it instead of /geo/api/admin/releases/*,
 * the HTTP admin gate (login + 30-minute password reconfirmation) does not
 * run; every mutation writes its own `release.*` row into the platform audit
 * table so the trail still exists.
 *
 * `publish` only writes state.pending — the running supervisor performs the
 * switch once the platform is idle. That restart stops the current release
 * instance, so any development worker it spawned is terminated with it.
 *
 * This file deliberately lives OUTSIDE the release scope (release/manager.mjs
 * lists `dsh/scripts`, not `dsh/tools`), so an auth-bypassing publish tool is
 * never shipped inside a frozen product snapshot, and editing it does not
 * invalidate an already validated candidate.
 */
import { existsSync } from "node:fs";
import { randomBytes } from "node:crypto";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";
import { ReleaseManager } from "../release/manager.mjs";
import { launchRelease, healthy } from "../release/runtime.mjs";
import { inspectEndpoint } from "../scripts/startup-check.mjs";

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const source = path.dirname(root);
const envFile = process.env.GEO_ENV_FILE || path.join(root, ".env");
if (existsSync(envFile)) process.loadEnvFile(envFile);

const home = path.resolve(process.env.GEO_DSH_HOME || path.join(root, ".runtime/home"));
const directory = process.env.GEO_RELEASE_DIR || path.join(home, "releases");
const dataDir = process.env.GEO_DATA_DIR || path.join(home, "geosentinel");
const actor = process.env.GEO_RELEASE_ACTOR || "release-cli";
const PREVIEW_PORT = 8513;
const manager = new ReleaseManager(source, directory);

// This CLI is usually run from inside a development worker shell, where the
// GEO_ADMIN_DEV_* variables are inherited. Passing them through would boot the
// preview as a development platform (no /geo/api routes) instead of the real
// product, so strip them before spawning any release instance.
const releaseEnv = { ...process.env };
for (const key of Object.keys(releaseEnv))
  if (key === "GEO_ADMIN_DEVELOPMENT" || key.startsWith("GEO_ADMIN_DEV_") || key.startsWith("GEO_PREVIEW_"))
    delete releaseEnv[key];

const args = process.argv.slice(2);
const command = args[0] ?? "status";
const flag = (name, fallback = null) => {
  const index = args.indexOf("--" + name);
  return index === -1 || args[index + 1] === undefined ? fallback : args[index + 1];
};

function audit(action, target) {
  const file = path.join(dataDir, "platform.sqlite");
  if (!existsSync(file)) return console.error("release: audit skipped (platform database not found)");
  try {
    const db = new DatabaseSync(file);
    db.exec("PRAGMA busy_timeout=5000");
    db.prepare("INSERT INTO audit(at,actor,action,target) VALUES(?,?,?,?)")
      .run(Date.now(), actor, action, target ?? null);
    db.close();
  } catch (error) {
    console.error(`release: audit skipped (${error.message})`);
  }
}

const print = (value) => console.log(JSON.stringify(value, null, 2));

async function status() {
  const state = await manager.status();
  print({
    source, directory,
    active: state.active,
    candidate: state.candidate,
    pending: state.pending,
    lastError: state.lastError,
    changes: state.changes,
    loadedRelease: process.env.GEO_RELEASE_ID ?? null,
  });
}

async function prepare() {
  if (args.includes("--publish") && !flag("authorization", "").trim()) throw new Error("自动发布必须提供本次用户授权 --authorization");
  await checkEnvironment(root);
  const by = flag("by", actor);
  console.error(`release: freezing ${source} and validating (offline install, syntax, tests, docker build)…`);
  const candidate = await manager.prepare(by);
  audit("release.prepare", candidate.id);
  print(candidate);
  if (candidate.status !== "validated") process.exitCode = 1;
}

async function preview() {
  const state = await manager.state();
  const id = flag("id", state.candidate?.id);
  if (state.candidate?.id !== id || state.candidate.status !== "validated")
    throw new Error("候选版本尚未验证，先运行 prepare");
  if (state.pending) throw new Error("已有版本等待切换，不能启动新预览");
  if ((await inspectEndpoint({ host: "127.0.0.1", port: PREVIEW_PORT })).state !== "free")
    throw new Error(`${PREVIEW_PORT} 已被其他程序占用，未停止该程序`);
  const token = randomBytes(32).toString("hex");
  const password = randomBytes(24).toString("hex");
  const expires = Date.now() + 10 * 60000;
  const child = await launchRelease(manager, id, {
    // Keep the preview home beside the release directory (short path, separate
    // tree) so chat workspaces stay writable through Docker Desktop's Windows
    // file sharing.
    home: path.join(path.dirname(directory), "previews", id.slice(0, 8) + "-" + token.slice(0, 6)),
    args: ["--port", String(PREVIEW_PORT), "--no-open"],
    preview: { token, password, expires },
    env: releaseEnv,
  });
  if (!await healthy(PREVIEW_PORT, id)) {
    child.kill();
    throw new Error("预览服务未通过健康检查");
  }
  try { await acceptPreview(manager, id, password); } catch (error) { child.kill(); throw error; }
  try { await manager.locked(async () => {
    const current = await manager.state();
    if (current.pending || current.candidate?.id !== id) throw new Error("验收期间候选发生变化，请重新生成预览");
    current.candidate.acceptance = "passed";
    current.candidate.previewed = true;
    await manager.save(current);
  }); } catch(error) { child.kill(); throw error; }
  audit("release.preview", id);
  const hold = Number(flag("hold", command === "generate-preview" ? "600" : "0"));
  print({ id, url: `http://127.0.0.1:${PREVIEW_PORT}/geo/api/preview-login?token=${token}`,
    expires: new Date(expires).toISOString(), holdingSeconds: Number.isFinite(hold) ? hold : 0 });
  if (command === "generate-preview" && args.includes("--publish")) { try { await publish(false, id); } catch(error) { child.kill(); throw error; } }
  if (Number.isFinite(hold) && hold > 0) await new Promise((resolve) => setTimeout(resolve, hold * 1000));
  child.kill();
}

async function publish(rollback, selectedId) {
  const id = selectedId ?? flag("id");
  if (!id) throw new Error("需要 --id <版本号>");
  const by = flag("by", actor);
  const request = await manager.request(id, by, rollback, flag("authorization", ""));
  audit(rollback ? "release.rollback" : "release.publish", id);
  print({ requested: request, note: "运行中的 supervisor 会在平台空闲后切换版本；切换会重启正式实例。" });
}

async function cancel() {
  const result = await manager.locked(async () => {
    const state = await manager.state();
    if (state.pending?.phase === "switching") throw new Error("版本正在切换，不能取消");
    if (!state.pending) return { cancelled: false, note: "没有等待切换的版本" };
    const cleared = state.pending.id;
    state.pending = null;
    await manager.save(state);
    return { cancelled: true, cleared };
  });
  if (result.cancelled) audit("release.cancel", result.cleared);
  print(result);
}

try {
  if (command === "status") await status();
  else if (command === "prepare") await prepare();
  else if (command === "generate-preview") { await prepare(); if (!process.exitCode) await preview(); }
  else if (command === "preview") await preview();
  else if (command === "publish") await publish(false);
  else if (command === "rollback") await publish(true);
  else if (command === "cancel") await cancel();
  else throw new Error(`未知命令：${command}`);
} catch (error) {
  console.error(`release: ${error.message}`);
  process.exitCode = 1;
}
