import path from "node:path";
import { ReleaseManager } from "./manager.mjs";
import { launchRelease, healthy } from "./runtime.mjs";
import { startupOptions, inspectEndpoint } from "../scripts/startup-check.mjs";

export async function runProduct(root, args) {
  const options = startupOptions(args), endpoint = await inspectEndpoint(options);
  if (endpoint.state === "running") { console.log("GeoSentinel 已在运行：" + endpoint.url); return; }
  if (endpoint.state !== "free") throw new Error("端口已占用，未停止任何现有服务");
  if (options.host !== "127.0.0.1") throw new Error("版本发布入口固定绑定 127.0.0.1；公网使用既有 HTTPS 代理");
  const home = path.resolve(process.env.GEO_DSH_HOME || path.join(root, ".runtime/home"));
  const manager = new ReleaseManager(path.dirname(root), process.env.GEO_RELEASE_DIR || path.join(home, "releases"));
  let state = await manager.state(), initializing = !state.active;
  if (!state.active) {
    console.log("首次建立正式发布快照，正在验证依赖、代码和计算镜像……");
    const candidate = await manager.prepare("bootstrap");
    if (candidate.status !== "validated") throw new Error(candidate.error);
    state = { ...state, active: candidate.id };
  }
  let child, stopping = false, switching = false;
  const start = async (id) => {
    child = await launchRelease(manager, id, { home, args: options.args });
    child.on("error", (error) => console.error("发布实例启动失败：", error.message));
    child.on("message", (message) => { if (message?.type === "geosentinel:release-ready") void changeVersion(); });
    child.on("exit", (code) => { if (!switching && !stopping) { console.error("正式服务退出：", code); process.exitCode = code || 1; } });
    return child;
  };
  const stop = async (instance) => {
    if (!instance || instance.exitCode !== null) return;
    const ended = new Promise((resolve) => instance.once("exit", resolve));
    // The running release only accepts the exit while it is idle, and a short
    // busy window (an active development session, an in-flight request) used to
    // cancel the whole switch after one 15-second attempt. Re-ask instead: each
    // message makes the instance re-check its own idle state.
    for (let attempt = 0; attempt < 8; attempt += 1) {
      if (instance.exitCode !== null) return;
      instance.send({ type: "geosentinel:release-exit" });
      const settled = await Promise.race([ended.then(() => true), new Promise((resolve) => setTimeout(() => resolve(false), 10000).unref())]);
      if (settled) return;
    }
    throw new Error("旧实例没有安全退出，取消切换");
  };
  async function changeVersion() {
    if (switching || stopping) return;
    switching = true;
    const previous = (await manager.state()).active, previousChild = child;
    try {
      const pending = await manager.locked(async () => { const current = await manager.state(); if (!current.pending) return null; current.pending.phase = "switching"; await manager.save(current); return current.pending; });
      if (!pending) return;
      await manager.verify(pending.id);
      await stop(child);
      await start(pending.id);
      if (!await healthy(options.port, pending.id)) throw new Error("新版健康检查未通过");
      await manager.activated(pending.id, pending.by);
      console.log("已发布：" + pending.id);
    } catch (error) {
      console.error("发布切换失败：", error.message);
      // A failed drain must leave the still-running original instance untouched.
      const needsRestore = previousChild.exitCode !== null;
      if (needsRestore && child?.exitCode === null) { const ended = new Promise((resolve) => child.once("exit", resolve)); child.kill(); await ended; }
      const failed = await manager.state(); failed.pending = null; failed.lastError = error.message; await manager.save(failed);
      if (needsRestore) await start(previous);
    } finally { switching = false; }
  }
  for (const signal of ["SIGINT", "SIGTERM"]) process.on(signal, () => { stopping = true; child?.kill(signal); });
  await start(state.active);
  if (!await healthy(options.port, state.active)) { child.kill(); throw new Error("正式版本启动未通过健康检查"); }
  if (initializing) await manager.activated(state.active);
  console.log("GeoSentinel 正式版本 " + state.active + "：" + endpoint.url);
}
