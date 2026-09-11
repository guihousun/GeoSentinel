import { acceptPreview, checkEnvironment } from "./acceptance.mjs";
import { readFileSync } from "node:fs";
import path from "node:path";
import { randomBytes } from "node:crypto";
import { ReleaseManager } from "./manager.mjs";
import { launchRelease, healthy } from "./runtime.mjs";
import { inspectEndpoint } from "../scripts/startup-check.mjs";

export function releaseService({ source, directory, runtime, store, extraIdle = () => true }) {
  const manager = new ReleaseManager(source, directory);
  let phase = "idle", preparing = false, previewChild, preview, readySent = false, closing = false, mutations = 0, operationError;
  const idle = () => mutations === 0 && extraIdle() && runtime.db.prepare("SELECT count(*) AS n FROM runtime_jobs WHERE status IN ('queued','running','cancelling')").get().n === 0;
  const draining = () => { try { return Boolean(JSON.parse(readFileSync(path.join(directory, "state.json"), "utf8")).pending); } catch (error) { if (error.code === "ENOENT") return false; throw error; } };
  runtime.admissionGuard = draining;
  const timer = setInterval(() => {
    try {
      if (closing || preparing || !process.send || readySent || !draining() || !idle()) return;
      readySent = true; process.send({ type: "geosentinel:release-ready" });
    } catch { operationError = "发布状态不可读取，自动切换已暂停"; }
  }, 1000);
  timer.unref();
  const stopPreview = () => { if (previewChild?.connected) previewChild.send({ type: "geosentinel:preview-exit" }); previewChild = null; preview = null; };
  const onExit = () => stopPreview();
  process.once("exit", onExit);
  const onMessage = async (message) => {
    if (message?.type !== "geosentinel:release-exit") return;
    // Not idle yet: re-arm the ready signal so the supervisor is told again
    // once the platform drains, instead of the request silently dying here.
    if (!idle()) { readySent = false; return; }
    closing = true; clearInterval(timer); stopPreview(); runtime.close(); store.close();
    process.exit(75);
  };
  process.on("message", onMessage);
  const service = {
    beginMutation() { if (readySent && draining()) throw new Error("版本正在切换，请稍后操作"); mutations++; let done = false; return () => { if (!done) mutations--; done = true; }; },
    async status() { const status = await manager.status(); return { ...status, phase, lastError: operationError || status.lastError, preparing, loadedRelease: process.env.GEO_RELEASE_ID ?? null, supervised: Boolean(process.send), previewUrl: preview?.expires > Date.now() ? preview.url : null }; },
    saveProduct: (product) => manager.saveProduct(product),
    prepare(by) {
      if (preparing) throw new Error("正在验证版本");
      preparing = true; phase = "checking"; operationError = null;
      void (async () => { await checkEnvironment(path.join(source, "dsh")); phase = "building"; const candidate = await manager.prepare(by); if (candidate.status !== "validated") throw new Error(candidate.error); phase = "previewing"; await service.preview(candidate.id, true); phase = "ready"; })().catch((error) => { operationError = error.message; phase = "failed"; }).finally(() => { preparing = false; });
      return { accepted: true };
    },
    async preview(id, internal = false) {
      if (preparing && !internal) throw new Error("版本正在验证，稍后再预览");
      return manager.locked(async () => {
      const state = await manager.state();
      if (state.pending) throw new Error("发布切换期间不能启动新预览");
      if (state.candidate?.id !== id || state.candidate.status !== "validated") throw new Error("候选版本尚未验证");
      if (preview?.id === id && preview.expires > Date.now() && state.candidate.acceptance === "passed") return { url: preview.url };
      if (previewChild?.exitCode === null) { const ended = new Promise((resolve) => previewChild.once("exit", resolve)); previewChild.send({ type: "geosentinel:preview-exit" }); await ended; }
      if ((await inspectEndpoint({ host: "127.0.0.1", port: 8513 })).state !== "free") throw new Error("8513 已被其他程序占用，未停止该程序");
      const token = randomBytes(32).toString("hex"), password = randomBytes(24).toString("hex"), expires = Date.now() + 10 * 60000;
      // A preview home lives beside the release directory, not inside it: the
      // chat workspace path then stays short enough for Docker Desktop's
      // Windows file sharing, and an abandoned preview tree cannot poison the
      // release tree that production reads from.
      const previewRoot = path.join(path.dirname(directory), "previews");
      previewChild = await launchRelease(manager, id, { home: path.join(previewRoot, id.slice(0, 8) + "-" + token.slice(0, 6)), args: ["--port", "8513", "--no-open"], preview: { token, password, expires } });
      if (!await healthy(8513, id)) { stopPreview(); throw new Error("预览服务未通过健康检查"); }
      preview = { id, expires, url: "http://127.0.0.1:8513/geo/api/preview-login?token=" + token };
      phase = "accepting";
      try { await acceptPreview(manager, id, password); phase = "ready"; } catch (error) { stopPreview(); throw error; }
      state.candidate.previewed = true; state.candidate.acceptance = "passed"; await manager.save(state);
      return { url: preview.url };
      });
    },
    async publish(id, by, rollback) {
      if (!process.send) throw new Error("请通过新版 geosentinel 启动器启动正式版后再发布");
      readySent = false;
      return manager.request(id, by, rollback, "管理员在发布页面点击发布此版本");
    },
    async cancel() { return manager.locked(async () => { const state = await manager.state(); if (state.pending?.phase === "switching") throw new Error("版本正在切换，不能取消"); state.pending = null; readySent = false; await manager.save(state); return { cancelled: true }; }); },
    close() { clearInterval(timer); stopPreview(); process.off("message", onMessage); process.off("exit", onExit); },
  };
  return service;
}
