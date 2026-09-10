import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { readSnapshot } from "./snapshot.mjs";

export function monitorService(ctx) {
  const directory =
    process.env.GEO_MONITOR_DIR || path.join(process.env.DSH_HOME, "monitor");
  const enabled = process.env.GEO_MONITOR_ENABLED !== "false";
  if (enabled && process.env.GEO_MONITOR_EXTERNAL !== "true")
    ctx.effect(() => {
      const keys = [
        "PATH",
        "Path",
        "SystemRoot",
        "WINDIR",
        "TEMP",
        "TMP",
        "HOME",
        "USERPROFILE",
        "DOCKER_HOST",
        "DOCKER_CONTEXT",
        "DOCKER_CONFIG",
        "GEO_MONITOR_MODEL",
        "GEO_MONITOR_TRANSLATE",
        "GEO_MONITOR_PROXY",
        "GEO_GEE_PROXY",
        "GEO_MONITOR_INTERVAL_MINUTES",
        "DEEPSEEK_API_KEY",
        "DeepSeek_API_KEY",
        "DEEPSEEK_BASE_URL",
        "DeepSeek_Coding_URL",
        "NTL_MONITOR_GDELT_ENABLED",
        "NTL_MONITOR_GDELT_QUERY",
        "NTL_MONITOR_ACLED_ENABLED",
        "NTL_MONITOR_ACLED_ACCESS_TOKEN",
        "NTL_MONITOR_ACLED_USERNAME",
        "NTL_MONITOR_ACLED_PASSWORD",
      ];
      const env = Object.fromEntries(
        keys
          .filter((k) => process.env[k] !== undefined)
          .map((k) => [k, process.env[k]]),
      );
      const worker = spawn(
        process.execPath,
        [fileURLToPath(new URL("./worker.mjs", import.meta.url))],
        {
          env: { ...env, GEO_MONITOR_DIR: directory },
          windowsHide: true,
          stdio: ["ignore", "ignore", "pipe"],
        },
      );
      worker.stderr.on("data", () => {});
      worker.on("error", () =>
        console.error("Public monitor process failed to start"),
      );
      const stop = () => worker.kill();
      process.once("exit", stop);
      return () => { process.off("exit", stop); stop(); };
    });
  return async () => ({ ...(await readSnapshot(directory)), enabled });
}
