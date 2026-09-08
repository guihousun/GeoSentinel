import { readFileSync, existsSync } from "node:fs";
import { spawn, spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const envFile = process.env.GEO_ENV_FILE || path.join(root, ".env");
if (existsSync(envFile)) process.loadEnvFile(envFile);
const directory =
  process.env.GEO_MONITOR_DIR ||
  path.join(
    process.env.GEO_DSH_HOME || path.join(root, ".runtime/home"),
    "monitor",
  );
const worker = path.join(root, "monitoring/worker.mjs");
if (process.argv.includes("--stop")) {
  const lock = path.join(directory, "worker.lock");
  if (!existsSync(lock)) process.exit(0);
  const { pid } = JSON.parse(readFileSync(lock, "utf8"));
  if (!Number.isSafeInteger(pid) || pid < 1)
    throw new Error("Invalid monitor PID");
  const result =
    process.platform === "win32"
      ? spawnSync(
          "powershell.exe",
          [
            "-NoProfile",
            "-Command",
            `(Get-CimInstance Win32_Process -Filter "ProcessId=${pid}").CommandLine`,
          ],
          { encoding: "utf8", windowsHide: true },
        )
      : spawnSync("ps", ["-p", String(pid), "-o", "args="], {
          encoding: "utf8",
        });
  if (!result.stdout.trim()) {
    console.log("Monitor is already stopped");
    process.exit(0);
  }
  if (!result.stdout.toLowerCase().includes(worker.toLowerCase()))
    throw new Error("PID does not belong to this monitor; refusing to stop it");
  process.kill(pid);
  console.log("Stopped this workspace monitor");
} else {
  const args = process.argv.slice(2);
  if (args.some((x) => !["--once", "--translate-cache"].includes(x)))
    throw new Error("Allowed flags: --once, --translate-cache, --stop");
  const child = spawn(process.execPath, [worker, ...args], {
    env: { ...process.env, GEO_MONITOR_DIR: directory },
    stdio: "inherit",
    windowsHide: true,
  });
  child.on("exit", (code) => {
    process.exitCode = code ?? 1;
  });
  for (const signal of ["SIGINT", "SIGTERM"])
    process.on(signal, () => child.kill(signal));
}
