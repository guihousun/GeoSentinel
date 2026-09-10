import { spawnSync } from "node:child_process";
import path from "node:path";
export function usablePowerShell(env = process.env, probe = spawnSync) {
  const configured = env.GEO_ADMIN_PWSH_PATH;
  const candidates = configured ? [configured] : [path.join(env.ProgramFiles || env.PROGRAMFILES || "C:\\Program Files", "PowerShell/7/pwsh.exe"), ...(env.Path || env.PATH || "").split(path.delimiter).filter(Boolean).map((entry) => path.join(entry.replace(/^"|"$/g, ""), "pwsh.exe"))];
  for (const candidate of [...new Set(candidates)]) {
    const result = probe(candidate, ["-NoLogo", "-NoProfile", "-NonInteractive", "-Command", "exit 0"], { windowsHide: true, stdio: "ignore", timeout: 5000 });
    if (!result.error && result.status === 0) return candidate;
  }
  throw new Error("找不到可运行的 PowerShell 7；请安装后重试，或配置 GEO_ADMIN_PWSH_PATH");
}
