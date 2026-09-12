import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

// A relocated deployment keeps its old path alive through a directory junction, and the
// process that is already running keeps the spelling it was launched with. Comparing
// those two spellings as strings made `restart.ps1` call the platform's own service
// "a different service" and refuse to stop it (measured 2026-09-12, moving
// D:\GeoSentinel-DSH to E:\GeoSentinel\project). `scripts/path-alias.ps1` resolves the
// chain instead; this pins that a junction at ANY level is followed.
const scripts = path.join(path.dirname(path.dirname(fileURLToPath(import.meta.url))), "scripts");
const aliasScript = path.join(scripts, "path-alias.ps1");

const powershell = () => {
  for (const candidate of ["pwsh", "powershell"]) {
    const probe = spawnSync(candidate, ["-NoProfile", "-Command", "exit 0"], { encoding: "utf8", windowsHide: true });
    if (probe.status === 0) return candidate;
  }
  return null;
};

test("真实路径解析会逐级跟随目录联接（部署换盘后仍能认出自己的服务）", (t) => {
  if (process.platform !== "win32") { t.skip("仅 Windows 有目录联接"); return; }
  const shell = powershell();
  if (!shell) { t.skip("未找到 PowerShell"); return; }
  const root = mkdtempSync(path.join(tmpdir(), "geo-alias-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const real = path.join(root, "real", "inner");
  mkdirSync(real, { recursive: true });
  const link = path.join(root, "alias");
  const created = spawnSync("cmd", ["/c", "mklink", "/J", link, path.join(root, "real")], { encoding: "utf8", windowsHide: true });
  if (created.status !== 0) { t.skip("无法创建目录联接：" + (created.stderr || created.stdout || "").slice(0, 120)); return; }

  const script = [
    `. '${aliasScript}'`,
    `$resolved = Resolve-RealPath '${path.join(link, "inner")}'`,
    `$expected = Resolve-RealPath '${real}'`,
    `"resolved=$resolved"`,
    `"same=$($resolved -eq $expected)"`,
    // A path with no link must come back unchanged, so the helper never invents a target.
    `"plain=$((Resolve-RealPath '${real}') -eq '${real}')"`,
  ].join("; ");
  const result = spawnSync(shell, ["-NoProfile", "-Command", script], { encoding: "utf8", windowsHide: true });
  assert.equal(result.status, 0, result.stderr?.slice(0, 400));
  assert.match(result.stdout, /same=True/, `联接链应解析到同一实体：${result.stdout}`);
  assert.match(result.stdout, /plain=True/, `无联接路径应原样返回：${result.stdout}`);
});
