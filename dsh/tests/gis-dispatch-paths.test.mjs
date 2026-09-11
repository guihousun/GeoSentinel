import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

// The GIS dispatcher decides which workspace-relative paths a registered tool accepts.
// Agents kept passing an earlier job's directory WITHOUT the `outputs/` prefix
// (`20260911-180003-gee-235ce3/imagery.tif`) and got a bare "Workspace-relative path
// required" — four failed jobs across two benchmark cases on 2026-09-12. The fix accepts
// that shape and the error now names the accepted forms, so both are pinned here.
const dockerDir = path.join(path.dirname(path.dirname(fileURLToPath(import.meta.url))), "docker");

const pythonAvailable = () => {
  try { return spawnSync("python", ["-c", "print(1)"], { encoding: "utf8", windowsHide: true }).status === 0; }
  catch { return false; }
};

test("GIS tool paths accept every documented root plus a bare job directory", async (t) => {
  if (!pythonAvailable()) { t.skip("python 不可用，跳过（容器内由镜像保证）"); return; }
  const root = mkdtempSync(path.join(tmpdir(), "geo-gis-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  for (const name of ["inputs", "previous", "outputs", "share"]) mkdirSync(path.join(root, name), { recursive: true });

  const script = `
import json, sys
sys.path.insert(0, r"${dockerDir.replaceAll("\\", "\\\\")}")
from gis_dispatch import scoped_path
root = r"${root.replaceAll("\\", "\\\\")}"
out = {}
for label, value, output in [
    ("jobdir", "20260911-180003-gee-235ce3/imagery.tif", False),
    ("outputs", "outputs/20260911-180003-gee-235ce3/imagery.tif", False),
    ("inputs", "inputs/a.tif", False),
    ("share", "share/全球基础数据/x.tif", False),
    ("write", "outputs/out.tif", True),
    ("bare_output", "clipped.tif", True),
    ("share_output", "share/全球基础数据/x.tif", True),
]:
    try:
        out[label] = scoped_path(value, output=output, root=root).replace("\\\\", "/")
    except ValueError as error:
        out[label] = "error: " + str(error)[:100]
for label, value in [("rejected", "bad/path.tif"), ("parent", "inputs/../share/x.tif"), ("absolute", "/workspace/inputs/a.tif")]:
    try:
        scoped_path(value, root=root)
        out[label] = "accepted"
    except ValueError as error:
        out[label] = str(error)
print(json.dumps(out, ensure_ascii=False))`;
  // PYTHONIOENCODING: without it the child writes its message in the machine's locale
  // encoding (cp936 here) and the captured text is mojibake, which would make the
  // assertion below fail on wording rather than on behaviour.
  const result = spawnSync("python", ["-c", script], { encoding: "utf8", windowsHide: true, env: { ...process.env, PYTHONIOENCODING: "utf-8" } });
  assert.equal(result.status, 0, result.stderr?.slice(0, 400));
  const seen = JSON.parse(result.stdout);

  // A bare job directory and the explicit `outputs/<jobId>/…` form both mean the
  // earlier job's artifacts; the dispatcher maps them to `previous/…`.
  assert.match(seen.jobdir, /^previous\/20260911-180003-gee-235ce3\/imagery\.tif$/);
  assert.equal(seen.jobdir, seen.outputs);
  assert.match(seen.inputs, /^inputs\/a\.tif$/);
  assert.match(seen.share, /^share\/.+x\.tif$/);
  assert.match(seen.write, /^outputs\/out\.tif$/);
  // A bare file name as an OUTPUT can only mean this job's outputs directory, so it is
  // accepted instead of costing the agent its turn (B01 lost a clip job to this on
  // 2026-09-12); the shared library stays read-only and is still refused as an output.
  assert.match(seen.bare_output, /^outputs\/clipped\.tif$/);
  assert.match(seen.share_output, /^error:/);
  // An unknown root is refused WITH the accepted forms named (the old message did not
  // say what to write instead, so agents retried the same wrong path).
  assert.match(seen.rejected, /inputs\//);
  assert.match(seen.rejected, /outputs\/<作业ID>/);
  // Every refusal names the offending value and the accepted forms — the old bare
  // "Workspace-relative path required" left the agent guessing.
  for (const key of ["rejected", "parent", "absolute"]) {
    assert.match(seen[key], /的|收到/, `${key} 的错误信息应说明原因与可用形式`);
    assert.ok(seen[key].includes("share/"), `${key} 的错误信息应列出 share/ 形式`);
  }
});
