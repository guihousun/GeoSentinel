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
]:
    try:
        out[label] = scoped_path(value, output=output, root=root).replace("\\\\", "/")
    except ValueError as error:
        out[label] = "error: " + str(error)[:100]
try:
    scoped_path("bad/path.tif", root=root)
    out["rejected"] = "accepted"
except ValueError as error:
    out["rejected"] = str(error)
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
  // An unknown root is refused WITH the accepted forms named (the old message did not
  // say what to write instead, so agents retried the same wrong path).
  assert.match(seen.rejected, /inputs\//);
  assert.match(seen.rejected, /outputs\/<作业ID>/);
});
