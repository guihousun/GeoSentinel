import { spawnSync } from "node:child_process";
import assert from "node:assert/strict";

for (const [value, expected] of [[null, 1800], ["11", 11], ["0", null]]) {
  const code = `import runpy\nm = runpy.run_path('/opt/geosentinel/worker.py')\nseen = []\nm['signal'].alarm = seen.append\ntry:\n    m['main']()\nexcept FileNotFoundError:\n    pass\nexcept ValueError:\n    assert ${expected === null ? "True" : "False"}\nassert seen == ${expected === null ? "[]" : `[${expected}]`}\nprint('worker timeout verified')`;
  const result = spawnSync("docker", ["run", "--rm", "--network", "none", "--read-only", ...(value === null ? [] : ["-e", `GEO_JOB_TIMEOUT_SECONDS=${value}`]), "--entrypoint", "python", "geosentinel-gis:0.1", "-c", code], { encoding: "utf8", windowsHide: true, timeout: 30000 });
  assert.equal(result.status, 0, result.stderr);
}
console.log(JSON.stringify({ passed: true, checks: ["image-default-1800-seconds", "host-timeout-override", "invalid-timeout-rejected"] }));
