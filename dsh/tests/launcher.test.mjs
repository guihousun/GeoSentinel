import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { launcherArgs } from "../cli/options.mjs";
import { adminOptions } from "../scripts/admin-start.mjs";

test("administrator launcher denies public binding and keeps native DSH isolated", () => {
  assert.deepEqual(adminOptions([]), { host: "127.0.0.1", port: 8514, noOpen: false });
  assert.equal(adminOptions(["--port=8515", "--no-open"]).port, 8515);
  for (const args of [["--host", "0.0.0.0"], ["--trusted-host", "example.com"], ["--port", "0"]]) assert.throws(() => adminOptions(args));
});

test("product command defaults to 8511 and preserves explicit startup arguments", () => {
  assert.deepEqual(launcherArgs([]), ["--port", "8511"]);
  assert.deepEqual(launcherArgs(["web", "--no-open"]), ["--no-open", "--port", "8511"]);
  assert.deepEqual(launcherArgs(["start", "--port=8512"]), ["--port=8512"]);
  assert.deepEqual(launcherArgs(["--port", "8513"]), ["--port", "8513"]);
  assert.equal(launcherArgs(["--help"]), null);
  assert.throws(() => launcherArgs(["restart"]), /未知命令/);
});

test("product command help works outside checkout without loading credentials or DSH", () => {
  const cli = fileURLToPath(new URL("../cli/geosentinel.mjs", import.meta.url));
  const result = spawnSync(process.execPath, [cli, "--help"], { cwd: tmpdir(), encoding: "utf8", windowsHide: true,
    env: { ...process.env, GEO_ENV_FILE: "missing-configuration-must-not-be-read.env" } });
  assert.equal(result.status, 0);
  assert.match(result.stdout, /8511/);
  assert.match(result.stdout, /geosentinel web/);
});
