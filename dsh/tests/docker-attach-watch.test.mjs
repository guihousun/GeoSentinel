import test from "node:test";
import assert from "node:assert/strict";
import { containerState } from "../plugins/research/docker.mjs";

// The attached `docker start --attach` child is the only completion signal the runner
// used to have, and Docker Desktop occasionally loses it: a boundary job's container
// had already stopped with a complete result on disk (2026-09-12) while the job stayed
// `running` for 13+ minutes and then consumed the case's whole budget. The watchdog
// decides from `docker inspect --format '{{.State.Running}} {{.State.ExitCode}}'`, so
// this parser is the part that must never misread.
test("container state parsing drives the attach watchdog", () => {
  assert.deepEqual(containerState("true 0\n"), { running: true, exitCode: 0 });
  assert.deepEqual(containerState("false 0\n"), { running: false, exitCode: 0 });
  assert.deepEqual(containerState("  false   137  "), { running: false, exitCode: 137 });
  assert.deepEqual(containerState("FALSE 1"), { running: false, exitCode: 1 });
  // Unusable output must not be read as "finished": finishing on garbage would cut a
  // live job short, which is worse than the stall this watchdog removes.
  for (const value of ["", "  ", "Error: No such object: geosentinel-x", "true", "running 0", "false -1", null, undefined])
    assert.equal(containerState(value), null, JSON.stringify(value));
});
