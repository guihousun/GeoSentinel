import test from "node:test";
import assert from "node:assert/strict";
import { containerState, containerProcesses } from "../plugins/research/docker.mjs";

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

// The second stall shape: the daemon says the container is running while nothing is
// alive inside it (`docker top` returns a header only). The watchdog needs two
// consecutive empty samples, so the counter must treat a header-only answer as 0 and a
// real listing as >0.
test("docker top parsing detects a container with no processes", () => {
  assert.equal(containerProcesses("UID                 PID                 PPID                C                   STIME               TTY                 TIME                CMD\n"), 0);
  assert.equal(containerProcesses(""), 0);
  assert.equal(containerProcesses("  \n \n"), 0);
  const listing = [
    "UID                 PID                 PPID                C                   STIME               TTY                 TIME                CMD",
    "10001               20313               20291               0                   19:03               ?                   00:00:00            python /opt/geosentinel/worker.py",
  ].join("\n");
  assert.equal(containerProcesses(listing), 1);
  // Two rows plus a header, with CRLF line endings as Docker returns them on Windows.
  assert.equal(containerProcesses("UID PID\n10001 1\r\n10001 2\r\n"), 2);
});
