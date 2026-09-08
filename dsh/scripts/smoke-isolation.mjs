import assert from "node:assert/strict";
import { mkdtemp, writeFile } from "node:fs/promises";
import path from "node:path";
import { PlatformStore } from "../plugins/platform/store.mjs";
import { DockerRunner } from "../plugins/research/docker.mjs";

const root = await mkdtemp(path.resolve(".runtime/isolation-"));
const store = new PlatformStore(root);
const admin = store.bootstrapAdmin("admin", "acceptance-only-password");
const identities = [];
for (const name of ["alice", "bob"]) {
  const user = store.acceptInvite(
    store.invite(admin),
    name,
    `${name}-acceptance-password`,
  );
  const project = store.createProject(user, `${name} private project`),
    chat = store.createChat(user, project.id);
  const identity = {
    user,
    projectId: project.id,
    chatId: chat.id,
    root: store.chatRoot(user, chat.id),
  };
  await writeFile(
    path.join(store.projectRoot(user, project.id), "inputs", `${name}.txt`),
    name,
  );
  identities.push(identity);
}
const runner = new DockerRunner({ store });
try {
  const results = await Promise.all(
    identities.map((id) =>
      runner.run(
        id,
        { kind: "execute" },
        {
          script: `from pathlib import Path
import socket, os, json
assert os.getuid() == 10001
assert not Path('/var/run/docker.sock').exists()
assert not Path('/home/worker/.config/earthengine/credentials').exists()
assert sorted(p.name for p in Path('inputs').iterdir()) == ['${id.user.username}.txt']
try:
    socket.create_connection(('1.1.1.1',443),timeout=1)
    raise AssertionError('analysis network unexpectedly reachable')
except OSError:
    pass
Path('outputs/private.txt').write_text('${id.user.username}')
print('ISOLATED_${id.user.username}')
`,
        },
      ),
    ),
  );
  assert.match(results[0].stdout, /ISOLATED_alice/);
  assert.match(results[1].stdout, /ISOLATED_bob/);
  assert.notEqual(results[0].jobId, results[1].jobId);
  const running = runner.run(
    identities[0],
    { kind: "execute" },
    { script: "import time\ntime.sleep(60)" },
  );
  const rejected = assert.rejects(running, /cancel|Docker exited/i);
  await assert.rejects(
    () => runner.run(identities[0], { kind: "execute" }, { script: "pass" }),
    /已有计算任务/,
  );
  await runner.cancelChat(identities[0].chatId);
  await rejected;
  assert.equal(runner.running.size, 0);
  const limited = new DockerRunner({ store, maxOutputBytes: 128 });
  await assert.rejects(
    () =>
      limited.run(
        identities[1],
        { kind: "execute" },
        {
          script:
            "from pathlib import Path\nPath('outputs/large.bin').write_bytes(b'x'*1024)",
        },
      ),
    /output size exceeded/,
  );
  assert.equal(limited.running.size, 0);
  const report = {
    passed: true,
    checks: [
      "concurrent-private-inputs",
      "nonroot-readonly-worker",
      "no-analysis-network",
      "no-credentials-or-docker-socket",
      "stdout-return",
      "one-job-per-user",
      "cancel-during-startup",
      "output-limit",
    ],
    at: new Date().toISOString(),
  };
  await writeFile(
    ".runtime/isolation-acceptance.json",
    JSON.stringify(report, null, 2),
  );
  console.log(JSON.stringify(report));
} finally {
  store.close();
}
