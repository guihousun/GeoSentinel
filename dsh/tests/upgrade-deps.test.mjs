import test from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { promisify } from "node:util";

const run = promisify(execFile);
const script = fileURLToPath(new URL("../scripts/upgrade-deps.mjs", import.meta.url));

// The upgrade step must be reversible: whatever the install or the verification does,
// the previous dependency tree has to come back when the new one is not usable. These
// tests drive the failure paths with injected commands, so the guarantee does not depend
// on a real pnpm run.
async function fixture({ installed = "0.1.2-rc.1", pinned = "0.1.5-rc.1" } = {}) {
  const root = await mkdtemp(path.join(tmpdir(), "geo-upgrade-"));
  await writeFile(path.join(root, "package.json"), JSON.stringify({ dependencies: { "@deepseek-ai/dsh-web-app": pinned } }));
  await writeFile(path.join(root, "pnpm-lock.yaml"), "lockfileVersion: '9.0'\n");
  const manifest = path.join(root, "node_modules", "@deepseek-ai", "dsh-web-app");
  await mkdir(manifest, { recursive: true });
  await writeFile(path.join(manifest, "package.json"), JSON.stringify({ version: installed }));
  await writeFile(path.join(root, "node_modules", "marker.txt"), "old tree");
  return root;
}

const upgrade = (root, env = {}) => run(process.execPath, [script, "--apply", "--root", root], {
  env: { ...process.env, ...env },
}).then(({ stdout }) => ({ code: 0, stdout }), (error) => ({ code: error.code ?? 1, stdout: `${error.stdout ?? ""}${error.stderr ?? ""}` }));

test("a dry run changes nothing", async (t) => {
  const root = await fixture();
  t.after(() => rm(root, { recursive: true, force: true }));
  const result = await run(process.execPath, [script, "--root", root]);
  assert.match(result.stdout, /干跑/);
  assert.equal(await readFile(path.join(root, "node_modules", "marker.txt"), "utf8"), "old tree");
  assert.deepEqual((await readdir(root)).filter((name) => name.startsWith("node_modules.bak")), []);
});

test("a successful upgrade keeps the new tree and the backup", async (t) => {
  const root = await fixture();
  t.after(() => rm(root, { recursive: true, force: true }));
  // A working installer lays down the pinned version, then the check passes.
  const fakeInstall = path.join(root, "fake-install.mjs");
  await writeFile(fakeInstall, `import { mkdirSync, writeFileSync } from "node:fs";\nimport path from "node:path";\nconst manifest = path.join(process.cwd(), "node_modules", "@deepseek-ai", "dsh-web-app");\nmkdirSync(manifest, { recursive: true });\nwriteFileSync(path.join(manifest, "package.json"), JSON.stringify({ version: "0.1.5-rc.1" }));\nwriteFileSync(path.join(process.cwd(), "node_modules", "marker.txt"), "new tree");\n`);
  const result = await upgrade(root, { GEO_UPGRADE_INSTALL: `"${process.execPath}" "${fakeInstall}"`, GEO_UPGRADE_CHECK: "exit 0" });
  assert.equal(result.code, 0, result.stdout);
  assert.match(result.stdout, /完成：@deepseek-ai\/dsh-web-app 0.1.5-rc\.1/);
  assert.equal(await readFile(path.join(root, "node_modules", "marker.txt"), "utf8"), "new tree");
  const backups = (await readdir(root)).filter((name) => name.startsWith("node_modules.bak-"));
  assert.equal(backups.length, 1, "升级成功后应保留一份可回滚的备份");
  assert.equal(await readFile(path.join(root, backups[0], "marker.txt"), "utf8"), "old tree");
});

test("a version mismatch after a clean install is treated as a failure", async (t) => {
  const root = await fixture();
  t.after(() => rm(root, { recursive: true, force: true }));
  // Installer "succeeds" but leaves the wrong version in place: that must not be accepted.
  const fakeInstall = path.join(root, "fake-install.mjs");
  await writeFile(fakeInstall, `import { mkdirSync, writeFileSync } from "node:fs";\nimport path from "node:path";\nconst manifest = path.join(process.cwd(), "node_modules", "@deepseek-ai", "dsh-web-app");\nmkdirSync(manifest, { recursive: true });\nwriteFileSync(path.join(manifest, "package.json"), JSON.stringify({ version: "0.1.4-rc.1" }));\n`);
  const result = await upgrade(root, { GEO_UPGRADE_INSTALL: `"${process.execPath}" "${fakeInstall}"`, GEO_UPGRADE_CHECK: "exit 0" });
  assert.equal(result.code, 1, result.stdout);
  assert.match(result.stdout, /与 pin 0\.1\.5-rc\.1 不一致/);
  assert.equal(await readFile(path.join(root, "node_modules", "marker.txt"), "utf8"), "old tree");
});

test("a failed install puts the previous dependency tree back", async (t) => {
  const root = await fixture();
  t.after(() => rm(root, { recursive: true, force: true }));
  // The injected installer writes a partial tree and then fails, like a broken install.
  const fakeInstall = path.join(root, "fake-install.mjs");
  await writeFile(fakeInstall, `import { mkdirSync, writeFileSync } from "node:fs";\nimport path from "node:path";\nmkdirSync(path.join(process.cwd(), "node_modules", "@deepseek-ai", "dsh-web-app"), { recursive: true });\nwriteFileSync(path.join(process.cwd(), "node_modules", "partial.txt"), "half");\nprocess.exit(3);\n`);
  const result = await upgrade(root, { GEO_UPGRADE_INSTALL: `"${process.execPath}" "${fakeInstall}"`, GEO_UPGRADE_CHECK: "exit 0" });
  assert.equal(result.code, 1, result.stdout);
  assert.match(result.stdout, /升级失败/);
  // The old tree is back, marker and all; the broken one is kept aside for inspection.
  assert.equal(await readFile(path.join(root, "node_modules", "marker.txt"), "utf8"), "old tree");
  assert.equal((await readdir(root)).some((name) => name.startsWith("node_modules.failed-")), true);
  assert.equal((await readdir(root)).some((name) => name.startsWith("node_modules.bak-")), false);
});
