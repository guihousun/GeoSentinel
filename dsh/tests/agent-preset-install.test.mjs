import test from "node:test";
import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import path from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import {
  PRESET_FILES,
  MARKER_FILE,
  describeInstall,
  installProductPreset,
  presetSourceDir,
  presetTargetDir,
} from "../plugins/platform/agent-preset.mjs";

// A preset is an agent-plane composition, and on the 0.1.5 line the profile names the
// product's own as the default. A running release must not read the deployment's source
// tree, so the platform plugin copies it from the running installation into the preset
// roster's user root at boot — the only root the roster reads that the product owns.
const root = fileURLToPath(new URL("..", import.meta.url));

async function withSource(t, files = {}) {
  const dir = await mkdtemp(path.join(tmpdir(), "geo-preset-src-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  for (const name of PRESET_FILES) await writeFile(path.join(dir, name), files[name] ?? `# ${name}\n`);
  return dir;
}
async function withTarget(t) {
  const dir = await mkdtemp(path.join(tmpdir(), "geo-preset-home-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  return path.join(dir, "user-root", "geosentinel");
}

test("a boot install writes the preset and its marker", async (t) => {
  const source = await withSource(t, { "agent.cordis.yml": "- id: persona\n" });
  const target = await withTarget(t);
  const result = await installProductPreset({ source, target });
  assert.equal(result.status, "installed");
  assert.deepEqual(result.changed, PRESET_FILES);
  assert.equal(await readFile(path.join(target, "agent.cordis.yml"), "utf8"), "- id: persona\n");
  const marker = JSON.parse(await readFile(path.join(target, MARKER_FILE), "utf8"));
  assert.equal(marker.source, source);
  assert.equal(marker.preset, "geosentinel");
  assert.deepEqual(Object.keys(marker.files).sort(), [...PRESET_FILES].sort());
  assert.match(describeInstall(result), /已写入/);
});

test("a second boot rewrites nothing", async (t) => {
  const source = await withSource(t);
  const target = await withTarget(t);
  await installProductPreset({ source, target });
  const before = await stat(path.join(target, "agent.cordis.yml"));
  const result = await installProductPreset({ source, target });
  assert.equal(result.status, "current");
  assert.deepEqual(result.changed, []);
  assert.equal((await stat(path.join(target, "agent.cordis.yml"))).mtimeMs, before.mtimeMs);
});

test("a changed preset is refreshed, and the marker records the new bytes", async (t) => {
  const source = await withSource(t);
  const target = await withTarget(t);
  await installProductPreset({ source, target });
  await writeFile(path.join(source, "agent.cordis.yml"), "- id: persona\n  disabled: true\n");
  const result = await installProductPreset({ source, target });
  assert.equal(result.status, "installed");
  assert.deepEqual(result.changed, ["agent.cordis.yml"]);
  assert.equal(await readFile(path.join(target, "agent.cordis.yml"), "utf8"), "- id: persona\n  disabled: true\n");
  const marker = JSON.parse(await readFile(path.join(target, MARKER_FILE), "utf8"));
  const digest = createHash("sha256").update("- id: persona\n  disabled: true\n").digest("hex");
  assert.equal(marker.files["agent.cordis.yml"], digest);
  assert.equal(await installProductPreset({ source, target }).then((r) => r.status), "current");
});

test("a moved installation updates the marker even when the bytes are identical", async (t) => {
  const first = await withSource(t, { "agent.cordis.yml": "- id: persona\n" });
  const second = await withSource(t, { "agent.cordis.yml": "- id: persona\n" });
  const target = await withTarget(t);
  await installProductPreset({ source: first, target });
  const result = await installProductPreset({ source: second, target });
  assert.equal(result.status, "installed");
  assert.deepEqual(result.changed, []);
  assert.equal(JSON.parse(await readFile(path.join(target, MARKER_FILE), "utf8")).source, second);
});

test("a directory the product did not write is left alone", async (t) => {
  const source = await withSource(t);
  const target = await withTarget(t);
  await mkdir(target, { recursive: true });
  await writeFile(path.join(target, "agent.cordis.yml"), "# 管理员自己的预设\n");
  const result = await installProductPreset({ source, target });
  assert.equal(result.status, "foreign");
  assert.equal(await readFile(path.join(target, "agent.cordis.yml"), "utf8"), "# 管理员自己的预设\n");
  assert.equal(await stat(path.join(target, MARKER_FILE)).then(() => true, () => false), false);
  assert.match(describeInstall(result), /不是产品写入的预设目录/);
  // A marker owned by something else is equally not ours.
  await writeFile(path.join(target, MARKER_FILE), JSON.stringify({ owner: "someone-else", files: {} }));
  assert.equal((await installProductPreset({ source, target })).status, "foreign");
});

test("a failing delivery is reported, never thrown", async (t) => {
  const target = await withTarget(t);
  const missing = path.join(await mkdtemp(path.join(tmpdir(), "geo-preset-none-")), "absent");
  const result = await installProductPreset({ source: missing, target });
  assert.equal(result.status, "failed");
  assert.match(result.reason, /预设源文件不可读/);
  assert.equal(await stat(target).then(() => true, () => false), false);
  assert.match(describeInstall(result), /未写入/);
  assert.equal((await installProductPreset({ source: undefined, target })).status, "failed");
});

test("the delivery path is the preset roster's user root", () => {
  assert.equal(
    presetTargetDir({ DSH_HOME: "D:\\geo-home" }, "C:\\Users\\nobody"),
    path.join("D:\\geo-home", ".agent-presets", "geosentinel"),
  );
  // No DSH_HOME means the documented default home, `~/.dsh` (the roster computes the same
  // root, so a launcher that configures nothing still finds the preset).
  assert.equal(
    presetTargetDir({}, path.join("C:", "Users", "nobody")),
    path.join("C:", "Users", "nobody", ".dsh", ".agent-presets", "geosentinel"),
  );
  assert.equal(presetTargetDir({ DSH_HOME: "   " }, "C:\\Users\\nobody"), path.join("C:\\Users\\nobody", ".dsh", ".agent-presets", "geosentinel"));
  // The shipped source is this checkout's own preset, beside the plugin that delivers it.
  assert.equal(presetSourceDir(), path.join(root, "profile", "agent-presets", "geosentinel") + path.sep);
});
