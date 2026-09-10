import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { tmpdir } from "node:os";
import {
  SHARE_MOUNT_ROOT,
  listShare,
  parseShareDirs,
  shareChildren,
  shareContained,
  shareMountTarget,
  shareTarget,
} from "../plugins/platform/share.mjs";
import { shareMounts } from "../plugins/research/docker.mjs";
import { workspaceView, viewEntries, VIEW } from "../plugins/platform/workspace-view.mjs";

async function fixture(t) {
  const root = await mkdtemp(path.join(tmpdir(), "geo-share-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const base = path.join(root, "share");
  await mkdir(path.join(base, "缅甸地理", "缅甸交通"), { recursive: true });
  await mkdir(path.join(base, "边界"), { recursive: true });
  await writeFile(path.join(base, "缅甸地理", "缅甸地理.pdf"), "pdf");
  await writeFile(path.join(base, "缅甸地理", "缅甸交通", "路网.shp"), "shp");
  await writeFile(path.join(base, "缅甸地理", "缅甸交通", "路网.dbf"), "dbf");
  await writeFile(path.join(base, "边界", "gadm36_MMR_0.shp"), "shp");
  await writeFile(path.join(base, "缅甸地理", ".hidden"), "hidden");
  const outside = path.join(root, "outside.txt");
  await writeFile(outside, "no");
  return { root, base, outside };
}

test("shared data configuration accepts one root or several named roots", async (t) => {
  const { root, base } = await fixture(t);
  const single = parseShareDirs({ GEO_SHARE_DIR: base });
  assert.deepEqual(single, [{ name: "", root: path.resolve(base) }]);
  const named = parseShareDirs({ GEO_SHARE_DIRS: `边界=${path.join(base, "边界")};坏/名字=x;=x;边界=other` });
  assert.deepEqual(named.map((entry) => entry.name), ["边界"]);
  // A root that does not exist is still parsed (the listing stays empty) — a typo
  // must show up in the startup log, not silently drop the whole configuration.
  assert.equal(parseShareDirs({ GEO_SHARE_DIRS: `缺=${path.join(base, "nope")}` }).length, 1);
  // Both forms together: the unnamed root is addressed directly, named ones get
  // one extra level.
  assert.equal(parseShareDirs({ GEO_SHARE_DIR: base, GEO_SHARE_DIRS: `缅甸地理=${path.join(base, "缅甸地理")}` }).length, 2);
  // No configuration is a valid state: the feature stays off.
  assert.deepEqual(parseShareDirs({}), []);
  assert.deepEqual(parseShareDirs({ GEO_SHARE_DIR: "   " }), []);
  void root;
});

test("share aliases resolve inside a root and never escape it", async (t) => {
  const { base, outside } = await fixture(t);
  const entries = parseShareDirs({ GEO_SHARE_DIR: base });
  assert.equal(shareTarget(entries, "share/缅甸地理/缅甸地理.pdf"), path.join(base, "缅甸地理", "缅甸地理.pdf"));
  assert.equal(shareTarget(entries, "share\\边界\\gadm36_MMR_0.shp"), path.join(base, "边界", "gadm36_MMR_0.shp"));
  assert.equal(shareTarget(entries, "share/../../outside.txt"), null);
  assert.equal(shareTarget(entries, "share/缅甸地理/../.hidden"), null);
  assert.equal(shareTarget(entries, "share/"), null);
  assert.equal(shareTarget(entries, "inputs/a.tif"), null);
  assert.equal(shareTarget(entries, path.join(outside)), null);
  assert.equal(shareContained(entries, path.join(base, "边界", "gadm36_MMR_0.shp")), true);
  assert.equal(shareContained(entries, outside), false);

  // A named root only answers to its own name, never as a bare subdirectory.
  const named = parseShareDirs({ GEO_SHARE_DIRS: `缅甸地理=${path.join(base, "缅甸地理")}` });
  assert.equal(shareTarget(named, "share/缅甸地理/缅甸地理.pdf"), path.join(base, "缅甸地理", "缅甸地理.pdf"));
  assert.equal(shareTarget(named, "share/边界/gadm36_MMR_0.shp"), null);
});

test("shared data lists without shapefile companions and groups directories", async (t) => {
  const { base } = await fixture(t);
  const listed = await listShare(parseShareDirs({ GEO_SHARE_DIR: base }));
  assert.deepEqual(listed.files.map((file) => file.relative).sort(), [
    "缅甸地理/缅甸地理.pdf", "缅甸地理/缅甸交通/路网.shp", "边界/gadm36_MMR_0.shp",
  ].sort());
  assert.equal(listed.truncated, false);
  const top = shareChildren(listed.files);
  assert.deepEqual(top.directories.map((entry) => entry.name).sort(), ["边界", "缅甸地理"].sort());
  assert.deepEqual(shareChildren(listed.files, "缅甸地理").files.map((entry) => entry.name), ["缅甸地理.pdf"]);
  assert.deepEqual(shareChildren(listed.files, "缅甸地理/缅甸交通").files.map((entry) => entry.name), ["路网.shp"]);
});

test("the explorer exposes shared data as a read-only group", async (t) => {
  const { root, base } = await fixture(t);
  const chatRoot = path.join(root, "chat");
  await mkdir(path.join(chatRoot, "outputs"), { recursive: true });
  const virtualRoot = "/工作区/缅甸案例";
  const view = await workspaceView({
    chatRoot,
    inputsRoot: path.join(chatRoot, "inputs"),
    projectInputsRoot: path.join(root, "project-inputs"),
    share: parseShareDirs({ GEO_SHARE_DIR: base }),
  });
  assert.deepEqual(viewEntries(view, virtualRoot, virtualRoot).map((entry) => entry.name),
    [VIEW.uploads, VIEW.results, VIEW.share, VIEW.history]);
  const top = viewEntries(view, virtualRoot, `${virtualRoot}/${VIEW.share}`);
  assert.deepEqual(top.map((entry) => entry.name).sort(), ["边界", "缅甸地理"].sort());
  const nested = viewEntries(view, virtualRoot, `${virtualRoot}/${VIEW.share}/缅甸地理/缅甸交通`);
  const file = nested.find((entry) => entry.name === "路网.shp");
  assert.equal(file.isDir, false);
  assert.equal(file.real, path.join(base, "缅甸地理", "缅甸交通", "路网.shp"));

  const plain = await workspaceView({ chatRoot, inputsRoot: path.join(chatRoot, "inputs"), projectInputsRoot: path.join(root, "project-inputs") });
  assert.deepEqual(viewEntries(plain, virtualRoot, virtualRoot).map((entry) => entry.name),
    [VIEW.uploads, VIEW.results, VIEW.history]);
});

test("shared data is mounted read-only into the analysis container", async (t) => {
  const { base } = await fixture(t);
  const single = shareMounts(parseShareDirs({ GEO_SHARE_DIR: base }));
  assert.deepEqual(single, ["--mount", `type=bind,source=${path.resolve(base)},target=${SHARE_MOUNT_ROOT},readonly`]);
  const named = shareMounts(parseShareDirs({ GEO_SHARE_DIRS: `缅甸地理=${path.join(base, "缅甸地理")}` }));
  assert.equal(named[1], `type=bind,source=${path.join(base, "缅甸地理")},target=${SHARE_MOUNT_ROOT}/缅甸地理,readonly`);
  assert.equal(shareMountTarget({ name: "" }), SHARE_MOUNT_ROOT);
  // No configuration: no mount at all, so the sandbox is unchanged.
  assert.deepEqual(shareMounts(parseShareDirs({})), []);
  // A path Docker cannot bind is refused loudly instead of silently mounting nothing.
  assert.throws(() => shareMounts([{ name: "", root: "C:/a,b" }]));
});
