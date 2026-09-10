import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, mkdir, writeFile, symlink, rm } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { Context } from "@deepseek-ai/cordis";
import { loadSkills, parseSkillFile, registerProductSkills } from "../plugins/platform/skills.mjs";
import { containedPath, containedWrite } from "../plugins/platform/files.mjs";

const root = fileURLToPath(new URL("../skills", import.meta.url));

test("the shipped skill library loads, is well named and carries no legacy identifier", () => {
  const skills = loadSkills(root);
  // The catalog is an explicit contract: a new skill must be a deliberate
  // addition here, not an accidental file drop. Maintainer-facing skills
  // (harness editing, NTL routing regression) stay out of the research catalog.
  assert.deepEqual(skills.map((skill) => skill.name).sort(), [
    "analysis-ready-preprocessing",
    "architecture-and-capability-map",
    "claim-evidence-chain",
    "code-execution-validation",
    "conflict-ntl-workflow",
    "disaster-event-observation-workflow",
    "event-context-and-timeline",
    "event-window-analysis",
    "evidence-synthesis-and-acceptance",
    "gee-acquisition-strategy",
    "gee-dataset-selection",
    "gee-ntl-date-boundary-handling",
    "geospatial-visualization-cjk",
    "latest-observation-availability",
    "ntl-statistics-and-time-series",
    "provenance-and-evidence-boundary",
    "structured-handoff",
    "task-planning-and-routing",
    "temporal-and-aoi-resolution",
    "thematic-modeling",
    "workspace-and-artifact-contract",
  ]);
  const names = new Set();
  for (const skill of skills) {
    assert.match(skill.name, /^[a-z0-9]+(?:-[a-z0-9]+)*$/);
    assert.equal(names.has(skill.name), false, `duplicate skill ${skill.name}`);
    names.add(skill.name);
    assert.ok(skill.description.length > 10, skill.name);
    assert.ok(skill.content.length > 100, skill.name);
    assert.equal(skill.resourceBase.kind, "directory");
    assert.equal(skill.resourceBase.path, path.join(root, skill.name));
    assert.ok(existsSync(path.join(skill.resourceBase.path, "SKILL.md")), skill.name);
    for (const stale of ["NTL_Engineer", "Code_Assistant", "typed_package", "TaskPlan", "NTL_SCRIPT_CONTRACT", "ntl.script.contract"])
      assert.equal(skill.content.includes(stale), false, `${skill.name} still mentions ${stale}`);
  }
  for (const required of ["architecture-and-capability-map", "gee-dataset-selection", "task-planning-and-routing", "event-context-and-timeline", "claim-evidence-chain"])
    assert.ok(names.has(required), required);
});

test("the product library registers into the skill registry layer as runtime skills", async () => {
  const ctx = new Context();
  const registered = [];
  ctx.provide("skills", { register: (skill) => { registered.push(skill); return () => {}; } });
  registerProductSkills(ctx, root);
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(registered.length, 21);
  for (const skill of registered) {
    assert.equal(skill.source, "custom");
    assert.equal(skill.resourceBase.kind, "directory");
    assert.ok(skill.content.length > 100, skill.name);
    assert.equal(typeof skill.description, "string");
  }
  await ctx.stop?.();
});

test("skill frontmatter parsing rejects malformed entries and folds block scalars", () => {
  assert.deepEqual(parseSkillFile("---\nname: demo-skill\ndescription: short\n---\nbody\n", "x"), {
    name: "demo-skill",
    description: "short",
    whenToUse: undefined,
    content: "body",
  });
  const folded = parseSkillFile("---\nname: demo-skill\ndescription: >\n  first line\n  second line\nwhenToUse: use it\n---\nbody\n", "x");
  assert.equal(folded.description, "first line second line");
  assert.equal(folded.whenToUse, "use it");
  assert.throws(() => parseSkillFile("no frontmatter", "x"), /frontmatter/);
  assert.throws(() => parseSkillFile("---\nname: Demo Skill\ndescription: x\n---\nbody", "x"), /invalid name/);
  assert.throws(() => parseSkillFile("---\nname: demo-skill\ndescription:\n---\nbody", "x"), /description/);
});

test("the model-facing filesystem tools stay inside the workspace and the skill library", async (t) => {
  const base = await mkdtemp(path.join(tmpdir(), "geo-fence-"));
  t.after(() => rm(base, { recursive: true, force: true }));
  const workspace = path.join(base, "chat"), skills = path.join(base, "skills"), outside = path.join(base, "outside");
  for (const directory of [workspace, skills, outside]) await mkdir(directory, { recursive: true });
  await writeFile(path.join(outside, "secret.txt"), "no");
  const roots = [workspace, skills];

  assert.equal(containedPath(roots, "inputs/a.tif", workspace), true);
  assert.equal(containedPath(roots, "outputs/job/report.md", workspace), true);
  assert.equal(containedPath(roots, "**/*.md", workspace), true);
  assert.equal(containedPath(roots, path.join(skills, "gee-dataset-selection", "SKILL.md"), workspace), true);
  assert.equal(containedPath(roots, undefined, workspace), true);
  assert.equal(containedPath(roots, "", workspace), true);

  assert.equal(containedPath(roots, "../../outside/secret.txt", workspace), false);
  assert.equal(containedPath(roots, path.join(outside, "secret.txt"), workspace), false);
  assert.equal(containedPath(roots, path.join(base, "..", "secret.txt"), workspace), false);
  assert.equal(containedPath(roots, "a\0b", workspace), false);
  assert.equal(containedPath(roots, "x".repeat(5000), workspace), false);
  assert.equal(containedPath(roots, 42, workspace), false);

  try {
    await symlink(outside, path.join(workspace, "link"), process.platform === "win32" ? "junction" : "dir");
  } catch (error) {
    t.diagnostic(`symlink unavailable: ${error.code}`);
    return;
  }
  assert.equal(containedPath(roots, "link/secret.txt", workspace), false);
});

test("model-authored writes stay inside the chat outputs directory", async (t) => {
  const base = await mkdtemp(path.join(tmpdir(), "geo-write-fence-"));
  t.after(() => rm(base, { recursive: true, force: true }));
  const workspace = path.join(base, "chat"), outside = path.join(base, "outside");
  for (const directory of [path.join(workspace, "outputs"), path.join(workspace, "inputs"), path.join(workspace, ".dsh-uploads", "chat"), path.join(workspace, "memory"), outside])
    await mkdir(directory, { recursive: true });
  await writeFile(path.join(outside, "secret.txt"), "no");

  assert.equal(containedWrite(workspace, "outputs/job/report.md"), true);
  assert.equal(containedWrite(workspace, "outputs/统计表.csv"), true);
  assert.equal(containedWrite(workspace, path.join(workspace, "outputs", "a", "b.txt")), true);

  // Uploads, project material, runtime memory and the skill library are read-only.
  assert.equal(containedWrite(workspace, "inputs/a.tif"), false);
  assert.equal(containedWrite(workspace, ".dsh-uploads/chat/a.pdf"), false);
  assert.equal(containedWrite(workspace, "memory/notes.md"), false);
  assert.equal(containedWrite(workspace, "../inputs/a.tif"), false);
  assert.equal(containedWrite(workspace, "outputs/../../outside/secret.txt"), false);
  assert.equal(containedWrite(workspace, path.join(outside, "secret.txt")), false);
  assert.equal(containedWrite(workspace, ""), false);
  assert.equal(containedWrite(workspace, undefined), false);
  assert.equal(containedWrite(workspace, 42), false);
  assert.equal(containedWrite(workspace, "a\0b"), false);
  assert.equal(containedWrite(workspace, "x".repeat(5000)), false);

  try {
    await symlink(outside, path.join(workspace, "outputs", "link"), process.platform === "win32" ? "junction" : "dir");
  } catch (error) {
    t.diagnostic(`symlink unavailable: ${error.code}`);
    return;
  }
  assert.equal(containedWrite(workspace, "outputs/link/secret.txt"), false);
});
