import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, mkdir, writeFile, readFile, rm } from "node:fs/promises";
import path from "node:path";
import { tmpdir } from "node:os";
import { ReleaseManager, validateProduct, validateProfile, command } from "../release/manager.mjs";

const product = { schema: "geosentinel.product.v1", defaultModel: { provider: "deepseek-official", model: "deepseek-v4-flash" }, monitorEnabled: true };
test("build commands do not inherit product credentials or runtime settings", async (t) => {
  const keys = ["DEEPSEEK_API_KEY", "GEO_DATA_DIR", "DSH_HOME"];
  const original = Object.fromEntries(keys.map((key) => [key, process.env[key]]));
  t.after(() => { for (const key of keys) { if (original[key] === undefined) delete process.env[key]; else process.env[key] = original[key]; } });
  for (const key of keys) process.env[key] = "release-test-only";
  await command(process.execPath, ["-e", `process.exit(Number(${JSON.stringify(keys)}.some(key => process.env[key])))`], process.cwd());
});
test("product publication refuses personal authority, credentials and unsafe profiles", async () => {
  assert.equal(validateProduct(product), product);
  for (const field of ["apiKey", "admin", "fullAccess", "cwd"]) assert.throws(() => validateProduct({ ...product, [field]: true }));
  assert.throws(() => validateProduct({ ...product, roleTools: { 分析助手: ["bash"] } }));
  assert.throws(() => validateProduct({ ...product, roleTools: { 分析助手: ["pwsh"] } }));
  assert.throws(() => validateProduct({ ...product, roleTools: { 分析助手: ["str_replace_editor"] } }));
  assert.throws(() => validateProduct({ ...product, roleTools: { NTL_Analyst: ["geo_inspect_raster"] } }));
  // write/edit are the fenced write surface (outputs/ of the chat only), so a
  // research role may hold them alongside the read-only tools.
  assert.equal(validateProduct({ ...product, roleTools: { 分析助手: ["geo_inspect_raster", "skill", "read", "glob", "write", "edit"] } }).roleTools.分析助手.length, 6);
  assert.equal(validateProduct({ ...product, roleTools: { 事件助手: ["web_search", "web_fetch"] } }).roleTools.事件助手.length, 2);
  assert.equal(validateProduct({ ...product, roleTools: { 分析助手: ["read_document"] } }).roleTools.分析助手.length, 1);
  // Published remote-MCP queries are allowed by name shape; anything that is not
  // a well-formed `mcp__<server>__<tool>` (or a geo_/known tool) stays refused.
  assert.equal(validateProduct({ ...product, roleTools: { 事件助手: ["mcp__amap__maps_geo", "mcp__cmr__get_collections"] } }).roleTools.事件助手.length, 2);
  for (const bad of ["mcp__amap__", "mcp____maps_geo", "mcp__Amap__maps_geo", "mcp__amap__maps geo", "amap__maps_geo"])
    assert.throws(() => validateProduct({ ...product, roleTools: { 事件助手: [bad] } }), /用户研究角色/, bad);
  // render_ui / validate_dsh_ui are supervisor-only rendering tools: a research
  // role must not be able to receive them through the product configuration.
  assert.throws(() => validateProduct({ ...product, roleTools: { 分析助手: ["render_ui"] } }));
  const profile = await readFile(new URL("../profile/cordis.patch.yml", import.meta.url), "utf8");
  const rows = validateProfile(profile);
  assert.ok(rows.length);
  assert.throws(() => validateProfile(profile.replace("- id: connection\n  disabled: true", "- id: connection\n  disabled: false").replace("- id: connection\r\n  disabled: true", "- id: connection\r\n  disabled: false")));
  // The shipped product.json is the published role table: it must stay valid and
  // stay inside what the agent-teams row declares, so a role cannot be granted a
  // tool the release never exposed.
  const shipped = JSON.parse(await readFile(new URL("../profile/product.json", import.meta.url), "utf8"));
  assert.deepEqual(validateProduct(shipped), shipped);
  const declared = rows.find((row) => row.id === "agent-teams").config.roleTools;
  for (const [role, tools] of Object.entries(shipped.roleTools)) {
    for (const tool of tools) assert.ok(declared[role]?.includes(tool), `${role} 的 ${tool} 未在 agent-teams 角色表中声明`);
    assert.ok(tools.includes("write") && tools.includes("edit"), `${role} 缺少受限的 write/edit`);
  }
});
test("snapshot, explicit publish, stale draft rejection and rollback preserve the active version", async (t) => {
  const dir = await mkdtemp(path.join(tmpdir(), "geo-release-")), source = path.join(dir, "source"), fork = path.join(dir, "fork");
  t.after(() => rm(dir, { recursive: true, force: true }));
  for (const folder of ["plugins", "profile", "skills", "monitoring", "docker", "release", "development", "scripts", "cli", "tests"]) await mkdir(path.join(source, "dsh", folder), { recursive: true });
  await mkdir(path.join(source, "packages/ntl_toolkit/src"), { recursive: true });
  for (const folder of ["lib", "skills"]) await mkdir(path.join(fork, folder), { recursive: true });
  await mkdir(path.join(source, "dsh/skills/example-skill"), { recursive: true });
  await writeFile(path.join(source, "dsh/skills/example-skill/SKILL.md"), "---\nname: example-skill\ndescription: fixture\n---\n\nbody\n");
  await writeFile(path.join(source, "dsh/profile/product.json"), JSON.stringify(product));
  await writeFile(path.join(source, "dsh/profile/cordis.patch.yml"), await readFile(new URL("../profile/cordis.patch.yml", import.meta.url)));
  await writeFile(path.join(source, "dsh/package.json"), JSON.stringify({ dependencies: {} }));
  await writeFile(path.join(source, "dsh/pnpm-lock.yaml"), "lockfileVersion: '9.0'\n");
  // The monitor collector mounts this adapter file by path at run time, so the
  // freeze carries it explicitly and refuses to validate without it.
  await mkdir(path.join(source, "monitoring"), { recursive: true });
  await writeFile(path.join(source, "monitoring/sources.py"), "def collect_monitor_candidates():\n    return [], []\n");
  for (const file of ["package.json", "pnpm-lock.yaml", "cordis.patch.yml", "LICENSE"]) await writeFile(path.join(fork, file), file === "package.json" ? '{"version":"test"}' : "test");
  await writeFile(path.join(source, "dsh/.env"), "DO_NOT_PUBLISH=private");
  await writeFile(path.join(source, "dsh/plugins/view.js"), "export const title='first';");
  const manager = new ReleaseManager(source, path.join(dir, "releases"), fork);
  manager.build = async (id) => manager.verify(id);
  const first = await manager.prepare("admin");
  assert.equal(first.status, "validated");
  assert.equal((await manager.state()).active, null);
  assert.equal(Object.keys((await manager.verify(first.id)).hashes).some((name) => name.endsWith(".env")), false);
  assert.ok(Object.keys((await manager.verify(first.id)).hashes).includes("dsh/skills/example-skill/SKILL.md"));
  assert.ok(Object.keys((await manager.verify(first.id)).hashes).includes("monitoring/sources.py"));
  await assert.rejects(manager.request(first.id, "admin"), /预览/);
  let state = await manager.state(); state.candidate.previewed = true; await manager.save(state);
  await manager.request(first.id, "admin");
  assert.equal((await manager.state()).active, null);
  await manager.activated(first.id);
  const original = await readFile(path.join(manager.releaseRoot(first.id), "app/dsh/plugins/view.js"), "utf8");
  await writeFile(path.join(source, "dsh/plugins/view.js"), "export const title='second';");
  assert.equal(await readFile(path.join(manager.releaseRoot(first.id), "app/dsh/plugins/view.js"), "utf8"), original);
  const second = await manager.prepare("admin");
  state = await manager.state(); state.candidate.previewed = true; await manager.save(state);
  await writeFile(path.join(source, "dsh/plugins/view.js"), "export const title='unverified';");
  await assert.rejects(manager.request(second.id, "admin"), /重新生成/);
  await manager.request(first.id, "admin", true);
  assert.equal((await manager.state()).pending.rollback, true);
  await manager.activated(first.id);
  await writeFile(path.join(manager.releaseRoot(first.id), "app/dsh/plugins/view.js"), "tampered");
  await assert.rejects(manager.verify(first.id), /被改动/);
  manager.build = async () => { throw new Error("validation failed"); };
  assert.equal((await manager.prepare("admin")).status, "failed");
  assert.equal((await manager.state()).active, first.id);
});
