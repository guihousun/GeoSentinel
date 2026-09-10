import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import { capabilityHandler } from "../plugins/developer/index.mjs";
import { readPolicy } from "../plugins/platform/capability-policy.mjs";

function fixture(t) {
  const dir = mkdtempSync(path.join(tmpdir(), "geo-capabilities-"));
  const source = path.join(dir, "source");
  const home = path.join(dir, "home");
  mkdirSync(path.join(source, "dsh/profile"), { recursive: true });
  mkdirSync(path.join(source, "dsh/skills/demo-skill"), { recursive: true });
  writeFileSync(path.join(source, "dsh/profile/product.json"), JSON.stringify({
    schema: "geosentinel.product.v1",
    defaultModel: { provider: "deepseek-official", model: "deepseek-v4-flash" },
    monitorEnabled: true,
    roleTools: { 分析助手: ["geo_inspect_raster", "read"] },
  }));
  writeFileSync(path.join(source, "dsh/skills/demo-skill/SKILL.md"), "---\nname: demo-skill\ndescription: 演示技能\n---\n\n正文内容\n");
  mkdirSync(path.join(home, "geosentinel"), { recursive: true });
  const previous = { source: process.env.GEO_ADMIN_DEV_SOURCE, data: process.env.GEO_DATA_DIR, home: process.env.GEO_DSH_HOME };
  process.env.GEO_ADMIN_DEV_SOURCE = source;
  process.env.GEO_DATA_DIR = path.join(home, "geosentinel");
  delete process.env.GEO_DSH_HOME;
  t.after(() => {
    for (const [key, value] of [["GEO_ADMIN_DEV_SOURCE", previous.source], ["GEO_DATA_DIR", previous.data], ["GEO_DSH_HOME", previous.home]])
      if (value === undefined) delete process.env[key]; else process.env[key] = value;
    rmSync(dir, { recursive: true, force: true });
  });
  return { handler: capabilityHandler(), policyFile: path.join(home, "capability-policy.json") };
}

const call = async (handler, method, body) => {
  const chunks = body === undefined ? [] : [Buffer.from(JSON.stringify(body))];
  const req = { method, headers: { host: "127.0.0.1:8514" }, async *[Symbol.asyncIterator]() { yield* chunks; } };
  let status = 0, payload = "";
  const res = { writeHead(code) { status = code; }, end(text) { payload = text; } };
  await handler(req, res);
  return { status, value: payload === "" ? null : JSON.parse(payload) };
};

test("the admin capability panel lists published capabilities and narrows them", async (t) => {
  const { handler, policyFile } = fixture(t);
  const initial = await call(handler, "GET");
  assert.equal(initial.status, 200);
  assert.deepEqual(initial.value.groups.map((group) => group.id), ["main", "分析助手", "skills"]);
  assert.ok(initial.value.groups[0].items.some((item) => item.name === "geo_list_files" && item.enabled));
  assert.deepEqual(initial.value.groups[1].items.map((item) => item.name), ["geo_inspect_raster", "read"]);
  assert.deepEqual(initial.value.groups[2].items, [{ name: "demo-skill", description: "演示技能", enabled: true }]);
  assert.equal(initial.value.revision, 0);

  const off = await call(handler, "POST", { group: "main", name: "geo_execute_python", enabled: false });
  assert.equal(off.status, 200);
  assert.equal(off.value.groups[0].items.find((item) => item.name === "geo_execute_python").enabled, false);
  assert.equal(off.value.revision, 1);
  assert.deepEqual(readPolicy(policyFile).main, ["geo_execute_python"]);

  const role = await call(handler, "POST", { group: "分析助手", name: "read", enabled: false });
  assert.equal(role.status, 200);
  assert.equal(role.value.groups[1].items.find((item) => item.name === "read").enabled, false);
  assert.deepEqual(readPolicy(policyFile).roles, { 分析助手: ["read"] });

  const skill = await call(handler, "POST", { group: "skills", name: "demo-skill", enabled: false });
  assert.equal(skill.status, 200);
  assert.deepEqual(readPolicy(policyFile).skills, ["demo-skill"]);

  assert.equal((await call(handler, "POST", { group: "main", name: "bash", enabled: false })).status, 400);
  assert.equal((await call(handler, "POST", { group: "main" })).status, 400);
  assert.equal((await call(handler, "PUT")).status, 405);
  const foreign = { method: "GET", headers: { host: "evil.example" } };
  let status = 0;
  await handler(foreign, { writeHead(code) { status = code; }, end() {} });
  assert.equal(status, 403);
});
