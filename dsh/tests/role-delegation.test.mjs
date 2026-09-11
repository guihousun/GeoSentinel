import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { parseProfile } from "../release/profile-schema.mjs";
import { validatePreset } from "../release/manager.mjs";
import { PRESET_FILES, presetSourceDir } from "../plugins/platform/agent-preset.mjs";

const root = fileURLToPath(new URL("..", import.meta.url));

// The role boundary is enforced at SPAWN time by the delegation tool's own configuration:
// each role row pins the child's persona and tool table, so a specialist's first request
// already carries its own tools. A child spawned through the generic `subagent` row keeps
// the supervisor's whole surface (measured: 74 tools where the role table has 21), so
// those rows are the boundary, not a nicety.
//
// The rows live on the AGENT plane — in the product's own preset — because a preset is
// what composes a spawned agent, and because the mounted preset would otherwise add the
// vendor's generic rows back on top of any host-plane role rows.
test("each research role has a spawn-time delegation row in the default preset", async () => {
  const profile = parseProfile(await readFile(path.join(root, "profile/cordis.patch.yml"), "utf8"));
  const row = profile.find((entry) => entry?.id === "agent-presets");
  assert.ok(row, "profile 缺少 agent-presets 行");
  assert.notEqual(row.disabled, true, "默认 agent preset 行不能禁用");
  const product = JSON.parse(await readFile(path.join(root, "profile/product.json"), "utf8"));
  const dir = path.join(root, "profile/agent-presets", row.config.default);
  // The delivery path and this test must agree, or the release would ship a preset the
  // boot copy never writes.
  assert.equal(presetSourceDir(), `${dir}${path.sep}`, "profile 的默认 preset 与交付目录不一致");
  for (const name of PRESET_FILES) await readFile(path.join(dir, name));
  // validatePreset is the release guard: three enabled role rows whose persona names the
  // role and whose table equals the published role table, no generic or external spawner,
  // and host shell plus vendor fan-out orchestration left closed.
  assert.ok(validatePreset(await readFile(path.join(dir, "agent.cordis.yml"), "utf8"), product).length > 0);
});

test("the role rows are not mounted a second time on the host plane", async () => {
  const profile = parseProfile(await readFile(path.join(root, "profile/cordis.patch.yml"), "utf8"));
  const installed = profile.flatMap((row) => (Array.isArray(row?.insert) ? row.insert : row?.config && Array.isArray(row.config) ? row.config : [row]));
  const spawners = installed.filter((row) => row?.name === "@deepseek-ai/dsh-tool-subagent");
  assert.deepEqual(spawners.map((row) => row?.id), [], "profile 不得再挂角色委派行（与 preset 重复注册同名工具）");
});

test("the supervisor cannot spawn a specialist outside its role composition", async () => {
  const { MAIN_TOOLS, ROLE_DELEGATION, TEAM_TOOLS } = await import("../plugins/platform/catalog.mjs");
  // Spawning tools must be the role-pinned ones only; the generic spawners would create a
  // child with the supervisor's full surface.
  for (const name of ["subagent", "subagent_fork"]) {
    assert.equal(TEAM_TOOLS.includes(name), false, `${name} 会派生无角色组合的子代理，不应在允许清单里`);
    assert.equal(MAIN_TOOLS.includes(name), false, `${name} 不应出现在主管的工具清单里`);
  }
  assert.deepEqual(Object.values(ROLE_DELEGATION).sort(), ["delegate_analysis", "delegate_data", "delegate_event"]);
  for (const name of Object.values(ROLE_DELEGATION)) assert.ok(MAIN_TOOLS.includes(name), `主管应能用 ${name} 派发`);
  // The control tools address existing children and cannot create one.
  for (const name of ["send_message", "list_agents", "interrupt_agent"]) assert.ok(TEAM_TOOLS.includes(name), name);
});
