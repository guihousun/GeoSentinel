import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { parseProfile } from "../release/profile-schema.mjs";

const root = fileURLToPath(new URL("..", import.meta.url));
const ROLE_OF_TOOL = { delegate_data: "数据助手", delegate_analysis: "分析助手", delegate_event: "事件助手" };

// The role boundary on DSH 0.1.5 is enforced at SPAWN time by the delegation tool's own
// configuration: each `geo-delegate-*` row pins the child's persona and tool table, so a
// specialist's first request already carries its own tools. A child spawned through the
// generic `subagent` tool keeps the supervisor's whole surface (measured: 74 tools where
// the role table has 21), so these rows are the boundary, not a nicety.
test("each research role has a spawn-time delegation tool with its own persona and tool table", async () => {
  const profile = parseProfile(await readFile(path.join(root, "profile/cordis.patch.yml"), "utf8"));
  const product = JSON.parse(await readFile(path.join(root, "profile/product.json"), "utf8"));
  const inserted = profile.find((row) => Array.isArray(row?.insert))?.insert ?? [];
  const rows = inserted.filter((row) => row.name === "@deepseek-ai/dsh-tool-subagent");
  assert.equal(rows.length, 3, "需要三条按角色的委派工具行");
  const names = rows.map((row) => row.config?.toolName).sort();
  assert.deepEqual(names, Object.keys(ROLE_OF_TOOL).sort());
  for (const row of rows) {
    const role = ROLE_OF_TOOL[row.config.toolName];
    assert.ok(role, row.config.toolName);
    assert.equal(row.config.provider, "spawn");
    assert.equal(row.config.backgroundMode, "continuable");
    assert.ok(String(row.config.persona ?? "").includes(role), `${row.id} 的 persona 应点明 ${role}`);
    // The table must equal the published role table: `product.json` is what the
    // member-restriction fallback and the admin capability panel read, so a drift would
    // silently widen or narrow a specialist's surface.
    assert.deepEqual([...row.config.toolFilter.allow].sort(), [...(product.roleTools?.[role] ?? [])].sort(),
      `${row.id} 的工具表与 product.json.roleTools.${role} 不一致`);
  }
});

test("the supervisor cannot spawn a specialist outside its role composition", async () => {
  const { MAIN_TOOLS, TEAM_TOOLS } = await import("../plugins/platform/catalog.mjs");
  // Spawning tools must be the role-pinned ones only; the generic spawners would create a
  // child with the supervisor's full surface.
  for (const name of ["subagent", "subagent_fork"]) {
    assert.equal(TEAM_TOOLS.includes(name), false, `${name} 会派生无角色组合的子代理，不应在允许清单里`);
    assert.equal(MAIN_TOOLS.includes(name), false, `${name} 不应出现在主管的工具清单里`);
  }
  for (const name of Object.keys(ROLE_OF_TOOL)) assert.ok(MAIN_TOOLS.includes(name), `主管应能用 ${name} 派发`);
  // The control tools address existing children and cannot create one.
  for (const name of ["send_message", "list_agents", "interrupt_agent"]) assert.ok(TEAM_TOOLS.includes(name), name);
});
