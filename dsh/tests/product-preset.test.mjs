import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { parseProfile } from "../release/profile-schema.mjs";

const root = fileURLToPath(new URL("..", import.meta.url));
const PRESET_DIR = path.join(root, "profile/agent-presets/geosentinel");
const PRESET = path.join(PRESET_DIR, "agent.cordis.yml");
const ROLE_OF_TOOL = { delegate_data: "数据助手", delegate_analysis: "分析助手", delegate_event: "事件助手" };
// Rows the vendor composition enables (or leaves inert) that this product must not
// expose, each carrying its reason in the file itself.
const MUST_STAY_DISABLED = ["tool-bash", "tool-pwsh", "workflow-worker-thread", "tool-workflow", "tool-ralph"];
const NEVER_ENABLED_TOOL_NAMES = ["subagent", "subagent_fork", "subagent_codex", "subagent_claude_code"];

// The product owns its agent preset because agents join their PARENT's preset: a
// specialist composed under the shipped `standard` preset inherits that preset's
// generic `subagent` / `subagent_fork` rows, which carry no persona and no tool
// filter — measured as the role table plus `subagent`, i.e. a specialist that can
// spawn a grandchild no role restriction covers. Presets have no include/override
// mechanism, so the only way to change one row is to own the file.
//
// The preset is prepared but not yet mounted: the product profile still mounts the
// three delegation rows directly. Switching over is a composition change that needs
// its own verified cycle, and these assertions keep the prepared file honest until
// then — including the three deviations from the vendor composition, asserted as
// EFFECTIVE state (a row left `disabled: true` concedes nothing).
function flatten(composition) {
  const rows = [];
  for (const row of composition) {
    if (Array.isArray(row?.config)) rows.push(...row.config);
    else rows.push(row);
  }
  return rows;
}

const enabled = (rows) => rows.filter((row) => row?.disabled !== true);

test("the product preset delegates to exactly three roles and nothing else", async () => {
  const rows = flatten(parseProfile(await readFile(PRESET, "utf8")));
  const product = JSON.parse(await readFile(path.join(root, "profile/product.json"), "utf8"));

  const spawners = enabled(rows).filter((row) => row.name === "@deepseek-ai/dsh-tool-subagent");
  assert.deepEqual(spawners.map((row) => row.config?.toolName).sort(), Object.keys(ROLE_OF_TOOL).sort(),
    "enabled delegation rows must be exactly the three role tools");

  for (const row of spawners) {
    const role = ROLE_OF_TOOL[row.config.toolName];
    assert.equal(row.config.provider, "spawn");
    assert.equal(row.config.backgroundMode, "continuable");
    assert.ok(String(row.config.persona ?? "").includes(role), `${row.id} 的 persona 应点明 ${role}`);
    assert.deepEqual([...row.config.toolFilter.allow].sort(), [...(product.roleTools?.[role] ?? [])].sort(),
      `${row.id} 的工具表与 product.json.roleTools.${role} 不一致`);
  }

  // The supervisor keeps the product's control surface, which the profile's
  // TEAM_TOOLS list also names.
  const ids = enabled(rows).map((row) => row.id);
  for (const id of ["tool-subagent-control", "tool-subagent-list-agents"]) {
    assert.ok(ids.includes(id), `preset 缺少启用的 ${id}`);
  }
});

test("the product preset exposes no generic or external spawner", async () => {
  const rows = flatten(parseProfile(await readFile(PRESET, "utf8")));
  for (const row of enabled(rows)) {
    assert.ok(!NEVER_ENABLED_TOOL_NAMES.includes(row.config?.toolName),
      `${row.id} 不得启用通用/外部 spawner（toolName=${row.config?.toolName}）`);
  }
});

test("the product preset disables host shell and vendor fan-out orchestration", async () => {
  const rows = flatten(parseProfile(await readFile(PRESET, "utf8")));
  for (const id of MUST_STAY_DISABLED) {
    const row = rows.find((candidate) => candidate.id === id);
    assert.ok(row, `preset 缺少 ${id}（保留该行以便看清产品与上游的差异）`);
    assert.equal(row.disabled, true, `${id} 必须保持 disabled: true`);
  }
  // A shell row that is present but enabled would defeat the whole point.
  for (const row of enabled(rows)) {
    assert.ok(row.name !== "@deepseek-ai/dsh-tool-bash" && row.name !== "@deepseek-ai/dsh-tool-pwsh",
      `${row.id} 不得启用宿主机 shell`);
  }
});

test("the product preset's own persona is role-neutral and not a coding-agent framing", async () => {
  const composition = parseProfile(await readFile(PRESET, "utf8"));
  const persona = composition.find((row) => row?.id === "persona");
  assert.ok(persona, "preset 缺少 persona 行");
  const prefix = String(persona.config?.prefix ?? "");
  assert.match(prefix, /GeoSentinel/, "persona 应点明平台身份");
  assert.doesNotMatch(prefix, /coding agent/i,
    "persona 不得沿用上游的编码 Agent 身份描述（本 preset 同时覆盖主管与专家）");
  assert.match(String(persona.config?.suffix ?? ""), /\{\{cwd\}\}/, "persona 应保留工作目录占位符");
});

test("the product preset ships roster metadata", async () => {
  const meta = parseProfile(await readFile(path.join(PRESET_DIR, "preset.yml"), "utf8"));
  assert.equal(typeof meta.name, "string");
  assert.ok(meta.name.length > 0);
  assert.equal(typeof meta.description, "string");
  assert.equal(typeof meta.order, "number");
});
