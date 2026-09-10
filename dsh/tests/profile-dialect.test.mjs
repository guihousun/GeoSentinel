import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { validateProfile } from "../release/manager.mjs";
import { parseProfile, stringifyProfile } from "../release/profile-schema.mjs";

const require = createRequire(import.meta.resolve("@deepseek-ai/dsh-base"));
const { isJsExpr, evaluate } = require("@deepseek-ai/cordis-plugin-loader");
const profile = await readFile(new URL("../profile/cordis.patch.yml", import.meta.url), "utf8");
const inserts = (rows) => rows.flatMap((row) => row.insert ?? []);

// The release pipeline parses the product profile and writes the boot profile
// again on every launch. A remote MCP row reads its credential from the
// environment through `!!js`, so a pipeline that loses the tag ships a row with
// a literal `https://…?key=${process.env.AMAP_API_KEY}` URL that fails silently
// under `failOnStartupError: false`. These tests pin the dialect.
test("the release pipeline preserves !!js expressions in the boot profile", () => {
  const rows = validateProfile(profile);
  const amap = inserts(rows).find((row) => row.id === "mcp-amap");
  assert.ok(amap, "产品 profile 缺少 mcp-amap 行");
  assert.ok(isJsExpr(amap.config.url), "!!js 在解析阶段必须保持为表达式节点，不能退化成字面量字符串");
});

test("a re-serialized profile keeps !!js readable and evaluable by the DSH loader", () => {
  process.env.AMAP_API_KEY = "profile-dialect-test-key";
  try {
    const rows = validateProfile(profile);
    // the mutation launchRelease performs before writing the boot profile
    rows.push({ id: "agent-default-model", config: { provider: "deepseek-official", model: "deepseek-v4-flash" } });
    const written = stringifyProfile(rows);
    assert.match(written, /url: !!js "`https:\/\/mcp\.amap\.com\/mcp\?key=\$\{process\.env\.AMAP_API_KEY\}`"/, "启动 profile 必须保留 !!js 标签并按 YAML 规则加引号");
    const back = parseProfile(written);
    const amap = inserts(back).find((row) => row.id === "mcp-amap");
    assert.ok(isJsExpr(amap.config.url));
    // The loader evaluates the expression at entry activation: this is the
    // credential substitution the remote MCP server needs.
    assert.equal(evaluate({}, amap.config.url.__jsExpr), "https://mcp.amap.com/mcp?key=profile-dialect-test-key");
    // A plain parse must NOT be mistaken for a working row: the literal string
    // is exactly the failure this dialect prevents.
    assert.notEqual(typeof amap.config.url, "string");
    for (const id of ["connection", "api-remotes", "directory-picker", "tool-cordis", "tool-workflow", "web-runtime"])
      assert.equal(back.find((row) => row.id === id)?.disabled, true, `权限边界行 ${id} 丢失`);
    // Native delegation comes from the session preset, not from a host row, so the
    // preset mechanism is enabled while the picker UI stays hidden.
    assert.equal(back.find((row) => row.id === "agent-presets")?.disabled, false, "agent-presets 必须启用（原生委派依赖它）");
    assert.equal(back.find((row) => row.id === "ui-agent-preset")?.disabled, true, "普通用户不应有预设选择入口");
  } finally { delete process.env.AMAP_API_KEY; }
});
