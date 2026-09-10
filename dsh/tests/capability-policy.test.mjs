import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import {
  EMPTY_POLICY, allowedTools, parsePolicy, policyPath, readPolicy, sharedHome, skillEnabled, writePolicy,
} from "../plugins/platform/capability-policy.mjs";

test("the capability policy narrows a published ceiling and never extends it", () => {
  const published = ["geo_list_files", "read", "web_search", "render_ui"];
  const policy = parsePolicy(JSON.stringify({
    revision: 4,
    main: { disabled: ["web_search"] },
    roles: { 分析助手: { disabled: ["read"] } },
    skills: { disabled: ["thematic-modeling"] },
  }));
  assert.deepEqual(allowedTools(policy, "main", published), ["geo_list_files", "read", "render_ui"]);
  // A role inherits the global switch-offs plus its own.
  assert.deepEqual(allowedTools(policy, "分析助手", published), ["geo_list_files", "render_ui"]);
  assert.deepEqual(allowedTools(policy, "事件助手", published), ["geo_list_files", "read", "render_ui"]);
  // Names outside the published list can never appear.
  assert.deepEqual(allowedTools(policy, "main", ["geo_list_files"]), ["geo_list_files"]);
  assert.equal(skillEnabled(policy, "thematic-modeling"), false);
  assert.equal(skillEnabled(policy, "gee-dataset-selection"), true);
});

test("policy documents are validated, versioned and round-trip through disk", (t) => {
  const dir = mkdtempSync(path.join(tmpdir(), "geo-policy-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const file = path.join(dir, "capability-policy.json");
  assert.deepEqual(readPolicy(file), EMPTY_POLICY);
  assert.throws(() => parsePolicy("[]"), /JSON 对象/);
  assert.throws(() => parsePolicy(JSON.stringify({ main: { disabled: "read" } })), /字符串数组/);
  assert.throws(() => parsePolicy(JSON.stringify({ skills: { disabled: [1] } })), /字符串数组/);
  const written = writePolicy(file, { revision: 2, main: ["read", "read"], roles: { 分析助手: ["glob"] }, skills: ["genui"] });
  assert.equal(written.revision, 3);
  assert.deepEqual(written.main, ["read"]);
  assert.deepEqual(readPolicy(file), written);
  // A malformed file is treated as "nothing disabled" instead of breaking boot.
  writeFileSync(file, "{not json");
  assert.deepEqual(readPolicy(file), EMPTY_POLICY);
});

test("the policy file is shared between the product instance and admin workers", () => {
  const home = path.join("D:", "geo-home");
  assert.equal(sharedHome({ GEO_DATA_DIR: path.join(home, "geosentinel") }), home);
  assert.equal(sharedHome({ GEO_ADMIN_DEV_HOME: path.join(home, "development", "admin-id") }), home);
  assert.equal(sharedHome({ GEO_DSH_HOME: home, GEO_DATA_DIR: "D:/other" }), home);
  assert.equal(policyPath({ GEO_DATA_DIR: path.join(home, "geosentinel") }), path.join(home, "capability-policy.json"));
  assert.equal(policyPath({}), null);
});
