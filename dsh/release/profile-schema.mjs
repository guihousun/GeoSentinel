import { createRequire } from "node:module";
import { readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

/**
 * The product profile is a Cordis **entry list**, and its YAML dialect is
 * narrower than plain YAML: DSH parses it with `js-yaml`'s JSON schema extended
 * by one custom tag (`@deepseek-ai/dsh-app-boot`, "the entry-list YAML
 * dialect"), where
 *
 *   `!!js <expr>` → `{ __jsExpr: "<expr>" }` → evaluated at entry activation
 *
 * A remote MCP row needs that tag to read its credential from the environment
 * (`url: !!js '`https://…?key=${process.env.AMAP_API_KEY}`'`) instead of
 * freezing a secret into the release. Parsing the profile with a plain schema
 * turns the tag into a **literal string**, so the row would boot with an
 * unusable URL and — because remote rows use `failOnStartupError: false` — fail
 * silently. This module therefore gives the release pipeline the same dialect
 * DSH boots with, so `!!js` survives parse → mutate → re-serialize.
 *
 * Only expressions are supported here, exactly like the loader's own tag: no
 * other YAML type is registered, and the JSON schema keeps the profile JSON-safe.
 */
/**
 * Load `js-yaml` from the same dependency tree the boot loader uses.
 *
 * This module runs both from the source checkout and from inside a frozen
 * snapshot (the frozen platform plugin drives publish), so a top-level
 * `require` is tried first and the content-addressed store second — the same
 * fallback the release runtime uses for a dependency that pnpm does not hoist.
 */
function loadJsYaml() {
  const base = fileURLToPath(import.meta.resolve("@deepseek-ai/dsh-base"));
  const require = createRequire(base);
  try {
    return require("js-yaml");
  } catch (error) {
    if (error.code !== "MODULE_NOT_FOUND") throw error;
  }
  const store = path.join(path.dirname(path.dirname(path.dirname(base))), ".pnpm");
  const entry = (readdirSync(store, { withFileTypes: true }) ?? [])
    .filter((candidate) => candidate.isDirectory() && candidate.name.startsWith("js-yaml@"))
    .map((candidate) => candidate.name)
    .sort()[0];
  if (!entry) throw new Error("发布快照缺少依赖：js-yaml");
  return require(path.join(store, entry, "node_modules/js-yaml"));
}

const yaml = loadJsYaml();

/** Same tag contract as the boot loader's `JsExpr`. */
const JsExpr = new yaml.Type("tag:yaml.org,2002:js", {
  kind: "scalar",
  resolve: (data) => typeof data === "string",
  construct: (data) => ({ __jsExpr: data }),
  predicate: (value) => value instanceof Object && "__jsExpr" in value,
  represent: (value) => value.__jsExpr,
});

/** The entry-list dialect: JSON schema plus the `!!js` expression tag. */
export const profileSchema = yaml.JSON_SCHEMA.extend(JsExpr);

/**
 * Parse a profile/entry-list document in DSH's own dialect.
 * @param text - raw `cordis.patch.yml` / `cordis.yml` contents.
 * @returns the entry list, with `!!js` scalars kept as `{ __jsExpr }` nodes.
 */
export function parseProfile(text) {
  return yaml.load(text, { schema: profileSchema, filename: "cordis.patch.yml" });
}

/**
 * Serialize an entry list back to the same dialect, preserving `__jsExpr`
 * nodes as `!!js` scalars. Quoting is the emitter's decision, so an expression
 * whose text needs quotes (it starts with a backtick) stays valid YAML.
 * @param rows - entry list produced by {@link parseProfile}.
 */
export function stringifyProfile(rows) {
  return yaml.dump(rows, { schema: profileSchema, lineWidth: -1, noRefs: true, quotingType: '"' });
}
