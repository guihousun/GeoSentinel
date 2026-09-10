import path from "node:path";
import { readFileSync, writeFileSync, renameSync, mkdirSync } from "node:fs";

// Ordinary-user capability policy.
//
// The published release owns the CEILING: the platform allowlist, the shipped
// skills and the role tool tables. This policy is a NARROWING layer the
// administrator manages from the admin-mode workbench, applied without a
// release: it can only switch OFF capabilities that the release already
// published, never add one. The file lives beside the deployment home so the
// product instance and every admin development worker read the same document.
const MAX_ITEMS = 400;

/** Shared deployment home: `<home>/geosentinel` in the product, `<home>/development/<id>` in a worker. */
export function sharedHome(env = process.env) {
  if (env.GEO_DSH_HOME) return env.GEO_DSH_HOME;
  if (env.GEO_DATA_DIR) return path.dirname(env.GEO_DATA_DIR);
  if (env.GEO_ADMIN_DEV_HOME) return path.dirname(path.dirname(env.GEO_ADMIN_DEV_HOME));
  if (env.DSH_HOME) return path.dirname(path.dirname(env.DSH_HOME));
  return null;
}

export function policyPath(env = process.env) {
  const home = sharedHome(env);
  return home === null ? null : path.join(home, "capability-policy.json");
}

const strings = (value, field) => {
  if (value === undefined) return [];
  if (!Array.isArray(value) || value.length > MAX_ITEMS || value.some((item) => typeof item !== "string" || item.length > 200))
    throw new Error(`${field} 必须是字符串数组（最多 ${MAX_ITEMS} 项）`);
  return value;
};

/** Parse and validate a policy document. Throws on malformed input. */
export function parsePolicy(text) {
  const value = JSON.parse(text);
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("策略文件必须是 JSON 对象");
  const main = strings(value.main?.disabled, "main.disabled");
  const skills = strings(value.skills?.disabled, "skills.disabled");
  const roles = {};
  if (value.roles !== undefined) {
    if (!value.roles || typeof value.roles !== "object" || Array.isArray(value.roles)) throw new Error("roles 必须是对象");
    for (const [role, entry] of Object.entries(value.roles))
      roles[role] = strings(entry?.disabled, `roles.${role}.disabled`);
  }
  return { revision: Number.isSafeInteger(value.revision) ? value.revision : 0, main, roles, skills };
}

export const EMPTY_POLICY = Object.freeze({ revision: 0, main: [], roles: {}, skills: [] });

/** Read the policy; a missing or malformed file means "nothing disabled". */
export function readPolicy(file = policyPath()) {
  if (!file) return { ...EMPTY_POLICY };
  try {
    return parsePolicy(readFileSync(file, "utf8"));
  } catch (error) {
    if (error.code !== "ENOENT") console.error("GeoSentinel: 能力策略不可用（按未禁用处理）：" + error.message);
    return { ...EMPTY_POLICY };
  }
}

export function writePolicy(file, policy) {
  const next = { revision: (Number.isSafeInteger(policy.revision) ? policy.revision : 0) + 1,
    main: { disabled: [...new Set(policy.main ?? [])].sort() },
    roles: Object.fromEntries(Object.entries(policy.roles ?? {}).map(([role, list]) => [role, { disabled: [...new Set(list ?? [])].sort() }])),
    skills: { disabled: [...new Set(policy.skills ?? [])].sort() } };
  mkdirSync(path.dirname(file), { recursive: true });
  const temporary = file + ".tmp";
  writeFileSync(temporary, JSON.stringify(next, null, 2));
  renameSync(temporary, file);
  return parsePolicy(JSON.stringify(next));
}

/** Tool names still callable for one scope: published allowlist minus the policy. */
export function allowedTools(policy, scope, published) {
  const disabled = new Set([...(policy?.main ?? []), ...(scope && scope !== "main" ? policy?.roles?.[scope] ?? [] : [])]);
  return [...published].filter((name) => !disabled.has(name));
}

export function skillEnabled(policy, name) {
  return !(policy?.skills ?? []).includes(name);
}
