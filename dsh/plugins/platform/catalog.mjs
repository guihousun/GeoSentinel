import { GIS_TOOL_NAMES } from "../research/gis-tools.mjs";
import { loadSkills } from "./skills.mjs";

/**
 * The role → delegation-tool mapping, stated once. Each value is the `toolName` of one
 * product row mounted on the native `@deepseek-ai/dsh-tool-subagent` plugin, and that
 * row is what pins the child's persona and tool table — so a specialist is composed AT
 * SPAWN and its very first request already carries its own tools. The rows live in the
 * product's own agent preset (`profile/agent-presets/geosentinel/agent.cordis.yml`,
 * selected as the default by the profile and delivered into the preset roster's user
 * root at boot); the release guard and the tests read THIS table rather than restating
 * it. The generic `subagent` tool carries no persona/toolFilter, so a child spawned
 * through it kept the supervisor's whole surface (measured: 74 tools where the role
 * table has 21) — it is therefore not in `TEAM_TOOLS`, and neither is `subagent_fork`.
 */
export const ROLE_DELEGATION = {
  数据助手: "delegate_data",
  分析助手: "delegate_analysis",
  事件助手: "delegate_event",
};

// The published capability ceiling of the ordinary-user product. The platform
// guard and the agent restriction read these lists; the admin-mode capability
// panel renders them so the administrator sees exactly what a release exposes.
export const TEAM_TOOLS = [
  // One delegation tool per research role. The control tools stay: they address
  // children that already exist and cannot create one.
  ...Object.values(ROLE_DELEGATION),
  "send_message",
  "list_agents",
  "interrupt_agent",
];
/** The native plan-mode exit tool: it is how the supervisor submits its plan. */
export const PLAN_TOOLS = ["exit_plan_mode"];
export const DOMAIN_TOOLS = [
  "geo_list_files",
  "geo_read_evidence",
  "geo_write_report",
  "geo_write_evidence",
  "geo_inspect_raster",
  "geo_execute_python",
  "geo_download_gee",
  "geo_download_boundary",
  ...GIS_TOOL_NAMES,
];
/** Read-only native filesystem tools, fenced to the chat workspace, project inputs and skills. */
export const FS_READ_TOOLS = ["read", "glob", "grep"];
/**
 * Native write tools. They exist so an agent can materialise a script, table or
 * note of its own instead of only calling the registered `geo_*` writers; the
 * path fence restricts them to the current chat's `outputs/`, so uploads,
 * project `inputs/`, `memory/` and the skill library remain read-only.
 */
export const FS_WRITE_TOOLS = ["write", "edit"];
/** Source verification over the web: the supervisor and the event tracker hold these. */
export const WEB_TOOLS = ["web_search", "web_fetch"];
/**
 * Remote MCP tools (streamable-http servers in the product profile). Both
 * servers are read-only queries against external services:
 *  - NASA CMR: official Earthdata catalog — collections, granules, variables,
 *    citations. This is how "never guess a dataset or band" is satisfied.
 *  - Amap: address↔coordinate, POI search, distance and routing.
 * Results are EXTERNAL sources: cite the query and its time, never present them
 * as platform observations. The Amap client-schema tools (maps_schema_*) only
 * build app deep links and stay out of the product on purpose.
 */
export const MCP_TOOLS = [
  "mcp__cmr__get_collections",
  "mcp__cmr__get_granules",
  "mcp__cmr__get_variables",
  "mcp__cmr__get_keywords",
  "mcp__cmr__get_citations",
  "mcp__cmr__get_services",
  "mcp__cmr__get_tools",
  "mcp__amap__maps_geo",
  "mcp__amap__maps_regeocode",
  "mcp__amap__maps_text_search",
  "mcp__amap__maps_around_search",
  "mcp__amap__maps_search_detail",
  "mcp__amap__maps_distance",
  "mcp__amap__maps_direction_driving",
  "mcp__amap__maps_direction_walking",
  "mcp__amap__maps_direction_transit_integrated",
];
/** Document reading for user uploads (PDF/DOCX/XLSX). */
export const DOCUMENT_TOOLS = ["read_document"];
/** Visualisation surface from the genui plugin; supervisor only. */
export const VISUAL_TOOLS = ["render_ui", "validate_dsh_ui"];
/** Every tool the supervisor may call, before the administrator's policy narrows it. */
export const MAIN_TOOLS = [
  ...TEAM_TOOLS, ...DOMAIN_TOOLS, ...FS_READ_TOOLS, ...FS_WRITE_TOOLS, ...WEB_TOOLS, ...DOCUMENT_TOOLS, ...VISUAL_TOOLS, ...MCP_TOOLS, ...PLAN_TOOLS,
  "ask_user_question", "skill",
];

/**
 * Capability groups the admin-mode panel manages: the supervisor, each research
 * role, and the shipped skill library.
 * @param product - parsed `dsh/profile/product.json`.
 * @param skillRoot - absolute shipped skill directory.
 */
export function capabilityCatalog(product, skillRoot) {
  const roles = product?.roleTools ?? {};
  return [
    { id: "main", label: "研究主管", kind: "tool", items: [...MAIN_TOOLS] },
    ...Object.entries(roles).map(([role, tools]) => ({ id: role, label: role, kind: "tool", items: [...tools] })),
    { id: "skills", label: "技能库", kind: "skill", items: loadSkills(skillRoot).map((skill) => ({ name: skill.name, description: skill.description })) },
  ];
}
