import { GIS_TOOL_NAMES } from "../research/gis-tools.mjs";
import { loadSkills } from "./skills.mjs";

// The published capability ceiling of the ordinary-user product. The platform
// guard and the agent restriction read these lists; the admin-mode capability
// panel renders them so the administrator sees exactly what a release exposes.
export const TEAM_TOOLS = [
  "agent_teams_create",
  "agent_teams_add_member",
  "agent_teams_remove_member",
  "agent_teams_create_task",
  "agent_teams_reassign_task",
  "agent_teams_claim_task",
  "agent_teams_update_task",
  "agent_teams_send_message",
  "agent_teams_status",
  "agent_teams_resume",
  "agent_teams_delete",
  "agent_teams_edit_plan",
];
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
/** Document reading for user uploads (PDF/DOCX/XLSX). */
export const DOCUMENT_TOOLS = ["read_document"];
/** Visualisation surface from the genui plugin; supervisor only. */
export const VISUAL_TOOLS = ["render_ui", "validate_dsh_ui"];
/** Every tool the supervisor may call, before the administrator's policy narrows it. */
export const MAIN_TOOLS = [
  ...TEAM_TOOLS, ...DOMAIN_TOOLS, ...FS_READ_TOOLS, ...FS_WRITE_TOOLS, ...WEB_TOOLS, ...DOCUMENT_TOOLS, ...VISUAL_TOOLS,
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
