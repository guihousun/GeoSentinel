/**
 * Role binding for delegated specialists.
 *
 * The product's four-role rule is a *tool* boundary, not a prompt request: a specialist
 * runs with its own role table only, never the supervisor's whole surface. Binding used
 * to happen purely by polling the native subagent catalog, and that polling can miss a
 * child entirely — a one-shot specialist finishes in seconds, and the catalog resolves
 * rows through live projections, so a child that has already settled is not listed any
 * more. Measured on 0.1.5: a delegated 数据助手 ran with the supervisor's 74 tools,
 * including `subagent` / `send_message`, because it was never bound.
 *
 * This module therefore exposes one binder used by both paths:
 *  - the native `subagent/start` event, which fires at spawn time (`{ id: childId }` plus
 *    the parent agent), and
 *  - the catalog poll, which still covers children that started before we subscribed.
 *
 * The role comes from the delegation label the supervisor wrote ("数据助手：…"), falling
 * back to the child's own first user message, so a supervisor that names the role in the
 * prompt body still gets a bound specialist.
 */
export function createRoleBinder({ store, agents, sessionController, roles, roleTools, report, toStored = (role) => role }) {
  const recorded = (chatId) =>
    new Map(store.db.prepare("SELECT id,role FROM agent_sessions WHERE chat_id=?").all(chatId).map((row) => [row.id, row.role]));

  async function resolveRole(childId, label) {
    const fromLabel = roles.find((name) => String(label ?? "").includes(name));
    if (fromLabel) return fromLabel;
    try {
      const child = await sessionController.inspect(childId);
      const first = (child.events ?? []).find((event) => event.type === "user/message");
      const body = JSON.stringify(first?.data?.content ?? "");
      return roles.find((name) => body.includes(name));
    } catch {
      return undefined;
    }
  }

  /**
   * Bind one child if it is not bound yet.
   * @returns the resolved role, or undefined when the child could not be attributed.
   */
  async function bind(chatId, childId, label) {
    if (typeof chatId !== "string" || typeof childId !== "string" || childId === "") return undefined;
    if (recorded(chatId).has(childId)) return undefined;
    const role = await resolveRole(childId, label);
    if (role === undefined) return undefined;
    try { store.recordChild(chatId, childId, toStored(role)); } catch { return undefined; }
    const table = roleTools[role];
    const child = agents?.get?.(childId);
    if (child && Array.isArray(table) && table.length) {
      try { child.ctx.tools.restrict({ allow: table }); }
      catch (error) { report(`GeoSentinel: 专家工具限制未生效：${error.message}`); }
    }
    return role;
  }

  /** Bind every unbound child the native catalog lists for one chat. */
  async function bindListed(chatId, entries) {
    let unbound;
    for (const entry of entries ?? []) {
      if (entry?.kind !== "child" || !entry.id) continue;
      const role = await bind(chatId, entry.id, entry.label);
      if (role === undefined && !recorded(chatId).has(entry.id)) unbound ??= entry;
    }
    return unbound;
  }

  return { bind, bindListed, resolveRole };
}

/** The parent session id behind whatever the runtime handed the listener. */
export function parentSessionIdOf(parent) {
  if (typeof parent === "string") return parent;
  const candidate = parent?.sessionId ?? parent?.session?.id ?? parent?.id;
  return typeof candidate === "string" && candidate !== "" ? candidate : undefined;
}
