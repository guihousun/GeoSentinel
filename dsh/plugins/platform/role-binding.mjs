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
 * back to the child's own durable record, so a supervisor that names the role in the
 * prompt body still gets a bound specialist.
 *
 * Known residual (measured, not hidden): binding happens after creation, so a one-shot
 * child's FIRST request still carries the supervisor's tool surface, and the agent-preset
 * layer re-adds `subagent` to the child on every preparation. The race-free fix is the
 * runtime's own spawn-time composition (`composition: { persona, toolFilter }`), which the
 * native `subagent` tool does not expose — that requires the product to spawn specialists
 * itself, and is tracked as the remaining task for the four-role boundary.
 */
/** Delay offsets (ms) at which the role restriction is (re-)applied after binding. */
export const REAPPLY_MS = [0, 60, 250, 1000];

export function createRoleBinder({ store, agents, sessionController, roles, roleTools, report, toStored = (role) => role, retries = 20, wait = 50, reapplyMs = REAPPLY_MS }) {
  const recorded = (chatId) =>
    new Map(store.db.prepare("SELECT id,role FROM agent_sessions WHERE chat_id=?").all(chatId).map((row) => [row.id, row.role]));

  /**
   * Resolve the role from the child's own durable record: the delegation descriptor the
   * runtime appends to the child session carries the supervisor's description as
   * `label` ("数据助手：…"), and when the supervisor wrote the role in the prompt body
   * instead, the child's first user message still names it.
   *
   * The spawn event itself carries neither, and the child session may not be inspectable
   * yet when it fires, so the caller retries briefly.
   */
  async function resolveRole(childId, label) {
    const fromLabel = roles.find((name) => String(label ?? "").includes(name));
    if (fromLabel) return fromLabel;
    try {
      const child = await sessionController.inspect(childId);
      const events = child.events ?? [];
      const descriptor = events.find((event) => event.type === "subagent/descriptor");
      const described = roles.find((name) => String(descriptor?.data?.label ?? "").includes(name));
      if (described) return described;
      const first = events.find((event) => event.type === "user/message");
      const body = JSON.stringify(first?.data?.content ?? "");
      return roles.find((name) => body.includes(name));
    } catch {
      return undefined;
    }
  }

  const pause = () => new Promise((resolve) => setTimeout(resolve, wait));

  /**
   * Bind one child if it is not bound yet.
   * @returns the resolved role, or undefined when the child could not be attributed.
   */
  async function bind(chatId, childId, label) {
    if (typeof chatId !== "string" || typeof childId !== "string" || childId === "") return undefined;
    if (recorded(chatId).has(childId)) return undefined;
    let role;
    for (let attempt = 0; attempt <= retries; attempt += 1) {
      role = await resolveRole(childId, label);
      if (role !== undefined) break;
      if (attempt < retries) await pause();
    }
    if (role === undefined) return undefined;
    if (recorded(chatId).has(childId)) return undefined;
    try { store.recordChild(chatId, childId, toStored(role)); } catch { return undefined; }
    const table = roleTools[role];
    if (!Array.isArray(table) || table.length === 0) return role;
    // The restriction is re-applied a few times across the creation window. Two limits are
    // measured and worth stating rather than hiding:
    //  - the FIRST request of a one-shot child is already in flight when binding happens,
    //    so it still carries the supervisor's whole surface (74 tools);
    //  - the agent-preset layer re-adds its own delegation rows per preparation, so the
    //    child ends up with its role table PLUS `subagent` (22 tools), not the table alone.
    // Closing both needs the child composed at spawn time — the runtime's create/resume
    // accepts `composition: { persona, toolFilter }`, but the native `subagent` tool does
    // not expose those parameters, so it requires the product to spawn specialists itself.
    for (const delay of reapplyMs) {
      if (delay > 0) await new Promise((resolve) => setTimeout(resolve, delay));
      const child = agents?.get?.(childId);
      if (!child) continue;
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

  /**
   * Bind a child that was just spawned. The runtime's `subagent/start` payload carries
   * only `{ runId, provider, id, local }` — the parent agent is emitted as a second
   * argument, which the event bus does not forward (measured: the listener received the
   * identity while the parent resolved to undefined). The parent is therefore read from
   * the child's own session header (`parentSession`), once the child agent exists.
   * @returns the resolved role, or undefined when nothing could be attributed in time.
   */
  async function bindSpawned(childId) {
    if (typeof childId !== "string" || childId === "") return undefined;
    for (let attempt = 0; attempt <= retries; attempt += 1) {
      const parent = agents?.get?.(childId)?.session?.header?.parentSession;
      if (typeof parent === "string" && parent !== "") return await bind(parent, childId);
      await pause();
    }
    return undefined;
  }

  return { bind, bindSpawned, bindListed, resolveRole };
}

/** The parent session id behind whatever the runtime handed the listener. */
export function parentSessionIdOf(parent) {
  if (typeof parent === "string") return parent;
  const candidate = parent?.sessionId ?? parent?.session?.id ?? parent?.id;
  return typeof candidate === "string" && candidate !== "" ? candidate : undefined;
}
