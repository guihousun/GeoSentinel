import { PlatformError } from "./store.mjs";
import { nativeEvent } from "./native-events.mjs";

// Display label for a stored child role. Legacy NTL_* ids from earlier records
// keep resolving so old chats still show the Chinese role name.
const labels = {
  数据助手: "数据助手", 分析助手: "分析助手", 事件助手: "事件助手",
  NTL_Data_Searcher: "数据助手", NTL_Analyst: "分析助手", NTL_Event_Tracker: "事件助手",
};

/** Read-only projection: platform ownership AND native parentage must agree. */
export function readonlySubagents({ store, subagents, sessionController }) {
  function authorize(user, root) {
    const account = store.db.prepare("SELECT disabled FROM users WHERE id=?").get(user.id);
    if (!account || account.disabled) throw new PlatformError(403, "账号不可用");
    store.chat(user, root);
    return new Map(store.listChildren(user, root).map((member) => [member.id, member.role]));
  }
  async function catalog(user, root) {
    authorize(user, root);
    const native = await subagents.remoteExportList(root, new AbortController().signal);
    const members = authorize(user, root);
    return { parentAvailable: native.parentAvailable, entries: native.entries.filter((entry) => members.has(entry.id)).map((entry) =>
      entry.kind === "child" ? { kind: "child", id: entry.id, activity: entry.activity, hasChildren: false,
        mode: entry.mode, label: labels[members.get(entry.id)] } : { kind: "diagnostic", id: entry.id, reason: entry.reason }) };
  }
  async function history(user, root, child) {
    const view = await catalog(user, root);
    const entry = view.entries.find((item) => item.id === child && item.kind === "child");
    if (!entry) throw new PlatformError(404, "子智能体记录不存在");
    const snapshot = await sessionController.inspect(child);
    if (!authorize(user, root).has(child)) throw new PlatformError(404, "子智能体记录不存在");
    return { events: snapshot.events.map(nativeEvent).filter(Boolean), running: entry.activity === "running",
      address: { parentSessionId: root, childSessionId: child, mode: entry.mode }, parentAvailable: view.parentAvailable, readOnly: true };
  }
  return { subagentCatalog: catalog, subagentHistory: history };
}
