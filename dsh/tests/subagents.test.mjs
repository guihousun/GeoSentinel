import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { PlatformStore } from "../plugins/platform/store.mjs";
import { readonlySubagents } from "../plugins/platform/subagents.mjs";
import { sidebarChat } from "../plugins/platform/sidebar-adapter.mjs";

test("native child records require account ownership and durable parentage, without resuming agents", async (t) => {
  const dir = mkdtempSync(path.join(tmpdir(), "geo-children-")), store = new PlatformStore(dir);
  t.after(() => { store.close(); rmSync(dir, { recursive: true, force: true }); });
  const alice = store.bootstrapAdmin("alice", "test-password");
  const bob = store.acceptInvite(store.invite(alice), "bob", "test-password");
  const root = store.createChat(alice, store.createProject(alice, "Study").id).id;
  const other = store.createChat(alice, store.listProjects(alice)[0].id).id;
  store.recordChild(root, "child", "NTL_Data_Searcher");
  store.recordChild(root, "wrong-parent", "NTL_Analyst");
  assert.equal(sidebarChat(alice, "child", store).id, root);
  assert.throws(() => sidebarChat(bob, "child", store), { status: 404 });
  assert.throws(() => sidebarChat(alice, "foreign", store), { status: 404 });
  const native = { entries: [{ kind: "child", id: "child", mode: "continuable", activity: "inactive", hasChildren: false },
    { kind: "child", id: "foreign", mode: "continuable", activity: "running" }], parentAvailable: false };
  let reads = 0, revoke = () => {};
  const bridge = readonlySubagents({ store, subagents: { async remoteExportList() { return native; } },
    sessionController: { async inspect(id) { assert.equal(id, "child"); reads++; revoke(); return { events: [
      { type: "provider/request", data: { secret: "never-display" } },
      { type: "assistant/message", data: { message: { content: [{ type: "text", text: "已检查影像" }] } } },
    ] }; } } });
  const catalog = await bridge.subagentCatalog(alice, root);
  assert.equal(catalog.entries.length, 1);
  assert.equal(catalog.entries[0].label, "数据助手");
  assert.equal(catalog.entries[0].activity, "inactive");
  const history = await bridge.subagentHistory(alice, root, "child");
  assert.equal(history.events.length, 1); assert.equal(history.readOnly, true); assert.equal(history.running, false);
  await assert.rejects(bridge.subagentCatalog(bob, root), { status: 404 });
  await assert.rejects(bridge.subagentHistory(alice, other, "child"), { status: 404 });
  await assert.rejects(bridge.subagentHistory(alice, root, "wrong-parent"), { status: 404 });
  await assert.rejects(bridge.subagentHistory(alice, root, "foreign"), { status: 404 });
  assert.equal(reads, 1);
  revoke = () => store.db.prepare("UPDATE users SET disabled=1 WHERE id=?").run(alice.id);
  await assert.rejects(bridge.subagentHistory(alice, root, "child"), { status: 403 });
});
