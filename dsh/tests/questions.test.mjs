import test from "node:test";
import assert from "node:assert/strict";
import { QuestionTransport } from "../plugins/platform/questions.mjs";

const user = { id: "alice" }, other = { id: "bob" };
const items = [{ id: "choice", question: "选择范围", options: [{ label: "全国" }, { label: "省级" }] }];
test("native skip is allowed for generic questions, while a plan requires a decision", async () => {
  const transport = new QuestionTransport({ approve() {} });
  const generic = transport.wait(user, "chat", items);
  await transport.answer(user, "chat", transport.snapshot(user, "chat").id, { answers: [{ id: "choice", selected: [] }] });
  assert.deepEqual((await generic).answers[0].selected, []);
  const plan = transport.wait(user, "chat", items, null, { teamId: "t", revision: "v1", approve: "全国" });
  await assert.rejects(transport.answer(user, "chat", transport.snapshot(user, "chat").id, { answers: [{ id: "choice", selected: [] }] }), { status: 400 });
  const cancelled = assert.rejects(plan, { code: "ASK_ABORTED" }); transport.close(); await cancelled;
});
test("concurrent plan submissions cannot execute the approval twice", async () => {
  let release, calls = 0;
  const transport = new QuestionTransport({ approve: () => { calls++; return new Promise((resolve) => { release = resolve; }); } });
  const pending = transport.wait(user, "chat", items, null, { teamId: "t", revision: "v1", approve: "全国" });
  const id = transport.snapshot(user, "chat").id, answer = { answers: [{ id: "choice", selected: ["全国"] }] };
  const first = transport.answer(user, "chat", id, answer);
  await assert.rejects(transport.answer(user, "chat", id, answer), { status: 409 });
  release(); await first; await pending; assert.equal(calls, 1);
});
test("question transport isolates owner, rejects forged and duplicate answers, and resolves native answer", async () => {
  const transport = new QuestionTransport({ approve() { throw new Error("generic question must not approve"); } });
  const waiting = transport.wait(user, "chat", items);
  const { id } = transport.snapshot(user, "chat");
  assert.equal(transport.snapshot(other, "chat"), null);
  await assert.rejects(transport.answer(other, "chat", id, { answers: [] }), { status: 409 });
  await assert.rejects(transport.answer(user, "chat", id, { answers: [{ id: "choice", selected: ["forged"] }] }), { status: 400 });
  const answer = { answers: [{ id: "choice", selected: ["省级"] }] };
  await transport.answer(user, "chat", id, answer);
  assert.deepEqual(await waiting, answer);
  await assert.rejects(transport.answer(user, "chat", id, answer), { status: 409 });
});
test("plan answer checks bound revision before resolving; cancellation never approves", async () => {
  let calls = 0;
  const transport = new QuestionTransport({ approve(_user, chat, team, revision) {
    assert.equal(chat, "chat"); assert.equal(team, "team"); assert.equal(revision, "old-revision");
    calls++; throw Object.assign(new Error("方案已变化"), { status: 409 });
  } });
  const waiting = transport.wait(user, "chat", [{ id: "plan", question: "确认？", options: [{ label: "确认" }] }], null,
    { teamId: "team", revision: "old-revision", approve: "确认" });
  const { id } = transport.snapshot(user, "chat");
  await assert.rejects(transport.answer(user, "chat", id, { answers: [{ id: "plan", selected: ["确认"] }] }), { status: 409 });
  assert.equal(calls, 1); assert.equal(transport.snapshot(user, "chat").id, id);
  const rejected = assert.rejects(waiting, { code: "ASK_CANCELLED" });
  await transport.answer(user, "chat", id, null, true); await rejected;
  assert.equal(calls, 1);
});
test("request lifetime rejects stale responses and native free-text answers are supported", async () => {
  const transport = new QuestionTransport({ approve() {} }), controller = new AbortController();
  const waiting = transport.wait(user, "chat", items, controller.signal);
  const old = transport.snapshot(user, "chat").id;
  const rejected = assert.rejects(waiting, { code: "ASK_ABORTED" });
  controller.abort(); await rejected;
  const next = transport.wait(user, "chat", [{ id: "detail", question: "补充需求" }]);
  await assert.rejects(transport.answer(user, "chat", old, { answers: [] }), { status: 409 });
  const reply = { answers: [{ id: "detail", selected: [], custom: "2020 年" }] };
  await transport.answer(user, "chat", transport.snapshot(user, "chat").id, reply);
  assert.deepEqual(await next, reply);
});
