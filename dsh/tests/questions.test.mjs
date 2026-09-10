import test from "node:test";
import assert from "node:assert/strict";
import { QuestionTransport } from "../plugins/platform/questions.mjs";

const user = { id: "alice" }, other = { id: "bob" };
const items = [{ id: "choice", question: "选择范围", options: [{ label: "全国" }, { label: "省级" }] }];

// Plan review is native (plan mode + the plan UI), so this transport only carries
// the model's own clarifying questions: skipping a question is a native ability.
test("a generic question may be answered by skipping it", async () => {
  const transport = new QuestionTransport({});
  const waiting = transport.wait(user, "chat", items);
  await transport.answer(user, "chat", transport.snapshot(user, "chat").id, { answers: [{ id: "choice", selected: [] }] });
  assert.deepEqual((await waiting).answers[0].selected, []);
  transport.close();
});

test("question transport isolates owner, rejects forged and duplicate answers, and resolves native answer", async () => {
  const transport = new QuestionTransport({});
  const waiting = transport.wait(user, "chat", items);
  const { id } = transport.snapshot(user, "chat");
  assert.equal(transport.snapshot(other, "chat"), null);
  await assert.rejects(transport.answer(other, "chat", id, { answers: [] }), { status: 409 });
  await assert.rejects(transport.answer(user, "chat", id, { answers: [{ id: "choice", selected: ["forged"] }] }), { status: 400 });
  await assert.rejects(transport.answer(user, "chat", id, { answers: [] }), { status: 400 });
  const answer = { answers: [{ id: "choice", selected: ["省级"] }] };
  await transport.answer(user, "chat", id, answer);
  assert.deepEqual(await waiting, answer);
  await assert.rejects(transport.answer(user, "chat", id, answer), { status: 409 });
});

test("only one question may wait per chat", async () => {
  const transport = new QuestionTransport({});
  const waiting = transport.wait(user, "chat", items);
  assert.throws(() => transport.wait(user, "chat", items), { status: 409 });
  transport.close();
  await assert.rejects(waiting, { code: "ASK_ABORTED" });
});

test("request lifetime rejects stale responses and native free-text answers are supported", async () => {
  const transport = new QuestionTransport({}), controller = new AbortController();
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
