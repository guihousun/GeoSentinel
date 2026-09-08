import test from "node:test";
import assert from "node:assert/strict";
import { ResearchAdmission } from "../plugins/platform/admission.mjs";

test("research admission caps concurrent users and rejects same-user parallel chats atomically", async () => {
  const gate = new ResearchAdmission(async () => true, 2);
  const results = await Promise.allSettled([
    gate.claim("alice", "a"),
    gate.claim("alice", "b"),
  ]);
  assert.equal(results.filter((x) => x.status === "fulfilled").length, 1);
  await gate.claim("bob", "c");
  await assert.rejects(() => gate.claim("carol", "d"), /并发上限/);
  await gate.claim("bob", "c");
  gate.release("c");
  await gate.claim("carol", "d");
});
