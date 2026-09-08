import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, mkdir, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import {
  listEvidenceFiles,
  readEvidence,
  writeReport,
} from "../plugins/research/evidence.mjs";
import { publicEvent } from "../plugins/platform/public-events.mjs";

test("source evidence is bounded, traceable and project-relative", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "geo-evidence-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  await mkdir(path.join(root, "event"));
  await writeFile(
    path.join(root, "event", "notes.md"),
    "# 来源\nhttps://example.org/report\n仅此资料可支持。",
  );
  const source = await readEvidence(root, "event/notes.md");
  assert.equal(source.sha256.length, 64);
  assert.equal(source.trust, "untrusted_source_material");
  assert.deepEqual(await listEvidenceFiles(root), ["event/notes.md"]);
  await assert.rejects(() => readEvidence(root, "../secret.txt"), /路径/);
  await writeFile(path.join(root, "large.txt"), Buffer.alloc(128 * 1024 + 1));
  await assert.rejects(() => readEvidence(root, "large.txt"), /128 KiB/);
  await assert.rejects(() => readEvidence(root, "code.py"), /仅支持/);
});

test("public events never expose system snapshots or provider requests", () => {
  assert.equal(
    publicEvent({
      type: "user/message",
      data: {
        source: { kind: "snapshot" },
        content: [{ type: "text", text: "internal config" }],
      },
    }),
    null,
  );
  assert.equal(
    publicEvent({ type: "llm/request", data: { apiKey: "secret" } }),
    null,
  );
  assert.deepEqual(
    publicEvent({
      type: "user/message",
      seq: 1,
      time: 2,
      data: {
        source: { kind: "user" },
        content: [{ type: "text", text: "你好" }],
        apiKey: "secret",
      },
    }).data,
    { content: [{ type: "text", text: "你好" }] },
  );
  assert.equal(
    publicEvent({
      type: "assistant/chunk",
      data: { chunk: { type: "reasoning-delta", text: "internal" } },
    }),
    null,
  );
});

test("reports require existing scoped sources and cannot overwrite arbitrary paths", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "geo-report-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  await mkdir(path.join(root, "inputs"));
  await mkdir(path.join(root, "outputs"));
  await writeFile(
    path.join(root, "inputs", "source.txt"),
    "unverified test evidence",
  );
  const paths = { projectRoot: root, chatRoot: root };
  const report = await writeReport(paths, {
    filename: "事件核查.md",
    content: "仅依据已提供的未核实材料。",
    source_paths: ["inputs/source.txt"],
  });
  assert.match(report.path, /^outputs\/[\w-]+\/事件核查\.md$/);
  assert.equal(report.sha256.length, 64);
  await assert.rejects(
    () =>
      writeReport(paths, {
        filename: "../escape.md",
        content: "x",
        source_paths: ["inputs/source.txt"],
      }),
    /文件名/,
  );
  await assert.rejects(
    () =>
      writeReport(paths, {
        filename: "test.md",
        content: "x",
        source_paths: ["inputs/../secret"],
      }),
    /路径/,
  );
  await assert.rejects(
    () =>
      writeReport(paths, {
        filename: "test.md",
        content: "x",
        source_paths: [],
      }),
    /引用/,
  );
});
