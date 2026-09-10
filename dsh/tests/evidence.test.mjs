import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, mkdir, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import {
  listEvidenceFiles,
  readEvidence,
  writeEvidence,
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

test("the claim-evidence chain requires real sources, recorded contradictions and stated limits", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "geo-claim-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  await mkdir(path.join(root, "inputs"));
  await mkdir(path.join(root, "outputs"));
  await writeFile(path.join(root, "outputs", "antl.csv"), "region,antl\n上海,12.5\n");
  const paths = { projectRoot: root, chatRoot: root };
  const claim = (overrides = {}) => ({
    text: "2020 年上海各区 ANTL 均值为 12.5",
    type: "statistical",
    confidence: "high",
    evidence: [
      { kind: "dataset", stance: "supporting", source: "outputs/antl.csv", note: "分区统计产物" },
      { kind: "web", stance: "contradicting", source: "https://example.org/report", retrievedAt: "2026-09-09T12:00:00Z" },
    ],
    ...overrides,
  });
  const artifact = await writeEvidence(paths, {
    filename: "上海灯光核查.json",
    topic: "2020 年上海夜间灯光水平",
    method: "GEE 年度合成 + 分区统计（ANTL）",
    claims: [claim()],
    limitations: ["仅有单一年度产品，无法判断趋势"],
  });
  assert.match(artifact.path, /^outputs\/[\w-]+\/上海灯光核查\.json$/);
  assert.match(artifact.markdownPath, /^outputs\/[\w-]+\/上海灯光核查\.md$/);
  assert.equal(artifact.claims, 1);
  assert.equal(artifact.evidence, 2);
  assert.equal(artifact.supporting, 1);
  assert.equal(artifact.contradicting, 1);
  assert.equal(artifact.sha256.length, 64);
  assert.deepEqual(artifact.sources, ["outputs/antl.csv", "https://example.org/report"]);

  await assert.rejects(() => writeEvidence(paths, {
    filename: "x.json", topic: "t", limitations: ["l"],
    claims: [claim({ evidence: [{ kind: "dataset", stance: "supporting", source: "outputs/missing.csv" }] })],
  }), /不存在/);
  await assert.rejects(() => writeEvidence(paths, {
    filename: "x.json", topic: "t", limitations: ["l"],
    claims: [claim({ evidence: [{ kind: "dataset", stance: "supporting", source: "../../etc/passwd" }] })],
  }), /inputs\/ 或 outputs\//);
  await assert.rejects(() => writeEvidence(paths, {
    filename: "x.json", topic: "t", limitations: ["l"],
    claims: [claim({ evidence: [{ kind: "web", stance: "supporting", source: "https://example.org/a" }] })],
  }), /检索时间/);
  await assert.rejects(() => writeEvidence(paths, {
    filename: "x.json", topic: "t", limitations: ["l"],
    claims: [claim({
      evidence: [
        { kind: "dataset", stance: "contradicting", source: "outputs/antl.csv" },
        { kind: "dataset", stance: "neutral", source: "outputs/antl.csv" },
      ],
    })],
  }), /confidence 必须为 low/);
  await assert.rejects(() => writeEvidence(paths, {
    filename: "x.json", topic: "t", limitations: [], claims: [claim()],
  }), /限制/);
  await assert.rejects(() => writeEvidence(paths, {
    filename: "x.json", topic: "t", limitations: ["l"], claims: [],
  }), /断言/);
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
