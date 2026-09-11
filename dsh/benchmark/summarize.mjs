#!/usr/bin/env node
/**
 * Merge one or more benchmark reports and print a tier summary.
 * Later reports win for a repeated case id, so a targeted `--only` rerun can
 * replace an earlier result without losing the rest.
 *
 *   node dsh/benchmark/summarize.mjs report.json [rerun.json ...]
 *   node dsh/benchmark/summarize.mjs --cases cases.json --markdown report.json ...
 *
 * With `--cases` every record is re-scored against the CURRENT expectations
 * (same logic as rescore.mjs) and the corrected verdict is reported, with the
 * stored verdict kept alongside so a scoring fix cannot silently rewrite a run.
 */
import { readFile } from "node:fs/promises";
import path from "node:path";

const argv = process.argv.slice(2);
const markdown = argv.includes("--markdown");
const casesIndex = argv.indexOf("--cases");
const casesFile = casesIndex === -1 ? null : argv[casesIndex + 1];
const files = argv.filter((value, index) => !value.startsWith("--") && index !== casesIndex + 1);
if (!files.length) {
  console.error("用法：node dsh/benchmark/summarize.mjs [--cases cases.json] [--markdown] <report.json> [more.json ...]");
  process.exitCode = 1;
} else {
  const suite = casesFile ? JSON.parse(await readFile(path.resolve(casesFile), "utf8")) : null;
  const byId = new Map();
  for (const file of files) {
    const report = JSON.parse(await readFile(path.resolve(file), "utf8"));
    for (const record of report.results ?? []) byId.set(record.id, record);
  }
  const matches = (pattern, text) => {
    try {
      return new RegExp(pattern, "s").test(text);
    } catch {
      return text.includes(pattern);
    }
  };
  const records = [...byId.values()].map((record) => {
    const item = suite?.cases.find((entry) => entry.id === record.id);
    if (!item) return { ...record, rescored: null };
    const expect = item.expect ?? {};
    const checks = [];
    for (const tool of expect.tools ?? []) checks.push({ name: "tool:" + tool, passed: record.tools?.includes(tool) });
    for (const extension of expect.artifacts ?? []) checks.push({ name: "artifact:" + extension, passed: (record.files ?? []).some((file) => file.endsWith(extension)) });
    for (const keyword of expect.keywords ?? []) checks.push({ name: "keyword:" + keyword, passed: matches(keyword, record.text ?? "") });
    for (const keyword of expect.forbidden ?? []) checks.push({ name: "forbidden:" + keyword, passed: !matches(keyword, record.text ?? "") });
    const passed = checks.filter((check) => check.passed).length;
    return { ...record, rescored: { passed, total: checks.length, pass: checks.length === 0 || passed === checks.length, failed: checks.filter((check) => !check.passed).map((check) => check.name) } };
  });
  const verdict = (record) => record.rescored ? record.rescored.pass : record.pass;
  // Running out of the case's declared budget is not the same evidence as finishing
  // and missing a check. RESULTS-2026-09-10 §3 left that distinction open ("应该区分
  // '没做' 与 '来不及'"), and without it a budget cap reads as a capability gap.
  const budgetNote = (record) => {
    const errors = (record.errors ?? []).map(String);
    if (errors.some((value) => value.startsWith("case-timeout"))) return "（预算用尽）";
    if (errors.some((value) => value.startsWith("queue-timeout"))) return "（排队超时）";
    return "";
  };
  const tiers = new Map();
  const counts = (record) => record.rescored ? record.rescored.passed / record.rescored.total : record.score ?? 0;
  for (const record of records) {
    const tier = tiers.get(record.tier) ?? { pass: 0, count: 0, score: 0 };
    tier.count++;
    tier.score += counts(record);
    if (verdict(record)) tier.pass++;
    tiers.set(record.tier, tier);
  }
  const sorted = records.sort((left, right) => left.id.localeCompare(right.id));
  if (markdown) {
    console.log(`| 用例 | 档位 | 判定 | 通过/总数 | 耗时 | 作业目录 | 产物 | 方案审批 | 未过项 |`);
    console.log(`| --- | --- | --- | --- | --- | --- | --- | --- | --- |`);
    for (const record of sorted) {
      const result = record.rescored;
      const dirs = new Set((record.files ?? []).map((file) => String(file).split("/")[0])).size;
      console.log(`| ${record.id} ${record.title} | ${record.tier} | ${verdict(record) ? "PASS" : "FAIL"}${result && result.pass !== record.pass ? "（判定修正）" : ""}${budgetNote(record)} | ${result ? result.passed + "/" + result.total : record.passed + "/" + record.total} | ${record.elapsedSeconds}s | ${dirs} | ${(record.files ?? []).length} | ${record.approvals ?? 0} | ${result ? result.failed.join(", ") : ""} |`);
    }
  } else {
    console.log(`合计 ${records.length} 例${suite ? "（判定按当前用例模式重新计算）" : ""}`);
    for (const [tier, value] of tiers)
      console.log(`  ${tier}: ${value.pass}/${value.count} 全通过，平均得分 ${(value.score / value.count).toFixed(2)}`);
    console.log("\n逐例：");
    for (const record of sorted)
      console.log(`  ${record.id} ${verdict(record) ? "PASS" : "FAIL"}${budgetNote(record)} ${record.rescored ? record.rescored.passed + "/" + record.rescored.total : record.passed + "/" + record.total} ${record.elapsedSeconds}s ${record.title}${record.rescored && !record.rescored.pass ? " · 未过：" + record.rescored.failed.join(", ") : ""}`);
  }
  const failed = sorted.filter((record) => !verdict(record));
  console.log(`\n未通过 ${failed.length} 例：${failed.map((record) => record.id).join(", ") || "无"}`);
  const exhausted = sorted.filter((record) => budgetNote(record));
  if (exhausted.length)
    console.log(`预算受限 ${exhausted.length} 例（${exhausted.map((record) => record.id).join(", ")}）：这些结果说明"来不及"，不等于"没做"，要下能力结论应先放宽预算重跑。`);
}
