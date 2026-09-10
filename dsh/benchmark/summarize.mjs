#!/usr/bin/env node
/**
 * Merge one or more benchmark reports and print a tier summary.
 * Later reports win for a repeated case id, so a targeted `--only` rerun can
 * replace an earlier result without losing the rest.
 *
 *   node dsh/benchmark/summarize.mjs report.json [rerun.json ...]
 */
import { readFile } from "node:fs/promises";
import path from "node:path";

const files = process.argv.slice(2);
if (!files.length) {
  console.error("用法：node dsh/benchmark/summarize.mjs <report.json> [more.json ...]");
  process.exitCode = 1;
} else {
  const byId = new Map();
  for (const file of files) {
    const report = JSON.parse(await readFile(path.resolve(file), "utf8"));
    for (const record of report.results ?? []) byId.set(record.id, record);
  }
  const records = [...byId.values()];
  const tiers = new Map();
  for (const record of records) {
    const tier = tiers.get(record.tier) ?? { pass: 0, count: 0, score: 0 };
    tier.count++;
    tier.score += record.score ?? 0;
    if (record.pass) tier.pass++;
    tiers.set(record.tier, tier);
  }
  console.log(`合计 ${records.length} 例`);
  for (const [tier, value] of tiers)
    console.log(`  ${tier}: ${value.pass}/${value.count} 全通过，平均得分 ${(value.score / value.count).toFixed(2)}`);
  console.log("\n逐例：");
  for (const record of records.sort((left, right) => left.id.localeCompare(right.id)))
    console.log(`  ${record.id} ${record.pass ? "PASS" : "FAIL"} ${record.passed}/${record.total} ${record.elapsedSeconds}s ${record.title}${record.errors?.length ? " · " + record.errors.slice(0, 2).join("; ") : ""}`);
  const failed = records.filter((record) => !record.pass);
  if (failed.length) console.log(`\n未通过 ${failed.length} 例：${failed.map((record) => record.id).join(", ")}`);
}
