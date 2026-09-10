#!/usr/bin/env node
/**
 * Re-score saved report(s) with the current cases.json expectations.
 * The captured answers, tools and files are unchanged: only the check patterns
 * are re-applied, so a scoring fix can be verified without rerunning the case.
 *
 *   node dsh/benchmark/rescore.mjs <cases.json> <report.json> [more.json ...]
 */
import { readFile } from "node:fs/promises";
import path from "node:path";

const [casesFile, ...reportFiles] = process.argv.slice(2);
const suite = JSON.parse(await readFile(path.resolve(casesFile), "utf8"));
const byId = new Map(suite.cases.map((item) => [item.id, item]));
const records = [];
for (const file of reportFiles) {
  const report = JSON.parse(await readFile(path.resolve(file), "utf8"));
  for (const record of report.results ?? []) records.push(record);
}
const matches = (pattern, text) => {
  try {
    return new RegExp(pattern, "s").test(text);
  } catch {
    return text.includes(pattern);
  }
};
let changed = 0;
for (const record of records) {
  const item = byId.get(record.id);
  if (!item) continue;
  const expect = item.expect ?? {};
  const checks = [];
  for (const tool of expect.tools ?? []) checks.push({ name: "tool:" + tool, passed: record.tools?.includes(tool) });
  for (const extension of expect.artifacts ?? []) checks.push({ name: "artifact:" + extension, passed: (record.files ?? []).some((file) => file.endsWith(extension)) });
  for (const keyword of expect.keywords ?? []) checks.push({ name: "keyword:" + keyword, passed: matches(keyword, record.text ?? "") });
  for (const keyword of expect.forbidden ?? []) checks.push({ name: "forbidden:" + keyword, passed: !matches(keyword, record.text ?? "") });
  const passed = checks.filter((check) => check.passed).length;
  const verdict = checks.length === 0 || passed === checks.length;
  const before = record.pass;
  if (before !== verdict) changed++;
  console.log(`${record.id} ${before ? "PASS" : "FAIL"} -> ${verdict ? "PASS" : "FAIL"} ${passed}/${checks.length}${before !== verdict ? "  (scoring fix)" : ""}`);
  if (!verdict) console.log("    未过项：" + checks.filter((check) => !check.passed).map((check) => check.name).join(", "));
}
console.log(`\n共 ${records.length} 例，判定变化 ${changed} 例`);
