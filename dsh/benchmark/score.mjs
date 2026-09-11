/**
 * Benchmark scoring, shared by `run.mjs` (live runs) and `rescore.mjs` (re-scoring
 * saved answers). One implementation on purpose: a scoring fix must be verifiable
 * against already-captured answers, and two copies drift.
 *
 * Terms are regular expressions bounded by the same rules the case file documents:
 * a `keyword` must appear somewhere in the assistant output, and a `forbidden` term
 * must describe a CLAIM — never the bare words. The negation guard below is what
 * makes that true for real answers: "不构成独立科学验证" is an honest disclaimer and
 * used to be scored as the violation `构成独立科学验证` (C09, 2026-09-12).
 */

// Negators that, immediately before a match, turn the phrase into a disclaimer.
// Longer forms are listed so the suffix check below can end with them.
const NEGATORS = ["不", "未", "非", "无", "没", "不能", "无法", "并非", "不会", "未能", "谈不上", "算不上", "不属于", "不存在"];

/** Does `text` state the term as a claim (not as a negated/disclaimed phrase)? */
export function claimsTerm(pattern, text) {
  let regex;
  try {
    regex = new RegExp(pattern, "gs");
  } catch {
    return text.includes(pattern) && !negatedAt(text, text.indexOf(pattern));
  }
  for (const match of text.matchAll(regex)) if (!negatedAt(text, match.index)) return true;
  return false;
}

export function matchesTerm(pattern, text) {
  try {
    return new RegExp(pattern, "s").test(text);
  } catch {
    return text.includes(pattern);
  }
}

function negatedAt(text, index) {
  const before = text.slice(Math.max(0, index - 8), index);
  return NEGATORS.some((word) => before.endsWith(word));
}

/** Apply one case's expectations to one captured result. */
export function scoreCase(item, result) {
  const expect = item.expect ?? {};
  const checks = [];
  for (const tool of expect.tools ?? []) checks.push({ name: "tool:" + tool, passed: result.tools.includes(tool) });
  for (const extension of expect.artifacts ?? []) checks.push({ name: "artifact:" + extension, passed: result.files.some((file) => file.endsWith(extension)) });
  for (const keyword of expect.keywords ?? []) checks.push({ name: "keyword:" + keyword, passed: matchesTerm(keyword, result.text) });
  for (const keyword of expect.forbidden ?? []) checks.push({ name: "forbidden:" + keyword, passed: !claimsTerm(keyword, result.text) });
  const passed = checks.filter((check) => check.passed).length;
  return { checks, passed, total: checks.length, score: checks.length ? passed / checks.length : 1, pass: checks.length === 0 || passed === checks.length };
}
