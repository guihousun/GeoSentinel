import { open, readdir, mkdir, writeFile, lstat } from "node:fs/promises";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { PlatformError, workspacePath } from "../platform/store.mjs";

// Claim–Evidence chain: the framework's reliability core. A claim is only as
// good as the traceable evidence behind it, and contradicting evidence must be
// recorded rather than averaged away.
const CLAIM_TYPES = new Set(["factual", "temporal", "spatial", "statistical", "relational", "interpretation"]);
const CONFIDENCE = new Set(["high", "medium", "low"]);
const EVIDENCE_KINDS = new Set(["dataset", "document", "remote_sensing", "statistic", "web"]);
const STANCES = new Set(["supporting", "contradicting", "neutral"]);
const MAX_CLAIMS = 30;
const MAX_EVIDENCE_PER_CLAIM = 20;
const MAX_ARTIFACT_BYTES = 256 * 1024;

function text(value, field, max) {
  if (typeof value !== "string" || !value.trim()) throw new PlatformError(400, `${field} 必须是非空文本`);
  const trimmed = value.trim();
  if (trimmed.length > max) throw new PlatformError(413, `${field} 超过 ${max} 字符`);
  return trimmed;
}

function optionalText(value, field, max) {
  if (value === undefined || value === null || value === "") return undefined;
  return text(value, field, max);
}

function oneOf(value, field, allowed, fallback) {
  if (value === undefined || value === null || value === "") return fallback;
  if (typeof value !== "string" || !allowed.has(value))
    throw new PlatformError(400, `${field} 必须是 ${[...allowed].join(" / ")} 之一`);
  return value;
}

/** Resolve one evidence source: a real workspace file, or an http(s) URL. */
async function resolveSource({ projectRoot, chatRoot }, source, field) {
  const value = text(source, field, 500);
  if (/^https?:\/\//i.test(value)) {
    if (value.length > 500) throw new PlatformError(413, `${field} URL 过长`);
    return { kind: "url", value };
  }
  const match = /^(inputs|outputs)\/(.+)$/.exec(value);
  if (!match) throw new PlatformError(400, `${field} 必须是 inputs/ 或 outputs/ 下的文件，或 http(s) 链接`);
  const root = path.join(match[1] === "inputs" ? projectRoot : chatRoot, match[1]);
  let info;
  try {
    info = await lstat(workspacePath(root, match[2]));
  } catch {
    throw new PlatformError(400, `${field} 指向的文件不存在：${value}`);
  }
  if (!info.isFile()) throw new PlatformError(400, `${field} 指向的不是文件：${value}`);
  return { kind: "file", value };
}

/**
 * Validate and persist a Claim–Evidence artifact plus its rendered matrix.
 * @returns the artifact record with counts and checksums.
 */
export async function writeEvidence(
  { projectRoot, chatRoot },
  { filename, topic, claims, limitations, method },
) {
  if (typeof filename !== "string" || !/^[\p{L}\p{N}_ -]{1,100}\.json$/u.test(filename))
    throw new PlatformError(400, "证据链须使用简单的 .json 文件名");
  const question = text(topic, "topic", 2000);
  if (!Array.isArray(claims) || claims.length < 1 || claims.length > MAX_CLAIMS)
    throw new PlatformError(400, `claims 须为 1 至 ${MAX_CLAIMS} 条断言`);
  if (!Array.isArray(limitations) || limitations.length < 1 || limitations.length > 20)
    throw new PlatformError(400, "必须写明 1 至 20 条限制与不确定性");
  const limits = limitations.map((item, index) => text(item, `limitations[${index}]`, 500));
  const methodNote = optionalText(method, "method", 2000);

  const sources = new Set();
  const normalized = [];
  for (const [index, raw] of claims.entries()) {
    if (!raw || typeof raw !== "object" || Array.isArray(raw))
      throw new PlatformError(400, `claims[${index}] 必须是对象`);
    const id = optionalText(raw.id, `claims[${index}].id`, 40) ?? `C${index + 1}`;
    const claimText = text(raw.text, `claims[${index}].text`, 2000);
    const type = oneOf(raw.type, `claims[${index}].type`, CLAIM_TYPES, "factual");
    const confidence = oneOf(raw.confidence, `claims[${index}].confidence`, CONFIDENCE, undefined);
    if (!confidence) throw new PlatformError(400, `claims[${index}] 必须标注 confidence（high/medium/low）`);
    if (!Array.isArray(raw.evidence) || raw.evidence.length < 1 || raw.evidence.length > MAX_EVIDENCE_PER_CLAIM)
      throw new PlatformError(400, `claims[${index}].evidence 须为 1 至 ${MAX_EVIDENCE_PER_CLAIM} 条证据`);
    const evidence = [];
    for (const [position, item] of raw.evidence.entries()) {
      if (!item || typeof item !== "object" || Array.isArray(item))
        throw new PlatformError(400, `claims[${index}].evidence[${position}] 必须是对象`);
      const field = `claims[${index}].evidence[${position}]`;
      const kind = oneOf(item.kind, `${field}.kind`, EVIDENCE_KINDS, undefined);
      if (!kind) throw new PlatformError(400, `${field} 必须标注 kind`);
      const stance = oneOf(item.stance, `${field}.stance`, STANCES, undefined);
      if (!stance) throw new PlatformError(400, `${field} 必须标注 stance（supporting/contradicting/neutral）`);
      const source = await resolveSource({ projectRoot, chatRoot }, item.source, `${field}.source`);
      let retrievedAt;
      if (source.kind === "url") {
        if (item.retrievedAt === undefined || item.retrievedAt === null || item.retrievedAt === "")
          throw new PlatformError(400, `${field} 是网页证据，必须给出 retrievedAt 检索时间`);
        retrievedAt = text(item.retrievedAt, `${field}.retrievedAt`, 60);
        if (Number.isNaN(Date.parse(retrievedAt)))
          throw new PlatformError(400, `${field}.retrievedAt 必须是可解析的检索时间`);
      } else retrievedAt = optionalText(item.retrievedAt, `${field}.retrievedAt`, 60);
      sources.add(source.value);
      evidence.push({
        kind,
        stance,
        source: source.value,
        ...(retrievedAt ? { retrievedAt } : {}),
        ...(optionalText(item.note, `${field}.note`, 1000) ? { note: item.note.trim() } : {}),
        ...(item.confidence ? { confidence: oneOf(item.confidence, `${field}.confidence`, CONFIDENCE, undefined) } : {}),
      });
    }
    const supporting = evidence.filter((item) => item.stance === "supporting").length;
    if (supporting === 0 && confidence !== "low")
      throw new PlatformError(400, `claims[${index}] 没有支持性证据，confidence 必须为 low`);
    normalized.push({
      id,
      text: claimText,
      type,
      confidence,
      ...(optionalText(raw.time, `claims[${index}].time`, 200) ? { time: raw.time.trim() } : {}),
      ...(optionalText(raw.location, `claims[${index}].location`, 200) ? { location: raw.location.trim() } : {}),
      evidence,
      ...(optionalText(raw.interpretation, `claims[${index}].interpretation`, 2000)
        ? { interpretation: raw.interpretation.trim() }
        : {}),
    });
  }

  const artifact = {
    schema: "geosentinel.evidence.v1",
    topic: question,
    createdAt: new Date().toISOString(),
    ...(methodNote ? { method: methodNote } : {}),
    claims: normalized,
    limitations: limits,
  };
  const payload = JSON.stringify(artifact, null, 2);
  if (Buffer.byteLength(payload, "utf8") > MAX_ARTIFACT_BYTES)
    throw new PlatformError(413, "证据链超过 256 KiB，请拆分研究问题");

  const counts = normalized.reduce(
    (total, claim) => {
      total.evidence += claim.evidence.length;
      for (const item of claim.evidence) total[item.stance] += 1;
      return total;
    },
    { evidence: 0, supporting: 0, contradicting: 0, neutral: 0 },
  );

  const job = randomUUID();
  const directory = path.join(chatRoot, "outputs", job);
  await mkdir(directory);
  await writeFile(path.join(directory, filename), payload, { flag: "wx" });
  const markdown = renderEvidence(artifact, counts);
  const markdownName = filename.replace(/\.json$/i, ".md");
  await writeFile(path.join(directory, markdownName), markdown, { flag: "wx" });
  const record = {
    path: `outputs/${job}/${filename}`,
    markdownPath: `outputs/${job}/${markdownName}`,
    sha256: createHash("sha256").update(payload).digest("hex"),
    claims: normalized.length,
    ...counts,
    sources: [...sources],
    createdAt: artifact.createdAt,
  };
  await writeFile(path.join(directory, "provenance.json"), JSON.stringify(record, null, 2));
  return record;
}

function renderEvidence(artifact, counts) {
  const lines = [
    `# 证据链：${artifact.topic}`,
    "",
    `- 生成时间：${artifact.createdAt}`,
    `- 断言 ${artifact.claims.length} 条；证据 ${counts.evidence} 条（支持 ${counts.supporting} / 反驳 ${counts.contradicting} / 中性 ${counts.neutral}）`,
    ...(artifact.method ? [`- 方法：${artifact.method}`] : []),
    "",
  ];
  for (const claim of artifact.claims) {
    lines.push(`## ${claim.id} ${claim.text}`, "");
    lines.push(
      `- 类型：${claim.type}；置信度：${claim.confidence}` +
        (claim.time ? `；时间：${claim.time}` : "") +
        (claim.location ? `；地点：${claim.location}` : ""),
    );
    lines.push("", "| 立场 | 类型 | 来源 | 说明 |", "| --- | --- | --- | --- |");
    for (const item of claim.evidence)
      lines.push(
        `| ${item.stance} | ${item.kind} | ${item.source}${item.retrievedAt ? `（检索 ${item.retrievedAt}）` : ""} | ${item.note ?? ""} |`,
      );
    if (claim.interpretation) lines.push("", `解释（非观测事实）：${claim.interpretation}`);
    lines.push("");
  }
  lines.push("## 限制与不确定性", "");
  for (const item of artifact.limitations) lines.push(`- ${item}`);
  lines.push("");
  return lines.join("\n");
}

export async function listEvidenceFiles(
  root,
  prefix = "",
  budget = { left: 2000 },
) {
  const files = [];
  for (const entry of await readdir(root, { withFileTypes: true })) {
    if (--budget.left < 0)
      throw new PlatformError(413, "资料数量超出单次浏览范围");
    if (entry.isSymbolicLink() || entry.name.startsWith(".")) continue;
    const name = prefix + entry.name;
    if (entry.isDirectory())
      files.push(
        ...(await listEvidenceFiles(
          path.join(root, entry.name),
          name + "/",
          budget,
        )),
      );
    else if (entry.isFile()) files.push(name);
  }
  return files;
}

export async function readEvidence(root, relative) {
  if (
    ![".txt", ".md", ".json", ".csv", ".geojson"].includes(
      path.extname(relative).toLowerCase(),
    )
  ) {
    throw new PlatformError(
      400,
      "仅支持文本、Markdown、JSON、CSV 和 GeoJSON 证据",
    );
  }
  const filename = workspacePath(root, relative);
  const handle = await open(filename, "r");
  try {
    const info = await handle.stat();
    if (!info.isFile() || info.size > 128 * 1024)
      throw new PlatformError(
        413,
        "文本证据须小于 128 KiB；较大数据请交由分析助手处理",
      );
    const bytes = Buffer.alloc(128 * 1024 + 1);
    const { bytesRead } = await handle.read(bytes, 0, bytes.length, 0);
    if (bytesRead > 128 * 1024) throw new PlatformError(413, "文本证据过大");
    const content = bytes.subarray(0, bytesRead);
    return {
      path: relative,
      sha256: createHash("sha256").update(content).digest("hex"),
      text: new TextDecoder("utf-8", { fatal: true }).decode(content),
      trust: "untrusted_source_material",
      retrievedAt: new Date().toISOString(),
    };
  } finally {
    await handle.close();
  }
}

export async function writeReport(
  { projectRoot, chatRoot },
  { filename, content, source_paths },
) {
  if (
    typeof filename !== "string" ||
    !/^[\p{L}\p{N}_ -]{1,100}\.md$/u.test(filename)
  )
    throw new PlatformError(400, "报告须使用简单的 Markdown 文件名");
  if (
    typeof content !== "string" ||
    !content.trim() ||
    Buffer.byteLength(content, "utf8") > 128 * 1024
  )
    throw new PlatformError(413, "报告正文须为 1 至 128 KiB");
  if (
    !Array.isArray(source_paths) ||
    source_paths.length < 1 ||
    source_paths.length > 20
  )
    throw new PlatformError(400, "报告须引用 1 至 20 个当前项目文件");
  for (const source of source_paths) {
    if (typeof source !== "string" || !/^(inputs|outputs)\//.test(source))
      throw new PlatformError(400, "来源须为 inputs/ 或 outputs/ 文件");
    const prefix = source.split("/")[0],
      root = path.join(prefix === "inputs" ? projectRoot : chatRoot, prefix);
    const info = await lstat(
      workspacePath(root, source.slice(prefix.length + 1)),
    );
    if (!info.isFile()) throw new PlatformError(400, "来源不是文件");
  }
  const id = randomUUID(),
    directory = path.join(chatRoot, "outputs", id);
  await mkdir(directory);
  await writeFile(path.join(directory, filename), content, { flag: "wx" });
  const artifact = {
    path: `outputs/${id}/${filename}`,
    sha256: createHash("sha256").update(content).digest("hex"),
    source_paths,
    createdAt: new Date().toISOString(),
  };
  await writeFile(
    path.join(directory, "provenance.json"),
    JSON.stringify(artifact, null, 2),
  );
  return artifact;
}
