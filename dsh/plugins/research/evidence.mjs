import { open, readdir, mkdir, writeFile, lstat } from "node:fs/promises";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { PlatformError, workspacePath } from "../platform/store.mjs";

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
