import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

// The product's agent preset, delivered at boot.
//
// On the 0.1.5 line a session's tools are composed by an AGENT PRESET, not by the
// host profile: the web-app bundle disables 23 host tool rows and its own row names
// `default: standard` — the vendor's full CODING agent. The product therefore ships
// its own preset (`profile/agent-presets/geosentinel/`, asserted by
// tests/product-preset.test.mjs) and the profile selects it as the default.
//
// A preset is a composition, so `@deepseek-ai/dsh-agent-presets` reads it from a
// root. The shipped root is inside the installed package; a deployment's own presets
// belong in `<dshHome>/.agent-presets`, which the same plugin appends as its LAST
// root (`includeUserRoot`, `<DSH_HOME>` or `~/.dsh`; an earlier root wins a duplicate
// id, and no shipped preset is called `geosentinel`). A running release must not read
// the deployment's source tree, so this module copies the preset from the running
// installation's own `profile/` directory into that home root. Discovery re-reads its
// roots per call, so a boot-time write is in place long before the first session.

export const PRESET_ID = "geosentinel";
export const PRESET_FILES = ["agent.cordis.yml", "preset.yml"];
export const MARKER_FILE = ".geosentinel-preset.json";
const OWNER = "@geosentinel/dsh-platform";

/** The preset directory shipped beside this plugin: `<dshRoot>/profile/agent-presets/<id>/`. */
export function presetSourceDir(moduleUrl = import.meta.url) {
  return fileURLToPath(new URL(`../../profile/agent-presets/${PRESET_ID}/`, moduleUrl));
}

/** Where the preset must land: the preset roster's user root, `<dshHome>/.agent-presets/<id>/`. */
export function presetTargetDir(env = process.env, homeDir = os.homedir()) {
  const configured = typeof env?.DSH_HOME === "string" ? env.DSH_HOME.trim() : "";
  const home = configured.length > 0 ? configured : path.join(homeDir, ".dsh");
  return path.join(home, ".agent-presets", PRESET_ID);
}

const sha256 = (buffer) => createHash("sha256").update(buffer).digest("hex");

async function readSource(source) {
  const files = new Map();
  for (const name of PRESET_FILES) {
    const file = path.join(source, name);
    let buffer;
    try {
      buffer = await readFile(file);
    } catch (error) {
      throw new Error(`预设源文件不可读：${file}（${error instanceof Error ? error.message : String(error)}）`);
    }
    files.set(name, buffer);
  }
  return files;
}

async function readMarker(target) {
  try {
    const parsed = JSON.parse(await readFile(path.join(target, MARKER_FILE), "utf8"));
    return typeof parsed === "object" && parsed !== null ? parsed : undefined;
  } catch {
    // A missing or unreadable marker means this directory is not ours to manage:
    // the caller then leaves whatever is there untouched.
    return undefined;
  }
}

/**
 * Copy the product preset into the roster's user root, writing only when the bytes
 * differ. Never calls `rm` and never writes a file it does not own: a directory
 * holding our marker is ours to refresh, and one without it belongs to whoever
 * authored it — an administrator who wrote their own `geosentinel` preset keeps it,
 * and callers are expected to surface that loudly instead of overwriting.
 *
 * @returns a summary; `status` is `installed`, `current`, `foreign` or `failed`.
 *          A failure is reported, never thrown: a preset the product could not
 *          deliver must not stop the platform from booting.
 */
export async function installProductPreset({ source, target, now = () => new Date().toISOString() } = {}) {
  const result = { status: "failed", source, target, changed: [], reason: undefined };
  try {
    if (typeof source !== "string" || typeof target !== "string") {
      result.reason = "需要 source 与 target 两个目录路径";
      return result;
    }
    const files = await readSource(source);
    const hashes = Object.fromEntries([...files].map(([name, buffer]) => [name, sha256(buffer)]));

    if (existsSync(target)) {
      const marker = await readMarker(target);
      if (marker === undefined || marker.owner !== OWNER) {
        result.status = "foreign";
        result.reason = `${target} 已存在且不是产品写入的预设目录，保持原样`;
        return result;
      }
      const changed = PRESET_FILES.filter((name) => marker.files?.[name] !== hashes[name]);
      // A moved installation (a new release directory) leaves the bytes identical but
      // the recorded source stale, so the marker is rewritten even when no file is.
      if (changed.length === 0 && marker.source === source) {
        result.status = "current";
        return result;
      }
      for (const name of changed) await writeFile(path.join(target, name), files.get(name));
      await writeMarker(target, source, hashes, now);
      result.status = "installed";
      result.changed = changed;
      return result;
    }

    await mkdir(target, { recursive: true });
    for (const name of PRESET_FILES) await writeFile(path.join(target, name), files.get(name));
    await writeMarker(target, source, hashes, now);
    result.status = "installed";
    result.changed = [...PRESET_FILES];
    return result;
  } catch (error) {
    result.status = "failed";
    result.reason = error instanceof Error ? error.message : String(error);
    return result;
  }
}

async function writeMarker(target, source, hashes, now) {
  const marker = { owner: OWNER, preset: PRESET_ID, source, files: hashes, writtenAt: now() };
  await writeFile(path.join(target, MARKER_FILE), `${JSON.stringify(marker, null, 2)}\n`);
}

/** One log line for the platform plugin: what happened, in the product's language. */
export function describeInstall(result) {
  switch (result?.status) {
    case "installed":
      return `GeoSentinel: 已写入产品 agent preset（${PRESET_ID}）到 ${result.target}，文件 ${result.changed.join("、")}`;
    case "current":
      return `GeoSentinel: 产品 agent preset（${PRESET_ID}）已是最新，未改写 ${result.target}`;
    case "foreign":
      return `GeoSentinel: 警告——${result.target} 不是产品写入的预设目录，未改写；该目录会成为默认 preset 的解析结果，请自行核对`;
    default:
      return `GeoSentinel: 警告——产品 agent preset（${PRESET_ID}）未写入 ${result?.target ?? "(未知目录)"}：${result?.reason ?? "未知原因"}`;
  }
}
