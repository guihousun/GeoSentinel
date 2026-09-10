import { createRequire } from "node:module";
import { readFile, readdir, stat } from "node:fs/promises";
import path from "node:path";
import { parse } from "acorn";

const require = createRequire(import.meta.url);
const YAML = createRequire(import.meta.resolve("@deepseek-ai/dsh-base"))("yaml");

const appearanceRoot = () => process.env.GEO_DSH_HOME || process.env.DSH_HOME || "";
const liveSource = () => (process.env.GEO_APPEARANCE_SOURCE ?? "development") !== "product";
const wallpaperBytes = 2 * 1024 * 1024;

/**
 * The administrator development home whose appearance was touched last.
 *
 * The research shell is a single global theme, so a live appearance can only
 * come from one administrator. Picking the most recently modified development
 * home makes "what I just changed" win, which is unambiguous on a one-admin
 * deployment and deterministic otherwise.
 */
async function newestDevelopmentHome() {
  const root = path.join(appearanceRoot(), "development");
  let entries;
  try { entries = await readdir(root, { withFileTypes: true }); } catch { return null; }
  let best = null;
  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const dir = path.join(root, entry.name);
    for (const file of ["dream-skin.json", "settings.yaml"]) {
      try {
        const info = await stat(path.join(dir, file));
        if (!best || info.mtimeMs > best.mtimeMs) best = { dir, mtimeMs: info.mtimeMs };
      } catch { /* a home may only carry one of the two files */ }
    }
  }
  return best?.dir ?? null;
}

/**
 * Wallpaper safety only: the bytes must really be a PNG/JPEG/WebP data URI
 * within the product size budget. Everything else is taken as the development
 * home states it, so the research view renders exactly what the creation view
 * shows — including a background opacity below the draft importer's 0.65 floor.
 */
function safeWallpaper(value) {
  if (typeof value !== "string" || !value) return "";
  const match = /^data:image\/(png|jpeg|webp);base64,([A-Za-z0-9+/=]+)$/.exec(value);
  if (!match || value.length > 3 * wallpaperBytes) return "";
  const bytes = Buffer.from(match[2], "base64");
  if (bytes.length > wallpaperBytes) return "";
  const ok = (match[1] === "png" && bytes.subarray(0, 8).equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10])))
    || (match[1] === "jpeg" && bytes[0] === 255 && bytes[1] === 216)
    || (match[1] === "webp" && bytes.toString("ascii", 0, 4) === "RIFF" && bytes.toString("ascii", 8, 12) === "WEBP");
  return ok ? value : "";
}

const clampNumber = (value, min, max, fallback) => {
  const number = Number(value);
  return Number.isFinite(number) ? Math.min(max, Math.max(min, number)) : fallback;
};

/**
 * Live appearance from the administrator development home, or null when the
 * feature is off, no home exists, or the files are unreadable. Callers fall
 * back to the published product appearance, so a malformed home can never
 * break the shell.
 */
export async function liveAppearance() {
  if (!liveSource()) return null;
  const dir = await newestDevelopmentHome();
  if (!dir) return null;
  try {
    const skin = JSON.parse(await readFile(path.join(dir, "dream-skin.json"), "utf8"));
    const settings = YAML.parse(await readFile(path.join(dir, "settings.yaml"), "utf8")) ?? {};
    const id = typeof skin["dsh-dream-skin:skin"] === "string" && /^[a-z0-9-]{1,60}$/.test(skin["dsh-dream-skin:skin"])
      ? skin["dsh-dream-skin:skin"] : "system";
    return {
      skin: id,
      scheme: settings["ui-theme"]?.preference === "light" ? "light" : "dark",
      fontSize: Math.round(clampNumber(settings["ui-theme"]?.fontSize, 14, 24, 16)),
      wallpaper: skin["dsh-dream-skin:wallpaper-kind"] === "image" ? safeWallpaper(skin["dsh-dream-skin:wallpaper"]) : "",
      wash: clampNumber(skin["dsh-dream-skin:wallpaper-opacity"], 0, 1, .8),
      blur: clampNumber(skin["dsh-dream-skin:wallpaper-blur"], 0, 30, 0),
    };
  } catch { return null; }
}

/** Cheap cache key: changes exactly when the live appearance inputs change. */
export async function appearanceStamp() {
  if (!liveSource()) return "product";
  const dir = await newestDevelopmentHome();
  if (!dir) return "none";
  let stamp = dir;
  for (const file of ["dream-skin.json", "settings.yaml"]) {
    try { const info = await stat(path.join(dir, file)); stamp += `|${file}:${info.mtimeMs}:${info.size}`; } catch { stamp += `|${file}:-`; }
  }
  return stamp;
}

// Read only the published theme data. Do not execute the upstream bundle's
// DOM observers, wallpaper importers or shared, unauthenticated settings API.
export async function dreamSkinTheme() {
  const product = JSON.parse(await readFile(new URL("../../profile/product.json", import.meta.url), "utf8"));
  const live = await liveAppearance();
  const appearance = live ?? product.appearance;
  const source = await readFile(require.resolve("dsh-dream-skin/client"), "utf8");
  const pending = [parse(source, { ecmaVersion: "latest", sourceType: "script" })];
  let skins;
  while (pending.length) {
    const node = pending.pop();
    if (node.type === "VariableDeclarator" && node.id.name === "SKINS") skins = literal(node.init);
    for (const value of Object.values(node)) {
      if (Array.isArray(value)) {
        for (const child of value) if (child?.type) pending.push(child);
      } else if (value?.type) pending.push(value);
    }
  }
  const theme = !appearance || appearance.skin === "system" ? appearance?.scheme === "light" ? { colorScheme: "light", tokens: {} } : skins?.find((item) => item.id === "midnight") : skins?.find((item) => item.id === appearance.skin);
  // A live appearance may name a skin this build does not ship; fall back to
  // the published appearance instead of failing the whole shell.
  if (!theme && live) return dreamSkinTheme();
  if (!theme) throw new Error("所选主题不是当前产品支持的内置主题，请先将自定义主题加入产品插件");
  const tokens = {
    ...theme.tokens,
    "--dsw-alias-label-tertiary": theme.colorScheme === "dark" ? "#a4a4b2" : "#53535f",
    "--dsw-alias-border-l2": theme.colorScheme === "dark" ? "#626274" : "#92929e",
  };
  if (appearance?.wallpaper) {
    const dark = theme.colorScheme === "dark";
    tokens["--dsw-alias-bg-base"] = dark ? `rgba(20,20,24,${appearance.wash})` : `rgba(255,255,255,${appearance.wash})`;
  }
  return { ...theme, id: "geosentinel-midnight", tokens, fontSize: appearance?.fontSize ?? 16, wallpaper: appearance?.wallpaper || "", wallpaperBlur: appearance?.blur ?? 0 };
}

function literal(node) {
  if (node.type === "Literal" && typeof node.value === "string") return node.value;
  if (node.type === "ArrayExpression") return node.elements.map(literal);
  if (node.type === "ObjectExpression") return Object.fromEntries(node.properties.map((property) => {
    if (property.type !== "Property" || property.computed || property.method || property.kind !== "init") throw new Error("Non-static Dream Skin theme data");
    return [property.key.name ?? property.key.value, literal(property.value)];
  }));
  throw new Error("Non-static Dream Skin theme data");
}
