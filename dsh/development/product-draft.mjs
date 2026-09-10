import { readFile } from "node:fs/promises";
import path from "node:path";
import { createRequire } from "node:module";
const require = createRequire(import.meta.resolve("@deepseek-ai/dsh-base")), YAML = require("yaml");
export function validateAppearance(value) {
  if (!value || Object.keys(value).some((key) => !["skin", "scheme", "fontSize", "wallpaper", "wash", "blur"].includes(key))) throw new Error("外观配置字段无效");
  if (!/^[a-z0-9-]{1,60}$/.test(value.skin || "") || !["dark", "light"].includes(value.scheme)) throw new Error("主题无效");
  if (!Number.isInteger(value.fontSize) || value.fontSize < 14 || value.fontSize > 24) throw new Error("产品字号范围为 14 至 24px");
  if (!Number.isFinite(value.wash) || value.wash < 0.65 || value.wash > 1 || !Number.isFinite(value.blur) || value.blur < 0 || value.blur > 30) throw new Error("背景透明度或模糊范围无效");
  if (value.wallpaper) {
    const match = /^data:image\/(png|jpeg|webp);base64,([A-Za-z0-9+/=]+)$/.exec(value.wallpaper);
    if (!match || value.wallpaper.length > 3 * 1024 * 1024) throw new Error("背景仅支持不超过 2 MiB 的 PNG、JPEG 或 WebP 图片；外部链接和 SVG 不会同步");
    const bytes = Buffer.from(match[2], "base64");
    if (bytes.length > 2 * 1024 * 1024 || !(match[1] === "png" && bytes.subarray(0, 8).equals(Buffer.from([137,80,78,71,13,10,26,10])) || match[1] === "jpeg" && bytes[0] === 255 && bytes[1] === 216 || match[1] === "webp" && bytes.toString("ascii", 0, 4) === "RIFF" && bytes.toString("ascii", 8, 12) === "WEBP")) throw new Error("背景图片格式校验失败");
  }
  return value;
}
async function optional(file, parse) { try { return parse(await readFile(file, "utf8")); } catch (error) { if (error.code === "ENOENT") return {}; throw error; } }
export async function developmentDraft(home, userId) {
  const root = path.join(home, "development", userId);
  const settings = await optional(path.join(root, "settings.yaml"), YAML.parse);
  const skin = await optional(path.join(root, "dream-skin.json"), JSON.parse);
  if (["url", "gradient"].includes(skin["dsh-dream-skin:wallpaper-kind"])) throw new Error("当前仅同步上传的图片背景；外部链接或渐变背景需要先作为产品样式接入，未导入任何设置");
  const model = settings["agent-default-model"];
  const appearance = validateAppearance({ skin: skin["dsh-dream-skin:skin"] || "system", scheme: settings["ui-theme"]?.preference === "light" ? "light" : "dark", fontSize: settings["ui-theme"]?.fontSize ?? 16,
    wallpaper: skin["dsh-dream-skin:wallpaper"] || "", wash: Math.max(.65, Number(skin["dsh-dream-skin:wallpaper-opacity"] ?? .8)), blur: Number(skin["dsh-dream-skin:wallpaper-blur"] ?? 0) });
  return { ...(model ? { defaultModel: { provider: model.provider, model: model.model } } : {}), appearance };
}
