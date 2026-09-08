import { readFile } from "node:fs/promises";
import path from "node:path";

export const typeLabels = {
  wildfires: "野火",
  wildfire: "野火",
  severe_storms: "风暴",
  floods: "洪涝",
  fl: "洪涝",
  eq: "地震",
  earthquake: "地震",
  tc: "热带气旋",
  dr: "干旱",
  vo: "火山活动",
  volcanoes: "火山活动",
  conflict: "冲突事件",
  geopolitical: "地缘政治新闻",
  other: "公开事件",
};
export const safeUrl = (value) => {
  try {
    const url = new URL(value);
    return ["https:", "http:"].includes(url.protocol) &&
      !url.username &&
      !url.password
      ? url.href
      : "";
  } catch {
    return "";
  }
};
const clean = (value, max = 300) =>
  typeof value === "string"
    ? value.replace(/\s+/g, " ").trim().slice(0, max)
    : "";
export function normalizeCandidate(value, now = Date.now()) {
  if (!value?.event_id || !value.source_name || !safeUrl(value.source_url))
    return null;
  const lon = value.longitude,
    lat = value.latitude;
  const located =
    typeof lon === "number" &&
    Number.isFinite(lon) &&
    Math.abs(lon) <= 180 &&
    typeof lat === "number" &&
    Number.isFinite(lat) &&
    Math.abs(lat) <= 90;
  const type = clean(value.event_type, 60) || "other";
  const location =
    value.source_name === "GDELT"
      ? ""
      : clean(value.location_name || value.country, 180);
  return {
    id: clean(value.event_id, 100),
    title: clean(value.title),
    summary: clean(value.summary, 600),
    source: clean(value.source_name, 80),
    url: safeUrl(value.source_url),
    type,
    severity:
      value.source_name === "GDELT"
        ? "low"
        : ["high", "medium", "low"].includes(value.severity)
          ? value.severity
          : "low",
    publishedAt:
      Number.isFinite(value.published_at) && value.published_at > 0
        ? value.published_at * 1000
        : null,
    longitude: located ? lon : null,
    latitude: located ? lat : null,
    location,
    displayTitle: `${typeLabels[type] || "公开事件"}${location ? " · " + location : ""}`,
    displayLocation: location || "地点待核验",
    languageStatus: "original",
    seenAt: now,
  };
}
export function mergeEvents(
  previous,
  incoming,
  now = Date.now(),
  ttl = 7 * 86400000,
) {
  const items = new Map(
    previous.filter((e) => e.seenAt > now - ttl).map((e) => [e.id, e]),
  );
  for (const event of incoming) items.set(event.id, event);
  const rank = { high: 3, medium: 2, low: 1 };
  return [...items.values()]
    .sort(
      (a, b) =>
        rank[b.severity] - rank[a.severity] ||
        (b.publishedAt || 0) - (a.publishedAt || 0),
    )
    .slice(0, 200);
}
export async function readSnapshot(directory, now = Date.now()) {
  try {
    const snapshot = JSON.parse(
      await readFile(path.join(directory, "snapshot.json"), "utf8"),
    );
    const heartbeat = Number(snapshot.heartbeatAt) || 0;
    const stale =
      !snapshot.lastSuccessAt ||
      now - snapshot.lastSuccessAt >
        Math.max(120000, snapshot.intervalMs * 2) ||
      now - heartbeat > 90000;
    return {
      ...snapshot,
      items: (snapshot.items || []).filter(
        (e) => e.seenAt > now - 7 * 86400000,
      ),
      stale,
      workerOnline: snapshot.state !== "stopped" && now - heartbeat < 90000,
    };
  } catch (error) {
    return {
      items: [],
      sources: [],
      state: error.code === "ENOENT" ? "waiting" : "error",
      stale: true,
      workerOnline: false,
      lastSuccessAt: null,
      intervalMs: 1800000,
    };
  }
}
