import { open, readdir, stat } from "node:fs/promises";
import path from "node:path";
import { isShapefileSidecar, readShapefilePreview } from "./shapefile.mjs";
import { readGeoTiffPreview } from "./tiff.mjs";

// Geospatial files the sidebar can render: GeoJSON / JSON / CSV / shapefile as
// vector previews, GeoTIFF as a bounded PNG preview plus metadata. Shapefile
// companions (.shx/.dbf/.prj…) belong to the .shp entry and are never listed on
// their own.
const SPATIAL_EXTENSIONS = new Set([".geojson", ".json", ".csv", ".shp", ".tif", ".tiff"]);
export const PREVIEW_EXTENSIONS = [".geojson", ".json", ".csv", ".shp", ".tif", ".tiff"];
const HIDDEN_DIRS = new Set(["memory"]);
const MAX_SOURCE_BYTES = 64 * 1024 * 1024;
const MAX_FEATURES = 600;
const MAX_LIST = 400;

const LON_KEYS = ["lon", "lng", "long", "longitude", "x", "经度"];
const LAT_KEYS = ["lat", "latitude", "y", "纬度"];

const number = (value) => {
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  // An empty cell is missing data, not 0 — treating it as the origin would put
  // a fabricated point in the Gulf of Guinea on the map.
  const text = String(value ?? "").trim();
  if (!text) return null;
  const parsed = Number(text);
  return Number.isFinite(parsed) ? parsed : null;
};

/** Recursively list geospatial files under one chat workspace. */
export async function listSpatialFiles(root) {
  const files = [];
  async function walk(directory, relative) {
    if (files.length >= MAX_LIST) return;
    let entries;
    try {
      entries = await readdir(directory, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      if (files.length >= MAX_LIST) return;
      if (entry.name.startsWith(".") || entry.isSymbolicLink()) continue;
      if (!relative && HIDDEN_DIRS.has(entry.name)) continue;
      const child = relative ? `${relative}/${entry.name}` : entry.name;
      if (entry.isDirectory()) {
        await walk(path.join(directory, entry.name), child);
        continue;
      }
      const extension = path.extname(entry.name).toLowerCase();
      if (!SPATIAL_EXTENSIONS.has(extension)) continue;
      if (isShapefileSidecar(entry.name)) continue;
      const info = await stat(path.join(directory, entry.name)).catch(() => null);
      if (!info?.isFile() || info.size > MAX_SOURCE_BYTES) continue;
      files.push({ path: child, size: info.size, extension, sidecars: sidecarCount(relative, entry.name, entries) });
    }
  }
  await walk(root, "");
  return files.sort((left, right) => left.path.localeCompare(right.path, "zh-Hans"));
}

/** How many companion files (shx/dbf/prj…) this shapefile carries. */
function sidecarCount(_relative, name, entries) {
  if (path.extname(name).toLowerCase() !== ".shp") return 0;
  const stem = name.slice(0, -4).toLowerCase();
  return entries.filter((entry) => entry.isFile() && isShapefileSidecar(entry.name) && entry.name.toLowerCase().startsWith(stem)).length;
}

function geoJsonFeatures(value) {
  if (!value || typeof value !== "object") return null;
  const type = value.type;
  if (type === "FeatureCollection") return Array.isArray(value.features) ? value.features : [];
  if (type === "Feature") return [value];
  if (typeof type === "string" && /^(Point|MultiPoint|LineString|MultiLineString|Polygon|MultiPolygon|GeometryCollection)$/.test(type))
    return [{ type: "Feature", properties: {}, geometry: value }];
  return null;
}

const isPoint = (value) =>
  Array.isArray(value) && value.length >= 2 && Number.isFinite(value[0]) && Number.isFinite(value[1]);
const ringList = (value) =>
  Array.isArray(value) && value.length > 0 && value.every((ring) => Array.isArray(ring) && ring.length > 0 && ring.every(isPoint));
/**
 * Leaflet throws on malformed geometry ("Invalid LatLng object"), so validate
 * the coordinate nesting here and drop bad features instead of breaking the
 * whole viewer. Real-world GeoJSON is frequently malformed.
 */
export function geometryValid(geometry) {
  if (!geometry || typeof geometry.type !== "string") return false;
  const coordinates = geometry.coordinates;
  switch (geometry.type) {
    case "Point": return isPoint(coordinates);
    case "MultiPoint":
    case "LineString": return Array.isArray(coordinates) && coordinates.length > 0 && coordinates.every(isPoint);
    case "MultiLineString":
    case "Polygon": return ringList(coordinates);
    case "MultiPolygon":
      return Array.isArray(coordinates) && coordinates.length > 0 && coordinates.every(ringList);
    default: return false;
  }
}

function boundsOf(features) {
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  const visit = (coordinates) => {
    if (!Array.isArray(coordinates)) return;
    if (typeof coordinates[0] === "number" && typeof coordinates[1] === "number") {
      const [x, y] = coordinates;
      if (Number.isFinite(x) && Number.isFinite(y)) {
        minX = Math.min(minX, x); maxX = Math.max(maxX, x);
        minY = Math.min(minY, y); maxY = Math.max(maxY, y);
      }
      return;
    }
    for (const child of coordinates) visit(child);
  };
  for (const feature of features) visit(feature?.geometry?.coordinates);
  return Number.isFinite(minX) ? [minX, minY, maxX, maxY] : null;
}

function splitCsvLine(line, delimiter) {
  const values = [];
  let current = "", quoted = false;
  for (let index = 0; index < line.length; index++) {
    const character = line[index];
    if (quoted) {
      if (character === '"' && line[index + 1] === '"') { current += '"'; index++; }
      else if (character === '"') quoted = false;
      else current += character;
    } else if (character === '"') quoted = true;
    else if (character === delimiter) { values.push(current); current = ""; }
    else current += character;
  }
  values.push(current);
  return values;
}

function csvFeatures(text) {
  const lines = text.split(/\r?\n/).filter((line) => line.trim() !== "");
  if (lines.length < 2) return { features: [], lonKey: null, latKey: null };
  const delimiter = (lines[0].match(/;/g) ?? []).length > (lines[0].match(/,/g) ?? []).length ? ";" : ",";
  const header = splitCsvLine(lines[0], delimiter).map((value) => value.trim().replace(/^"|"$/g, ""));
  const lower = header.map((value) => value.toLowerCase());
  const lonIndex = lower.findIndex((value) => LON_KEYS.includes(value));
  const latIndex = lower.findIndex((value) => LAT_KEYS.includes(value));
  if (lonIndex < 0 || latIndex < 0) return { features: [], lonKey: null, latKey: null };
  const features = [];
  for (const line of lines.slice(1, MAX_FEATURES + 1)) {
    const values = splitCsvLine(line, delimiter);
    const lon = number(values[lonIndex]);
    const lat = number(values[latIndex]);
    if (lon === null || lat === null || Math.abs(lon) > 180 || Math.abs(lat) > 90) continue;
    const properties = {};
    header.forEach((key, index) => {
      if (index === lonIndex || index === latIndex) return;
      const raw = values[index];
      if (raw === undefined || raw === "") return;
      properties[key] = raw.length > 120 ? raw.slice(0, 120) : raw;
    });
    features.push({ type: "Feature", properties, geometry: { type: "Point", coordinates: [lon, lat] } });
  }
  return { features, lonKey: header[lonIndex], latKey: header[latIndex] };
}

function summarize(features, extra = {}) {
  const geometryTypes = {};
  const bounded = [];
  let droppedInvalid = 0;
  for (const feature of features) {
    if (bounded.length >= MAX_FEATURES) break;
    const geometry = feature?.geometry;
    if (!geometry || typeof geometry.type !== "string") continue;
    if (!geometryValid(geometry)) {
      droppedInvalid++;
      continue;
    }
    geometryTypes[geometry.type] = (geometryTypes[geometry.type] ?? 0) + 1;
    const properties = feature.properties && typeof feature.properties === "object" ? feature.properties : {};
    const boundedProperties = {};
    for (const [key, value] of Object.entries(properties).slice(0, 24))
      boundedProperties[key] = typeof value === "string" ? value.slice(0, 200) : value;
    bounded.push({ type: "Feature", properties: boundedProperties, geometry });
  }
  return {
    featureCount: features.length,
    returned: bounded.length,
    droppedInvalid,
    geometryTypes,
    bounds: boundsOf(bounded),
    features: bounded,
    ...extra,
  };
}

/**
 * Parse one geospatial file into a bounded, client-renderable summary.
 * Unknown or non-spatial JSON is reported as unsupported instead of guessed.
 */
export async function readSpatialFile(file, extension) {
  const info = await stat(file).catch(() => null);
  if (!info?.isFile()) return { kind: "missing" };
  if (info.size > MAX_SOURCE_BYTES) return { kind: "too_large", size: info.size };
  if (extension === ".shp") return readShapefilePreview(file, MAX_FEATURES);
  if (extension === ".tif" || extension === ".tiff") {
    try {
      return await readGeoTiffPreview(file, info.size);
    } catch (error) {
      return { kind: "unsupported", reason: "无法解析该 TIFF：" + error.message };
    }
  }
  const handle = await open(file, "r");
  let text;
  try {
    const buffer = Buffer.alloc(info.size);
    if (info.size) await handle.read(buffer, 0, info.size, 0);
    text = buffer.toString("utf8");
  } finally {
    await handle.close();
  }
  if (extension === ".csv") {
    const parsed = csvFeatures(text);
    if (!parsed.lonKey || !parsed.latKey)
      return { kind: "unsupported", reason: "CSV 未找到经纬度列（支持 lon/lng/longitude/x 与 lat/latitude/y）" };
    return { kind: "csv", lonKey: parsed.lonKey, latKey: parsed.latKey, ...summarize(parsed.features) };
  }
  let value;
  try {
    value = JSON.parse(text);
  } catch {
    return { kind: "unsupported", reason: "文件不是有效 JSON" };
  }
  const features = geoJsonFeatures(value);
  if (!features) {
    const keys = value && typeof value === "object" && !Array.isArray(value) ? Object.keys(value).slice(0, 20) : [];
    return { kind: "json", reason: "该 JSON 不含 GeoJSON 几何，未在地图上渲染", keys };
  }
  return { kind: "geojson", ...summarize(features) };
}

export const spatialLimits = Object.freeze({
  maxSourceBytes: MAX_SOURCE_BYTES,
  maxFeatures: MAX_FEATURES,
  maxList: MAX_LIST,
  extensions: PREVIEW_EXTENSIONS,
});
