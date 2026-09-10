import { open } from "node:fs/promises";
import path from "node:path";

// Minimal ESRI Shapefile + DBF reader for the sidebar preview. It covers the
// geometry types the platform and its users actually exchange (point, polyline,
// polygon, multipoint, with optional Z/M sections) and reads attributes from the
// companion .dbf. Anything else is reported as unsupported instead of guessed.

const SHAPE_TYPES = {
  0: "空", 1: "点", 3: "线", 5: "面", 8: "多点",
  11: "点Z", 13: "线Z", 15: "面Z", 18: "多点Z",
  21: "点M", 23: "线M", 25: "面M", 28: "多点M",
};

/** Companion files that belong to a shapefile and never stand alone in a list. */
export const SHAPEFILE_SIDECARS = [".shx", ".dbf", ".prj", ".cpg", ".sbn", ".sbx", ".qix", ".aih", ".ain", ".atx", ".fbn", ".fbx", ".ixs", ".mxs", ".shp.xml"];

export const isShapefileSidecar = (name) => {
  const lower = String(name).toLowerCase();
  return SHAPEFILE_SIDECARS.some((extension) => lower.endsWith(extension));
};

async function readRange(file, offset, length) {
  const handle = await open(file, "r");
  try {
    const buffer = Buffer.alloc(length);
    await handle.read(buffer, 0, length, offset);
    return buffer;
  } finally {
    await handle.close();
  }
}

/** Read the .dbf attribute table. Returns an array aligned with shape records. */
export async function readDbf(file) {
  let buffer;
  try {
    buffer = await readRange(file, 0, 32);
  } catch {
    return null;
  }
  const recordCount = buffer.readUInt32LE(4);
  const headerSize = buffer.readUInt16LE(8);
  const recordSize = buffer.readUInt16LE(10);
  if (!recordCount || !headerSize || !recordSize) return null;
  const descriptorsRaw = await readRange(file, 32, Math.max(0, headerSize - 32));
  const fields = [];
  for (let at = 0; at + 32 <= descriptorsRaw.length; at += 32) {
    if (descriptorsRaw[at] === 0x0d) break;
    const name = descriptorsRaw.toString("latin1", at, at + 11).replace(/\0.*$/, "").trim();
    const type = String.fromCharCode(descriptorsRaw[at + 11]);
    const length = descriptorsRaw[at + 16];
    const decimals = descriptorsRaw[at + 17];
    if (name) fields.push({ name, type, length, decimals });
  }
  const records = [];
  const chunkSize = Math.min(recordCount, 5000) * recordSize;
  const table = await readRange(file, headerSize, Math.min(chunkSize + 1, Number.MAX_SAFE_INTEGER));
  for (let index = 0; index < recordCount; index++) {
    const at = index * recordSize;
    if (at + recordSize > table.length) break;
    if (table[at] === 0x2a) {
      records.push(null);
      continue;
    }
    const record = {};
    let cursor = at + 1;
    for (const field of fields) {
      const raw = table.toString("utf8", cursor, cursor + field.length);
      const text = raw.replace(/\0/g, "").trim();
      cursor += field.length;
      if (field.type === "N" || field.type === "F") record[field.name] = text === "" ? null : Number(text);
      else if (field.type === "L") record[field.name] = text === "T" || text === "Y";
      else record[field.name] = text;
    }
    records.push(record);
  }
  return { fields: fields.map((field) => field.name), records };
}

/**
 * Read a shapefile's geometry and attributes into GeoJSON features.
 * @param shpPath - absolute path to the .shp file
 * @param maxFeatures - bound on returned features
 */
export async function readShapefilePreview(shpPath, maxFeatures = 600) {
  const handle = await open(shpPath, "r");
  let header;
  try {
    header = Buffer.alloc(100);
    await handle.read(header, 0, 100, 0);
  } finally {
    await handle.close();
  }
  if (header.readInt32BE(0) !== 9994) return { kind: "unsupported", reason: "文件头不是 ESRI Shapefile（缺少 9994 魔数）" };
  const fileLengthBytes = header.readInt32BE(24) * 2;
  const shapeType = header.readInt32LE(32);
  const headerBounds = [header.readDoubleLE(36), header.readDoubleLE(44), header.readDoubleLE(52), header.readDoubleLE(60)];

  const attributes = await readDbf(path.join(path.dirname(shpPath), path.basename(shpPath, path.extname(shpPath)) + ".dbf"));
  const features = [];
  const geometryTypes = {};
  const resolved = shapeType % 10 === 1 || shapeType % 10 === 3 || shapeType % 10 === 5 || shapeType % 10 === 8 ? shapeType % 10 : shapeType;
  let cursor = 100;
  const chunk = 4 * 1024 * 1024;
  let window = Buffer.alloc(0);
  let windowStart = 0;
  let recordIndex = 0;
  while (cursor + 8 <= fileLengthBytes && features.length < maxFeatures) {
    if (cursor < windowStart || cursor + 8 > windowStart + window.length) {
      windowStart = cursor;
      window = await readRange(shpPath, windowStart, Math.min(chunk, fileLengthBytes - windowStart));
    }
    const offset = cursor - windowStart;
    const contentLength = window.readInt32BE(offset + 4) * 2;
    const bodyStart = cursor + 8;
    if (contentLength < 4) break;
    if (bodyStart + contentLength > windowStart + window.length) {
      windowStart = bodyStart;
      window = await readRange(shpPath, windowStart, Math.min(chunk, fileLengthBytes - windowStart));
    }
    const at = bodyStart - windowStart;
    const recordType = window.readInt32LE(at);
    const properties = attributes?.records?.[recordIndex] ?? {};
    recordIndex += 1;
    cursor = bodyStart + contentLength;
    const effective = recordType || resolved;
    if (recordType === 0) continue;
    const base = effective % 10;
    if (base === 1) {
      const x = window.readDoubleLE(at + 4);
      const y = window.readDoubleLE(at + 12);
      geometryTypes.Point = (geometryTypes.Point ?? 0) + 1;
      features.push({ type: "Feature", properties, geometry: { type: "Point", coordinates: [x, y] } });
      continue;
    }
    if (base === 8) {
      const numPoints = window.readInt32LE(at + 36);
      const points = [];
      for (let index = 0; index < numPoints; index++) {
        const pointAt = at + 40 + index * 16;
        points.push([window.readDoubleLE(pointAt), window.readDoubleLE(pointAt + 8)]);
      }
      geometryTypes.MultiPoint = (geometryTypes.MultiPoint ?? 0) + 1;
      features.push({ type: "Feature", properties, geometry: { type: "MultiPoint", coordinates: points } });
      continue;
    }
    if (base === 3 || base === 5) {
      const numParts = window.readInt32LE(at + 36);
      const numPoints = window.readInt32LE(at + 40);
      const parts = [];
      for (let index = 0; index < numParts; index++) parts.push(window.readInt32LE(at + 44 + index * 4));
      const pointsStart = at + 44 + numParts * 4;
      const rings = [];
      for (let index = 0; index < numParts; index++) {
        const from = parts[index];
        const to = index + 1 < numParts ? parts[index + 1] : numPoints;
        const ring = [];
        for (let pointIndex = from; pointIndex < to; pointIndex++) {
          const pointAt = pointsStart + pointIndex * 16;
          ring.push([window.readDoubleLE(pointAt), window.readDoubleLE(pointAt + 8)]);
        }
        if (ring.length) rings.push(ring);
      }
      if (!rings.length) continue;
      if (base === 3) {
        geometryTypes.LineString = (geometryTypes.LineString ?? 0) + rings.length;
        for (const ring of rings) features.push({ type: "Feature", properties, geometry: { type: "LineString", coordinates: ring } });
      } else {
        geometryTypes.Polygon = (geometryTypes.Polygon ?? 0) + 1;
        features.push({ type: "Feature", properties, geometry: { type: "Polygon", coordinates: rings } });
      }
      continue;
    }
    return {
      kind: "unsupported",
      reason: `Shapefile 几何类型 ${SHAPE_TYPES[shapeType] ?? shapeType} 暂不支持预览`,
      shapeType: SHAPE_TYPES[shapeType] ?? shapeType,
      bounds: headerBounds,
      attributeFields: attributes?.fields ?? [],
    };
  }
  const bounded = features.slice(0, maxFeatures);
  const bounds = boundsOf(bounded) ?? headerBounds;
  return {
    kind: "vector",
    format: "shapefile",
    shapeType: SHAPE_TYPES[shapeType] ?? String(shapeType),
    featureCount: bounded.length,
    returned: bounded.length,
    truncated: features.length >= maxFeatures,
    geometryTypes,
    bounds,
    attributeFields: attributes?.fields ?? [],
    features: bounded,
    sidecars: SHAPEFILE_SIDECARS,
    note: attributes ? "" : "未找到或无法解析同名 .dbf，属性表为空。",
  };
}

function boundsOf(features) {
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  const visit = (coordinates) => {
    if (!Array.isArray(coordinates)) return;
    if (typeof coordinates[0] === "number" && typeof coordinates[1] === "number") {
      if (Number.isFinite(coordinates[0]) && Number.isFinite(coordinates[1])) {
        minX = Math.min(minX, coordinates[0]);
        maxX = Math.max(maxX, coordinates[0]);
        minY = Math.min(minY, coordinates[1]);
        maxY = Math.max(maxY, coordinates[1]);
      }
      return;
    }
    for (const child of coordinates) visit(child);
  };
  for (const feature of features) visit(feature?.geometry?.coordinates);
  return Number.isFinite(minX) ? [minX, minY, maxX, maxY] : null;
}
