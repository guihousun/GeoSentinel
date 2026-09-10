import { open } from "node:fs/promises";
import { inflateSync, deflateSync, crc32 } from "node:zlib";

// A bounded, dependency-free GeoTIFF preview. It reads the tags the platform's
// own exports carry (classic and BigTIFF, strips or tiles, uncompressed or
// DEFLATE, single band or RGB) and renders a small PNG. Anything it cannot
// decode honestly (LZW, JPEG, huge rasters) returns metadata without pixels
// instead of a guessed image.

const TAG = {
  ImageWidth: 256, ImageLength: 257, BitsPerSample: 258, Compression: 259,
  Photometric: 262, StripOffsets: 273, SamplesPerPixel: 277, RowsPerStrip: 278,
  StripByteCounts: 279, PlanarConfig: 284, Predictor: 317,
  TileWidth: 322, TileLength: 323, TileOffsets: 324, TileByteCounts: 325,
  SampleFormat: 339, ModelPixelScale: 33550, ModelTiepoint: 33922,
  ModelTransformation: 34264, GeoKeyDirectory: 34735, GeoDoubleParams: 34736,
  GeoAsciiParams: 34737, GdalNodata: 42113,
};

const SIZES = { 1: 1, 2: 1, 3: 2, 4: 4, 5: 8, 6: 1, 7: 1, 8: 2, 9: 4, 10: 8, 11: 4, 12: 8 };
const MAX_PREVIEW_PIXELS = 4_000_000;
const MAX_FILE_BYTES = 64 * 1024 * 1024;

const COMPRESSION = { 1: "未压缩", 5: "LZW", 6: "JPEG", 7: "JPEG", 8: "DEFLATE", 32773: "PackBits" };
const SAMPLE_FORMAT = { 1: "无符号整数", 2: "有符号整数", 3: "浮点" };

function readValues(buffer, little, type, count, offset) {
  const values = [];
  for (let index = 0; index < count; index++) {
    const at = offset + index * (SIZES[type] ?? 1);
    if (type === 1 || type === 2 || type === 6 || type === 7) values.push(buffer[at]);
    else if (type === 3) values.push(little ? buffer.readUInt16LE(at) : buffer.readUInt16BE(at));
    else if (type === 4) values.push(little ? buffer.readUInt32LE(at) : buffer.readUInt32BE(at));
    else if (type === 5) {
      const numerator = little ? buffer.readUInt32LE(at) : buffer.readUInt32BE(at);
      const denominator = little ? buffer.readUInt32LE(at + 4) : buffer.readUInt32BE(at + 4);
      values.push(numerator / (denominator || 1));
    } else if (type === 8) values.push(little ? buffer.readInt16LE(at) : buffer.readInt16BE(at));
    else if (type === 9) values.push(little ? buffer.readInt32LE(at) : buffer.readInt32BE(at));
    else if (type === 11) values.push(little ? buffer.readFloatLE(at) : buffer.readFloatBE(at));
    else if (type === 12) values.push(little ? buffer.readDoubleLE(at) : buffer.readDoubleBE(at));
    else if (type === 16) values.push(Number(little ? buffer.readBigUInt64LE(at) : buffer.readBigUInt64BE(at)));
    else if (type === 17) values.push(little ? buffer.readInt8(at) : buffer.readInt8(at));
    else if (type === 18) values.push(Number(little ? buffer.readBigInt64LE(at) : buffer.readBigInt64BE(at)));
    else values.push(buffer[at]);
  }
  return values;
}

/** Parse the first IFD and its GeoKeys. */
export function parseTiff(buffer) {
  const order = buffer.toString("latin1", 0, 2);
  if (order !== "II" && order !== "MM") throw new Error("不是 TIFF 文件");
  const little = order === "II";
  const magic = little ? buffer.readUInt16LE(2) : buffer.readUInt16BE(2);
  const big = magic === 43;
  if (magic !== 42 && magic !== 43) throw new Error("不支持的 TIFF 版本");
  const headerSize = big ? 8 : 4;
  const ifdOffset = big
    ? Number(little ? buffer.readBigUInt64LE(4) : buffer.readBigUInt64BE(4))
    : little ? buffer.readUInt32LE(4) : buffer.readUInt32BE(4);
  const countSize = big ? 8 : 2;
  const count = big
    ? Number(little ? buffer.readBigUInt64LE(ifdOffset) : buffer.readBigUInt64BE(ifdOffset))
    : little ? buffer.readUInt16LE(ifdOffset) : buffer.readUInt16BE(ifdOffset);
  const entrySize = big ? 20 : 12;
  const valueFieldSize = big ? 8 : 4;
  const tags = new Map();
  for (let index = 0; index < count; index++) {
    const base = ifdOffset + countSize + index * entrySize;
    const tag = little ? buffer.readUInt16LE(base) : buffer.readUInt16BE(base);
    const type = little ? buffer.readUInt16LE(base + 2) : buffer.readUInt16BE(base + 2);
    const valueCount = big
      ? Number(little ? buffer.readBigUInt64LE(base + 4) : buffer.readBigUInt64BE(base + 4))
      : little ? buffer.readUInt32LE(base + 4) : buffer.readUInt32BE(base + 4);
    const fieldOffset = base + (big ? 12 : 8);
    const byteLength = (SIZES[type] ?? 1) * valueCount;
    let dataOffset = fieldOffset;
    if (byteLength > valueFieldSize) {
      dataOffset = big
        ? Number(little ? buffer.readBigUInt64LE(fieldOffset) : buffer.readBigUInt64BE(fieldOffset))
        : little ? buffer.readUInt32LE(fieldOffset) : buffer.readUInt32BE(fieldOffset);
    }
    if (dataOffset + byteLength > buffer.length) continue;
    tags.set(tag, { type, count: valueCount, values: readValues(buffer, little, type, valueCount, dataOffset) });
  }
  void headerSize;
  return { little, big, tags };
}

function geoInfo(tags) {
  const scale = tags.get(TAG.ModelPixelScale)?.values;
  const tiepoint = tags.get(TAG.ModelTiepoint)?.values;
  const matrix = tags.get(TAG.ModelTransformation)?.values;
  const keys = tags.get(TAG.GeoKeyDirectory)?.values;
  const ascii = tags.get(TAG.GeoAsciiParams)?.values;
  let crs = "";
  if (ascii?.length) crs = Buffer.from(ascii.filter((code) => code !== 0 && code !== 124)).toString("latin1").trim();
  let geographic = false;
  let projectedCode = null;
  if (keys && keys.length >= 4) {
    const count = keys[3];
    for (let index = 0; index < count; index++) {
      const base = 4 + index * 4;
      const keyId = keys[base];
      const value = keys[base + 3];
      if (keyId === 1024) geographic = value === 2;
      if (keyId === 2048) geographic = true;
      if (keyId === 3072) projectedCode = value;
    }
  }
  if (matrix?.length === 16) {
    const [a, b, , , c, d] = matrix;
    return { crs, geographic: geographic || Math.abs(a) < 1, origin: [c, d], pixel: [a, b] };
  }
  if (scale?.length >= 2 && tiepoint?.length >= 6)
    return { crs, geographic: geographic || (Math.abs(scale[0]) < 1 && Math.abs(scale[1]) < 1), origin: [tiepoint[3], tiepoint[4]], pixel: [scale[0], -scale[1]] };
  return { crs, geographic, origin: null, pixel: null, projectedCode };
}

/** One raw-sample reader for the (bits, sampleFormat) pair the file declares. */
function sampleReader(little, bits, sampleFormat) {
  if (sampleFormat === 3 && bits === 32) return (buffer, at) => (little ? buffer.readFloatLE(at) : buffer.readFloatBE(at));
  if (sampleFormat === 3 && bits === 64) return (buffer, at) => (little ? buffer.readDoubleLE(at) : buffer.readDoubleBE(at));
  if (sampleFormat === 2 && bits === 8) return (buffer, at) => buffer.readInt8(at);
  if (sampleFormat === 2 && bits === 16) return (buffer, at) => (little ? buffer.readInt16LE(at) : buffer.readInt16BE(at));
  if (sampleFormat === 2 && bits === 32) return (buffer, at) => (little ? buffer.readInt32LE(at) : buffer.readInt32BE(at));
  if (bits === 8) return (buffer, at) => buffer.readUInt8(at);
  if (bits === 16) return (buffer, at) => (little ? buffer.readUInt16LE(at) : buffer.readUInt16BE(at));
  if (bits === 32) return (buffer, at) => (little ? buffer.readUInt32LE(at) : buffer.readUInt32BE(at));
  if (bits === 64) return (buffer, at) => Number(little ? buffer.readBigUInt64LE(at) : buffer.readBigUInt64BE(at));
  return null;
}

function decodeSamples(buffer, little, tags, width, height, bits, sampleFormat, compression) {
  const read = sampleReader(little, bits, sampleFormat);
  if (!read) return null;
  const values = new Float64Array(width * height).fill(NaN);
  const bytesPerSample = bits / 8;
  const inflate = (chunk) => {
    try {
      return compression === 8 ? inflateSync(chunk) : chunk;
    } catch {
      return null;
    }
  };
  const copyBlock = (data, blockWidth, blockHeight, offsetX, offsetY) => {
    if (!data) return;
    for (let row = 0; row < blockHeight; row++) {
      const targetY = offsetY + row;
      if (targetY >= height) break;
      for (let column = 0; column < blockWidth; column++) {
        const targetX = offsetX + column;
        if (targetX >= width) break;
        const at = (row * blockWidth + column) * bytesPerSample;
        if (at + bytesPerSample > data.length) continue;
        values[targetY * width + targetX] = read(data, at);
      }
    }
  };
  const tileWidth = tags.get(TAG.TileWidth)?.values?.[0];
  const tileLength = tags.get(TAG.TileLength)?.values?.[0];
  if (tileWidth && tileLength) {
    const offsets = tags.get(TAG.TileOffsets)?.values ?? [];
    const counts = tags.get(TAG.TileByteCounts)?.values ?? [];
    const across = Math.ceil(width / tileWidth);
    for (let index = 0; index < offsets.length; index++) {
      const data = inflate(buffer.subarray(offsets[index], offsets[index] + (counts[index] ?? 0)));
      copyBlock(data, tileWidth, tileLength, (index % across) * tileWidth, Math.floor(index / across) * tileLength);
    }
    return values;
  }
  const rowsPerStrip = tags.get(TAG.RowsPerStrip)?.values?.[0] ?? height;
  const offsets = tags.get(TAG.StripOffsets)?.values ?? [];
  const counts = tags.get(TAG.StripByteCounts)?.values ?? [];
  for (let index = 0; index < offsets.length; index++) {
    const data = inflate(buffer.subarray(offsets[index], offsets[index] + (counts[index] ?? 0)));
    copyBlock(data, width, rowsPerStrip, 0, index * rowsPerStrip);
  }
  return values;
}

const VIRIDIS = [
  [68, 1, 84], [72, 40, 120], [62, 74, 137], [49, 104, 142], [38, 130, 142],
  [31, 158, 137], [53, 183, 121], [109, 205, 89], [180, 222, 44], [253, 231, 37],
];
const colorize = (ratio) => {
  const clamped = Math.max(0, Math.min(1, ratio));
  const position = clamped * (VIRIDIS.length - 1);
  const low = Math.floor(position);
  const high = Math.min(VIRIDIS.length - 1, low + 1);
  const mix = position - low;
  return [0, 1, 2].map((channel) => Math.round(VIRIDIS[low][channel] + (VIRIDIS[high][channel] - VIRIDIS[low][channel]) * mix));
};

function encodePng(width, height, rgb) {
  const raw = Buffer.alloc((width * 3 + 1) * height);
  for (let row = 0; row < height; row++) {
    raw[row * (width * 3 + 1)] = 0;
    rgb.copy(raw, row * (width * 3 + 1) + 1, row * width * 3, (row + 1) * width * 3);
  }
  const chunk = (type, data) => {
    const length = Buffer.alloc(4);
    length.writeUInt32BE(data.length);
    const body = Buffer.concat([Buffer.from(type, "latin1"), data]);
    const checksum = Buffer.alloc(4);
    checksum.writeUInt32BE(crc32(body) >>> 0);
    return Buffer.concat([length, body, checksum]);
  };
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(width, 0);
  ihdr.writeUInt32BE(height, 4);
  ihdr[8] = 8;
  ihdr[9] = 2;
  return Buffer.concat([
    Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
    chunk("IHDR", ihdr),
    chunk("IDAT", deflateSync(raw, { level: 6 })),
    chunk("IEND", Buffer.alloc(0)),
  ]);
}

/**
 * Read a GeoTIFF into metadata plus a bounded PNG preview.
 * @param file - absolute path to the .tif/.tiff file
 */
export async function readGeoTiffPreview(file, size) {
  const handle = await open(file, "r");
  let buffer;
  try {
    const length = Math.min(size, MAX_FILE_BYTES);
    buffer = Buffer.alloc(length);
    await handle.read(buffer, 0, length, 0);
  } finally {
    await handle.close();
  }
  const { little, tags } = parseTiff(buffer);
  const width = tags.get(TAG.ImageWidth)?.values?.[0] ?? 0;
  const height = tags.get(TAG.ImageLength)?.values?.[0] ?? 0;
  const bits = tags.get(TAG.BitsPerSample)?.values?.[0] ?? 1;
  const sampleFormat = tags.get(TAG.SampleFormat)?.values?.[0] ?? 1;
  const compression = tags.get(TAG.Compression)?.values?.[0] ?? 1;
  const samplesPerPixel = tags.get(TAG.SamplesPerPixel)?.values?.[0] ?? 1;
  const photo = tags.get(TAG.Photometric)?.values?.[0] ?? 1;
  const nodata = tags.get(TAG.GdalNodata)?.values
    ? Buffer.from(tags.get(TAG.GdalNodata).values.filter((code) => code !== 0)).toString("latin1").trim()
    : null;
  const geo = geoInfo(tags);
  const info = {
    kind: "raster",
    width,
    height,
    bands: samplesPerPixel,
    bitsPerSample: bits,
    sampleFormat: SAMPLE_FORMAT[sampleFormat] ?? String(sampleFormat),
    compression: COMPRESSION[compression] ?? String(compression),
    photometric: photo,
    crs: geo.crs || (geo.geographic ? "地理坐标（未命名）" : "未声明"),
    nodata,
    bounds: null,
    preview: null,
    note: "",
  };
  if (geo.origin && geo.pixel) {
    const [originX, originY] = geo.origin;
    const [pixelX, pixelY] = geo.pixel;
    const xs = [originX, originX + pixelX * width];
    const ys = [originY, originY + pixelY * height];
    info.bounds = [Math.min(...xs), Math.min(...ys), Math.max(...xs), Math.max(...ys)];
    info.geographicBounds = geo.geographic;
  }
  const unsupported = compression !== 1 && compression !== 8;
  if (unsupported) {
    info.note = `压缩方式为 ${info.compression}，浏览器预览只支持未压缩与 DEFLATE；已给出元数据与范围。`;
    return info;
  }
  if (samplesPerPixel !== 1) {
    info.note = `含 ${samplesPerPixel} 个波段，预览只渲染单波段栅格；已给出元数据与范围。`;
    return info;
  }
  if (width * height > MAX_PREVIEW_PIXELS) {
    info.note = `像元数 ${width * height} 超过预览上限 ${MAX_PREVIEW_PIXELS}；已给出元数据与范围。`;
    return info;
  }
  const raw = decodeSamples(buffer, little, tags, width, height, bits, sampleFormat, compression);
  if (!raw) {
    info.note = "条纹/瓦片布局无法解析，已给出元数据与范围。";
    return info;
  }
  const { png, min, max } = renderPreview(raw, width, height, nodata, sampleFormat);
  info.preview = `data:image/png;base64,${png.toString("base64")}`;
  info.valueRange = [min, max];
  return info;
}


function renderPreview(values, width, height, nodata, sampleFormat) {
  const nodataValue = nodata ? Number(nodata) : NaN;
  const valid = [];
  for (const value of values) {
    if (!Number.isFinite(value)) continue;
    if (Number.isFinite(nodataValue) && value === nodataValue) continue;
    valid.push(value);
  }
  valid.sort((left, right) => left - right);
  const pick = (ratio) => valid.length ? valid[Math.min(valid.length - 1, Math.max(0, Math.floor(ratio * (valid.length - 1))))] : 0;
  const low = pick(0.02);
  const high = pick(0.98);
  const span = high - low || 1;
  const scale = Math.min(1, 320 / Math.max(width, height));
  const outWidth = Math.max(1, Math.round(width * scale));
  const outHeight = Math.max(1, Math.round(height * scale));
  const rgb = Buffer.alloc(outWidth * outHeight * 3, 0);
  for (let row = 0; row < outHeight; row++) {
    const sourceY = Math.min(height - 1, Math.floor(row / scale));
    for (let column = 0; column < outWidth; column++) {
      const sourceX = Math.min(width - 1, Math.floor(column / scale));
      const value = values[sourceY * width + sourceX];
      const at = (row * outWidth + column) * 3;
      if (!Number.isFinite(value) || (Number.isFinite(nodataValue) && value === nodataValue)) {
        rgb[at] = rgb[at + 1] = rgb[at + 2] = 245;
        continue;
      }
      const [red, green, blue] = colorize((value - low) / span);
      rgb[at] = red;
      rgb[at + 1] = green;
      rgb[at + 2] = blue;
    }
  }
  void sampleFormat;
  return { png: encodePng(outWidth, outHeight, rgb), min: low, max: high };
}
