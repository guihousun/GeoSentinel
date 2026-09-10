import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, mkdir, writeFile, rm, readFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import path from "node:path";
import { readGeoTiffPreview } from "../plugins/platform/tiff.mjs";
import { readShapefilePreview, isShapefileSidecar, readDbf } from "../plugins/platform/shapefile.mjs";
import { buildZip } from "../plugins/platform/zip.mjs";
import { readSpatialFile, listSpatialFiles, PREVIEW_EXTENSIONS } from "../plugins/platform/spatial.mjs";
import { workspaceView, viewEntries } from "../plugins/platform/workspace-view.mjs";
import { createSidebarFileHandler } from "../plugins/platform/sidebar-adapter.mjs";
import { PlatformStore } from "../plugins/platform/store.mjs";

/** Little-endian, uncompressed, single-band float32 GeoTIFF. */
function buildTiff({ width, height, values, scale, origin }) {
  const entries = [];
  const extra = [];
  let extraOffsetBase = 0;
  const pixelData = Buffer.alloc(width * height * 4);
  for (let index = 0; index < values.length; index++) pixelData.writeFloatLE(values[index], index * 4);
  const push = (tag, type, count, payload) => {
    entries.push({ tag, type, count, payload });
  };
  const u16 = (array) => {
    const buffer = Buffer.alloc(array.length * 2);
    array.forEach((value, index) => buffer.writeUInt16LE(value, index * 2));
    return buffer;
  };
  const doubles = (array) => {
    const buffer = Buffer.alloc(array.length * 8);
    array.forEach((value, index) => buffer.writeDoubleLE(value, index * 8));
    return buffer;
  };
  const ifdOffset = 8;
  const entryCount = 13;
  const ifdSize = 2 + entryCount * 12 + 4;
  extraOffsetBase = ifdOffset + ifdSize;
  push(256, 3, 1, u16([width]));
  push(257, 3, 1, u16([height]));
  push(258, 3, 1, u16([32]));
  push(259, 3, 1, u16([1]));
  push(262, 3, 1, u16([1]));
  push(273, 4, 1, null); // strip offset — patched below
  push(277, 3, 1, u16([1]));
  push(278, 3, 1, u16([height]));
  push(279, 4, 1, null); // strip byte count — patched below
  push(339, 3, 1, u16([3]));
  push(33550, 12, 3, doubles([scale, scale, 0]));
  push(33922, 12, 6, doubles([0, 0, 0, origin[0], origin[1], 0]));
  push(34735, 3, 16, u16([1, 1, 0, 3, 1024, 0, 1, 2, 2048, 0, 1, 4326, 2057, 0, 1, 9102]));
  const entryBuffers = [];
  let extraCursor = extraOffsetBase;
  for (const entry of entries) {
    const buffer = Buffer.alloc(12);
    buffer.writeUInt16LE(entry.tag, 0);
    buffer.writeUInt16LE(entry.type, 2);
    buffer.writeUInt32LE(entry.count, 4);
    if (entry.payload === null) buffer.writeUInt32LE(0, 8);
    else if (entry.payload.length <= 4) entry.payload.copy(buffer, 8);
    else {
      buffer.writeUInt32LE(extraCursor, 8);
      extra.push(entry.payload);
      extraCursor += entry.payload.length + (entry.payload.length % 2);
    }
    entryBuffers.push(buffer);
  }
  const header = Buffer.alloc(8);
  header.write("II", 0, "latin1");
  header.writeUInt16LE(42, 2);
  header.writeUInt32LE(ifdOffset, 4);
  const ifd = Buffer.alloc(ifdSize);
  ifd.writeUInt16LE(entryCount, 0);
  entryBuffers.forEach((buffer, index) => buffer.copy(ifd, 2 + index * 12));
  ifd.writeUInt32LE(0, 2 + entryCount * 12);
  const extras = [];
  for (const payload of extra) {
    extras.push(payload);
    if (payload.length % 2) extras.push(Buffer.alloc(1));
  }
  const extrasBuffer = Buffer.concat(extras);
  const pixelOffset = extraOffsetBase + extrasBuffer.length;
  ifd.writeUInt32LE(pixelOffset, 2 + 5 * 12 + 8);
  ifd.writeUInt32LE(pixelData.length, 2 + 8 * 12 + 8);
  return Buffer.concat([header, ifd, extrasBuffer, pixelData]);
}

function buildShapefile({ shapes, shapeType }) {
  const records = [];
  shapes.forEach((shape, index) => {
    let body;
    if (shapeType === 1) {
      body = Buffer.alloc(4 + 16);
      body.writeInt32LE(1, 0);
      body.writeDoubleLE(shape[0][0], 4);
      body.writeDoubleLE(shape[0][1], 12);
    } else {
      const rings = shape;
      const points = rings.flat();
      const bbox = [
        Math.min(...points.map((point) => point[0])), Math.min(...points.map((point) => point[1])),
        Math.max(...points.map((point) => point[0])), Math.max(...points.map((point) => point[1])),
      ];
      const header = Buffer.alloc(44);
      header.writeInt32LE(shapeType, 0);
      bbox.forEach((value, position) => header.writeDoubleLE(value, 4 + position * 8));
      header.writeInt32LE(rings.length, 36);
      header.writeInt32LE(points.length, 40);
      const partsBuffer = Buffer.alloc(rings.length * 4);
      let cursor = 0;
      rings.forEach((ring, ringIndex) => {
        partsBuffer.writeInt32LE(cursor, ringIndex * 4);
        cursor += ring.length;
      });
      const pointsBuffer = Buffer.alloc(points.length * 16);
      points.forEach((point, pointIndex) => {
        pointsBuffer.writeDoubleLE(point[0], pointIndex * 16);
        pointsBuffer.writeDoubleLE(point[1], pointIndex * 16 + 8);
      });
      body = Buffer.concat([header, partsBuffer, pointsBuffer]);
    }
    const record = Buffer.alloc(8 + body.length);
    record.writeInt32BE(index + 1, 0);
    record.writeInt32BE(body.length / 2, 4);
    body.copy(record, 8);
    records.push(record);
  });
  const all = Buffer.concat(records);
  const allPoints = shapes.flat(2);
  const header = Buffer.alloc(100);
  header.writeInt32BE(9994, 0);
  header.writeInt32BE((100 + all.length) / 2, 24);
  header.writeInt32LE(1000, 28);
  header.writeInt32LE(shapeType, 32);
  [Math.min(...allPoints.map((p) => p[0])), Math.min(...allPoints.map((p) => p[1])),
   Math.max(...allPoints.map((p) => p[0])), Math.max(...allPoints.map((p) => p[1]))]
    .forEach((value, index) => header.writeDoubleLE(value, 36 + index * 8));
  return Buffer.concat([header, all]);
}

function buildDbf(names) {
  const field = Buffer.alloc(32);
  field.write("NAME", 0, "latin1");
  field[11] = 0x43;
  field[16] = 10;
  const headerSize = 32 + 32 + 1;
  const header = Buffer.alloc(headerSize);
  header[0] = 3;
  header.writeUInt32LE(names.length, 4);
  header.writeUInt16LE(headerSize, 8);
  header.writeUInt16LE(11, 10);
  field.copy(header, 32);
  header[headerSize - 1] = 0x0d;
  const records = names.map((name) => Buffer.concat([Buffer.from(" "), Buffer.from(name, "utf8").subarray(0, 10).subarray(0, 10).length < 10 ? Buffer.concat([Buffer.from(name, "utf8"), Buffer.alloc(10 - Buffer.from(name, "utf8").length, 0x20)]) : Buffer.from(name, "utf8").subarray(0, 10)]));
  return Buffer.concat([header, ...records]);
}

test("GeoTIFF preview reads tags, bounds and renders a PNG", async (t) => {
  const directory = await mkdtemp(path.join(tmpdir(), "geo-tiff-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const file = path.join(directory, "demo.tif");
  const values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
  await writeFile(file, buildTiff({ width: 4, height: 3, values, scale: 0.5, origin: [100, 40] }));

  const preview = await readGeoTiffPreview(file, 4096);
  assert.equal(preview.kind, "raster");
  assert.equal(preview.width, 4);
  assert.equal(preview.height, 3);
  assert.equal(preview.bands, 1);
  assert.equal(preview.compression, "未压缩");
  assert.equal(preview.sampleFormat, "浮点");
  assert.deepEqual(preview.bounds.map((value) => Number(value.toFixed(2))), [100, 38.5, 102, 40]);
  assert.equal(preview.geographicBounds, true);
  assert.match(preview.preview, /^data:image\/png;base64,iVBOR/);
  assert.ok(preview.valueRange[0] <= 1.5 && preview.valueRange[1] >= 11, JSON.stringify(preview.valueRange));

  const throughRoute = await readSpatialFile(file, ".tif");
  assert.equal(throughRoute.kind, "raster");
  assert.match(throughRoute.preview, /^data:image\/png;base64,/);
  assert.ok(PREVIEW_EXTENSIONS.includes(".tiff"));
});

test("shapefile preview reads geometry, attributes and hides its companions", async (t) => {
  const directory = await mkdtemp(path.join(tmpdir(), "geo-shp-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const ring = [[121.5, 31.2], [121.6, 31.2], [121.6, 31.3], [121.5, 31.2]];
  await writeFile(path.join(directory, "districts.shp"), buildShapefile({ shapes: [[ring]], shapeType: 5 }));
  await writeFile(path.join(directory, "districts.shx"), Buffer.alloc(108));
  await writeFile(path.join(directory, "districts.prj"), Buffer.from('GEOGCS["WGS 84"]', "utf8"));
  await writeFile(path.join(directory, "districts.dbf"), buildDbf(["外滩"]));

  assert.equal(isShapefileSidecar("districts.shx"), true);
  assert.equal(isShapefileSidecar("districts.dbf"), true);
  assert.equal(isShapefileSidecar("districts.shp"), false);

  const attributes = await readDbf(path.join(directory, "districts.dbf"));
  assert.deepEqual(attributes.fields, ["NAME"]);
  assert.equal(attributes.records[0].NAME, "外滩");

  const preview = await readShapefilePreview(path.join(directory, "districts.shp"), 600);
  assert.equal(preview.kind, "vector");
  assert.equal(preview.shapeType, "面");
  assert.equal(preview.featureCount, 1);
  assert.deepEqual(preview.geometryTypes, { Polygon: 1 });
  assert.deepEqual(preview.bounds.map((value) => Number(value.toFixed(2))), [121.5, 31.2, 121.6, 31.3]);
  assert.equal(preview.features[0].properties.NAME, "外滩");
  assert.deepEqual(preview.features[0].geometry.coordinates[0][0], [121.5, 31.2]);

  // The viewer lists the .shp once: companions are counted, not listed.
  const files = await listSpatialFiles(directory);
  assert.deepEqual(files.map((file) => file.path), ["districts.shp"]);
  assert.equal(files[0].sidecars, 3);
});

test("shapefile downloads bundle the whole set as a zip", async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), "geo-shp-zip-"));
  const store = new PlatformStore(root);
  const admin = store.bootstrapAdmin("admin", "admin-password");
  const user = store.acceptInvite(store.invite(admin), "alice", "user-password");
  const chat = store.createChat(user, store.createProject(user, "p").id, "演示");
  const chatRoot = store.chatRoot(user, chat.id);
  await mkdir(path.join(chatRoot, "outputs", "job-1"), { recursive: true });
  const ring = [[121.5, 31.2], [121.6, 31.2], [121.6, 31.3], [121.5, 31.2]];
  await writeFile(path.join(chatRoot, "outputs", "job-1", "area.shp"), buildShapefile({ shapes: [[ring]], shapeType: 5 }));
  await writeFile(path.join(chatRoot, "outputs", "job-1", "area.shx"), Buffer.alloc(108));
  await writeFile(path.join(chatRoot, "outputs", "job-1", "area.dbf"), buildDbf(["外滩"]));
  const cookie = "geosentinel_session=" + store.login("alice", "user-password").token;

  let handler;
  const server = createServer((req, res) => handler(req, res));
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const host = `127.0.0.1:${server.address().port}`;
  handler = createSidebarFileHandler({ store, hosts: [host] });
  t.after(async () => {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    store.close();
    await rm(root, { recursive: true, force: true });
  });
  const base = `http://${host}`;
  const response = await fetch(`${base}/sidebar/file?sessionId=${chat.id}&path=${encodeURIComponent("/工作区/演示/过程记录/job-1/area.shp")}&download=1`, { headers: { cookie } });
  assert.equal(response.status, 200);
  assert.equal(response.headers.get("content-type"), "application/zip");
  assert.match(response.headers.get("content-disposition"), /attachment; filename\*=UTF-8''area\.zip/);
  const archive = Buffer.from(await response.arrayBuffer());
  assert.equal(archive.readUInt32LE(0), 0x04034b50);
  const names = [];
  for (let at = 0; at < archive.length - 4; at++)
    if (archive.readUInt32LE(at) === 0x04034b50) {
      const length = archive.readUInt16LE(at + 26);
      names.push(archive.toString("utf8", at + 30, at + 30 + length));
    }
  assert.deepEqual(names.sort(), ["area.dbf", "area.shp", "area.shx"]);
});

test("zip writer produces a readable archive envelope", async () => {
  const archive = buildZip([
    { name: "a.txt", data: Buffer.from("hello") },
    { name: "b.txt", data: Buffer.from("x".repeat(500)) },
  ]);
  assert.equal(archive.readUInt32LE(0), 0x04034b50);
  const end = archive.length - 22;
  assert.equal(archive.readUInt32LE(end), 0x06054b50);
  assert.equal(archive.readUInt16LE(end + 8), 2);
  assert.equal(archive.readUInt16LE(end + 10), 2);
});

test("workspace view hides shapefile companions and keeps the main file", async (t) => {
  const directory = await mkdtemp(path.join(tmpdir(), "geo-view-sidecar-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  await mkdir(path.join(directory, "outputs", "job-1"), { recursive: true });
  await writeFile(path.join(directory, "outputs", "job-1", "roads.shp"), Buffer.alloc(128));
  await writeFile(path.join(directory, "outputs", "job-1", "roads.dbf"), Buffer.alloc(64));
  await writeFile(path.join(directory, "outputs", "job-1", "roads.shx"), Buffer.alloc(108));
  await writeFile(path.join(directory, "outputs", "job-1", "chart.png"), Buffer.from([0x89, 0x50]));
  const view = await workspaceView({ chatRoot: directory, inputsRoot: path.join(directory, "in"), projectInputsRoot: path.join(directory, "in2") });
  const spatial = view.grouped.get("空间数据").map((entry) => entry.display);
  const charts = view.grouped.get("图表").map((entry) => entry.display);
  assert.deepEqual(spatial, ["roads.shp"]);
  assert.deepEqual(charts, ["chart.png"]);
  assert.equal(view.grouped.get("其他文件").length, 0);
});

test("saved files keep their bytes regardless of preview", async (t) => {
  const directory = await mkdtemp(path.join(tmpdir(), "geo-preview-readonly-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const file = path.join(directory, "demo.tif");
  const bytes = buildTiff({ width: 2, height: 2, values: [1, 2, 3, 4], scale: 1, origin: [0, 2] });
  await writeFile(file, bytes);
  await readGeoTiffPreview(file, bytes.length);
  assert.equal((await readFile(file)).length, bytes.length);
});

test("the workspace view shows uploads under the name the user chose", async (t) => {
  const directory = await mkdtemp(path.join(tmpdir(), "geo-view-uploads-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const uploads = path.join(directory, ".dsh-uploads");
  await mkdir(uploads, { recursive: true });
  // `dsh-file-upload` prefixes with 16 hex characters, the platform's own uploads with
  // a UUID; both are storage detail, so the view shows the chosen name in each case.
  await writeFile(path.join(uploads, "8430ff1533e2f64f-acceptance.md"), "# 验收\n");
  await writeFile(path.join(uploads, "0f1e2d3c-4b5a-6978-8c9d-0a1b2c3d4e5f-report.pdf"), "%PDF-1.4\n");
  await writeFile(path.join(uploads, "notes.txt"), "no prefix\n");
  const view = await workspaceView({ chatRoot: directory, inputsRoot: path.join(directory, "in"), projectInputsRoot: path.join(directory, "in2") });
  const names = view.uploads.map((entry) => entry.name).sort();
  assert.deepEqual(names, ["acceptance.md", "notes.txt", "report.pdf"]);
  // The real path still points at the stored file, prefix and all.
  const report = view.uploads.find((entry) => entry.name === "report.pdf");
  assert.equal(path.basename(report.real), "0f1e2d3c-4b5a-6978-8c9d-0a1b2c3d4e5f-report.pdf");
});
