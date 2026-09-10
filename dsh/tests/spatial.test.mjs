import test from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdtemp, mkdir, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { listSpatialFiles, readSpatialFile, spatialLimits } from "../plugins/platform/spatial.mjs";
import { createPlatformHandler } from "../plugins/platform/http.mjs";
import { PlatformStore } from "../plugins/platform/store.mjs";

const geoJson = {
  type: "FeatureCollection",
  features: [
    { type: "Feature", properties: { name: "外滩", antl: 58.35 }, geometry: { type: "Point", coordinates: [121.49, 31.24] } },
    { type: "Feature", properties: { name: "崇明", antl: 2.62 }, geometry: { type: "Polygon", coordinates: [[[121.1, 31.6], [121.9, 31.6], [121.9, 31.9], [121.1, 31.6]]] } },
  ],
};

test("spatial reader keeps geometry, bounds and columns bounded", async (t) => {
  const directory = await mkdtemp(path.join(tmpdir(), "geo-spatial-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  await mkdir(path.join(directory, "outputs", "job-1"), { recursive: true });
  await mkdir(path.join(directory, "memory"), { recursive: true });
  await writeFile(path.join(directory, "outputs", "job-1", "boundary.geojson"), JSON.stringify(geoJson));
  await writeFile(path.join(directory, "outputs", "job-1", "points.csv"), "name,lon,lat,antl\n外滩,121.49,31.24,58.35\n崇明,121.4,31.6,2.62\n坏行,,,\n");
  await writeFile(path.join(directory, "outputs", "job-1", "summary.json"), JSON.stringify({ status: "ok", count: 3 }));
  await writeFile(path.join(directory, "outputs", "job-1", "notes.md"), "# notes");
  await writeFile(path.join(directory, "memory", "scratch.geojson"), JSON.stringify(geoJson));

  const files = await listSpatialFiles(directory);
  assert.deepEqual(files.map((file) => file.path).sort(), [
    "outputs/job-1/boundary.geojson",
    "outputs/job-1/points.csv",
    "outputs/job-1/summary.json",
  ]);

  const boundary = await readSpatialFile(path.join(directory, "outputs", "job-1", "boundary.geojson"), ".geojson");
  assert.equal(boundary.kind, "geojson");
  assert.equal(boundary.featureCount, 2);
  assert.deepEqual(boundary.geometryTypes, { Point: 1, Polygon: 1 });
  assert.deepEqual(boundary.bounds.map((value) => Number(value.toFixed(2))), [121.1, 31.24, 121.9, 31.9]);
  assert.equal(boundary.features[0].properties.name, "外滩");

  const csv = await readSpatialFile(path.join(directory, "outputs", "job-1", "points.csv"), ".csv");
  assert.equal(csv.kind, "csv");
  assert.equal(csv.lonKey, "lon");
  assert.equal(csv.latKey, "lat");
  assert.equal(csv.featureCount, 2);
  assert.deepEqual(csv.features[1].geometry.coordinates, [121.4, 31.6]);

  const summary = await readSpatialFile(path.join(directory, "outputs", "job-1", "summary.json"), ".json");
  assert.equal(summary.kind, "json");
  assert.match(summary.reason, /不含 GeoJSON/);
  assert.deepEqual(summary.keys, ["status", "count"]);

  // Malformed geometry must be dropped, not handed to Leaflet (which throws
  // "Invalid LatLng object" and would blank the whole viewer).
  await writeFile(path.join(directory, "outputs", "job-1", "broken.geojson"), JSON.stringify({
    type: "FeatureCollection",
    features: [
      { type: "Feature", properties: { name: "坏面" }, geometry: { type: "Polygon", coordinates: [[121.5, 31.2], [121.6, 31.2], [121.6, 31.3]] } },
      { type: "Feature", properties: { name: "好点" }, geometry: { type: "Point", coordinates: [121.4, 31.2] } },
      { type: "Feature", properties: { name: "空点" }, geometry: { type: "Point", coordinates: [] } },
    ],
  }));
  const broken = await readSpatialFile(path.join(directory, "outputs", "job-1", "broken.geojson"), ".geojson");
  assert.equal(broken.featureCount, 3);
  assert.equal(broken.returned, 1);
  assert.equal(broken.droppedInvalid, 2);
  assert.deepEqual(broken.geometryTypes, { Point: 1 });

  await writeFile(path.join(directory, "outputs", "job-1", "no-coords.csv"), "name,value\n上海,1\n");
  const noCoords = await readSpatialFile(path.join(directory, "outputs", "job-1", "no-coords.csv"), ".csv");
  assert.equal(noCoords.kind, "unsupported");
  assert.match(noCoords.reason, /经纬度列/);

  const missing = await readSpatialFile(path.join(directory, "outputs", "job-1", "absent.geojson"), ".geojson");
  assert.equal(missing.kind, "missing");
  assert.deepEqual(spatialLimits.extensions.sort(), [".csv", ".geojson", ".json", ".shp", ".tif", ".tiff"]);
});

test("spatial routes stay ownership-checked and fenced to the chat workspace", async (t) => {
  const directory = await mkdtemp(path.join(tmpdir(), "geo-spatial-http-"));
  const store = new PlatformStore(directory);
  const admin = store.bootstrapAdmin("admin", "test-admin-password");
  const alice = store.acceptInvite(store.invite(admin), "alice", "test-alice-password");
  const bob = store.acceptInvite(store.invite(admin), "bob", "test-bob-password");
  const project = store.createProject(alice, "spatial");
  const chat = store.createChat(alice, project.id);
  const root = store.chatRoot(alice, chat.id);
  await mkdir(path.join(root, "outputs", "job-1"), { recursive: true });
  await writeFile(path.join(root, "outputs", "job-1", "boundary.geojson"), JSON.stringify(geoJson));
  await writeFile(path.join(root, "outputs", "job-1", "chart.png"), Buffer.from([0x89, 0x50]));
  // Uploaded project data must be viewable too: it lands in the project inputs
  // directory, not in the chat workspace.
  const inputs = path.join(store.projectRoot(alice, project.id), "inputs");
  await mkdir(inputs, { recursive: true });
  await writeFile(path.join(inputs, "uploaded_points.csv"), "name,lon,lat\n外滩,121.49,31.24\n");
  const handler = createPlatformHandler({ store, bridge: {}, hosts: ["127.0.0.1:0"], secureCookies: false });
  const server = createServer((req, res) => { req.headers.host = "127.0.0.1:0"; return handler(req, res); });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(async () => {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    store.close();
    await rm(directory, { recursive: true, force: true });
  });
  const base = `http://127.0.0.1:${server.address().port}/geo/api`;
  const a = "geosentinel_session=" + store.login("alice", "test-alice-password").token;
  const b = "geosentinel_session=" + store.login("bob", "test-bob-password").token;
  const call = (route, cookie = a) => fetch(base + route, { headers: { cookie } });

  const list = await call(`/chats/${chat.id}/spatial/list`);
  assert.equal(list.status, 200);
  assert.deepEqual((await list.json()).files.map((file) => file.path), [
    "inputs/uploaded_points.csv",
    "outputs/job-1/boundary.geojson",
  ]);

  const read = await call(`/chats/${chat.id}/spatial/read?path=${encodeURIComponent("outputs/job-1/boundary.geojson")}`);
  assert.equal(read.status, 200);
  assert.equal((await read.json()).featureCount, 2);

  const uploaded = await call(`/chats/${chat.id}/spatial/read?path=${encodeURIComponent("inputs/uploaded_points.csv")}`);
  assert.equal(uploaded.status, 200);
  assert.equal((await uploaded.json()).kind, "csv");
  assert.equal((await call(`/chats/${chat.id}/spatial/read?path=${encodeURIComponent("inputs/../secret.geojson")}`)).status, 400);

  assert.equal((await call(`/chats/${chat.id}/spatial/read?path=${encodeURIComponent("outputs/../secret.geojson")}`)).status, 400);
  assert.equal((await call(`/chats/${chat.id}/spatial/read?path=${encodeURIComponent("outputs/job-1/chart.png")}`)).status, 400);
  assert.equal((await call(`/chats/${chat.id}/spatial/list`, b)).status, 404);
  assert.equal((await fetch(`${base}/chats/${chat.id}/spatial/list`)).status, 401);
});
