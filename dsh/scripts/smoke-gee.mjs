import assert from "node:assert/strict";
import { mkdtemp, writeFile } from "node:fs/promises";
import path from "node:path";
import { PlatformStore } from "../plugins/platform/store.mjs";
import { DockerRunner } from "../plugins/research/docker.mjs";

process.loadEnvFile(process.env.GEO_ENV_FILE ?? ".env");
const root = await mkdtemp(path.resolve(".runtime/gee-")), store = new PlatformStore(root);
const user = store.createUser("gee_check", "isolated-gee-check-password"), project = store.createProject(user, "GEE readiness"), chat = store.createChat(user, project.id);
const identity = { user, projectId: project.id, chatId: chat.id, root: store.chatRoot(user, chat.id) };
const runner = new DockerRunner({ store, toolkitRoot: path.resolve("../packages/ntl_toolkit/src"), geeCredentials: process.env.GEO_GEE_CREDENTIALS, geeProject: process.env.GEE_DEFAULT_PROJECT_ID });
const checks = [];
try {
  for (const parameters of [
    { dataset_id: "USGS/SRTMGL1_003", bands: ["elevation"], bbox: [121.45, 31.15, 121.5, 31.2], scale: 500, asset_type: "Image" },
    { dataset_id: "NOAA/VIIRS/DNB/ANNUAL_V21", bands: ["average_masked"], bbox: [121.45, 31.15, 121.5, 31.2], scale: 500, asset_type: "ImageCollection", start_date: "2020-01-01", end_date: "2020-12-31", reducer: "first" },
  ]) {
    const download = await runner.run(identity, { kind: "gee-download", parameters });
    const inspected = await runner.run(identity, { kind: "inspect", source: "outputs", path: `${download.jobId}/imagery.tif` });
    assert.ok(inspected.result.valid_pixels > 0);
    checks.push({ dataset: parameters.dataset_id, files: download.files, raster: inspected.result });
    console.log(JSON.stringify(checks.at(-1)));
  }
  const report = { passed: true, workspace: root, checks, at: new Date().toISOString() };
  await writeFile(".runtime/gee-readiness-acceptance.json", JSON.stringify(report, null, 2));
} finally { runner.runtime.close(); store.close(); }
