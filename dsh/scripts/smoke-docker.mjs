import path from "node:path";
import { readFileSync, writeFileSync } from "node:fs";
import { DockerRunner } from "../plugins/research/docker.mjs";
import { PlatformStore } from "../plugins/platform/store.mjs";
if (process.env.GEO_ENV_FILE) process.loadEnvFile(process.env.GEO_ENV_FILE);
const store = new PlatformStore(".runtime/home/geosentinel");
const account = JSON.parse(readFileSync(".runtime/qa-account.json"));
const { user } = store.login(account.username, account.password);
const project = store.createProject(user, "Docker 与 GEE 验收"),
  chat = store.createChat(user, project.id, "下载和分析");
const identity = {
  user,
  projectId: project.id,
  chatId: chat.id,
  root: store.chatRoot(user, chat.id),
};
const runner = new DockerRunner({
  store,
  toolkitRoot: path.resolve("../packages/ntl_toolkit/src"),
  geeCredentials: process.env.GEO_GEE_CREDENTIALS,
  geeProject: process.env.GEE_DEFAULT_PROJECT_ID,
});
try {
  const local = await runner.run(
    identity,
    { kind: "execute" },
    {
      script: `import json, socket
from pathlib import Path
import numpy, rasterio, geopandas
from osgeo import gdal
assert gdal.VersionInfo()
assert not Path('/home/worker/.config/earthengine/credentials').exists()
try:
    Path('/etc/geosentinel-forbidden').write_text('no')
    raise AssertionError('root filesystem writable')
except OSError:
    pass
Path('outputs/acceptance.json').write_text(json.dumps({'gdal': gdal.VersionInfo(), 'isolated': True}))
`,
    },
  );
  console.log(JSON.stringify({ localWorker: local }));
  const downloaded = await runner.run(identity, {
    kind: "gee-download",
    parameters: {
      dataset_id: "USGS/SRTMGL1_003",
      bands: ["elevation"],
      bbox: [96.1, 16.7, 96.15, 16.75],
      scale: 500,
      asset_type: "Image",
    },
  });
  console.log(JSON.stringify({ geeDownload: downloaded }));
  const inspected = await runner.run(identity, {
    kind: "inspect",
    source: "outputs",
    path: `${downloaded.jobId}/imagery.tif`,
  });
  console.log(JSON.stringify({ analysis: inspected }));
  writeFileSync(
    ".runtime/docker-gee-acceptance.json",
    JSON.stringify(
      { projectId: project.id, chatId: chat.id, local, downloaded, inspected },
      null,
      2,
    ),
  );
} finally {
  store.close();
}
