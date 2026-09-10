import assert from "node:assert/strict";
import { mkdtemp, writeFile } from "node:fs/promises";
import path from "node:path";
import { PlatformStore } from "../plugins/platform/store.mjs";
import { DockerRunner } from "../plugins/research/docker.mjs";
process.loadEnvFile(process.env.GEO_ENV_FILE ?? ".env");
const root = await mkdtemp(path.resolve(".runtime/migrated-tools-")), store = new PlatformStore(root);
const user = store.createUser("migration_check", "isolated-test-password"), project = store.createProject(user, "Migration acceptance"), chat = store.createChat(user, project.id);
const identity = { user, projectId: project.id, chatId: chat.id, root: store.chatRoot(user, chat.id) };
const runner = new DockerRunner({ store, toolkitRoot: path.resolve("../packages/ntl_toolkit/src") });
const checks = [];
try {
  for (const parameters of [{ provider: "datav", adcode: "310000", scope: "children", expected_count: 16 }, { provider: "datav", adcode: "420100", scope: "children" }, { provider: "geoboundaries", country: "MMR", adm_level: 0 }]) {
    const result = await runner.run(identity, { kind: "boundary-download", parameters });
    checks.push({ provider: parameters.provider, adcode: parameters.adcode, jobId: result.jobId, count: result.result.feature_count });
    console.log(JSON.stringify(checks.at(-1)));
  }
  const boundary = 'previous/' + checks[0].jobId + '/boundary.geojson';
  const fixture = await runner.run(identity, { kind: "execute" }, { script: "import geopandas as gpd, rasterio, numpy as np\nfrom rasterio.transform import from_bounds\nb=gpd.read_file(" + JSON.stringify(boundary) + ").total_bounds\nwith rasterio.open('outputs/synthetic.tif','w',driver='GTiff',width=512,height=512,count=1,dtype='float32',crs='EPSG:4326',transform=from_bounds(*b,512,512),nodata=-9999) as dst: dst.write(np.full((1,512,512),10,dtype='float32'))\n" });
  const zonal = await runner.run(identity, { kind: "gis", operation: "calculate_zonal_statistics", parameters: { vector_path: boundary, raster_paths: ['outputs/' + fixture.jobId + '/synthetic.tif'], output_path: "outputs/zonal.csv", selected_indices: ["ANTL"] } });
  assert.ok(zonal.files.some((f) => f.path === zonal.jobId + "/zonal.csv"));
  assert.equal(zonal.result.metrics.polygon_count, 16);
  assert.equal(zonal.result.metrics.row_count, 17);
  await runner.run(identity, { kind: "execute" }, { script: "import pandas as pd\ndf=pd.read_csv('previous/" + zonal.jobId + "/zonal.csv')\nassert len(df[df.Region!='Global_Summary'])==16\nassert len(df[df.Region=='Global_Summary'])==1\nassert df['ANTL'].notna().all()\nassert (df['ANTL']-10).abs().max()<1e-6\n" });
  checks.push({ operation: "calculate_zonal_statistics", result: zonal.result, fixture: "synthetic constant 10 raster, not observed NTL" });
  await writeFile(".runtime/migrated-tools-acceptance.json", JSON.stringify({ checks, passed: true }, null, 2));
} finally { runner.runtime.close(); store.close(); }
