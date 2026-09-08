import { PlatformStore } from "../plugins/platform/store.mjs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { existsSync } from "node:fs";
const runtimeRoot = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const envFile = process.env.GEO_ENV_FILE ?? path.join(runtimeRoot, ".env");
if (existsSync(envFile)) process.loadEnvFile(envFile);
const root =
  process.env.GEO_DATA_DIR ??
  path.join(
    process.env.GEO_DSH_HOME ?? path.join(runtimeRoot, ".runtime/home"),
    "geosentinel",
  );
const username = process.argv[2];
if (!username || !process.env.GEO_BOOTSTRAP_PASSWORD) {
  throw new Error(
    "Usage: set GEO_BOOTSTRAP_PASSWORD in the environment, then node scripts/admin.mjs <username>",
  );
}
const store = new PlatformStore(root);
try {
  const user = store.bootstrapAdmin(
    username,
    process.env.GEO_BOOTSTRAP_PASSWORD,
  );
  console.log(`Administrator created: ${user.username}`);
} finally {
  store.close();
}
