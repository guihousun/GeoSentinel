import { existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { PlatformStore } from "../plugins/platform/store.mjs";
import { RuntimeLedger } from "../plugins/platform/runtime.mjs";
import { cleanupDeleted } from "../plugins/platform/maintenance.mjs";

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const env = process.env.GEO_ENV_FILE ?? path.join(root, ".env");
if (existsSync(env)) process.loadEnvFile(env);
const args = process.argv.slice(2), daysArg = args.find((v) => v.startsWith("--days="));
if (args.some((v) => v !== "--apply" && !v.startsWith("--days="))) throw new Error("Usage: node scripts/maintenance.mjs [--days=30] [--apply]");
const data = process.env.GEO_DATA_DIR ?? path.join(process.env.GEO_DSH_HOME ?? path.join(root, ".runtime/home"), "geosentinel");
if (!existsSync(path.join(data, "platform.sqlite"))) throw new Error("Platform database not found; check GEO_DATA_DIR");
const store = new PlatformStore(data);
let runtime;
try {
  runtime = new RuntimeLedger(store, { recover: false });
  console.log(JSON.stringify(await cleanupDeleted(store, runtime, { days: daysArg ? Number(daysArg.slice(7)) : 30, apply: args.includes("--apply") }), null, 2));
} finally { runtime?.close(); store.close(); }
