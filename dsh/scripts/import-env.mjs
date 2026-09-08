import { readFileSync, writeFileSync, existsSync } from "node:fs";
import { parseEnv } from "node:util";
import { parseArgs } from "node:util";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const { values } = parseArgs({ options: { source: { type: "string" }, home: { type: "string" }, credentials: { type: "string" }, proxy: { type: "string" }, port: { type: "string", default: "8510" }, "monitor-dir": { type: "string" } } });
if (!values.source || !values.home || !values.credentials) throw new Error("Usage: node scripts/import-env.mjs --source <existing.env> --home <existing DSH home> --credentials <Earth Engine credentials> [--proxy <URL>] [--port 8511] [--monitor-dir <directory>]");
const target = path.join(root, ".env");
if (existsSync(target)) throw new Error("dsh/.env already exists; edit it deliberately instead of overwriting it");
const source = parseEnv(readFileSync(values.source, "utf8"));
const config = parseEnv(readFileSync(path.join(root, ".env.example"), "utf8"));
for (const key of Object.keys(config)) if (source[key] !== undefined) config[key] = source[key];
config.DEEPSEEK_API_KEY = source.DEEPSEEK_API_KEY || source.DeepSeek_API_KEY || "";
config.DEEPSEEK_BASE_URL = source.DEEPSEEK_BASE_URL || source.DeepSeek_Coding_URL || config.DEEPSEEK_BASE_URL;
config.GEO_DSH_HOME = path.resolve(values.home).replaceAll("\\", "/");
config.GEO_GEE_CREDENTIALS = path.resolve(values.credentials).replaceAll("\\", "/");
if (!existsSync(config.GEO_GEE_CREDENTIALS) || !config.GEE_DEFAULT_PROJECT_ID) throw new Error("A credential file and GEE_DEFAULT_PROJECT_ID are required");
if (values.proxy) {
  const proxy = new URL(values.proxy);
  if (!["http:", "https:"].includes(proxy.protocol)) throw new Error("GEE proxy must use HTTP or HTTPS");
  config.GEO_GEE_PROXY = values.proxy;
}
if (values["monitor-dir"]) config.GEO_MONITOR_DIR = path.resolve(values["monitor-dir"]).replaceAll("\\", "/");
const port = Number(values.port);
if (!Number.isSafeInteger(port) || port < 1 || port > 65535) throw new Error("Invalid port");
config.GEO_ALLOWED_HOSTS = `127.0.0.1:${port},localhost:${port}`;
const body = "# Local GeoSentinel configuration. Never commit this file.\n" + Object.entries(config).map(([key, value]) => `${key}=${JSON.stringify(value)}`).join("\n") + "\n";
const roundtrip = parseEnv(body);
if (Object.entries(config).some(([k, v]) => roundtrip[k] !== v)) throw new Error("Configuration contains unsupported quoting; no file written");
writeFileSync(target, body, { flag: "wx", mode: 0o600 });
console.log(JSON.stringify({ written: target, home: config.GEO_DSH_HOME, credentialConfigured: true, projectConfigured: true, proxyConfigured: Boolean(config.GEO_GEE_PROXY), note: "Restart with this GEO_ENV_FILE; the source configuration was not changed" }));
