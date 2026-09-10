import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { runProduct } from "../release/supervisor.mjs";

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const envFile = process.env.GEO_ENV_FILE || path.join(root, ".env");
if (existsSync(envFile)) process.loadEnvFile(envFile);
if (!process.env.DEEPSEEK_API_KEY && process.env.DeepSeek_API_KEY) process.env.DEEPSEEK_API_KEY = process.env.DeepSeek_API_KEY;
if (!process.env.DEEPSEEK_BASE_URL && process.env.DeepSeek_Coding_URL) process.env.DEEPSEEK_BASE_URL = process.env.DeepSeek_Coding_URL;
try { await runProduct(root, process.argv.slice(2)); }
catch (error) { console.error("GeoSentinel 启动失败：" + error.message); process.exitCode = 1; }
