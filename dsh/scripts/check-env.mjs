import { readFile, access } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { runtimeLimits } from "../plugins/platform/runtime.mjs";
import { dockerMemoryMiB } from "../plugins/research/docker.mjs";
import { usablePowerShell } from "../development/shell.mjs";

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const envFile = process.env.GEO_ENV_FILE ?? path.join(root, ".env");
try {
  process.loadEnvFile(envFile);
} catch (error) {
  if (error.code !== "ENOENT") throw error;
}
const report = [];
const check = (name, ok, detail) =>
  report.push({ name, ok: Boolean(ok), detail });
if (process.platform === "win32") {
  try { check("Administrator PowerShell", true, usablePowerShell()); }
  catch (error) { check("Administrator PowerShell", false, error.message); }
}
try {
  const limits = runtimeLimits(), reserve = Number(process.env.GEO_MIN_FREE_DISK_MIB ?? 1024);
  check("Resource admission limits", Number.isSafeInteger(reserve) && reserve >= 0, { ...limits, dockerMemoryMiB: dockerMemoryMiB(), minFreeDiskMiB: reserve });
} catch (error) { check("Resource admission limits", false, error.message); }
check(
  "Node.js",
  Number(process.versions.node.split(".")[0]) >= 24,
  process.versions.node,
);
const manifest = JSON.parse(
  await readFile(path.join(root, "package.json"), "utf8"),
);
check(
  "DSH pin",
  manifest.dependencies["@deepseek-ai/dsh"] === "0.1.2-rc.1",
  manifest.dependencies["@deepseek-ai/dsh"],
);
const listing = spawnSync(
  process.platform === "win32" ? "cmd.exe" : "pnpm",
  process.platform === "win32"
    ? ["/d", "/s", "/c", "pnpm list --json --depth Infinity"]
    : ["list", "--json", "--depth", "Infinity"],
  {
    cwd: root,
    encoding: "utf8",
    windowsHide: true,
    maxBuffer: 32 * 1024 * 1024,
  },
);
const versions = new Set(),
  mismatches = new Set();
function visit(node) {
  for (const section of [
    "dependencies",
    "devDependencies",
    "optionalDependencies",
  ])
    for (const [name, record] of Object.entries(node[section] ?? {})) {
      if (name.startsWith("@deepseek-ai/dsh")) {
        versions.add(`${name}@${record.version}`);
        if (record.version !== manifest.dependencies["@deepseek-ai/dsh"])
          mismatches.add(`${name}@${record.version}`);
      }
      visit(record);
    }
}
if (listing.status === 0) JSON.parse(listing.stdout).forEach(visit);
check(
  "Resolved DSH dependency graph",
  listing.status === 0 && versions.size > 100 && !mismatches.size,
  { packages: versions.size, mismatches: [...mismatches] },
);
const fork = path.resolve(
  process.env.GEO_AGENT_TEAMS_DIR ??
    path.join(root, "../../GeoSentinel-AgentTeams"),
);
let built = false;
try {
  await access(path.join(fork, "lib/index.js"));
  built = true;
} catch {}
check("AgentTeams fork build", built, fork);
check(
  "Model credential configured",
  process.env.DEEPSEEK_API_KEY || process.env.DeepSeek_API_KEY,
  "presence only; not printed",
);
check(
  "GEE project configured",
  process.env.GEE_DEFAULT_PROJECT_ID,
  "presence only; not printed",
);
let credential = false;
check("Boundary place-name lookup", true, (process.env.AMAP_API_KEY || process.env.amap_api_key) ? "Amap configured (not printed)" : "Optional Amap key missing; use verified adcode or geoBoundaries/GEE");
try {
  if (process.env.GEO_GEE_CREDENTIALS) {
    await access(process.env.GEO_GEE_CREDENTIALS);
    credential = true;
  }
} catch {}
check("GEE credential file", credential, "presence only; not printed");
const docker = spawnSync(
  "docker",
  [
    "image",
    "inspect",
    process.env.GEO_GIS_IMAGE ?? "geosentinel-gis:0.1",
    "--format",
    "{{.Id}}",
  ],
  { encoding: "utf8", windowsHide: true, timeout: 15000 },
);
check(
  "Docker GIS image",
  docker.status === 0,
  docker.status === 0
    ? docker.stdout.trim()
    : "Build dsh/docker and start Docker first",
);
console.log(
  JSON.stringify({ ok: report.every((x) => x.ok), checks: report }, null, 2),
);
if (report.some((x) => !x.ok)) process.exitCode = 1;
