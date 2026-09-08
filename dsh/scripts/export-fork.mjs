import { spawnSync } from "node:child_process";
import { mkdir, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

// Generate a reproducible source patch without staging or committing the developer's fork.
const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const fork = path.resolve(
  process.env.GEO_AGENT_TEAMS_DIR ??
    path.join(root, "../../GeoSentinel-AgentTeams"),
);
const temporaryIndex = path.join(root, ".runtime/fork-export.index");
await mkdir(path.dirname(temporaryIndex), { recursive: true });
await mkdir(path.join(root, "vendor"), { recursive: true });
const env = { ...process.env, GIT_INDEX_FILE: temporaryIndex };
function git(args) {
  const result = spawnSync("git", args, {
    cwd: fork,
    env,
    windowsHide: true,
    encoding: "utf8",
    maxBuffer: 8 * 1024 * 1024,
  });
  if (result.status !== 0) throw new Error(result.stderr);
  return result.stdout;
}
try {
  const base = "da2e2e49242c6ecd7e801a74dba0c8268a0a2f81";
  git(["read-tree", base]);
  git([
    "add",
    "--",
    "src/index.ts",
    "src/tools.ts",
    "src/members.ts",
    "src/geosentinel-policy.ts",
    "scripts/clean-build.mjs",
    "scripts/geosentinel-policy.test.mjs",
    "GEOSENTINEL.md",
    "package.json",
    "pnpm-lock.yaml",
  ]);
  const patch = git(["diff", "--cached", "--binary", base]);
  await writeFile(
    path.join(root, "vendor/agentteams-geosentinel.patch"),
    patch,
  );
  await writeFile(
    path.join(root, "vendor/agentteams-source.json"),
    JSON.stringify(
      {
        upstream: "https://github.com/NanmiCoder/dsh-agent-teams.git",
        tag: "v0.1.15",
        commit: base,
        license: "MIT",
        patch: "agentteams-geosentinel.patch",
      },
      null,
      2,
    ) + "\n",
  );
  await writeFile(
    path.join(root, "vendor/agentteams-LICENSE"),
    git(["show", `${base}:LICENSE`]),
  );
  console.log("Exported independent AgentTeams fork source patch and license.");
} finally {
  await rm(temporaryIndex, { force: true });
}
