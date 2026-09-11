import path from "node:path";
import { pathToFileURL } from "node:url";
import { readFile } from "node:fs/promises";
import { launchRelease } from "./runtime.mjs";
export async function launchCompatibleRelease(manager, id, options) {
  await manager.verify(id);
  const root = path.join(manager.releaseRoot(id), "app/dsh");
  const pkg = JSON.parse(await readFile(path.join(root, "package.json"), "utf8"));
  const version = pkg.dependencies?.["@deepseek-ai/dsh"];
  if (version !== "0.1.2-rc.1") return launchRelease(manager, id, options);
  const state = await manager.state();
  if (state.active !== id && !state.history.some(h => h.id === id && h.status === "active")) throw new Error("旧版启动仅限已发布的回滚版本");
  // Verify the frozen modules before importing the legacy launch implementation.
  const { ReleaseManager } = await import(pathToFileURL(path.join(root, "release/manager.mjs")));
  const legacy = await import(pathToFileURL(path.join(root, "release/runtime.mjs")));
  return legacy.launchRelease(new ReleaseManager(manager.source, manager.directory), id, options);
}
