import test from "node:test";
import assert from "node:assert/strict";
import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

// The plugin contract, enforced instead of described: a plugin is a directory whose
// package name, module export and composition row agree, and which some plane really
// mounts. Both halves of the name→source link (`release/runtime.mjs` for the product
// plane, `development/worker.mjs` + `scripts/admin-start.mjs` for the admin plane) are
// written by hand, so a rename that misses one of them fails here rather than at boot.
const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const PRODUCT_PLANE = ["platform", "research", "workbench"];
const ADMIN_PLANE = ["developer"];

const read = (relative) => readFile(path.join(root, relative), "utf8");

test("每个插件包都符合插件契约，并且确实被某个面挂载", async () => {
  const directories = (await readdir(path.join(root, "plugins"), { withFileTypes: true }))
    .filter((entry) => entry.isDirectory()).map((entry) => entry.name).sort();
  const expected = [...PRODUCT_PLANE, ...ADMIN_PLANE].sort();
  assert.deepEqual(directories, expected, "插件目录集合变化时，plugins/README.md 与两个面的挂载行必须一起更新");

  const profile = await read("profile/cordis.patch.yml");
  const adminStart = await read("scripts/admin-start.mjs");
  const worker = await read("development/worker.mjs");
  const runtime = await read("release/runtime.mjs");

  for (const directory of directories) {
    const manifest = JSON.parse(await read(`plugins/${directory}/package.json`));
    const packageName = `@geosentinel/dsh-${directory}`;
    assert.equal(manifest.name, packageName, `${directory}: package.json 的 name 必须等于目录名派生的包名`);
    assert.equal(manifest.private, true, `${directory}: 产品插件不发布到 npm，private 必须为 true`);
    assert.equal(manifest.type, "module", `${directory}: 必须是 ESM`);
    // Both `"main": "index.mjs"` and `"exports": { ".": "./index.mjs" }` are valid
    // Node spellings of the same entry, so the contract is the resolved file, not
    // the spelling — normalise before comparing instead of churning the manifests.
    const entry = String(manifest.exports?.["."] ?? manifest.main ?? "").replace(/^\.\//, "");
    assert.equal(entry, "index.mjs", `${directory}: 服务端入口必须解析到 index.mjs`);
    if (manifest.exports?.["./client"]) {
      // The browser half's PATH is the plugin's business (`workbench` keeps it under
      // `native/`, `developer` beside the server half); what must hold is that it
      // resolves to a real file and that the client platform is declared.
      const client = String(manifest.exports["./client"]).replace(/^\.\//, "");
      await read(`plugins/${directory}/${client}`).catch(() => assert.fail(`${directory}: exports["./client"] 指向不存在的文件 ${client}`));
      assert.equal(manifest.dsh?.client?.platform, "web", `${directory}: 有浏览器半边时必须声明 dsh.client.platform`);
    }

    const source = await read(`plugins/${directory}/index.mjs`);
    // The RESOLUTION key is the package name, asserted above through package.json and
    // below through the row and the plane's link table. The module's exported `name` is
    // a short diagnostic label every product plugin spells `geosentinel-<dir>`; keeping
    // it stable means logs and diagnostics stay greppable across releases.
    const declared = /export const name\s*=\s*["']([^"']+)["']/.exec(source)?.[1];
    assert.equal(declared, `geosentinel-${directory}`, `${directory}: index.mjs 导出的 name 必须稳定为 geosentinel-${directory}`);
    assert.match(source, /export (?:async )?function apply\s*\(/, `${directory}: 必须导出 apply(ctx, config)`);
    const inject = /export const inject\s*=\s*\[([^\]]*)\]/.exec(source)?.[1];
    if (inject !== undefined)
      for (const service of inject.split(",").map((value) => value.trim().replace(/^["']|["']$/g, "")).filter(Boolean))
        assert.match(service, /^[A-Za-z][\w.]*$/, `${directory}: inject 项 ${service} 必须是服务名，不是路径`);

    if (PRODUCT_PLANE.includes(directory)) {
      assert.ok(runtime.includes(`"${directory}"`), `${directory}: release/runtime.mjs 的链接表必须包含它`);
      assert.ok(profile.includes(`name: '${packageName}'`), `${directory}: 产品 profile 必须有它的挂载行`);
    } else {
      assert.ok(adminStart.includes(packageName), `${directory}: 管理员插件必须在 scripts/admin-start.mjs 里链接并挂载`);
      assert.ok(worker.includes(`"${directory}"`), `${directory}: 开发面 worker 的链接表必须包含它`);
      assert.ok(!profile.includes(`name: '${packageName}'`), `${directory}: 管理员插件不得进入产品 profile（普通用户看不到它）`);
    }
  }
});

test("插件说明与实际插件集合一致，不留下未挂载的孤儿目录或未记录的插件", async () => {
  const documentation = await read("plugins/README.md");
  for (const directory of [...PRODUCT_PLANE, ...ADMIN_PLANE])
    assert.ok(documentation.includes(`\`${directory}/\``), `plugins/README.md 必须记录插件 ${directory}`);
  for (const plugin of await readdir(path.join(root, "plugins"), { withFileTypes: true }).then((entries) => entries.filter((entry) => entry.isDirectory())))
    assert.ok(documentation.includes(`@geosentinel/dsh-${plugin.name}`), `plugins/README.md 必须写出 ${plugin.name} 的包名`);
});
