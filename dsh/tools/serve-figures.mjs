#!/usr/bin/env node
/**
 * Serve rendered figures to a browser over loopback HTTP.
 *
 * Why this exists: the harness GUI has no route that turns a workspace PNG into
 * a browser-reachable URL — `/api/upload` only accepts POST/DELETE, the client
 * previews uploads from local `blob:` URLs, `ctx.fs.fileUrl()` yields a `file:`
 * URI, and the GenUI `image` component refuses `file:`/`data:`. The product
 * instance solves this with its own artifact route
 * (`/geo/api/chats/:id/files?path=…&inline=1`), but a development session
 * deliberately does not serve `/geo/api/*`, so figures produced for an
 * administrator have no address at all.
 *
 * This tool is only a viewing aid for that case: it exposes one directory
 * read-only on 127.0.0.1 and prints the URL of every figure it can find, ready
 * to paste into a `dsh-ui` `image` component. It never writes, never listens on
 * a public interface, and is not part of any release.
 *
 *   node dsh/tools/serve-figures.mjs [--dir <目录>] [--port 8931] [--list-only]
 *
 * Defaults: `--dir dsh/.runtime/gui-images`, `--port 8931`.
 */
import { createServer } from "node:http";
import { readdir, stat } from "node:fs/promises";
import path from "node:path";

const argv = process.argv.slice(2);
const flag = (name, fallback = undefined) => {
  const index = argv.indexOf("--" + name);
  return index === -1 ? fallback : argv[index + 1];
};
const directory = path.resolve(String(flag("dir", path.join("dsh", ".runtime", "gui-images"))));
const port = Number(flag("port", 8931));
const listOnly = argv.includes("--list-only");
const TYPES = new Map([
  [".png", "image/png"], [".jpg", "image/jpeg"], [".jpeg", "image/jpeg"],
  [".webp", "image/webp"], [".gif", "image/gif"], [".svg", "image/svg+xml"],
]);

/** Every servable figure under the directory, as `{ name, relative, bytes }`. */
async function figures(root, prefix = "") {
  const found = [];
  for (const entry of await readdir(path.join(root, prefix), { withFileTypes: true })) {
    if (entry.name.startsWith(".") || entry.isSymbolicLink()) continue;
    const relative = prefix ? `${prefix}/${entry.name}` : entry.name;
    if (entry.isDirectory()) {
      found.push(...(await figures(root, relative)));
      continue;
    }
    if (!TYPES.has(path.extname(entry.name).toLowerCase())) continue;
    const info = await stat(path.join(root, relative)).catch(() => null);
    if (info?.isFile()) found.push({ name: entry.name, relative, bytes: info.size });
  }
  return found;
}

const found = await figures(directory);
if (listOnly) {
  console.log(`目录：${directory}`);
  for (const figure of found) console.log(`  ${figure.relative}  ${figure.bytes.toLocaleString()} B`);
  console.log(`\n共 ${found.length} 张；` + (found.length ? "加 --port 启动后用下面的 URL 贴进 dsh-ui 的 image 组件：" : "先放入图片再启动。"));
  for (const figure of found) console.log(`  http://127.0.0.1:${port}/${figure.relative}`);
  process.exit(0);
}

const server = createServer(async (request, response) => {
  const requestPath = decodeURIComponent((request.url ?? "/").split("?")[0]).replace(/^\/+/, "");
  const type = TYPES.get(path.extname(requestPath).toLowerCase());
  const target = path.resolve(directory, requestPath);
  if (request.method !== "GET" || !type || !target.startsWith(directory + path.sep)) {
    response.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
    response.end("仅提供只读图片：GET /<文件名>\n");
    return;
  }
  const info = await stat(target).catch(() => null);
  if (!info?.isFile()) {
    response.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
    response.end("文件不存在\n");
    return;
  }
  const { createReadStream } = await import("node:fs");
  response.writeHead(200, { "content-type": type, "cache-control": "no-store", "content-length": String(info.size) });
  createReadStream(target).pipe(response);
});

server.listen(port, "127.0.0.1", () => {
  console.log(`图表服务已启动：http://127.0.0.1:${port}/ （只读，仅回环）`);
  console.log(`目录：${directory}`);
  for (const figure of found) console.log(`  http://127.0.0.1:${port}/${figure.relative}`);
  console.log("把上面的 URL 放进 dsh-ui 的 image 组件即可在对话里显示；Ctrl+C 停止。");
});
