import { createServer } from "node:http";
import path from "node:path";
import { PlatformStore } from "../plugins/platform/store.mjs";
import { createPlatformHandler } from "../plugins/platform/http.mjs";
import { developmentGateway } from "../development/gateway.mjs";
import { workbenchHandler } from "../plugins/workbench/index.mjs";
import { releaseService } from "../release/service.mjs";
import { RuntimeLedger } from "../plugins/platform/runtime.mjs";
import { createSidebarHandler } from "../plugins/platform/sidebar-adapter.mjs";
process.loadEnvFile(".env");
const home = path.resolve(".runtime/development-qa"), source = path.resolve("..");
const store = new PlatformStore(path.join(home, "store"));
if (!store.db.prepare("SELECT id FROM users LIMIT 1").get()) { store.bootstrapAdmin("qa_admin", "qa-admin-password"); store.createUser("qa_user", "qa-user-password"); }
const hosts = ["127.0.0.1:8516", "localhost:8516", "geo-qa.example:8516"], runtime = new RuntimeLedger(store);
const development = developmentGateway({ store, hosts, source, home });
const releases = releaseService({ source, directory: path.join(home, "releases"), runtime, store });
const api = createPlatformHandler({ store, hosts, runtime, development, releases, bridge: {}, secureCookies: false });
const web = workbenchHandler();
const sidebar = createSidebarHandler({ store, hosts });
const server = createServer((req, res) => {
  if (req.url.startsWith("/sidebar/api/")) return sidebar(req, res);
  if (req.url.startsWith("/geo/api/")) return api(req, res);
  if (req.url.startsWith("/geo/development/")) return development.http(req, res);
  return web(req, res);
});
server.on("upgrade", (req, socket, head) => { if (req.url.startsWith("/geo/development/")) void development.upgrade(req, socket, head); else socket.destroy(); });
server.listen(8516, "127.0.0.1", () => console.log("Isolated development acceptance: http://127.0.0.1:8516"));
for (const signal of ["SIGINT", "SIGTERM"]) process.once(signal, () => { development.close(); releases.close(); server.close(); runtime.close(); store.close(); });
