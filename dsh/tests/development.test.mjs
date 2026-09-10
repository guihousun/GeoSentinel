import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { tmpdir } from "node:os";
import { PlatformStore } from "../plugins/platform/store.mjs";
import { developmentAccess } from "../development/access.mjs";
import { rewriteDevelopmentHtml } from "../development/gateway.mjs";
import { usablePowerShell } from "../development/shell.mjs";
import { developmentDraft, validateAppearance } from "../development/product-draft.mjs";

test("remote administrators can unlock development; users, cross-origin and revoked logins cannot", async (t) => {
  const dir = await mkdtemp(path.join(tmpdir(), "geo-admin-access-")), store = new PlatformStore(dir);
  t.after(async () => { store.close(); await rm(dir, { recursive: true, force: true }); });
  store.bootstrapAdmin("administrator", "qa-secret-password");
  store.createUser("researcher", "qa-user-password");
  const admin = store.login("administrator", "qa-secret-password"), normal = store.login("researcher", "qa-user-password");
  const req = (token, origin = "https://geo.example") => ({ method: "POST", headers: { host: "geo.example", origin, cookie: "geosentinel_session=" + token }, socket: { remoteAddress: "203.0.113.8" } });
  let now = 1000; const access = developmentAccess(store, ["geo.example"], { clock: () => now, duration: 100 });
  assert.throws(() => access.check(req(admin.token)), /确认/);
  assert.throws(() => access.confirm(req(normal.token), "qa-user-password"), /管理员/);
  assert.throws(() => access.confirm(req(admin.token, "https://evil.example"), "qa-secret-password"), /来源/);
  assert.throws(() => access.confirm(req(admin.token), "incorrect"), /密码/);
  access.confirm(req(admin.token), "qa-secret-password");
  assert.equal(access.check(req(admin.token)).id, admin.user.id);
  const otherLogin = store.login("administrator", "qa-secret-password");
  assert.throws(() => access.check(req(otherLogin.token)), /确认/);
  now = 1200; assert.throws(() => access.check(req(admin.token)), /确认/);
  access.confirm(req(admin.token), "qa-secret-password");
  store.logout(admin.token); assert.throws(() => access.check(req(admin.token)), /登录/);
});
test("developer HTML routes native assets under the authenticated prefix", () => {
  const html = '<html><head><base href="/"><script>window.boot={"url":"/plugins/a.js"}</script><script src="/assets/a.js"></script></head></html>';
  const result = rewriteDevelopmentHtml(html, "window.test=true;");
  assert.ok(result.includes('<base href="/geo/development/">'));
  assert.ok(result.includes('"url":"/geo/development/plugins/a.js"'));
  assert.ok(result.includes('src="/geo/development/assets/a.js"'));
  assert.ok(result.indexOf("window.test=true") < result.indexOf("window.boot"));
});
test("PowerShell startup skips broken aliases and honors an explicit administrator path", () => {
  const attempted = [];
  const value = usablePowerShell({ ProgramFiles: "broken", PATH: "working" }, (file) => { attempted.push(file); return file.startsWith("working") ? { status: 0 } : { error: { code: "EACCES" } }; });
  assert.ok(value.startsWith("working")); assert.equal(attempted.length, 2);
  assert.equal(usablePowerShell({ GEO_ADMIN_PWSH_PATH: "explicit" }, () => ({ status: 0 })), "explicit");
  assert.throws(() => usablePowerShell({ GEO_ADMIN_PWSH_PATH: "broken" }, () => ({ status: 1 })), /PowerShell 7/);
});
test("product draft copies appearance and model but never administrator authority or credentials", async (t) => {
  const home = await mkdtemp(path.join(tmpdir(), "geo-draft-")); t.after(() => rm(home, { recursive: true, force: true }));
  const root = path.join(home, "development/admin"); await mkdir(root, { recursive: true });
  await writeFile(path.join(root, "settings.yaml"), 'agent-default-model:\n  provider: deepseek-official\n  model: deepseek-v4-flash\nui-theme:\n  preference: light\n  fontSize: 18\npermissions:\n  fullAccess: true\nprivate:\n  apiKey: DO_NOT_COPY\n');
  await writeFile(path.join(root, "dream-skin.json"), JSON.stringify({ "dsh-dream-skin:skin": "system", "dsh-dream-skin:builtin-last": "light" }));
  const draft = await developmentDraft(home, "admin");
  assert.deepEqual(draft.defaultModel, { provider: "deepseek-official", model: "deepseek-v4-flash" });
  assert.equal(draft.appearance.fontSize, 18); assert.equal(draft.appearance.scheme, "light");
  assert.equal(JSON.stringify(draft).includes("DO_NOT_COPY"), false); assert.equal(draft.permissions, undefined);
  await writeFile(path.join(root, "dream-skin.json"), JSON.stringify({ "dsh-dream-skin:wallpaper-kind": "url" }));
  await assert.rejects(developmentDraft(home, "admin"), /图片背景/);
});
test("appearance input rejects executable backgrounds and invalid dimensions", () => {
  const appearance = { skin: "midnight", scheme: "dark", fontSize: 16, wallpaper: "", wash: .8, blur: 0 };
  assert.equal(validateAppearance(appearance), appearance);
  for (const wallpaper of ["https://private.example/image", "data:image/svg+xml;base64,PHN2Zz4=", "data:image/png;base64,ZmFrZQ=="])
    assert.throws(() => validateAppearance({ ...appearance, wallpaper }));
  assert.throws(() => validateAppearance({ ...appearance, fontSize: 8 }));
  assert.throws(() => validateAppearance({ ...appearance, wash: .1 }));
});
