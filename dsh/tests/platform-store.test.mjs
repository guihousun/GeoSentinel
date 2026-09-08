import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, mkdirSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { PlatformStore, workspacePath } from "../plugins/platform/store.mjs";

function fixture(t) {
  const dir = mkdtempSync(path.join(tmpdir(), "geosentinel-platform-"));
  const store = new PlatformStore(dir);
  t.after(() => {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  });
  const admin = store.bootstrapAdmin("admin", "valid-admin-password");
  const a = store.acceptInvite(
    store.invite(admin),
    "alice",
    "alice-valid-password",
  );
  const b = store.acceptInvite(
    store.invite(admin),
    "bob",
    "bob-valid-password",
  );
  return { store, admin, a, b, dir };
}
test("invitations are mandatory, single-use and cannot mint administrators", (t) => {
  const { store, admin, a } = fixture(t);
  const invitation = store.invite(admin);
  assert.equal(
    store.acceptInvite(invitation, "carol", "carol-valid-password").admin,
    false,
  );
  assert.throws(() =>
    store.acceptInvite(invitation, "other", "other-valid-password"),
  );
  assert.throws(() => store.invite(a), { status: 403 });
  assert.throws(() => store.bootstrapAdmin("admin2", "valid-admin-password"));
  assert.throws(() =>
    store.acceptInvite("invalid", "other", "other-valid-password"),
  );
});
test("persistent invitations support multiple registrations, eight-character passwords and revocation", (t) => {
  const { store, admin, a } = fixture(t);
  assert.throws(() => store.persistentInvite(a), { status: 403 });
  const code = store.persistentInvite(admin);
  assert.throws(() => store.acceptInvite(code, "short", "1234567"), { status: 400 });
  for (const username of ["persist_a", "persist_b"]) {
    assert.equal(store.acceptInvite(code, username, "Test!234").admin, false);
    assert.equal(store.login(username, "Test!234").user.username, username);
  }
  assert.throws(() => store.revokeInvite(a, code), { status: 403 });
  store.revokeInvite(admin, code);
  assert.throws(() => store.acceptInvite(code, "revoked", "Test!234"), { status: 400 });
});
test("two users own multiple projects and chats without cross-user access", (t) => {
  const { store, a, b } = fixture(t);
  const p1 = store.createProject(a, "缅甸粮食供应");
  const p2 = store.createProject(a, "洪灾暴露");
  store.createProject(b, "私人项目");
  const c1 = store.createChat(a, p1.id),
    c2 = store.createChat(a, p1.id);
  assert.equal(store.listProjects(a).length, 2);
  assert.equal(store.listChats(a, p1.id).length, 2);
  assert.notEqual(store.chatRoot(a, c1.id), store.chatRoot(a, c2.id));
  for (const access of [
    () => store.project(b, p1.id),
    () => store.chat(b, c1.id),
    () => store.createChat(b, p2.id),
    () => store.updateProject(b, p1.id, { deleted: true }),
    () => store.chatRoot(b, c2.id),
  ])
    assert.throws(access, { status: 404 });
});
test("disabled accounts revoke every login and persisted session secrets are hashed", (t) => {
  const { store, admin, a } = fixture(t);
  const first = store.login("alice", "alice-valid-password"),
    second = store.login("alice", "alice-valid-password");
  assert.equal(store.authenticate(first.token).id, a.id);
  assert.notEqual(
    store.db.prepare("SELECT hash FROM logins LIMIT 1").get().hash,
    first.token,
  );
  store.disableUser(admin, a.id);
  for (const value of [first.token, second.token])
    assert.throws(() => store.authenticate(value), { status: 401 });
});

test("password changes verify current password and revoke every existing login", (t) => {
  const { store, a } = fixture(t);
  const first = store.login("alice", "alice-valid-password");
  assert.throws(
    () => store.changePassword(a, "wrong-password", "new-valid-password"),
    { status: 403 },
  );
  assert.equal(store.authenticate(first.token).id, a.id);
  store.changePassword(a, "alice-valid-password", "new-valid-password");
  assert.throws(() => store.authenticate(first.token), { status: 401 });
  assert.throws(() => store.login("alice", "alice-valid-password"), {
    status: 401,
  });
  assert.equal(store.login("alice", "new-valid-password").user.id, a.id);
});
test("project and chat deletion removes access without deleting stored evidence", (t) => {
  const { store, a } = fixture(t);
  const p = store.createProject(a, "Project"),
    c = store.createChat(a, p.id);
  store.updateProject(a, p.id, { archived: true });
  assert.throws(() => store.createChat(a, p.id), { status: 409 });
  store.updateProject(a, p.id, { deleted: true });
  assert.throws(() => store.chat(a, c.id), { status: 404 });
});
test("workspace paths reject traversal, Windows escapes and symlink escapes", (t) => {
  const { dir } = fixture(t);
  assert.equal(
    workspacePath(dir, "safe/file.tif"),
    path.join(dir, "safe", "file.tif"),
  );
  for (const file of [
    "../secret",
    "/etc/passwd",
    "C:/secret",
    "x\\..\\secret",
    "x/../secret",
    "x//file",
    "x:stream",
  ])
    assert.throws(() => workspacePath(dir, file));
  const outside = mkdtempSync(path.join(tmpdir(), "geosentinel-outside-"));
  t.after(() => rmSync(outside, { recursive: true, force: true }));
  symlinkSync(outside, path.join(dir, "escape"), "junction");
  assert.throws(() => workspacePath(dir, "escape/secret"), { status: 403 });
});
