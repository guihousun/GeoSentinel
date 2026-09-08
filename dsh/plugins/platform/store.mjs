import { DatabaseSync } from "node:sqlite";
import {
  randomBytes,
  randomUUID,
  scryptSync,
  timingSafeEqual,
  createHash,
} from "node:crypto";
import { mkdirSync, realpathSync, existsSync, lstatSync } from "node:fs";
import path from "node:path";

export class PlatformError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}
const digest = (value) => createHash("sha256").update(value).digest("hex");
const token = () => randomBytes(32).toString("base64url");
const notFound = () => {
  throw new PlatformError(404, "资源不存在");
};
const cleanTitle = (value) => {
  if (typeof value !== "string" || !value.trim() || value.length > 160)
    throw new PlatformError(400, "名称应为 1 至 160 个字符");
  return value.trim();
};
function passwordHash(password) {
  if (
    typeof password !== "string" ||
    password.length < 8 ||
    password.length > 256
  )
    throw new PlatformError(400, "密码须为 8 至 256 个字符");
  const salt = randomBytes(16).toString("hex");
  return `${salt}:${scryptSync(password, salt, 32).toString("hex")}`;
}
function passwordMatches(password, hash) {
  if (typeof password !== "string" || password.length > 256) return false;
  const [salt, stored] = hash.split(":");
  return timingSafeEqual(
    scryptSync(password, salt, 32),
    Buffer.from(stored, "hex"),
  );
}

export class PlatformStore {
  constructor(root) {
    mkdirSync(root, { recursive: true });
    this.root = realpathSync(root);
    this.db = new DatabaseSync(path.join(this.root, "platform.sqlite"));
    this.db
      .exec(`PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;
      CREATE TABLE IF NOT EXISTS users(id TEXT PRIMARY KEY,username TEXT UNIQUE NOT NULL,password TEXT NOT NULL,admin INTEGER NOT NULL DEFAULT 0,disabled INTEGER NOT NULL DEFAULT 0);
      CREATE TABLE IF NOT EXISTS invitations(hash TEXT PRIMARY KEY,expires INTEGER NOT NULL,used INTEGER NOT NULL DEFAULT 0,created_by TEXT NOT NULL REFERENCES users(id));
      CREATE TABLE IF NOT EXISTS logins(hash TEXT PRIMARY KEY,user_id TEXT NOT NULL REFERENCES users(id),expires INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS projects(id TEXT PRIMARY KEY,user_id TEXT NOT NULL REFERENCES users(id),title TEXT NOT NULL,archived INTEGER NOT NULL DEFAULT 0,deleted INTEGER NOT NULL DEFAULT 0,created INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS chats(id TEXT PRIMARY KEY,project_id TEXT NOT NULL REFERENCES projects(id),title TEXT NOT NULL,deleted INTEGER NOT NULL DEFAULT 0,created INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS agent_sessions(id TEXT PRIMARY KEY,chat_id TEXT NOT NULL REFERENCES chats(id),role TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS audit(id INTEGER PRIMARY KEY,at INTEGER NOT NULL,actor TEXT,action TEXT NOT NULL,target TEXT);
    `);
    this.transaction(() => {
      if (!this.db.prepare("PRAGMA table_info(invitations)").all().some((column) => column.name === "reusable"))
        this.db.exec("ALTER TABLE invitations ADD COLUMN reusable INTEGER NOT NULL DEFAULT 0");
      for (const table of ["projects", "chats"]) {
        const columns = this.db.prepare(`PRAGMA table_info(${table})`).all();
        for (const name of ["deleted_at", "purged_at"])
          if (!columns.some((c) => c.name === name)) this.db.exec(`ALTER TABLE ${table} ADD COLUMN ${name} INTEGER`);
        this.db.prepare(`UPDATE ${table} SET deleted_at=? WHERE deleted=1 AND deleted_at IS NULL`).run(Date.now());
      }
    });
    this.dummyPassword = passwordHash(randomBytes(24).toString("hex"));
  }
  close() {
    this.db.close();
  }
  transaction(fn) {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const result = fn();
      this.db.exec("COMMIT");
      return result;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }
  audit(actor, action, target) {
    this.db
      .prepare("INSERT INTO audit(at,actor,action,target) VALUES(?,?,?,?)")
      .run(Date.now(), actor, action, target);
  }
  createUser(username, password, admin = false) {
    if (
      typeof username !== "string" ||
      !/^[A-Za-z0-9_\-]{3,40}$/.test(username)
    )
      throw new PlatformError(
        400,
        "账号须为 3 至 40 位字母、数字、下划线或连字符",
      );
    const record = {
      id: randomUUID(),
      username: username.toLowerCase(),
      admin: Boolean(admin),
    };
    const hash = passwordHash(password);
    if (
      this.db
        .prepare("SELECT id FROM users WHERE username=?")
        .get(record.username)
    )
      throw new PlatformError(409, "账号已存在");
    this.db
      .prepare("INSERT INTO users(id,username,password,admin) VALUES(?,?,?,?)")
      .run(record.id, record.username, hash, Number(admin));
    return record;
  }
  bootstrapAdmin(username, password) {
    return this.transaction(() => {
      if (this.db.prepare("SELECT count(*) AS n FROM users").get().n)
        throw new PlatformError(409, "管理员已初始化");
      return this.createUser(username, password, true);
    });
  }
  requireAdmin(user) {
    const current = this.db
      .prepare("SELECT admin,disabled FROM users WHERE id=?")
      .get(user.id);
    if (!current?.admin || current.disabled)
      throw new PlatformError(403, "需要管理员权限");
  }
  invite(user, ttlMs = 86400000) {
    this.requireAdmin(user);
    if (!Number.isSafeInteger(ttlMs) || ttlMs < 60000 || ttlMs > 7 * 86400000)
      throw new PlatformError(400, "邀请有效期无效");
    const secret = token();
    this.db
      .prepare("INSERT INTO invitations(hash,expires,created_by) VALUES(?,?,?)")
      .run(digest(secret), Date.now() + ttlMs, user.id);
    this.audit(user.id, "invite.create", null);
    return secret;
  }
  persistentInvite(user) {
    this.requireAdmin(user);
    const secret = token();
    this.db.prepare("INSERT INTO invitations(hash,expires,created_by,reusable) VALUES(?,0,?,1)")
      .run(digest(secret), user.id);
    this.audit(user.id, "invite.persistent.create", digest(secret));
    return secret;
  }
  revokeInvite(user, secret) {
    this.requireAdmin(user);
    this.db.prepare("UPDATE invitations SET used=1 WHERE hash=?").run(digest(secret));
    this.audit(user.id, "invite.revoke", digest(secret));
  }
  acceptInvite(secret, username, password) {
    if (typeof secret !== "string" || secret.length > 128)
      throw new PlatformError(400, "邀请无效或已使用");
    return this.transaction(() => {
      const invitation = this.db
        .prepare("SELECT * FROM invitations WHERE hash=?")
        .get(digest(secret));
      if (!invitation || invitation.used || (!invitation.reusable && invitation.expires < Date.now()))
        throw new PlatformError(400, "邀请无效或已使用");
      const user = this.createUser(username, password);
      if (!invitation.reusable)
        this.db.prepare("UPDATE invitations SET used=1 WHERE hash=?").run(digest(secret));
      this.audit(user.id, "invite.accept", null);
      return user;
    });
  }
  login(username, password) {
    const row = this.db
      .prepare("SELECT * FROM users WHERE username=?")
      .get(String(username).toLowerCase());
    const valid = passwordMatches(
      password,
      row?.password ?? this.dummyPassword,
    );
    if (!row || !valid || row.disabled)
      throw new PlatformError(401, "账号或密码错误");
    const secret = token();
    this.db
      .prepare("INSERT INTO logins(hash,user_id,expires) VALUES(?,?,?)")
      .run(digest(secret), row.id, Date.now() + 7 * 86400000);
    this.audit(row.id, "login", null);
    return {
      token: secret,
      user: { id: row.id, username: row.username, admin: Boolean(row.admin) },
    };
  }
  authenticate(secret) {
    if (typeof secret !== "string" || secret.length > 128)
      throw new PlatformError(401, "请先登录");
    const user = this.db
      .prepare(
        "SELECT u.id,u.username,u.admin FROM logins l JOIN users u ON u.id=l.user_id WHERE l.hash=? AND l.expires>? AND u.disabled=0",
      )
      .get(digest(secret), Date.now());
    if (!user) throw new PlatformError(401, "请先登录");
    return { ...user, admin: Boolean(user.admin) };
  }
  logout(secret) {
    if (typeof secret === "string")
      this.db.prepare("DELETE FROM logins WHERE hash=?").run(digest(secret));
  }
  changePassword(user, currentPassword, newPassword) {
    const row = this.db
      .prepare("SELECT password FROM users WHERE id=? AND disabled=0")
      .get(user.id);
    if (!row || !passwordMatches(currentPassword, row.password))
      throw new PlatformError(403, "当前密码错误");
    const hash = passwordHash(newPassword);
    this.transaction(() => {
      this.db
        .prepare("UPDATE users SET password=? WHERE id=?")
        .run(hash, user.id);
      this.db.prepare("DELETE FROM logins WHERE user_id=?").run(user.id);
      this.audit(user.id, "password.change", null);
    });
  }
  disableUser(admin, userId, disabled = true) {
    this.requireAdmin(admin);
    if (userId === admin.id) throw new PlatformError(400, "不能停用当前管理员");
    this.transaction(() => {
      this.db
        .prepare("UPDATE users SET disabled=? WHERE id=?")
        .run(Number(disabled), userId);
      this.db.prepare("DELETE FROM logins WHERE user_id=?").run(userId);
      this.audit(admin.id, "user.disable", userId);
    });
  }
  listUsers(admin) {
    this.requireAdmin(admin);
    return this.db
      .prepare("SELECT id,username,admin,disabled FROM users ORDER BY username")
      .all();
  }
  createProject(user, title) {
    if (this.listProjects(user).length >= 100)
      throw new PlatformError(429, "研究项目已达到账号配额");
    const project = {
      id: randomUUID(),
      user_id: user.id,
      title: cleanTitle(title),
      created: Date.now(),
    };
    this.db
      .prepare("INSERT INTO projects(id,user_id,title,created) VALUES(?,?,?,?)")
      .run(project.id, user.id, project.title, project.created);
    mkdirSync(path.join(this.projectRoot(user, project.id), "inputs"), {
      recursive: true,
    });
    this.audit(user.id, "project.create", project.id);
    return project;
  }
  listProjects(user) {
    return this.db
      .prepare(
        "SELECT id,title,archived,created FROM projects WHERE user_id=? AND deleted=0 ORDER BY created DESC",
      )
      .all(user.id);
  }
  project(user, id) {
    return (
      this.db
        .prepare(
          "SELECT * FROM projects WHERE id=? AND user_id=? AND deleted=0",
        )
        .get(id, user.id) ?? notFound()
    );
  }
  updateProject(user, id, { title, archived, deleted } = {}) {
    this.project(user, id);
    if (title !== undefined)
      this.db
        .prepare("UPDATE projects SET title=? WHERE id=?")
        .run(cleanTitle(title), id);
    for (const [key, value] of Object.entries({ archived, deleted }))
      if (value !== undefined) {
        if (typeof value !== "boolean")
          throw new PlatformError(400, "状态无效");
        this.db
          .prepare(`UPDATE projects SET ${key}=? WHERE id=?`)
          .run(Number(value), id);
        if (key === "deleted") this.db.prepare("UPDATE projects SET deleted_at=? WHERE id=?").run(value ? Date.now() : null, id);
      }
    this.audit(user.id, "project.update", id);
  }
  projectRoot(user, id) {
    this.project(user, id);
    return path.join(this.root, "users", user.id, "projects", id);
  }
  createChat(user, projectId, title = "新研究对话") {
    const project = this.project(user, projectId);
    if (project.archived) throw new PlatformError(409, "项目已归档");
    if (this.listChats(user, projectId).length >= 200)
      throw new PlatformError(429, "项目对话已达到配额");
    const chat = {
      id: randomUUID(),
      project_id: projectId,
      title: cleanTitle(title),
      created: Date.now(),
    };
    this.db
      .prepare("INSERT INTO chats(id,project_id,title,created) VALUES(?,?,?,?)")
      .run(chat.id, projectId, chat.title, chat.created);
    for (const dir of ["inputs", "outputs", "memory"])
      mkdirSync(path.join(this.chatRoot(user, chat.id), dir), {
        recursive: true,
      });
    return chat;
  }
  listChats(user, projectId) {
    this.project(user, projectId);
    return this.db
      .prepare(
        "SELECT * FROM chats WHERE project_id=? AND deleted=0 ORDER BY created",
      )
      .all(projectId);
  }
  chat(user, id) {
    return (
      this.db
        .prepare(
          "SELECT c.*,p.user_id FROM chats c JOIN projects p ON c.project_id=p.id WHERE c.id=? AND p.user_id=? AND c.deleted=0 AND p.deleted=0",
        )
        .get(id, user.id) ?? notFound()
    );
  }
  chatRoot(user, id) {
    const c = this.chat(user, id);
    return path.join(this.projectRoot(user, c.project_id), "chats", c.id);
  }
  updateChat(user, id, { title, deleted } = {}) {
    this.chat(user, id);
    if (title !== undefined)
      this.db
        .prepare("UPDATE chats SET title=? WHERE id=?")
        .run(cleanTitle(title), id);
    if (deleted === true)
      this.db.prepare("UPDATE chats SET deleted=1,deleted_at=? WHERE id=?").run(Date.now(), id);
  }
  recordChild(chatId, childId, role) {
    if (
      !["NTL_Data_Searcher", "NTL_Analyst", "NTL_Event_Tracker"].includes(role)
    )
      throw new PlatformError(400, "未知研究角色");
    const previous = this.db
      .prepare("SELECT * FROM agent_sessions WHERE id=?")
      .get(childId);
    if (previous && (previous.chat_id !== chatId || previous.role !== role))
      throw new PlatformError(409, "助手归属冲突");
    this.db
      .prepare(
        "INSERT OR IGNORE INTO agent_sessions(id,chat_id,role) VALUES(?,?,?)",
      )
      .run(childId, chatId, role);
  }
  listChildren(user, chatId) {
    this.chat(user, chatId);
    return this.db
      .prepare("SELECT id,role FROM agent_sessions WHERE chat_id=?")
      .all(chatId);
  }
}

/** Reject traversal, platform-specific absolute paths and symlinks before file operations. */
export function workspacePath(root, relative) {
  if (
    typeof relative !== "string" ||
    !relative ||
    relative.includes("\0") ||
    relative.includes("\\") ||
    relative.includes(":") ||
    path.isAbsolute(relative)
  )
    throw new PlatformError(400, "文件路径无效");
  const parts = relative.split("/");
  if (parts.some((p) => !p || p === "." || p === ".."))
    throw new PlatformError(400, "文件路径无效");
  const canonical = realpathSync(root);
  let current = canonical;
  for (const part of parts) {
    current = path.join(current, part);
    const info = lstatSync(current, { throwIfNoEntry: false });
    if (info?.isSymbolicLink()) throw new PlatformError(403, "禁止符号链接");
  }
  if (!current.startsWith(canonical + path.sep))
    throw new PlatformError(403, "文件超出工作区");
  return current;
}
