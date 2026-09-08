import { randomUUID, createHash } from "node:crypto";
import { hostname } from "node:os";
import { PlatformError } from "./store.mjs";

export function runtimeLimits(env = process.env) {
  const number = (key, fallback, max = 100) => {
    const n = env[key] === undefined ? fallback : Number(env[key]);
    if (!Number.isSafeInteger(n) || n < 1 || n > max) throw new Error(`Invalid ${key}`);
    return n;
  };
  return { research: { global: number("GEO_RESEARCH_CONCURRENCY", 10), user: number("GEO_USER_RESEARCH_CONCURRENCY", 2) },
    docker: { global: number("GEO_DOCKER_CONCURRENCY", 10), user: number("GEO_USER_DOCKER_CONCURRENCY", 2) },
    pending: { global: 200, user: 20 }, promptsPerMinute: 20 };
}
const alive = (pid) => { try { process.kill(pid, 0); return true; } catch (e) { return e.code !== "ESRCH"; } };

/** One host process owns dispatch. SQLite persists waiting work, not the agent loop. */
export class RuntimeLedger {
  constructor(store, { pid = process.pid, host = hostname(), isAlive = alive, clock = Date.now, acquire = true, recover = true, limits = runtimeLimits() } = {}) {
    this.store = store; this.db = store.db; this.clock = clock; this.token = randomUUID(); this.limits = limits;
    this.scope = createHash("sha256").update(store.root).digest("hex").slice(0, 24);
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS runtime_owner(id INTEGER PRIMARY KEY CHECK(id=1),pid INTEGER NOT NULL,host TEXT NOT NULL,token TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS runtime_jobs(id TEXT PRIMARY KEY,kind TEXT NOT NULL,operation TEXT NOT NULL,user_id TEXT NOT NULL,chat_id TEXT NOT NULL,payload TEXT NOT NULL,status TEXT NOT NULL,created INTEGER NOT NULL,started INTEGER,finished INTEGER,error TEXT,output_bytes INTEGER NOT NULL DEFAULT 0,recovery INTEGER NOT NULL DEFAULT 0);
      CREATE INDEX IF NOT EXISTS runtime_jobs_queue ON runtime_jobs(kind,status,created);
      CREATE TABLE IF NOT EXISTS runtime_rates(user_id TEXT PRIMARY KEY,count INTEGER NOT NULL,until INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS runtime_usage(user_id TEXT NOT NULL,day TEXT NOT NULL,prompts INTEGER NOT NULL DEFAULT 0,upload_bytes INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(user_id,day));
    `);
    if (acquire) store.transaction(() => {
      const owner = this.db.prepare("SELECT * FROM runtime_owner WHERE id=1").get();
      if (owner && (owner.host !== host || isAlive(owner.pid))) throw new Error("此平台数据目录已有服务或维护进程占用；不能从另一端口重复启动");
      this.db.prepare("INSERT OR REPLACE INTO runtime_owner VALUES(1,?,?,?)").run(pid, host, this.token);
      // Never replay partially executed work. Research requests not yet dispatched survive.
      if (recover) this.db.prepare("UPDATE runtime_jobs SET status='interrupted',finished=?,error='服务重启，任务中断；请检查已有产物后重新提交',recovery=CASE WHEN status IN ('running','cancelling') THEN 1 ELSE 0 END WHERE status IN ('running','cancelling') OR (kind='docker' AND status='queued')").run(clock());
    });
    this.acquired = acquire;
  }
  assertOwner() {
    if (!this.acquired || this.db.prepare("SELECT token FROM runtime_owner WHERE id=1").get()?.token !== this.token)
      throw new PlatformError(503, "任务调度进程未就绪");
  }
  close() { if (this.acquired && this.db.isOpen) this.db.prepare("DELETE FROM runtime_owner WHERE token=?").run(this.token); this.acquired = false; }
  usage(userId, field, amount = 1) {
    if (!["prompts", "upload_bytes"].includes(field) || !Number.isSafeInteger(amount) || amount < 0) throw new Error("Invalid usage metric");
    const day = new Date(this.clock()).toISOString().slice(0, 10);
    this.db.prepare(`INSERT INTO runtime_usage(user_id,day,${field}) VALUES(?,?,?) ON CONFLICT(user_id,day) DO UPDATE SET ${field}=${field}+excluded.${field}`).run(userId, day, amount);
  }
  throttle(userId) {
    this.store.transaction(() => {
      const now = this.clock(), rate = this.db.prepare("SELECT * FROM runtime_rates WHERE user_id=?").get(userId);
      if (rate?.until > now && rate.count >= this.limits.promptsPerMinute) throw new PlatformError(429, "提交过于频繁，请稍后再试");
      this.db.prepare("INSERT OR REPLACE INTO runtime_rates VALUES(?,?,?)").run(userId, rate?.until > now ? rate.count + 1 : 1, rate?.until > now ? rate.until : now + 60000);
    });
  }
  enqueue({ id = randomUUID(), kind, operation, user, chatId, payload }) {
    this.assertOwner(); this.store.chat(user, chatId);
    if (!["research", "docker"].includes(kind)) throw new Error("Invalid queue kind");
    return this.store.transaction(() => {
      const previous = this.get(id);
      if (previous) { if (previous.user_id !== user.id || previous.chat_id !== chatId) throw new PlatformError(409, "请求归属不匹配"); return previous; }
      const counts = this.db.prepare("SELECT count(*) AS total,sum(user_id=?) AS own FROM runtime_jobs WHERE status='queued'").get(user.id);
      if (counts.total >= this.limits.pending.global || counts.own >= this.limits.pending.user) throw new PlatformError(429, "等待队列已满，请取消部分等待任务后重试");
      const job = { id, kind, operation, user_id: user.id, chat_id: chatId, payload: JSON.stringify(payload), status: "queued", created: this.clock() };
      this.db.prepare("INSERT INTO runtime_jobs(id,kind,operation,user_id,chat_id,payload,status,created) VALUES(?,?,?,?,?,?,?,?)").run(...Object.values(job));
      this.store.audit(user.id, "queue.enqueue", id);
      return job;
    });
  }
  queued(kind) { return this.db.prepare("SELECT * FROM runtime_jobs WHERE kind=? AND status='queued' ORDER BY created,rowid").all(kind); }
  running(kind) { return this.db.prepare("SELECT * FROM runtime_jobs WHERE kind=? AND status IN ('running','cancelling')").all(kind); }
  get(id) { return this.db.prepare("SELECT * FROM runtime_jobs WHERE id=?").get(id); }
  start(id) {
    this.assertOwner();
    return this.store.transaction(() => {
      const job = this.get(id); if (job?.status !== "queued") return false;
      const rows = this.running(job.kind), limits = this.limits[job.kind];
      if (rows.length >= limits.global || rows.filter((r) => r.user_id === job.user_id).length >= limits.user || (job.kind === "research" && rows.some((r) => r.chat_id === job.chat_id))) return false;
      const valid = this.db.prepare("SELECT u.id FROM users u JOIN projects p ON p.user_id=u.id JOIN chats c ON c.project_id=p.id WHERE u.id=? AND c.id=? AND u.disabled=0 AND p.deleted=0 AND p.archived=0 AND c.deleted=0").get(job.user_id, job.chat_id);
      if (!valid) { this.finish(id, "cancelled", "账号或项目已不可用"); return false; }
      this.db.prepare("UPDATE runtime_jobs SET status='running',started=? WHERE id=?").run(this.clock(), id); return true;
    });
  }
  finish(id, status, error = null, bytes = 0) {
    if (!["completed", "failed", "cancelled", "interrupted"].includes(status)) throw new Error("Invalid terminal state");
    this.db.prepare("UPDATE runtime_jobs SET status=?,finished=?,error=?,output_bytes=? WHERE id=? AND status IN ('queued','running','cancelling')").run(status, this.clock(), error?.slice(0, 1000) ?? null, bytes, id);
  }
  cancelling(kind, chatId) { this.db.prepare("UPDATE runtime_jobs SET status='cancelling' WHERE kind=? AND chat_id=? AND status='running'").run(kind, chatId); }
  cancelQueued(chatId) { this.db.prepare("UPDATE runtime_jobs SET status='cancelled',finished=?,error='用户取消等待任务' WHERE chat_id=? AND status='queued'").run(this.clock(), chatId); }
  needsRecovery(kind) { return this.db.prepare("SELECT * FROM runtime_jobs WHERE kind=? AND recovery=1").all(kind); }
  recovered(id) { this.db.prepare("UPDATE runtime_jobs SET recovery=0 WHERE id=?").run(id); }
  snapshot(user, chatId) {
    this.store.chat(user, chatId);
    const jobs = this.db.prepare("SELECT id,kind,operation,status,created,started,finished,error,payload FROM runtime_jobs WHERE chat_id=? ORDER BY created DESC,rowid DESC LIMIT 100").all(chatId).map(({ payload, ...job }) => ({ ...job, label: job.operation === "prompt" ? String(JSON.parse(payload).text ?? "").slice(0, 120) : job.operation === "approve" ? "执行已确认的研究方案" : "空间数据计算" }));
    return { waiting: jobs.filter((j) => j.status === "queued"), running: jobs.filter((j) => ["running", "cancelling"].includes(j.status)), latest: jobs[0] ?? null };
  }
  summary(userId) {
    const filter = userId ? " WHERE user_id=?" : "", args = userId ? [userId] : [];
    return { limits: this.limits,
      jobs: this.db.prepare(`SELECT user_id,kind,status,count(*) AS count,sum(CASE WHEN started IS NOT NULL AND finished IS NOT NULL THEN max(0,finished-started) ELSE 0 END) AS elapsed_ms,sum(output_bytes) AS output_bytes FROM runtime_jobs${filter} GROUP BY user_id,kind,status`).all(...args),
      usage: this.db.prepare(`SELECT user_id,sum(prompts) AS prompts,sum(upload_bytes) AS upload_bytes FROM runtime_usage${filter} GROUP BY user_id`).all(...args) };
  }
}
