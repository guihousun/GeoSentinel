import { spawn } from "node:child_process";
import { mkdir, writeFile, readdir, readFile, lstat, unlink } from "node:fs/promises";
import { realpathSync, statSync } from "node:fs";
import path from "node:path";
import { randomBytes, randomUUID } from "node:crypto";
import { RuntimeLedger } from "../platform/runtime.mjs";
import { WorkspaceFiles } from "../platform/files.mjs";
import { PlatformError } from "../platform/store.mjs";
import { parseShareDirs, shareMountTarget } from "../platform/share.mjs";

/** Administrator-curated shared data (`GEO_SHARE_DIRS`), read-only everywhere. */
const SHARE_LIBRARIES = parseShareDirs();

/** `--mount` entries that expose the shared data roots inside the sandbox. */
export function shareMounts(entries = SHARE_LIBRARIES, mountOne = defaultShareMount) {
  return entries.flatMap((entry) => mountOne(entry.root, shareMountTarget(entry)));
}

function defaultShareMount(source, target) {
  if (source.includes(",")) throw new Error("Docker bind paths cannot contain commas");
  return ["--mount", `type=bind,source=${source},target=${target},readonly`];
}

export function dockerMemoryMiB(value = process.env.GEO_DOCKER_MEMORY_MIB ?? 3072) {
  const memory = Number(value);
  if (!Number.isSafeInteger(memory) || memory < 256 || memory > 65536) throw new Error("GEO_DOCKER_MEMORY_MIB must be an integer between 256 and 65536");
  return memory;
}

function invoke(args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn("docker", args, {
      windowsHide: true,
      stdio: ["ignore", "pipe", "pipe"],
    });
    const timeout = setTimeout(() => child.kill(), options.timeoutMs ?? 30000);
    if (options.attach) clearTimeout(timeout);
    let output = "";
    for (const stream of [child.stdout, child.stderr])
      stream.on("data", (chunk) => {
        output = (output + chunk.toString()).slice(-32000);
        options.onOutput?.(chunk.toString());
      });
    child.on("error", (error) => { clearTimeout(timeout); reject(error); });
    child.on("close", (code) => {
      clearTimeout(timeout);
      code === 0
        ? resolve(output)
        : reject(new Error(`Docker exited ${code}: ${output.slice(-3000)}`));
    });
  });
}
const mount = (source, target, readonly = true) => {
  if (source.includes(","))
    throw new Error("Docker bind paths cannot contain commas");
  return [
    "--mount",
    `type=bind,source=${source},target=${target}${readonly ? ",readonly" : ""}`,
  ];
};
async function removeContainer(name) {
  try { await invoke(["rm", "-f", name]); }
  catch (error) {
    const names = await invoke(["ps", "-a", "--filter", `name=${name}`, "--format", "{{.Names}}"]);
    if (names.trim().split(/\r?\n/).includes(name)) throw error;
  }
}

async function outputFiles(
  root,
  prefix = "",
  budget = { bytes: 0, count: 0 },
  maximum = 256 * 1024 * 1024,
) {
  const files = [];
  for (const entry of await readdir(root, { withFileTypes: true })) {
    if (++budget.count > 2000)
      throw new Error("Worker output file count exceeded");
    const filename = path.join(root, entry.name),
      info = await lstat(filename);
    if (info.isSymbolicLink())
      throw new Error("Worker produced a symbolic link");
    if (info.isDirectory())
      files.push(
        ...(await outputFiles(
          filename,
          prefix + entry.name + "/",
          budget,
          maximum,
        )),
      );
    else if (info.isFile()) {
      budget.bytes += info.size;
      if (budget.bytes > maximum)
        throw new Error("Worker output size exceeded");
      files.push({ path: prefix + entry.name, size: info.size });
    }
  }
  return files;
}

export class DockerRunner {
  constructor({
    store,
    runtime,
    image = "geosentinel-gis:0.1",
    toolkitRoot,
    geeCredentials,
    geeProject,
    geeProxy = process.env.GEO_GEE_PROXY,
    timeoutMs = 30 * 60 * 1000,
    memoryMiB = dockerMemoryMiB(),
    maxOutputBytes = 256 * 1024 * 1024,
  }) {
    Object.assign(this, {
      store,
      image,
      toolkitRoot,
      geeCredentials,
      geeProject,
      geeProxy,
      timeoutMs,
      maxOutputBytes,
    });
    this.memoryMiB = dockerMemoryMiB(memoryMiB);
    this.runtime = runtime ?? new RuntimeLedger(store);
    this.storage = new WorkspaceFiles(store, this.runtime);
    this.running = new Map();
  }
  async recover() {
    const pending = this.runtime.needsRecovery("docker");
    if (!pending.length) return;
    const interruptedNames = new Set(pending.map((job) => `geosentinel-${job.id}`));
    const names = await invoke(["ps", "-a", "--filter", `label=geosentinel.scope=${this.runtime.scope}`, "--format", "{{.Names}}"]);
    for (const name of names.trim().split(/\r?\n/).filter(Boolean)) {
      if (!/^geosentinel-[0-9a-f-]{36}$/.test(name)) throw new Error("Unexpected managed container name");
      if (interruptedNames.has(name)) await removeContainer(name);
    }
    for (const job of pending) {
      await writeFile(path.join(this.store.root, "jobs", job.id, "manifest.json"), JSON.stringify({ ...job, status: "interrupted" })).catch((e) => { if (e.code !== "ENOENT") throw e; });
      this.runtime.recovered(job.id);
      this.runtime.finish(job.id, "interrupted", "遗留容器已清理，请检查已有产物后重新提交");
    }
  }
  async ensureRecovered() {
    if (!this.recovery) this.recovery = this.recover().catch((e) => { this.recovery = null; throw e; });
    await this.recovery;
  }
  async waitForSlot(id, signal) {
    while (true) {
      if (signal?.aborted || this.running.get(id)?.cancelled) this.runtime.finish(id, "cancelled", "计算等待已取消");
      const current = this.runtime.get(id);
      if (current.status === "running") return;
      if (current.status !== "queued") throw new Error("Task cancelled or interrupted");
      // Eligible FIFO: a saturated account never blocks another account.
      for (const candidate of this.runtime.queued("docker"))
        if (this.running.has(candidate.id)) this.runtime.start(candidate.id);
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }
  async cancelChat(chatId) {
    const jobs = [...this.running.values()].filter(
      (job) => job.chatId === chatId,
    );
    await Promise.all(
      jobs.map(async (job) => {
        job.cancelled = true;
        if (job.status === "queued") this.runtime.finish(job.id, "cancelled", "用户取消计算");
        if (job.status !== "queued") await removeContainer(job.container);
        await job.done;
      }),
    );
  }
  async run(identity, request, { signal, script } = {}) {
    if (signal?.aborted) throw new Error("Task cancelled");
    const requiresGee = request.kind === "gee-download" || (request.kind === "boundary-download" && request.parameters?.provider === "gee");
    const networked = requiresGee || request.kind === "boundary-download";
    if (requiresGee) {
      if (!this.geeCredentials || !this.geeProject) throw new PlatformError(503, "平台 GEE 授权未配置：请管理员检查实际 GEO_ENV_FILE 中的 GEO_GEE_CREDENTIALS 和 GEE_DEFAULT_PROJECT_ID；这不是数据集不存在，也不需要用户上传替代影像");
      try { if (!statSync(this.geeCredentials).isFile()) throw new Error("not a file"); }
      catch { throw new PlatformError(503, "平台 GEE 凭据文件不可读取，请管理员检查凭据路径和权限"); }
    }
    await this.ensureRecovered();
    await this.storage.checkSpace();
    if (!["inspect", "execute", "gee-download", "boundary-download", "gis"].includes(request.kind))
      throw new Error("Unsupported operation");
    // Job ids double as output directory names, so they carry time, operation
    // and a short random suffix instead of an opaque UUID: sortable, readable
    // in the file explorer, and still collision-free.
    const stamp = new Date().toISOString().slice(0, 19).replace(/[-:]/g, "").replace("T", "-");
    const slug = (request.kind === "gis" ? `gis-${String(request.operation ?? "job")}` : request.kind.replace(/-download$/, ""))
      .toLowerCase().replace(/[^a-z0-9-]/g, "").slice(0, 24) || "job";
    const id = `${stamp}-${slug}-${randomBytes(3).toString("hex")}`,
      container = `geosentinel-${id}`;
    const job = {
      id,
      container,
      chatId: identity.chatId,
      userId: identity.user.id,
      status: "queued",
      startedAt: Date.now(),
      cancelled: false,
    };
    let settle;
    Object.defineProperty(job, "done", {
      value: new Promise((resolve) => {
        settle = resolve;
      }),
    });
    this.runtime.enqueue({ id, kind: "docker", operation: request.kind, user: identity.user, chatId: identity.chatId, payload: { kind: request.kind } });
    this.running.set(id, job);
    const jobRoot = path.join(this.store.root, "jobs", id),
      output = path.join(identity.root, "outputs", id);
    let timer, outputTimer, abortListener, outputError, outputBytes = 0, created = false;
    try {
      await mkdir(jobRoot, { recursive: true });
      await mkdir(output, { recursive: true });
      // Materialize the outputs directory on the host before the container
      // mounts it: a brand-new Windows directory is not always visible through
      // Docker Desktop's 9p cache, and the first container write then fails.
      try {
        const probe = path.join(output, ".geosentinel-probe");
        await writeFile(probe, "");
        await unlink(probe);
      } catch {}
      await writeFile(
        path.join(jobRoot, "request.json"),
        JSON.stringify(request),
      );
      if (request.kind === "execute") {
        if (typeof script !== "string" || script.length > 100000)
          throw new Error("Invalid analysis script");
        await writeFile(path.join(jobRoot, "script.py"), script);
      }
      await writeFile(path.join(jobRoot, "manifest.json"), JSON.stringify(job));
      await this.waitForSlot(id, signal);
      await this.storage.checkSpace(this.maxOutputBytes);
      if (job.cancelled || signal?.aborted || this.runtime.get(id)?.status !== "running") throw new Error("Task cancelled");
      job.status = "starting";
      const args = [
        "create",
        "--name",
        container,
        "--label",
        "app=geosentinel",
        "-e",
        `GEO_JOB_TIMEOUT_SECONDS=${Math.ceil(this.timeoutMs / 1000)}`,
        "--label",
        `geosentinel.scope=${this.runtime.scope}`,
        "--user",
        "10001:10001",
        "--read-only",
        "--cap-drop=ALL",
        "--security-opt",
        "no-new-privileges",
        "--pids-limit",
        "128",
        "--memory",
        `${this.memoryMiB}m`,
        "--cpus",
        "1",
        "--tmpfs",
        "/tmp:rw,nosuid,nodev,size=256m",
        ...mount(jobRoot, "/request"),
        ...mount(output, "/workspace/outputs", false),
        ...mount(
          path.join(
            this.store.projectRoot(identity.user, identity.projectId),
            "inputs",
          ),
          "/workspace/inputs",
        ),
        ...mount(path.join(identity.root, "outputs"), "/workspace/previous"),
        // Administrator-curated shared data, read-only inside the sandbox: the
        // same roots an agent reads on the host as `share/<名称>/…` are visible
        // here as `/workspace/share/<名称>/…`, so large geodata can be analysed
        // without copying it into the project inputs. Read-only is deliberate —
        // the container can never modify shared data, and it never gets a host
        // path back.
        ...shareMounts(SHARE_LIBRARIES),
      ];
      if (requiresGee || request.kind === "gis") args.push(...mount(realpathSync(this.toolkitRoot), "/opt/ntl-toolkit"), "-e", "PYTHONPATH=/opt/ntl-toolkit");
      if (requiresGee) {
        if (!this.geeCredentials || !this.geeProject)
          throw new Error("Platform GEE authentication is not configured");
        args.push(
          ...mount(
            realpathSync(this.geeCredentials),
            "/home/worker/.config/earthengine/credentials",
          ),
          "-e",
          `GEE_DEFAULT_PROJECT_ID=${this.geeProject}`,
        );
      }
      if (request.kind === "boundary-download" && request.parameters?.provider === "datav") {
        const key = process.env.AMAP_API_KEY || process.env.amap_api_key;
        if (key) args.push("-e", `AMAP_API_KEY=${key}`);
      }
      if (networked) {
        if (this.geeProxy && request.parameters?.provider !== "datav") {
          if (!["http:", "https:"].includes(new URL(this.geeProxy).protocol))
            throw new Error("Unsupported acquisition proxy");
          for (const key of [
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "http_proxy",
            "https_proxy",
          ])
            args.push("-e", `${key}=${this.geeProxy}`);
        }
      } else args.push("--network", "none");
      args.push(this.image);
      created = true;
      await invoke(args);
      if (job.cancelled || signal?.aborted) throw new Error("Task cancelled");
      const cancel = () => {
        job.cancelled = true;
        void invoke(["rm", "-f", container]).catch(() => {});
      };
      abortListener = cancel;
      signal?.addEventListener("abort", cancel, { once: true });
      timer = setTimeout(cancel, this.timeoutMs);
      let checking = false;
      outputTimer = setInterval(async () => {
        if (checking) return;
        checking = true;
        try {
          await this.storage.checkSpace();
          await outputFiles(
            output,
            "",
            { bytes: 0, count: 0 },
            this.maxOutputBytes,
          );
        } catch (error) {
          outputError = error;
          cancel();
        } finally {
          checking = false;
        }
        // A 3s cadence keeps the disk guard and output cap while cutting host
        // reads on the Windows bind mount: walking the tree every second while
        // the container writes the same directory is what makes Docker
        // Desktop's 9p layer return EIO.
      }, 3000);
      if (signal?.aborted) cancel();
      job.status = "running";
      await writeFile(path.join(jobRoot, "manifest.json"), JSON.stringify(job));
      let log = "";
      const runContainer = async () => {
        log = "";
        await invoke(["start", "--attach", container], {
          attach: true,
          onOutput: (chunk) => {
            log = (log + chunk).slice(-64000);
          },
        });
      };
      const rebuild = async () => {
        await invoke(["rm", "-f", container]).catch(() => {});
        await invoke(args);
      };
      // Docker Desktop's 9p bind mount intermittently returns EIO while the
      // Windows workspace is mounted into the VM. That is an environment fault,
      // not a task result: the same container usually succeeds on a later run.
      // Retry with a short backoff, both when the container itself dies and
      // when the worker returns an error envelope caused by the mount.
      const transientMountError = (text) =>
        /Input\/output error|Errno 5|\bEIO\b/i.test(String(text));
      const retryMount = async () => {
        for (const delay of [2000, 6000]) {
          if (job.cancelled || signal?.aborted) return false;
          await new Promise((resolve) => setTimeout(resolve, delay));
          if (job.cancelled || signal?.aborted) return false;
          try {
            await rebuild();
            await runContainer();
            return true;
          } catch (error) {
            if (!transientMountError(error.message)) throw error;
          }
        }
        return false;
      };
      try {
        await runContainer();
      } catch (error) {
        if (!transientMountError(error.message) || job.cancelled || signal?.aborted)
          throw error;
        job.retriedForMountError = true;
        if (!(await retryMount())) throw error;
      }
      await writeFile(path.join(jobRoot, "execution.log"), log);
      if (job.cancelled) throw new Error("Task cancelled");
      const readOutcome = async () => {
        const files = (
          await outputFiles(
            output,
            "",
            { bytes: 0, count: 0 },
            this.maxOutputBytes,
          )
        ).map((file) => ({ ...file, path: `${id}/${file.path}` }));
        const resultInfo = await lstat(path.join(output, "result.json"));
        if (resultInfo.size > 1024 * 1024)
          throw new Error("Worker result envelope exceeded");
        return {
          files,
          result: JSON.parse(
            await readFile(path.join(output, "result.json"), "utf8"),
          ),
        };
      };
      let { files, result } = await readOutcome();
      if (
        result?.status === "error" &&
        transientMountError(JSON.stringify(result)) &&
        !job.cancelled &&
        !signal?.aborted
      ) {
        job.retriedForMountError = true;
        if (!(await retryMount())) {
          ({ files, result } = await readOutcome());
        } else {
          await writeFile(path.join(jobRoot, "execution.log"), log);
          if (job.cancelled) throw new Error("Task cancelled");
          ({ files, result } = await readOutcome());
        }
      }
      if (Array.isArray(result.outputs))
        for (const artifact of result.outputs) {
          if (
            typeof artifact.path === "string" &&
            artifact.path.startsWith("/workspace/outputs/")
          ) {
            artifact.path = `outputs/${id}/${artifact.path.slice("/workspace/outputs/".length)}`;
          }
        }
      job.status = "completed";
      outputBytes = files.reduce((sum, file) => sum + file.size, 0);
      return {
        jobId: id,
        files,
        result,
        ...(request.kind === "execute" ? { stdout: log.slice(-16000) } : {}),
      };
    } catch (error) {
      job.status = job.cancelled ? "cancelled" : "failed";
      job.error = String((outputError ?? error).message).slice(0, 3000);
      throw outputError ?? error;
    } finally {
      clearTimeout(timer);
      clearInterval(outputTimer);
      if (abortListener) signal?.removeEventListener("abort", abortListener);
      let cleanupError;
      try { if (created) await removeContainer(container); }
      catch (error) { cleanupError = error; }
      if (cleanupError) {
        this.runtime.db.prepare("UPDATE runtime_jobs SET status='cancelling',recovery=1,error='容器清理失败，暂不释放计算名额' WHERE id=?").run(id);
        this.recovery = null;
      } else this.runtime.finish(id, job.status, job.status === "completed" ? null : job.cancelled ? "用户取消或计算超时" : "计算失败，请检查任务日志", outputBytes);
      job.finishedAt = Date.now();
      await writeFile(
        path.join(jobRoot, "manifest.json"),
        JSON.stringify(job),
      ).catch(() => {});
      this.running.delete(id);
      settle();
      if (cleanupError) throw cleanupError;
    }
  }
}
