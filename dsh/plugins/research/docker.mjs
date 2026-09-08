import { spawn } from "node:child_process";
import { mkdir, writeFile, readdir, readFile, lstat } from "node:fs/promises";
import { realpathSync } from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { RuntimeLedger } from "../platform/runtime.mjs";
import { WorkspaceFiles } from "../platform/files.mjs";

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
    await this.ensureRecovered();
    await this.storage.checkSpace();
    if (!["inspect", "execute", "gee-download"].includes(request.kind))
      throw new Error("Unsupported operation");
    const id = randomUUID(),
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
      ];
      if (request.kind === "gee-download") {
        if (!this.geeCredentials || !this.geeProject)
          throw new Error("Platform GEE authentication is not configured");
        args.push(
          ...mount(
            realpathSync(this.geeCredentials),
            "/home/worker/.config/earthengine/credentials",
          ),
          ...mount(realpathSync(this.toolkitRoot), "/opt/ntl-toolkit"),
          "-e",
          "PYTHONPATH=/opt/ntl-toolkit",
          "-e",
          `GEE_DEFAULT_PROJECT_ID=${this.geeProject}`,
        );
        if (this.geeProxy) {
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
      }, 1000);
      if (signal?.aborted) cancel();
      job.status = "running";
      await writeFile(path.join(jobRoot, "manifest.json"), JSON.stringify(job));
      let log = "";
      await invoke(["start", "--attach", container], {
        attach: true,
        onOutput: (chunk) => {
          log = (log + chunk).slice(-64000);
        },
      });
      await writeFile(path.join(jobRoot, "execution.log"), log);
      if (job.cancelled) throw new Error("Task cancelled");
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
      const result = JSON.parse(
        await readFile(path.join(output, "result.json"), "utf8"),
      );
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
