import { spawn } from "node:child_process";
import { mkdir, writeFile, readdir, readFile, lstat } from "node:fs/promises";
import { realpathSync } from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { PlatformError } from "../platform/store.mjs";

function invoke(args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn("docker", args, {
      windowsHide: true,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let output = "";
    for (const stream of [child.stdout, child.stderr])
      stream.on("data", (chunk) => {
        output = (output + chunk.toString()).slice(-32000);
        options.onOutput?.(chunk.toString());
      });
    child.on("error", reject);
    child.on("close", (code) =>
      code === 0
        ? resolve(output)
        : reject(new Error(`Docker exited ${code}: ${output.slice(-3000)}`)),
    );
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
    image = "geosentinel-gis:0.1",
    toolkitRoot,
    geeCredentials,
    geeProject,
    geeProxy = process.env.GEO_GEE_PROXY,
    maxConcurrent = 2,
    timeoutMs = 600000,
    maxOutputBytes = 256 * 1024 * 1024,
  }) {
    Object.assign(this, {
      store,
      image,
      toolkitRoot,
      geeCredentials,
      geeProject,
      geeProxy,
      maxConcurrent,
      timeoutMs,
      maxOutputBytes,
    });
    this.running = new Map();
  }
  async cancelChat(chatId) {
    const jobs = [...this.running.values()].filter(
      (job) => job.chatId === chatId,
    );
    await Promise.all(
      jobs.map(async (job) => {
        job.cancelled = true;
        await invoke(["rm", "-f", job.container]).catch(() => {});
        await job.done;
      }),
    );
  }
  async run(identity, request, { signal, script } = {}) {
    if (signal?.aborted) throw new Error("Task cancelled");
    if (this.running.size >= this.maxConcurrent)
      throw new PlatformError(429, "计算资源忙，请稍后重试");
    if (
      [...this.running.values()].some((job) => job.userId === identity.user.id)
    )
      throw new PlatformError(429, "当前账号已有计算任务");
    if (!["inspect", "execute", "gee-download"].includes(request.kind))
      throw new Error("Unsupported operation");
    const id = randomUUID(),
      container = `geosentinel-${id}`;
    const job = {
      id,
      container,
      chatId: identity.chatId,
      userId: identity.user.id,
      status: "starting",
      startedAt: Date.now(),
      cancelled: false,
    };
    let settle;
    Object.defineProperty(job, "done", {
      value: new Promise((resolve) => {
        settle = resolve;
      }),
    });
    this.running.set(id, job);
    const jobRoot = path.join(this.store.root, "jobs", id),
      output = path.join(identity.root, "outputs", id);
    let timer, outputTimer, abortListener, outputError;
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
      const args = [
        "create",
        "--name",
        container,
        "--label",
        "app=geosentinel",
        "--user",
        "10001:10001",
        "--read-only",
        "--cap-drop=ALL",
        "--security-opt",
        "no-new-privileges",
        "--pids-limit",
        "128",
        "--memory",
        "2g",
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
      await invoke(["rm", "-f", container]).catch(() => {});
      job.finishedAt = Date.now();
      await writeFile(
        path.join(jobRoot, "manifest.json"),
        JSON.stringify(job),
      ).catch(() => {});
      this.running.delete(id);
      settle();
    }
  }
}
