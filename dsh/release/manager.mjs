import { readdir, readFile, writeFile, mkdir, rename, lstat, open, unlink } from "node:fs/promises";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import { validateAppearance } from "../development/product-draft.mjs";
import { MAIN_TOOLS, ROLE_DELEGATION } from "../plugins/platform/catalog.mjs";
import { parseProfile } from "./profile-schema.mjs";

const roots = ["dsh/plugins", "dsh/profile", "dsh/skills", "dsh/monitoring", "dsh/docker", "dsh/release", "dsh/development", "dsh/scripts", "dsh/cli", "dsh/tests", "packages/ntl_toolkit/src"];
const singles = ["dsh/package.json", "dsh/pnpm-lock.yaml", "monitoring/sources.py"];
// The monitor collector container mounts the legacy feed adapters by path
// (`<app>/monitoring/sources.py`), so the freeze must carry them explicitly:
// without this entry the release validates while live collection silently
// fails to mount.
const requiredFrozen = ["monitoring/sources.py"];
const sha = (data) => createHash("sha256").update(data).digest("hex");
// Ordinary-user boundaries the product keeps closed in the shipped profile.
// `connection` and `web-runtime` are deliberately NOT here on the 0.1.5 line: the
// native stack depends on the `connection`/`fileUploads` services they publish
// (open-in-app, the upload dock, the deliverables panel and the session
// controller all wait for them), and disabling either aborts the plugin tree.
// Ordinary users still never reach the native shell — `/` is behind DSH's own
// single-user token auth — while `agent-presets` provides the delegation tools
// under the platform allowlist and guard.
const requiredDisabled = ["api-remotes", "directory-picker", "tool-cordis", "tool-workflow"];
// The non-domain tools a research role may hold: loading a skill, reading its
// references/ inside the fenced workspace + skill roots, writing only inside the
// chat's own outputs/ (write/edit), verifying sources on the web (event tracker
// only), reading an uploaded document, and the published remote-MCP queries
// (`mcp__<server>__<tool>`). An MCP name is only a SHAPE here: the product table
// still has to be a subset of the agent-teams row, so a role cannot be granted a
// tool the release never declared.
const ROLE_EXTRA_TOOLS = new Set(["skill", "read", "glob", "write", "edit", "web_search", "web_fetch", "read_document"]);
const MCP_TOOL_NAME = /^mcp__[a-z0-9_-]{1,32}__[a-z0-9_.-]{1,64}$/;
// The three fixed research roles. The agent-teams fork also accepts the legacy
// NTL_* ids for teams created before the rename.
const ROLE_NAMES = new Set(["数据助手", "分析助手", "事件助手"]);
export async function readJson(file, fallback = null) { try { return JSON.parse(await readFile(file, "utf8")); } catch (error) { if (error.code === "ENOENT") return fallback; throw error; } }
export async function atomicJson(file, value) {
  await mkdir(path.dirname(file), { recursive: true });
  const temporary = file + "." + randomUUID() + ".tmp";
  await writeFile(temporary, JSON.stringify(value, null, 2), { flag: "wx" });
  await rename(temporary, file);
}
export function validateProduct(value) {
  if (!value || value.schema !== "geosentinel.product.v1" || Object.keys(value).some((k) => !["schema", "defaultModel", "monitorEnabled", "roleTools", "appearance"].includes(k))) throw new Error("产品配置字段无效；不接收管理员权限或凭据");
  if (value.appearance) validateAppearance(value.appearance);
  if (value.defaultModel?.provider !== "deepseek-official" || !/^deepseek-[a-zA-Z0-9.-]{1,80}$/.test(value.defaultModel?.model ?? "") || Object.keys(value.defaultModel).some((k) => !["provider", "model"].includes(k))) throw new Error("默认模型必须使用已部署的 DeepSeek 提供方；不能在发布配置中保存密钥或自定义连接地址");
  if (typeof value.monitorEnabled !== "boolean") throw new Error("monitorEnabled 必须为布尔值");
  if (value.roleTools !== undefined) {
    if (!value.roleTools || Array.isArray(value.roleTools) || Object.keys(value.roleTools).some((role) => !ROLE_NAMES.has(role))) throw new Error("角色工具配置无效");
    for (const tools of Object.values(value.roleTools)) if (!Array.isArray(tools) || tools.length > 100 || tools.some((name) => !/^geo_[a-z0-9_]+$/.test(name) && !ROLE_EXTRA_TOOLS.has(name) && !MCP_TOOL_NAME.test(name))) throw new Error("用户研究角色只能分配已注册的 geo_ 工具、受限的 skill/read/glob/write/edit/web_search/web_fetch/read_document，以及已发布的 mcp__<server>__<tool>");
  }
  return value;
}
export function validateProfile(text) {
  // The entry-list dialect keeps `!!js` expressions as `{ __jsExpr }` nodes, so
  // a profile that reads a credential from the environment stays an expression
  // instead of degrading into a literal string. See profile-schema.mjs.
  const rows = parseProfile(text);
  if (!Array.isArray(rows)) throw new Error("产品 profile 格式无效");
  for (const id of requiredDisabled) if (rows.filter((r) => r.id === id).length !== 1 || rows.find((r) => r.id === id)?.disabled !== true) throw new Error("不能发布普通用户权限放宽：" + id);
  const inserts = rows.flatMap((row) => row.insert ?? []);
  for (const id of ["geosentinel-platform", "geosentinel-research", "geosentinel-workbench"]) if (!inserts.some((row) => row.id === id && !row.disabled)) throw new Error("产品必要插件缺失：" + id);
  if (inserts.some((r) => requiredDisabled.includes(r.id))) throw new Error("禁止通过重复插件覆盖权限边界");
  return rows;
}
// The agent preset is the agent-plane half of the role boundary: it is what composes a
// specialist, and the profile names it as the default every session mounts. A release
// whose default preset is missing, unparsable, or quietly short of a role row would
// ship a supervisor that cannot delegate — so the freeze validates the exact preset the
// profile names, with the same invariants tests/product-preset.test.mjs asserts,
// including that nothing re-opens host shell or vendor fan-out orchestration.
export function validatePreset(text, product) {
  const rows = parseProfile(text);
  if (!Array.isArray(rows)) throw new Error("agent preset 格式无效");
  const flat = rows.flatMap((row) => (Array.isArray(row?.config) ? row.config : [row]));
  const enabled = flat.filter((row) => row?.disabled !== true);
  const spawners = enabled.filter((row) => row.name === "@deepseek-ai/dsh-tool-subagent");
  const byTool = new Map(spawners.map((row) => [row.config?.toolName, row]));
  for (const [role, tool] of Object.entries(ROLE_DELEGATION)) {
    const row = byTool.get(tool);
    if (!row) throw new Error(`agent preset 缺少启用的角色委派工具：${tool}（${role}）`);
    if (row.config?.provider !== "spawn" || row.config?.backgroundMode !== "continuable") throw new Error(`角色委派行配置不完整：${tool}`);
    if (!String(row.config?.persona ?? "").includes(role)) throw new Error(`角色委派行 persona 未点明 ${role}：${tool}`);
    const table = row.config?.toolFilter?.allow;
    if (!Array.isArray(table) || table.length === 0) throw new Error(`角色委派行缺少工具表：${tool}`);
    const expected = product?.roleTools?.[role];
    if (expected !== undefined && [...table].sort().join(" ") !== [...expected].sort().join(" "))
      throw new Error(`角色工具表与产品配置不一致：${role}`);
  }
  if (spawners.length !== Object.keys(ROLE_DELEGATION).length) throw new Error("agent preset 的委派工具数不等于角色数");
  for (const row of enabled) {
    if (["subagent", "subagent_fork", "subagent_codex", "subagent_claude_code"].includes(row.config?.toolName))
      throw new Error("agent preset 启用了通用或外部 spawner：" + String(row.config?.toolName));
    if (row.name === "@deepseek-ai/dsh-tool-bash" || row.name === "@deepseek-ai/dsh-tool-pwsh")
      throw new Error("agent preset 启用了宿主机 shell：" + String(row.id));
  }
  for (const id of ["tool-bash", "tool-pwsh", "workflow-worker-thread", "tool-workflow", "tool-ralph"])
    if (flat.find((row) => row?.id === id)?.disabled !== true) throw new Error("agent preset 必须保持关闭：" + id);
  return rows;
}
async function filesUnder(root, prefix, output) {
  for (const entry of await readdir(path.join(root, prefix), { withFileTypes: true })) {
    if (["node_modules", "__pycache__", ".runtime", ".env", ".git", ".playwright-cli"].includes(entry.name)) continue;
    const relative = prefix + "/" + entry.name, info = await lstat(path.join(root, relative));
    if (info.isSymbolicLink()) throw new Error("发布源中不允许符号链接：" + relative);
    if (entry.isDirectory()) await filesUnder(root, relative, output);
    else if (entry.isFile() && /\.(mjs|js|json|yml|yaml|py|css|html|md|txt|svg|png|jpg|jpeg|webp|woff|woff2|ico)$|(?:^|\/)Dockerfile$/.test(relative)) {
      if (info.size > 8 * 1024 * 1024) throw new Error("发布源文件过大：" + relative);
      output[relative] = await readFile(path.join(root, relative));
    }
  }
}
export async function collectSource(root) {
  const files = {};
  for (const prefix of roots) await filesUnder(root, prefix, files);
  for (const file of singles) files[file] = await readFile(path.join(root, file));
  for (const file of requiredFrozen) if (!files[file]?.length) throw new Error("发布源缺少必需文件：" + file);
  // The AgentTeams fork is no longer part of the product: the 0.1.5 line runs the
  // four-role flow on the native subagent plane (dsh/plugins/team), so nothing is
  // frozen from an external checkout any more.
  // The default agent preset must travel with the release: the profile names it by id, and
  // a session whose default preset cannot be resolved loses its delegation tools.
  const product = validateProduct(JSON.parse(files["dsh/profile/product.json"]));
  const profileRows = validateProfile(files["dsh/profile/cordis.patch.yml"].toString());
  const presetRow = profileRows.find((row) => row.id === "agent-presets");
  const defaultPreset = presetRow?.config?.default;
  if (typeof defaultPreset !== "string" || defaultPreset.length === 0) throw new Error("产品 profile 必须指定默认 agent preset");
  if (presetRow.disabled === true) throw new Error("默认 agent preset 行不能禁用");
  const presetDir = `dsh/profile/agent-presets/${defaultPreset}/`;
  for (const name of ["agent.cordis.yml", "preset.yml"])
    if (!files[presetDir + name]?.length) throw new Error("发布源缺少默认 agent preset 文件：" + presetDir + name);
  validatePreset(files[presetDir + "agent.cordis.yml"].toString(), product);
  for (const [file, data] of Object.entries(files)) if (/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|(?:sk-[A-Za-z0-9]{24,}|gh[pousr]_[A-Za-z0-9]{25,})/.test(data.toString())) throw new Error("发布源疑似包含凭据：" + file);
  return files;
}
export function fileHashes(files) { return Object.fromEntries(Object.keys(files).sort().map((name) => [name, sha(files[name])])); }
export function changes(before, after) { return [...new Set([...Object.keys(before ?? {}), ...Object.keys(after)])].sort().filter((name) => before?.[name] !== after[name]).map((name) => ({ path: name, kind: !before?.[name] ? "added" : !after[name] ? "removed" : "modified" })); }
export function command(exe, args, cwd, timeout = 600000) {
  return new Promise((resolve, reject) => {
    const inherited = new Set(["PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC", "HOME", "USERPROFILE", "APPDATA", "LOCALAPPDATA", "TEMP", "TMP", "PROGRAMFILES", "PROGRAMW6432", "PNPM_HOME", "NPM_CONFIG_STORE_DIR", "DOCKER_HOST", "DOCKER_CONFIG", "DOCKER_CONTEXT", "DOCKER_CERT_PATH", "DOCKER_TLS_VERIFY"]);
    const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => inherited.has(key.toUpperCase())));
    env.CI = "true";
    const child = spawn(exe, args, { cwd, env, windowsHide: true, shell: false, stdio: ["ignore", "pipe", "pipe"] });
    let output = "";
    for (const stream of [child.stdout, child.stderr]) stream.on("data", (data) => { output = (output + data.toString()).slice(-16000); });
    const timer = setTimeout(() => child.kill(), timeout);
    child.on("error", (error) => { clearTimeout(timer); reject(error); });
    child.on("exit", (code) => { clearTimeout(timer); code === 0 ? resolve(output) : reject(new Error("验证命令失败：" + exe + " " + args[0] + "\n" + output)); });
  });
}
export class ReleaseManager {
  constructor(sourceRoot, directory) { this.source = sourceRoot; this.directory = directory; }
  state() { return readJson(path.join(this.directory, "state.json"), { active: null, candidate: null, pending: null, history: [] }); }
  save(state) { return atomicJson(path.join(this.directory, "state.json"), state); }
  releaseRoot(id) { if (!/^[a-f0-9]{16}$/.test(id ?? "")) throw new Error("无效发布版本"); return path.join(this.directory, "versions", id); }
  async locked(work) {
    await mkdir(this.directory, { recursive: true });
    const lock = path.join(this.directory, "operation.lock");
    let handle;
    try { handle = await open(lock, "wx"); } catch (e) { if (e.code === "EEXIST") throw new Error("发布操作正在进行，请稍候；异常退出后需管理员检查 operation.lock"); throw e; }
    try { return await work(); } finally { await handle.close(); await unlink(lock); }
  }
  async snapshot() {
    const files = await collectSource(this.source), hashes = fileHashes(files), id = sha(JSON.stringify(hashes)).slice(0, 16);
    const root = this.releaseRoot(id), manifestFile = path.join(root, "manifest.json");
    if (!await readJson(manifestFile)) {
      for (const [name, contents] of Object.entries(files)) { const file = path.join(root, "app", name); await mkdir(path.dirname(file), { recursive: true }); await writeFile(file, contents, { flag: "wx" }); }
      await atomicJson(manifestFile, { id, hashes, created: Date.now(), product: JSON.parse(files["dsh/profile/product.json"]) });
    }
    return { id, root, hashes };
  }
  async verify(id) {
    const root = this.releaseRoot(id), manifest = await readJson(path.join(root, "manifest.json"));
    if (!manifest) throw new Error("发布快照不存在");
    for (const [name, hash] of Object.entries(manifest.hashes)) if (sha(await readFile(path.join(root, "app", name))) !== hash) throw new Error("发布快照被改动：" + name);
    return manifest;
  }
  async status() {
    const state = await this.state(), files = await collectSource(this.source);
    const active = state.active ? await readJson(path.join(this.releaseRoot(state.active), "manifest.json")) : null;
    const product = JSON.parse(files["dsh/profile/product.json"]);
    const roleTools = product.roleTools ?? {};
    const plugins = Object.keys(files).filter((name) => /^dsh\/plugins\/[^/]+\/package.json$/.test(name) && !name.includes("/developer/")).map((file) => { const pkg = JSON.parse(files[file]); return { name: pkg.name, version: pkg.version }; });
    const pkg = JSON.parse(files["dsh/package.json"]);
    for (const name of ["dsh-better-sidebar", "dsh-dream-skin"]) plugins.push({ name, version: pkg.dependencies[name] });
    // What the release can grant a role: the platform's published allowlist.
    const availableTools = [...MAIN_TOOLS].sort();
    return { ...state, product, roleTools, availableTools, plugins, changes: changes(active?.hashes, fileHashes(files)), sourceRoot: this.source };
  }
  async saveProduct(product) { return this.locked(async () => {
    validateProduct(product); const state = await this.state(); if (state.pending) throw new Error("发布切换期间不能修改产品配置");
    const available = new Set(MAIN_TOOLS);
    if (Object.values(product.roleTools ?? {}).flat().some((tool) => !available.has(tool))) throw new Error("角色配置包含尚未声明的产品工具");
    await atomicJson(path.join(this.source, "dsh/profile/product.json"), product); return product;
  }); }
  async prepare(by) {
    return this.locked(async () => {
      const state = await this.state();
      if (state.pending) throw new Error("已有版本等待发布");
      const snapshot = await this.snapshot(), { id, root } = snapshot;
      state.candidate = { id, status: "validating", by, at: Date.now(), previewed: false };
      await this.save(state);
      try {
        await this.build(id);
        state.candidate.status = "validated";
      } catch (error) { state.candidate.status = "failed"; const message = String(error.message); state.candidate.error = message.length > 5000 ? message.slice(0, 2400) + "\n...\n" + message.slice(-2400) : message; }
      await this.save(state);
      return state.candidate;
    });
  }
  async build(id) {
    const candidate = await this.verify(id), state = await this.state();
    if (state.active) {
      const active = await this.verify(state.active);
      for (const file of ["dsh/plugins/platform/store.mjs", "dsh/plugins/platform/runtime.mjs"])
        if (candidate.hashes[file] !== active.hashes[file]) throw new Error("涉及数据存储模块变更，需要人工迁移评审，不能直接同步：" + file);
    }
    const root = this.releaseRoot(id), app = path.join(root, "app"), cwd = path.join(app, "dsh");
    const pnpm = (args, directory) => process.platform === "win32" ? command(process.env.ComSpec || "cmd.exe", ["/d", "/c", "pnpm", ...args], directory) : command("pnpm", args, directory);
    await pnpm(["install", "--offline", "--frozen-lockfile", "--ignore-scripts"], cwd);
    const manifest = await this.verify(id);
    for (const file of Object.keys(manifest.hashes).filter((name) => /\.(js|mjs)$/.test(name))) await command(process.execPath, ["--check", path.join(app, file)], cwd, 30000);
    const tests = (await readdir(path.join(cwd, "tests"))).filter((file) => file.endsWith(".test.mjs")).map((file) => path.join("tests", file));
    await command(process.execPath, ["--test", ...tests], cwd);
    await command("docker", ["build", "--provenance=false", "-t", "geosentinel-gis:release-" + id, "docker"], cwd);
    const image = (await command("docker", ["image", "inspect", "geosentinel-gis:release-" + id, "--format", "{{.Id}}"], cwd)).trim();
    if (!/^sha256:[a-f0-9]{64}$/.test(image)) throw new Error("无法固定计算镜像摘要");
    await atomicJson(path.join(root, "validation.json"), { passed: true, at: Date.now(), image });
  }
  async activated(id, by = "bootstrap") {
    return this.locked(async () => {
    const state = await this.state();
    state.active = id; state.pending = null;
    state.history.unshift({ id, by, at: Date.now(), status: "active" });
    state.history = state.history.slice(0, 30);
    await this.save(state);
    });
  }
  async request(id, user, rollback = false) {
    return this.locked(async () => {
      const state = await this.state();
      if (state.pending) throw new Error("已有版本等待发布");
      if (rollback ? !state.history.some((h) => h.id === id && h.status === "active") : state.candidate?.id !== id || state.candidate?.status !== "validated" || !state.candidate?.previewed) throw new Error("请先验证并打开候选版本预览");
      const manifest = await this.verify(id);
      if (!rollback && sha(JSON.stringify(fileHashes(await collectSource(this.source, this.fork)))) !== sha(JSON.stringify(manifest.hashes))) throw new Error("开发源码在验证后发生变化，请重新生成版本");
      state.pending = { id, by: user, at: Date.now(), rollback };
      await this.save(state); return state.pending;
    });
  }
}
