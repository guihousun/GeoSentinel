import { readFile } from "node:fs/promises";
import path from "node:path";
export function assertReleaseGate({ id, evidence, controller, version, authorization, now = Date.now() }) {
  if (typeof authorization !== "string" || !authorization.trim() || authorization.length > 2000) throw new Error("需要本次发布的用户授权说明");
  if (!evidence || evidence.id !== id || evidence.passed !== true || evidence.check !== "preview-upload-v1") throw new Error("请先生成预览并通过上传读取验收");
  if (!Number.isFinite(evidence.at) || now - evidence.at > 3600000 || evidence.at > now) throw new Error("预览验收已过期，请重新生成预览");
  if (!controller || !Number.isFinite(controller.at) || controller.protocol !== 1 || controller.version !== version || now - controller.at > 15000 || controller.at > now) throw new Error("发布控制器不支持目标版本或未就绪，需要维护升级；正式服务未停止");
}
export async function releaseGate(manager, id, authorization) {
  const json = async (file) => { try { return JSON.parse(await readFile(file, "utf8")); } catch { return null; } };
  const root = manager.releaseRoot(id), state = await manager.state();
  const manifest = await manager.verify(id);
  const pkg = await json(path.join(root, "app/dsh/package.json"));
  const installed = await json(path.join(root, "app/dsh/node_modules/@deepseek-ai/dsh-web-app/package.json"));
  const validation = await json(path.join(root, "validation.json"));
  if (!validation?.passed || !installed?.version || installed?.version !== pkg?.dependencies?.["@deepseek-ai/dsh"]) throw new Error("候选安装版本或构建校验无效");
  assertReleaseGate({ id, authorization, version: installed.version, evidence: await json(path.join(root, "acceptance.json")), controller: await json(path.join(manager.directory, "controller.json")) });
  if (state.active) { await manager.verify(state.active); if (!(await json(path.join(manager.releaseRoot(state.active), "validation.json")))?.passed) throw new Error("回滚版本未通过校验"); }
  return { authorization: authorization.trim(), manifest, at: Date.now() };
}
