#!/usr/bin/env node
import { fileURLToPath } from "node:url";
import { launcherArgs } from "./options.mjs";

try {
  if (process.argv[2] === "admin") {
    const { startAdmin } = await import("../scripts/admin-start.mjs");
    await startAdmin(process.argv.slice(3));
  } else {
  const args = launcherArgs(process.argv.slice(2));
  if (args === null) {
    console.log(`GeoSentinel - 地缘环境智能计算平台

  geosentinel                      启动平台，默认端口 8511
  geosentinel web                  同上
  geosentinel admin                本机原生开发入口（8514，主机权限）
  geosentinel --port 8512 --no-open 指定端口，不自动打开浏览器

使用注册目录中的 dsh/.env；GEO_ENV_FILE 可显式指定其他配置。
不会修改全局 dsh 命令，也不会停止已有服务。
重启请在 dsh 目录运行 .\\scripts\\restart.ps1 -Port 8511。`);
  } else {
    const entry = new URL("../scripts/start.mjs", import.meta.url);
    process.argv = [process.execPath, fileURLToPath(entry), ...args];
    await import(entry.href);
  }
  }
} catch (error) {
  console.error(`GeoSentinel: ${error.message}`);
  process.exitCode = 1;
}
