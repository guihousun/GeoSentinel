import { createServer } from "node:net";

export function startupOptions(args) {
  const forwarded = [...args];
  function option(name, fallback) {
    let value = fallback;
    for (let i = 0; i < args.length; i++) {
      if (args[i] === name) value = args[++i];
      else if (args[i].startsWith(name + "=")) value = args[i].slice(name.length + 1);
    }
    return value;
  }
  const rawPort = option("--port", "8510");
  if (!/^\d+$/.test(rawPort ?? "") || Number(rawPort) < 1 || Number(rawPort) > 65535)
    throw new Error("--port 必须是 1–65535 的整数；平台使用固定端口，不支持随机端口 0。");
  const host = option("--host", "127.0.0.1");
  if (!host || host.startsWith("-")) throw new Error("--host 缺少有效地址。");
  if (!args.some((arg) => arg === "--port" || arg.startsWith("--port=")))
    forwarded.push("--port", "8510");
  return { host, port: Number(rawPort), args: forwarded };
}

export async function inspectEndpoint({ host, port }, timeout = 1500) {
  const address = host.includes(":") ? `[${host}]` : host;
  const url = `http://${address}:${port}/`;
  // A bind probe catches non-HTTP listeners as well as healthy web services.
  const free = await new Promise((resolve, reject) => {
    const server = createServer();
    server.once("error", (error) => {
      if (error.code === "EADDRINUSE") resolve(false);
      else reject(error);
    });
    server.listen({ host, port, exclusive: true }, () => server.close(() => resolve(true)));
  });
  if (free) return { state: "free", url };
  try {
    const response = await fetch(url + "geo/api/health", {
      signal: AbortSignal.timeout(timeout), redirect: "error",
    });
    const body = await response.json();
    if (response.ok && body.status === "ok" && body.service === "geosentinel-dsh")
      return { state: "running", url };
  } catch { /* Busy, unhealthy, or non-HTTP: never stop its process automatically. */ }
  return { state: "occupied", url };
}
