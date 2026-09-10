export function launcherArgs(input) {
  const args = [...input];
  if (["web", "start"].includes(args[0])) args.shift();
  else if (args[0] && !args[0].startsWith("-")) throw new Error("未知命令，请运行 geosentinel --help");
  if (args.includes("--help") || args.includes("-h")) return null;
  if (!args.some((arg) => arg === "--port" || arg.startsWith("--port="))) args.push("--port", "8511");
  return args;
}
