import { readFileSync, writeFileSync } from "node:fs";
const mode = process.argv[2] ?? "status",
  base = "http://127.0.0.1:8510/geo/api";
const account = JSON.parse(readFileSync(".runtime/qa-account.json"));
const login = await fetch(base + "/auth/login", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify(account),
});
const cookie = login.headers.get("set-cookie")?.split(";")[0];
async function api(route, method = "GET", data) {
  const r = await fetch(base + route, {
    method,
    headers: { cookie, "content-type": "application/json" },
    body: data ? JSON.stringify(data) : undefined,
  });
  const value = await r.json();
  if (!r.ok)
    throw new Error(JSON.stringify({ route, status: r.status, ...value }));
  return value;
}
if (mode === "start") {
  const nighttime = process.argv.includes("--ntl");
  const { project } = await api("/projects", "POST", {
    title: "真实 GEE 多智能体验收",
  });
  const { chat } = await api(`/projects/${project.id}/chats`, "POST", {
    title: nighttime
      ? "仰光 VIIRS 夜间灯光获取与统计"
      : "仰光高程数据获取与统计",
  });
  await api(`/chats/${chat.id}/prompt`, "POST", {
    text: nighttime
      ? "请制定并执行一个夜间灯光研究任务：获取仰光附近 bbox [96.1,16.7,96.15,16.75] 的 NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG 数据，波段 avg_rad，500 米尺度。asset_type=ImageCollection，start_date=2022-01-01，end_date=2022-02-01，reducer=mean。由数据助手获取数据、分析助手检查有效像元的平均值和范围，并交付 GeoTIFF 与统计报告。先给出方案等待我确认；不要做额外校正，不把指定尺度称为数据原生分辨率，不根据均值推断经济变化。"
      : "请制定并执行一个研究任务：获取仰光附近 bbox [96.1,16.7,96.15,16.75] 的 USGS/SRTMGL1_003 数据，波段 elevation，500 米分辨率。由数据助手获取数据、分析助手统计有效像元的平均值和范围，并提供下载文件。先给出研究方案，等待我确认再执行；数据产品是静态 Image，不需要日期。",
  });
  writeFileSync(
    ".runtime/research-session.json",
    JSON.stringify({
      projectId: project.id,
      chatId: chat.id,
      dataset: nighttime ? "VIIRS" : "SRTM",
    }),
  );
  console.log(JSON.stringify({ submitted: true, chatId: chat.id }));
} else {
  const session = JSON.parse(readFileSync(".runtime/research-session.json"));
  const plan = await api(`/chats/${session.chatId}/plan`);
  if (mode === "approve")
    console.log(
      JSON.stringify(
        await api(`/chats/${session.chatId}/approve`, "POST", {
          teamId: plan.team?.id,
          revision: plan.team?.revision,
        }),
      ),
    );
  else {
    const history = await api(`/chats/${session.chatId}/history`);
    console.log(
      JSON.stringify(
        {
          plan,
          latest: history.events
            ?.filter((e) =>
              ["assistant/message", "tool/call", "turn/end"].includes(e.type),
            )
            .slice(-8),
        },
        null,
        2,
      ),
    );
  }
}
