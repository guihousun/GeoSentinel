import assert from "node:assert/strict";
import { readFile, writeFile } from "node:fs/promises";
const base = "http://127.0.0.1:8510/geo/api",
  mode = process.argv[2] ?? "status";
const account = JSON.parse(await readFile(".runtime/qa-account.json", "utf8"));
const login = await fetch(base + "/auth/login", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify(account),
});
const cookie = login.headers.get("set-cookie").split(";")[0];
async function api(route, method = "GET", data) {
  const res = await fetch(base + route, {
    method,
    headers: { cookie, "content-type": "application/json" },
    body: data ? JSON.stringify(data) : undefined,
  });
  const value = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(value));
  return value;
}
if (mode === "start") {
  const { project } = await api("/projects", "POST", {
    title: "事件助手边界验收",
  });
  const { chat } = await api(`/projects/${project.id}/chats`, "POST", {
    title: "演示材料时间线",
  });
  const fixture = {
    source_kind: "synthetic_test_fixture",
    notice: "以下为软件验收虚构资料，不是实际新闻或真实事件。",
    events: [
      {
        date: "2026-01-01",
        event: "演示道路临时关闭",
        source: "test-fixture:A",
        confidence: "未核实",
      },
      {
        date: "2026-01-03",
        event: "演示道路局部恢复",
        source: "test-fixture:B",
        confidence: "未核实",
      },
    ],
  };
  await api(`/projects/${project.id}/files`, "POST", {
    name: "event-fixture.json",
    base64: Buffer.from(JSON.stringify(fixture)).toString("base64"),
  });
  await api(`/chats/${chat.id}/prompt`, "POST", {
    text: "请制定一个受控研究方案，交由事件助手读取我上传的 event-fixture.json，梳理其中两条记录的时间线、来源和不确定性。它是纯软件验收虚构资料，严禁当作真实新闻，也不要补充其他事件。请先等待我的方案确认，然后由事件助手完成材料核查，你最终汇总。",
  });
  await writeFile(
    ".runtime/event-session.json",
    JSON.stringify({ projectId: project.id, chatId: chat.id }),
  );
  console.log(JSON.stringify({ submitted: true, chatId: chat.id }));
} else {
  const session = JSON.parse(
    await readFile(".runtime/event-session.json", "utf8"),
  );
  const plan = await api(`/chats/${session.chatId}/plan`);
  if (mode === "approve") {
    assert.ok(plan.team.members.some((m) => m.name === "NTL_Event_Tracker"));
    console.log(
      JSON.stringify(
        await api(`/chats/${session.chatId}/approve`, "POST", {
          teamId: plan.team.id,
          revision: plan.team.revision,
        }),
      ),
    );
  } else {
    const history = await api(`/chats/${session.chatId}/history`);
    const messages = history.events
      .filter(
        (e) => e.type === "assistant/message" && e.data.message.content.length,
      )
      .slice(-4);
    console.log(
      JSON.stringify({ status: history.status, plan, messages }, null, 2),
    );
    if (
      history.status === "completed" &&
      history.events.some(
        (e) =>
          e.agentRole === "NTL_Event_Tracker" &&
          e.type === "tool/call" &&
          e.data.name === "geo_read_evidence",
      )
    ) {
      const text = JSON.stringify(messages);
      assert.match(text, /虚构|演示|验收/);
      assert.match(text, /2026-01-01/);
      assert.match(text, /2026-01-03/);
      await writeFile(
        ".runtime/event-acceptance.json",
        JSON.stringify(
          {
            passed: true,
            syntheticFixture: true,
            chatId: session.chatId,
            role: "NTL_Event_Tracker",
            at: new Date().toISOString(),
          },
          null,
          2,
        ),
      );
    }
  }
}
