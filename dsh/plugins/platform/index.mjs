import path from "node:path";
import { randomUUID } from "node:crypto";
import { PlatformStore, PlatformError } from "./store.mjs";
import { createPlatformHandler } from "./http.mjs";
import { publicEvent } from "./public-events.mjs";
import { ResearchAdmission } from "./admission.mjs";
import { monitorService } from "../../monitoring/host.mjs";
import { nativeEvent } from "./native-events.mjs";
import { registerSidebarAdapter } from "./sidebar-adapter.mjs";
import { QuestionTransport } from "./questions.mjs";
import * as askUserTool from "@deepseek-ai/dsh-tool-ask-user";

export const name = "geosentinel-platform";
export const inject = [
  "webServer",
  "sessionController",
  "agents",
  "tools",
  "geosentinelTeams",
  "userQuestions",
];
const TEAM_TOOLS = [
  "agent_teams_create",
  "agent_teams_add_member",
  "agent_teams_remove_member",
  "agent_teams_create_task",
  "agent_teams_reassign_task",
  "agent_teams_claim_task",
  "agent_teams_update_task",
  "agent_teams_send_message",
  "agent_teams_status",
  "agent_teams_resume",
  "agent_teams_delete",
  "agent_teams_edit_plan",
];
const DOMAIN_TOOLS = [
  "geo_list_files",
  "geo_read_evidence",
  "geo_write_report",
  "geo_inspect_raster",
  "geo_execute_python",
  "geo_download_gee",
];

export function apply(ctx, config = {}) {
  const monitor = monitorService(ctx);
  const store = new PlatformStore(
    process.env.GEO_DATA_DIR ?? path.join(process.env.DSH_HOME, "geosentinel"),
  );
  const prepared = new WeakSet();
  const admission = new ResearchAdmission(async (chatId) => {
    const agent = ctx.agents.get(chatId);
    if (!agent) return false;
    if (agent.status === "running") return true;
    const team = await ctx.geosentinelTeams.inspectTeam(agent);
    return Boolean(
      team &&
        team.phase !== "staged" &&
        !team.halted &&
        (team.tasks.some(
          (t) => !["completed", "failed", "cancelled"].includes(t.status),
        ) ||
          team.members.some((m) => m.status === "working")),
    );
  });
  ctx.on("geosentinel/member-created", (record) =>
    store.recordChild(record.captainId, record.memberId, record.role),
  );
  const allowed = new Set([...TEAM_TOOLS, ...DOMAIN_TOOLS, "ask_user_question"]);
  ctx.plugin(askUserTool);
  const identityForAgent = (agent) => {
    const cwd = agent?.session?.header?.cwd;
    if (!cwd) throw new PlatformError(403, "任务缺少工作区");
    const row = store.db
      .prepare(
        "SELECT c.id,c.project_id,p.user_id,u.username,u.admin,u.disabled FROM chats c JOIN projects p ON c.project_id=p.id JOIN users u ON u.id=p.user_id WHERE c.id=? AND c.deleted=0 AND p.deleted=0",
      )
      .get(path.basename(cwd));
    if (!row || row.disabled)
      throw new PlatformError(403, "任务无权访问工作区");
    const user = {
      id: row.user_id,
      username: row.username,
      admin: Boolean(row.admin),
    };
    if (path.resolve(cwd) !== store.chatRoot(user, row.id))
      throw new PlatformError(403, "任务工作区不匹配");
    return { user, chatId: row.id, projectId: row.project_id, root: cwd };
  };
  ctx.effect(() =>
    ctx.tools.guard((exec) => {
      if (!allowed.has(exec.name))
        return "This tool is not available in the managed GeoSentinel product";
      try {
        identityForAgent(exec.agent);
        if (exec.name === "ask_user_question" && !ctx.agents.roots().includes(exec.agent)) return "Only the research supervisor may ask the user";
      } catch {
        return "GeoSentinel task ownership is unavailable";
      }
    }),
  );
  async function agentFor(user, chatId) {
    store.chat(user, chatId);
    if (!ctx.agents.get(chatId))
      await ctx.sessionController.create({
        sessionId: chatId,
        cwd: store.chatRoot(user, chatId),
      });
    const result = await ctx.sessionController.resolveAgent(chatId);
    if ("error" in result) throw new PlatformError(409, "研究对话尚未就绪");
    const agent = result.agent;
    identityForAgent(agent);
    if (!prepared.has(agent)) {
      agent.ctx.tools.restrict({ allow: [...allowed] });
      prepared.add(agent);
    }
    return agent;
  }
  const bridge = {
    async questions(user, chatId, reopen = false) {
      const agent = await agentFor(user, chatId);
      const team = await ctx.geosentinelTeams.inspectTeam(agent);
      const key = team?.phase === "staged" && !team.halted ? `${team.id}:${team.approvalRevision}` : null;
      const previous = questions.reviews.get(chatId);
      if (previous && previous !== key) {
        if (questions.pending.get(chatId)?.review) questions.cancel(chatId);
        else questions.reviews.delete(chatId);
      }
      if (reopen && agent.status === "running") throw new PlatformError(409, "方案正在整理，请等待当前回复完成");
      if (key && agent.status !== "running" && (previous !== key || reopen) && !questions.pending.has(chatId)) {
        questions.reviews.set(chatId, key);
        const approve = "确认方案并开始";
        const items = [{ id: randomReviewId(), question: "请审阅研究方案", header: "研究方案", detail: [team.description, ...team.tasks.map((t, i) => `${i + 1}. ${t.subject}`)].filter(Boolean).join("\n\n"),
          options: [{ label: approve }, { label: "暂不执行" }], intent: { kind: "plan-review", approve } }];
        reviewMetadata.set(items, { teamId: team.id, revision: team.approvalRevision, approve });
        void ctx.userQuestions.ask({ agent, questions: items }).catch((error) => {
          if (!["ASK_CANCELLED", "ASK_ABORTED"].includes(error.code)) console.error("GeoSentinel question:", error.code ?? error.name);
        });
      }
      return { pending: questions.snapshot(user, chatId) };
    },
    async answerQuestion(user, chatId, data) {
      store.chat(user, chatId);
      await questions.answer(user, chatId, data.requestId, data.answer, data.cancel === true);
      return { accepted: true };
    },
    async nativeHistory(user, chatId) {
      const agent = await agentFor(user, chatId);
      const snapshot = await ctx.sessionController.inspect(chatId);
      return { events: snapshot.events.map(nativeEvent).filter(Boolean), running: agent.status === "running" };
    },
    async create(user, chat) {
      await ctx.sessionController.create({
        sessionId: chat.id,
        cwd: store.chatRoot(user, chat.id),
      });
      await agentFor(user, chat.id);
    },
    async prompt(user, chatId, text, requestId) {
      await agentFor(user, chatId);
      await admission.claim(user.id, chatId);
      try {
        return await ctx.sessionController.prompt(
          {
            sessionId: chatId,
            requestId,
            mode: "queue",
            content: [{ type: "text", text }],
            clientTimeZone: "Asia/Shanghai",
          },
          new AbortController().signal,
        );
      } catch (error) {
        admission.release(chatId);
        throw error;
      }
    },
    async history(user, chatId) {
      store.chat(user, chatId);
      const agent = await agentFor(user, chatId);
      const snapshot = await ctx.sessionController.inspect(chatId);
      const events = snapshot.events
        .map(publicEvent)
        .filter(Boolean)
        .map((e) => ({ ...e, agentRole: "NTL_Engineer" }));
      const team = await ctx.geosentinelTeams.inspectTeam(agent);
      for (const member of team?.members ?? [])
        if (member.id) store.recordChild(chatId, member.id, member.name);
      for (const member of store.listChildren(user, chatId)) {
        const child = await ctx.sessionController.inspect(member.id);
        events.push(
          ...child.events
            .map(publicEvent)
            .filter(Boolean)
            .map((e) => ({ ...e, agentRole: member.role })),
        );
      }
      const running =
        agent.status === "running" ||
        (team &&
          !team.halted &&
          team.phase !== "staged" &&
          team.tasks.some((t) => t.status !== "completed"));
      return {
        events: events.sort((a, b) => a.time - b.time),
        status:
          team?.phase === "staged"
            ? "waiting_approval"
            : team?.halted
              ? "stopped"
              : running
                ? "running"
                : events.some((e) => e.type === "turn/end")
                  ? "completed"
                  : "idle",
      };
    },
    async *follow(user, chatId, signal) {
      store.chat(user, chatId);
      const queue = [],
        children = new Set(),
        pumps = [];
      let wake;
      const enqueue = (event) => {
        queue.push(event);
        wake?.();
        wake = undefined;
      };
      const agent = await agentFor(user, chatId);
      async function discover() {
        const team = await ctx.geosentinelTeams.inspectTeam(agent);
        for (const member of team?.members ?? [])
          if (member.id) store.recordChild(chatId, member.id, member.name);
        for (const member of store.listChildren(user, chatId))
          if (!children.has(member.id)) {
            children.add(member.id);
            pumps.push(
              pump(
                {
                  kind: "subagent",
                  parentSessionId: chatId,
                  childSessionId: member.id,
                  mode: "continuable",
                },
                member.role,
              ),
            );
          }
      }
      async function pump(address, agentRole) {
        try {
          for await (const frame of ctx.sessionController.follow(
            { address, maxMessages: 80 },
            signal,
          )) {
            if (frame.type === "snapshot") {
              enqueue({ type: "snapshot", agentRole });
              if (agentRole === "NTL_Engineer") await discover();
            } else {
              const event = publicEvent(frame.event ?? frame);
              if (event) enqueue({ ...event, agentRole });
              if (event?.type === "agent-teams/member-added") await discover();
            }
          }
        } catch (error) {
          if (!signal.aborted)
            enqueue({ type: "stream/error", message: "进度连接暂时中断" });
        }
      }
      pumps.push(pump({ kind: "session", sessionId: chatId }, "NTL_Engineer"));
      const onAbort = () => wake?.();
      signal.addEventListener("abort", onAbort, { once: true });
      try {
        while (!signal.aborted) {
          if (!queue.length)
            await new Promise((resolve) => {
              wake = resolve;
            });
          while (queue.length) yield queue.shift();
        }
      } finally {
        signal.removeEventListener("abort", onAbort);
      }
    },
    async plan(user, chatId) {
      const agent = await agentFor(user, chatId);
      const team = await ctx.geosentinelTeams.inspectTeam(agent);
      return {
        team: team
          ? {
              id: team.id,
              revision: team.approvalRevision,
              name: team.name,
              description: team.description,
              phase: team.phase ?? "running",
              halted: team.halted ?? false,
              tasks: team.tasks.map((t) => ({
                id: t.id,
                subject: t.subject,
                description: t.description,
                status: t.status,
                assignee: t.assignee,
              })),
              members: team.members.map((m) => ({
                name: m.name,
                role: m.role,
                status: m.status,
              })),
            }
          : null,
      };
    },
    async approve(user, chatId, teamId, revision) {
      const agent = await agentFor(user, chatId);
      const current = await ctx.geosentinelTeams.inspectTeam(agent);
      if (!current || current.id !== teamId || current.phase !== "staged")
        throw new PlatformError(409, "方案已变化，请刷新后确认");
      if (current.approvalRevision !== revision)
        throw new PlatformError(409, "方案已变化，请刷新后确认");
      await admission.claim(user.id, chatId);
      const result = await ctx.geosentinelTeams.approveStagedTeam(
        agent,
        teamId,
        undefined,
        revision,
      );
      store.audit(user.id, "plan.approve", `${chatId}:${teamId}`);
      return result;
    },
    async cancel(user, chatId) {
      questions.cancel(chatId);
      const agent = await agentFor(user, chatId);
      await ctx.geosentinelTeams.cancelTeam(agent);
      ctx.sessionController.cancel({ sessionId: chatId });
      await ctx.get("geosentinelResearch")?.cancelChat(chatId);
      admission.release(chatId);
      store.audit(user.id, "task.cancel", chatId);
    },
    async cancelProject(user, projectId) {
      for (const chat of store.listChats(user, projectId))
        await bridge.cancel(user, chat.id);
    },
    async cancelUser(userId) {
      const row = store.db
        .prepare("SELECT id,username,admin FROM users WHERE id=?")
        .get(userId);
      if (!row) return;
      const owner = { ...row, admin: Boolean(row.admin) };
      for (const project of store.listProjects(owner))
        await bridge.cancelProject(owner, project.id);
    },
  };
  const reviewMetadata = new WeakMap();
  const randomReviewId = () => `plan-${randomUUID()}`;
  const questions = new QuestionTransport({ approve: (...args) => bridge.approve(...args), audit: (...args) => store.audit(...args) });
  ctx.on("user-questions/request", (request, next) => {
    if (!request.agent) return next();
    const identity = identityForAgent(request.agent);
    if (!ctx.agents.roots().includes(request.agent)) return next();
    return questions.wait(identity.user, identity.chatId, request.questions, request.signal, reviewMetadata.get(request.questions));
  });
  async function ensureResearchExecution(identity) {
    const agent = await agentFor(identity.user, identity.chatId);
    const team = await ctx.geosentinelTeams.inspectTeam(agent);
    if (team?.phase === "staged")
      throw new PlatformError(409, "研究方案尚未获得用户确认");
    if (team?.halted) throw new PlatformError(409, "研究任务已停止");
  }
  ctx.provide("geosentinelPlatform", {
    store,
    bridge,
    identityForAgent,
    ensureResearchExecution,
  });
  registerSidebarAdapter(ctx, {
    store, hosts: process.env.GEO_ALLOWED_HOSTS?.split(",").map((value) => value.trim()).filter(Boolean) ?? config.hosts ?? ["127.0.0.1:8510", "localhost:8510"],
  });
  ctx.effect(() =>
    ctx.webServer.register({
      kind: "prefix",
      path: "/geo/api",
      handler: createPlatformHandler({
        monitor,
        store,
        bridge,
        hosts: process.env.GEO_ALLOWED_HOSTS?.split(",")
          .map((x) => x.trim())
          .filter(Boolean) ??
          config.hosts ?? ["127.0.0.1:8510", "localhost:8510"],
        secureCookies:
          process.env.GEO_SECURE_COOKIES === undefined
            ? config.secureCookies !== false
            : process.env.GEO_SECURE_COOKIES !== "false",
        onError: (error) =>
          console.error(`GeoSentinel: ${error.name}: ${error.message}`),
      }),
    }),
  );
  ctx.on("dispose", () => { questions.close(); store.close(); });
}
