window.__ModuleLoader__.load({
  id: "@geosentinel/dsh-workbench",
  factory(require) {
    const React = require("react");
    const h = React.createElement;
    const { createSnapshotStore } = require("@deepseek-ai/dsh-client-store");
    const { createScope, scopeOf, MutableSessionEventSource } = require("@deepseek-ai/dsh-api-session-controller/client");
    const icons = require("@deepseek-ai/dsh-client-ui-primitives");
    const betterSidebarPlugin = require("dsh-better-sidebar/client");
    const managedTheme = require("@geosentinel/dsh-theme");
    const fail = (message) => ({ ok: false, error: { code: "geosentinel/unavailable", message } });
    const ok = (value) => ({ ok: true, value });
    function teamTodos(team) {
      return (team?.tasks ?? []).map((task) => {
        let status = task.status, note = "";
        if (status === "failed") note = "失败";
        else if (status === "cancelled") note = "已取消";
        else if (!["pending", "in_progress", "completed"].includes(status)) note = `状态待核验：${status}`;
        else if (team.halted && status !== "completed") note = "已停止";
        if (note) status = "pending";
        return { content: `${task.id}. ${note ? `[${note}] ` : ""}${task.subject}`, status };
      });
    }
    function apply(ctx) {
      const state = createSnapshotStore({ user: null, checking: true, projects: [], activeProject: null, error: "", panel: null, team: null, files: [] });
      const set = (patch) => state.set({ ...state.getSnapshot(), ...patch });
      const list = createSnapshotStore({ ids: [], byId: {}, current: undefined, phase: "ready", subagentsByParent: {}, jobsBySession: {}, currentAddress: undefined });
      const workspaces = createSnapshotStore({ phase: "ready", items: [], archivedSessionIds: [] });
      const records = new Map();
      const remoteListeners = new Map();
      let stream, poll, refreshTimer, sidebarFiber, generation = 0;
      const selectionKey = () => `geosentinel:selection:${state.getSnapshot().user?.id}`;
      function remember(id) { try { if (id) localStorage.setItem(selectionKey(), id); else localStorage.removeItem(selectionKey()); } catch {} }
      async function api(route, method = "GET", body) {
        const response = await fetch("/geo/api" + route, { method, credentials: "same-origin",
          headers: body === undefined ? {} : { "Content-Type": "application/json" },
          body: body === undefined ? undefined : JSON.stringify(body) });
          const data = await response.json();
        if (!response.ok) {
          if (response.status === 401) { clear(); set({ user: null }); }
          throw new Error(data.error ?? data.message ?? `请求失败 (${response.status})`);
        }
        return data;
      }
      function run(action) { return (...args) => Promise.resolve().then(() => action(...args)).catch((error) => set({ error: error.message })); }
      function clear() {
        generation++; stream?.close(); clearInterval(poll); clearTimeout(refreshTimer);
        sidebarFiber?.dispose(); sidebarFiber = undefined;
        list.set({ ...list.getSnapshot(), ids: [], byId: {}, current: undefined });
        for (const record of records.values()) { record.question?.controller.abort(); record.fiber.dispose(); }
        records.clear(); workspaces.set({ phase: "ready", items: [], archivedSessionIds: [] });
        set({ projects: [], activeProject: null, team: null, scheduling: null, queueError: null, pendingQuestion: null, files: [], users: [], invite: null, panel: null });
      }
      async function refresh() {
        if (!state.getSnapshot().user) return;
        const epoch = generation;
        const { projects } = await api("/projects");
        const items = [], byId = {};
        for (const project of projects) {
          const { chats } = await api(`/projects/${project.id}/chats`);
          items.push({ workspaceId: project.id, title: project.title, sessionIds: chats.map((c) => c.id) });
          for (const chat of chats) byId[chat.id] = { id: chat.id, title: chat.title, displayTitle: chat.title, projectId: project.id,
            blank: records.get(chat.id)?.session.getSnapshot().blank ?? false, running: false, updatedAt: chat.created };
        }
        if (epoch !== generation) return;
        const previousCurrent = list.getSnapshot().current;
        let current = previousCurrent;
        if (!current) { try { current = localStorage.getItem(selectionKey()); } catch {} }
        workspaces.set({ phase: "ready", items, archivedSessionIds: [] });
        list.set({ ...list.getSnapshot(), ids: Object.keys(byId), byId, current: byId[current] ? current : undefined });
        set({ projects, activeProject: projects.some((p) => p.id === state.getSnapshot().activeProject) ? state.getSnapshot().activeProject : projects[0]?.id });
        if (!sidebarFiber) sidebarFiber = ctx.plugin(betterSidebarPlugin);
        if (byId[current] && (current !== previousCurrent || !records.has(current))) open(current);
      }
      function binding(id) {
        if (!list.getSnapshot().byId[id]) return undefined;
        if (records.has(id)) return records.get(id);
        const scope = createScope(ctx, id);
        const lifecycle = createSnapshotStore({ sessionId: id, queue: [], pendingSubmissions: [], running: false, subagent: null,
          removed: false, openState: "loading", openError: null, hasMore: false, loadingOlder: false, promptError: null,
          blank: true, lastAgentError: null, promptAttempted: false, awaitingFirstTurn: false });
        const update = (patch) => lifecycle.set({ ...lifecycle.getSnapshot(), ...patch });
        const projections = new Map();
        const eventSource = new MutableSessionEventSource();
        const session = { sessionId: id, getSnapshot: lifecycle.getSnapshot, subscribe: lifecycle.subscribe,
          projections: { faceOf(key) { if (!projections.has(key)) projections.set(key, createSnapshotStore(undefined)); return projections.get(key); } },
          beginSubmission({ text, images, onRetire }) {
            const requestId = crypto.randomUUID();
            update({ pendingSubmissions: [{ requestId, text, images, time: Date.now() }], promptAttempted: true });
            session.retire = onRetire;
            return { requestId, abandon: () => update({ pendingSubmissions: [] }) };
          },
          async prompt(content, mode) {
            try {
              if (content.some((part) => part.type !== "text")) throw new Error("请通过资料与产出上传文件。");
              if (mode === "steer") throw new Error("请先停止当前任务，再发送新问题。");
              await api(`/chats/${id}/prompt`, "POST", { text: content.map((p) => p.text).join("\n") });
              update({ running: true, promptError: null }); await load(id);
              return ok({ accepted: true });
            } catch (error) { update({ pendingSubmissions: [], promptError: { op: "send", error: { code: "geosentinel/send", message: error.message } } }); return fail(error.message); }
          },
          async cancel() { try { await api(`/chats/${id}/cancel`, "POST"); update({ pendingSubmissions: [] }); await load(id); return ok({ accepted: true }); } catch (e) { return fail(e.message); } },
          async rename(title) { await api(`/chats/${id}`, "PATCH", { title }); await refresh(); return ok({ title, seq: 0 }); },
          loadOlder: async () => {}, command: async () => fail("此产品不开放终端命令。"),
          updateQueue: async () => fail("请停止任务后重新提交。"), readAttachment: async () => fail("请在资料与产出查看文件。"),
        };
        const record = { ...scope, sessionId: id, session, eventSource, update, lastEvents: "" };
        records.set(id, record); return record;
      }
      async function load(id) {
        const record = binding(id); if (!record || record.loading) return;
        record.loading = true;
        try {
          const data = await api(`/chats/${id}/native-history`);
          if (records.get(id) !== record) return;
          const signature = JSON.stringify(data.events);
          if (record.lastEvents !== signature) {
            record.eventSource.replace(data.events.map((event) => ({ type: "event", event })), false);
            record.lastEvents = signature;
          }
          record.update({ openState: "open", openError: null, blank: data.events.length === 0, running: data.running });
          if (record.session.getSnapshot().pendingSubmissions.length && data.events.some((e) => e.type === "user/message" && e.time >= record.session.getSnapshot().pendingSubmissions[0].time - 1000)) {
            record.update({ pendingSubmissions: [] }); record.session.retire?.({ reason: "observed", attachments: [] });
          }
          if (list.getSnapshot().current === id) {
            set({ scheduling: data.scheduling, queueError: data.queueError });
            const { team } = await api(`/chats/${id}/plan`);
            if (records.get(id) === record) record.session.projections.faceOf("todos").set(teamTodos(team));
            if (list.getSnapshot().current === id) { set({ team }); if (!data.running) await files(); }
            const { pending } = await api(`/chats/${id}/questions`);
            if (records.get(id) === record) syncQuestion(record, pending);
          }
        } catch (error) {
          if (records.get(id) === record) record.update({ openState: "error", openError: { code: "geosentinel/history", message: error.message } });
        } finally { record.loading = false; }
      }
      function syncQuestion(record, pending) {
        if (list.getSnapshot().current === record.sessionId) set({ pendingQuestion: pending });
        if (record.question?.id === pending?.id) return;
        record.question?.controller.abort(); record.question = null;
        if (!pending) return;
        const listener = remoteListeners.get("user-questions/request");
        if (!listener) return;
        const controller = new AbortController(), request = { id: pending.id, controller };
        record.question = request;
        // Reuse the upstream PendingQuestion, draft store, composer and answer lifecycle.
        Promise.resolve(listener.call(record.ctx, { questions: pending.questions, signal: controller.signal }, () => Promise.reject(new Error("没有可用的提问界面"))))
          .then((answer) => api(`/chats/${record.sessionId}/questions`, "POST", { requestId: pending.id, answer }))
          .catch(async (error) => {
            if (controller.signal.aborted) return;
            if (error.code === "ASK_CANCELLED") await api(`/chats/${record.sessionId}/questions`, "POST", { requestId: pending.id, cancel: true });
            else throw error;
          }).catch((error) => { if (!controller.signal.aborted) set({ error: error.message }); })
          .finally(() => { if (record.question === request) { record.question = null; void load(record.sessionId); } });
      }
      function open(id) {
        if (!list.getSnapshot().byId[id]) throw new Error("对话不存在。");
        binding(id); list.set({ ...list.getSnapshot(), current: id });
        set({ activeProject: list.getSnapshot().byId[id].projectId, team: null, scheduling: null, queueError: null, pendingQuestion: null, files: [] });
        remember(id); ctx.get("layout")?.closeDetails();
        stream?.close(); clearInterval(poll); clearTimeout(refreshTimer);
        stream = new EventSource(`/geo/api/chats/${id}/events`);
        stream.onmessage = () => { clearTimeout(refreshTimer); refreshTimer = setTimeout(() => load(id), 120); };
        stream.onerror = () => load(id);
        poll = setInterval(() => load(id), 4000); void load(id);
      }
      async function selectProject(id) {
        const chat = list.getSnapshot().ids.find((key) => list.getSnapshot().byId[key].projectId === id);
        if (chat) open(chat);
        else { clearSelection(); set({ activeProject: id }); }
      }
      function clearSelection() {
        stream?.close(); clearInterval(poll); clearTimeout(refreshTimer); remember(null);
        list.set({ ...list.getSnapshot(), current: undefined }); set({ team: null, scheduling: null, queueError: null, files: [] });
      }
      async function create({ workspaceId } = {}) {
        const projectId = workspaceId ?? state.getSnapshot().activeProject;
        if (!projectId) throw new Error("请先新建研究项目。");
        const { chat } = await api(`/projects/${projectId}/chats`, "POST", { title: "新研究对话" });
        await refresh(); open(chat.id); return chat.id;
      }
      const sessions = { list, binding, open, create, refresh, clear: clearSelection,
        scope: (id) => binding(id)?.ctx, scopeOf, sessionOf: (context) => binding(scopeOf(context))?.session,
        searchResultLimit: 20, subagentAddress: () => undefined, setSubagentCatalogOpen() {}, refreshSubagents: async () => {},
        fork: async () => { const message = "当前入口暂不支持复制历史分支。请在左栏新建对话。"; set({ error: message }); throw new Error(message); }, search: async () => ok({ items: [], hasMore: false }) };
      ctx.provide("sessions", sessions);
      ctx.provide("connection", { state: createSnapshotStore({ state: "connected" }), generation: createSnapshotStore({ phase: "ready", revision: 1 }) });
      ctx.provide("uiWorkspace", { connectWorkspace: async (id) => { await selectProject(id); return list.getSnapshot().current ?? await create({ workspaceId: id }); } });
      ctx.provide("remote", { $host: { platform: "managed", isLoopback: false }, $on: (event, listener) => {
        if (event !== "user-questions/request") return () => {};
        remoteListeners.set(event, listener);
        return () => { if (remoteListeners.get(event) === listener) remoteListeners.delete(event); };
      },
        session: { openWorkspacePath: async () => fail("主机路径不可访问，请使用资料与产出。") } });
      ctx.provide("remote.session", ctx.remote.session);
      const preferences = new Map();
      ctx.provide("settingsScope", { bind({ namespace }) {
        if (!preferences.has(namespace)) {
          const defaults = namespace.includes("locale") ? { preference: "zh" } : namespace.includes("theme") ? { preference: "dark", fontSize: 16 } : {};
          const value = createSnapshotStore({ status: "ready", value: defaults, revision: 0, error: null });
          preferences.set(namespace, { ...value, set: async (field, next) => value.set({ ...value.getSnapshot(), value: { ...value.getSnapshot().value, [field]: next } }) });
        }
        return preferences.get(namespace);
      } });
      ctx.inject(["theme"], (inner) => {
        inner.effect(() => inner.theme.register(managedTheme));
        inner.theme.setTheme(managedTheme.id);
      });
      ctx.inject(["locale"], (inner) => {
        inner.effect(() => inner.locale.addLanguage({ id: "zh-Hans", label: "中文", fallback: "zh" }));
        inner.effect(() => inner.locale.register("conversation", "zh-Hans", {
          "hero.headline": "", "hero.preview": "",
          "hero.chooseWorkspace": "选择研究项目",
          "placeholder.workspace": "选择研究项目",
          "placeholder.hero": "输入研究问题",
          "placeholder.default": "输入研究问题",
          "todo.title": "研究任务",
          "todo.progress.pending": "未完成 {pending}",
        }));
        inner.locale.setLocale("zh-Hans");
      });
      ctx.slots.provideRoot({ hooks: { workspaces } });
      const useState = () => React.useSyncExternalStore(state.subscribe, state.getSnapshot);
      const useList = () => React.useSyncExternalStore(list.subscribe, list.getSnapshot);
      const button = (label, icon, action, props = {}) => {
        const { iconOnly, className, ...rest } = props;
        return h("button", { type: "button", title: label, "aria-label": label, onClick: run(action), ...rest, className: ["geo-native-action", className].filter(Boolean).join(" ") }, icon && h(icon), iconOnly ? null : label);
      };
      function Sidebar({ collapsed }) {
        const s = useState(), l = useList();
        return h("aside", { className: "geo-native-sidebar" },
          h("div", { className: "geo-native-toolbar" }, !collapsed && h("strong", null, "地缘环境智能计算平台"), button("收起或展开侧栏", icons.IconPanelLeftOutline16, () => ctx.get("layout").toggleSidebar(), { iconOnly: true })),
          s.user && !collapsed && h(React.Fragment, null,
            button("新建项目", icons.IconProjectAddOutline16, () => set({ panel: "project" })),
            h("nav", { "aria-label": "研究项目", className: "geo-native-projects" }, ...s.projects.map((p) => h("section", { key: p.id },
              h("div", { className: "geo-native-project-title" }, button(p.title, icons.IconFolderOpenOutline16, () => selectProject(p.id)), button("新建对话", icons.IconPlusOutline16, () => create({ workspaceId: p.id }), { iconOnly: true, disabled: p.archived }), button("管理项目", icons.IconSettingsOutline16, () => set({ panel: "edit", editing: { kind: "projects", id: p.id, title: p.title } }), { iconOnly: true })),
              ...l.ids.filter((id) => l.byId[id].projectId === p.id).map((id) => h("div", { key: id, className: "geo-native-chat-row" }, button(l.byId[id].displayTitle, icons.IconNewChatOutline16, () => open(id), { className: l.current === id ? "selected" : "" }), button("管理对话", icons.IconSettingsOutline16, () => set({ panel: "edit", editing: { kind: "chats", id, title: l.byId[id].displayTitle } }), { iconOnly: true })))))),
            h("div", { className: "geo-native-bottom" },
              button("公共监测", icons.IconGlobeOutline14, () => openResearchTab("monitor")),
              button("资料与产出", icons.IconFolderOpenOutline16, async () => { await files(); openResearchTab("files"); }),
              s.user.admin && button("管理中心", icons.IconSettingsOutline16, async () => { const data = await api("/admin/users"); set({ panel: "admin", users: data.users }); }),
              button(s.user.username, icons.IconUserOutline16, () => set({ panel: "account" })))),
          collapsed && button("公共监测", icons.IconGlobeOutline14, () => openResearchTab("monitor"), { iconOnly: true }));
      }
      function openResearchTab(kind) {
        const service = ctx.get("betterSidebar");
        if (!list.getSnapshot().current || !service) { set({ panel: kind }); return; }
        ctx.get("layout").closeDetails();
        service.openTab({ type: `geosentinel:${kind}`, url: location.origin + (kind === "monitor" ? "/geo/api/monitor/events" : "/geo/api/projects") });
      }
      async function files() {
        const projectId = state.getSnapshot().activeProject, id = list.getSnapshot().current;
        const inputs = projectId ? (await api(`/projects/${projectId}/files`)).files : [];
        const outputs = id ? (await api(`/chats/${id}/files`)).files : [];
        if (state.getSnapshot().activeProject !== projectId || list.getSnapshot().current !== id) return;
        set({ files: [...inputs.map((f) => ({ ...f, path: f.name, input: true })), ...outputs.map((f) => ({ ...f, path: f.name }))] });
      }
      function Plan() {
        const { team, pendingQuestion, scheduling, queueError } = useState(); const { current } = useList();
        if (current && (scheduling?.waiting.length || queueError)) return h("div", { className: "geo-native-queue" },
          h("div", { className: "geo-native-toolbar" }, h("span", { role: "status", "aria-live": "polite" }, queueError ?? `等待调度 · ${scheduling.waiting.length} 项`),
            button(scheduling?.running.length ? "停止并取消等待" : "取消等待", icons.IconCloseOutline16, async () => { await binding(current).session.cancel(); })),
          scheduling?.waiting.length > 0 && h("details", null, h("summary", null, "等待任务"), h("ol", null, ...scheduling.waiting.slice().reverse().map((job) => h("li", { key: job.id }, job.label)))));
        if (current && !scheduling?.running.length && ["failed", "interrupted"].includes(scheduling?.latest?.status)) return h("div", { className: "geo-native-queue", role: "status" }, scheduling.latest.error);
        if (!team || pendingQuestion || team.phase !== "staged" || team.halted) return null;
        return h("div", { className: "geo-native-plan-actions geo-native-review-reopen" },
            h(icons.Button, { variant: "primary", icon: h(icons.IconPlayOutline16), onClick: run(async () => {
              const { pending } = await api(`/chats/${current}/review`, "POST"); syncQuestion(binding(current), pending);
            }) }, "审阅方案"));
      }
      function Usage({ admin = false }) {
        const [data, change] = React.useState(null), [error, problem] = React.useState("");
        const refreshUsage = () => api(admin ? "/admin/usage" : "/account/usage").then(change).catch((e) => problem(e.message));
        React.useEffect(() => { let live = true; api(admin ? "/admin/usage" : "/account/usage").then((value) => live && change(value)).catch((e) => live && problem(e.message)); return () => { live = false; }; }, [admin]);
        const sum = (rows, field) => (rows ?? []).reduce((total, row) => total + Number(row[field] ?? 0), 0);
        const jobs = data?.jobs ?? [], docker = jobs.filter((job) => job.kind === "docker");
        return h("details", { className: "geo-native-usage" }, h("summary", null, admin ? "平台资源用量" : "我的资源用量"),
          error ? h("p", { role: "alert" }, error) : !data ? h("p", null, "正在加载…") : h(React.Fragment, null,
            h("p", null, `已接收问题 ${sum(data.usage, "prompts")} 次 · 等待 ${sum(jobs.filter((j) => j.status === "queued"), "count")} 项`),
            h("p", null, `累计上传 ${(sum(data.usage, "upload_bytes") / 1048576).toFixed(1)} MiB`),
            h("p", null, `计算作业 ${sum(docker, "count")} 次 · 累计运行 ${(sum(docker, "elapsed_ms") / 60000).toFixed(1)} 分钟`),
            data.storage && h("p", null, `当前工作区 ${(data.storage.bytes / 1048576).toFixed(1)} MiB`)),
          button("刷新用量", icons.IconRefreshOutline16, refreshUsage));
      }
      function Overlay() {
        const s = useState(), [joining, join] = React.useState(false), [busy, setBusy] = React.useState(false);
        const dialog = React.useRef(null);
        React.useLayoutEffect(() => { const element = dialog.current; if (element && !element.open) element.showModal(); }, [s.checking, s.user, s.panel, s.error]);
        React.useEffect(() => { const key = (event) => { if (event.key === "Escape" && s.user) set({ panel: null, error: "" }); }; window.addEventListener("keydown", key); return () => window.removeEventListener("keydown", key); }, [s.user]);
        const submit = (action) => async (event) => { event.preventDefault(); setBusy(true); set({ error: "" }); try { await action(new FormData(event.currentTarget)); } catch (error) { set({ error: error.message }); } finally { setBusy(false); } };
        if (s.checking) return h("div", { className: "geo-native-auth" }, "正在加载账号…");
        if (!s.user) return h("dialog", { ref: dialog, className: "geo-native-auth", "aria-label": joining ? "注册账号" : "登录", onCancel: (e) => e.preventDefault() }, h("form", { className: "geo-native-auth-form", onSubmit: submit(async (data) => {
          const credentials = { username: data.get("username"), password: data.get("password") };
          if (joining) await api("/auth/join", "POST", { ...credentials, invite: data.get("invite") });
          await api("/auth/login", "POST", credentials); location.reload();
        }) }, h("h1", null, "地缘环境智能计算平台"), h("h2", null, joining ? "注册账号" : "登录"),
          h("label", null, "账号", h("input", { name: "username", required: true, autoComplete: "username" })),
          h("label", null, "密码", h("input", { name: "password", type: "password", minLength: 8, required: true, autoComplete: joining ? "new-password" : "current-password" })),
          joining && h("label", null, "邀请码", h("input", { name: "invite", required: true })),
          s.error && h("p", { role: "alert" }, s.error), h("button", { type: "submit", disabled: busy }, busy ? "处理中…" : joining ? "注册并登录" : "登录"),
          button(joining ? "已有账号，返回登录" : "使用邀请码注册", null, () => { join(!joining); set({ error: "" }); })));
        if (!s.panel && !s.error) return null;
        return h("dialog", { ref: dialog, className: "geo-native-dialog", "aria-label": "平台面板", onCancel: () => set({ panel: null, error: "" }) },
            button("关闭", icons.IconCloseOutline16, () => set({ panel: null, error: "" }), { iconOnly: true, className: "geo-native-close" }),
            s.error && h("p", { role: "alert" }, s.error),
            s.panel === "project" && h("form", { onSubmit: submit(async (data) => { const { project } = await api("/projects", "POST", { title: data.get("title") }); await refresh(); set({ activeProject: project.id, panel: null }); await create({ workspaceId: project.id }); }) },
              h("h2", null, "新建研究项目"), h("label", null, "项目名称", h("input", { name: "title", maxLength: 160, required: true, autoFocus: true })), h("button", { type: "submit", disabled: busy }, "创建")),
            s.panel === "edit" && h("form", { key: s.editing.id, onSubmit: submit(async (data) => { await api(`/${s.editing.kind}/${s.editing.id}`, "PATCH", { title: data.get("title") }); await refresh(); set({ panel: null }); }) },
              h("h2", null, s.editing.kind === "projects" ? "管理项目" : "管理对话"),
              h("label", null, "名称", h("input", { name: "title", defaultValue: s.editing.title, required: true, maxLength: 160 })),
              h("button", { type: "submit", disabled: busy }, "保存名称"),
              button("删除", icons.IconTrashOutline16, () => set({ panel: "delete" }))),
            s.panel === "delete" && h(React.Fragment, null, h("h2", null, "确认删除"), h("p", null, `删除“${s.editing.title}”后，该入口将不再显示。`),
              button("取消", null, () => set({ panel: "edit" })), button("确认删除", icons.IconTrashOutline16, async () => { await api(`/${s.editing.kind}/${s.editing.id}`, "DELETE"); clearSelection(); await refresh(); set({ panel: null }); })),
            s.panel === "account" && h(React.Fragment, null, h("h2", null, s.user.username), h(Usage), h("form", { onSubmit: submit(async (data) => { await api("/auth/password", "POST", { currentPassword: data.get("current"), newPassword: data.get("next") }); clear(); location.reload(); }) },
              h("label", null, "当前密码", h("input", { name: "current", type: "password", autoComplete: "current-password", required: true })), h("label", null, "新密码", h("input", { name: "next", type: "password", autoComplete: "new-password", minLength: 8, required: true })), h("button", { type: "submit", disabled: busy }, "修改密码")), button("退出登录", icons.IconUserOutline16, async () => { await api("/auth/logout", "POST"); clear(); location.reload(); })),
            s.panel === "admin" && s.user.admin && h(React.Fragment, null, h("h2", null, "管理中心"), h(Usage, { admin: true }), button("生成邀请码", icons.IconPlusOutline16, async () => { const data = await api("/admin/invites", "POST"); set({ invite: data.invite }); }), s.invite && h("input", { value: s.invite, readOnly: true, "aria-label": "邀请码" }),
              ...(s.users ?? []).map((user) => h("div", { key: user.id, className: "geo-native-user" }, h("span", null, user.username), button(user.disabled ? "启用" : "停用", null, async () => { await api(`/admin/users/${user.id}`, "PATCH", { disabled: !user.disabled }); set({ users: (await api("/admin/users")).users }); }, { disabled: user.id === s.user.id })))),
            s.panel === "files" && h(React.Fragment, null, h("h2", null, "资料与产出"), h("input", { type: "file", "aria-label": "上传资料", disabled: !s.activeProject, onChange: run(async (event) => { const file = event.target.files[0]; if (!file) return; if (file.size > 16 * 1024 * 1024) throw new Error("文件不能超过 16 MiB"); const base64 = await new Promise((resolve, reject) => { const reader = new FileReader(); reader.onload = () => resolve(reader.result.split(",")[1]); reader.onerror = reject; reader.readAsDataURL(file); }); await api(`/projects/${s.activeProject}/files`, "POST", { name: file.name, base64 }); await files(); }) }),
              s.files.length === 0 ? h("p", null, "暂无资料与产出") : s.files.map((file) => h("div", { key: (file.input ? "in" : "out") + file.path }, file.input ? h("span", null, file.path) : h("a", { href: `/geo/api/chats/${list.getSnapshot().current}/files?path=${encodeURIComponent(file.path)}`, download: true }, file.path)))),
            s.panel === "monitor" && h(Monitor));
      }
      function Monitor() {
        const [data, change] = React.useState(null), [error, problem] = React.useState("");
        const mapElement = React.useRef(null), map = React.useRef(null), markers = React.useRef(null);
        React.useEffect(() => {
          if (!window.L || !mapElement.current) return;
          const instance = L.map(mapElement.current, { zoomControl: false, minZoom: 0, maxZoom: 8, worldCopyJump: false });
          instance.attributionControl.setPrefix(false); instance.attributionControl.addAttribution("Natural Earth · 概览边界");
          L.control.zoom({ position: "bottomright", zoomInTitle: "放大", zoomOutTitle: "缩小" }).addTo(instance);
          instance.fitBounds([[-58, -180], [80, 180]], { animate: false }); map.current = instance;
          markers.current = L.layerGroup().addTo(instance);
          const resize = new ResizeObserver(() => instance.invalidateSize()); resize.observe(mapElement.current);
          let live = true;
          fetch("/geo/vendor/world.json", { cache: "no-store" }).then((r) => r.json()).then((world) => {
            if (!live) return;
            for (const feature of world.features.filter((f) => f.type === 3)) L.polygon(feature.geometry.map((ring) => ring.map(([x, y]) => [Math.atan(Math.sinh(Math.PI * (1 - 2 * y / 4096))) * 180 / Math.PI, x / 4096 * 360 - 180])), { pane: "tilePane", color: "#9bafb8", weight: 0.65, fillColor: "#f9fbfa", fillOpacity: 1, interactive: false }).addTo(instance);
          }).catch(() => live && problem("底图加载失败，可继续查看事件列表。"));
          return () => { live = false; resize.disconnect(); instance.remove(); map.current = null; markers.current = null; };
        }, []);
        React.useEffect(() => {
          if (!markers.current) return; markers.current.clearLayers();
          for (const item of data?.items ?? []) if (Number.isFinite(item.latitude) && Number.isFinite(item.longitude)) {
            const text = document.createElement("span"); text.textContent = item.displayTitle ?? item.title;
            L.circleMarker([item.latitude, item.longitude], { radius: 6, color: item.severity === "high" ? "#b94b4b" : item.severity === "medium" ? "#9d6714" : "#287a7c", fillOpacity: 0.65, weight: 1.5 }).bindTooltip(text).addTo(markers.current);
          }
        }, [data]);
        React.useEffect(() => { let live = true; const update = () => api("/monitor/events").then((value) => live && change(value)).catch((e) => live && problem(e.message)); update(); const timer = setInterval(update, 60000); return () => { live = false; clearInterval(timer); }; }, []);
        return h("div", { className: "geo-native-monitor" }, h("div", { className: "geo-native-toolbar" }, h("h2", null, "公共监测"), button("全球范围", icons.IconGlobeOutline14, () => map.current?.fitBounds([[-58, -180], [80, 180]], { animate: false }), { iconOnly: true })), h("div", { className: "geo-native-map", ref: mapElement, "aria-label": "全球事件地图" }),
          error && h("p", { role: "alert" }, error), !data && !error && h("p", null, "正在加载监测数据…"),
          data && h("p", null, `${data.items.length} 条来源记录${data.stale ? " · 数据待更新" : ""}`),
          ...(data?.items ?? []).map((item) => h("article", { key: item.id, className: "geo-native-event" }, h("strong", null, item.displayTitle ?? item.title), h("small", null, item.source),
            h("a", { href: item.url, target: "_blank", rel: "noopener noreferrer" }, "查看来源"),
            button("带入研究对话", icons.IconNewChatOutline16, async () => { const id = list.getSnapshot().current; if (!id) throw new Error("请先选择研究对话。"); const result = await api(`/chats/${id}/monitor-context`, "POST", { eventId: item.id }); const input = ctx.get("conversation").input; const shell = input.for(binding(id).ctx); shell.setDraft(`请核验这条公共监测线索：${item.displayTitle ?? item.title}\n来源文件：${result.path}\n先区分已确认事实和待核验信息。`); set({ panel: null }); }))));
      }
      function FilesPanel() {
        const s = useState(), l = useList();
        React.useEffect(() => { void files().catch((error) => set({ error: error.message })); }, [l.current, s.activeProject]);
        return h("div", { className: "geo-native-files" }, h("h2", null, "资料与产出"), button("上传资料", icons.IconPaperclipOutline16, () => set({ panel: "files" })),
          s.files.length ? s.files.map((file) => h("div", { key: (file.input ? "in" : "out") + file.path }, file.input ? h("span", null, file.path) : h("a", { href: `/geo/api/chats/${l.current}/files?path=${encodeURIComponent(file.path)}`, download: true }, file.path))) : h("p", null, "暂无资料与产出"));
      }
      ctx.inject(["betterSidebar"], (inner) => {
        inner.effect(() => inner.betterSidebar.registerTab({ id: "geosentinel:monitor", title: "公共监测", single: true, order: 0, icon: h(icons.IconGlobeOutline14), component: () => h(Monitor) }));
        inner.effect(() => inner.betterSidebar.registerTab({ id: "geosentinel:files", title: "资料与产出", single: true, order: 1, icon: h(icons.IconFolderOpenOutline16), component: () => h(FilesPanel) }));
      });
      ctx.slots.inject("sidebar", () => ctx.slots.register({ name: "sidebar" }, Sidebar));
      function ProjectPicker({ open: visible, onPick, onClose }) {
        const s = useState();
        const picker = React.useRef(null);
        React.useLayoutEffect(() => { if (visible && picker.current && !picker.current.open) picker.current.showModal(); }, [visible]);
        if (!visible) return null;
        return h("dialog", { ref: picker, className: "geo-native-picker", onCancel: onClose, "aria-label": "选择研究项目" }, h("h2", null, "选择研究项目"),
          ...s.projects.map((project) => button(project.title, icons.IconFolderOpenOutline16, () => onPick(project.id), { key: project.id })),
          !s.projects.length && h("p", null, "暂无研究项目"), button("新建项目", icons.IconPlusOutline16, () => { onClose(); set({ panel: "project" }); }), button("关闭", icons.IconCloseOutline16, onClose));
      }
      ctx.slots.inject("conversation.hero.workspace", () => ctx.slots.register({ name: "conversation.hero.workspace" }, ProjectPicker));
      ctx.slots.inject("shell.overlay", () => ctx.slots.register({ name: "shell.overlay", id: "geo-account" }, Overlay));
      ctx.slots.inject("conversation.input.dock", () => ctx.slots.register({ name: "conversation.input.dock", id: "geo-plan", order: -10 }, Plan));
      ctx.slots.inject("conversation.hero.brand.mark", () => ctx.slots.register({ name: "conversation.hero.brand.mark" }, () => h("h1", { className: "geo-native-hero" }, h("span", null, "地缘环境"), h("span", null, "智能计算平台"))));
      ctx.slots.inject("conversation.session.header.utilities", () => ctx.slots.register({ name: "conversation.session.header.utilities", id: "geo-monitor" }, () => button("公共监测", icons.IconGlobeOutline14, () => openResearchTab("monitor"))));
      api("/auth/status").then(async ({ user }) => { set({ user, checking: false }); if (user) await refresh(); }).catch((error) => set({ checking: false, error: error.message }));
      ctx.on("dispose", () => { clear(); });
    }
    return { inject: ["slots"], apply };
  },
});
