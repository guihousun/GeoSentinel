window.__ModuleLoader__.load({
  id: "@geosentinel/dsh-workbench",
  factory(require) {
    if (globalThis.__GEOSENTINEL_DEVELOPMENT__) return { apply() {} };
    // This overlay belongs to the PRODUCT SHELL, which injects the theme module into the
    // same module table (`window.__ModuleLoader__.load({id:"@geosentinel/dsh-theme"…})`)
    // before the entry runs. A native DSH host — the administrator development instance
    // mounts `geosentinel-workbench` as its own client entry — has no such module, and a
    // failed require here takes down the WHOLE loader entry: that GUI then reports
    // "Failed to load plugins … require(\"@geosentinel/dsh-theme\") missed the module
    // table". Its absence therefore means "not our shell", and the overlay stays inert
    // instead of breaking the host it does not belong to.
    let managedTheme;
    try { managedTheme = require("@geosentinel/dsh-theme"); } catch { return { apply() {} }; }
    const React = require("react");
    const h = React.createElement;
    const { createSnapshotStore } = require("@deepseek-ai/dsh-client-store");
    const { createScope, scopeOf, MutableSessionEventSource } = require("@deepseek-ai/dsh-api-session-controller/client");
    const icons = require("@deepseek-ai/dsh-client-ui-primitives");
    // The product's own sidebar panel used to require this third-party plugin. From
    // the 0.1.5 line the native sidebar family owns the single `sidebar` slot (and
    // the native chat requires it), so the overlay keeps working either way.
    let betterSidebarPlugin = null;
    try { betterSidebarPlugin = require("dsh-better-sidebar/client"); } catch {}
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
      const state = createSnapshotStore({ user: null, checking: true, projects: [], activeProject: null, error: "", panel: null, team: null, files: [], view: "research", developmentItems: [], developmentUrl: null });
      const set = (patch) => state.set({ ...state.getSnapshot(), ...patch });
      const list = createSnapshotStore({ ids: [], byId: {}, current: undefined, phase: "ready", subagentsByParent: {}, jobsBySession: {}, currentAddress: undefined });
      const workspaces = createSnapshotStore({ phase: "ready", items: [], archivedSessionIds: [] });
      const records = new Map();
      const addresses = new Map(), catalogRequests = new Map();
      const rootFor = (id) => addresses.get(id)?.parentSessionId ?? id;
      const readOnlyMessage = "子智能体运行记录只读，请回到主对话指挥或批准。";
      const remoteListeners = new Map();
      let stream, poll, refreshTimer, sidebarFiber, generation = 0, titlePolledAt = 0;
      let serverRelease;
      let developmentFrame, developmentReady = false, pendingDevelopmentCommand;
      const selectionKey = () => `geosentinel:selection:${state.getSnapshot().user?.id}`;
      function remember(id) { try { if (id) localStorage.setItem(selectionKey(), id); else localStorage.removeItem(selectionKey()); } catch {} }
      async function api(route, method = "GET", body) {
        const response = await fetch("/geo/api" + route, { method, credentials: "same-origin",
          headers: body === undefined ? {} : { "Content-Type": "application/json" },
          body: body === undefined ? undefined : JSON.stringify(body) });
          const data = await response.json();
        const version = response.headers?.get?.("x-geosentinel-release");
        if (version && serverRelease && version !== serverRelease) set({ releaseUpdate: true });
        if (version && !serverRelease) serverRelease = version;
        if (!response.ok) {
          if (response.status === 401 && !route.startsWith("/admin/development")) { clear(); set({ user: null }); }
          const error = new Error(data.error ?? data.message ?? `请求失败 (${response.status})`); error.status = response.status; throw error;
        }
        return data;
      }
      function run(action) { return (...args) => Promise.resolve().then(() => action(...args)).catch((error) => set({ error: error.message })); }
      function clear() {
        generation++; stream?.close(); clearInterval(poll); clearTimeout(refreshTimer);
        sidebarFiber?.dispose(); sidebarFiber = undefined;
        list.set({ ...list.getSnapshot(), ids: [], byId: {}, current: undefined, currentAddress: undefined, subagentsByParent: {} });
        for (const record of records.values()) { record.question?.controller.abort(); record.fiber.dispose(); }
        records.clear(); addresses.clear(); catalogRequests.clear(); workspaces.set({ phase: "ready", items: [], archivedSessionIds: [] });
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
            // The native right sidebar's file tab reads the session's `cwd` to know which
            // workspace to list; without it the tab bails with "这个会话没有工作区目录".
            // The host publishes the chat's UI-only virtual root (`/工作区/<title>`), so
            // the value stays one implementation of that address, not a browser mirror.
            ...(chat.root ? { cwd: chat.root } : {}),
            blank: records.get(chat.id)?.session.getSnapshot().blank ?? false, running: false, updatedAt: chat.created };
        }
        if (epoch !== generation) return;
        const previousCurrent = list.getSnapshot().current;
        let current = previousCurrent;
        if (!current) { try { current = localStorage.getItem(selectionKey()); } catch {} }
        workspaces.set({ phase: "ready", items, archivedSessionIds: [] });
        const ids = Object.keys(byId);
        for (const [id, address] of addresses) {
          if (byId[address.parentSessionId]) byId[id] = { ...list.getSnapshot().byId[id], projectId: byId[address.parentSessionId].projectId };
          else { addresses.delete(id); records.get(id)?.fiber.dispose(); records.delete(id); }
        }
        list.set({ ...list.getSnapshot(), ids, byId, current: byId[current] ? current : undefined, currentAddress: byId[current] ? addresses.get(current) : undefined,
          subagentsByParent: Object.fromEntries(Object.entries(list.getSnapshot().subagentsByParent).filter(([id]) => ids.includes(id))) });
        set({ projects, activeProject: projects.some((p) => p.id === state.getSnapshot().activeProject) ? state.getSnapshot().activeProject : projects[0]?.id });
        if (betterSidebarPlugin && !sidebarFiber) sidebarFiber = ctx.plugin(betterSidebarPlugin);
        if (byId[current] && (current !== previousCurrent || !records.has(current))) open(current);
      }
      function binding(id) {
        if (!list.getSnapshot().byId[id]) return undefined;
        if (records.has(id)) return records.get(id);
        const scope = createScope(ctx, id);
        const lifecycle = createSnapshotStore({ sessionId: id, queue: [], pendingSubmissions: [], running: false, subagent: addresses.has(id) ? { address: addresses.get(id) } : null,
          removed: false, openState: "loading", openError: null, hasMore: false, loadingOlder: false, promptError: null,
          blank: true, lastAgentError: null, promptAttempted: false, awaitingFirstTurn: false });
        const update = (patch) => lifecycle.set({ ...lifecycle.getSnapshot(), ...patch });
        const projections = new Map();
        const eventSource = new MutableSessionEventSource();
        const session = { sessionId: id, getSnapshot: lifecycle.getSnapshot, subscribe: lifecycle.subscribe,
          projections: { faceOf(key) { if (!projections.has(key)) projections.set(key, createSnapshotStore(undefined)); return projections.get(key); } },
          // 0.1.5 calls this as beginSubmission({ mode, text, attachments, onRetire })
          // and its chat bubble renders `submission.attachments.map(...)`; the older
          // line used `images`. Emit both, plus the placement the native controller
          // derives, so neither line's renderer reads an undefined field.
          beginSubmission({ text, attachments, images, mode, onRetire }) {
            if (addresses.has(id)) throw new Error(readOnlyMessage);
            const requestId = crypto.randomUUID();
            const files = attachments ?? images ?? [];
            const running = session.getSnapshot().running;
            update({ pendingSubmissions: [{ requestId, text, attachments: files, images: files,
              placement: running ? mode === "steer" ? "steering" : "queued" : "transcript", time: Date.now() }], promptAttempted: true });
            session.retire = onRetire;
            return { requestId, abandon: () => update({ pendingSubmissions: [] }) };
          },
          async prompt(content, mode) {
            try {
              if (addresses.has(id)) throw new Error(readOnlyMessage);
              if (content.some((part) => part.type !== "text")) throw new Error("请用输入框的上传按钮添加文件。");
              if (mode === "steer") throw new Error("请先停止当前任务，再发送新问题。");
              await api(`/chats/${id}/prompt`, "POST", { text: content.map((p) => p.text).join("\n") });
              update({ running: true, promptError: null }); await load(id);
              return ok({ accepted: true });
            } catch (error) { update({ pendingSubmissions: [], promptError: { op: "send", error: { code: "geosentinel/send", message: error.message } } }); return fail(error.message); }
          },
          async cancel() { try { if (addresses.has(id)) throw new Error(readOnlyMessage); await api(`/chats/${id}/cancel`, "POST"); update({ pendingSubmissions: [] }); await load(id); return ok({ accepted: true }); } catch (e) { return fail(e.message); } },
          async rename(title) { if (addresses.has(id)) return fail(readOnlyMessage); await api(`/chats/${id}`, "PATCH", { title }); await refresh(); return ok({ title, seq: 0 }); },
          loadOlder: async () => {}, command: async () => fail("此产品不开放终端命令。"),
          updateQueue: async () => fail("请停止任务后重新提交。"), readAttachment: async () => fail("本平台不在对话里内联显示附件，请在会话工作区查看文件。"),
        };
        const record = { ...scope, sessionId: id, session, eventSource, update, lastEvents: "" };
        records.set(id, record); return record;
      }
      async function load(id) {
        const record = binding(id); if (!record || record.loading) return;
        record.loading = true;
        try {
          const address = addresses.get(id);
          const data = await api(address ? `/chats/${address.parentSessionId}/subagents/${id}/history` : `/chats/${id}/native-history`);
          if (records.get(id) !== record) return;
          const signature = JSON.stringify(data.events);
          if (record.lastEvents !== signature) {
            record.eventSource.replace(data.events.map((event) => ({ type: "event", event })), false);
            record.lastEvents = signature;
          }
          record.update({ openState: "open", openError: null, blank: data.events.length === 0, running: data.running,
            subagent: address ? { address, parentAvailable: data.parentAvailable } : null });
          // The sidebar title comes from the product chat table, which mirrors the
          // native session title once the harness derives it from the first
          // message; re-read the list while a chat still shows its placeholder.
          if (!address && (list.getSnapshot().byId[id]?.title ?? "").startsWith("新研究对话") && Date.now() - titlePolledAt > 8000) {
            titlePolledAt = Date.now();
            void refresh().catch(() => {});
          }
          if (record.session.getSnapshot().pendingSubmissions.length && data.events.some((e) => e.type === "user/message" && e.time >= record.session.getSnapshot().pendingSubmissions[0].time - 1000)) {
            record.update({ pendingSubmissions: [] }); record.session.retire?.({ reason: "observed", attachments: [] });
          }
          if (list.getSnapshot().current === id) {
            void refreshSubagents(rootFor(id));
            if (address) return;
            set({ scheduling: data.scheduling, queueError: data.queueError });
            const { team } = await api(`/chats/${id}/plan`);
            if (records.get(id) === record) record.session.projections.faceOf("todos").set(teamTodos(team));
            if (list.getSnapshot().current === id) set({ team });
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
      // 0.1.5 replaced the layout service face: `closeDetails` is gone and the right
      // pane is `closeRightbar` (the 0.1.5 service exposes selectPanel / closeRightbar
      // / openRightbar / toggleSidebar / beginNavigation). Every optional layout call
      // goes through here, so a renamed or absent native method can never break
      // opening a session again: the acceptance run failed exactly this way —
      // "ctx.get(...)?.closeDetails is not a function" left the project dialog stuck
      // and the new chat unopened.
      function layoutCall(name, ...args) {
        const layout = ctx.get("layout");
        if (!layout) return;
        const method = typeof layout[name] === "function" ? name
          : name === "closeDetails" && typeof layout.closeRightbar === "function" ? "closeRightbar" : undefined;
        if (method === undefined) return;
        try { layout[method](...args); } catch (error) { console.warn(`layout.${method} failed:`, error); }
      }
      function open(id) {
        if (!list.getSnapshot().byId[id]) throw new Error("对话不存在。");
        binding(id); list.set({ ...list.getSnapshot(), current: id, currentAddress: addresses.get(id) });
        set({ view: "research", activeProject: list.getSnapshot().byId[id].projectId, team: null, scheduling: null, queueError: null, pendingQuestion: null, files: [] });
        remember(rootFor(id)); layoutCall("closeDetails");
        stream?.close(); clearInterval(poll); clearTimeout(refreshTimer);
        stream = new EventSource(`/geo/api/chats/${rootFor(id)}/events`);
        stream.onmessage = () => { clearTimeout(refreshTimer); refreshTimer = setTimeout(() => load(id), 120); };
        stream.onerror = () => load(id);
        poll = setInterval(() => load(id), 4000); void load(id);
      }
      async function refreshSubagents(parent) {
        if (!list.getSnapshot().ids.includes(parent)) return;
        if (catalogRequests.has(parent)) return catalogRequests.get(parent);
        const epoch = generation;
        const updateCatalog = (value) => list.set({ ...list.getSnapshot(), subagentsByParent: { ...list.getSnapshot().subagentsByParent, [parent]: value } });
        if (!list.getSnapshot().subagentsByParent[parent]) updateCatalog({ state: "loading", entries: [], error: null });
        const request = (async () => {
          try {
            const catalog = await api(`/chats/${parent}/subagents`);
            if (epoch !== generation || !list.getSnapshot().ids.includes(parent)) return;
            const byId = { ...list.getSnapshot().byId }, valid = new Set();
            for (const entry of catalog.entries) if (entry.kind === "child") {
              valid.add(entry.id);
              addresses.set(entry.id, { parentSessionId: parent, childSessionId: entry.id, mode: entry.mode });
              byId[entry.id] = { id: entry.id, displayTitle: entry.label, origin: "subagent", parentId: parent,
                projectId: byId[parent].projectId, running: entry.activity === "running", blank: false };
            }
            for (const [id, address] of addresses) if (address.parentSessionId === parent && !valid.has(id)) {
              if (list.getSnapshot().current === id) open(parent);
              addresses.delete(id); delete byId[id]; records.get(id)?.fiber.dispose(); records.delete(id);
            }
            list.set({ ...list.getSnapshot(), byId });
            updateCatalog({ ...catalog, state: "ready", error: null });
          } catch (error) {
            if (epoch === generation) updateCatalog({ state: "error", entries: [], error: { code: "geosentinel/subagents", message: error.message } });
          }
        })();
        catalogRequests.set(parent, request);
        try { await request; } finally { if (catalogRequests.get(parent) === request) catalogRequests.delete(parent); }
      }
      async function openSubagent(address) {
        await refreshSubagents(address.parentSessionId);
        const verified = addresses.get(address.childSessionId);
        if (!verified || verified.parentSessionId !== address.parentSessionId || verified.mode !== address.mode ||
          list.getSnapshot().subagentsByParent[address.parentSessionId]?.state !== "ready") throw new Error("子智能体记录不可访问。");
        open(address.childSessionId);
      }
      async function selectProject(id) {
        set({ view: "research" });
        const chat = list.getSnapshot().ids.find((key) => list.getSnapshot().byId[key].projectId === id);
        if (chat) open(chat);
        else { clearSelection(); set({ activeProject: id }); }
      }
      function clearSelection() {
        stream?.close(); clearInterval(poll); clearTimeout(refreshTimer); remember(null);
        list.set({ ...list.getSnapshot(), current: undefined, currentAddress: undefined }); set({ team: null, scheduling: null, queueError: null, files: [] });
      }
      async function create({ workspaceId } = {}) {
        const projectId = workspaceId ?? state.getSnapshot().activeProject;
        if (!projectId) throw new Error("请先新建研究项目。");
        const { chat } = await api(`/projects/${projectId}/chats`, "POST", { title: "新研究对话" });
        await refresh(); open(chat.id); return chat.id;
      }
      const sessions = { list, binding, open, create, refresh, clear: clearSelection,
        scope: (id) => binding(id)?.ctx, scopeOf, sessionOf: (context) => binding(scopeOf(context))?.session,
        searchResultLimit: 20, subagentAddress: (id) => addresses.get(id), openSubagent, refreshSubagents,
        setSubagentCatalogOpen(parent, visible) { if (visible) void refreshSubagents(parent); },
        fork: async () => { const message = "当前入口暂不支持复制历史分支。请在左栏新建对话。"; set({ error: message }); throw new Error(message); }, search: async () => ok({ items: [], hasMore: false }) };
      ctx.provide("sessions", sessions);
      ctx.provide("connection", { state: createSnapshotStore({ state: "connected" }), generation: createSnapshotStore({ phase: "ready", revision: 1 }) });
      // The native provider of this service (`@deepseek-ai/dsh-client-ui-workspace`)
      // cannot activate here: it waits for the `workspaces` controller and
      // `remote.directoryPicker`, and directory picking belongs to the host plane the
      // product keeps closed (see native-host.mjs). Two booted plugins inject it —
      // `ui-conversation` calls `connectWorkspace`, `ui-sidebar` calls `startSession` —
      // so the product implements the SAME face against its own model, where one
      // native "workspace" is one GeoSentinel project and one session is one chat.
      // Directory picking, archiving and branch forking have no product equivalent on
      // this plane, so they fail loudly with a Chinese message instead of pretending.
      const needProject = () => { const id = state.getSnapshot().activeProject; if (!id) throw new Error("请先新建研究项目。"); return id; };
      const workspaceNavigation = {
        connectWorkspace: async (id) => { await selectProject(id); return list.getSnapshot().current ?? await create({ workspaceId: id }); },
        openSession: (id) => { open(id); },
        openWorkspace: async (id, beforeOpen) => {
          const sessionId = await workspaceNavigation.connectWorkspace(id);
          beforeOpen?.(sessionId); open(sessionId);
        },
        // `ui-sidebar` calls this from an onClick handler, so it must never throw
        // synchronously: a missing project is reported through the product error slot.
        startSession: async (id) => { try { await create({ workspaceId: id ?? needProject() }); } catch (error) { set({ error: error.message }); } },
        forkSession: async () => { await sessions.fork(); },
        archiveSession: async () => { throw new Error("平台按项目归档研究对话，暂不支持单独归档会话。"); },
        pickDirectory: async () => { throw new Error("主机目录选择不可用，请使用平台的项目入口。"); },
        listDirectory: async () => { throw new Error("主机目录浏览不可用，请使用平台的项目入口。"); },
        createDirectory: async () => { throw new Error("主机目录创建不可用，请使用平台的项目入口。"); },
      };
      ctx.provide("uiWorkspace", workspaceNavigation);
      // The native right sidebar (`ui-sidebar-right` + `-files` + `-documentpreview`)
      // reads the session workspace through the Host Remote namespace
      // `remote.workspaceFiles`. That namespace normally arrives over the browser API
      // plane, which the product keeps closed, so the product answers it from its OWN
      // explorer surface: `/sidebar/api/fs.tree` and `/sidebar/file`, both
      // ownership-checked and both already serving the product's virtual view
      // (上传的文件 / 分析结果 / 过程记录, display names resolved back to real files on
      // the host). Two consequences worth stating instead of hiding:
      //  - the tree addresses a virtual path, so it can never name a host file directly;
      //  - the product's explorer publishes no file mtime, so `version` is derived from
      //    the byte size: a same-size edit is not distinguishable by version (reads are
      //    always served with `cache-control: no-store`, so content is never stale).
      const PAGE_BYTES = 2 * 1024 * 1024, FULL_FILE_BYTES = 32 * 1024 * 1024;
      async function explorer(route, body) {
        const response = await fetch("/sidebar/api/" + route, { method: "POST", credentials: "same-origin",
          headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
        const data = await response.json().catch(() => ({}));
        if (!response.ok || data.ok === false) throw new Error(data.error?.message ?? `文件接口失败 (${response.status})`);
        return data.value;
      }
      async function explorerBytes(sessionId, target) {
        const query = new URLSearchParams({ sessionId, path: target });
        const response = await fetch(`/sidebar/file?${query}`, { credentials: "same-origin" });
        if (!response.ok) throw new Error(response.status === 404 ? "文件不存在" : `文件读取失败 (${response.status})`);
        return await response.arrayBuffer();
      }
      const fileRoots = new Map();
      const workspaceRootOf = async (sessionId) => {
        if (!fileRoots.has(sessionId)) fileRoots.set(sessionId, (await explorer("session.cwd", { sessionId })).root);
        return fileRoots.get(sessionId);
      };
      // The native tree descends by joining `parent + "/" + name`, so after the first
      // level it addresses the view with RELATIVE paths while the host expects the
      // absolute virtual path. Accept both, and refuse anything that could leave the
      // session's own view. `.` segments are dropped the way the host would resolve
      // them (`./报告.md` is an ordinary sibling reference); `..` is refused outright.
      function normalizeSegments(value) {
        const segments = [];
        for (const segment of String(value ?? "").split("/")) {
          if (segment === "" || segment === ".") continue;
          if (segment === "..") throw new Error("文件路径无效");
          segments.push(segment);
        }
        return segments;
      }
      function assertSafe(value) {
        if (value.includes("\0") || value.includes("\\")) throw new Error("文件路径无效");
        return value;
      }
      async function virtualAddress(sessionId, target) {
        const value = assertSafe(typeof target === "string" ? target : "");
        const root = await workspaceRootOf(sessionId);
        if (value === "" || value === "/") return root;
        // An absolute address must be inside this session's own virtual root; a relative
        // one is resolved against it. Both are rebuilt from validated segments, so the
        // address the host receives can never point outside the view.
        if (value.startsWith("/")) {
          if (value !== root && !value.startsWith(root + "/")) throw new Error("文件路径超出工作区");
          return [root, ...normalizeSegments(value.slice(root.length))].join("/");
        }
        return [root, ...normalizeSegments(value)].join("/");
      }
      const fileVersion = (size) => (typeof size === "number" ? String(size) : "0");
      // The file panel's live change feed. The native provider
      // (`@deepseek-ai/dsh-api-workspace-files`) pulls
      // `remote.workspaceFiles.changes(sessionId, signal)` and expects an async iterable of
      // frames: one `{kind:"ready"}` first (which it acknowledges), then
      // `{kind:"change", change:{absolutePath, version}}` when a file appears or changes and
      // `{kind:"change", change:{absolutePath, absent:true}}` when it is gone. The wire
      // protocol normally wraps each item as `{value, accept}`; the product answers this
      // namespace itself, so it must wrap them the same way or the consumer reads
      // `item.value` of `undefined`.
      //
      // The product has no filesystem watcher for an ordinary user — the Host observes its OWN
      // instrumented writes, while a user's workspace is served through `/sidebar/api/fs.tree`.
      // So the feed is derived: the chat's own event stream says when the agent did something,
      // the grouped listing is re-read then (plus a slow safety poll for changes that came from
      // outside the agent), and the two listings are diffed. Only the session's own view is
      // walked, the administrator's read-only library is skipped because it cannot change
      // during a session, and both the directory walk and the file count are capped so a
      // pathological workspace cannot make an open preview into a load source.
      const WATCH_IDLE_MS = 15000, WATCH_EVENT_DEBOUNCE_MS = 400, WATCH_MAX_DIRS = 400, WATCH_MAX_FILES = 4000, WATCH_MAX_QUEUE = 500;
      // Kept equal to `SHARE_LABEL` in plugins/platform/share.mjs by tests/native-watch.test.mjs.
      const WATCH_SHARE_LABEL = "共享数据（只读）";
      // The administrator's library appears in the view as one group, so its own path ends with
      // the label while its children carry it as a segment.
      const isSharedView = (path) => path === WATCH_SHARE_LABEL || path.endsWith(`/${WATCH_SHARE_LABEL}`) || path.includes(`/${WATCH_SHARE_LABEL}/`);
      async function workspaceSnapshot(sessionId) {
        const files = new Map();
        const queue = [await virtualAddress(sessionId, "")];
        let directories = 0;
        while (queue.length > 0 && directories < WATCH_MAX_DIRS && files.size < WATCH_MAX_FILES) {
          const directory = queue.shift();
          directories += 1;
          const { entries } = await explorer("fs.tree", { sessionId, path: directory });
          for (const entry of entries) {
            if (typeof entry.path !== "string") continue;
            if (entry.isDir) { if (!isSharedView(entry.path)) queue.push(entry.path); continue; }
            files.set(entry.path, fileVersion(entry.size));
          }
        }
        return files;
      }
      function workspaceChanges(previous, next) {
        const changes = [];
        for (const [path, version] of next) if (previous.get(path) !== version) changes.push({ absolutePath: path, version });
        for (const path of previous.keys()) if (!next.has(path)) changes.push({ absolutePath: path, absent: true });
        return changes;
      }
      // The feed is live from the CALL, not from the first pull: the native transport streams
      // frames as they occur and buffers them for the consumer, and an async generator cannot do
      // that (it only advances when pulled, so a write that lands between pulls would be folded
      // into the next baseline instead of being reported). Hence a small queue: the watcher runs
      // on its own, the consumer drains it, and cancelling the signal stops both.
      function watchWorkspace(sessionId, signal) {
        const pending = [];
        let waiting, wakeSleep, stream, ended = false, wakeups = 0;
        const deliver = (frame) => {
          if (ended) return;
          // The consumer iterates the feed and reads `item.value` (the frame) and
          // `item.accept()` (its acknowledgement), so the ITERATOR RESULT's value is the
          // envelope, not the frame.
          const item = { value: { value: frame, accept() {} }, done: false };
          if (waiting !== undefined) { const resolve = waiting; waiting = undefined; resolve(item); return; }
          pending.push(item);
          // A consumer that stops pulling must not grow this without bound.
          if (pending.length > WATCH_MAX_QUEUE) pending.splice(0, pending.length - WATCH_MAX_QUEUE);
        };
        const finish = () => {
          if (ended) return;
          ended = true;
          try { stream?.close(); } catch { /* the page may already be gone */ }
          if (waiting !== undefined) { const resolve = waiting; waiting = undefined; resolve({ value: undefined, done: true }); }
        };
        // A message on the chat's stream means the agent just did something: cut the wait short.
        const bump = () => { wakeups += 1; const wake = wakeSleep; wakeSleep = undefined; wake?.(); };
        const run = async () => {
          try {
            if (typeof EventSource === "function") {
              stream = new EventSource(`/geo/api/chats/${rootFor(sessionId)}/events`);
              stream.onmessage = bump;
              stream.onerror = bump;
            }
          } catch { stream = undefined; }
          // `ready` says the subscription is live, which is why the stream opens first; the
          // baseline listing follows immediately so a write right after readiness is reported.
          deliver({ kind: "ready" });
          let previous;
          try { previous = await workspaceSnapshot(sessionId); } catch { previous = undefined; }
          while (!ended && signal?.aborted !== true) {
            await new Promise((resolve) => {
              const timer = setTimeout(done, WATCH_IDLE_MS);
              wakeSleep = done;
              signal?.addEventListener?.("abort", done, { once: true });
              function done() { clearTimeout(timer); wakeSleep = undefined; signal?.removeEventListener?.("abort", done); resolve(); }
            });
            if (ended || signal?.aborted === true) break;
            if (wakeups > 0) {
              wakeups = 0;
              // Let a burst of writes settle before listing, so one turn reports one batch.
              await new Promise((resolve) => setTimeout(resolve, WATCH_EVENT_DEBOUNCE_MS));
              if (ended || signal?.aborted === true) break;
            }
            let next;
            // A transient listing failure must not end the feed: the next beat retries.
            try { next = await workspaceSnapshot(sessionId); } catch { continue; }
            if (previous === undefined) { previous = next; continue; }
            for (const change of workspaceChanges(previous, next)) deliver({ kind: "change", change });
            previous = next;
          }
        };
        signal?.addEventListener?.("abort", finish, { once: true });
        void run().catch(() => {}).finally(finish);
        return {
          [Symbol.asyncIterator]() { return this; },
          next() {
            if (pending.length > 0) return Promise.resolve(pending.shift());
            if (ended) return Promise.resolve({ value: undefined, done: true });
            return new Promise((resolve) => { waiting = resolve; });
          },
        };
      }
      function workspaceFilesFace() {
        const stats = async (sessionId, target) => {
          const path = await virtualAddress(sessionId, target);
          const parent = path.slice(0, path.lastIndexOf("/")) || path;
          const { entries } = await explorer("fs.tree", { sessionId, path: parent });
          const entry = entries.find((item) => item.path === path);
          if (!entry) throw new Error("文件不存在");
          return { absolutePath: path, version: fileVersion(entry.size), ...(typeof entry.size === "number" ? { bytes: entry.size } : {}) };
        };
        const page = async (sessionId, target, range) => {
          const offset = Number.isSafeInteger(range?.offset) && range.offset > 0 ? range.offset : 1;
          const limit = Number.isSafeInteger(range?.limit) && range.limit > 0 ? range.limit : 2000;
          const path = await virtualAddress(sessionId, target);
          const buffer = await explorerBytes(sessionId, path);
          const info = await stats(sessionId, path);
          const decoded = new TextDecoder("utf-8", { fatal: false }).decode(buffer);
          if (decoded.includes("\0")) throw new Error(`“${path}”不是文本文件`);
          const lines = decoded.split("\n");
          const slice = lines.slice(offset - 1, offset - 1 + limit);
          const text = slice.join("\n");
          if (new TextEncoder().encode(text).length > PAGE_BYTES) throw new Error(`“${path}”的这一页超出 2 MiB 上限`);
          return { ...info, offset, text, lines: slice.length, eof: offset - 1 + slice.length >= lines.length };
        };
        const complete = async (sessionId, target) => {
          const path = await virtualAddress(sessionId, target);
          const buffer = await explorerBytes(sessionId, path);
          if (buffer.byteLength > FULL_FILE_BYTES) throw new Error(`“${path}”超过 32 MiB 的整文件读取上限`);
          const bytes = new Uint8Array(buffer);
          let binary = "";
          for (let index = 0; index < bytes.length; index += 0x8000) binary += String.fromCharCode(...bytes.subarray(index, index + 0x8000));
          return { absolutePath: path, version: fileVersion(bytes.length), bytes: bytes.length, offset: 0, data: btoa(binary), eof: true };
        };
        return {
          list: async (sessionId, target) => {
            try {
              const path = await virtualAddress(sessionId, target);
              const { entries } = await explorer("fs.tree", { sessionId, path });
              // The native tree wants lstat-style entries and derives child addresses by
              // joining names, so only the name, kind and size travel.
              return ok({ path, truncated: false,
                entries: entries.map((entry) => ({ name: entry.name, type: entry.isDir ? "directory" : "file",
                  ...(typeof entry.size === "number" ? { size: entry.size } : {}) })) });
            } catch (error) { return fail(error.message); }
          },
          stat: async (sessionId, target) => { try { return ok(await stats(sessionId, target)); } catch (error) { return fail(error.message); } },
          read: async (sessionId, target, range) => { try { return ok(await page(sessionId, target, range)); } catch (error) { return fail(error.message); } },
          readAll: async (sessionId, target) => { try { return ok(await complete(sessionId, target)); } catch (error) { return fail(error.message); } },
          readRelated: async (sessionId, target, relativePath) => {
            try {
              const relative = assertSafe(String(relativePath ?? ""));
              if (relative === "" || relative.startsWith("/") || /^[a-z][a-z\d+.-]*:/iu.test(relative))
                throw new Error("关联文件路径无效");
              const base = await virtualAddress(sessionId, target);
              const parent = base.slice(0, base.lastIndexOf("/"));
              return ok(await complete(sessionId, `${parent}/${relative}`));
            } catch (error) { return fail(error.message); }
          },
          // Live change feed for the panel's open files and the resource provider behind the
          // document preview. It reports only what the session's own listing shows (see
          // `watchWorkspace` for the frame contract and why it is derived, not observed).
          changes: (sessionId, signal) => watchWorkspace(sessionId, signal) };
      }
      ctx.provide("remote", { $host: { platform: "managed", isLoopback: false }, $on: (event, listener) => {
        if (event !== "user-questions/request") return () => {};
        remoteListeners.set(event, listener);
        return () => { if (remoteListeners.get(event) === listener) remoteListeners.delete(event); };
      },
        session: { openWorkspacePath: async () => fail("主机路径不可访问，请在会话工作区查看文件。") },
        // The native right sidebar's file tree and document preview read the workspace
        // through this Remote namespace (`list` / `read` / `readAll` / `readRelated` /
        // `stat`). It normally arrives over the browser API plane, which the product
        // keeps closed, so the product answers it from its OWN explorer surface
        // (`/sidebar/api/fs.tree` and `/sidebar/file`, both ownership-checked) — the
        // same virtual view the retired product file panel used: 上传的文件 /
        // 分析结果 / 过程记录, with display names resolved back to real files on the
        // host. Paths are virtual, so they can never name a host file directly.
        workspaceFiles: workspaceFilesFace() });
      ctx.provide("remote.session", ctx.remote.session);
      ctx.provide("remote.workspaceFiles", ctx.remote.workspaceFiles);
      const preferences = new Map();
      ctx.provide("settingsScope", { bind({ namespace }) {
        if (!preferences.has(namespace)) {
          const defaults = namespace.includes("locale") ? { preference: "zh" } : namespace.includes("theme") ? { preference: managedTheme.colorScheme, fontSize: managedTheme.fontSize || 16 } : {};
          const value = createSnapshotStore({ status: "ready", value: defaults, revision: 0, error: null });
          preferences.set(namespace, { ...value, set: async (field, next) => value.set({ ...value.getSnapshot(), value: { ...value.getSnapshot().value, [field]: next } }) });
        }
        return preferences.get(namespace);
      } });
      ctx.inject(["theme"], (inner) => {
        inner.effect(() => inner.theme.register(managedTheme));
        inner.theme.setTheme(managedTheme.id);
      });
      if (managedTheme.wallpaper) ctx.effect(() => {
        const backdrop = document.createElement("div"); backdrop.className = "geo-product-wallpaper"; backdrop.style.backgroundImage = `url("${managedTheme.wallpaper}")`; backdrop.style.filter = `blur(${managedTheme.wallpaperBlur}px)`;
        document.body.prepend(backdrop); return () => backdrop.remove();
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
        inner.effect(() => inner.locale.register("subagent", "zh-Hans", {
          "mode.continuable": "持久会话", "mode.oneShot": "单次会话",
          "count.total.one": "{count} 个子智能体", "count.total.other": "{count} 个子智能体",
          "count.running.one": "{count} 个子智能体，正在运行", "count.running.other": "{count} 个子智能体，正在运行",
          "switcher.aria": "切换子智能体：{title}", "tree.aria": "子智能体运行记录",
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
      function sendDevelopment(command) {
        if (developmentReady && developmentFrame?.contentWindow) developmentFrame.contentWindow.postMessage(command, location.origin);
        else pendingDevelopmentCommand = command;
      }
      async function enterDevelopment(command) {
        if (!state.getSnapshot().user?.admin) return;
        if (command) pendingDevelopmentCommand = command;
        try {
          const result = await api("/admin/development/start", "POST");
          set({ view: "development", panel: null, error: "", developmentUrl: result.url + "?embedded=1" });
          layoutCall("closeDetails");
          if (developmentReady && pendingDevelopmentCommand) { const next = pendingDevelopmentCommand; pendingDevelopmentCommand = null; sendDevelopment(next); }
        } catch (error) {
          if ([401, 403].includes(error.status)) set({ panel: "development", developmentTarget: "development" });
          else throw error;
        }
      }
      function DevelopmentWorkspace() {
        const s = useState(), [left, setLeft] = React.useState(280);
        React.useLayoutEffect(() => {
          document.body.dataset.geoView = s.view;
          const column = document.querySelector('.geo-native-sidebar');
          const measure = () => { if (column) setLeft(column.getBoundingClientRect().right); };
          const observer = new ResizeObserver(measure); if (column) observer.observe(column); measure(); window.addEventListener("resize", measure);
          return () => { observer.disconnect(); window.removeEventListener("resize", measure); delete document.body.dataset.geoView; };
        }, [s.view]);
        React.useEffect(() => {
          const receive = (event) => {
            if (event.origin !== location.origin || event.source !== developmentFrame?.contentWindow || !state.getSnapshot().user?.admin) return;
            const message = event.data;
            if (message?.type === "geo:development-ready") { developmentReady = true; if (pendingDevelopmentCommand) { const command = pendingDevelopmentCommand; pendingDevelopmentCommand = null; sendDevelopment(command); } }
            else if (message?.type === "geo:development-catalog" && Array.isArray(message.items)) {
              const items = message.items.filter((item) => typeof item.id === "string" && typeof item.title === "string").slice(0, 200);
              const current = state.getSnapshot(), editing = current.panel === "edit" && current.editing?.kind === "development" ? current.editing : null;
              set({ developmentItems: items, developmentCurrent: message.current, ...(editing && !items.some((item) => item.id === editing.id) ? { panel: null, error: "" } : {}) });
            }
            else if (message?.type === "geo:research") set({ view: "research" });
            else if (message?.type === "geo:releases") set({ panel: "releases" });
            else if (message?.type === "geo:development-expired") { developmentReady = false; set({ panel: "development", developmentTarget: "development", developmentUrl: null }); }
            else if (message?.type === "geo:development-error") set({ error: String(message.message).slice(0, 500) });
          };
          window.addEventListener("message", receive); return () => window.removeEventListener("message", receive);
        }, []);
        if (!s.user?.admin || !s.developmentUrl) return null;
        // No second toolbar here: the embedded native header already shows the
        // task title, and every action that bar carried exists in the sidebar
        // (mode tabs, 新建创造任务, 管理中心, 产品配置与发布, 管理员设置) or in the
        // embedded header itself. Keeping it only cost a row of chrome.
        return h("section", { className: "geo-unified-development", hidden: s.view !== "development", style: { left }, "aria-label": "创造任务" },
          h("iframe", { ref: (element) => { developmentFrame = element; }, src: s.developmentUrl, title: "原生 DSH 创造任务", className: "geo-development-frame" }));
      }
      // Project/chat navigation. The product renders it inside its own sidebar on the
      // older line; from 0.1.5 the native sidebar owns that column and exposes the
      // `sidebar.workspaces` region, whose native filler is `ui-workspace` — the plugin
      // whose activation needs the closed host directory picker. Filling the native
      // position with the product's own project/chat model keeps navigation instead of
      // trading it away. `wide` is the native sidebar's own collapsed signal: in the
      // rail the native toggle stays and the hero picker still covers selection, which
      // is the same behaviour the product's own collapsed sidebar had.
      function ProjectTree({ wide = true }) {
        const s = useState(), l = useList();
        if (!s.user || !wide) return null;
        return h("div", { className: "geo-native-tree" },
          s.previewMode && h("strong", { role: "status" }, "用户版预览"),
          s.releaseUpdate && button("新版已发布，刷新页面", icons.IconRefreshOutline16, () => location.reload()),
          s.user.admin && h("div", { className: "geo-mode-tabs", role: "group", "aria-label": "任务模式" }, button("研究任务", icons.IconNewChatOutline16, () => set({ view: "research" }), { "aria-pressed": s.view === "research" }), button("创造任务", icons.IconSettingsOutline16, () => enterDevelopment(), { "aria-pressed": s.view === "development" })),
          s.view !== "development" && button("新建项目", icons.IconProjectAddOutline16, () => set({ panel: "project" })),
          h("nav", { "aria-label": s.view === "development" ? "创造任务" : "研究项目", className: "geo-native-projects" }, ...s.projects.filter(() => s.view !== "development").map((p) => h("section", { key: p.id },
            h("div", { className: "geo-native-project-title" }, button(p.title, icons.IconFolderOpenOutline16, () => selectProject(p.id)), button("新建对话", icons.IconPlusOutline16, () => create({ workspaceId: p.id }), { iconOnly: true, disabled: p.archived }), button("管理项目", icons.IconSettingsOutline16, () => set({ panel: "edit", editing: { kind: "projects", id: p.id, title: p.title } }), { iconOnly: true })),
            ...l.ids.filter((id) => l.byId[id].projectId === p.id).map((id) => h("div", { key: id, className: "geo-native-chat-row" }, button(l.byId[id].displayTitle, icons.IconNewChatOutline16, () => open(id), { className: s.view === "research" && l.current === id ? "selected" : "" }), button("管理对话", icons.IconSettingsOutline16, () => set({ panel: "edit", editing: { kind: "chats", id, title: l.byId[id].displayTitle } }), { iconOnly: true }))))),
            s.user.admin && s.view === "development" && h("section", { className: "geo-development-project" }, h("div", { className: "geo-native-project-title" }, button("GeoSentinel 开发", icons.IconFolderOpenOutline16, () => enterDevelopment()), button("新建创造任务", icons.IconPlusOutline16, () => enterDevelopment({ type: "geo:development-create" }), { iconOnly: true })),
              ...s.developmentItems.map((item) => h("div", { key: item.id, className: "geo-native-chat-row" }, button(item.title, icons.IconNewChatOutline16, () => enterDevelopment({ type: "geo:development-open", id: item.id }), { className: s.view === "development" && s.developmentCurrent === item.id ? "selected" : "" }), item.running && h("span", { role: "status" }, "运行中"), button("管理创造任务", icons.IconSettingsOutline16, () => set({ panel: "edit", editing: { kind: "development", id: item.id, title: item.title } }), { iconOnly: true }))))));
      }
      // The product's tool entries. From 0.1.5 they live in the native sidebar's
      // `sidebar.footer.action` list, which is what the retired better-sidebar tabs
      // used to provide — including 监测简报 and 空间数据, so those panels get an entry
      // back instead of only being reachable from inside the conversation header.
      function ProductEntries({ wide = true }) {
        const s = useState();
        if (!s.user) return null;
        if (!wide) return button("全球事件监测", icons.IconGlobeOutline14, () => openResearchTab("monitor"), { iconOnly: true });
        return h("div", { className: "geo-native-bottom" },
          button("全球事件监测", icons.IconGlobeOutline14, () => openResearchTab("monitor")),
          button("监测简报", icons.IconListPenOutline16, () => openResearchTab("briefs")),
          button("空间数据", icons.IconDataOutline16, () => openResearchTab("spatial")),
          s.user.admin && button("管理中心", icons.IconSettingsOutline16, async () => { const data = await api("/admin/users"); set({ panel: "admin", users: data.users }); }),
          s.user.admin && button("产品配置与发布", icons.IconSettingsOutline16, () => set({ panel: "releases" })),
          s.user.admin && button("管理员设置", icons.IconSettingsOutline16, () => enterDevelopment({ type: "geo:development-settings" })),
          button(s.user.username, icons.IconUserOutline16, () => set({ panel: "account" })));
      }
      function Sidebar({ collapsed }) {
        return h("aside", { className: "geo-native-sidebar" },
          h("div", { className: "geo-native-toolbar" }, !collapsed && h("strong", null, "地缘环境智能计算平台"), button("收起或展开侧栏", icons.IconPanelLeftOutline16, () => layoutCall("toggleSidebar"), { iconOnly: true })),
          h(ProjectTree, { wide: !collapsed }), h(ProductEntries, { wide: !collapsed }));
      }
      function openResearchTab(kind) {
        const service = ctx.get("betterSidebar");
        if (!list.getSnapshot().current || !service) { set({ panel: kind }); return; }
        layoutCall("closeDetails");
        service.openTab({ type: `geosentinel:${kind}`, url: location.origin + (kind === "monitor" ? "/geo/api/monitor/events" : "/geo/api/projects") });
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
            s.panel === "edit" && h("form", { key: s.editing.id, onSubmit: submit(async (data) => {
                // Creation tasks live in the embedded development process; the
                // shell can only forward the request, so the panel stays open
                // until the refreshed catalog confirms the result.
                if (s.editing.kind === "development") { set({ error: "" }); sendDevelopment({ type: "geo:development-rename", id: s.editing.id, title: data.get("title") }); return; }
                await api(`/${s.editing.kind}/${s.editing.id}`, "PATCH", { title: data.get("title") }); await refresh(); set({ panel: null }); }) },
              h("h2", null, s.editing.kind === "projects" ? "管理项目" : s.editing.kind === "development" ? "管理创造任务" : "管理对话"),
              h("label", null, "名称", h("input", { name: "title", defaultValue: s.editing.title, required: true, maxLength: 160 })),
              h("button", { type: "submit", disabled: busy }, "保存名称"),
              button("删除", icons.IconTrashOutline16, () => set({ panel: "delete" }))),
            s.panel === "delete" && h(React.Fragment, null, h("h2", null, "确认删除"), h("p", null, `删除“${s.editing.title}”后，该入口将不再显示。`),
              button("取消", null, () => set({ panel: "edit" })), button("确认删除", icons.IconTrashOutline16, async () => {
                if (s.editing.kind === "development") { set({ error: "", panel: "edit" }); sendDevelopment({ type: "geo:development-delete", id: s.editing.id }); return; }
                await api(`/${s.editing.kind}/${s.editing.id}`, "DELETE"); clearSelection(); await refresh(); set({ panel: null }); })),
            s.panel === "account" && h(React.Fragment, null, h("h2", null, s.user.username), h(Usage), h("form", { onSubmit: submit(async (data) => { await api("/auth/password", "POST", { currentPassword: data.get("current"), newPassword: data.get("next") }); clear(); location.reload(); }) },
              h("label", null, "当前密码", h("input", { name: "current", type: "password", autoComplete: "current-password", required: true })), h("label", null, "新密码", h("input", { name: "next", type: "password", autoComplete: "new-password", minLength: 8, required: true })), h("button", { type: "submit", disabled: busy }, "修改密码")), button("退出登录", icons.IconUserOutline16, async () => { await api("/auth/logout", "POST"); clear(); location.reload(); })),
            s.panel === "admin" && s.user.admin && h(React.Fragment, null, h("h2", null, "管理中心"), h(Usage, { admin: true }), button("生成邀请码", icons.IconPlusOutline16, async () => { const data = await api("/admin/invites", "POST"); set({ invite: data.invite }); }), s.invite && h("input", { value: s.invite, readOnly: true, "aria-label": "邀请码" }),
              ...(s.users ?? []).map((user) => h("div", { key: user.id, className: "geo-native-user" }, h("span", null, user.username), button(user.disabled ? "启用" : "停用", null, async () => { await api(`/admin/users/${user.id}`, "PATCH", { disabled: !user.disabled }); set({ users: (await api("/admin/users")).users }); }, { disabled: user.id === s.user.id })))),
            s.panel === "development" && s.user.admin && h("form", { onSubmit: submit(async (data) => { await api("/admin/development/unlock", "POST", { password: data.get("password") }); if (s.developmentTarget === "releases") { set({ panel: "releases" }); return; } await enterDevelopment(); }) },
              h("h2", null, "管理员开发模式"), h("p", null, "拥有服务主机的文件与执行权限。修改仅在验证并发布后应用于普通用户。"),
              h("label", null, "确认管理员密码", h("input", { name: "password", type: "password", autoComplete: "current-password", required: true })),
              h("button", { type: "submit", disabled: busy }, busy ? "正在准备开发环境…" : "确认并继续")),
            s.panel === "releases" && s.user.admin && h(ReleasePanel),
            s.panel === "monitor" && h(Monitor),
            s.panel === "briefs" && h(Briefs),
            s.panel === "spatial" && h(Spatial));
      }
      const MONITOR_TYPES = {
        conflict: { label: "冲突", color: "#991b1b" },
        geopolitical: { label: "地缘政治", color: "#7c3aed" },
        earthquake: { label: "地震", color: "#b45309" },
        wildfires: { label: "野火", color: "#c2410c" },
        storm: { label: "风暴", color: "#1d4ed8" },
        tc: { label: "热带气旋", color: "#0e7490" },
        floods: { label: "洪涝", color: "#0369a1" },
        dr: { label: "干旱", color: "#a16207" },
        vo: { label: "火山", color: "#9f1239" },
        landslide: { label: "滑坡", color: "#4d7c0f" },
        other: { label: "其他公开事件", color: "#475569" },
      };
      const MONITOR_ALIAS = {
        eq: "earthquake", earthquakes: "earthquake", earthquake: "earthquake",
        wf: "wildfires", wildfire: "wildfires", wildfires: "wildfires",
        fl: "floods", flood: "floods", floods: "floods",
        drought: "dr", volcanoes: "vo", volcano: "vo",
        severe_storms: "storm", storm: "storm", storms: "storm",
        landslides: "landslide", landslide: "landslide",
        conflict: "conflict", geopolitical: "geopolitical", tc: "tc", dr: "dr", vo: "vo",
      };
      const MONITOR_LEGEND = Object.keys(MONITOR_TYPES);
      const MONITOR_SOURCE_STATUS = { error: "本轮不可用", rate_limited: "已限流", disabled: "未启用", degraded: "降级" };
      const MONITOR_BRIEF_STATE = { unavailable: "本轮简报生成失败，仅显示来源字段", partial: "部分记录尚未生成简报", original: "未启用中文整理，按原始专名展示", cached: "简报来自缓存" };
      const monitorKey = (type) => (MONITOR_TYPES[type] ? type : MONITOR_ALIAS[type] ?? "other");
      const monitorType = (type) => MONITOR_TYPES[monitorKey(type)];
      const monitorTime = (value) => value ? new Date(value).toLocaleString("zh-CN", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" }) : "时间未说明";
      // Display grading. The level is derived from the source-declared severity
      // plus one explicit, visible recency rule: a high-severity lead published
      // within MONITOR_KEY_HOURS is drawn as a key lead. No magnitude, impact or
      // confidence is invented here — sources that never declared a level stay low.
      const MONITOR_KEY_HOURS = 72;
      const MONITOR_LEVELS = {
        key: { label: "重点线索", size: 21, hint: `来源标记为高关注且发布于 ${MONITOR_KEY_HOURS} 小时内` },
        high: { label: "高关注", size: 15, hint: "来源标记为高关注，发布时间较早" },
        medium: { label: "中关注", size: 10, hint: "来源标记为中关注" },
        low: { label: "低关注", size: 6, hint: "来源标记为低关注，多为背景线索" },
      };
      const MONITOR_LEVEL_ORDER = ["key", "high", "medium", "low"];
      const MONITOR_WINDOWS = [{ hours: 24, label: "24 小时" }, { hours: 72, label: "72 小时" }, { hours: 168, label: "7 天" }, { hours: null, label: "全部" }];
      const MONITOR_SNAPSHOT_CAP = 200;
      const monitorAgeHours = (item, now) => Number.isFinite(item.publishedAt) ? (now - item.publishedAt) / 3600000 : null;
      // The snapshot already carries a tested `level`. The local rule is the same
      // one, kept as a fallback so a payload produced by an older release still
      // renders with a grade instead of a blank symbol.
      const monitorLevelKey = (item, now) => MONITOR_LEVELS[item?.level] ? item.level : (item.severity === "high" ? (monitorAgeHours(item, now) !== null && monitorAgeHours(item, now) <= MONITOR_KEY_HOURS ? "key" : "high") : item.severity === "medium" ? "medium" : "low");
      const monitorCoordinates = (item) => Number.isFinite(item.latitude) && Number.isFinite(item.longitude);
      const monitorCoordinate = (point) => `${Math.abs(point.lat).toFixed(2)}°${point.lat >= 0 ? "N" : "S"} ${Math.abs(point.lng).toFixed(2)}°${point.lng >= 0 ? "E" : "W"}`;
      // One popup for the map, shared by every level: source fields only, with the
      // brief sections marked as reported points rather than verified facts.
      function monitorPopup(item, level) {
        const style = monitorType(item.type);
        const popup = document.createElement("div");
        popup.className = "geo-monitor-popup";
        const heading = document.createElement("strong"); heading.textContent = item.displayTitle ?? item.title;
        const meta = document.createElement("span"); meta.textContent = `${style.label} · ${MONITOR_LEVELS[level].label} · ${item.source} · ${monitorTime(item.publishedAt)}`;
        popup.append(heading, meta);
        if (item.brief?.summary) { const line = document.createElement("p"); line.textContent = item.brief.summary; popup.append(line); }
        if (item.brief?.facts?.length) {
          const list = document.createElement("ul");
          for (const fact of item.brief.facts) { const entry = document.createElement("li"); entry.textContent = fact; list.append(entry); }
          popup.append(list);
        }
        if (item.brief?.significance) { const line = document.createElement("p"); line.textContent = "关注点：" + item.brief.significance; popup.append(line); }
        if (item.brief?.uncertainty) { const line = document.createElement("p"); line.textContent = "待核实：" + item.brief.uncertainty; popup.append(line); }
        const place = document.createElement("em");
        place.textContent = `${item.displayLocation ?? item.location ?? "地点待核验"}${monitorCoordinates(item) ? ` · ${monitorCoordinate({ lat: item.latitude, lng: item.longitude })}` : " · 无坐标"}`;
        popup.append(place);
        return popup;
      }
      function Monitor() {
        const [data, change] = React.useState(null), [error, problem] = React.useState("");
        const [selected, select] = React.useState(null);
        const [hidden, hide] = React.useState(() => new Set());
        // Research default: key leads, high and medium attention. Low-severity
        // leads are a background layer the user switches on explicitly.
        const [levels, setLevels] = React.useState(() => new Set(["key", "high", "medium"]));
        const [hours, setHours] = React.useState(null);
        const [fullscreen, full] = React.useState(false);
        const levelsTouched = React.useRef(false);
        const mapElement = React.useRef(null), map = React.useRef(null), markers = React.useRef(null), pins = React.useRef(new Map());
        const cursorElement = React.useRef(null), selectedRef = React.useRef(null), rootElement = React.useRef(null);
        selectedRef.current = selected;
        const now = React.useMemo(() => Date.now(), [data]);
        const toggleType = (key) => hide((previous) => { const next = new Set(previous); next.has(key) ? next.delete(key) : next.add(key); return next; });
        const toggleLevel = (key) => { levelsTouched.current = true; setLevels((previous) => { const next = new Set(previous); next.has(key) ? next.delete(key) : next.add(key); return next; }); };
        // The expanded view widens the default filter once, so "full screen shows
        // more" is true without overriding a deliberate choice. The panel is also
        // handed to the browser's own full-screen mode: inside the sidebar a
        // transformed ancestor would otherwise trap the overlay in the column.
        const toggleFullscreen = () => {
          if (fullscreen) {
            full(false);
            if (document.fullscreenElement) document.exitFullscreen?.().catch(() => {});
            return;
          }
          full((previous) => { if (!levelsTouched.current) setLevels(new Set(MONITOR_LEVEL_ORDER)); return true; });
          const element = rootElement.current;
          if (element?.requestFullscreen) element.requestFullscreen().then(() => map.current?.invalidateSize()).catch(() => {});
        };
        React.useEffect(() => {
          const onChange = () => { if (!document.fullscreenElement) full(false); };
          document.addEventListener("fullscreenchange", onChange);
          return () => document.removeEventListener("fullscreenchange", onChange);
        }, []);
        React.useEffect(() => {
          if (!window.L || !mapElement.current) return;
          const instance = L.map(mapElement.current, { zoomControl: false, minZoom: 0, maxZoom: 8, worldCopyJump: false, preferCanvas: true });
          instance.attributionControl.setPrefix(false); instance.attributionControl.addAttribution("Natural Earth · 概览边界");
          L.control.zoom({ position: "bottomright", zoomInTitle: "放大", zoomOutTitle: "缩小" }).addTo(instance);
          instance.fitBounds(WORLD_BOUNDS, { animate: false }); map.current = instance;
          markers.current = L.layerGroup().addTo(instance);
          const onMove = (event) => { const element = cursorElement.current; if (element) element.textContent = monitorCoordinate(event.latlng); };
          const onOut = () => { const element = cursorElement.current; if (element) element.textContent = "光标未在地图上"; };
          instance.on("mousemove", onMove); instance.on("mouseout", onOut);
          instance.on("click", () => select(null));
          const resize = new ResizeObserver(() => instance.invalidateSize()); resize.observe(mapElement.current);
          let live = true;
          fetch("/geo/vendor/world.json", { cache: "no-store" }).then((r) => r.json()).then((world) => {
            if (!live) return;
            for (const feature of world.features.filter((f) => f.type === 3)) L.polygon(feature.geometry.map((ring) => ring.map(([x, y]) => [Math.atan(Math.sinh(Math.PI * (1 - 2 * y / 4096))) * 180 / Math.PI, x / 4096 * 360 - 180])), { pane: "tilePane", color: "#a8bcc6", weight: 0.6, fillColor: "#f7fbfc", fillOpacity: 1, interactive: false }).addTo(instance);
          }).catch(() => live && problem("底图加载失败，可继续查看事件列表。"));
          return () => { live = false; instance.off("mousemove", onMove); instance.off("mouseout", onOut); resize.disconnect(); instance.remove(); map.current = null; markers.current = null; pins.current = new Map(); };
        }, []);
        React.useEffect(() => { const timer = setTimeout(() => map.current?.invalidateSize(), 80); return () => clearTimeout(timer); }, [fullscreen]);
        React.useEffect(() => {
          if (!fullscreen) return;
          // Capture phase + stopPropagation: Escape leaves the expanded view and
          // must not also dismiss the whole monitor panel behind it.
          const onKey = (event) => {
            if (event.key !== "Escape") return;
            event.preventDefault();
            event.stopPropagation();
            full(false);
          };
          document.addEventListener("keydown", onKey, true);
          return () => document.removeEventListener("keydown", onKey, true);
        }, [fullscreen]);
        const items = React.useMemo(() => data?.items ?? [], [data]);
        const located = React.useMemo(() => items.filter(monitorCoordinates), [items]);
        const levelOf = React.useCallback((item) => monitorLevelKey(item, now), [now]);
        // Filters apply to the whole snapshot, not only to the mapped subset:
        // a fresh high-attention lead from a news feed has no coordinates and must
        // still be readable in the list and the key-lead rail.
        const shown = React.useMemo(() => items.filter((item) =>
          !hidden.has(monitorKey(item.type)) &&
          levels.has(levelOf(item)) &&
          (hours === null || (monitorAgeHours(item, now) ?? Infinity) <= hours)
        ), [items, hidden, levels, hours, now, levelOf]);
        const plotted = React.useMemo(() => shown.filter(monitorCoordinates), [shown]);
        React.useEffect(() => {
          if (!markers.current) return;
          markers.current.clearLayers(); pins.current = new Map();
          for (const item of plotted) {
            const level = levelOf(item), style = monitorType(item.type), size = MONITOR_LEVELS[level].size;
            const marker = L.marker([item.latitude, item.longitude], {
              icon: L.divIcon({ className: "geo-monitor-pin-host", html: `<span class="geo-monitor-pin level-${level}" style="--geo-pin:${style.color}"></span>`, iconSize: [size, size], iconAnchor: [size / 2, size / 2] }),
              keyboard: false, riseOnHover: true, zIndexOffset: level === "key" ? 1000 : level === "high" ? 400 : 0,
            });
            marker.bindPopup(monitorPopup(item, level), { maxWidth: 360, closeButton: true });
            marker.bindTooltip(item.displayTitle ?? item.title, { permanent: fullscreen && level === "key", direction: "right", offset: [size / 2 + 2, 0], className: "geo-monitor-label", opacity: 1 });
            marker.on("click", () => select(item.id));
            marker.addTo(markers.current);
            pins.current.set(item.id, marker);
            if (selectedRef.current === item.id) marker.getElement()?.querySelector(".geo-monitor-pin")?.classList.add("is-selected");
          }
        }, [plotted, fullscreen, levelOf]);
        React.useEffect(() => {
          for (const [id, pin] of pins.current) pin.getElement()?.querySelector(".geo-monitor-pin")?.classList.toggle("is-selected", id === selected);
          const marker = selected ? pins.current.get(selected) : null;
          if (!marker || !map.current) return;
          map.current.flyTo(marker.getLatLng(), Math.max(map.current.getZoom(), fullscreen ? 5 : 4), { duration: 0.6 });
          marker.openPopup();
        }, [selected, fullscreen, plotted]);
        React.useEffect(() => { let live = true; const update = () => api("/monitor/events").then((value) => live && change(value)).catch((e) => live && problem(e.message)); update(); const timer = setInterval(update, 60000); return () => { live = false; clearInterval(timer); }; }, []);
        const sources = data?.sources ?? [];
        // Level counts describe the snapshot (so the chips and the rail agree);
        // the "located" count stays visible in the status line.
        const levelCounts = MONITOR_LEVEL_ORDER.reduce((total, key) => { total[key] = items.filter((item) => levelOf(item) === key).length; return total; }, {});
        const typeCounts = items.reduce((total, item) => { const key = monitorKey(item.type); total[key] = (total[key] ?? 0) + 1; return total; }, {});
        const keyItems = items.filter((item) => levelOf(item) === "key" && !hidden.has(monitorKey(item.type)));
        const selectedItem = selected ? items.find((item) => item.id === selected) ?? null : null;
        const snapshotAt = data?.lastSuccessAt ? new Date(data.lastSuccessAt).toLocaleString("zh-CN", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" }) : "尚未完成采集";
        const bringIntoChat = async (item) => {
          const current = list.getSnapshot().current, id = rootFor(current);
          if (!id) throw new Error("请先选择研究对话。");
          if (current !== id) open(id);
          const result = await api(`/chats/${id}/monitor-context`, "POST", { eventId: item.id });
          const input = ctx.get("conversation").input;
          const shell = input.for(binding(id).ctx);
          shell.setDraft(`请核验这条全球事件监测线索：${item.displayTitle ?? item.title}\n来源文件：${result.path}\n先区分已确认事实和待核验信息。`);
          set({ panel: null });
        };
        const briefBlock = (item) => item.brief && h("div", { className: "geo-monitor-report" },
          h("p", { className: "geo-monitor-brief" }, item.brief.summary),
          item.brief.facts?.length > 0 && h("ul", { className: "geo-monitor-facts" }, ...item.brief.facts.map((fact, index) => h("li", { key: index }, fact))),
          item.brief.significance && h("p", { className: "geo-monitor-note" }, h("b", null, "关注点："), item.brief.significance),
          item.brief.uncertainty && h("p", { className: "geo-monitor-note" }, h("b", null, "待核实："), item.brief.uncertainty));
        // One event card, used by the compact list and the expanded rail.
        const eventCard = (item) => {
          const key = monitorKey(item.type), style = MONITOR_TYPES[key], level = levelOf(item);
          return h("article", { key: item.id, "data-severity": item.severity, "data-level": level, className: selected === item.id ? "geo-native-event selected" : "geo-native-event", onClick: () => select(item.id) },
            h("div", { className: "geo-monitor-event-head" },
              h("span", { className: "geo-monitor-badge", style: { background: style.color } }, style.label),
              h("strong", null, item.displayTitle ?? item.title),
              h("span", { className: `geo-monitor-sev level-${level}`, title: MONITOR_LEVELS[level].hint }, MONITOR_LEVELS[level].label)),
            h("small", null, `${item.source} · ${monitorTime(item.publishedAt)}${item.displayLocation ? " · " + item.displayLocation : ""}${monitorCoordinates(item) ? "" : " · 无坐标（仅列表，不猜位置）"}`),
            briefBlock(item),
            h("div", { className: "geo-monitor-actions" },
              h("a", { href: item.url, target: "_blank", rel: "noopener noreferrer", onClick: (event) => event.stopPropagation() }, "查看来源"),
              button("带入研究对话", icons.IconNewChatOutline16, () => bringIntoChat(item))));
        };
        return h("div", { ref: rootElement, className: "geo-native-monitor" + (fullscreen ? " fullscreen" : "") },
          h("div", { className: "geo-native-toolbar" }, h("h2", null, "全球事件监测"),
            data && h("span", { role: "status", className: "geo-monitor-status" }, fullscreen
              ? `快照 ${snapshotAt} · ${sources.length} 个渠道 · ${levelCounts.key} 条重点线索${data.stale ? " · 数据待更新" : ""}`
              : `${shown.length}/${items.length} 条线索 · ${plotted.length} 条已上图${data.stale ? " · 数据待更新" : ""}`),
            button("全球范围", icons.IconGlobeOutline14, () => { select(null); map.current?.fitBounds(WORLD_BOUNDS, { animate: false }); }, { iconOnly: true }),
            button(fullscreen ? "退出全屏" : "全屏查看", null, toggleFullscreen, { className: "geo-monitor-full", "aria-pressed": fullscreen })),
          h("div", { className: "geo-monitor-filters" },
            h("div", { className: "geo-monitor-filter-group" }, h("b", { className: "geo-monitor-filter-title" }, "关注等级"),
              ...MONITOR_LEVEL_ORDER.map((key) => h("button", { key, type: "button", className: levels.has(key) ? "on" : "off", title: MONITOR_LEVELS[key].hint, "aria-pressed": levels.has(key), onClick: () => toggleLevel(key) },
                h("i", { className: `geo-monitor-dot level-${key}` }), `${MONITOR_LEVELS[key].label} ${levelCounts[key]}`))),
            h("div", { className: "geo-monitor-filter-group" }, h("b", { className: "geo-monitor-filter-title" }, "时间窗"),
              ...MONITOR_WINDOWS.map((window) => h("button", { key: String(window.hours), type: "button", className: hours === window.hours ? "on" : "", "aria-pressed": hours === window.hours, onClick: () => setHours(window.hours) }, window.label))),
            h("p", { className: "geo-monitor-note" }, `分级规则：来源标记的高关注条目在 ${MONITOR_KEY_HOURS} 小时内记为“重点线索”，超时保留为“高关注”；等级与类别都取自来源字段，不代表核实结论。`)),
          h("div", { className: "geo-monitor-body" },
            h("div", { className: "geo-monitor-stage" },
              h("div", { className: "geo-native-map", ref: mapElement, "aria-label": "全球事件地图" }),
              h("span", { className: "geo-monitor-cursor", ref: cursorElement, "aria-hidden": "true" }, "光标未在地图上"),
              h("div", { className: "geo-monitor-legend" }, ...MONITOR_LEGEND.map((key) => h("button", { key, type: "button", className: hidden.has(key) ? "off" : "", title: "点击隐藏或显示该类事件", onClick: () => toggleType(key) }, h("i", { style: { background: MONITOR_TYPES[key].color } }), `${MONITOR_TYPES[key].label}${typeCounts[key] ? " " + typeCounts[key] : ""}`)))),
            fullscreen && h("aside", { className: "geo-monitor-rail" },
              h("section", { className: "geo-monitor-meta" }, h("h3", null, "研究口径与快照"),
                h("ul", null,
                  h("li", null, `快照时间：${snapshotAt}${data?.stale ? "（数据待更新）" : ""}`),
                  h("li", null, `采集进程：${data?.workerOnline ? "运行中" : "未连接"} · 状态 ${data?.state ?? "未说明"} · 刷新间隔 ${data?.intervalMs ? Math.round(data.intervalMs / 60000) + " 分钟" : "未说明"}`),
                  h("li", null, `线索 ${items.length} 条（上层快照按关注度最多保留 ${MONITOR_SNAPSHOT_CAP} 条）· 有坐标 ${located.length} 条 · 当前筛选显示 ${shown.length} 条（已上图 ${plotted.length} 条）`),
                  h("li", null, "记录数量不等于已核实事件总数；本面板展示来源线索与来源字段，不代表平台观测结论或因果判断。"))),
              h("section", { className: "geo-monitor-keys" }, h("h3", null, `重点线索 ${keyItems.length}`),
                keyItems.length === 0
                  ? h("p", { className: "geo-monitor-note" }, `当前快照没有符合“${MONITOR_KEY_HOURS} 小时内高关注”的条目。`)
                  : h("ol", null, ...keyItems.slice(0, 12).map((item) => h("li", { key: item.id, className: selected === item.id ? "selected" : "", onClick: () => select(item.id) },
                    h("div", { className: "geo-monitor-keyhead" }, h("i", { className: "geo-monitor-dot level-key" }), h("strong", null, item.displayTitle ?? item.title)),
                    h("small", null, `${item.source} · ${monitorTime(item.publishedAt)}${item.displayLocation ? " · " + item.displayLocation : ""}`),
                    item.brief?.summary && h("p", null, item.brief.summary))))),
              selectedItem && h("section", { className: "geo-monitor-detail" }, h("h3", null, "线索详情"),
                h("p", { className: "geo-monitor-detail-title" }, selectedItem.displayTitle ?? selectedItem.title),
                h("p", { className: "geo-monitor-note" }, `来源 ${selectedItem.source} · ${monitorTime(selectedItem.publishedAt)} · ${MONITOR_LEVELS[levelOf(selectedItem)].label}${monitorCoordinates(selectedItem) ? ` · ${monitorCoordinate({ lat: selectedItem.latitude, lng: selectedItem.longitude })}` : " · 来源未提供坐标"}`),
                briefBlock(selectedItem),
                h("div", { className: "geo-monitor-actions" },
                  h("a", { href: selectedItem.url, target: "_blank", rel: "noopener noreferrer" }, "查看来源"),
                  button("带入研究对话", icons.IconNewChatOutline16, () => bringIntoChat(selectedItem)))),
              sources.length > 0 && h("section", { className: "geo-monitor-sources" }, h("b", { className: "geo-monitor-sources-title" }, "数据源渠道"),
                ...sources.map((source) => h("span", { key: source.name, className: source.status === "ok" ? "" : "off", title: source.note || "" }, `${source.name} · ${source.status === "ok" ? source.count + " 条" : MONITOR_SOURCE_STATUS[source.status] ?? source.status}`)))),
          error && h("p", { role: "alert" }, error), !data && !error && h("p", null, "正在加载监测数据…"),
          data && data.translation && data.translation !== "translated" && h("p", { className: "geo-monitor-note" }, "简报状态：", MONITOR_BRIEF_STATE[data.translation] ?? data.translation, data.translationIssue ? `（${data.translationIssue}）` : ""),
          !fullscreen && sources.length > 0 && h("div", { className: "geo-monitor-sources" }, h("b", { className: "geo-monitor-sources-title" }, "数据源渠道"), ...sources.map((source) => h("span", { key: source.name, className: source.status === "ok" ? "" : "off", title: source.note || "" }, `${source.name} · ${source.status === "ok" ? source.count + " 条" : MONITOR_SOURCE_STATUS[source.status] ?? source.status}`))),
          !fullscreen && shown.length === 0 && items.length > 0 && h("p", { className: "geo-monitor-note" }, "当前筛选下没有事件；打开全屏或调整等级、类别、时间窗筛选。"),
          !fullscreen && h("div", { className: "geo-monitor-list" }, ...shown.map(eventCard))));
      }
      const WORLD_BOUNDS = [[-58, -180], [80, 180]];
      const WORLD_ATTRIBUTION = "Natural Earth · 概览边界";
      // One basemap for the monitor and the spatial-data viewer: vendored
      // Natural Earth polygons, no tile key and no external requests.
      function mountWorldMap(element, onError) {
        const instance = L.map(element, { zoomControl: false, minZoom: 0, maxZoom: 9, worldCopyJump: false, preferCanvas: true });
        instance.attributionControl.setPrefix(false);
        instance.attributionControl.addAttribution(WORLD_ATTRIBUTION);
        L.control.zoom({ position: "bottomright", zoomInTitle: "放大", zoomOutTitle: "缩小" }).addTo(instance);
        instance.fitBounds(WORLD_BOUNDS, { animate: false });
        let live = true;
        fetch("/geo/vendor/world.json", { cache: "no-store" }).then((r) => r.json()).then((world) => {
          if (!live) return;
          for (const feature of world.features.filter((f) => f.type === 3)) L.polygon(feature.geometry.map((ring) => ring.map(([x, y]) => [Math.atan(Math.sinh(Math.PI * (1 - 2 * y / 4096))) * 180 / Math.PI, x / 4096 * 360 - 180])), { pane: "tilePane", color: "#a8bcc6", weight: 0.6, fillColor: "#f7fbfc", fillOpacity: 1, interactive: false }).addTo(instance);
        }).catch(() => onError?.("底图加载失败，仍可查看数据摘要。"));
        const resize = new ResizeObserver(() => instance.invalidateSize());
        resize.observe(element);
        return { map: instance, dispose() { live = false; resize.disconnect(); instance.remove(); } };
      }
      const SPATIAL_STYLE = {
        Point: "#0369a1", MultiPoint: "#0369a1", LineString: "#7c3aed", MultiLineString: "#7c3aed",
        Polygon: "#0e7490", MultiPolygon: "#0e7490", GeometryCollection: "#475569",
      };
      const SPATIAL_LABEL = { Point: "点", MultiPoint: "多点", LineString: "线", MultiLineString: "多线", Polygon: "面", MultiPolygon: "多面", GeometryCollection: "几何集合" };
      const formatBytes = (value) => value >= 1048576 ? (value / 1048576).toFixed(1) + " MiB" : value >= 1024 ? (value / 1024).toFixed(0) + " KiB" : value + " B";
      function Spatial() {
        const [state, change] = React.useState({ chatId: null, files: [], error: "", selected: null, data: null, busy: false });
        const mapElement = React.useRef(null), map = React.useRef(null), layer = React.useRef(null), current = React.useRef(null);
        React.useEffect(() => {
          if (!window.L || !mapElement.current) return;
          const mounted = mountWorldMap(mapElement.current, (message) => change((previous) => ({ ...previous, error: message })));
          map.current = mounted.map;
          layer.current = L.layerGroup().addTo(mounted.map);
          return () => { layer.current = null; map.current = null; mounted.dispose(); };
        }, []);
        const loadList = React.useCallback(async (chatId) => {
          try {
            const result = await api(`/chats/${chatId}/spatial/list`);
            change((previous) => ({ ...previous, chatId, files: result.files, error: "", selected: null, data: null }));
            layer.current?.clearLayers();
            map.current?.fitBounds(WORLD_BOUNDS, { animate: false });
          } catch (error) {
            change((previous) => ({ ...previous, chatId, files: [], error: error.message, selected: null, data: null }));
          }
        }, []);
        React.useEffect(() => {
          const tick = () => {
            const id = rootFor(list.getSnapshot().current);
            if (!id) { if (current.current) { current.current = null; change((previous) => ({ ...previous, chatId: null, files: [], selected: null, data: null, error: "" })); layer.current?.clearLayers(); } return; }
            if (id === current.current) return;
            current.current = id;
            void loadList(id);
          };
          tick();
          const timer = setInterval(tick, 3000);
          return () => clearInterval(timer);
        }, [loadList]);
        const openFile = async (file) => {
          const chatId = current.current;
          if (!chatId) return;
          change((previous) => ({ ...previous, busy: true, selected: file.path, error: "" }));
          try {
            const data = await api(`/chats/${chatId}/spatial/read?path=${encodeURIComponent(file.path)}`);
            change((previous) => ({ ...previous, data, busy: false }));
            try {
              draw(data);
            } catch (error) {
              layer.current?.clearLayers();
              change((previous) => ({ ...previous, error: "该文件的几何无法在地图上绘制：" + error.message }));
            }
          } catch (error) {
            change((previous) => ({ ...previous, data: null, busy: false, error: error.message }));
            layer.current?.clearLayers();
          }
        };
        const draw = (data) => {
          const group = layer.current, instance = map.current;
          if (!group || !instance) return;
          group.clearLayers();
          const features = data?.features ?? [];
          for (const feature of features) {
            const type = feature?.geometry?.type;
            if (!type) continue;
            if (type === "Point" || type === "MultiPoint") {
              const points = type === "Point" ? [feature.geometry.coordinates] : feature.geometry.coordinates;
              for (const point of points) {
                if (!Array.isArray(point) || !Number.isFinite(point[0]) || !Number.isFinite(point[1])) continue;
                L.circleMarker([point[1], point[0]], { radius: 4.5, color: "#ffffff", weight: 1.3, fillColor: SPATIAL_STYLE[type], fillOpacity: 0.85 })
                  .bindTooltip(String(feature.properties?.name ?? feature.properties?.NAME ?? feature.properties?.label ?? data.path ?? "要素"), { direction: "top", offset: [0, -5] })
                  .addTo(group);
              }
              continue;
            }
            L.geoJSON(feature, { style: { color: SPATIAL_STYLE[type] ?? "#475569", weight: 1.2, fillColor: SPATIAL_STYLE[type] ?? "#475569", fillOpacity: 0.18 } }).addTo(group);
          }
          // A raster has no vector features: show where it sits instead.
          const bounds = data?.bounds;
          if (data?.kind === "raster" && Array.isArray(bounds) && bounds.every((value) => Number.isFinite(value)))
            L.rectangle([[bounds[1], bounds[0]], [bounds[3], bounds[2]]], { color: "#7c3aed", weight: 1.4, fillColor: "#7c3aed", fillOpacity: 0.12 })
              .bindTooltip("栅格覆盖范围", { direction: "top", offset: [0, -5] })
              .addTo(group);
          if (Array.isArray(bounds) && bounds.every((value) => Number.isFinite(value)))
            instance.fitBounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]], { animate: false, padding: [16, 16], maxZoom: 9 });
        };
        const located = state.data?.featureCount ?? 0;
        const types = Object.entries(state.data?.geometryTypes ?? {});
        const columns = Object.keys(state.data?.features?.[0]?.properties ?? {}).slice(0, 6);
        const table = state.data?.features?.length > 0
          ? h("table", { className: "geo-spatial-table" },
              h("thead", null, h("tr", null, h("th", { key: "#" }, "#"), ...columns.map((key) => h("th", { key }, key)))),
              h("tbody", null, ...state.data.features.slice(0, 12).map((feature, index) => h("tr", { key: index },
                h("td", { key: "#" }, index + 1),
                ...columns.map((key) => h("td", { key }, String(feature.properties?.[key] ?? "")))))))
          : null;
        const chips = state.files.length
          ? state.files.map((file) => h("button", { key: file.path, type: "button", className: state.selected === file.path ? "on" : "", title: file.path, onClick: () => void openFile(file) }, `${file.scope === "inputs" ? "上传 · " : ""}${file.path.split("/").pop()} · ${formatBytes(file.size)}${file.sidecars ? ` · 附 ${file.sidecars} 个配套文件` : ""}`))
          : [h("span", { key: "empty" }, "没有可预览的空间数据文件")];
        const isRaster = state.data?.kind === "raster";
        const report = state.data
          ? h("div", { className: "geo-monitor-report" },
              h("p", { className: "geo-monitor-brief" }, isRaster
                ? `${state.selected} · ${state.data.width}×${state.data.height} 像元 · ${state.data.bands} 波段`
                : `${state.selected} · ${located} 个要素${state.data.returned < located ? `（地图只绘制前 ${state.data.returned} 个）` : ""}`),
              isRaster && state.data.preview && h("img", { className: "geo-spatial-raster", src: state.data.preview, alt: state.selected, loading: "lazy" }),
              isRaster && h("p", { className: "geo-monitor-note" }, h("b", null, "栅格信息："), `${state.data.sampleFormat} ${state.data.bitsPerSample} 位 · ${state.data.compression} · ${state.data.crs}`),
              isRaster && state.data.valueRange && h("p", { className: "geo-monitor-note" }, h("b", null, "渲染拉伸："), `${state.data.valueRange[0].toFixed(2)} – ${state.data.valueRange[1].toFixed(2)}（2%–98% 分位）`),
              isRaster && state.data.note && h("p", { className: "geo-monitor-note" }, state.data.note),
              !isRaster && types.length > 0 && h("p", { className: "geo-monitor-note" }, h("b", null, "几何类型："), types.map(([type, count]) => `${SPATIAL_LABEL[type] ?? type} ${count}`).join(" · ")),
              !isRaster && state.data.format === "shapefile" && h("p", { className: "geo-monitor-note" }, `Shapefile（${state.data.shapeType}）· 配套文件 ${state.data.sidecars?.length ?? 0} 种随 .shp 一起下载`),
              state.data.bounds && h("p", { className: "geo-monitor-note" }, h("b", null, "范围："), state.data.bounds.map((value) => value.toFixed(3)).join(", ")),
              state.data.kind === "csv" && h("p", { className: "geo-monitor-note" }, h("b", null, "坐标列："), `${state.data.lonKey} / ${state.data.latKey}`),
              state.data.reason && h("p", { className: "geo-monitor-note" }, state.data.reason, state.data.keys?.length ? `（字段：${state.data.keys.join("、")}）` : ""),
              state.data.note && !isRaster && h("p", { className: "geo-monitor-note" }, state.data.note),
              table)
          : null;
        return h("div", { className: "geo-native-spatial" },
          h("div", { className: "geo-native-toolbar" }, h("h2", null, "空间数据"),
            state.chatId && h("span", { role: "status", className: "geo-monitor-status" }, `${state.files.length} 个可预览文件`),
            state.chatId && button("刷新", icons.IconRefreshOutline16, () => { current.current = null; }, { iconOnly: true })),
          !state.chatId && h("p", { className: "geo-monitor-note" }, "先选择一个研究对话；本标签显示该对话工作区与项目上传（inputs/）内的 GeoJSON / JSON / CSV。"),
          state.chatId && h("div", { className: "geo-monitor-sources" }, h("b", { className: "geo-monitor-sources-title" }, "文件"), ...chips),
          h("div", { className: "geo-native-map", ref: mapElement, "aria-label": "空间数据地图" }),
          state.error && h("p", { role: "alert" }, state.error),
          state.busy && h("p", null, "正在读取…"),
          report);
      }
      function Briefs() {
        const [data, change] = React.useState(null), [error, problem] = React.useState("");
        React.useEffect(() => { let live = true; const update = () => api("/monitor/events").then((value) => live && change(value)).catch((e) => live && problem(e.message)); update(); const timer = setInterval(update, 60000); return () => { live = false; clearInterval(timer); }; }, []);
        const items = (data?.items ?? []).filter((item) => item.brief?.summary);
        return h("div", { className: "geo-native-briefs" },
          h("div", { className: "geo-native-toolbar" }, h("h2", null, "监测简报"),
            data && h("span", { role: "status", className: "geo-monitor-status" }, `${items.length} 条简报 · 共 ${(data.items ?? []).length} 条线索${data.stale ? " · 数据待更新" : ""}`)),
          error && h("p", { role: "alert" }, error), !data && !error && h("p", null, "正在加载监测简报…"),
          data && items.length === 0 && h("p", { className: "geo-monitor-note" }, "本轮尚未生成简报；来源字段仍可在全球事件监测标签查看。"),
          ...items.map((item) => {
            const style = monitorType(item.type);
            const level = MONITOR_LEVELS[item.level] ? item.level : null;
            return h("article", { key: item.id, "data-severity": item.severity, "data-level": level ?? undefined, className: "geo-native-event" },
              h("div", { className: "geo-monitor-event-head" },
                h("span", { className: "geo-monitor-badge", style: { background: style.color } }, style.label),
                h("strong", null, item.displayTitle ?? item.title),
                level && h("span", { className: `geo-monitor-sev level-${level}`, title: MONITOR_LEVELS[level].hint }, MONITOR_LEVELS[level].label)),
              h("small", null, `${item.source} · ${monitorTime(item.publishedAt)}${item.displayLocation ? " · " + item.displayLocation : ""}`),
              h("div", { className: "geo-monitor-report" },
                h("p", { className: "geo-monitor-brief" }, item.brief.summary),
                item.brief.facts?.length > 0 && h("ul", { className: "geo-monitor-facts" }, ...item.brief.facts.map((fact, index) => h("li", { key: index }, fact))),
                item.brief.significance && h("p", { className: "geo-monitor-note" }, h("b", null, "关注点："), item.brief.significance),
                item.brief.uncertainty && h("p", { className: "geo-monitor-note" }, h("b", null, "待核实："), item.brief.uncertainty)),
              h("div", { className: "geo-monitor-actions" },
                h("a", { href: item.url, target: "_blank", rel: "noopener noreferrer" }, "查看来源"),
                h("a", { href: `/geo/api/monitor/report?eventId=${encodeURIComponent(item.id)}` }, "下载 Markdown"),
                button("带入研究对话", icons.IconNewChatOutline16, async () => { const current = list.getSnapshot().current, id = rootFor(current); if (!id) throw new Error("请先选择研究对话。"); if (current !== id) open(id); const result = await api(`/chats/${id}/monitor-context`, "POST", { eventId: item.id }); const input = ctx.get("conversation").input; const shell = input.for(binding(id).ctx); shell.setDraft(`请核验这条全球事件监测线索：${item.displayTitle ?? item.title}\n来源文件：${result.path}\n先区分已确认事实和待核验信息。`); set({ panel: null }); })));
          }));
      }
      function ReleasePanel() {
        const [data, update] = React.useState(null), [error, problem] = React.useState(""), [busy, working] = React.useState(false);
        const [model, setModel] = React.useState(""), [monitor, setMonitor] = React.useState(true), [edited, edit] = React.useState(false), [roles, setRoles] = React.useState({}), [appearance, setAppearance] = React.useState(null);
        React.useEffect(() => {
          let live = true;
          const refresh = () => api("/admin/releases").then((value) => { if (live) { update(value); problem(""); } }).catch((e) => { if (live) problem(e.message); });
          refresh(); const timer = setInterval(refresh, 2500);
          return () => { live = false; clearInterval(timer); };
        }, []);
        React.useEffect(() => { if (data && !edited) { setModel(data.product.defaultModel.model); setMonitor(data.product.monitorEnabled); setRoles(data.roleTools); setAppearance(data.product.appearance || null); } }, [data, edited]);
        const act = (operation, body) => async () => { working(true); problem(""); try { const result = await api("/admin/releases/" + operation, "POST", body); if (result.url) window.open(result.url, "_blank", "noopener,noreferrer"); update(await api("/admin/releases")); if (operation === "product") edit(false); } catch (e) { problem(e.message); } finally { working(false); } };
        const status = { validating: "正在验证", validated: "验证通过", failed: "验证失败" };
        return h("div", { className: "geo-release-panel" }, h("h2", null, "产品配置与发布"),
          button("解锁开发与发布", icons.IconSettingsOutline16, () => set({ panel: "development", developmentTarget: "releases" })),
          button("应用开发设置到草稿", icons.IconSettingsOutline16, async () => { working(true); try { const draft = await api("/admin/development/draft", "POST"); if (draft.defaultModel && draft.defaultModel.provider !== "deepseek-official") throw new Error("该模型提供方尚未接入普通版，未导入设置"); if (draft.defaultModel) setModel(draft.defaultModel.model); setAppearance(draft.appearance); edit(true); problem(""); } catch (error) { problem(error.message); } finally { working(false); } }, { disabled: busy || Boolean(data?.pending) }),
          error && h("p", { role: "alert" }, error), !data ? h("p", { role: "status" }, "正在读取版本…") : h(React.Fragment, null,
            h("dl", null, h("dt", null, "正式版本"), h("dd", null, data.active ?? "尚未建立快照"), h("dt", null, "开发目录"), h("dd", null, data.sourceRoot)),
            h("label", null, "用户默认模型", h("input", { value: model, onChange: (e) => { setModel(e.target.value); edit(true); }, disabled: busy || Boolean(data.pending) })),
            appearance && h("p", { role: "status" }, `待保存外观：${appearance.skin} · ${appearance.scheme === "dark" ? "深色" : "浅色"} · ${appearance.fontSize}px${appearance.wallpaper ? " · 包含图片背景" : " · 无图片背景"}`),
            h("label", { className: "geo-release-toggle" }, h("input", { type: "checkbox", checked: monitor, onChange: (e) => { setMonitor(e.target.checked); edit(true); }, disabled: busy || Boolean(data.pending) }), "全球事件监测"),
            h("details", null, h("summary", null, "角色工具"), ...Object.entries(roles).map(([role, assigned]) => h("fieldset", { key: role }, h("legend", null, { 数据助手: "数据助手", 分析助手: "分析助手", 事件助手: "事件助手", NTL_Data_Searcher: "数据助手", NTL_Analyst: "分析助手", NTL_Event_Tracker: "事件助手" }[role]),
              ...(data.availableTools ?? [...new Set(Object.values(data.roleTools).flat())].sort()).map((tool) => h("label", { className: "geo-release-toggle", key: tool }, h("input", { type: "checkbox", checked: assigned.includes(tool), disabled: busy || Boolean(data.pending), onChange: (e) => { setRoles({ ...roles, [role]: e.target.checked ? [...assigned, tool] : assigned.filter((name) => name !== tool) }); edit(true); } }), tool))))),
            button("保存产品配置", icons.IconSettingsOutline16, act("product", { schema: "geosentinel.product.v1", defaultModel: { provider: "deepseek-official", model }, monitorEnabled: monitor, roleTools: roles, ...(appearance ? { appearance } : {}) }), { disabled: !edited || busy || Boolean(data.pending) }),
            h("details", null, h("summary", null, "产品插件"), ...data.plugins.map((plugin) => h("div", { className: "geo-release-history", key: plugin.name }, h("code", null, plugin.name), h("span", null, plugin.version)))),
            h("h3", null, "待发布变更 · " + data.changes.length),
            h("div", { className: "geo-release-diff" }, ...data.changes.map((item) => h("div", { key: item.path }, h("span", null, { added: "新增", modified: "修改", removed: "删除" }[item.kind]), h("code", null, item.path)))),
            data.candidate && h("p", { role: "status" }, "候选 " + data.candidate.id + " · " + status[data.candidate.status]),
            data.candidate?.error && h("details", null, h("summary", null, "验证错误"), h("pre", null, data.candidate.error)),
            data.lastError && h("p", { role: "alert" }, data.lastError),
            h("div", { className: "geo-release-actions" },
              button(data.preparing ? "正在验证…" : "验证并生成版本", icons.IconRefreshOutline16, act("prepare"), { disabled: busy || data.preparing || edited || Boolean(data.pending) }),
              button("预览用户版", icons.IconGlobeOutline14, act("preview", { id: data.candidate?.id }), { disabled: busy || data.candidate?.status !== "validated" || Boolean(data.pending) }),
              button("确认同步发布", icons.IconPlayOutline16, act("publish", { id: data.candidate?.id }), { disabled: busy || !data.supervised || data.candidate?.status !== "validated" || !data.candidate?.previewed || Boolean(data.pending) })),
            data.previewUrl && h("a", { href: data.previewUrl, target: "_blank", rel: "noopener noreferrer" }, "打开候选版本预览"),
            data.pending && h("div", { role: "status" }, h("p", null, data.pending.phase === "switching" ? "正在切换正式版本" : "等待当前研究结束，暂不接收新任务"),
              button("取消本次发布", icons.IconCloseOutline16, act("cancel"), { disabled: busy || data.pending.phase === "switching" })),
            h("h3", null, "发布记录"), ...data.history.map((entry, index) => h("div", { className: "geo-release-history", key: entry.id + ":" + index }, h("span", null, entry.id + " · " + entry.by + " · " + new Date(entry.at).toLocaleString()),
              entry.id !== data.active && button("回滚到此版本", icons.IconRefreshOutline16, act("rollback", { id: entry.id }), { disabled: busy || Boolean(data.pending) })))));
      }
      ctx.inject(["betterSidebar"], (inner) => {
        inner.effect(() => inner.betterSidebar.registerTab({ id: "geosentinel:monitor", title: "全球事件监测", single: true, order: 0, icon: h(icons.IconGlobeOutline14), component: () => h(Monitor) }));
        inner.effect(() => inner.betterSidebar.registerTab({ id: "geosentinel:briefs", title: "监测简报", single: true, order: 1, icon: h(icons.IconListPenOutline16), component: () => h(Briefs) }));
        inner.effect(() => inner.betterSidebar.registerTab({ id: "geosentinel:spatial", title: "空间数据", single: true, order: 2, icon: h(icons.IconDataOutline16), component: () => h(Spatial) }));
        // Files use better-sidebar's own explorer, fed by the fenced read-only
        // sidebar adapter, so there is no second, worse file list to maintain.
      });
      // From 0.1.5 the native sidebar family owns the single `sidebar` slot, and the
      // native chat itself waits for its `sidebarRight` service, so this overlay only
      // registers the product's own sidebar on the older line (the host tells us which
      // build this is). On the native line the product contributes at the two positions
      // that slot shell declares for exactly this purpose: `sidebar.workspaces` (single,
      // the session-list region — no other registrar in this bundle, because its native
      // owner `ui-workspace` needs the closed directory picker) and
      // `sidebar.footer.action` (list, the tool entries that the retired better-sidebar
      // tabs used to carry). Reusing those positions is what keeps project/chat
      // navigation and the 监测简报 / 空间数据 entries on 0.1.5.
      if (!globalThis.__GEOSENTINEL_NATIVE_SIDEBAR__) ctx.slots.inject("sidebar", () => ctx.slots.register({ name: "sidebar" }, Sidebar));
      else {
        ctx.slots.inject("sidebar.workspaces", () => ctx.slots.register({ name: "sidebar.workspaces" }, ProjectTree));
        ctx.slots.inject("sidebar.footer.action", () => ctx.slots.register({ name: "sidebar.footer.action", id: "geo-entries" }, ProductEntries));
        // The native sidebar's brand row is two slots whose fallbacks are the DSH fish
        // logo and the "本地构建 <version>" label. The product's own sidebar used to
        // carry the product identity in that corner, so both positions are filled here
        // instead of leaving a vendor build label in a product shell.
        ctx.slots.inject("sidebar.brand.mark", () => ctx.slots.register({ name: "sidebar.brand.mark" }, ({ size = 24 }) =>
          h("svg", { width: size, height: size, viewBox: "0 0 24 24", fill: "none", "aria-hidden": "true" },
            h("circle", { cx: 12, cy: 12, r: 9, stroke: "currentColor", strokeWidth: 1.6 }),
            h("path", { d: "M3 12h18M12 3c3.2 3.6 3.2 14.4 0 18M12 3c-3.2 3.6-3.2 14.4 0 18", stroke: "currentColor", strokeWidth: 1.2 }))));
        ctx.slots.inject("sidebar.brand.name", () => ctx.slots.register({ name: "sidebar.brand.name" }, () =>
          h("span", { className: "geo-native-brand" }, "地缘环境智能计算平台")));
      }
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
      ctx.slots.inject("shell.overlay", () => ctx.slots.register({ name: "shell.overlay", id: "geo-development-workspace", order: -10 }, DevelopmentWorkspace));
      ctx.slots.inject("conversation.input.dock", () => ctx.slots.register({ name: "conversation.input.dock", id: "geo-plan", order: -10 }, Plan));
      // Uploads use the installed dsh-file-upload dock (paperclip, drag & drop,
      // file cards, per-session `.dsh-uploads/`), so the product's own upload
      // control is gone: one upload entry, one storage layout.
      // Replace only the child composer seat; history, catalog and lineage stay native.
      ctx.slots.inject("conversation.composer", () => ctx.slots.register({ name: "conversation.composer", id: "geo-child-readonly", priority: 100,
        select: (owner) => owner.session?.subagent ? { address: owner.session.subagent.address } : null },
      ({ matched }) => h("div", { className: "geo-native-child-readonly" }, h("span", { role: "status" }, "子智能体运行记录 · 只读"),
        button("返回主对话", icons.IconNewChatOutline16, () => open(matched.address.parentSessionId)))));
      ctx.slots.inject("conversation.hero.brand.mark", () => ctx.slots.register({ name: "conversation.hero.brand.mark" }, () => h("h1", { className: "geo-native-hero" }, h("span", null, "地缘环境"), h("span", null, "智能计算平台"))));
      ctx.slots.inject("conversation.session.header.utilities", () => ctx.slots.register({ name: "conversation.session.header.utilities", id: "geo-monitor" }, () => button("全球事件监测", icons.IconGlobeOutline14, () => openResearchTab("monitor"))));
      api("/auth/status").then(async ({ user, preview }) => { set({ user, checking: false, previewMode: preview === true }); if (user) { await refresh(); if (user.admin && typeof location !== "undefined" && ["#releases", "#development"].includes(location.hash)) set({ panel: location.hash.slice(1), developmentTarget: "development" }); } }).catch((error) => set({ checking: false, error: error.message }));
      ctx.on("dispose", () => { clear(); });
    }
    return { inject: ["slots"], apply };
  },
});
