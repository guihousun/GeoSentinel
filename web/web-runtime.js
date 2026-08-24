(() => {
  "use strict";

  const API_ROOT = "/api";
  const state = {
    user: null,
    threads: [],
    threadId: "",
    modelName: "deepseek-v4-flash",
    runId: "",
    eventSource: null,
    currentTarget: "main",
    monitorTimer: null,
  };

  const AGENTS = {
    NTL_Engineer: "地缘分析师",
    Data_Searcher: "数据检索助手",
    NTL_Knowledge_Base: "知识库助手",
    Knowledge_Base_Searcher: "知识库助手",
    Code_Assistant: "代码助手",
  };

  const $ = (selector, root = document) => root.querySelector(selector);
  const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];

  const PREVIEW_THEME_KEY = "geointer:preview-theme";
  const PREVIEW_THEMES = {
    default: "当前样式",
    "fiori-shell": "Fiori 双层外壳",
    "nautical-light": "海图蓝白",
    "satellite-gray": "卫星灰",
    "cool-slate": "冷灰蓝",
    "split-workbench": "双层工作台",
  };

  async function api(path, options = {}) {
    const headers = new Headers(options.headers || {});
    headers.set("Accept", "application/json");
    if (options.body && !headers.has("Content-Type")) headers.set("Content-Type", "application/json");
    const response = await fetch(`${API_ROOT}${path}`, { credentials: "same-origin", ...options, headers });
    let payload = {};
    try {
      payload = await response.json();
    } catch (_) {
      payload = {};
    }
    if (!response.ok) {
      throw new Error(payload?.error?.message || payload?.detail || "请求未成功，请稍后重试。");
    }
    return payload;
  }

  function cloneWithoutListeners(element) {
    if (!element) return null;
    const clone = element.cloneNode(true);
    element.replaceWith(clone);
    return clone;
  }

  function taskTitle(thread) {
    return String(thread?.thread_title || thread?.last_question || "").trim() || "未命名研究任务";
  }

  function timeText(value) {
    const date = new Date(Number(value) * 1000);
    return value && !Number.isNaN(date.getTime())
      ? date.toLocaleString("zh-CN", { hour: "2-digit", minute: "2-digit" })
      : "刚刚";
  }

  function refreshIcons() {
    if (window.lucide) window.lucide.createIcons();
  }

  function monitorSeverityLabel(value) {
    return ({ high: "高关注", medium: "持续跟踪", low: "观察" })[String(value || "").toLowerCase()] || "待核验";
  }

  function monitorTime(value) {
    const stamp = Number(value || 0);
    if (!stamp) return "时间待核验";
    const delta = Math.max(0, Math.floor(Date.now() / 1000) - stamp);
    if (delta < 3600) return `${Math.max(1, Math.floor(delta / 60))} 分钟前`;
    if (delta < 86400) return `${Math.floor(delta / 3600)} 小时前`;
    return `${Math.floor(delta / 86400)} 天前`;
  }

  function renderMonitorTicker(items) {
    const track = $("#monitor-ticker-track");
    if (!track) return;
    track.replaceChildren();
    const labels = items.length ? [...items.slice(0, 6), ...items.slice(0, 6)].map((item) => item.display_title || item.title || "未命名监测事件") : ["等待事件监测服务完成首轮同步"];
    labels.forEach((text) => {
      const label = document.createElement("span");
      label.textContent = text;
      track.append(label);
    });
  }

  function renderMonitorEvents(items, monitorStatus = {}) {
    const list = $("#monitor-event-list");
    const status = $("#monitor-source-state");
    if (status) {
      const lastRun = monitorStatus.last_run || {};
      status.textContent = monitorStatus.running
        ? "正在同步"
        : (lastRun.updated_at ? `已同步 · ${monitorTime(lastRun.updated_at)}` : (monitorStatus.enabled ? "等待首轮同步" : "监测未启用"));
    }
    renderMonitorTicker(items);
    if (!list) return;
    list.replaceChildren();
    if (!items.length) {
      const empty = document.createElement("p");
      empty.className = "monitor-empty";
      empty.textContent = monitorStatus.enabled ? "尚无已通过来源校验的监测事件。服务会在首轮同步完成后更新。" : "事件监测服务尚未启用；不会展示演示事件。";
      list.append(empty);
      return;
    }
    items.slice(0, 8).forEach((item, index) => {
      const article = document.createElement("article");
      article.className = `monitor-event ${String(item.severity || "low").toLowerCase()}`;
      const order = document.createElement("span");
      order.textContent = String(index + 1).padStart(2, "0");
      const copy = document.createElement("div");
      const title = document.createElement("strong");
      title.textContent = String(item.display_title || item.title || "未命名监测事件");
      const detail = document.createElement("small");
      detail.textContent = [item.source_name, item.display_location || item.location_name || item.country, monitorTime(item.published_at || item.updated_at)].filter(Boolean).join(" · ") || "来源和位置待核验";
      copy.append(title, detail);
      const question = Array.isArray(item.research_questions) ? item.research_questions[0] : "";
      if (question) {
        const button = document.createElement("button");
        button.type = "button";
        button.dataset.eventTask = question;
        button.textContent = "进入研判";
        copy.append(button);
      }
      const badge = document.createElement("em");
      badge.textContent = monitorSeverityLabel(item.severity);
      article.append(order, copy, badge);
      list.append(article);
    });
  }

  async function loadMonitorEvents() {
    const payload = await api("/monitor/events");
    const items = Array.isArray(payload.items) ? payload.items : [];
    const monitorStatus = payload.status || {};
    renderMonitorEvents(items, monitorStatus);
    window.dispatchEvent(new CustomEvent("geointer:monitor-events", { detail: { items, status: monitorStatus } }));
  }

  function bindMonitorQueue() {
    const list = cloneWithoutListeners($("#monitor-event-list"));
    list?.addEventListener("click", (event) => {
      const button = event.target.closest("[data-event-task]");
      if (!button) return;
      const input = $("#chat-input");
      if (input) {
        input.value = button.dataset.eventTask || "";
        input.focus();
      }
    });
  }


  function updateRailToggle(side, collapsed) {
    const button = $(`#${side}-rail-toggle`);
    if (!button) return;
    const isLeft = side === "left";
    const direction = isLeft ? "左侧导航栏" : "右侧监测栏";
    const iconName = isLeft
      ? (collapsed ? "panel-left-open" : "panel-left-close")
      : (collapsed ? "panel-right-open" : "panel-right-close");
    button.replaceChildren();
    const icon = document.createElement("i");
    icon.setAttribute("data-lucide", iconName);
    button.append(icon);
    button.setAttribute("aria-expanded", String(!collapsed));
    button.setAttribute("aria-label", `${collapsed ? "展开" : "收起"}${direction}`);
    button.title = `${collapsed ? "展开" : "收起"}${direction}`;
    refreshIcons();
  }

  function setRailCollapsed(side, collapsed, persist = true) {
    const shell = $(".app-shell");
    if (!shell) return;
    shell.classList.toggle(`${side}-rail-collapsed`, collapsed);
    if (persist) {
      try {
        window.localStorage.setItem(`geointer:${side}-rail-collapsed`, String(collapsed));
      } catch (_) {
        // Layout should remain usable even when local storage is unavailable.
      }
    }
    updateRailToggle(side, collapsed);
    window.requestAnimationFrame(() => window.dispatchEvent(new Event("resize")));
    window.setTimeout(() => window.dispatchEvent(new Event("resize")), 220);
  }

  function bindRailControls() {
    ["left", "right"].forEach((side) => {
      const button = cloneWithoutListeners($(`#${side}-rail-toggle`));
      let collapsed = false;
      try {
        collapsed = window.localStorage.getItem(`geointer:${side}-rail-collapsed`) === "true";
      } catch (_) {
        collapsed = false;
      }
      setRailCollapsed(side, collapsed, false);
      button?.addEventListener("click", () => {
        const shell = $(".app-shell");
        setRailCollapsed(side, !shell?.classList.contains(`${side}-rail-collapsed`));
      });
    });
  }

  function applyPreviewTheme(themeId, persist = true) {
    const nextTheme = Object.hasOwn(PREVIEW_THEMES, themeId) ? themeId : "default";
    document.documentElement.dataset.previewTheme = nextTheme;
    const current = $("#theme-preview-current");
    if (current) current.textContent = PREVIEW_THEMES[nextTheme];
    $$('[data-preview-theme]').forEach((option) => {
      option.setAttribute("aria-checked", String(option.dataset.previewTheme === nextTheme));
    });
    if (persist) {
      try {
        window.localStorage.setItem(PREVIEW_THEME_KEY, nextTheme);
      } catch (_) {
        // Theme preview remains available even when local storage is unavailable.
      }
    }
    window.requestAnimationFrame(() => window.dispatchEvent(new Event("resize")));
  }

  function bindThemePreview() {
    const toggle = cloneWithoutListeners($("#theme-preview-toggle"));
    const menu = $("#theme-preview-menu");
    if (!toggle || !menu) return;

    let savedTheme = "default";
    try {
      savedTheme = window.localStorage.getItem(PREVIEW_THEME_KEY) || "default";
    } catch (_) {
      savedTheme = "default";
    }
    applyPreviewTheme(savedTheme, false);

    const closeMenu = (restoreFocus = false) => {
      menu.hidden = true;
      toggle.setAttribute("aria-expanded", "false");
      if (restoreFocus) toggle.focus();
    };
    const openMenu = () => {
      menu.hidden = false;
      toggle.setAttribute("aria-expanded", "true");
      menu.querySelector('[aria-checked="true"]')?.focus();
    };

    toggle.addEventListener("click", () => {
      if (menu.hidden) openMenu();
      else closeMenu();
    });
    menu.addEventListener("click", (event) => {
      const option = event.target.closest("[data-preview-theme]");
      if (!option) return;
      applyPreviewTheme(option.dataset.previewTheme);
      closeMenu(true);
    });
    document.addEventListener("click", (event) => {
      if (!menu.hidden && !event.target.closest(".theme-preview-control")) closeMenu();
    });
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && !menu.hidden) closeMenu(true);
    });
  }

  function setAuthNotice(message = "", tone = "") {
    const notice = $("#auth-notice");
    if (!notice) return;
    notice.textContent = message;
    notice.dataset.tone = tone;
  }

  function setAuthGate(visible) {
    const gate = $("#auth-gate");
    const shell = $(".app-shell");
    const themeControl = $(".theme-preview-control");
    const topbarActions = $(".topbar-actions");
    const exportButton = $("#export-brief");
    if (gate) gate.hidden = !visible;
    document.body.classList.toggle("auth-active", visible);
    if (themeControl) {
      themeControl.classList.toggle("theme-preview-auth-position", visible);
      if (visible && gate) gate.append(themeControl);
      else if (topbarActions) topbarActions.insertBefore(themeControl, exportButton || null);
    }
    if (shell) {
      shell.inert = visible;
      shell.setAttribute("aria-hidden", String(visible));
    }
  }

  function setRuntimeNotice(message = "", tone = "") {
    const notice = $("#runtime-notice");
    if (!notice) return;
    notice.textContent = message;
    notice.dataset.tone = tone;
    notice.hidden = !message;
  }

  function clearMessages(targetId) {
    const stream = $(`#${targetId}`);
    if (stream) stream.replaceChildren();
  }

  function cleanInlineText(value) {
    return String(value || "")
      .replace(/\*\*(.*?)\*\*/g, "$1")
      .replace(/`([^`]+)`/g, "$1")
      .trim();
  }

  function appendMessageBlocks(container, text) {
    const lines = String(text || "").replace(/\r\n?/g, "\n").split("\n");
    let index = 0;
    const isListLine = (line) => /^(?:[-*•]\s+|\d+[.)]\s+)/.test(line.trim());
    const isStructuralLine = (line) => /^```/.test(line.trim()) || /^#{1,3}\s+/.test(line.trim()) || isListLine(line);

    while (index < lines.length) {
      const raw = lines[index];
      const line = raw.trim();
      if (!line) {
        index += 1;
        continue;
      }
      if (/^```/.test(line)) {
        const code = [];
        index += 1;
        while (index < lines.length && !/^```/.test(lines[index].trim())) {
          code.push(lines[index]);
          index += 1;
        }
        if (index < lines.length) index += 1;
        const pre = document.createElement("pre");
        pre.textContent = code.join("\n");
        container.append(pre);
        continue;
      }
      const heading = line.match(/^#{1,3}\s+(.+)$/);
      if (heading) {
        const title = document.createElement("h5");
        title.textContent = cleanInlineText(heading[1]);
        container.append(title);
        index += 1;
        continue;
      }
      if (isListLine(line)) {
        const ordered = /^\d+[.)]\s+/.test(line);
        const list = document.createElement(ordered ? "ol" : "ul");
        while (index < lines.length) {
          const candidate = lines[index].trim();
          const matchesType = ordered ? /^\d+[.)]\s+/.test(candidate) : /^[-*•]\s+/.test(candidate);
          if (!matchesType) break;
          const item = document.createElement("li");
          item.textContent = cleanInlineText(candidate.replace(ordered ? /^\d+[.)]\s+/ : /^[-*•]\s+/, ""));
          list.append(item);
          index += 1;
        }
        container.append(list);
        continue;
      }

      const paragraph = [];
      while (index < lines.length) {
        const candidate = lines[index].trim();
        if (!candidate || isStructuralLine(candidate)) break;
        paragraph.push(candidate);
        index += 1;
      }
      const node = document.createElement("p");
      node.textContent = cleanInlineText(paragraph.join(" "));
      container.append(node);
    }
  }

  function addMessage(targetId, role, text, { author = "地缘分析师", timestamp = "刚刚", tags = [] } = {}) {
    const stream = $(`#${targetId}`);
    if (!stream) return;
    stream.querySelector(".chat-empty-state")?.remove();
    const article = document.createElement("article");
    article.className = `message ${role === "user" ? "user" : "assistant"}`;
    const avatar = document.createElement("span");
    avatar.className = `message-avatar ${role === "user" ? "human" : "ai"}`;
    avatar.textContent = role === "user" ? "HY" : "AI";
    const body = document.createElement("div");
    const meta = document.createElement("div");
    meta.className = "message-meta";
    const name = document.createElement("strong");
    name.textContent = role === "user" ? "研究者" : author;
    const time = document.createElement("time");
    time.textContent = timestamp;
    meta.append(name, time);
    const content = document.createElement("div");
    content.className = "message-content";
    appendMessageBlocks(content, text);
    body.append(meta, content);
    if (tags.length) {
      const tagRow = document.createElement("div");
      tagRow.className = "message-tags";
      tags.forEach((tag) => {
        const item = document.createElement("span");
        item.textContent = tag;
        tagRow.append(item);
      });
      body.append(tagRow);
    }
    article.append(avatar, body);
    stream.append(article);
    stream.scrollTop = stream.scrollHeight;
  }

  function renderHistory(records, targetId) {
    clearMessages(targetId);
    const visible = (records || []).filter(
      (record) => ["user", "assistant"].includes(record.role) && String(record.content || "").trim(),
    );
    if (!visible.length) {
      const stream = $(`#${targetId}`);
      if (!stream) return;
      const empty = document.createElement("div");
      empty.className = "chat-empty-state";
      const title = document.createElement("strong");
      title.textContent = "开始一项研究";
      const copy = document.createElement("p");
      copy.textContent = "输入研究问题，地缘分析师将根据任务需要调度数据、空间和证据工具。";
      empty.append(title, copy);
      stream.append(empty);
      return;
    }
    visible.forEach((record) => addMessage(targetId, record.role, record.content, { timestamp: timeText(record.ts) }));
  }

  function updateThreadSelector() {
    const selector = $("#thread-selector");
    if (!selector) return;
    selector.replaceChildren();
    state.threads.forEach((thread) => {
      const option = document.createElement("option");
      option.value = thread.thread_id;
      option.textContent = taskTitle(thread);
      option.selected = thread.thread_id === state.threadId;
      selector.append(option);
    });
    selector.disabled = !state.threads.length;
  }

  function updateTaskContext(thread) {
    const context = $("#workspace-context");
    if (context) context.textContent = `当前任务：${taskTitle(thread)}`;
    const accountName = state.user?.username || "未登录";
    const account = $("#account-name");
    const accountSummary = $("#account-summary-name");
    if (account) account.textContent = accountName;
    if (accountSummary) accountSummary.textContent = accountName;
  }

  function addActivity(text, tone = "") {
    const list = $("#run-activity-list");
    if (!list) return;
    const stream = $("#task-stream");
    if (stream) {
      stream.hidden = false;
      stream.dataset.state = "running";
    }
    list.querySelector(".run-activity-empty")?.remove();
    const item = document.createElement("li");
    item.dataset.tone = tone;
    const stamp = document.createElement("time");
    stamp.textContent = new Date().toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
    const copy = document.createElement("span");
    copy.textContent = text;
    item.append(stamp, copy);
    list.prepend(item);
    while (list.children.length > 8) list.lastElementChild?.remove();
  }

  function setAgentState(agentName, value, active = false) {
    const label = AGENTS[agentName] || agentName;
    $$(".run-agent-chip").filter((chip) => chip.dataset.agent === label).forEach((chip) => {
      chip.classList.toggle("active", active);
      const badge = $(".run-agent-state", chip);
      if (badge) badge.textContent = value;
    });
  }

  function resetAgents(done = false) {
    $$(".run-agent-chip").forEach((chip) => {
      chip.classList.remove("active");
      const badge = $(".run-agent-state", chip);
      if (!badge) return;
      badge.textContent = chip.dataset.agent === "地缘分析师" && done ? "已完成" : (chip.dataset.agent === "知识库助手" || chip.dataset.agent === "代码助手" ? "按需" : "待命");
    });
  }

  function setRunUi(running) {
    const statusNode = $("#thread-status span:last-child");
    if (statusNode) statusNode.textContent = running ? "任务运行中" : state.user ? "地缘分析师已就绪" : "登录后开始";
    const stream = $("#task-stream");
    const list = $("#run-activity-list");
    const hasActivity = Boolean(list?.querySelector("li:not(.run-activity-empty)"));
    if (stream) {
      stream.hidden = !running && !hasActivity;
      stream.dataset.state = running ? "running" : hasActivity ? "complete" : "idle";
    }
    const streamState = $("#run-activity-state");
    if (streamState) streamState.textContent = running ? "运行中" : hasActivity ? "本轮完成" : "等待任务";
    const stop = $("#stop-run");
    if (stop) stop.hidden = !running;
    $$("#chat-form textarea, #chat-form .send-button, #logistics-chat-form textarea, #logistics-chat-form .send-button").forEach((node) => {
      node.disabled = running || !state.user;
    });
  }

  function renderArtifacts(items) {
    const panel = $("#artifact-panel");
    const list = $("#artifact-list");
    const count = $("#artifact-count");
    const summaryCount = $("#artifact-summary-count");
    const exportButton = $("#export-brief");
    const artifacts = Array.isArray(items) ? items : [];
    if (count) count.textContent = String(artifacts.length);
    if (summaryCount) {
      summaryCount.textContent = String(artifacts.length);
      summaryCount.hidden = !artifacts.length;
    }
    if (exportButton) exportButton.disabled = !artifacts.length;
    if (!panel || !list) return;
    panel.hidden = !artifacts.length;
    list.replaceChildren();
    artifacts.slice(0, 8).forEach((item) => {
      const row = document.createElement("li");
      const link = document.createElement("a");
      const encodedPath = String(item.path || "").split("/").map(encodeURIComponent).join("/");
      link.href = `${API_ROOT}/threads/${encodeURIComponent(state.threadId)}/outputs/${encodedPath}`;
      link.textContent = item.name || item.path;
      link.title = item.path || item.name;
      const meta = document.createElement("small");
      meta.textContent = item.size_label || "文件";
      row.append(link, meta);
      list.append(row);
    });
  }

  function resetRunStream() {
    const list = $("#run-activity-list");
    if (list) {
      const empty = document.createElement("li");
      empty.className = "run-activity-empty";
      empty.textContent = "开始任务后，将在此显示实际的智能体和工具进度。";
      list.replaceChildren(empty);
    }
    const stream = $("#task-stream");
    if (stream) {
      stream.hidden = true;
      stream.dataset.state = "idle";
    }
    const streamState = $("#run-activity-state");
    if (streamState) streamState.textContent = "等待任务";
  }

  async function loadThread(threadId, { reconnect = true } = {}) {
    if (!threadId) return;
    const payload = await api(`/threads/${encodeURIComponent(threadId)}`);
    state.threadId = threadId;
    const thread = payload.thread || state.threads.find((item) => item.thread_id === threadId) || { thread_id: threadId };
    const index = state.threads.findIndex((item) => item.thread_id === threadId);
    if (index >= 0) state.threads[index] = { ...state.threads[index], ...thread };
    updateThreadSelector();
    updateTaskContext(thread);
    renderHistory(payload.messages, "chat-messages");
    renderHistory(payload.messages, "logistics-chat-messages");
    renderArtifacts(payload.artifacts);
    if (!payload.active_run) resetRunStream();
    if (reconnect && payload.active_run?.run_id) {
      state.runId = payload.active_run.run_id;
      setRunUi(true);
      openEventStream(payload.active_run.run_id);
    } else if (!payload.active_run) {
      state.runId = "";
      setRunUi(false);
    }
  }

  async function loadThreads({ createWhenEmpty = true } = {}) {
    const payload = await api("/threads");
    state.threads = payload.items || [];
    if (!state.threads.length && createWhenEmpty) {
      await createThread();
      return;
    }
    updateThreadSelector();
    if (state.threads.length) await loadThread(state.threadId || state.threads[0].thread_id);
  }

  async function createThread() {
    const payload = await api("/threads", { method: "POST", body: JSON.stringify({}) });
    const thread = payload.thread;
    state.threads.unshift(thread);
    state.threadId = thread.thread_id;
    updateThreadSelector();
    await loadThread(thread.thread_id, { reconnect: false });
    setRuntimeNotice("已建立新的研究任务。", "success");
  }

  function closeEventStream() {
    state.eventSource?.close();
    state.eventSource = null;
  }

  function handleRunEvent(event) {
    const kind = event.kind;
    const payload = event.payload || {};
    if (kind === "status") {
      addActivity("任务已启动，正在读取技能和可用工具。", "running");
      setAgentState("NTL_Engineer", "运行中", true);
      return;
    }
    if (kind === "reasoning_delta") {
      (payload.messages || []).forEach((message) => {
        if (message.role === "tool") {
          addActivity(`工具已完成：${message.tool_name || message.agent || "未命名工具"}`, "success");
          return;
        }
        if (message.agent && AGENTS[message.agent]) {
          setAgentState(message.agent, "工作中", true);
          addActivity(`${AGENTS[message.agent]}正在处理任务。`, "running");
        }
      });
      return;
    }
    if (kind === "reasoning_custom") {
      addActivity("收到运行进度更新。", "running");
      return;
    }
    if (["final_answer", "error", "interrupted", "no_final"].includes(kind)) {
      addMessage("chat-messages", "assistant", payload.text || "任务已结束。");
      if (state.currentTarget === "logistics") addMessage("logistics-chat-messages", "assistant", payload.text || "任务已结束。");
      const success = kind === "final_answer";
      addActivity(success ? "已形成研究结论。" : "任务以非完整状态结束。", success ? "success" : "warning");
      setRuntimeNotice(payload.text || "任务已结束。", success ? "success" : "warning");
      return;
    }
    if (kind === "warning") {
      addActivity(payload.text || "任务出现额外警告。", "warning");
      return;
    }
    if (kind === "artifacts") {
      renderArtifacts(payload.items || []);
      addActivity("已收集本次任务生成的文件。", "success");
      return;
    }
    if (kind === "done") {
      closeEventStream();
      state.runId = "";
      setRunUi(false);
      resetAgents(true);
    }
  }

  function openEventStream(runId) {
    if (!runId || state.eventSource?.dataset?.runId === runId) return;
    closeEventStream();
    const source = new EventSource(`${API_ROOT}/runs/${encodeURIComponent(runId)}/events`);
    source.dataset.runId = runId;
    source.addEventListener("run", (message) => {
      try {
        handleRunEvent(JSON.parse(message.data));
      } catch (_) {
        addActivity("运行事件格式异常。", "warning");
      }
    });
    source.onerror = () => {
      if (state.runId) setRuntimeNotice("运行连接短暂中断，正在尝试恢复。", "warning");
    };
    state.eventSource = source;
  }

  async function sendQuestion(text, target = "main") {
    const question = String(text || "").trim();
    if (!question) return;
    if (!state.threadId) await createThread();
    if (state.runId) {
      setRuntimeNotice("当前任务仍在运行。请等待完成或先停止任务。", "warning");
      return;
    }
    state.currentTarget = target;
    addMessage("chat-messages", "user", question);
    if (target === "logistics") addMessage("logistics-chat-messages", "user", question);
    setRunUi(true);
    resetAgents(false);
    setRuntimeNotice("正在启动研究任务。", "running");
    try {
      const run = await api(`/threads/${encodeURIComponent(state.threadId)}/runs`, {
        method: "POST",
        body: JSON.stringify({ question, model_name: state.modelName }),
      });
      state.runId = run.run_id;
      openEventStream(run.run_id);
    } catch (error) {
      setRunUi(false);
      resetAgents(false);
      addActivity(error.message, "error");
      setRuntimeNotice(error.message, "error");
    }
  }

  async function uploadSelectedFiles(files) {
    if (!state.threadId || !files?.length) return;
    for (const file of files) {
      setRuntimeNotice(`正在上传 ${file.name}。`, "running");
      const response = await fetch(`${API_ROOT}/threads/${encodeURIComponent(state.threadId)}/uploads/${encodeURIComponent(file.name)}`, {
        method: "PUT",
        credentials: "same-origin",
        headers: { "Content-Type": file.type || "application/octet-stream" },
        body: file,
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(payload?.error?.message || payload?.detail || `上传 ${file.name} 失败。`);
      addActivity(`已上传输入资料：${payload.name}`, "success");
    }
    setRuntimeNotice("资料已保存到当前任务工作区，智能体会在任务中读取相关文件。", "success");
  }

  function bindTaskControls() {
    const mainForm = cloneWithoutListeners($("#chat-form"));
    const mainInput = $("#chat-input", mainForm);
    mainForm?.addEventListener("submit", (event) => {
      event.preventDefault();
      const value = mainInput?.value || "";
      if (mainInput) mainInput.value = "";
      sendQuestion(value, "main");
    });
    mainInput?.addEventListener("keydown", (event) => {
      if (event.key === "Enter" && !event.shiftKey) {
        event.preventDefault();
        mainForm?.requestSubmit();
      }
    });

    const topicForm = cloneWithoutListeners($("#logistics-chat-form"));
    const topicInput = $("#logistics-chat-input", topicForm);
    topicForm?.addEventListener("submit", (event) => {
      event.preventDefault();
      const question = String(topicInput?.value || "").trim();
      if (!question) return;
      const node = $("#logistics-context-node")?.textContent?.trim() || "尚未选择节点";
      const layers = $("#logistics-layer-state")?.textContent?.trim() || "专题图层";
      if (topicInput) topicInput.value = "";
      sendQuestion(`${question}\n\n[粮食物流专题上下文]\n空间焦点：${node}\n当前图层：${layers}`, "logistics");
    });
    topicInput?.addEventListener("keydown", (event) => {
      if (event.key === "Enter" && !event.shiftKey) {
        event.preventDefault();
        topicForm?.requestSubmit();
      }
    });

    const newTask = cloneWithoutListeners($("#new-task"));
    newTask?.addEventListener("click", () => createThread().catch((error) => setRuntimeNotice(error.message, "error")));
    const stop = cloneWithoutListeners($("#stop-run"));
    stop?.addEventListener("click", async () => {
      if (!state.runId) return;
      try {
        await api(`/runs/${encodeURIComponent(state.runId)}/cancel`, { method: "POST" });
        setRuntimeNotice("已请求停止任务，将在当前步骤结束后中断。", "warning");
      } catch (error) {
        setRuntimeNotice(error.message, "error");
      }
    });
    const selector = cloneWithoutListeners($("#thread-selector"));
    selector?.addEventListener("change", () => loadThread(selector.value).catch((error) => setRuntimeNotice(error.message, "error")));

    const logout = cloneWithoutListeners($("#logout-button"));
    logout?.addEventListener("click", logoutAccount);
    const attach = cloneWithoutListeners($("#chat-form .composer-tool"));
    const uploadInput = $("#upload-input");
    attach?.addEventListener("click", () => uploadInput?.click());
    uploadInput?.addEventListener("change", async () => {
      try {
        await uploadSelectedFiles(uploadInput.files);
      } catch (error) {
        setRuntimeNotice(error.message, "error");
      }
      uploadInput.value = "";
    });

    const exportButton = cloneWithoutListeners($("#export-brief"));
    exportButton?.addEventListener("click", () => {
      const evidence = $("#rail-evidence");
      if (evidence) evidence.open = true;
      $("#artifact-panel")?.scrollIntoView({ behavior: "smooth", block: "nearest" });
    });
    bindMonitorQueue();
    $$('[data-logistics-prompt]').forEach((button) => {
      const next = cloneWithoutListeners(button);
      next?.addEventListener("click", () => {
        if (topicInput) {
          topicInput.value = next.dataset.logisticsPrompt || "";
          topicInput.focus();
        }
      });
    });
    $$(".agent-row").forEach((row) => cloneWithoutListeners(row));
  }

  function showAuthMode(mode) {
    $("#auth-login-form").hidden = mode !== "login";
    $("#auth-register-form").hidden = mode !== "register";
    $$('[data-auth-mode]').forEach((button) => {
      const active = button.dataset.authMode === mode;
      button.classList.toggle("active", active);
      button.setAttribute("aria-selected", String(active));
    });
    setAuthNotice("");
  }

  function bindAuth() {
    $$('[data-auth-mode]').forEach((button) => button.addEventListener("click", () => showAuthMode(button.dataset.authMode)));
    $("#auth-login-form")?.addEventListener("submit", async (event) => {
      event.preventDefault();
      const username = $("#login-username")?.value || "";
      const password = $("#login-password")?.value || "";
      try {
        setAuthNotice("正在验证账号。", "running");
        const payload = await api("/auth/login", { method: "POST", body: JSON.stringify({ username, password }) });
        await activateSession(payload.user);
      } catch (error) {
        setAuthNotice(error.message, "error");
      }
    });
    $("#auth-register-form")?.addEventListener("submit", async (event) => {
      event.preventDefault();
      const username = $("#register-username")?.value || "";
      const password = $("#register-password")?.value || "";
      try {
        setAuthNotice("正在创建账号。", "running");
        const payload = await api("/auth/register", { method: "POST", body: JSON.stringify({ username, password }) });
        await activateSession(payload.user);
      } catch (error) {
        setAuthNotice(error.message, "error");
      }
    });
  }

  async function activateSession(user) {
    state.user = user;
    setAuthGate(false);
    $("#logout-button").hidden = false;
    const accountPanel = $("#account-panel");
    if (accountPanel) accountPanel.open = false;
    $("#new-task").disabled = false;
    setRunUi(false);
    await loadThreads();
  }

  async function logoutAccount() {
    closeEventStream();
    try {
      await api("/auth/logout", { method: "POST" });
    } catch (_) {
      // The local view still needs to reset when the network request fails.
    }
    state.user = null;
    state.threads = [];
    state.threadId = "";
    state.runId = "";
    renderHistory([], "chat-messages");
    renderArtifacts([]);
    updateThreadSelector();
    setAuthGate(true);
    $("#logout-button").hidden = true;
    $("#new-task").disabled = true;
    const account = $("#account-name");
    const accountSummary = $("#account-summary-name");
    if (account) account.textContent = "未登录";
    if (accountSummary) accountSummary.textContent = "未登录";
    setRunUi(false);
    resetAgents(false);
    resetRunStream();
  }

  async function bootstrap() {
    bindThemePreview();
    bindRailControls();
    bindTaskControls();
    bindAuth();
    loadMonitorEvents().catch(() => renderMonitorEvents([], { enabled: false }));
    state.monitorTimer = window.setInterval(() => loadMonitorEvents().catch(() => {}), 60 * 1000);
    try {
      const me = await api("/me");
      if (me.authenticated) {
        state.modelName = me.models?.[0] || state.modelName;
        await activateSession(me.user);
      } else {
        setAuthGate(true);
        setRunUi(false);
      }
    } catch (error) {
      setAuthGate(true);
      setAuthNotice(`无法连接研究服务：${error.message}`, "error");
    }
    refreshIcons();
  }

  window.addEventListener("beforeunload", () => { if (state.monitorTimer) window.clearInterval(state.monitorTimer); });

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", bootstrap);
  else bootstrap();
})();
