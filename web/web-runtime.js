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
    eventSourceRunId: "",
    eventSeq: 0,
    eventReconnectAttempts: 0,
    eventReconnectTimer: null,
    agentStreams: new Map(),
    currentTarget: "main",
    monitorTimer: null,
    monitorEventsByKey: new Map(),
    recentCaseRequests: new Map(),
    pendingMonitorDraft: null,
    workspaceFiles: { inputs: [], outputs: [], files: [] },
    fileRootFilter: "all",
    usedThreads: new Set(),
    activeRightRailTab: "monitor",
    previewFile: null,
    previewRequestToken: 0,
    rightRailWidth: 356,
    workspaceMap: null,
    workspaceMapLayers: new Map(),
  };

  const CASE_REQUEST_COOLDOWN_MS = 10 * 1000;

  const AGENTS = {
    NTL_Engineer: "地缘分析师",
    NTL_Data_Searcher: "数据助手",
    Data_Searcher: "数据助手",
    NTL_Analyst: "分析助手",
    NTL_Event_Tracker: "事件助手",
  };

  const AGENT_ALIASES = {
    Data_Searcher: "NTL_Data_Searcher",
    Engineer: "NTL_Engineer",
    Analyst: "NTL_Analyst",
    Event_Tracker: "NTL_Event_Tracker",
  };
  const AGENT_STREAM_TEXT_LIMIT = 24000;

  const $ = (selector, root = document) => root.querySelector(selector);
  const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];

  const PREVIEW_THEME_KEY = "geointer:preview-theme";
  const RIGHT_RAIL_WIDTH_KEY = "geointer:right-rail-width";
  const RIGHT_RAIL_MIN_WIDTH = 300;
  const RIGHT_RAIL_MAX_WIDTH = 720;
  const RIGHT_RAIL_CHAT_MIN_WIDTH = 520;
  const LOCKED_PREVIEW_THEME = "cool-slate";
  const PREVIEW_THEMES = {
    "cool-slate": "冷灰蓝",
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
      const error = new Error(payload?.error?.message || payload?.detail || "请求未成功，请稍后重试。");
      error.code = payload?.error?.code || "request_failed";
      error.details = payload?.error?.details || {};
      throw error;
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

  function monitorCaseKey(item) {
    return String(
      item.dedup_key
      || item.candidate_id
      || item.source_url
      || `${item.title || "event"}:${item.published_at || item.updated_at || "unknown"}`,
    ).slice(0, 180);
  }

  function monitorResearchPrompt(item) {
    const title = String(item.display_title || item.title || "未命名监测事件").trim();
    const location = String(item.display_location || item.location_name || item.country || "相关区域").trim();
    const stamp = Number(item.published_at || item.updated_at || 0);
    const eventTime = stamp
      ? `${new Date(stamp * 1000).toLocaleString("zh-CN", { timeZone: "UTC", hour12: false })} UTC`
      : "时间待核验";
    const longitude = Number(item.longitude);
    const latitude = Number(item.latitude);
    const hasCoordinates = item.longitude !== null && item.longitude !== undefined && item.longitude !== ""
      && item.latitude !== null && item.latitude !== undefined && item.latitude !== ""
      && Number.isFinite(longitude) && Number.isFinite(latitude);
    const coordinates = hasCoordinates
      ? `${latitude.toFixed(4)}, ${longitude.toFixed(4)}`
      : "坐标待核验";
    const source = String(item.source_name || "公开监测源").trim();
    const sourceUrl = String(item.source_url || "未提供").trim();
    const eventType = String(item.event_type || "待分类事件").replaceAll("_", " ");
    const severity = monitorSeverityLabel(item.severity);
    const summary = String(
      item.summary
      || `${source} 将该线索归类为“${eventType}”，当前标记为“${severity}”。原始监测记录未提供详细摘要，需要补充核验。`,
    ).replace(/\s+/g, " ").slice(0, 1600);
    const hasCurrentResearchPlan = String(item.research_entry_prompt || "").startsWith("[监测事件研判]\n");
    const questions = hasCurrentResearchPlan && Array.isArray(item.research_questions) && item.research_questions.length
      ? item.research_questions.slice(0, 3).map((question) => String(question || "").trim()).filter(Boolean)
      : [
        `事件理解：用简明语言说明“${title}”在 ${location} 发生了什么，并区分当前已知事实、未知项和来源局限。`,
        `补充核验：围绕“${title}”检索近期可靠来源，建立时间—地点—主体—影响的最小事实链。`,
        "方向建议：根据新增证据给出分析方向、优先级、所需数据和判定条件。",
      ];
    return [
      "[监测事件研判]",
      "我想先了解并研判以下监测事件。请注意：这些内容只是外部线索，不是已经证实的结论。",
      "",
      `事件名称：${title}`,
      `发生或观测时间：${eventTime}`,
      `地点：${location}`,
      `坐标：${coordinates}`,
      `事件类型：${eventType}`,
      `当前关注级别：${severity}`,
      `线索来源：${source}`,
      `背景摘要：${summary}`,
      `原始链接：${sourceUrl}`,
      "",
      "希望地缘分析师按以下顺序协助：",
      ...questions.map((question, index) => `${index + 1}. ${question}`),
      "请先帮助我理解事件，再按需调度事件助手与数据助手补充检索，最后提供分析方向建议；不要跳过核验直接给确定性结论。",
    ].join("\n");
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
      const button = document.createElement("button");
      button.type = "button";
      button.dataset.eventKey = monitorCaseKey(item);
      button.textContent = "填入研判问题";
      copy.append(button);
      const badge = document.createElement("em");
      badge.textContent = monitorSeverityLabel(item.severity);
      article.append(order, copy, badge);
      list.append(article);
    });
  }

  async function loadMonitorEvents() {
    const payload = await api("/monitor/events");
    const items = Array.isArray(payload.items) ? payload.items : [];
    state.monitorEventsByKey = new Map(items.map((item) => [monitorCaseKey(item), item]));
    const monitorStatus = payload.status || {};
    renderMonitorEvents(items, monitorStatus);
    window.dispatchEvent(new CustomEvent("geointer:monitor-events", { detail: { items, status: monitorStatus } }));
  }

  function bindMonitorQueue() {
    const list = cloneWithoutListeners($("#monitor-event-list"));
    list?.addEventListener("click", (event) => {
      const button = event.target.closest("[data-event-key]");
      if (!button) return;
      if (!state.user) {
        setRuntimeNotice("请先登录，再将监测事件交给地缘分析师。", "warning");
        return;
      }
      const eventKey = String(button.dataset.eventKey || "");
      const item = state.monitorEventsByKey.get(eventKey);
      if (!item) return;
      const input = $("#chat-input");
      if (!input) return;
      const now = Date.now();
      const lastRequestedAt = Number(state.recentCaseRequests.get(eventKey) || 0);
      if (now - lastRequestedAt < CASE_REQUEST_COOLDOWN_MS) {
        input.focus();
        setRuntimeNotice("该事件的研判问题已经在输入框中，请检查或修改后发送。", "warning");
        return;
      }
      if (String(input.value || "").trim()) {
        input.focus();
        setRuntimeNotice("输入框中已有未发送内容，系统没有覆盖。请先处理当前草稿。", "warning");
        return;
      }
      const prompt = monitorResearchPrompt(item);
      const title = String(item.display_title || item.title || "该事件").trim();
      state.recentCaseRequests.set(eventKey, now);
      state.pendingMonitorDraft = { eventKey, title, prompt };
      input.value = prompt;
      input.dispatchEvent(new Event("input", { bubbles: true }));
      input.focus();
      input.scrollIntoView({ behavior: "smooth", block: "center" });
      setRuntimeNotice(`已生成“${title}”的研判问题，请检查或修改后点击发送。`, "success");
    });
  }


  function updateRailToggle(side, collapsed) {
    const button = $(`#${side}-rail-toggle`);
    if (!button) return;
    const isLeft = side === "left";
    const direction = isLeft ? "左侧导航栏" : "右侧工作区";
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
    const nextTheme = Object.hasOwn(PREVIEW_THEMES, themeId) ? themeId : LOCKED_PREVIEW_THEME;
    document.documentElement.dataset.previewTheme = nextTheme;
    const current = $("#theme-preview-current");
    if (current) current.textContent = PREVIEW_THEMES[nextTheme];
    $$('[data-preview-theme]').forEach((option) => {
      option.setAttribute("aria-checked", String(option.dataset.previewTheme === nextTheme));
    });
    window.dispatchEvent(new CustomEvent("geointer:theme-change", { detail: { themeId: nextTheme } }));
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
    $$(".theme-preview-control").forEach((control) => {
      control.hidden = true;
      control.setAttribute("aria-hidden", "true");
    });
    applyPreviewTheme(LOCKED_PREVIEW_THEME);
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

  function parkTaskStream() {
    const home = $("#task-stream-home");
    const taskStream = $("#task-stream");
    if (home && taskStream) home.insertAdjacentElement("afterend", taskStream);
  }

  function positionTaskStreamAfterLatestQuestion() {
    const conversation = $("#chat-messages");
    const taskStream = $("#task-stream");
    if (!conversation || !taskStream) return;
    const questions = $$(".message.user", conversation);
    const latestQuestion = questions[questions.length - 1];
    if (latestQuestion) latestQuestion.insertAdjacentElement("afterend", taskStream);
    else parkTaskStream();
  }

  function clearMessages(targetId) {
    const stream = $(`#${targetId}`);
    if (targetId === "chat-messages") parkTaskStream();
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
    var avatarIcon = document.createElement("i");
    avatarIcon.setAttribute("data-lucide", role === "user" ? "user-round" : "bot");
    avatarIcon.setAttribute("aria-hidden", "true");
    avatar.append(avatarIcon);
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
    const list = $("#thread-list");
    if (!list) return;
    list.replaceChildren();
    if (!state.threads.length) {
      const empty = document.createElement("p");
      empty.className = "thread-list-empty";
      empty.textContent = "登录后加载任务";
      list.append(empty);
      return;
    }
    state.threads.forEach((thread) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "thread-list-item";
      button.dataset.threadId = thread.thread_id;
      button.setAttribute("role", "option");
      const active = thread.thread_id === state.threadId;
      button.classList.toggle("active", active);
      button.setAttribute("aria-selected", String(active));
      const icon = document.createElement("i");
      icon.setAttribute("data-lucide", active ? "message-square-text" : "message-square");
      icon.setAttribute("aria-hidden", "true");
      const label = document.createElement("span");
      label.textContent = taskTitle(thread);
      button.append(icon, label);
      button.addEventListener("click", () => {
        if (thread.thread_id === state.threadId) return;
        loadThread(thread.thread_id).catch((error) => setRuntimeNotice(error.message, "error"));
      });
      list.append(button);
    });
    refreshIcons();
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

  function canonicalAgentName(value) {
    const raw = String(value || "").trim();
    if (!raw) return "";
    if (Object.hasOwn(AGENT_ALIASES, raw)) return AGENT_ALIASES[raw];
    if (Object.hasOwn(AGENTS, raw) && raw.startsWith("NTL_")) return raw;
    const match = Object.entries(AGENTS).find(([name, label]) => name.startsWith("NTL_") && label === raw);
    return match?.[0] || "";
  }

  function resetAgentStreams() {
    state.agentStreams.clear();
    $("#agent-stream-list")?.replaceChildren();
  }

  function ensureAgentStream(agentName) {
    const canonical = canonicalAgentName(agentName);
    if (!canonical) return null;
    const existing = state.agentStreams.get(canonical);
    if (existing) return existing;
    const list = $("#agent-stream-list");
    if (!list) return null;

    const card = document.createElement("article");
    card.className = "agent-stream-card";
    card.dataset.agent = canonical;
    card.dataset.state = "streaming";
    const head = document.createElement("header");
    const name = document.createElement("strong");
    name.textContent = AGENTS[canonical];
    const status = document.createElement("span");
    status.textContent = "输出中";
    head.append(name, status);
    const content = document.createElement("p");
    content.className = "agent-stream-text";
    card.append(head, content);
    list.append(card);

    const entry = { card, content, status, text: "", messageId: "", hasDelta: false, renderScheduled: false };
    state.agentStreams.set(canonical, entry);
    addActivity(`${AGENTS[canonical]}开始输出。`, "running");
    return entry;
  }

  function updateAgentStream(agentName, value, { messageId = "", snapshot = false } = {}) {
    const text = String(value ?? "");
    if (!text) return;
    const canonical = canonicalAgentName(agentName);
    const entry = ensureAgentStream(canonical);
    if (!entry || (snapshot && entry.hasDelta)) return;
    if (snapshot) {
      entry.text = text;
    } else {
      if (messageId && entry.messageId && messageId !== entry.messageId && entry.text.trim()) entry.text += "\n\n";
      entry.text += text;
      entry.hasDelta = true;
    }
    if (messageId) entry.messageId = messageId;
    if (entry.text.length > AGENT_STREAM_TEXT_LIMIT) entry.text = `…${entry.text.slice(-AGENT_STREAM_TEXT_LIMIT)}`;
    entry.card.dataset.state = "streaming";
    entry.status.textContent = "输出中";
    setAgentState(canonical, "输出中", true);
    if (!entry.renderScheduled) {
      entry.renderScheduled = true;
      window.requestAnimationFrame(() => {
        entry.renderScheduled = false;
        const scroller = $("#task-stream-content");
        const stickToBottom = !scroller || scroller.scrollHeight - scroller.scrollTop - scroller.clientHeight < 40;
        entry.content.textContent = entry.text;
        if (scroller && stickToBottom) scroller.scrollTop = scroller.scrollHeight;
      });
    }
  }

  function finishAgentStreams(status = "success") {
    const label = status === "success" ? "已完成" : "已停止";
    state.agentStreams.forEach((entry, agentName) => {
      entry.card.dataset.state = status === "success" ? "complete" : "stopped";
      entry.status.textContent = label;
      setAgentState(agentName, label, false);
    });
  }

  function setRightRailTab(tabId, { focus = false, expand = false } = {}) {
    const tabs = ["monitor", "files", "preview", "map"];
    const nextTab = tabs.includes(tabId) ? tabId : "monitor";
    if (expand) setRailCollapsed("right", false);
    state.activeRightRailTab = nextTab;
    $$('[data-right-tab]').forEach((button) => {
      const active = button.dataset.rightTab === nextTab;
      button.setAttribute("aria-selected", String(active));
      button.tabIndex = active ? 0 : -1;
      if (active && focus) button.focus();
    });
    $$('[data-right-panel]').forEach((panel) => {
      panel.hidden = panel.dataset.rightPanel !== nextTab;
    });
    if (nextTab === "files" && state.threadId) void loadWorkspaceFiles();
    if (nextTab === "map") {
      var mapInstance = ensureWorkspaceMap();
      if (mapInstance && state.workspaceFiles.files && state.workspaceFiles.files.length) {
        void autoAddWorkspaceLayers(state.workspaceFiles.files);
      }
      window.requestAnimationFrame(() => window.dispatchEvent(new Event("resize")));
      window.setTimeout(() => window.dispatchEvent(new Event("resize")), 180);
    }
    window.requestAnimationFrame(() => window.dispatchEvent(new Event("resize")));
    window.setTimeout(() => window.dispatchEvent(new Event("resize")), 180);
  }

  function bindRightRailTabs() {
    const tabs = $$('[data-right-tab]');
    tabs.forEach((button) => {
      button.addEventListener("click", () => setRightRailTab(button.dataset.rightTab));
      button.addEventListener("keydown", (event) => {
        const currentIndex = tabs.indexOf(button);
        let nextIndex = currentIndex;
        if (event.key === "ArrowRight") nextIndex = (currentIndex + 1) % tabs.length;
        else if (event.key === "ArrowLeft") nextIndex = (currentIndex - 1 + tabs.length) % tabs.length;
        else if (event.key === "Home") nextIndex = 0;
        else if (event.key === "End") nextIndex = tabs.length - 1;
        else return;
        event.preventDefault();
        setRightRailTab(tabs[nextIndex].dataset.rightTab, { focus: true });
      });
    });
    setRightRailTab("monitor");
  }

  function rightRailWidthBounds() {
    if (window.innerWidth < 1024) {
      return { minimum: RIGHT_RAIL_MIN_WIDTH, maximum: RIGHT_RAIL_MAX_WIDTH };
    }
    const grid = $("#primary-workspace");
    const availableWidth = grid?.clientWidth || window.innerWidth || 1280;
    const maximum = Math.min(
      RIGHT_RAIL_MAX_WIDTH,
      Math.max(RIGHT_RAIL_MIN_WIDTH, availableWidth - RIGHT_RAIL_CHAT_MIN_WIDTH - 12),
    );
    return { minimum: RIGHT_RAIL_MIN_WIDTH, maximum };
  }

  function applyRightRailWidth(value, { persist = false, notify = true } = {}) {
    const { minimum, maximum } = rightRailWidthBounds();
    const numeric = Number(value);
    const next = Math.round(Math.min(maximum, Math.max(minimum, Number.isFinite(numeric) ? numeric : 356)));
    state.rightRailWidth = next;
    $(".app-shell")?.style.setProperty("--right-rail-user-width", `${next}px`);
    const separator = $("#right-rail-resizer");
    if (separator) {
      separator.setAttribute("aria-valuemin", String(minimum));
      separator.setAttribute("aria-valuemax", String(maximum));
      separator.setAttribute("aria-valuenow", String(next));
      separator.setAttribute("aria-valuetext", `右侧工作区宽度 ${next} 像素`);
    }
    if (persist) {
      try {
        window.localStorage.setItem(RIGHT_RAIL_WIDTH_KEY, String(next));
      } catch (_) {
        // Resizing remains available when local storage is unavailable.
      }
    }
    if (notify) window.requestAnimationFrame(() => window.dispatchEvent(new Event("resize")));
  }

  function bindRightRailResize() {
    const separator = $("#right-rail-resizer");
    const rail = $("#right-rail");
    if (!separator || !rail) return;
    let savedWidth = 356;
    try {
      savedWidth = Number(window.localStorage.getItem(RIGHT_RAIL_WIDTH_KEY)) || 356;
    } catch (_) {
      savedWidth = 356;
    }
    applyRightRailWidth(savedWidth);

    let pointerId = null;
    let rightEdge = 0;
    const finishResize = () => {
      if (pointerId === null) return;
      if (separator.hasPointerCapture?.(pointerId)) separator.releasePointerCapture(pointerId);
      pointerId = null;
      document.body.classList.remove("resizing-right-rail");
      applyRightRailWidth(state.rightRailWidth, { persist: true });
    };
    separator.addEventListener("pointerdown", (event) => {
      if (event.button !== 0 || window.innerWidth < 1024) return;
      pointerId = event.pointerId;
      rightEdge = rail.getBoundingClientRect().right;
      separator.setPointerCapture?.(pointerId);
      document.body.classList.add("resizing-right-rail");
      event.preventDefault();
    });
    separator.addEventListener("pointermove", (event) => {
      if (pointerId !== event.pointerId) return;
      applyRightRailWidth(rightEdge - event.clientX);
    });
    separator.addEventListener("pointerup", finishResize);
    separator.addEventListener("pointercancel", finishResize);
    separator.addEventListener("dblclick", () => applyRightRailWidth(356, { persist: true }));
    separator.addEventListener("keydown", (event) => {
      const { minimum, maximum } = rightRailWidthBounds();
      const step = event.shiftKey ? 48 : 16;
      let next = state.rightRailWidth;
      if (event.key === "ArrowLeft") next += step;
      else if (event.key === "ArrowRight") next -= step;
      else if (event.key === "Home") next = minimum;
      else if (event.key === "End") next = maximum;
      else return;
      event.preventDefault();
      applyRightRailWidth(next, { persist: true });
    });
    window.addEventListener("resize", () => {
      if (window.innerWidth >= 1024) applyRightRailWidth(state.rightRailWidth, { notify: false });
    });
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
      badge.textContent = chip.dataset.agent === "地缘分析师" && done ? "已完成" : (chip.dataset.agent === "分析助手" || chip.dataset.agent === "事件助手" ? "按需" : "待命");
    });
  }

  function setTaskStreamExpanded(expanded) {
    const toggle = $("#task-stream-toggle");
    const content = $("#task-stream-content");
    const stream = $("#task-stream");
    const next = Boolean(expanded);
    if (toggle) toggle.setAttribute("aria-expanded", String(next));
    if (content) content.hidden = !next;
    stream?.classList.toggle("is-expanded", next);
    const label = $(".task-stream-toggle-label", toggle || document);
    if (label) label.textContent = next ? "收起详情" : "查看详情";
  }

  function setRunUi(running) {
    const statusNode = $("#thread-status span:last-child");
    if (statusNode) statusNode.textContent = running ? "任务运行中" : state.user ? "地缘分析师已就绪" : "登录后开始";
    const stream = $("#task-stream");
    const list = $("#run-activity-list");
    const hasActivity = Boolean(list?.querySelector("li:not(.run-activity-empty)"));
    if (stream) {
      if (running) positionTaskStreamAfterLatestQuestion();
      stream.hidden = !running && !hasActivity;
      stream.dataset.state = running ? "running" : hasActivity ? "complete" : "idle";
      if (running) {
        window.requestAnimationFrame(() => {
          const conversation = $("#chat-messages");
          if (conversation) conversation.scrollTop = conversation.scrollHeight;
        });
      }
    }
    const streamState = $("#run-activity-state");
    if (streamState) streamState.textContent = running ? "运行中" : hasActivity ? "本轮完成" : "等待任务";
    const stop = $("#stop-run");
    if (stop) {
      stop.hidden = !running;
      if (!running) {
        stop.disabled = false;
        stop.textContent = "停止任务";
      }
    }
    $$("#chat-form textarea, #chat-form .send-button, #logistics-chat-form textarea, #logistics-chat-form .send-button").forEach((node) => {
      node.disabled = running || !state.user;
    });
  }

  function encodedArtifactPath(item) {
    return String(item?.path || "").split("/").filter(Boolean).map(encodeURIComponent).join("/");
  }

  function artifactDownloadUrl(item, threadId = state.threadId) {
    return `${API_ROOT}/threads/${encodeURIComponent(threadId)}/outputs/${encodedArtifactPath(item)}`;
  }

  function artifactPreviewKind(item) {
    const declared = String(item?.preview_kind || "").toLowerCase();
    if (["html", "image", "geo", "markdown", "pdf", "table", "text"].includes(declared)) return declared;
    const extension = String(item?.name || item?.path || "").toLowerCase().match(/\.[^.]+$/)?.[0] || "";
    if ([".gif", ".jpeg", ".jpg", ".png", ".webp"].includes(extension)) return "image";
    if ([".tif", ".tiff", ".shp", ".geojson", ".kml", ".kmz"].includes(extension)) return "geo";
    if ([".csv", ".json", ".tsv", ".xlsx"].includes(extension)) return "table";
    if ([".htm", ".html"].includes(extension)) return "html";
    if ([".markdown", ".md"].includes(extension)) return "markdown";
    if (extension === ".pdf") return "pdf";
    if ([".css", ".js", ".log", ".py", ".txt"].includes(extension)) return "text";
    return "";
  }

  function workspaceFileContentUrl(item, threadId = state.threadId) {
    const root = item?.root === "inputs" ? "inputs" : "outputs";
    return `${API_ROOT}/threads/${encodeURIComponent(threadId)}/file-preview/content/${root}/${encodedArtifactPath(item)}`;
  }

  function workspaceTablePreviewPath(item, threadId = state.threadId) {
    const root = item?.root === "inputs" ? "inputs" : "outputs";
    return `/threads/${encodeURIComponent(threadId)}/file-preview/table/${root}/${encodedArtifactPath(item)}`;
  }

  function workspaceFileIcon(item) {
    return ({
      html: "panels-top-left",
      image: "image",
      geo: "globe-2",
      markdown: "file-text",
      pdf: "file-type-2",
      table: "table-2",
      text: "file-code-2",
    })[artifactPreviewKind(item)] || "file";
  }

  function workspaceFileTypeLabel(item) {
    return ({
      html: "HTML 交互预览",
      image: "图片预览",
      geo: "地理空间预览",
      markdown: "Markdown 文档",
      pdf: "PDF 文档",
      table: "表格预览",
      text: "文本文件",
    })[artifactPreviewKind(item)] || "文件下载";
  }

  function renderWorkspaceFileList(items, listId, emptyText) {
    const list = $(`#${listId}`);
    if (!list) return;
    list.replaceChildren();
    if (!items.length) {
      const empty = document.createElement("li");
      empty.className = "workspace-file-empty";
      empty.textContent = emptyText;
      list.append(empty);
      return;
    }
    items.forEach((item) => {
      const row = document.createElement("li");
      row.className = "workspace-file-row";
      const button = document.createElement("button");
      button.type = "button";
      button.title = `预览 ${item.path || item.name || "文件"}`;
      const icon = document.createElement("i");
      icon.setAttribute("data-lucide", workspaceFileIcon(item));
      icon.setAttribute("aria-hidden", "true");
      const copy = document.createElement("span");
      copy.className = "workspace-file-copy";
      const name = document.createElement("strong");
      name.textContent = item.name || item.path || "未命名文件";
      const path = document.createElement("small");
      const root = String(item.root || "outputs") === "inputs" ? "研究资料" : "分析产出";
      const pathText = item.path || item.name || "";
      path.textContent = item.root ? `${root} · ${pathText}` : pathText;
      copy.append(name, path);
      const size = document.createElement("span");
      size.className = "workspace-file-size";
      size.textContent = item.size_label || "文件";
      button.append(icon, copy, size);
      button.addEventListener("click", () => void openWorkspaceFile(item));
      row.append(button);
      list.append(row);
    });
  }

  function renderWorkspaceFiles(files = {}) {
    const inputs = Array.isArray(files.inputs) ? files.inputs : [];
    const outputs = Array.isArray(files.outputs) ? files.outputs : [];
    // Unified stream: prefer the server-merged view, otherwise merge locally.
    const merged = Array.isArray(files.files) && files.files.length
      ? files.files
      : [...inputs, ...outputs].sort((a, b) => (Number(b.modified_ts) || 0) - (Number(a.modified_ts) || 0));
    state.workspaceFiles = { inputs, outputs, files: merged };
    const query = String($("#workspace-file-search")?.value || "").trim().toLocaleLowerCase("zh-CN");
    const rootFilter = String(state.fileRootFilter || "all");
    $$("[data-file-root-filter]").forEach((filter) => {
      filter.classList.toggle("active", String(filter.dataset.fileRootFilter) === rootFilter);
      filter.setAttribute("aria-pressed", String(filter.dataset.fileRootFilter === rootFilter));
    });
    const matches = (item) =>
      (!query || `${item.name || ""} ${item.path || ""}`.toLocaleLowerCase("zh-CN").includes(query)) &&
      (rootFilter === "all" || String(item.root || "outputs") === rootFilter);
    const visibleMerged = merged.filter(matches);
    renderWorkspaceFileList(visibleMerged, "input-file-list", query ? "没有匹配的任务文件" : "尚未上传文件");
    // Legacy grouped lists remain for narrow fallback; keep them in sync (hidden by default).
    renderWorkspaceFileList(inputs.filter(matches), "artifact-list", "尚无分析产出");
    const inputCount = $("#input-file-count");
    const outputCount = $("#artifact-count");
    const summaryCount = $("#artifact-summary-count");
    const exportButton = $("#export-brief");
    const total = inputs.length + outputs.length;
    if (inputCount) inputCount.textContent = query ? String(visibleMerged.length) : String(total);
    if (outputCount) outputCount.textContent = String(outputs.length);
    if (summaryCount) {
      summaryCount.textContent = String(total);
      summaryCount.hidden = !total;
    }
    if (exportButton) exportButton.disabled = !outputs.length;
    refreshIcons();
  }

  function isProcessFile(item) {
    if (item && item.is_process === true) return true;
    var name = String(item && (item.name || item.path) || "").toLowerCase();
    return /manifest|execution_history|evidence_report|route_state|contract|observation|task_plan|\.jsonl|\.py$/.test(name);
  }

  function renderWorkspaceFiles(files) {
    files = files || {};
    var inputs = Array.isArray(files.inputs) ? files.inputs : [];
    var outputs = Array.isArray(files.outputs) ? files.outputs : [];
    var merged = Array.isArray(files.files) && files.files.length
      ? files.files
      : [...inputs, ...outputs].sort(function (a, b) { return (Number(b.modified_ts) || 0) - (Number(a.modified_ts) || 0); });
    state.workspaceFiles = { inputs: inputs, outputs: outputs, files: merged };
    var query = String($("#workspace-file-search") && $("#workspace-file-search").value || "").trim().toLocaleLowerCase("zh-CN");
    var rootFilter = String(state.fileRootFilter || "all");
    $$("[data-file-root-filter]").forEach(function (filter) {
      filter.classList.toggle("active", String(filter.dataset.fileRootFilter) === rootFilter);
      filter.setAttribute("aria-pressed", String(filter.dataset.fileRootFilter === rootFilter));
    });
    var matches = function (item) {
      var hay = (item.name || "") + " " + (item.path || "");
      return (!query || hay.toLocaleLowerCase("zh-CN").includes(query)) &&
        (rootFilter === "all" || String(item.root || "outputs") === rootFilter);
    };
    var visibleMerged = merged.filter(matches);
    var deliverables = visibleMerged.filter(function (item) { return !isProcessFile(item); });
    var processes = visibleMerged.filter(function (item) { return isProcessFile(item); });
    renderWorkspaceFileList(deliverables, "input-file-list", query ? "没有匹配的任务文件" : "尚未上传文件");
    renderWorkspaceProcessFiles(processes);
    renderWorkspaceFileList(inputs.filter(matches), "artifact-list", "尚无分析产出");
    var inputCount = $("#input-file-count");
    var outputCount = $("#artifact-count");
    var summaryCount = $("#artifact-summary-count");
    var exportButton = $("#export-brief");
    var total = inputs.length + outputs.length;
    if (inputCount) inputCount.textContent = query ? String(deliverables.length) : String(total);
    if (outputCount) outputCount.textContent = String(outputs.length);
    if (summaryCount) {
      summaryCount.textContent = String(total);
      summaryCount.hidden = !total;
    }
    if (exportButton) exportButton.disabled = !outputs.length;
    autoAddWorkspaceLayers(merged);
    refreshIcons();
  }

  function renderWorkspaceProcessFiles(items) {
    var group = $("#workspace-process-group");
    if (!group) return;
    if (!items.length) { group.hidden = true; return; }
    group.hidden = false;
    var count = $("#workspace-process-count");
    if (count) count.textContent = String(items.length);
    renderWorkspaceFileList(items, "workspace-process-list", "没有过程文件");
  }

  function isGeoLayerFile(item) {
    var kind = artifactPreviewKind(item);
    if (kind !== "geo") return false;
    if (isProcessFile(item)) return false;
    var name = String((item && (item.name || item.path)) || "").toLowerCase();
    // GDAL sidecar metadata is not a layer.
    if (name.endsWith(".aux.xml") || name.indexOf(".aux.xml") >= 0) return false;
    return true;
  }

  function workspaceLayerKey(item) {
    return String((item.root || "outputs") + "/" + (item.path || item.name || ""));
  }

  function ensureWorkspaceMap() {
    if (state.workspaceMap) return state.workspaceMap;
    var holder = $("#workspace-map");
    if (!holder || !window.maplibregl) return null;
    holder.querySelector(".map-loading")?.remove();
    var style = {
      version: 8,
      sources: {
        base: {
          type: "raster",
          tiles: ["https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{y}/{x}"],
          tileSize: 256,
          maxzoom: 19,
          attribution: "Esri, HERE, Garmin, © OpenStreetMap contributors",
        },
      },
      layers: [{
        id: "base",
        type: "raster",
        source: "base",
        paint: {
          "raster-saturation": -0.75,
          "raster-contrast": 0.2,
          "raster-brightness-min": 0.16,
          "raster-brightness-max": 0.62,
          "raster-fade-duration": 0,
        },
      }],
      glyphs: "https://fonts.openmaptiles.org/{fontstack}/{range}.pbf",
    };
    var map = new maplibregl.Map({
      container: holder,
      style: style,
      center: [104.2, 35.9],
      zoom: 0.65,
      minZoom: 0.55,
      maxZoom: 14,
      attributionControl: false,
      interactive: true,
    });
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "bottom-right");
    // Show auto-load chip on any layer presence.
    map.on("load", function () {
      var chip = $("#map-layer-autoload");
      if (chip) chip.hidden = state.workspaceMapLayers.size === 0;
      var cached = Array.from(state.workspaceMapLayers.keys());
      cached.forEach(function (key) {
        var entry = state.workspaceMapLayers.get(key);
        if (entry && entry.payload) {
          var item = entry.item;
          var frame = layerFrame(item, map);
          if (!frame.imageUrl) return;
          if (!map.getSource(frame.sourceId)) {
            map.addSource(frame.sourceId, { type: "image", url: frame.imageUrl, coordinates: frame.coordinates });
          }
          if (!map.getLayer(frame.layerId)) {
            map.addLayer({
              id: frame.layerId,
              type: "raster",
              source: frame.sourceId,
              paint: { "raster-opacity": 0.82, "raster-fade-duration": 0 },
            });
          }
        }
      });
      refreshWorkspaceMapLayers();
      if (cached.length) {
        window.setTimeout(function () {
          fitWorkspaceMapToLayers();
        }, 260);
      }
    });
    state.workspaceMap = map;
    window.setTimeout(function () { map.resize(); }, 120);
    return map;
  }

  async function fetchGeoLayerPayload(item) {
    var geoPath = encodedArtifactPath(item);
    var url = "/threads/" + encodeURIComponent(state.threadId) + "/file-preview/geo/" + (item.root || "outputs") + "/" + geoPath;
    var res = await api(url);
    return res;
  }

  function layerFrame(item, map) {
    var payload = state.workspaceMapLayers.get(workspaceLayerKey(item)).payload;
    var envelope = payload && payload.envelope;
    var ok = envelope && Number.isFinite(envelope.minx) && envelope.minx < envelope.maxx
      && Number.isFinite(envelope.miny) && envelope.miny < envelope.maxy;
    var id = "wm-layer-" + Math.abs(hashString(workspaceLayerKey(item)));
    var png = payload && payload.image_png_base64;
    var coordinates = ok
      ? [[envelope.minx, envelope.maxy], [envelope.maxx, envelope.maxy], [envelope.maxx, envelope.miny], [envelope.minx, envelope.miny]]
      : [[-180, 85], [180, 85], [180, -85], [-180, -85]];
    return {
      id: id,
      sourceId: id + "-source",
      layerId: id + "-layer",
      coordinates: coordinates,
      imageUrl: png ? "data:image/png;base64," + png : null,
      envelope: envelope,
      ok: ok,
    };
  }

  function hashString(str) {
    var h = 0;
    for (var i = 0; i < str.length; i++) {
      h = (h * 31) + str.charCodeAt(i);
      h = h >>> 0;
    }
    return h;
  }

  async function autoAddWorkspaceLayers(files) {
    if (!state.threadId) return;
    var pending = (Array.isArray(files) ? files : []).filter(isGeoLayerFile);
    for (var i = 0; i < pending.length; i++) {
      var item = pending[i];
      var key = workspaceLayerKey(item);
      if (state.workspaceMapLayers.has(key)) continue;
      try {
        var payload = await fetchGeoLayerPayload(item);
        state.workspaceMapLayers.set(key, { item: item, payload: payload, visible: true });
        if (state.workspaceMap) await mountWorkspaceLayer(item);
      } catch (e) {
        // Unreadable (e.g. broken GeoTIFF): skip silently, keep the rest.
        state.workspaceMapLayers.set(key, { item: item, payload: null, visible: false });
      }
    }
    if (state.workspaceMap) refreshWorkspaceMapLayers();
  }

  async function mountWorkspaceLayer(item) {
    var map = state.workspaceMap;
    if (!map) return;
    if (!map.loaded()) {
      await new Promise(function (resolve) {
        if (map.loaded()) return resolve();
        map.once("load", resolve);
        window.setTimeout(resolve, 4000);
      });
    }
    if (!state.workspaceMap) return;
    map = state.workspaceMap;
    var entry = state.workspaceMapLayers.get(workspaceLayerKey(item));
    if (!entry || !entry.payload) return;
    var frame = layerFrame(item, map);
    if (!frame.imageUrl) return;
    if (!map.getSource(frame.sourceId)) {
      map.addSource(frame.sourceId, { type: "image", url: frame.imageUrl, coordinates: frame.coordinates });
    } else {
      map.getSource(frame.sourceId).setCoordinates(frame.coordinates);
    }
    if (!map.getLayer(frame.layerId)) {
      map.addLayer({
        id: frame.layerId,
        type: "raster",
        source: frame.sourceId,
        paint: { "raster-opacity": 0.82, "raster-fade-duration": 0 },
      });
    }
  }

  function fitWorkspaceMapToLayers() {
    var map = state.workspaceMap;
    if (!map) return;
    var frames = [];
    state.workspaceMapLayers.forEach(function (entry, key) {
      if (!entry.visible || !entry.payload || !entry.payload.envelope) return;
      var env = entry.payload.envelope;
      frames.push(env);
    });
    if (!frames.length) return;
    var minx = Infinity, miny = Infinity, maxx = -Infinity, maxy = -Infinity;
    frames.forEach(function (env) {
      minx = Math.min(minx, env.minx); maxx = Math.max(maxx, env.maxx);
      miny = Math.min(miny, env.miny); maxy = Math.max(maxy, env.maxy);
    });
    map.fitBounds([[minx, miny], [maxx, maxy]], { padding: 36, duration: 500 });
  }

  function refreshWorkspaceMapLayers() {
    var list = $("#workspace-map-layer-list");
    var totalEl = $("#workspace-map-layer-total");
    var countEl = $("#map-layer-count");
    var autoload = $("#map-layer-autoload");
    var map = state.workspaceMap;
    if (!list) return;
    list.replaceChildren();
    var entries = Array.from(state.workspaceMapLayers.entries());
    var visibleCount = 0;
    entries.forEach(function (pair) {
      var key = pair[0], entry = pair[1];
      if (entry.visible) visibleCount++;
      var li = document.createElement("li");
      li.className = "workspace-map-layer";
      var label = document.createElement("button");
      label.type = "button";
      label.className = "workspace-map-layer-label";
      var icon = document.createElement("i");
      icon.setAttribute("data-lucide", entry.payload && entry.payload.kind === "vector" ? "route" : "image");
      icon.setAttribute("aria-hidden", "true");
      var copy = document.createElement("span");
      copy.className = "workspace-map-layer-copy";
      var name = document.createElement("strong");
      name.textContent = entry.item.name || entry.item.path || "图层";
      copy.append(name);
      label.append(icon, copy);
      li.append(label);
      if (entry.payload) {
        var zoom = document.createElement("button");
        zoom.type = "button";
        zoom.className = "workspace-map-layer-action";
        zoom.title = "定位到该图层";
        zoom.setAttribute("aria-label", "定位到该图层");
        var zi = document.createElement("i");
        zi.setAttribute("data-lucide", "locate-fixed");
        zoom.append(zi);
        zoom.addEventListener("click", function () {
          var env = entry.payload && entry.payload.envelope;
          if (!map || !env) return;
          map.fitBounds([[env.minx, env.miny], [env.maxx, env.maxy]], { padding: 36, duration: 400 });
        });
        li.append(zoom);
      }
      var frame = entry._frame || (entry._frame = layerFrame(entry.item, map));
      label.addEventListener("click", function () {
        var env = entry.payload && entry.payload.envelope;
        if (map && env) map.fitBounds([[env.minx, env.miny], [env.maxx, env.maxy]], { padding: 36, duration: 400 });
      });
      var toggle = document.createElement("button");
      toggle.type = "button";
      toggle.className = "workspace-map-layer-toggle";
      toggle.title = entry.visible ? "隐藏图层" : "显示图层";
      toggle.setAttribute("aria-label", toggle.title);
      toggle.setAttribute("aria-pressed", String(entry.visible));
      var ti = document.createElement("i");
      ti.setAttribute("data-lucide", entry.visible ? "eye" : "eye-off");
      toggle.append(ti);
      toggle.addEventListener("click", function () {
        entry.visible = !entry.visible;
        var map2 = state.workspaceMap;
        if (map2 && frame.layerId && map2.getLayer(frame.layerId)) map2.setLayoutProperty(frame.layerId, "visibility", entry.visible ? "visible" : "none");
        refreshWorkspaceMapLayers();
      });
      li.append(toggle);
      list.append(li);
    });
    if (!entries.length) {
      list.innerHTML = "";
      var empty = document.createElement("li");
      empty.className = "workspace-map-empty";
      empty.textContent = "尚无地理空间数据。任务生成 tif / shp / geojson 等文件后将自动出现。";
      list.append(empty);
    }
    if (totalEl) totalEl.textContent = String(entries.length);
    if (countEl) {
      countEl.textContent = String(visibleCount);
      countEl.hidden = visibleCount === 0;
    }
    if (autoload) autoload.hidden = entries.length === 0;
    refreshIcons();
  }

  async function loadWorkspaceFiles() {
    if (!state.threadId) {
      renderWorkspaceFiles({ inputs: [], outputs: [] });
      return;
    }
    const payload = await api(`/threads/${encodeURIComponent(state.threadId)}/workspace-files`);
    // "/workspace-files" returns { inputs, outputs, files } at the top level.
    renderWorkspaceFiles(Array.isArray(payload.files) ? payload : payload.files || payload);
  }

  function renderWorkspacePreviewEmpty(message, detail = "从“项目文件”中选择另一个文件。") {
    const body = $("#workspace-preview-body");
    if (!body) return;
    const empty = document.createElement("div");
    empty.className = "workspace-preview-empty";
    const icon = document.createElement("i");
    icon.setAttribute("data-lucide", "file-search");
    icon.setAttribute("aria-hidden", "true");
    const title = document.createElement("strong");
    title.textContent = message;
    const copy = document.createElement("p");
    copy.textContent = detail;
    empty.append(icon, title, copy);
    body.replaceChildren(empty);
    refreshIcons();
  }

  function renderWorkspaceTable(payload, body, item) {
    const columns = Array.isArray(payload.columns) ? payload.columns : [];
    const rows = Array.isArray(payload.rows) ? payload.rows : [];
    if (!columns.length) {
      renderWorkspacePreviewEmpty("表格为空", "可以下载文件后在本地查看完整内容。");
      return;
    }
    const wrapper = document.createElement("div");
    wrapper.className = "workspace-preview-table";
    const scroll = document.createElement("div");
    scroll.className = "table-preview-scroll";
    scroll.dataset.columns = String(columns.length);
    const table = document.createElement("table");
    table.setAttribute("aria-label", `${item.name || "表格"}预览`);
    const thead = document.createElement("thead");
    const header = document.createElement("tr");
    columns.forEach((column) => {
      const cell = document.createElement("th");
      cell.scope = "col";
      cell.textContent = String(column ?? "");
      header.append(cell);
    });
    thead.append(header);
    const tbody = document.createElement("tbody");
    rows.forEach((row) => {
      const tr = document.createElement("tr");
      columns.forEach((_, index) => {
        const cell = document.createElement("td");
        cell.textContent = String(row?.[index] ?? "");
        tr.append(cell);
      });
      tbody.append(tr);
    });
    table.append(thead, tbody);
    scroll.append(table);
    const meta = document.createElement("p");
    meta.className = "artifact-preview-meta";
    const sheet = payload.sheet_name ? ` · 工作表：${payload.sheet_name}` : "";
    meta.textContent = `显示前 ${rows.length} 行${payload.truncated ? " · 其余内容请下载查看" : ""}${sheet}`;
    wrapper.append(scroll, meta);
    body.replaceChildren(wrapper);
  }

  async function renderWorkspaceGeo(payload, body, item) {
    const holder = document.createElement("div");
    holder.className = "workspace-preview-geo";
    const mapNode = document.createElement("div");
    mapNode.className = "workspace-preview-geo-map";
    const meta = document.createElement("p");
    meta.className = "artifact-preview-meta";
    holder.append(mapNode, meta);
    body.replaceChildren(holder);
    if (!window.maplibregl) {
      meta.textContent = "地图组件未加载，请刷新页面后重试。";
      return;
    }
    const envelope = payload.envelope || null;
    const ok = envelope && Number.isFinite(envelope.minx) && envelope.minx < envelope.maxx
      && Number.isFinite(envelope.miny) && envelope.miny < envelope.maxy;
    meta.textContent = payload.note
      ? (item.name || payload.name || "地理空间文件") + " · " + payload.note
      : (item.name || payload.name || "地理空间文件") + " · 预览";
    const style = {
      version: 8,
      sources: {
        base: {
          type: "raster",
          tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
          tileSize: 256,
          maxzoom: 19,
          attribution: "© OpenStreetMap contributors",
        },
      },
      layers: [{ id: "base", type: "raster", source: "base" }],
      glyphs: "https://fonts.openmaptiles.org/{fontstack}/{range}.pbf",
    };
    const map = new maplibregl.Map({
      container: mapNode,
      style,
      center: ok ? [(envelope.minx + envelope.maxx) / 2, (envelope.miny + envelope.maxy) / 2] : [0, 20],
      zoom: ok ? 3 : 0.8,
      minZoom: 0.55,
      maxZoom: 10,
      attributionControl: false,
    });
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "bottom-right");
    map.on("load", () => {
      if (!payload.image_png_base64) return;
      const imageUrl = "data:image/png;base64," + payload.image_png_base64;
      const coordinates = ok
        ? [
            [envelope.minx, envelope.maxy],
            [envelope.maxx, envelope.maxy],
            [envelope.maxx, envelope.miny],
            [envelope.minx, envelope.miny],
          ]
        : [
            [-180, 85], [180, 85], [180, -85], [-180, -85],
          ];
      map.addSource("preview-image", { type: "image", url: imageUrl, coordinates });
      map.addLayer({
        id: "preview-image-layer",
        type: "raster",
        source: "preview-image",
        paint: { "raster-opacity": 0.9, "raster-fade-duration": 0 },
      });
      if (ok) map.fitBounds([[envelope.minx, envelope.miny], [envelope.maxx, envelope.maxy]], { padding: 24, duration: 0 });
    });
    map.on("error", () => {
      meta.textContent = "地图预览加载异常，可下载文件查看。";
    });
    state.geoPreviewMaps = state.geoPreviewMaps || [];
    state.geoPreviewMaps.push(map);
    window.setTimeout(() => map.resize(), 80);
    return map;
  }

  async function openWorkspaceFile(item) {
    if (!state.threadId || !item) return;
    const requestToken = ++state.previewRequestToken;
    state.previewFile = item;
    const title = $("#workspace-preview-title");
    const kindLabel = $("#workspace-preview-kind");
    const download = $("#workspace-preview-download");
    const body = $("#workspace-preview-body");
    const kind = artifactPreviewKind(item);
    const contentUrl = workspaceFileContentUrl(item);
    if (title) {
      title.textContent = item.name || item.path || "文件预览";
      title.title = item.path || item.name || "";
    }
    if (kindLabel) kindLabel.textContent = `${item.root === "inputs" ? "研究资料" : "分析产出"} · ${workspaceFileTypeLabel(item)}`;
    if (download) {
      download.href = contentUrl;
      download.download = item.name || "";
      download.hidden = false;
    }
    if (!body) return;
    const loading = document.createElement("div");
    loading.className = "workspace-preview-empty";
    loading.textContent = "正在准备文件预览…";
    body.replaceChildren(loading);
    setRightRailTab("preview", { expand: true });

    try {
      if (kind === "image") {
        const figure = document.createElement("div");
        figure.className = "workspace-preview-image";
        const image = document.createElement("img");
        image.src = contentUrl;
        image.alt = `文件预览：${item.name || item.path || "图片"}`;
        image.loading = "lazy";
        image.decoding = "async";
        image.addEventListener("error", () => renderWorkspacePreviewEmpty("图片预览不可用", "请使用右上角“下载”查看原文件。"), { once: true });
        figure.append(image);
        body.replaceChildren(figure);
        return;
      }
      if (kind === "html" || kind === "pdf") {
        const frame = document.createElement("iframe");
        frame.className = "workspace-preview-frame";
        frame.src = contentUrl;
        frame.title = `${item.name || "文件"}预览`;
        frame.referrerPolicy = "no-referrer";
        if (kind === "html") frame.setAttribute("sandbox", "allow-scripts");
        body.replaceChildren(frame);
        return;
      }
      if (kind === "table") {
        const payload = await api(workspaceTablePreviewPath(item));
        if (requestToken !== state.previewRequestToken) return;
        renderWorkspaceTable(payload, body, item);
        return;
      }
      if (kind === "geo") {
        const geoPath = encodedArtifactPath(item);
        const payload = await api("/threads/" + encodeURIComponent(state.threadId) + "/file-preview/geo/" + (item.root || "outputs") + "/" + geoPath);
        if (requestToken !== state.previewRequestToken) return;
        await renderWorkspaceGeo(payload, body, item);
        return;
      }
      if (kind === "markdown" || kind === "text") {
        const response = await fetch(contentUrl, { credentials: "same-origin" });
        if (!response.ok) throw new Error("文本内容读取失败。");
        const text = await response.text();
        if (requestToken !== state.previewRequestToken) return;
        const content = document.createElement(kind === "text" ? "pre" : "div");
        content.className = "workspace-preview-markdown";
        if (kind === "markdown") appendMessageBlocks(content, text);
        else content.textContent = text;
        body.replaceChildren(content);
        return;
      }
      renderWorkspacePreviewEmpty("暂不支持内嵌预览", "仍可使用右上角“下载”打开该文件。 ");
    } catch (error) {
      if (requestToken !== state.previewRequestToken) return;
      const notice = document.createElement("p");
      notice.className = "workspace-preview-error";
      notice.textContent = `${error.message || "文件预览不可用"} 请下载后查看。`;
      body.replaceChildren(notice);
    }
  }

  function createArtifactPreviewMessage(targetId, artifactCount, renderContent) {
    const stream = $(`#${targetId}`);
    if (!stream) return null;
    stream.querySelectorAll(".artifact-preview-message").forEach((node) => node.remove());
    stream.querySelector(".chat-empty-state")?.remove();
    const article = document.createElement("article");
    article.className = "message assistant artifact-preview-message";
    const avatar = document.createElement("span");
    avatar.className = "message-avatar ai";
    avatar.textContent = "AI";
    const body = document.createElement("div");
    body.className = "artifact-preview-body";
    const toggle = document.createElement("button");
    toggle.type = "button";
    toggle.className = "artifact-preview-toggle";
    toggle.setAttribute("aria-expanded", "false");
    const panelId = `artifact-preview-${targetId}`;
    toggle.setAttribute("aria-controls", panelId);
    const copy = document.createElement("span");
    copy.className = "artifact-preview-toggle-copy";
    const title = document.createElement("strong");
    title.textContent = "研究产出";
    const hint = document.createElement("small");
    const countLabel = `${artifactCount} 个文件`;
    hint.textContent = `已生成 · ${countLabel}`;
    copy.append(title, hint);
    const chevron = document.createElement("span");
    chevron.className = "artifact-preview-chevron";
    chevron.setAttribute("aria-hidden", "true");
    toggle.append(copy, chevron);
    const grid = document.createElement("div");
    grid.className = "generated-output-grid";
    grid.id = panelId;
    grid.hidden = true;
    let initialized = false;
    toggle.addEventListener("click", () => {
      const expanded = toggle.getAttribute("aria-expanded") !== "true";
      toggle.setAttribute("aria-expanded", String(expanded));
      grid.hidden = !expanded;
      hint.textContent = expanded ? `${countLabel} · 点击收起` : `已生成 · ${countLabel}`;
      if (expanded && !initialized) {
        initialized = true;
        renderContent?.(grid);
      }
    });
    body.append(toggle, grid);
    article.append(avatar, body);
    stream.append(article);
    return { stream, article, grid, toggle };
  }

  function createArtifactCardHead(item, threadId) {
    const head = document.createElement("header");
    const title = document.createElement("strong");
    title.textContent = item.name || item.path || "研究产出";
    title.title = item.path || item.name || "";
    const download = document.createElement("a");
    download.href = artifactDownloadUrl(item, threadId);
    download.textContent = "下载";
    download.setAttribute("download", item.name || "");
    head.append(title, download);
    return head;
  }

  function renderImageArtifact(item, threadId) {
    const figure = document.createElement("figure");
    figure.className = "generated-image-preview";
    const link = document.createElement("a");
    link.href = artifactDownloadUrl(item, threadId);
    link.target = "_blank";
    link.rel = "noopener";
    const image = document.createElement("img");
    image.src = link.href;
    image.alt = `生成图片：${item.name || item.path || "未命名图片"}`;
    image.loading = "lazy";
    image.decoding = "async";
    const caption = document.createElement("figcaption");
    const name = document.createElement("strong");
    name.textContent = item.name || item.path || "生成图片";
    const meta = document.createElement("small");
    meta.textContent = `${item.size_label || "图片"} · 点击查看原图`;
    caption.append(name, meta);
    link.append(image, caption);
    figure.append(link);
    image.addEventListener("error", () => {
      figure.classList.add("preview-unavailable");
      image.remove();
      meta.textContent = "图片预览不可用，点击下载查看";
    }, { once: true });
    return figure;
  }

  async function renderTableArtifact(item, threadId, card) {
    const status = document.createElement("p");
    status.className = "artifact-preview-status";
    status.textContent = "正在读取表格预览…";
    card.append(status);
    try {
      const previewPath = encodedArtifactPath(item);
      const payload = await api(`/threads/${encodeURIComponent(threadId)}/artifacts/${previewPath}/preview`);
      if (!card.isConnected || state.threadId !== threadId) return;
      status.remove();
      const columns = Array.isArray(payload.columns) ? payload.columns : [];
      const rows = Array.isArray(payload.rows) ? payload.rows : [];
      if (!columns.length) {
        const empty = document.createElement("p");
        empty.className = "artifact-preview-status";
        empty.textContent = "表格为空，可下载文件查看。";
        card.append(empty);
        return;
      }
      const scroll = document.createElement("div");
      scroll.className = "table-preview-scroll";
      scroll.dataset.columns = String(columns.length);
      const table = document.createElement("table");
      table.setAttribute("aria-label", `${item.name || "生成表格"}预览`);
      const thead = document.createElement("thead");
      const headerRow = document.createElement("tr");
      columns.forEach((column) => {
        const cell = document.createElement("th");
        cell.scope = "col";
        cell.textContent = String(column ?? "");
        headerRow.append(cell);
      });
      thead.append(headerRow);
      const tbody = document.createElement("tbody");
      rows.forEach((row) => {
        const tr = document.createElement("tr");
        columns.forEach((_, index) => {
          const cell = document.createElement("td");
          cell.textContent = String(row?.[index] ?? "");
          tr.append(cell);
        });
        tbody.append(tr);
      });
      table.append(thead, tbody);
      scroll.append(table);
      const foot = document.createElement("p");
      foot.className = "artifact-preview-meta";
      const sheet = payload.sheet_name ? ` · 工作表：${payload.sheet_name}` : "";
      foot.textContent = `显示前 ${rows.length} 行${payload.truncated ? " · 其余内容请下载查看" : ""}${sheet}`;
      card.append(scroll, foot);
    } catch (error) {
      status.dataset.tone = "warning";
      status.textContent = `${error.message || "表格预览不可用"} 可下载文件查看。`;
    }
  }

  function renderArtifactPreviews(items, targetId) {
    const stream = $(`#${targetId}`);
    if (!stream) return;
    stream.querySelectorAll(".artifact-preview-message").forEach((node) => node.remove());
    const artifacts = (Array.isArray(items) ? items : [])
      .filter((item) => ["image", "table"].includes(artifactPreviewKind(item)))
      .slice(0, 6);
    if (!artifacts.length) return;
    const threadId = state.threadId;
    const message = createArtifactPreviewMessage(targetId, artifacts.length, (grid) => {
      artifacts.forEach((item) => {
        const kind = artifactPreviewKind(item);
        if (kind === "image") {
          grid.append(renderImageArtifact(item, threadId));
          return;
        }
        const card = document.createElement("section");
        card.className = "generated-table-preview";
        card.append(createArtifactCardHead(item, threadId));
        grid.append(card);
        void renderTableArtifact(item, threadId, card);
      });
    });
    if (!message) return;
    message.stream.scrollTop = message.stream.scrollHeight;
  }

  function renderArtifacts(items) {
    // Preserve the unified merged stream; only refresh the outputs subset.
    renderWorkspaceFiles({
      ...state.workspaceFiles,
      outputs: Array.isArray(items) && items.length ? items : state.workspaceFiles.outputs,
    });
  }

  function resetRunStream() {
    parkTaskStream();
    resetAgentStreams();
    setTaskStreamExpanded(false);
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

  async function pruneThreadIfBlank(threadId) {
    if (!threadId || !state.threads.some((item) => item.thread_id === threadId)) return;
    if (state.usedThreads.has(threadId)) return;
    try {
      const response = await fetch(`${API_ROOT}/threads/${encodeURIComponent(threadId)}`, {
        method: "DELETE",
        credentials: "same-origin",
        headers: { Accept: "application/json" },
      });
      if (response.ok) {
        state.threads = state.threads.filter((item) => item.thread_id !== threadId);
        updateThreadSelector();
        addActivity("已清理空白研究任务。", "success");
      }
      // 409 (has content) or other status: keep the thread untouched.
    } catch (_) {
      // Network hiccups must never block switching.
    }
  }

  async function loadThread(threadId, { reconnect = true, preserveRunStream = false } = {}) {
    if (!threadId) return;
    if (state.threadId && state.threadId !== threadId) {
      // Leaving a blank, never-used conversation: drop it instead of keeping
      // a growing stack of "未命名研究任务" placeholders.
      await pruneThreadIfBlank(state.threadId);
      closeEventStream({ resetCursor: true });
      state.runId = "";
      resetRunStream();
      resetAgents(false);
    }
    const payload = await api(`/threads/${encodeURIComponent(threadId)}`);
    state.threadId = threadId;
    const thread = payload.thread || state.threads.find((item) => item.thread_id === threadId) || { thread_id: threadId };
    const index = state.threads.findIndex((item) => item.thread_id === threadId);
    if (index >= 0) state.threads[index] = { ...state.threads[index], ...thread };
    updateThreadSelector();
    updateTaskContext(thread);
    renderHistory(payload.messages, "chat-messages");
    renderHistory(payload.messages, "logistics-chat-messages");
    positionTaskStreamAfterLatestQuestion();
    renderWorkspaceFiles(payload.files || { inputs: [], outputs: payload.artifacts || [] });
    renderArtifactPreviews(payload.artifacts, "chat-messages");
    renderArtifactPreviews(payload.artifacts, "logistics-chat-messages");
    if (!payload.active_run && !preserveRunStream) resetRunStream();
    if (reconnect && payload.active_run?.run_id) {
      if (state.runId !== payload.active_run.run_id) state.eventSeq = 0;
      state.runId = payload.active_run.run_id;
      setRunUi(true);
      openEventStream(payload.active_run.run_id);
    } else if (!payload.active_run) {
      closeEventStream({ resetCursor: true });
      state.runId = "";
      setRunUi(false);
    }
  }

  async function sweepBlankThreads(threads) {
    // After a reload, conversations that were created but never received a
    // question have no business surviving in the thread list.  The backend
    // DELETE endpoint only removes truly empty threads (409 otherwise), so
    // this sweep can never drop content.
    const result = [];
    for (const thread of threads || []) {
      const tid = String(thread.thread_id || "");
      if (!tid) continue;
      try {
        const response = await fetch(`${API_ROOT}/threads/${encodeURIComponent(tid)}`, {
          method: "DELETE",
          credentials: "same-origin",
          headers: { Accept: "application/json" },
        });
        if (!response.ok) result.push(thread);
      } catch (_) {
        result.push(thread);
      }
    }
    return result;
  }

  async function loadThreads({ createWhenEmpty = true } = {}) {
    const payload = await api("/threads");
    let threads = payload.items || [];
    if (threads.length) {
      threads = await sweepBlankThreads(threads);
    }
    state.threads = threads;
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

  function closeEventStream({ resetCursor = false } = {}) {
    if (state.eventReconnectTimer) window.clearTimeout(state.eventReconnectTimer);
    state.eventReconnectTimer = null;
    state.eventSource?.close();
    state.eventSource = null;
    state.eventSourceRunId = "";
    if (resetCursor) {
      state.eventSeq = 0;
      state.eventReconnectAttempts = 0;
    }
  }

  function handleRunEvent(event) {
    const kind = event.kind;
    const payload = event.payload || {};
    if (kind === "stream_gap") {
      addActivity("连接恢复成功；较早的部分运行明细已超出服务器缓冲区。", "warning");
      return;
    }
    if (kind === "status") {
      addActivity("任务已启动，正在读取技能和可用工具。", "running");
      setAgentState("NTL_Engineer", "运行中", true);
      return;
    }
    if (kind === "stopping") {
      addActivity(payload.text || "正在安全停止任务。", "warning");
      const stop = $("#stop-run");
      if (stop) {
        stop.disabled = true;
        stop.textContent = "停止中";
      }
      return;
    }
    if (kind === "reasoning_delta") {
      (payload.messages || []).forEach((message) => {
        const agentName = canonicalAgentName(message.agent);
        if (message.role === "tool") {
          if (agentName) setAgentState(agentName, "工具完成", true);
          const owner = agentName ? `${AGENTS[agentName]} · ` : "";
          addActivity(`${owner}工具已完成：${message.tool_name || "未命名工具"}`, "success");
          return;
        }
        if (message.role === "assistant" && agentName && message.text) {
          updateAgentStream(agentName, message.text, { messageId: message.message_id, snapshot: true });
          return;
        }
        if (agentName) {
          setAgentState(agentName, "工作中", true);
        }
      });
      return;
    }
    if (kind === "agent_stream_delta") {
      updateAgentStream(payload.agent, payload.text, { messageId: payload.message_id });
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
      finishAgentStreams(success ? "success" : kind);
      addActivity(success ? "已形成研究结论。" : "任务以非完整状态结束。", success ? "success" : "warning");
      const retryHint = payload.retryable ? " 可稍后重试。" : "";
      setRuntimeNotice(`${payload.text || "任务已结束。"}${retryHint}`, success ? "success" : "warning");
      return;
    }
    if (kind === "warning") {
      addActivity(payload.text || "任务出现额外警告。", "warning");
      return;
    }
    if (kind === "artifacts") {
      const artifacts = payload.items || [];
      renderArtifacts(artifacts);
      renderArtifactPreviews(artifacts, "chat-messages");
      if (state.currentTarget === "logistics") renderArtifactPreviews(artifacts, "logistics-chat-messages");
      addActivity("已收集本次任务生成的文件。", "success");
      return;
    }
    if (kind === "done") {
      closeEventStream({ resetCursor: true });
      state.runId = "";
      setRunUi(false);
      finishAgentStreams(payload.status || "success");
      if (state.threadId) loadThread(state.threadId, { reconnect: false, preserveRunStream: true }).catch(() => {});
    }
  }

  async function recoverRunStream(runId) {
    if (!runId || state.runId !== runId) return;
    try {
      const summary = await api(`/runs/${encodeURIComponent(runId)}`);
      if (["success", "error", "interrupted", "partial"].includes(summary.state)) {
        closeEventStream({ resetCursor: true });
        state.runId = "";
        setRunUi(false);
        finishAgentStreams(summary.state);
        if (state.threadId) await loadThread(state.threadId, { reconnect: false, preserveRunStream: true });
        setRuntimeNotice("运行已结束，已从持久化会话恢复结果。", summary.state === "success" ? "success" : "warning");
        return;
      }
      openEventStream(runId);
    } catch (error) {
      if (error.code === "run_not_found") {
        closeEventStream({ resetCursor: true });
        state.runId = "";
        setRunUi(false);
        resetAgents(false);
        if (state.threadId) await loadThread(state.threadId, { reconnect: false }).catch(() => {});
        setRuntimeNotice("服务已恢复，但上一次运行状态已失效；已重新载入持久化会话。", "warning");
        return;
      }
      scheduleRunStreamReconnect(runId);
    }
  }

  function scheduleRunStreamReconnect(runId) {
    if (!runId || state.runId !== runId || state.eventReconnectTimer) return;
    state.eventReconnectAttempts += 1;
    const delayMs = Math.min(15000, 1000 * (2 ** Math.min(state.eventReconnectAttempts - 1, 4)));
    state.eventReconnectTimer = window.setTimeout(() => {
      state.eventReconnectTimer = null;
      recoverRunStream(runId).catch(() => scheduleRunStreamReconnect(runId));
    }, delayMs);
  }

  function openEventStream(runId) {
    if (!runId || (state.eventSource && state.eventSourceRunId === runId)) return;
    closeEventStream();
    const source = new EventSource(`${API_ROOT}/runs/${encodeURIComponent(runId)}/events?after_seq=${encodeURIComponent(state.eventSeq)}`);
    state.eventSource = source;
    state.eventSourceRunId = runId;
    source.onopen = () => {
      state.eventReconnectAttempts = 0;
    };
    source.addEventListener("run", (message) => {
      try {
        const event = JSON.parse(message.data);
        const seq = Number(event.seq || message.lastEventId || 0);
        if (seq && seq <= state.eventSeq) return;
        if (seq) state.eventSeq = seq;
        handleRunEvent(event);
      } catch (_) {
        addActivity("运行事件格式异常。", "warning");
      }
    });
    source.onerror = () => {
      if (state.eventSource !== source) return;
      source.close();
      state.eventSource = null;
      state.eventSourceRunId = "";
      if (state.runId === runId) {
        setRuntimeNotice("运行连接短暂中断，正在从最近事件继续。", "warning");
        scheduleRunStreamReconnect(runId);
      }
    };
  }

  async function sendQuestion(text, target = "main", { displayText = "", requestKey = "" } = {}) {
    const question = String(text || "").trim();
    if (!question) return false;
    if (!state.threadId) await createThread();
    if (state.threadId) state.usedThreads.add(state.threadId);
    if (state.runId) {
      setRuntimeNotice("当前任务仍在运行。请等待完成或先停止任务。", "warning");
      return false;
    }
    state.currentTarget = target;
    const visibleQuestion = String(displayText || question).trim();
    addMessage("chat-messages", "user", visibleQuestion);
    if (target === "logistics") addMessage("logistics-chat-messages", "user", visibleQuestion);
    resetRunStream();
    resetAgents(false);
    setRunUi(true);
    setRuntimeNotice("正在启动研究任务。", "running");
    try {
      const run = await api(`/threads/${encodeURIComponent(state.threadId)}/runs`, {
        method: "POST",
        body: JSON.stringify({ question, model_name: state.modelName, request_key: requestKey }),
      });
      state.eventSeq = 0;
      state.eventReconnectAttempts = 0;
      state.runId = run.run_id;
      openEventStream(run.run_id);
      if (run.deduplicated) setRuntimeNotice("该事件研判已启动，本次重复请求已合并。", "warning");
      return true;
    } catch (error) {
      setRunUi(false);
      resetAgents(false);
      addActivity(error.message, "error");
      setRuntimeNotice(error.message, "error");
      return false;
    }
  }

  async function uploadSelectedFiles(files) {
    if (!state.threadId || !files?.length) return;
    for (const file of files) {
      // Dragging a folder keeps its relative subpath (FILE-API support) so
      // uploads can land in a free-form subdirectory below inputs/.
      const uploadName = String(file.webkitRelativePath || file.name || "").trim();
      setRuntimeNotice(`正在上传 ${file.name}。`, "running");
      const response = await fetch(`${API_ROOT}/threads/${encodeURIComponent(state.threadId)}/uploads/${encodeURIComponent(uploadName)}`, {
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
    await loadWorkspaceFiles();
    setRightRailTab("files", { expand: true });
  }

  function bindTaskControls() {
    const taskStreamToggle = cloneWithoutListeners($("#task-stream-toggle"));
    taskStreamToggle?.addEventListener("click", () => {
      setTaskStreamExpanded(taskStreamToggle.getAttribute("aria-expanded") !== "true");
    });
    const mainForm = cloneWithoutListeners($("#chat-form"));
    const mainInput = $("#chat-input", mainForm);
    mainForm?.addEventListener("submit", (event) => {
      event.preventDefault();
      const value = mainInput?.value || "";
      const pending = state.pendingMonitorDraft;
      const requestKey = pending && value.includes(pending.title)
        ? `monitor:${pending.eventKey}`
        : "";
      if (mainInput) mainInput.value = "";
      state.pendingMonitorDraft = null;
      sendQuestion(value, "main", { requestKey });
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
      stop.disabled = true;
      stop.textContent = "停止中";
      try {
        await api(`/runs/${encodeURIComponent(state.runId)}/cancel`, { method: "POST" });
        setRuntimeNotice("已请求停止任务，将在当前步骤结束后中断。", "warning");
      } catch (error) {
        stop.disabled = false;
        stop.textContent = "停止任务";
        setRuntimeNotice(error.message, "error");
      }
    });
    // Thread switching is handled per-row inside updateThreadSelector();
    // interactions are re-bound after each render, nothing to wire here.

    const logout = cloneWithoutListeners($("#logout-button"));
    logout?.addEventListener("click", logoutAccount);
    const attach = cloneWithoutListeners($("#chat-form .composer-tool"));
    const railUpload = cloneWithoutListeners($("#right-rail-upload"));
    const uploadInput = $("#upload-input");
    attach?.addEventListener("click", () => uploadInput?.click());
    railUpload?.addEventListener("click", () => uploadInput?.click());
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
      setRightRailTab("files", { expand: true });
      $("#artifact-panel")?.scrollIntoView({ behavior: "smooth", block: "nearest" });
    });
    const fileSearch = cloneWithoutListeners($("#workspace-file-search"));
    fileSearch?.addEventListener("input", () => renderWorkspaceFiles(state.workspaceFiles));
    $$("[data-file-root-filter]").forEach((filter) => {
      const next = cloneWithoutListeners(filter);
      next?.addEventListener("click", () => {
        state.fileRootFilter = String(next.dataset.fileRootFilter || "all");
        renderWorkspaceFiles(state.workspaceFiles);
      });
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
    closeEventStream({ resetCursor: true });
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
    state.workspaceFiles = { inputs: [], outputs: [] };
    state.previewFile = null;
    state.previewRequestToken += 1;
    renderWorkspaceFiles(state.workspaceFiles);
    renderWorkspacePreviewEmpty("选择一个文件", "从“项目文件”中打开研究资料或分析产出。");
    setRightRailTab("monitor");
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
    bindRightRailTabs();
    bindRightRailResize();
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
