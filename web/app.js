const LIGHT_MONITOR_MAP_THEMES = new Set(["fiori-shell", "nautical-light", "satellite-gray"]);

function monitorMapTheme(themeId = document.documentElement.dataset.previewTheme || "default") {
  return LIGHT_MONITOR_MAP_THEMES.has(themeId) ? "light" : "dark";
}

function createMapStyle(theme = "dark") {
  // Esri street map carries country boundaries, place labels and terrain
  // shading natively. The light preview themes show it as-is; the dark
  // workbench applies a strong luminance/desaturation filter so the SAME
  // boundary-bearing base reads as a dark theme instead of a separate
  // "dark gray canvas" (which has almost no boundary rendering at low zoom).
  const tileUrls = [
    "https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{y}/{x}",
  ];
  const rasterPaint = theme === "light"
    ? {
        "raster-saturation": -0.08,
        "raster-contrast": 0.05,
        "raster-brightness-min": 0.12,
        "raster-brightness-max": 0.94,
        "raster-fade-duration": 0,
      }
    : {
        "raster-saturation": -0.75,
        "raster-contrast": 0.2,
        "raster-brightness-min": 0.16,
        "raster-brightness-max": 0.62,
        "raster-fade-duration": 0,
      };
  return {
    version: 8,
    sources: {
      cartoBasemap: {
        type: "raster",
        tiles: tileUrls,
        tileSize: 256,
        maxzoom: 19,
        attribution: "Esri, HERE, Garmin, © OpenStreetMap contributors",
      },
    },
    layers: [
      { id: "carto-basemap", type: "raster", source: "cartoBasemap", paint: rasterPaint },
    ],
  };
}

function addStandardMapControls(map) {
  map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "bottom-right");
  map.addControl(new maplibregl.AttributionControl({ compact: true }), "bottom-right");
}

let monitorLocations = [];

function monitorPointItems(events) {
  return (Array.isArray(events) ? events : []).flatMap((event) => {
    const lng = Number(event.longitude);
    const lat = Number(event.latitude);
    if (!Number.isFinite(lng) || !Number.isFinite(lat) || Math.abs(lng) > 180 || Math.abs(lat) > 90) return [];
    const level = ["high", "medium", "low"].includes(String(event.severity || "")) ? String(event.severity) : "low";
    return [{
      name: String(event.display_title || event.title || "未命名监测事件"),
      detail: String(event.summary || ""),
      source: String(event.source_name || ""),
      location: String(event.display_location || event.location_name || event.country || ""),
      lng,
      lat,
      level,
      radius: level === "high" ? 22 : level === "medium" ? 17 : 13,
    }];
  });
}

const logisticsNodes = [
  { name: "Paletwa–Kaladan", lng: 93.02, lat: 21.39, role: "chokepoint", detail: "战略瓶颈点 · 河运、领土控制与粮食通达交汇" },
  { name: "Gangaw / Yaw", lng: 94.11, lat: 22.18, role: "source", detail: "生产源与转运界面 · 东部干旱区入口" },
  { name: "Rikhawdar–Mizoram", lng: 93.64, lat: 22.91, role: "gateway", detail: "跨境终端门户 · 西向替代入口" },
  { name: "Sittwe", lng: 92.90, lat: 20.15, role: "gateway", detail: "Kaladan 走廊沿海端点" },
  { name: "Kyauktaw", lng: 93.55, lat: 20.75, role: "chokepoint", detail: "南部河运与陆路连接节点" },
];

const logisticsCorridors = [
  { name: "Kaladan riverine corridor", role: "chokepoint", coordinates: [[92.90, 20.15], [93.55, 20.75], [93.02, 21.39]] },
  { name: "Dry-zone gateway belt", role: "source", coordinates: [[94.11, 22.18], [93.82, 21.76], [93.02, 21.39]] },
  { name: "Rikhawdar–Mizoram interface", role: "gateway", coordinates: [[93.64, 22.91], [93.38, 22.45], [93.02, 21.39]] },
];

const logisticsRasterManifest = window.FOOD_LOGISTICS_LAYER_MANIFEST || { layers: [] };
const logisticsRasterLayers = Object.fromEntries((logisticsRasterManifest.layers || []).map((layer) => [layer.key, layer]));
const logisticsNetworkLayerIds = ["logistics-corridor-lines", "logistics-node-rings", "logistics-node-points"];
const logisticsAdminLayerIds = ["myanmar-admin-fill", "myanmar-admin-lines"];
const logisticsRasterLayerOrder = ["accessibility", "food", "settlement"];

// Raster preview assets ship with the frontend package; probe them so a missing
// asset degrades to an honest "not deployed" state instead of a console 404.
const logisticsRasterUnavailable = new Set();

function probeLogisticsRasterAssets() {
  const probes = Object.entries(logisticsRasterLayers).map(async ([key, layer]) => {
    try {
      const response = await fetch(layer.url, { method: "HEAD", cache: "no-store" });
      if (!response.ok) logisticsRasterUnavailable.add(key);
    } catch (_) {
      logisticsRasterUnavailable.add(key);
    }
  });
  return Promise.all(probes).then(() => markLogisticsRasterAvailability());
}

function markLogisticsRasterAvailability() {
  if (!logisticsRasterUnavailable.size) return;
  document.querySelectorAll("[data-logistics-raster]").forEach((checkbox) => {
    if (!logisticsRasterUnavailable.has(checkbox.dataset.logisticsRaster)) return;
    checkbox.checked = false;
    checkbox.disabled = true;
    const choice = checkbox.closest(".layer-choice");
    if (choice) {
      choice.classList.add("layer-choice-unavailable");
      const em = choice.querySelector("em");
      if (em) em.textContent = "预览资源未随前端部署";
    }
  });
  syncLogisticsLayerState();
}
const myanmarBounds = [
  [92.17274709741929, 9.671713679673076],
  [101.16989157300668, 28.545538862132275],
];

const maps = {};
// Programmatic access for UI integration and diagnostics (no side effects).
window.geointerMaps = maps;
let activeMonitorMapTheme = "";

function pointFeatures(multiplier = 1) {
  return {
    type: "FeatureCollection",
    features: monitorLocations.map((item) => ({
      type: "Feature",
      geometry: { type: "Point", coordinates: [item.lng, item.lat] },
      properties: { ...item, radius: item.radius * multiplier },
    })),
  };
}

function updateMonitorMap(events) {
  monitorLocations = monitorPointItems(events);
  const source = maps.atlas?.getSource("signals");
  if (source) source.setData(pointFeatures(1.5));
}

window.addEventListener("geointer:monitor-events", (event) => {
  updateMonitorMap(event.detail?.items || []);
});

const DEFAULT_MONITOR_VIEW = { center: [104.2, 35.9], zoom: 0.65 };

function registerMonitorSignalLayers(map, multiplier) {
  if (!map.getSource("signals")) map.addSource("signals", { type: "geojson", data: pointFeatures(multiplier) });
  if (!map.getLayer("signal-rings")) {
    map.addLayer({
      id: "signal-rings",
      type: "circle",
      source: "signals",
      paint: {
        "circle-radius": ["get", "radius"],
        "circle-color": ["match", ["get", "level"], "high", "#d75b5d", "medium", "#d59737", "#2e9b91"],
        "circle-opacity": 0.15,
        "circle-stroke-width": 1,
        "circle-stroke-color": ["match", ["get", "level"], "high", "#f49898", "medium", "#f1be6e", "#80d8ce"],
      },
    });
  }
  if (!map.getLayer("signal-points")) {
    map.addLayer({
      id: "signal-points",
      type: "circle",
      source: "signals",
      paint: { "circle-radius": 5, "circle-color": ["match", ["get", "level"], "high", "#d75b5d", "medium", "#d59737", "#2e9b91"], "circle-stroke-width": 2, "circle-stroke-color": "#eaf2fb" },
    });
  }
}

function applyMonitorMapTheme(themeId) {
  const theme = monitorMapTheme(themeId);
  const pane = document.getElementById("atlas-map")?.closest(".compact-monitor-pane");
  if (pane) pane.dataset.mapTheme = theme;
  if (!maps.atlas || activeMonitorMapTheme === theme) return;
  activeMonitorMapTheme = theme;
  maps.atlas.setStyle(createMapStyle(theme));
}

window.addEventListener("geointer:theme-change", (event) => {
  applyMonitorMapTheme(event.detail?.themeId || "default");
});

function createMap(holderId, multiplier = 1, center = DEFAULT_MONITOR_VIEW.center, zoom = DEFAULT_MONITOR_VIEW.zoom) {
  const holder = document.getElementById(holderId);
  if (!holder || !window.maplibregl) return null;

  const theme = monitorMapTheme();
  activeMonitorMapTheme = theme;
  const pane = holder.closest(".compact-monitor-pane");
  if (pane) pane.dataset.mapTheme = theme;

  const map = new maplibregl.Map({
    container: holder,
    style: createMapStyle(theme),
    center,
    zoom,
    minZoom: 0.55,
    maxZoom: 6,
    attributionControl: false,
    interactive: true,
  });

  addStandardMapControls(map);
  map.on("style.load", () => registerMonitorSignalLayers(map, multiplier));
  map.on("load", () => {
    holder.querySelector(".map-loading")?.remove();
    registerMonitorSignalLayers(map, multiplier);
    map.on("mouseenter", "signal-points", () => { map.getCanvas().style.cursor = "pointer"; });
    map.on("mouseleave", "signal-points", () => { map.getCanvas().style.cursor = ""; });
    map.on("click", "signal-points", (event) => {
      const data = event.features?.[0]?.properties;
      if (!data) return;
      const popup = document.createElement("div");
      const title = document.createElement("strong");
      title.textContent = data.name || "未命名监测事件";
      const metadata = document.createElement("span");
      metadata.textContent = [data.source, data.location, data.level === "high" ? "高关注" : data.level === "medium" ? "持续跟踪" : "观察"].filter(Boolean).join(" · ");
      popup.append(title, document.createElement("br"), metadata);
      new maplibregl.Popup({ closeButton: false, offset: 10 })
        .setLngLat(event.lngLat)
        .setDOMContent(popup)
        .addTo(map);
    });
  });
  return map;
}

function createCountryProfileMap(holderId) {
  const holder = document.getElementById(holderId);
  if (!holder || !window.maplibregl) return null;

  const map = new maplibregl.Map({
    container: holder,
    style: createMapStyle("dark"),
    center: [96.67, 19.11],
    zoom: 4.4,
    minZoom: 3.2,
    maxZoom: 8,
    attributionControl: false,
    interactive: true,
  });

  addStandardMapControls(map);
  map.on("load", () => {
    holder.querySelector(".map-loading")?.remove();
    map.addSource("myanmar-profile-admin", {
      type: "geojson",
      data: "assets/boundaries/myanmar-adm1.geojson",
    });
    map.addLayer({
      id: "myanmar-profile-fill",
      type: "fill",
      source: "myanmar-profile-admin",
      paint: { "fill-color": "#315f87", "fill-opacity": 0.18 },
    });
    map.addLayer({
      id: "myanmar-profile-lines",
      type: "line",
      source: "myanmar-profile-admin",
      paint: { "line-color": "#9ac4e6", "line-width": 1.05, "line-opacity": 0.82 },
    });
    focusMyanmar(map);
  });
  return map;
}

function createLogisticsMap(holderId) {
  const holder = document.getElementById(holderId);
  if (!holder || !window.maplibregl) return null;

  const map = new maplibregl.Map({
    container: holder,
    style: createMapStyle("dark"),
    center: [96.67, 19.11],
    zoom: 4.4,
    minZoom: 3.8,
    maxZoom: 8,
    attributionControl: false,
    interactive: true,
  });

  addStandardMapControls(map);
  map.on("load", () => {
    holder.querySelector(".map-loading")?.remove();
    registerMyanmarAdministrativeBoundaries(map);
    registerLogisticsRasterLayers(map);
    registerMyanmarAdministrativeOutline(map);
    map.addSource("logistics-corridors", {
      type: "geojson",
      data: {
        type: "FeatureCollection",
        features: logisticsCorridors.map((corridor) => ({
          type: "Feature",
          geometry: { type: "LineString", coordinates: corridor.coordinates },
          properties: corridor,
        })),
      },
    });
    map.addLayer({
      id: "logistics-corridor-lines",
      type: "line",
      source: "logistics-corridors",
      paint: {
        "line-color": ["match", ["get", "role"], "chokepoint", "#df6d6f", "source", "#d9a04e", "#58b9ac"],
        "line-width": 3,
        "line-opacity": 0.8,
        "line-dasharray": [2, 1],
      },
    });
    map.addSource("logistics-nodes", {
      type: "geojson",
      data: {
        type: "FeatureCollection",
        features: logisticsNodes.map((node) => ({
          type: "Feature",
          geometry: { type: "Point", coordinates: [node.lng, node.lat] },
          properties: node,
        })),
      },
    });
    map.addLayer({
      id: "logistics-node-rings",
      type: "circle",
      source: "logistics-nodes",
      paint: {
        "circle-radius": ["match", ["get", "role"], "chokepoint", 15, "source", 12, 11],
        "circle-color": ["match", ["get", "role"], "chokepoint", "#df6d6f", "source", "#d9a04e", "#58b9ac"],
        "circle-opacity": 0.16,
        "circle-stroke-width": 1,
        "circle-stroke-color": ["match", ["get", "role"], "chokepoint", "#f4a0a1", "source", "#f1c778", "#8de0d3"],
      },
    });
    map.addLayer({
      id: "logistics-node-points",
      type: "circle",
      source: "logistics-nodes",
      paint: {
        "circle-radius": ["match", ["get", "role"], "chokepoint", 5, 4],
        "circle-color": ["match", ["get", "role"], "chokepoint", "#df6d6f", "source", "#d9a04e", "#58b9ac"],
        "circle-stroke-width": 1.5,
        "circle-stroke-color": "#f2f7fc",
      },
    });
    map.on("mouseenter", "logistics-node-points", () => { map.getCanvas().style.cursor = "pointer"; });
    map.on("mouseleave", "logistics-node-points", () => { map.getCanvas().style.cursor = ""; });
    map.on("click", "logistics-node-points", (event) => {
      const data = event.features?.[0]?.properties;
      if (!data) return;
      new maplibregl.Popup({ closeButton: false, offset: 10 })
        .setLngLat(event.lngLat)
        .setHTML(`<strong>${data.name}</strong><br><span>${data.detail}</span>`)
        .addTo(map);
      selectLogisticsNode(data.name, Number(data.lng), Number(data.lat));
    });
    syncLogisticsLayerState();
    focusMyanmar(map);
  });
  return map;
}

function registerLogisticsRasterLayers(map) {
  const layers = [
    ...logisticsRasterLayerOrder.map((key) => logisticsRasterLayers[key]).filter(Boolean),
    ...Object.values(logisticsRasterLayers).filter((layer) => !logisticsRasterLayerOrder.includes(layer.key)),
  ];
  layers.forEach((layer) => {
    if (logisticsRasterUnavailable.has(layer.key)) return;
    const sourceId = `logistics-raster-${layer.key}`;
    const layerId = `${sourceId}-layer`;
    map.addSource(sourceId, { type: "image", url: layer.url, coordinates: layer.coordinates });
    map.addLayer({
      id: layerId,
      type: "raster",
      source: sourceId,
      paint: {
        "raster-opacity": layer.key === "accessibility" ? 0.34 : layer.key === "settlement" ? 0.96 : 0.56,
        "raster-fade-duration": 0,
        "raster-resampling": layer.map_resampling || "linear",
      },
      layout: { visibility: "visible" },
    });
  });
}

function registerMyanmarAdministrativeBoundaries(map) {
  map.addSource("myanmar-admin", {
    type: "geojson",
    data: "assets/boundaries/myanmar-adm1.geojson",
  });
  map.addLayer({
    id: "myanmar-admin-fill",
    type: "fill",
    source: "myanmar-admin",
    paint: {
      "fill-color": "#83a8c8",
      "fill-opacity": 0.045,
    },
  });
}

function registerMyanmarAdministrativeOutline(map) {
  map.addLayer({
    id: "myanmar-admin-lines",
    type: "line",
    source: "myanmar-admin",
    paint: {
      "line-color": "#8eb4d5",
      "line-width": 1.1,
      "line-opacity": 0.78,
    },
  });
}

function focusMyanmar(map, duration = 0) {
  if (!map) return;
  map.fitBounds(myanmarBounds, {
    padding: { top: 58, right: 46, bottom: 58, left: 46 },
    maxZoom: 5.05,
    duration,
  });
}

function updateLogisticsLayerCopy({ adminEnabled, networkEnabled, rasterEnabled }) {
  const layerState = document.getElementById("logistics-layer-state");
  const note = document.getElementById("logistics-layer-note");
  const networkLegend = document.getElementById("logistics-network-legend");
  const availableKeys = Object.keys(logisticsRasterLayers).filter((key) => !logisticsRasterUnavailable.has(key));
  const visibleRasters = availableKeys.filter((key) => rasterEnabled[key]);
  const rasterLabels = visibleRasters.map((key) => logisticsRasterLayers[key].label);
  const unavailableLabels = Object.keys(logisticsRasterLayers)
    .filter((key) => logisticsRasterUnavailable.has(key))
    .map((key) => logisticsRasterLayers[key].label);
  if (layerState) {
    if (adminEnabled && networkEnabled && availableKeys.length === 3 && visibleRasters.length === 3) {
      layerState.textContent = "行政区划 + 三类空间证据";
    } else {
      const labels = [adminEnabled ? "行政区划" : null, networkEnabled ? "供应网络" : null, ...rasterLabels].filter(Boolean);
      layerState.textContent = labels.length ? labels.join(" + ") : "未显示专题图层";
    }
  }
  if (note) {
    if (unavailableLabels.length && !visibleRasters.length) {
      note.textContent = `${unavailableLabels.join("、")}栅格预览资源未随当前部署提供，暂不可用；行政区划与供应网络仍可正常查看。`;
    } else if (unavailableLabels.length) {
      note.textContent = `当前叠加${rasterLabels.join("、")}。${unavailableLabels.join("、")}预览资源未随当前部署提供；原始 GeoTIFF 保留用于分析。`;
    } else if (visibleRasters.length) {
      note.textContent = `当前叠加${rasterLabels.join("、")}。居民点为稀疏真实已占用像元的位置增强展示，不表示聚落面积；原始 GeoTIFF 保留用于分析。`;
    } else {
      note.textContent = "当前仅显示基础参照图层。可分别启用粮食生产、居民点与可达性栅格，观察其与节点和走廊的空间关系。";
    }
  }
  networkLegend?.classList.toggle("is-hidden", !networkEnabled);
}

function syncLogisticsLayerState() {
  const adminEnabled = document.querySelector('[data-logistics-layer="admin"]')?.checked ?? true;
  const networkEnabled = document.querySelector('[data-logistics-layer="network"]')?.checked ?? true;
  const rasterEnabled = Object.fromEntries(
    Object.keys(logisticsRasterLayers).map((key) => [key, document.querySelector(`[data-logistics-raster="${key}"]`)?.checked ?? true]),
  );
  const map = maps.logistics;
  if (map?.isStyleLoaded()) {
    logisticsAdminLayerIds.forEach((layerId) => {
      if (map.getLayer(layerId)) map.setLayoutProperty(layerId, "visibility", adminEnabled ? "visible" : "none");
    });
    logisticsNetworkLayerIds.forEach((layerId) => {
      if (map.getLayer(layerId)) map.setLayoutProperty(layerId, "visibility", networkEnabled ? "visible" : "none");
    });
    Object.keys(logisticsRasterLayers).forEach((key) => {
      const layerId = `logistics-raster-${key}-layer`;
      if (map.getLayer(layerId)) map.setLayoutProperty(layerId, "visibility", rasterEnabled[key] ? "visible" : "none");
    });
  }
  updateLogisticsLayerCopy({ adminEnabled, networkEnabled, rasterEnabled });
}

function refreshIcons() {
  if (window.lucide) window.lucide.createIcons();
}

function addMessageToStream(streamId, role, text, tags = [], author = "地缘分析师") {
  const stream = document.getElementById(streamId);
  if (!stream) return;
  stream.querySelector(".chat-empty-state")?.remove();
  const article = document.createElement("article");
  article.className = `message ${role}`;
  const avatar = document.createElement("span");
  avatar.className = `message-avatar ${role === "user" ? "human" : "ai"}`;
  const avatarIcon = document.createElement("i");
  avatarIcon.setAttribute("data-lucide", role === "user" ? "user-round" : "bot");
  avatarIcon.setAttribute("aria-hidden", "true");
  avatar.append(avatarIcon);
  const body = document.createElement("div");
  const meta = document.createElement("div");
  meta.className = "message-meta";
  const name = document.createElement("strong");
  name.textContent = role === "user" ? "研究者" : author;
  const time = document.createElement("time");
  time.textContent = "刚刚";
  meta.append(name, time);
  const paragraph = document.createElement("p");
  paragraph.textContent = text;
  body.append(meta, paragraph);
  if (tags.length) {
    const tagRow = document.createElement("div");
    tagRow.className = "message-tags";
    tags.forEach((tag) => {
      const tagNode = document.createElement("span");
      tagNode.textContent = tag;
      tagRow.append(tagNode);
    });
    body.append(tagRow);
  }
  article.append(avatar, body);
  stream.append(article);
  stream.scrollTop = stream.scrollHeight;
}

function addChatMessage(role, text, tags = [], author = "地缘分析师") {
  addMessageToStream("chat-messages", role, text, tags, author);
}

function addLogisticsChatMessage(role, text, tags = [], author = "地缘分析师") {
  addMessageToStream("logistics-chat-messages", role, text, tags, author);
}

function setActiveNav(navName) {
  document.querySelectorAll(".nav-item").forEach((item) => {
    const active = item.dataset.nav === navName;
    item.classList.toggle("active", active);
    if (active) item.setAttribute("aria-current", "page");
    else item.removeAttribute("aria-current");
  });
}

function setWorkspaceView(viewName) {
  const workspace = document.querySelector(".workspace");
  const views = {
    situation: document.getElementById("primary-workspace"),
    profiles: document.getElementById("profiles-module"),
    logistics: document.getElementById("logistics-module"),
  };
  workspace?.setAttribute("data-active-view", viewName);
  Object.entries(views).forEach(([name, view]) => {
    const isActive = name === viewName;
    view?.toggleAttribute("hidden", !isActive);
    view?.setAttribute("aria-hidden", String(!isActive));
  });
  window.scrollTo({ top: 0, left: 0, behavior: "auto" });
}

function setWorkspaceContext(value) {
  const context = document.getElementById("workspace-context");
  if (context) context.textContent = value;
}

function showPrimaryWorkspace({ updateHistory = true } = {}) {
  setWorkspaceView("situation");
  const title = document.getElementById("workspace-title");
  if (title) title.textContent = "地缘环境智能计算平台";
  setWorkspaceContext("当前任务：全球态势研判");
  if (updateHistory && window.location.hash) window.history.replaceState({}, "", `${window.location.pathname}${window.location.search}`);
}


function showProfilesModule({ updateHistory = true } = {}) {
  setWorkspaceView("profiles");
  const title = document.getElementById("workspace-title");
  if (title) title.textContent = "地缘环境智能计算平台";
  setWorkspaceContext("当前案例：缅甸");
  if (updateHistory && window.location.hash !== "#profiles") window.history.pushState({}, "", `${window.location.pathname}${window.location.search}#profiles`);
  if (!maps.profiles) maps.profiles = createCountryProfileMap("profile-map");
  window.requestAnimationFrame(() => maps.profiles?.resize());
}

function showLogisticsModule({ updateHistory = true } = {}) {
  setWorkspaceView("logistics");
  const title = document.getElementById("workspace-title");
  if (title) title.textContent = "地缘环境智能计算平台";
  setWorkspaceContext("专题案例：缅甸西部粮食安全");
  if (updateHistory && window.location.hash !== "#logistics") window.history.pushState({}, "", `${window.location.pathname}${window.location.search}#logistics`);
  if (!maps.logistics) maps.logistics = createLogisticsMap("logistics-map");
  window.requestAnimationFrame(() => maps.logistics?.resize());
}

function syncWorkspaceRoute() {
  const viewByHash = {
    "#profiles": ["profiles", showProfilesModule],
    "#logistics": ["logistics", showLogisticsModule],
  };
  const route = viewByHash[window.location.hash];
  if (!route) {
    if (window.location.hash === "#events") window.history.replaceState({}, "", `${window.location.pathname}${window.location.search}`);
    setActiveNav("situation");
    showPrimaryWorkspace({ updateHistory: false });
    return;
  }
  setActiveNav(route[0]);
  route[1]({ updateHistory: false });
}

function wireNavigation() {
  document.querySelectorAll(".nav-item").forEach((button) => {
    button.addEventListener("click", (event) => {
      const nav = button.dataset.nav;
      event.preventDefault();
      if (nav === "profiles") showProfilesModule();
      else if (nav === "logistics") showLogisticsModule();
      else showPrimaryWorkspace();
      setActiveNav(nav);
    });
  });
  document.getElementById("back-to-situation")?.addEventListener("click", () => {
    showPrimaryWorkspace();
    setActiveNav("situation");
  });
  window.addEventListener("popstate", syncWorkspaceRoute);
  window.addEventListener("hashchange", syncWorkspaceRoute);
}

function wireConsole() {
  document.querySelectorAll("[data-command]").forEach((button) => {
    button.addEventListener("click", () => {
      const command = button.dataset.command;
      const title = document.getElementById("task-title");
      if (title) title.textContent = `${command} · 已加入当前任务`;
      addChatMessage("assistant", `已把“${command}”加入当前研究任务。需要时我会调用相应的数据与分析工具。`, ["任务上下文", command]);
    });
  });
  document.getElementById("new-task")?.addEventListener("click", () => {
    const title = document.getElementById("task-title");
    if (title) title.textContent = "新研究任务 · 等待输入";
    addChatMessage("assistant", "已建立新的研究任务。请在下方输入问题，我会从任务边界和可用技能开始规划。", ["新线程"]);
  });
}

function wireModuleActions() {
  document.querySelectorAll("[data-event-task]").forEach((button) => {
    button.addEventListener("click", () => {
      const prompt = button.dataset.eventTask || "";
      showPrimaryWorkspace();
      setActiveNav("situation");
      const input = document.getElementById("chat-input");
      if (input) {
        input.value = prompt;
        input.focus();
      }
      addChatMessage("assistant", "已将该监测线索转为研究任务。请确认研究区域与时间范围后继续。", ["事件监测", "待核验"]);
    });
  });

  document.getElementById("profile-open-logistics")?.addEventListener("click", () => {
    showLogisticsModule();
    setActiveNav("logistics");
  });
}

function wireAgents() {
  document.querySelectorAll(".agent-row").forEach((button) => {
    button.addEventListener("click", () => {
      const agent = button.dataset.agent || "智能体";
      addChatMessage("assistant", `${agent} 的运行状态已展开。当前页面保留主对话，详细过程在右侧看板中持续更新。`, ["运行状态"], agent);
    });
  });
}

function resizeComposer(textarea) {
  if (!textarea) return;
  textarea.style.height = "auto";
  textarea.style.height = `${Math.min(textarea.scrollHeight, 96)}px`;
}

function wireChat() {
  const form = document.getElementById("chat-form");
  const input = document.getElementById("chat-input");
  input?.addEventListener("input", () => resizeComposer(input));
  resizeComposer(input);
  form?.addEventListener("submit", (event) => {
    event.preventDefault();
    const value = input?.value.trim();
    if (!value) return;
    addChatMessage("user", value);
    if (input) {
      input.value = "";
      resizeComposer(input);
    }
    window.setTimeout(() => addChatMessage("assistant", "已收到任务。我会先读取当前研究技能，再判断需要调度的事件、空间和证据工具。", ["规划中", "等待执行"]), 220);
  });
  input?.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      form?.requestSubmit();
    }
  });
  document.getElementById("clear-chat")?.addEventListener("click", () => {
    const stream = document.getElementById("chat-messages");
    if (stream) stream.replaceChildren();
    addChatMessage("assistant", "本轮对话已清空。当前线程和右侧调度看板仍保留。", ["已重置"]);
  });
}

function wireMapControls() {
  document.querySelectorAll("[data-map-action]").forEach((button) => {
    button.addEventListener("click", () => {
      const action = button.dataset.mapAction;
      const pane = document.querySelector(".map-pane");
      if (action === "reset" && maps.atlas) maps.atlas.flyTo({ center: DEFAULT_MONITOR_VIEW.center, zoom: DEFAULT_MONITOR_VIEW.zoom, duration: 500 });
      if (action === "layers") pane?.classList.toggle("layer-muted");
      if (action === "fullscreen") pane?.classList.toggle("is-expanded");
      window.setTimeout(() => maps.atlas?.resize(), 50);
    });
  });
}

function selectLogisticsNode(name, lng, lat) {
  const focus = document.getElementById("logistics-context-node");
  const copy = document.getElementById("logistics-context-copy");
  const input = document.getElementById("logistics-chat-input");
  const node = logisticsNodes.find((item) => item.name === name);
  if (focus) focus.textContent = name;
  if (copy) copy.textContent = node?.detail || "已选择专题节点，等待详细证据核验。";
  if (input) input.placeholder = `例如：评估 ${name} 受阻后对周边聚落可达性的影响`;
  if (maps.logistics) maps.logistics.flyTo({ center: [lng, lat], zoom: 6.4, duration: 650 });
}

function getLogisticsThreadContext() {
  const nodeName = document.getElementById("logistics-context-node")?.textContent?.trim() || "尚未选择节点";
  const nodeDetail = document.getElementById("logistics-context-copy")?.textContent?.trim() || "未选择节点";
  const layerState = document.getElementById("logistics-layer-state")?.textContent?.trim() || "物流网络";
  return { nodeName, nodeDetail, layerState };
}

function wireLogisticsModule() {
  document.querySelectorAll("[data-logistics-action]").forEach((button) => {
    button.addEventListener("click", () => {
      const action = button.dataset.logisticsAction;
      const pane = document.querySelector(".logistics-map-pane");
      if (action === "reset" && maps.logistics) focusMyanmar(maps.logistics, 550);
      if (action === "layers") pane?.classList.toggle("layers-open");
      window.setTimeout(() => maps.logistics?.resize(), 50);
    });
  });
  document.querySelectorAll("[data-logistics-layer], [data-logistics-raster]").forEach((control) => {
    control.addEventListener("change", () => {
      syncLogisticsLayerState();
    });
  });
}

function wireLogisticsChat() {
  const form = document.getElementById("logistics-chat-form");
  const input = document.getElementById("logistics-chat-input");
  input?.addEventListener("input", () => resizeComposer(input));
  resizeComposer(input);
  form?.addEventListener("submit", (event) => {
    event.preventDefault();
    const value = input?.value.trim();
    if (!value) return;
    const context = getLogisticsThreadContext();
    const hasNode = context.nodeName !== "尚未选择节点";
    addLogisticsChatMessage("user", value);
    addChatMessage("user", value);
    if (input) {
      input.value = "";
      resizeComposer(input);
    }

    window.setTimeout(() => {
      const response = hasNode
        ? `已将 ${context.nodeName} 作为空间焦点。接下来会先核验关联走廊、周边粮食生产腹地和居民点可达性；当前地图证据为“${context.layerState}”。如需形成情景结论，请补充时间窗、事件条件或受阻方式。`
        : `已记录专题问题。请在地图上点选节点，或补充目标走廊与时间窗；地缘分析师会结合当前“${context.layerState}”图层组织后续的节点、走廊与可达性研判。`;
      const tags = hasNode ? ["专题上下文", context.nodeName, "待补充条件"] : ["专题上下文", "待选择空间焦点"];
      addLogisticsChatMessage("assistant", response, tags);
      addChatMessage("assistant", response, tags);
    }, 220);
  });
  input?.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      form?.requestSubmit();
    }
  });
  document.querySelectorAll("[data-logistics-prompt]").forEach((button) => {
    button.addEventListener("click", () => {
      if (!input) return;
      input.value = button.dataset.logisticsPrompt || "";
      resizeComposer(input);
      form?.requestSubmit();
    });
  });
}

function wireExport() {
  document.getElementById("export-brief")?.addEventListener("click", () => {
    addChatMessage("assistant", "已整理当前地图、调度状态与证据链，研究简报已加入导出队列。", ["简报已整理"]);
  });
}

function init() {
  refreshIcons();
  probeLogisticsRasterAssets();
  maps.atlas = createMap("atlas-map", 1.5);
  wireNavigation();
  wireConsole();
  wireModuleActions();
  wireAgents();
  wireChat();
  wireMapControls();
  wireLogisticsModule();
  wireLogisticsChat();
  wireExport();
  syncWorkspaceRoute();
}

if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
else init();
