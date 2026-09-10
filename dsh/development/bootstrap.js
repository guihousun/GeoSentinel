(() => {
  const prefix = "/geo/development";
  const route = (input) => {
    const url = new URL(input, location.href);
    if (url.host === location.host && !url.pathname.startsWith("/geo/")) url.pathname = prefix + url.pathname;
    return url.href;
  };
  const fetch = window.fetch.bind(window);
  const expired = () => { if (window.parent !== window) window.parent.postMessage({ type: "geo:development-expired" }, location.origin); else location.replace("/geo/native/#development"); };
  window.fetch = async (input, init) => {
    const routed = input instanceof Request ? new Request(route(input.url), input) : route(input);
    const response = await fetch(routed, init);
    if (response.status === 401 && new URL(input instanceof Request ? routed.url : routed).pathname.startsWith(prefix + "/")) expired();
    return response;
  };
  const NativeSocket = window.WebSocket;
  window.WebSocket = class extends NativeSocket { constructor(url, protocols) { super(route(url), protocols); this.addEventListener("close", (event) => { if (event.code === 1008 && event.reason === "Access revoked") expired(); }); } };
  const NativeEvents = window.EventSource;
  window.EventSource = class extends NativeEvents { constructor(url, options) { super(route(url), options); } };
  window.__DSH_TRANSPORT__ = { fetch: window.fetch, ownsHost: true };
  window.__GEOSENTINEL_DEVELOPMENT__ = true;
  if (window.parent !== window) {
    document.documentElement.dataset.geoEmbedded = "true";
    const style = document.createElement("style");
    style.textContent = '[data-geo-embedded] div:has(> div > [data-slot="sidebar"]){grid-template-columns:0px minmax(0,1fr) var(--geo-embedded-details,0px)!important}[data-geo-embedded] [data-slot="sidebar"]{visibility:hidden}[data-geo-embedded] [data-slot="sidebar"] [role="dialog"]{visibility:visible}';
    document.head.append(style);
  }
})();
