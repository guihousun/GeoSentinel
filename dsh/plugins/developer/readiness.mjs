/** The listener alone is not readiness: native services activate asynchronously. */
export function announceDevelopmentReady(ctx, send) {
  return ctx.inject(["sessionController", "workspaceRegistry", "fileUploads"], (ready) => {
    send({ type: "geosentinel:development-ready", url: ready.connection.authenticatedUrl(`http://127.0.0.1:${ready.webServer.port}`) });
  });
}
