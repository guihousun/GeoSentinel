import test from "node:test";
import assert from "node:assert/strict";
import { announceDevelopmentReady } from "../plugins/developer/readiness.mjs";
test("development advertises readiness only after all native request services activate", () => {
  for (const port of [12345, 23456]) {
    const sent = []; let activate;
    const ctx = { inject(names, callback) { assert.deepEqual(names, ["sessionController", "workspaceRegistry", "fileUploads"]); activate = callback; return "dispose"; } };
    assert.equal(announceDevelopmentReady(ctx, m => sent.push(m)), "dispose");
    assert.deepEqual(sent, []);
    activate({ connection: { authenticatedUrl: url => url + "?token=test" }, webServer: { port } });
    assert.deepEqual(sent, [{ type: "geosentinel:development-ready", url: `http://127.0.0.1:${port}?token=test` }]);
  }
});
