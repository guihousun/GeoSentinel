import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, mkdir, writeFile, rm } from "node:fs/promises";
import path from "node:path";
import { tmpdir } from "node:os";
import { launchCompatibleRelease } from "../release/compatible-runtime.mjs";
test("legacy launch rejects tampered snapshots before importing runtime modules", async () => {
  await assert.rejects(launchCompatibleRelease({verify:async()=>{throw new Error("tampered")}},"id",{}),/tampered/);
});
test("legacy compatibility cannot launch an unpublished legacy candidate", async t => {
  const root=await mkdtemp(path.join(tmpdir(),"geo-legacy-launch-"));t.after(()=>rm(root,{recursive:true,force:true}));
  await mkdir(path.join(root,"app/dsh"),{recursive:true});
  await writeFile(path.join(root,"app/dsh/package.json"),JSON.stringify({dependencies:{"@deepseek-ai/dsh":"0.1.2-rc.1"}}));
  await assert.rejects(launchCompatibleRelease({verify:async()=>{},releaseRoot:()=>root,state:async()=>({active:"other",history:[]})},"id",{}),/已发布/);
});
