import { existsSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { parseProfile, stringifyDocument } from "../release/profile-schema.mjs";
export function normalizeUploadBundle(pkg, rows, bundleRows) {
  if (!pkg.dsh?.profile?.bundles?.includes("dsh-file-upload")) return null;
  const insert = bundleRows.flatMap(row => row.insert ?? []).find(row => row.name === "dsh-file-upload");
  if (!insert) throw new Error("dsh-file-upload bundle has no plugin entry");
  const next = structuredClone(pkg), patch = structuredClone(rows);
  next.dsh.profile.bundles = next.dsh.profile.bundles.filter(name => name !== "dsh-file-upload");
  const entries = patch.flatMap(row => row.insert ?? []);
  if (!entries.some(row => row.name === "dsh-file-upload" && row.id !== "file-upload")) {
    if (entries.some(row => row.id === "geo-file-upload")) throw new Error("geo-file-upload is already used by another plugin");
    patch.push({insert:[{...insert,id:"geo-file-upload"}]});
  }
  return {pkg:next,rows:patch};
}
export function migrateUploadBundle(profile, root) {
  const manifest = path.join(profile,"package.json"), patchFile = path.join(profile,"cordis.patch.yml");
  if (!existsSync(manifest)) return;
  const pkg = JSON.parse(readFileSync(manifest,"utf8"));
  if (!pkg.dsh?.profile?.bundles?.includes("dsh-file-upload")) return;
  const original = existsSync(patchFile) ? readFileSync(patchFile,"utf8") : "[]";
  const result = normalizeUploadBundle(pkg,parseProfile(original),parseProfile(readFileSync(path.join(root,"node_modules/dsh-file-upload/cordis.patch.yml"),"utf8")));
  for (const [file,text] of [[manifest,readFileSync(manifest,"utf8")],[patchFile,original]]) {
    const backup=file+".before-upload-compat";
    if (!existsSync(backup)) writeFileSync(backup,text,{flag:"wx"});
  }
  // Remove the collision first; a interrupted migration leaves native upload available.
  writeFileSync(manifest,JSON.stringify(result.pkg,null,2)+"\n");
  writeFileSync(patchFile,stringifyDocument(result.rows));
}

export function migrateLegacyTeams(profile) {
  const file=path.join(profile,"package.json");
  if (!existsSync(file)) return;
  const text=readFileSync(file,"utf8"), pkg=JSON.parse(text);
  if (!pkg.dsh?.profile?.bundles?.includes("@nanmicoder/dsh-agent-teams")) return;
  const backup=file+".before-legacy-teams-compat";
  if (!existsSync(backup)) writeFileSync(backup,text,{flag:"wx"});
  pkg.dsh.profile.bundles=pkg.dsh.profile.bundles.filter(name=>name!=="@nanmicoder/dsh-agent-teams");
  writeFileSync(file,JSON.stringify(pkg,null,2)+"\n");
}
