import { existsSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { parseProfile, stringifyDocument } from "../release/profile-schema.mjs";
export function normalizeBundleInserts(rows, bundleRows) {
  const known = new Map(bundleRows.flatMap(row => row.insert ?? []).filter(row=>row.id && row.name).map(row=>[row.id,row.name]));
  let changed = false;
  const result = [];
  for (const row of rows) {
    if (!Array.isArray(row.insert)) { result.push(row); continue; }
    let pending=[];
    const flush=()=>{if(pending.length){result.push({...row,insert:pending});pending=[];}};
    for(const entry of row.insert) {
      if (!known.has(entry.id)) { pending.push(entry); continue; }
      if (known.get(entry.id)!==entry.name) throw new Error(`插件ID冲突：${entry.id} 被不同插件使用，请核对配置`);
      if (Object.keys(entry).some(key=>!["id","name","config","disabled"].includes(key))) throw new Error(`重复插件含额外配置，需人工合并：${entry.id}`);
      flush(); changed=true;
      if (entry.config!==undefined || entry.disabled!==undefined) result.push({id:entry.id,...(entry.config!==undefined?{config:entry.config}:{}),...(entry.disabled!==undefined?{disabled:entry.disabled}:{})});
    }
    flush();
  }
  return changed ? result : null;
}
export function migrateBundleInserts(profile, root) {
  const file=path.join(profile,"cordis.patch.yml"), manifest=path.join(profile,"package.json");
  if(!existsSync(file)||!existsSync(manifest))return;
  const pkg=JSON.parse(readFileSync(manifest,"utf8")), bundleRows=[];
  for(const name of pkg.dsh?.profile?.bundles??[]) {
    if(!/^(@[a-z0-9_-]+\/)?[a-z0-9_.-]+$/i.test(name))continue;
    const dir=[path.join(profile,"node_modules",name),path.join(root,"node_modules",name)].find(dir=>existsSync(path.join(dir,"cordis.patch.yml")));
    if(dir)bundleRows.push(...parseProfile(readFileSync(path.join(dir,"cordis.patch.yml"),"utf8")));
  }
  const original=readFileSync(file,"utf8"), result=normalizeBundleInserts(parseProfile(original),bundleRows);
  if(!result)return;
  const backup=file+".before-bundle-dedup-"+Date.now();writeFileSync(backup,original,{flag:"wx"});
  writeFileSync(file,stringifyDocument(result));
}
