import { readdir, readFile, access, rm } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
const root=path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const decoder=new TextDecoder('utf-8',{fatal:true});let textFiles=0,modules=0,links=0;
async function walk(directory){
  for(const entry of await readdir(directory,{withFileTypes:true})){
    if(['.runtime','.playwright-cli','__pycache__'].includes(entry.name)||entry.name==='.env'||entry.name.startsWith('node_modules'))continue;
    const file=path.join(directory,entry.name);
    if(entry.isDirectory()){await walk(file);continue;}
    if(!/\.(md|json|mjs|js|css|html|py|yml|txt|patch)$|LICENSE$|\.example$/.test(file))continue;
    const text=decoder.decode(await readFile(file));textFiles++;
    if(/\.(mjs|js)$/.test(file)){
      const result=spawnSync(process.execPath,['--check',file],{encoding:'utf8',windowsHide:true});if(result.status)throw new Error(result.stderr);modules++;
    }
    if(file.endsWith('.md')){
      // Link scanning skips fenced blocks and inline code: a documented placeholder
      // such as `![说明](url)` is a template for authors, not a broken link.
      const prose=text.replace(/```[\s\S]*?```/g,'').replace(/`[^`\n]*`/g,'');
      for(const match of prose.matchAll(/\[[^\]]+\]\(([^)]+)\)/g)){
        const target=match[1];if(/^(https?:|#|mailto:)/.test(target))continue;
        await access(path.resolve(path.dirname(file),decodeURIComponent(target.split('#')[0])));links++;
      }
    }
  }
}
await walk(root);
const index=path.join(root,'.runtime/verification.index');
const env={...process.env,GIT_INDEX_FILE:index};
const git=args=>{const result=spawnSync('git',args,{cwd:path.dirname(root),env,encoding:'utf8',windowsHide:true,maxBuffer:8*1024*1024});if(result.status)throw new Error(result.stderr||result.stdout);return result.stdout;};
try{
  git(['read-tree','HEAD']);git(['add','--','dsh','AGENTS.md']);git(['diff','--cached','--check']);
  const filenames=git(['diff','--cached','--name-only']).trim().split('\n');
  const tracked=git(['ls-files','dsh/scripts/start.mjs','dsh/scripts/admin.mjs','dsh/scripts/restart.ps1','dsh/scripts/bootstrap.mjs','dsh/scripts/check-env.mjs']);
  if(tracked.trim().split('\n').length!==5)throw new Error('Deployment scripts are excluded from Git');
  if(filenames.some(f=>/\/\.runtime\/|\/node_modules\/|\/\.env$/.test(f)))throw new Error('Private runtime file would be staged');
  console.log(JSON.stringify({passed:true,textFiles,syntaxModules:modules,localMarkdownLinks:links,reviewFiles:filenames.length}));
  console.log(git(['diff','--cached','--stat']));
}finally{await rm(index,{force:true});}
