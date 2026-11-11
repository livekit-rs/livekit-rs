// Actual installed package identity. No SDK source replacement or dependency-closure claim.
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {readFile,realpath,readdir,lstat} from 'node:fs/promises';
import {createReadStream} from 'node:fs';
import {createHash} from 'node:crypto';
import {resolve,dirname,relative,sep} from 'node:path';
import {pathToFileURL} from 'node:url';
const digest=b=>createHash('sha256').update(b).digest('hex');
async function fileHash(path){const h=createHash('sha256');for await(const bytes of createReadStream(path))h.update(bytes);return h.digest('hex');}
export async function sdkIdentity(packagePath,name,expectedVersion,importInNode){
 const interop=await realpath(packagePath),resolver=createRequire(interop);
 const entry=await realpath(resolver.resolve(name));let root=dirname(entry),pkg,pkgPath,pkgBytes;
 for(;;){
  const candidate=resolve(root,'package.json');
  try{const bytes=await readFile(candidate),parsed=JSON.parse(bytes);if(parsed.name===name){pkg=parsed;pkgPath=candidate;pkgBytes=bytes;break;}}
  catch(error){if(error.code!=='ENOENT')throw error;}
  const parent=dirname(root);assert.notEqual(parent,root,'resolved package root required');root=parent;
 }
 assert.equal(pkg.version,expectedVersion,'actual package version must match locked expected contract');
 const lockPath=resolve(dirname(interop),'package-lock.json'),lockBytes=await readFile(lockPath),lock=JSON.parse(lockBytes);
 assert.equal(lock.packages[''].dependencies[name],pkg.version);
 const selected=lock.packages['node_modules/'+name];assert.equal(selected.version,pkg.version);assert.match(selected.integrity,/^sha512-/);
 const files=[];
 async function walk(dir){for(const ent of await readdir(dir,{withFileTypes:true})){
  if(ent.name==='node_modules')continue;
  const path=resolve(dir,ent.name),st=await lstat(path);assert(!st.isSymbolicLink(),'package symlink requires independent provenance');
  if(st.isDirectory())await walk(path);else{assert(st.isFile());files.push({path:relative(root,path).split(sep).join('/'),sha256:await fileHash(path)});}
 }}
 await walk(root);files.sort((a,b)=>a.path<b.path?-1:a.path>b.path?1:0);
 const bytes=await readFile(entry),entryHash=digest(bytes);
 const module=importInNode?await import(pathToFileURL(entry).href):undefined;
 assert.equal(await fileHash(entry),entryHash,'captured/loaded exact entry unchanged');assert.equal(digest(await readFile(pkgPath)),digest(pkgBytes));
 const nodePath=await realpath(process.execPath);
 return {module,entryBytes:bytes,identity:{name:pkg.name,version:pkg.version,entry_path:entry,entry_sha256:entryHash,actual_use:importInNode?'createRequire require-export resolution then exact file URL dynamic import':'createRequire require-export resolution; exact captured unchanged UMD bytes served as browser SDK',package_json_path:pkgPath,package_json_sha256:digest(pkgBytes),package_tree_sha256:digest(Buffer.from(JSON.stringify(files))),package_tree_files:files.length,package_tree_scope:'all regular package files excluding nested node_modules; not full dependency closure',interop_package_path:interop,interop_package_sha256:await fileHash(interop),lockfile_path:lockPath,lockfile_sha256:digest(lockBytes),selected_lock_integrity:selected.integrity,node_path:nodePath,node_version:process.version,node_sha256:await fileHash(nodePath)}};
}
