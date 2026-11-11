// Unchanged official ServerSDK; secrets/JWT/room identity never leave this process.
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {createHmac,timingSafeEqual,randomUUID,createHash} from 'node:crypto';
import {readFile,realpath,readdir,lstat} from 'node:fs/promises';
import {createReadStream} from 'node:fs';
import {dirname,resolve,relative,sep} from 'node:path';
import {pathToFileURL} from 'node:url';
let input=Buffer.alloc(0);for await(const chunk of process.stdin){input=Buffer.concat([input,chunk]);assert(input.length<=65536,'bounded private SDK input');}
const request=JSON.parse(input.toString('utf8'));input=undefined;
// Resolve once using the actual caller package, then import precisely that file.
// This intentionally keeps v1's require export condition (dist/index.cjs), while
// dynamic import is pinned to the same resolved path rather than a second lookup.
const resolver=createRequire(request.interop_package);
const resolvedEntry=await realpath(resolver.resolve('livekit-server-sdk'));
let packageRoot=dirname(resolvedEntry),packageFile,packageBytes,actualPackage;
for(;;){
 const candidate=resolve(packageRoot,'package.json');
 try{const bytes=await readFile(candidate);const value=JSON.parse(bytes);if(value.name==='livekit-server-sdk'){packageFile=candidate;packageBytes=bytes;actualPackage=value;break;}}
 catch(error){if(error.code!=='ENOENT')throw error;}
 const parent=dirname(packageRoot);assert.notEqual(parent,packageRoot,'resolved SDK package root must exist');packageRoot=parent;
}
assert.equal(actualPackage.version,'2.19.2','actual loaded package version, not declared dependency');
const interopPackage=await realpath(request.interop_package);
const lockFile=resolve(dirname(interopPackage),'package-lock.json');
const lockBytes=await readFile(lockFile);const lock=JSON.parse(lockBytes);
assert.equal(lock.packages[''].dependencies['livekit-server-sdk'],'2.19.2');
const selectedLock=lock.packages['node_modules/livekit-server-sdk'];assert.equal(selectedLock.version,actualPackage.version);assert.match(selectedLock.integrity,/^sha512-/);
const hash=bytes=>createHash('sha256').update(bytes).digest('hex');
async function fileHash(path){const h=createHash('sha256');for await(const b of createReadStream(path))h.update(b);return h.digest('hex');}
const packageFiles=[];
async function walk(dir){
 for(const entry of (await readdir(dir,{withFileTypes:true})).sort((a,b)=>a.name.localeCompare(b.name,'en'))){
  if(entry.name==='node_modules')continue; // dependencies are not this published package tree
  const path=resolve(dir,entry.name),st=await lstat(path);assert(!st.isSymbolicLink(),'SDK tree symlink requires separate source review');
  if(st.isDirectory())await walk(path);else{assert(st.isFile(),'regular SDK files only');packageFiles.push({path:relative(packageRoot,path).split(sep).join('/'),sha256:await fileHash(path)});}
 }
}
await walk(packageRoot);packageFiles.sort((a,b)=>a.path<b.path?-1:a.path>b.path?1:0);
const beforeEntry=await fileHash(resolvedEntry);
const sdkModule=await import(pathToFileURL(resolvedEntry).href);
const {AccessToken,RoomServiceClient}=sdkModule.default??sdkModule;
assert.equal(typeof AccessToken,'function');assert.equal(typeof RoomServiceClient,'function');
assert.equal(await fileHash(resolvedEntry),beforeEntry,'entry cannot change while being imported');
assert.equal(hash(await readFile(packageFile)),hash(packageBytes),'package identity stable across import');
const sdkIdentity={name:actualPackage.name,version:actualPackage.version,resolution_condition:'createRequire.resolve require export; exact returned file dynamically imported',
 interop_package_path:interopPackage,interop_package_sha256:await fileHash(interopPackage),package_json_path:packageFile,package_json_sha256:hash(packageBytes),
 entry_path:resolvedEntry,entry_sha256:beforeEntry,package_tree_scope:'all regular files below SDK package root, excluding nested dependency node_modules; no full dependency closure claim',
 package_tree_sha256:hash(Buffer.from(JSON.stringify(packageFiles))),package_tree_files:packageFiles.length,
 lockfile_path:lockFile,lockfile_sha256:hash(lockBytes),lock_selected_integrity:selectedLock.integrity,
 node_exec_path:process.execPath,node_real_path:await realpath(process.execPath),node_version:process.version,node_exec_sha256:await fileHash(await realpath(process.execPath))};
if(process.argv.includes('--identity-only')){process.stdout.write(JSON.stringify({status:'PASS_IDENTITY_ONLY',sdk_version:actualPackage.version,sdk_identity:sdkIdentity})+'\n');process.exit(0);}
const result={status:'FAIL',sdk_version:actualPackage.version,sdk_identity:sdkIdentity,jwt_hs256_valid:false,authenticated_readiness:false,wrong_secret_rejected:false,alternate_file_secret_rejected:false,created_listed_deleted:false,cleanup_errors:[]};
let ownedRoom;let client;let stage='token';
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function make(secret,room=''){
 const mint=new AccessToken(request.key,secret,{ttl:'2m'});mint.addGrant({roomCreate:true,roomList:true,roomAdmin:true,room});
 const token=await mint.toJwt();const parts=token.split('.');assert.equal(parts.length,3);
 const header=JSON.parse(Buffer.from(parts[0],'base64url'));
 assert.equal(header.alg,'HS256');const actual=Buffer.from(parts[2],'base64url');const expected=createHmac('sha256',secret).update(parts[0]+'.'+parts[1]).digest();
 assert.equal(actual.length,expected.length);assert(timingSafeEqual(actual,expected));
 return new RoomServiceClient(request.url,undefined,undefined,{token,requestTimeout:2,failover:false});
}
try {
 client=await make(request.secret);result.jwt_hs256_valid=true;stage='authenticated-readiness';
 const until=Date.now()+20000;let ready=false;
 while(Date.now()<until){try{await client.listRooms();ready=true;break;}catch{await sleep(100);}}
 assert(ready,'actual file secret must authenticate within fixed20s readiness');result.authenticated_readiness=true;
 stage='negative-secret';
 for(const [field,secret] of [['wrong_secret_rejected',request.secret+'-wrong'],['alternate_file_secret_rejected',request.other_secret]]){
  const bad=await make(secret);let rejected=false;try{await bad.listRooms();}catch(error){rejected=error.status===401||error.code==='unauthenticated';}
  assert(rejected,'non-selected file secret must be rejected');result[field]=true;
 }
 stage='actual-crud';ownedRoom='raw-keyfile-'+randomUUID();client=await make(request.secret,ownedRoom);
 const created=await client.createRoom({name:ownedRoom});assert.equal(created.name,ownedRoom);
 assert((await client.listRooms([ownedRoom])).some(room=>room.name===ownedRoom));
 await client.deleteRoom(ownedRoom);assert(!(await client.listRooms([ownedRoom])).some(room=>room.name===ownedRoom));ownedRoom=undefined;
 result.created_listed_deleted=true;result.status='PASS';
}catch{result.failure_stage=stage;}
finally{
 if(ownedRoom)try{await client.deleteRoom(ownedRoom);}catch{result.cleanup_errors.push('owned room delete failed');}
 if(result.cleanup_errors.length)result.status='FAIL';
 process.stdout.write(JSON.stringify(result)+'\n');if(result.status!=='PASS')process.exitCode=1;
}
