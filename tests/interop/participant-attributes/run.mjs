// Coordinator-run only. Real ServerSDK HTTP + authenticated binary WS; NO SDP/RTC claim.
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {readFile,writeFile,realpath} from 'node:fs/promises';
import {dirname,resolve,basename} from 'node:path';
import {randomUUID,createHash,createHmac,timingSafeEqual} from 'node:crypto';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
const exec=promisify(execFile),hash=b=>createHash('sha256').update(b).digest('hex');
const env=process.env,required=['INTEROP_PACKAGE','REFERENCE_URL','LIVEKIT_URL','REFERENCE_PID','RUST_PID','REFERENCE_BIN','RUST_BIN','REFERENCE_CONFIG','RUST_CONFIG','RESULT'];
for(const k of required)assert(env[k],`required ${k}`);
const loader=createRequire(resolve(env.INTEROP_PACKAGE));
async function identity(name,version){const entry=await realpath(loader.resolve(name));let dir=dirname(entry);for(;;){try{const p=resolve(dir,'package.json'),b=await readFile(p),j=JSON.parse(b);if(j.name===name){assert.equal(j.version,version);return {entry,entry_sha256:hash(await readFile(entry)),package_sha256:hash(b),version:j.version};}}catch(e){if(e.code!=='ENOENT')throw e;}const parent=dirname(dir);assert.notEqual(parent,dir);dir=parent;}}
const sdk=await identity('livekit-server-sdk','2.19.2'),protocol=await identity('@livekit/protocol','1.52.1');
const {AccessToken,RoomServiceClient}=loader(sdk.entry),{SignalRequest,SignalResponse}=loader(protocol.entry);
const key=env.LIVEKIT_API_KEY||'devkey',secret=env.LIVEKIT_API_SECRET||'secret',TIMEOUT=10000;
const report={status:'RUNNING',scope:'real ServerSDK control API and authenticated JOINING/JOINED binary WS; no SDP/ICE/RTC/media/quality claim',sdk,protocol,lock_sha256:hash(await readFile(resolve(dirname(env.INTEROP_PACKAGE),'package-lock.json'))),campaigns:[],cleanup_errors:[]};
let stage='setup';async function save(){await writeFile(env.RESULT,JSON.stringify(report,null,2)+'\n',{mode:0o600});}
async function processProof(pidText,binary,config){
 const pid=Number(pidText);assert(Number.isSafeInteger(pid)&&pid>1);process.kill(pid,0);const expected=await realpath(binary),configPath=await realpath(config);let observed,argv;
 try{observed=await realpath(`/proc/${pid}/exe`);assert.equal(observed,expected);argv=(await readFile(`/proc/${pid}/cmdline`)).toString('utf8').split('\0');}
 catch(e){if(e.code!=='ENOENT')throw e;const {stdout}=await exec('/bin/ps',['-p',String(pid),'-o','comm='],{timeout:TIMEOUT,maxBuffer:65536});const value=stdout.trim();assert(value,'actual server PID missing');if(value.startsWith('/'))assert.equal(await realpath(value),expected);else assert.equal(value,basename(expected));observed=value;argv=(await exec('/bin/ps',['-p',String(pid),'-o','command='],{timeout:TIMEOUT,maxBuffer:65536})).stdout.trim().split(/\s+/);}
 // Read actual startup argv privately: never persist it or possible credentials.
 assert(argv.some((arg,i)=>arg==='--config'&&argv[i+1]===configPath||arg==='--config='+configPath),'actual owned process must use this config file');
 const bytes=await readFile(configPath),lines=bytes.toString('utf8').split(/\r?\n/),apiStarts=lines.flatMap((line,i)=>line==='api:'?[i]:[]);assert.equal(apiStarts.length,1,'dedicated canonical api section required');
 let section=[];for(let i=apiStarts[0]+1;i<lines.length;i++){const line=lines[i];if(line&&!line.startsWith(' ')&&!line.startsWith('#'))break;if(line.trim()&&!line.trim().startsWith('#'))section.push(line);}
 assert.deepEqual(section,['  enable_psrpc_for_get_list_participants: true'],'actual profile requires owner get, no stale cache acceptance');
 return {pid_alive:true,pid,executable_observed:observed,binary_sha256:hash(await readFile(expected)),actual_startup_config_sha256:hash(bytes),owner_get_profile:true};
}
function bounded(p,label){let timer;return Promise.race([p,new Promise((_,reject)=>timer=setTimeout(()=>reject(Error(label+' timeout')),TIMEOUT))]).finally(()=>clearTimeout(timer));}
function claims(token){const a=token.split('.');assert.equal(a.length,3);assert.equal(JSON.parse(Buffer.from(a[0],'base64url')).alg,'HS256');const actual=Buffer.from(a[2],'base64url'),expected=createHmac('sha256',secret).update(a[0]+'.'+a[1]).digest();assert.equal(actual.length,expected.length);assert(timingSafeEqual(actual,expected),'actual refreshed token signature');return JSON.parse(Buffer.from(a[1],'base64url'));}
class Peer{
 constructor(url,token,identity,room){this.identity=identity;this.room=room;const initial=claims(token);assert.equal(initial.sub,identity);assert.equal(initial.video.room,room);this.video=initial.video;this.allowedAttributes=[initial.attributes??{}];this.events=[];this.waiters=new Set();this.error=undefined;this.closing=false;this.count=0;
  const u=new URL('/rtc',url);u.protocol=u.protocol.replace('http','ws');u.searchParams.set('access_token',token);u.searchParams.set('protocol','17');u.searchParams.set('sdk','unknown');u.searchParams.set('auto_subscribe','0');
  this.ws=new WebSocket(u);this.ws.binaryType='arraybuffer';this.closed=new Promise(done=>this.ws.addEventListener('close',event=>{if(!this.closing)this.fail(Error('unexpected WS close'));done(event.code);}));
  this.ws.addEventListener('error',()=>this.fail(Error('actual WS error')));
  this.ws.addEventListener('message',event=>{try{assert(event.data instanceof ArrayBuffer);assert(++this.count<=2048);const m=SignalResponse.fromBinary(new Uint8Array(event.data)).message;let value=m.value;if(m.case==='refreshToken'){value=claims(m.value);assert.equal(value.sub,this.identity,'every refreshed subject');assert.equal(value.video.room,this.room,'every refreshed room');assert.deepEqual(value.video,this.video,'every refreshed full original video grant');assert(this.allowedAttributes.some(attrs=>{try{assert.deepEqual(value.attributes??{},attrs);return true;}catch{return false;}}),'every refreshed full attributes');}const record={type:m.case,value};this.events.push(record);if(m.case==='leave')throw Error('actual server Leave');for(const waiter of this.waiters)waiter();}catch(e){this.fail(e);}});
 }
 fail(e){this.error??=e;for(const waiter of this.waiters)waiter();}
 async wait(predicate,label){const read=()=>{if(this.error)throw this.error;return this.events.find(predicate);};let value=read();if(value)return value;let wake;try{return await bounded(new Promise((done,reject)=>{wake=()=>{try{const x=read();if(x)done(x);}catch(e){reject(e);}};this.waiters.add(wake);wake();}),label);}finally{this.waiters.delete(wake);}}
 send(json){assert.equal(this.ws.readyState,WebSocket.OPEN);this.ws.send(SignalRequest.fromJson(json).toBinary());}
 async liveness(){const n=String(Date.now());const start=this.events.length;this.send({pingReq:{timestamp:n}});await this.wait(e=>this.events.indexOf(e)>=start&&e.type==='pongResp'&&String(e.value.lastPingTimestamp)===n,'actual PingReq/PongResp response');}
 async join(){const join=await this.wait(e=>e.type==='join','actual Join');assert.equal(join.value.participant.identity,this.identity);assert(join.value.participant.sid);this.sid=join.value.participant.sid;await this.liveness();}
 async close(){this.closing=true;if(this.ws.readyState!==WebSocket.CLOSED)this.ws.close(1000,'fixture complete');return bounded(this.closed,'actual owned WS close');}
}
function refreshCount(peer){return peer.events.filter(e=>e.type==='refreshToken').length;}
async function ownerParticipant(api,room,identity,predicate){const deadline=Date.now()+TIMEOUT;for(;;){try{const value=await api.getParticipant(room,identity);if(predicate(value)){assert(Date.now()<=deadline,'actual owner view deadline');return value;}}catch(e){if(e.code!=='not_found')throw e;}assert(Date.now()<deadline,'actual owner view did not converge');await new Promise(done=>setTimeout(done,10));}}
async function run(side,url,pid,binary,config){const api=new RoomServiceClient(url.replace(/^ws/,'http'),key,secret,{requestTimeout:10,failover:false});const row={side,process:await processProof(pid,binary,config),steps:[],cleanup_errors:[]};report.campaigns.push(row);let owned=false;const peers=[];const room='attr-control-'+randomUUID();try{
 const made=await api.createRoom({name:room});owned=true;assert.equal(made.name,room);const roomSid=made.sid;
 for(const allowed of [true,false]){
  const identity='attr-'+(allowed?'allowed-':'denied-')+randomUUID();const mint=new AccessToken(key,secret,{identity,ttl:'2m'});mint.addGrant({roomJoin:true,room,canPublish:false,canSubscribe:false,canPublishData:false,canUpdateOwnMetadata:allowed});const peer=new Peer(url,await mint.toJwt(),identity,room);peers.push(peer);await peer.join();
  let current=await ownerParticipant(api,room,identity,p=>p.sid===peer.sid);assert.equal(current.sid,peer.sid);assert([0,1].includes(current.state),'existing pre-RTC participant must be JOINING/JOINED');assert.equal(current.permission.canUpdateMetadata,allowed);
  const seedStart=peer.events.length;peer.allowedAttributes.push({k:'v'});current=await api.updateParticipant(room,identity,{attributes:{k:'v'}});assert.deepEqual(current.attributes,{k:'v'});await peer.wait(e=>peer.events.indexOf(e)>=seedStart&&e.type==='refreshToken'&&e.value.attributes?.k==='v','seed actual signed claims');peer.allowedAttributes=[{k:'v'}];await peer.liveness();
  for(const [kind,attrs]of [['same-value',{k:'v'}],['delete-absent',{absent:''}]]){
   // Official admin HTTP update returns the actual new version, then a fresh HTTP read agrees.
   const before=await api.getParticipant(room,identity),start=peer.events.length,count=refreshCount(peer);const updated=await api.updateParticipant(room,identity,{attributes:attrs});assert.equal(updated.sid,before.sid);assert.equal(updated.version,(before.version+1)>>>0);assert.deepEqual(updated.attributes,before.attributes);await peer.wait(e=>peer.events.indexOf(e)>=start&&e.type==='refreshToken','admin actual refresh');await peer.liveness();assert.equal(refreshCount(peer)-count,1);const reread=await ownerParticipant(api,room,identity,p=>p.version===updated.version);assert.equal(reread.version,updated.version);assert.deepEqual(reread.attributes,updated.attributes);row.steps.push({entry:'admin',allowed,kind,version_delta:updated.version-before.version,claims:refreshCount(peer)-count,attrs_unchanged:true,sid_same:true});
  }
  let requestId=100;
  for(const [kind,attrs]of [['same-value',{k:'v'}],['delete-absent',{absent:''}],['empty',{}]]){
   const before=await api.getParticipant(room,identity),start=peer.events.length,count=refreshCount(peer),id=++requestId;peer.send({updateMetadata:{attributes:attrs,requestId:id}});const response=await peer.wait(e=>peer.events.indexOf(e)>=start&&e.type==='requestResponse'&&e.value.requestId===id,'actual self RequestResponse');assert.equal(response.value.reason,allowed?0:2);if(!allowed)assert.equal(response.value.message,'does not have permission to update own metadata');await peer.liveness();const changed=allowed&&kind!=='empty',after=await ownerParticipant(api,room,identity,p=>p.version===((before.version+(changed?1:0))>>>0));assert.equal(after.version,(before.version+(changed?1:0))>>>0);assert.equal(after.sid,before.sid);assert.deepEqual(after.attributes,before.attributes);assert.equal(refreshCount(peer)-count,changed?1:0);row.steps.push({entry:'self',allowed,kind,reason:response.value.reason,version_delta:after.version-before.version,claims:refreshCount(peer)-count,attrs_unchanged:true,sid_same:true});
  }
  await peer.close();peers.pop();assert.equal((await api.listRooms([room]))[0].sid,roomSid,'same actual room generation');
 }
 assert.equal(row.steps.length,10);row.status='PASS';
 }finally{for(const peer of peers)try{await peer.close();}catch{row.cleanup_errors.push('owned WS close failed');}if(owned)try{await api.deleteRoom(room);}catch{row.cleanup_errors.push('owned room delete failed');}report.cleanup_errors.push(...row.cleanup_errors);await save();}}
try{await writeFile(env.RESULT,'',{flag:'wx',mode:0o600});await save();stage='official';await run('reference',env.REFERENCE_URL,env.REFERENCE_PID,env.REFERENCE_BIN,env.REFERENCE_CONFIG);stage='rust';await run('rust',env.LIVEKIT_URL,env.RUST_PID,env.RUST_BIN,env.RUST_CONFIG);assert.deepEqual(report.campaigns[1].steps,report.campaigns[0].steps,'source/candidate business observations exact');assert.equal(report.cleanup_errors.length,0);report.status='PASS_SCOPED';}catch(e){report.status='FAIL';report.failure={stage,type:e?.constructor?.name??'Error'};process.exitCode=1;}finally{await save();}
