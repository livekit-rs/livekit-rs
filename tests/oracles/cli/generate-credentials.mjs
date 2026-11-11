// Actual CLI output + unchanged official ServerSDK authenticated RoomService.
// Only safe counts/booleans/hashes leave this process; credentials stay in memory
// and a private temporary configuration removed after owned server shutdown.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { createServer } from 'node:net';
import { createSocket } from 'node:dgram';
import { mkdtemp, writeFile, readFile, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createRequire } from 'node:module';
const require=createRequire(new URL('../../interop/package.json',import.meta.url));
const {AccessToken,RoomServiceClient}=require('livekit-server-sdk');
const binaries=[{kind:'official',path:process.env.OFFICIAL_BIN},{kind:'rust',path:process.env.RUST_BIN}];
assert(binaries.every(b=>b.path),'OFFICIAL_BIN and RUST_BIN required');
assert(process.env.CREDENTIAL_EVIDENCE,'CREDENTIAL_EVIDENCE safe report path required');
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
const report={status:'RUNNING',scope:'actual generate-keys format/randomness and authenticated RoomService CRUD on isolated official and Rust binaries; no media claim',generators:[],servers:[],cleanup_errors:[]};
const owned=[];const rooms=[];let temp;let stage='setup';
const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function deadline(promise,label,ms){let timer;try{return await Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error(label)),ms);})]);}finally{clearTimeout(timer);}}
function child(binary,args,label){
 const env=Object.fromEntries(Object.entries(process.env).filter(([name])=>!name.startsWith('LIVEKIT_')));
 const processChild=spawn(binary,args,{stdio:['ignore','pipe','pipe'],env});
 const state={label,process:processChild,exit:undefined,output:'',bytes:0,overflow:false,stderr_bytes:0};
 state.done=new Promise(resolve=>{processChild.once('error',()=>{state.exit={spawn_error:true};resolve(state.exit);});processChild.once('close',(code,signal)=>{state.exit={code,signal};resolve(state.exit);});});
 processChild.stdout.on('data',data=>{state.bytes+=data.length;if(state.bytes<=65536)state.output+=data.toString('utf8');else state.overflow=true;});
 // Server logs may contain credential material. Drain without storing/printing.
 processChild.stderr.on('data',data=>{state.stderr_bytes+=data.length;});
 owned.push(state);return state;
}
async function stop(state){
 if(state.exit)return;
 state.process.kill('SIGINT');
 try{await deadline(state.done,'owned process shutdown deadline',20000);}catch{
  state.process.kill('SIGKILL');await deadline(state.done,'owned process forced shutdown deadline',5000);
  report.cleanup_errors.push('owned process required forced termination');
 }
}
function credentials(stdout){
 const match=/^API Key:  (API[23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz]{12})\nAPI Secret:  ([A-Za-z0-9]{43,52})\n$/.exec(stdout);
 assert(Boolean(match),'generate-keys exact two-line/base57-key/base62-256bit-secret contract');
 return {key:match[1],secret:match[2]};
}
async function freePort(protocol){
 if(protocol==='udp')return new Promise((resolve,reject)=>{const socket=createSocket('udp4');socket.once('error',reject);socket.bind(0,'127.0.0.1',()=>{const port=socket.address().port;socket.close(()=>resolve(port));});});
 return new Promise((resolve,reject)=>{const server=createServer();server.once('error',reject);server.listen(0,'127.0.0.1',()=>{const port=server.address().port;server.close(error=>error?reject(error):resolve(port));});});
}
async function client(url,pair,room,wrong=false){
 const secret=wrong?pair.secret.slice(0,-1)+(pair.secret.endsWith('A')?'B':'A'):pair.secret;
 const token=new AccessToken(pair.key,secret,{ttl:'2m'});
 token.addGrant({roomCreate:true,roomList:true,roomAdmin:true,room});
 return new RoomServiceClient(url,undefined,undefined,{token:await token.toJwt(),requestTimeout:5,failover:false});
}
async function ready(state,url,pair){
 const until=Date.now()+20000;
 while(Date.now()<until){
  assert(!state.exit,'isolated server exited before authenticated readiness');
  try{await (await client(url,pair,'')).listRooms();return;}catch{await delay(100);}
 }
 throw new Error('authenticated server readiness deadline');
}
try{
 temp=await mkdtemp(join(tmpdir(),'livekit-rs-generated-credentials-'));
 const pairs=[];
 for(const binary of binaries){
  stage='generate-'+binary.kind;
  const generated=[];
  for(let sample=0;sample<3;sample++){
   const state=child(binary.path,['generate-keys'],binary.kind+'-generator-'+sample);
   const exit=await deadline(state.done,'generate-keys deadline',10000);
   assert(exit.code===0&&!exit.signal&&!state.overflow,'generate-keys actual clean exit and bounded output');
   const pair=credentials(state.output);state.output='';generated.push(pair);pairs.push(pair);
  }
  assert(new Set(generated.map(v=>v.key)).size===3,'API keys must vary across actual executions');
  assert(new Set(generated.map(v=>v.secret)).size===3,'API secrets must vary across actual executions');
  report.generators.push({kind:binary.kind,binary_sha256:sha(await readFile(binary.path)),executions:3,exit_zero:true,exact_format:true,key_lengths:generated.map(v=>v.key.length),secret_lengths:generated.map(v=>v.secret.length),base57_keys:true,base62_secrets:true,unique_keys:true,unique_secrets:true});
 }
 assert(new Set(pairs.map(p=>p.key)).size===6&&new Set(pairs.map(p=>p.secret)).size===6,'all six actual generated pairs must be distinct');
 for(const binary of binaries){
  stage='serve-'+binary.kind;
  const http=Number(process.env[binary.kind==='official'?'OFFICIAL_CREDENTIAL_PORT':'RUST_CREDENTIAL_PORT'])||await freePort('tcp');
  const udp=Number(process.env[binary.kind==='official'?'OFFICIAL_CREDENTIAL_UDP_PORT':'RUST_CREDENTIAL_UDP_PORT'])||await freePort('udp');
  const config=join(temp,binary.kind+'.yaml');
  const text=`port: ${http}\nbind_addresses: [127.0.0.1]\nrtc:\n  tcp_port: 0\n  udp_port: ${udp}\n  node_ip: 127.0.0.1\n  use_external_ip: false\n  enable_loopback_candidate: true\nkeys:\n${pairs.map(p=>'  '+p.key+': '+p.secret).join('\n')}\nlogging:\n  level: error\n`;
  await writeFile(config,text,{mode:0o600});
  assert(((await stat(config)).mode&0o777)===0o600,'credential configuration must be mode0600');
  const state=child(binary.path,['--config',config],binary.kind+'-isolated-server');
  const url='http://127.0.0.1:'+http;
  await ready(state,url,pairs[0]);
  const summary={kind:binary.kind,authenticated_readiness:true,config_mode_0600:true,valid_generated_pairs:0,wrong_secret_rejections:0,created_listed_deleted:0};
  report.servers.push(summary);
  for(let index=0;index<pairs.length;index++){
   stage='credential-crud-'+binary.kind+'-'+index;
   const room='credential-fixture-'+randomUUID();
   const good=await client(url,pairs[index],room);const bad=await client(url,pairs[index],room,true);
   let rejected=false;try{await bad.listRooms();}catch(error){rejected=error.status===401||error.code==='unauthenticated';}
   assert(rejected,'incorrect generated-secret signature must be rejected with authentication failure');summary.wrong_secret_rejections++;
   const created=await good.createRoom({name:room});rooms.push({client:good,name:room});assert(created.name===room,'actual createRoom name');
   assert((await good.listRooms([room])).some(r=>r.name===room),'actual listRooms contains created room');
   await good.deleteRoom(room);rooms.pop();
   assert(!(await good.listRooms([room])).some(r=>r.name===room),'actual deleteRoom removes created room');
   summary.valid_generated_pairs++;summary.created_listed_deleted++;
  }
  await stop(state);assert(state.exit?.code===0&&!state.exit?.signal,'isolated server clean graceful exit');summary.clean_exit_zero=true;
 }
 report.status='PASS';
}catch{report.status='FAIL';report.failure_stage=stage;}
finally{
 for(const room of rooms)try{await room.client.deleteRoom(room.name);}catch{report.cleanup_errors.push('owned room cleanup failed');}
 for(const state of owned)try{await stop(state);}catch{report.cleanup_errors.push('owned process cleanup failed');}
 if(temp)try{await rm(temp,{recursive:true,force:true});}catch{report.cleanup_errors.push('private credential configuration cleanup failed');}
 report.processes=owned.map(s=>({label:s.label,exit:s.exit??{unobserved:true},stdout_bytes:s.bytes,stderr_bytes:s.stderr_bytes,output_overflow:s.overflow}));
 report.temporary_credentials_removed=Boolean(temp)&&report.cleanup_errors.length===0;
 if(report.cleanup_errors.length)report.status='FAIL';
 report.script_sha256=sha(await readFile(new URL(import.meta.url)));
 await writeFile(process.env.CREDENTIAL_EVIDENCE,JSON.stringify(report,null,2)+'\n',{mode:0o600});
}
if(report.status!=='PASS')process.exitCode=1;
console.log(report.status+' actual generated-credential format/randomness and authenticated CRUD');
