// Official generated worker messages over actual public WebSockets. This probe
// complements, and cannot replace, the unchanged AgentServer/native RTC fixture.
import { createRequire } from 'node:module';
import { resolve } from 'node:path';
import { writeFile, readFile } from 'node:fs/promises';
import assert from 'node:assert/strict';
const sdkRequire=createRequire(resolve(process.env.AGENT_SDK_ROOT,'package.json'));
const WebSocket=sdkRequire('ws');
const interopRequire=createRequire(new URL('../interop/package.json',import.meta.url));
const {AccessToken}=interopRequire('livekit-server-sdk');
const {WorkerMessage,ServerMessage}=sdkRequire('@livekit/protocol');
const referenceOnly=process.argv.includes('--reference-only');
const urls=[process.env.REFERENCE_URL,process.env.LIVEKIT_URL];
assert(urls[0]&&(referenceOnly||urls[1]),'actual reference URL and (unless --reference-only) Rust URL required');
const key=process.env.LIVEKIT_API_KEY||'devkey',secret=process.env.LIVEKIT_API_SECRET||'secret';
// Approved product identity exception, source: signal/ice.rs::server_info.
// Read the frozen workspace version instead of accepting arbitrary debug text.
const manifest=await readFile(new URL('../../Cargo.toml',import.meta.url),'utf8');
const packageBlock=manifest.match(/\[workspace\.package\]([^[]*)/s)?.[1];
const productVersion=packageBlock?.match(/^version\s*=\s*"([^"]+)"/m)?.[1];
assert(productVersion,'frozen workspace product version required');
const expectedDebugInfo='livecloud '+productVersion;
const productIdentityException={field:'register.serverInfo.debugInfo',reference:'absent or empty',rust:expectedDebugInfo,scope:'validated product build identity only; not byte-identical ServerInfo'};
const rows=[];const sockets=new Set();
const timeout=(promise,label,ms=12000)=>{let timer;return Promise.race([promise,new Promise((_,reject)=>timer=setTimeout(()=>reject(new Error(label+' timeout')),ms))]).finally(()=>clearTimeout(timer));};
async function jwt(agent){const t=new AccessToken(key,secret,{identity:'worker-wire-fixture'});t.addGrant(agent?{agent:true}:{roomJoin:true,room:'not-agent'});return t.toJwt();}
async function connect(url,token,query=''){
 return timeout(new Promise((resolve,reject)=>{
  const u=new URL('/agent',url);u.protocol=u.protocol.replace('http','ws');u.search=query;
  const ws=new WebSocket(u,{headers:token?{Authorization:'Bearer '+token}:{}});sockets.add(ws);
  const messages=[];const waiters=[];let closed;let failure;
  const fail=(error)=>{failure??=error;for(const waiter of waiters.splice(0))waiter.reject(error);};
  ws.on('message',(data,binary)=>{try{const value={codec:binary?'protobuf':'json',message:binary?ServerMessage.fromBinary(data).toJson():ServerMessage.fromJsonString(data.toString()).toJson()};const waiter=waiters.shift();if(waiter)waiter.resolve(value);else messages.push(value);}catch(error){fail(error);}});
  const ended=new Promise(resolveClose=>ws.once('close',(code)=>{closed=code;sockets.delete(ws);fail(new Error('worker closed before next message: '+code));resolveClose(code);}));
  ws.once('open',()=>resolve({
   send:(value,codec='protobuf')=>{const msg=WorkerMessage.fromJson(value);ws.send(codec==='json'?msg.toJsonString():msg.toBinary());},
   receive:()=>timeout(messages.length?Promise.resolve(messages.shift()):failure?Promise.reject(failure):new Promise((resolve,reject)=>waiters.push({resolve,reject})),'worker message'),
   closed:()=>timeout(ended,'worker close'),
   close:async()=>{if(closed===undefined)ws.close();await timeout(ended,'client close');},
  }));
  ws.once('unexpected-response',(_,response)=>{let body='';response.setEncoding('utf8');response.on('data',v=>body+=v);response.on('end',()=>{ws.terminate();resolve({rejected:{status:response.statusCode,body,contentType:response.headers['content-type']||''}});});});
  ws.on('error',error=>{fail(error);reject(error);});
 }), 'worker connection');
}
function normalized(value,side){
 const copy=structuredClone(value);const reg=copy.message.register;
 if(reg){assert.match(reg.workerId,/^AW_/);reg.workerId='<generated-worker-id>';if(reg.serverInfo){const info=reg.serverInfo;assert.match(info.nodeId,/^ND_/);info.nodeId='<generated-node-id>';if(side==='reference')assert(info.debugInfo===undefined||info.debugInfo==='','official reference debugInfo must be absent or empty');else assert.equal(info.debugInfo,expectedDebugInfo,'Rust must report its exact real product build version');info.debugInfo='<approved-product-identity>';}}
 const pong=copy.message.pong;if(pong){assert(BigInt(pong.timestamp)>0n);pong.timestamp='<server-now-ms>';}
 return copy;
}
async function run(url,out,side){
const token=await jwt(true);
 for(const method of ['GET','HEAD','POST']){const res=await fetch(new URL('/agent',url),{method});out.push({case:'non-upgrade-'+method,status:res.status,body:await res.text()});}
 for(const [name,t] of [['missing',undefined],['wrong-grant',await jwt(false)],['malformed','not-a-jwt']]){const conn=await connect(url,t);assert(conn.rejected,name+' unexpectedly upgraded');out.push({case:'auth-'+name,...conn.rejected});}
 for(const [name,registerCodec,query] of [['binary','protobuf',''],['json-switch','json','protocol=1'],['atoi-fallback','protobuf','protocol=invalid']]){
  const conn=await connect(url,token,query);assert(!conn.rejected);
  conn.send({ping:{timestamp:'123'}},registerCodec);out.push({case:name+'-pre-register-ping',...normalized(await conn.receive(),side)});
  conn.send({register:{type:'JT_ROOM',agentName:'wire-fixture',namespace:'fixture',version:'unchanged-generated-probe'}},registerCodec);out.push({case:name+'-register',...normalized(await conn.receive(),side)});
  // Unknown availability, unknown finished job and OSS migration are exercised
  // with a following real ping so a silently closed connection cannot pass.
  conn.send({availability:{jobId:'AJ_not_pending',available:true}},'protobuf');
  conn.send({updateJob:{jobId:'AJ_not_running',status:'JS_SUCCESS'}},'protobuf');
  conn.send({migrateJob:{jobIds:['AJ_not_running']}},'protobuf');
  conn.send({updateWorker:{status:'WS_AVAILABLE',load:0.2,seq:'5'}},'protobuf');
  conn.send({updateWorker:{status:'WS_FULL',load:1,seq:'4'}},'protobuf');
  conn.send({updateWorker:{status:'WS_AVAILABLE',load:0,seq:'0'}},'protobuf');
  const nextCodec=registerCodec==='json'?'protobuf':'json';
  conn.send({ping:{timestamp:'456'}},nextCodec);out.push({case:name+'-post-controls-ping',...normalized(await conn.receive(),side)});
  await conn.close();
 }
 for(const [name,message,afterRegister] of [['unknown-before',{},false],['invalid-deployment',{register:{deployment:'bad_deployment'}},false],['unknown-type',{register:{type:999}},false],['duplicate-register',{register:{}},true]]){
  const conn=await connect(url,token);assert(!conn.rejected);
  if(afterRegister){conn.send({register:{}});await conn.receive();}
  conn.send(message);out.push({case:name,closeCode:await conn.closed()});
 }
 return out;
}
let result;const reference=[];const rust=[];
const pairRows=()=>{rows.length=0;for(let i=0;i<Math.max(reference.length,rust.length);i++)rows.push({reference:reference[i]??null,rust:rust[i]??null});};
try{await run(urls[0],reference,'reference');if(referenceOnly){result={status:'REFERENCE_PASS',scope:'actual fixed Go reference only; Rust comparison NOT_RUN',reference_cases:reference.length,reference};}else{await run(urls[1],rust,'rust');pairRows();assert.deepEqual(rust,reference);result={status:'PASS',scope:'actual worker WS generated-message/control/auth profiles with validated product identity exception; not byte-identical; no SDK or WT media claim',paired_cases:rows.length,rows};}}
catch(error){pairRows();result={status:'FAIL',reference_only:referenceOnly,error:error.message,completed_reference_cases:reference.length,completed_rust_cases:rust.length,rows};process.exitCode=1;}
finally{result.product_identity_exception=productIdentityException;for(const socket of sockets)socket.terminate();if(process.env.AGENT_WIRE_EVIDENCE)await writeFile(process.env.AGENT_WIRE_EVIDENCE,JSON.stringify(result,null,2)+'\n');}
console.log(result.status+' worker wire '+rows.length+' paired observations');
