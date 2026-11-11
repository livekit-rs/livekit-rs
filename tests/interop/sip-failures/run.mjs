/** Independent optional profile; actual execution remains coordinator-owned. */
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {createServer,request as httpRequest} from 'node:http';
import {gunzipSync} from 'node:zlib';
import {readFile,mkdir,writeFile} from 'node:fs/promises';
import {dirname,resolve,join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {randomBytes} from 'node:crypto';
import {RejectingSipPeer} from './rejecting-peer.mjs';
const here=dirname(fileURLToPath(import.meta.url)),require=createRequire(resolve(process.env.INTEROP_PACKAGE||join(here,'../package.json')));
const {SipClient,RoomServiceClient}=require('livekit-server-sdk'),{SIPTransport}=require('@livekit/protocol');
const urls=[process.env.REFERENCE_URL,process.env.LIVEKIT_URL];assert.ok(urls.every(Boolean),'two real independently configured HTTP+SIP worker graphs required');assert.equal(process.env.SIP_FAILURES_WORKERS_READY,'1','coordinator must confirm real workers subscribed to dedicated brokers');
const keys=[process.env.REFERENCE_API_KEY||process.env.LIVEKIT_API_KEY||'devkey',process.env.LIVEKIT_API_KEY||'devkey'],secrets=[process.env.REFERENCE_API_SECRET||process.env.LIVEKIT_API_SECRET||'secret',process.env.LIVEKIT_API_SECRET||'secret'];
const evidence={profile:'f07-real-sip-rejections',status:'RUNNING',startedAt:new Date().toISOString(),scope:'Genuine authenticated SIP INVITE486/503 via unchanged SDK→real public HTTP→real PSRPC→unmodified SIP worker; exact full HTTP and SDK error metadata. No media/call-success claimed.',cases:[],differences:[]};
const output=resolve(process.env.SIP_FAILURES_OUTPUT||join(here,'output','sip-failures.json'));const sleep=ms=>new Promise(r=>setTimeout(r,ms));
const redact=value=>{let text=String(value).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<jwt>');for(const secret of secrets)if(secret)text=text.replaceAll(secret,'<api-secret>');return text;};
async function until(check,label){const end=Date.now()+10000;while(Date.now()<end){if(await check())return;await sleep(25);}throw Error(label);}
/** Byte-transparent HTTP response observer; no SDK modifications or generated errors. */
async function observeHttp(upstream){
 const captures=[],errors=[];let overflow=false;
 const server=createServer(async(req,res)=>{try{
  if(req.method!=='POST'||!req.url.startsWith('/twirp/livekit.SIP/'))throw Error('observer only permits genuine public SIP requests');
  const chunks=[];let size=0;for await(const chunk of req){size+=chunk.length;if(size>262144)throw Error('bounded HTTP request overflow');chunks.push(chunk);}
  const headers={};for(const [key,value]of Object.entries(req.headers))if(!['host','connection','content-length','transfer-encoding'].includes(key))headers[key]=value;
  const upstreamUrl=new URL(req.url,upstream);if(upstreamUrl.protocol!=='http:')throw Error('this local observer requires plain HTTP upstream');
  const upstreamResponse=await new Promise((resolve,reject)=>{
   const outgoing=httpRequest(upstreamUrl,{method:'POST',headers},incoming=>{const parts=[];let length=0;incoming.on('data',chunk=>{length+=chunk.length;if(length>262144){incoming.destroy(Error('bounded HTTP response overflow'));return;}parts.push(chunk);});incoming.on('error',reject);incoming.on('end',()=>resolve({status:incoming.statusCode,headers:incoming.headers,bytes:Buffer.concat(parts)}));});outgoing.on('error',reject);outgoing.setTimeout(25000,()=>outgoing.destroy(Error('actual upstream HTTP safety timeout')));outgoing.end(Buffer.concat(chunks));
  });
  const bytes=upstreamResponse.bytes;
  if(req.url.endsWith('/CreateSIPParticipant')){if(captures.length>=8){overflow=true;throw Error('bounded HTTP capture overflow');}const encoding=upstreamResponse.headers['content-encoding'];if(encoding&&encoding!=='gzip')throw Error('unhandled HTTP content encoding in exact observer');const text=(encoding==='gzip'?gunzipSync(bytes,{maxOutputLength:262144}):bytes).toString('utf8');let body=null;try{body=JSON.parse(text);}catch{}captures.push({status:upstreamResponse.status,contentType:upstreamResponse.headers['content-type'],rawBase64:bytes.toString('base64'),rawText:text,body});}
  const responseHeaders={};for(const [key,value]of Object.entries(upstreamResponse.headers))if(!['connection','transfer-encoding','content-length'].includes(key))responseHeaders[key]=value;
  res.writeHead(upstreamResponse.status,responseHeaders);res.end(bytes);
 }catch(error){errors.push(redact(error.message));res.destroy(error);}});
 await new Promise(r=>server.listen(0,'127.0.0.1',r));return {url:`http://127.0.0.1:${server.address().port}`,captures,errors,get overflow(){return overflow;},close:()=>new Promise(r=>server.close(r))};
}
function sdkError(error){return {name:error.name,message:error.message,status:error.status,code:error.code,metadata:error.metadata??{},sipStatusCode:error.sipStatusCode,sipStatus:error.sipStatus};}
/** Only explicitly structured generated IDs can be normalized; preserve raw. */
function comparable(observation){
 const ids=new Map();for(const values of [observation.http.body?.meta,observation.sdk.metadata])for(const [key,value]of Object.entries(values||{}))if(['sip_call_id','call_id','transfer_id','sipCallId','transferId'].includes(key)&&typeof value==='string'&&/^(SCL_|STR_)[A-Za-z0-9]+$/.test(value))ids.set(value,`<generated:${key}>`);
 const replace=value=>{if(typeof value==='string'){for(const [id,label]of ids)value=value.replaceAll(id,label);return value;}if(Array.isArray(value))return value.map(replace);if(value&&typeof value==='object')return Object.fromEntries(Object.entries(value).map(([k,v])=>[k,replace(v)]));return value;};
 return replace({http:{status:observation.http.status,contentType:observation.http.contentType,body:observation.http.body},sdk:observation.sdk});
}
try{
 const run=randomBytes(4).toString('hex');
 for(const status of [486,503]){
  const row={sipStatus:status,observations:[]};evidence.cases.push(row);
  for(let index=0;index<2;index++){
   const room=`f07-reject-${run}-${status}`,identity=`f07-rejected-${status}`,api=new RoomServiceClient(urls[index],keys[index],secrets[index]);let peer,observer,trunk,roomOwned=false;
   const observation={server:index===0?'reference':'rust',status:'RUNNING',cleanup:[]};row.observations.push(observation);
   try{
    peer=await new RejectingSipPeer(status).start();observer=await observeHttp(urls[index]);
    const sip=new SipClient(observer.url,keys[index],secrets[index],{requestTimeout:25,failover:false});
    await api.createRoom({name:room,emptyTimeout:30});roomOwned=true;
    trunk=await sip.createSipOutboundTrunk('f07-authenticated-rejection',`127.0.0.1:${peer.port}`,['+12025550123'],{transport:SIPTransport.SIP_TRANSPORT_UDP,authUsername:peer.username,authPassword:peer.password});
    let caught;try{await sip.createSipParticipant(trunk.sipTrunkId,'15550001',room,{participantIdentity:identity,waitUntilAnswered:true,ringingTimeout:10,maxCallDuration:30,timeout:25});}catch(error){caught=error;}
    assert.ok(caught,'actual rejected SIP call must not return a successful participant');
    observation.sdk=sdkError(caught);assert.equal(observer.captures.length,1,'one actual SDK call; observer cannot synthesize or retry a failure');observation.http=observer.captures[0];
    await until(()=>peer.snapshot().rejection.finalAcks>=1,'actual final SIP rejection ACK missing');
    observation.peer=peer.snapshot();assert.equal(observation.peer.challenges,1);assert.equal(observation.peer.authenticatedInvites,1);assert.equal(observation.peer.rejection.finalResponses,1);assert.equal(observation.peer.acceptedInvites,0);assert.equal(observation.peer.rtpSent,0);assert.deepEqual(observation.peer.errors,[]);assert.equal(observation.peer.overflow,false);assert.deepEqual(observer.errors,[]);assert.equal(observer.overflow,false);
    assert.ok(observation.http.status>=400&&observation.http.status<600,'must be actual public failure');
    assert.equal(observation.http.body?.meta?.sip_status_code,String(status),'genuine SIPStatus detail must reach actual public HTTP');assert.equal(observation.sdk.metadata.sip_status_code,String(status),'unchanged SDK must retain genuine SIPStatus metadata');
    await until(async()=>!(await api.listParticipants(room)).some(p=>p.identity===identity),'failed call left an actual RTC participant');observation.participantAbsentAfterFailure=true;observation.status='PASS';
   }catch(error){observation.status='FAIL';observation.failure={name:error.name,message:redact(error.message)};observation.peer=peer?.snapshot();if(observer)observation.observedHttp=observer.captures;process.exitCode=1;}
   finally{
    if(trunk)try{await new SipClient(urls[index],keys[index],secrets[index]).deleteSipTrunk(trunk.sipTrunkId);}catch{observation.cleanup.push('owned-trunk-delete-failed');}
    if(roomOwned)try{await api.deleteRoom(room);}catch{observation.cleanup.push('owned-room-delete-failed');}
    try{await peer?.close();}catch{observation.cleanup.push('peer-close-failed');}try{await observer?.close();}catch{observation.cleanup.push('http-observer-close-failed');}
    if(observation.cleanup.length){observation.status='FAIL';process.exitCode=1;}
   }
  }
  if(row.observations.every(o=>o.status==='PASS')){try{assert.deepEqual(comparable(row.observations[1]),comparable(row.observations[0]));row.exactErrorMatch=true;}catch{row.exactErrorMatch=false;evidence.differences.push(`SIP${status}: full HTTP/SDK code/message/metadata mismatch`);process.exitCode=1;}}
 }
 evidence.status=evidence.differences.length||evidence.cases.some(r=>r.observations.some(o=>o.status!=='PASS'))?'FAIL':'PASS';
}catch(error){evidence.status='FAIL';evidence.failure={name:error.name,message:redact(error.message)};process.exitCode=1;}
finally{evidence.finishedAt=new Date().toISOString();await mkdir(dirname(output),{recursive:true});await writeFile(output,JSON.stringify(evidence,null,2)+'\n');console.log(JSON.stringify({status:evidence.status,differences:evidence.differences}));}
