// PREPARED ONLY: root executes against actual external worker and dedicated server.
import assert from 'node:assert/strict';
import { DecodedToneWindow } from '../ingress-tone-window.mjs';
import {createRequire} from 'node:module';
import {pathToFileURL} from 'node:url';
import {resolve,join} from 'node:path';
import {mkdtemp,readFile,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {createServer} from 'node:http';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {randomUUID,createHash} from 'node:crypto';
const env=process.env;for(const name of ['INTEROP_PROJECT','AGENT_SDK_ROOT','LIVEKIT_URL','INGRESS_WORKER_PID','INGRESS_URL_OUTPUT'])assert(env[name],`${name} required`);
const require=createRequire(resolve(env.INTEROP_PROJECT,'package.json'));
const {IngressClient,RoomServiceClient,AccessToken}=require('livekit-server-sdk');assert.equal(JSON.parse(await readFile(resolve(env.INTEROP_PROJECT,'node_modules/livekit-server-sdk/package.json'),'utf8')).version,'2.19.2');
const rtc=await import(pathToFileURL(resolve(env.AGENT_SDK_ROOT,'node_modules/@livekit/rtc-node/dist/index.js')));
const exec=promisify(execFile),hash=x=>createHash('sha256').update(x).digest('hex');
const report={status:'RUNNING',scope:'actual HTTP OGG/Opus URL pull by unchanged external Ingress v1.5.0; decoded RTC sine, normal EOS and independent active Delete',phases:[],state_observations:[],cleanup_errors:[]};
const temp=await mkdtemp(join(tmpdir(),'livekit-rs-f10-url-'));const key=env.LIVEKIT_API_KEY||'devkey',secret=env.LIVEKIT_API_SECRET||'secret';
const client=new IngressClient(env.LIVEKIT_URL,key,secret),rooms=new RoomServiceClient(env.LIVEKIT_URL,key,secret),room=new rtc.Room();
const roomName=`f10-url-${randomUUID()}`,owned=new Set(),files=new Map(),sockets=new Set();let http,subscriberConnected=false,current,previousResource;let requestCount=0,responseBytes=0,phaseObservation;
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function wait(check,label,ms=45000){const until=Date.now()+ms;while(Date.now()<until){const result=await check();if(result)return result;await sleep(100);}throw Error(`${label} deadline`);}
async function children(){const {stdout}=await exec('/bin/ps',['-axo','pid=,ppid=,comm='],{maxBuffer:1024*1024});return stdout.split('\n').map(s=>s.trim().match(/^(\d+)\s+(\d+)\s+(.+)$/)).filter(Boolean).filter(m=>+m[2]===+env.INGRESS_WORKER_PID&&m[3].split('/').at(-1)==='ingress').map(m=>+m[1]);}
room.on(rtc.RoomEvent.TrackSubscribed,(track,pub,participant)=>{const state=current;if(!state||participant.identity!==state.identity||track.kind!==rtc.TrackKind.KIND_AUDIO||state.reader)return;state.stream=new rtc.AudioStream(track,{sampleRate:48000,numChannels:1});state.reader=state.stream.getReader();state.task=(async()=>{while(!state.stop){const {value:frame,done}=await state.reader.read();if(done)break;state.tone.push(frame.data);}})();state.task.catch(e=>{if(!state.stop)state.error=e;});});
async function closeReader(){if(!current)return;current.stop=true;try{if(current.reader)await current.reader.cancel();if(current.task)await current.task;}finally{current=null;}}
function stateProjection(value){const state=value?.state;return {present:!!value,state_present:!!state,status:state?.status??null,resource_sha256:state?.resourceId?hash(state.resourceId):null,started_at:state?.startedAt?.toString()??null,updated_at:state?.updatedAt?.toString()??null,ended_at:state?.endedAt?.toString()??null,error_sha256:state?.error?hash(state.error):null};}
function observeState(value,source){if(!phaseObservation)return;const state=stateProjection(value),serialized=JSON.stringify(state);phaseObservation.lookups+=(source==='ListIngress'?1:0);if(phaseObservation.last_projection!==serialized){phaseObservation.transitions.push({elapsed_ms:Date.now()-phaseObservation.started_ms,source,...state});phaseObservation.last_projection=serialized;}phaseObservation.last_elapsed_ms=Date.now()-phaseObservation.started_ms;}
async function lookup(id){try{const list=await client.listIngress({ingressId:id});assert(list.length<=1);observeState(list[0],'ListIngress');return list[0];}catch(error){if(phaseObservation)phaseObservation.lookup_errors.push({elapsed_ms:Date.now()-phaseObservation.started_ms,code:error?.code??null,message_sha256:hash(String(error?.message))});throw error;}}
try{
 assert.equal((await children()).length,0,'dedicated official worker must have no pre-existing handlers');
 for(const [name,duration]of [['eos',15],['delete',60]]){const file=join(temp,`${name}.ogg`);await exec(env.FFMPEG_BIN||'ffmpeg',['-nostdin','-hide_banner','-loglevel','error','-f','lavfi','-i',`sine=frequency=440:sample_rate=48000:duration=${duration}`,'-vn','-c:a','libopus','-b:a','64000','-ac','1','-f','ogg',file],{timeout:30000,maxBuffer:65536});const bytes=await readFile(file);assert(bytes.length>1000&&bytes.length<=8*1024*1024);files.set(`/${name}.ogg`,bytes);}
 http=createServer(async(req,res)=>{const bytes=files.get(req.url);if(!bytes){res.writeHead(404).end();return;}if(!['GET','HEAD'].includes(req.method)){res.writeHead(405).end();return;}requestCount++;let start=0,end=bytes.length-1,status=200;const range=req.headers.range;if(range){const match=/^bytes=(\d+)-(\d*)$/.exec(range);if(!match){res.writeHead(416,{'Content-Range':`bytes */${bytes.length}`}).end();return;}start=+match[1];end=match[2]?Math.min(+match[2],end):end;if(start>end||start>=bytes.length){res.writeHead(416,{'Content-Range':`bytes */${bytes.length}`}).end();return;}status=206;}const headers={'Content-Type':'audio/ogg','Content-Length':String(end-start+1),'Accept-Ranges':'bytes'};if(status===206)headers['Content-Range']=`bytes ${start}-${end}/${bytes.length}`;res.writeHead(status,headers);if(req.method==='HEAD')res.end();else{for(let offset=start;offset<=end&&!res.destroyed;offset+=1024){const chunk=bytes.subarray(offset,Math.min(offset+1024,end+1));res.write(chunk);responseBytes+=chunk.length;await sleep(125);}res.end();}});
 http.on('connection',socket=>{sockets.add(socket);socket.on('close',()=>sockets.delete(socket));});await new Promise((resolve,reject)=>{http.once('error',reject);http.listen(0,'127.0.0.1',resolve);});
 await rooms.createRoom({name:roomName});const token=new AccessToken(key,secret,{identity:`f10-reader-${randomUUID()}`});token.addGrant({roomJoin:true,room:roomName,canSubscribe:true,canPublish:false});await room.connect(env.LIVEKIT_URL.replace(/^http/,'ws'),await token.toJwt(),{autoSubscribe:true});subscriberConnected=true;
 for(const phase of ['eos','delete']){
  current={identity:`f10-source-${randomUUID()}`,tone:new DecodedToneWindow(),stop:false};const phaseRequests=requestCount;phaseObservation={phase,started_ms:Date.now(),lookups:0,transitions:[],lookup_errors:[]};report.state_observations.push(phaseObservation);
  const info=await client.createIngress(2,{name:`actual URL ${phase}`,url:`http://127.0.0.1:${http.address().port}/${phase}.ogg`,roomName,participantIdentity:current.identity,enableTranscoding:true});owned.add(info.ingressId);phaseObservation.ingress_sha256=hash(info.ingressId);observeState(info,'CreateIngress');assert.equal(info.reusable,false);assert.equal(info.inputType,2);
  const handler=await wait(async()=>{const ids=await children();assert(ids.length<=1);return ids[0];},'actual URL worker handler');
  const publishing=await wait(async()=>{const value=await lookup(info.ingressId);if(value?.state?.status===3)throw Error('real URL worker ENDPOINT_ERROR');return value?.state?.status===2?value:null;},'actual URL publishing state');
  const generation=publishing.state.resourceId;assert(generation&&generation!==previousResource);previousResource=generation;assert(BigInt(publishing.state.startedAt)>0n&&BigInt(publishing.state.updatedAt)>0n);
  const contentWindow=await wait(()=>{if(current.error)throw current.error;return current.tone.readyWindow();},'two consecutive seconds decoded URL sine');assert(requestCount>phaseRequests,'actual worker must fetch local HTTP object');
  const rms=contentWindow.rms,ratio=contentWindow.tone_power_ratio;assert(rms>100);assert(ratio>0.5);
  const summary={phase,actual_handler:true,actual_http_fetch:true,unique_resource_generation:true,decoded_frames:current.tone.frames,decoded_samples:current.tone.samples,rms,tone_power_ratio:ratio,content_window:contentWindow,decoded_content:current.tone.snapshot()};
  if(phase==='eos'){const ended=await wait(async()=>{const value=await lookup(info.ingressId);if(value?.state?.status===3)throw Error('URL ended in actual ERROR');return value?.state?.status===4?value:null;},'non-reusable URL normal ENDPOINT_COMPLETE');assert.equal(ended.state.resourceId,generation);assert(BigInt(ended.state.endedAt)>0n);summary.real_end_callback_complete=true;}
  else{assert.equal((await lookup(info.ingressId)).state.status,2,'must Delete an actually active stream, not post-EOS');const removed=await client.deleteIngress(info.ingressId);assert.equal(removed.ingressId,info.ingressId);owned.delete(info.ingressId);summary.deleted_while_publishing=true;}
  await wait(async()=>!(await children()).includes(handler),'actual URL handler exits',15000);await wait(()=>!Array.from(room.remoteParticipants.values()).some(p=>p.identity===current.identity),'actual URL participant leaves',15000);
  if(phase==='eos'){await client.deleteIngress(info.ingressId);owned.delete(info.ingressId);}await assert.rejects(()=>client.listIngress({ingressId:info.ingressId}),error=>error?.code==='not_found','deleted List-by-ID must return source not_found');summary.handler_exited=true;summary.participant_left=true;report.phases.push(summary);await closeReader();
 }
 report.http={source_bytes_per_second:8192,source_chunk_bytes:1024,source_chunk_interval_ms:125,requests:requestCount,response_bytes:responseBytes,fixture_sha256:Object.fromEntries([...files].map(([path,data])=>[path,hash(data)]))};assert.equal(report.phases.length,2);report.status='PASS';
}catch(e){report.status='FAIL';report.failure={name:e.name,message_sha256:hash(String(e.message))};}
finally{
 if(current)report.last_phase_decoded_content=current.tone.snapshot();
 for(const observation of report.state_observations){delete observation.last_projection;delete observation.started_ms;}
 try{await closeReader();}catch{report.cleanup_errors.push('audio reader close failed');}
 try{if(subscriberConnected)await room.disconnect();}catch{report.cleanup_errors.push('native room disconnect failed');}
 for(const id of owned){try{await client.deleteIngress(id);}catch{report.cleanup_errors.push('owned ingress delete failed');}}
 try{await rooms.deleteRoom(roomName);}catch{report.cleanup_errors.push('owned room delete failed');}
 try{await rtc.dispose();}catch{report.cleanup_errors.push('native RTC dispose failed');}
 if(http){for(const socket of sockets)socket.destroy();await new Promise(resolve=>http.close(resolve));}await rm(temp,{recursive:true,force:true});
 if(report.cleanup_errors.length)report.status='FAIL';await writeFile(env.INGRESS_URL_OUTPUT,JSON.stringify(report,null,2)+'\n',{mode:0o600});if(report.status!=='PASS')process.exitCode=1;
}
