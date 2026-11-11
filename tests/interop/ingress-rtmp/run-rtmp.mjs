// Isolated executable acceptance draft; imports unchanged installed SDK packages.
import assert from 'node:assert/strict';
import { DecodedToneWindow } from '../ingress-tone-window.mjs';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';
import { spawn, execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { readFile, writeFile } from 'node:fs/promises';
import { createHash, randomUUID } from 'node:crypto';
const env=process.env;
for(const key of ['INTEROP_PROJECT','AGENT_SDK_ROOT','LIVEKIT_URL','INGRESS_WORKER_PID','INGRESS_MEDIA_OUTPUT']) assert(env[key],`${key} required`);
const require=createRequire(resolve(env.INTEROP_PROJECT,'package.json'));
const {IngressClient,RoomServiceClient,AccessToken}=require('livekit-server-sdk');
assert.equal(JSON.parse(await readFile(resolve(env.INTEROP_PROJECT,'node_modules/livekit-server-sdk/package.json'),'utf8')).version,'2.19.2');
const rtc=await import(pathToFileURL(resolve(env.AGENT_SDK_ROOT,'node_modules/@livekit/rtc-node/dist/index.js')));
const exec=promisify(execFile), sha=x=>createHash('sha256').update(x).digest('hex');
const report={status:'RUNNING',scope:'unchanged official external Ingress v1.5.0 real RTMP AAC sine -> SFU -> unchanged rtc-node decoded PCM; active public Delete',observations:{},cleanup_errors:[]};
const secret=env.LIVEKIT_API_SECRET||'secret',key=env.LIVEKIT_API_KEY||'devkey';
const client=new IngressClient(env.LIVEKIT_URL,key,secret),rooms=new RoomServiceClient(env.LIVEKIT_URL,key,secret);
const roomName=`f09-ingress-${randomUUID()}`,publisher=`f09-publisher-${randomUUID()}`;
const room=new rtc.Room();let ingress,ffmpeg,stream,reader,audioTask,audioError;const tone=new DecodedToneWindow();let stopping=false;let ffmpegResult;const stderrHash=createHash('sha256');
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function wait(check,label,ms=45000){const end=Date.now()+ms;while(Date.now()<end){const value=await check();if(value)return value;await sleep(100);}throw Error(`${label} deadline`);}
async function children(){const {stdout}=await exec('/bin/ps',['-axo','pid=,ppid=,comm='],{maxBuffer:1024*1024});return stdout.split('\n').map(line=>line.trim().match(/^(\d+)\s+(\d+)\s+(.+)$/)).filter(Boolean).filter(m=>Number(m[2])===Number(env.INGRESS_WORKER_PID)&&m[3].split('/').at(-1)==='ingress').map(m=>Number(m[1]));}
let activeChild;
room.on(rtc.RoomEvent.TrackSubscribed,(track,publication,participant)=>{
 if(participant.identity!==publisher||track.kind!==rtc.TrackKind.KIND_AUDIO||stream)return;
 stream=new rtc.AudioStream(track,{sampleRate:48000,numChannels:1});reader=stream.getReader();
 audioTask=(async()=>{while(!stopping){const {value:frame,done}=await reader.read();if(done)break;tone.push(frame.data);}})();audioTask.catch(e=>{if(!stopping)audioError=e;});
});
try{
 assert.equal((await children()).length,0,'dedicated worker must have no pre-existing handlers');
 await rooms.createRoom({name:roomName});
 const token=new AccessToken(key,secret,{identity:`f09-subscriber-${randomUUID()}`});token.addGrant({roomJoin:true,room:roomName,canSubscribe:true,canPublish:false});
 await room.connect(env.LIVEKIT_URL.replace(/^http/,'ws'),await token.toJwt(),{autoSubscribe:true});
 ingress=await client.createIngress(0,{name:'actual RTMP sine',roomName,participantIdentity:publisher,participantName:'actual external Ingress',enableTranscoding:true});
 assert.equal(ingress.state.status,0);assert(ingress.streamKey);assert(ingress.url.startsWith('rtmp://'));
 const inputURL=`${ingress.url.replace(/\/$/,'')}/${ingress.streamKey}`;
 ffmpeg=spawn(env.FFMPEG_BIN||'ffmpeg',['-nostdin','-hide_banner','-loglevel','error','-re','-f','lavfi','-i','sine=frequency=440:sample_rate=48000','-vn','-c:a','aac','-b:a','128k','-ar','48000','-ac','1','-f','flv',inputURL],{stdio:['ignore','ignore','pipe']});
 ffmpeg.stderr.on('data',b=>stderrHash.update(b));ffmpegResult=new Promise((resolve,reject)=>{ffmpeg.once('error',reject);ffmpeg.once('close',(code,signal)=>resolve({code,signal}));});ffmpegResult.catch(()=>{});
 activeChild=await wait(async()=>{const ids=await children();assert(ids.length<=1,'unexpected multiple handlers');return ids[0];},'actual official run-handler child');
 const publishing=await wait(async()=>{const list=await client.listIngress({ingressId:ingress.ingressId});const value=list[0];if(value?.state?.status===3)throw Error('actual worker reports ENDPOINT_ERROR');return value?.state?.status===2?value:null;},'actual IOInfo publishing callback');
 assert(publishing.state.resourceId);assert(BigInt(publishing.state.startedAt)>0n);assert(BigInt(publishing.state.updatedAt)>0n);
 const contentWindow=await wait(()=>{if(audioError)throw audioError;return tone.readyWindow();},'two consecutive seconds of genuinely decoded 440Hz PCM');
 const rms=contentWindow.rms,tonePowerRatio=contentWindow.tone_power_ratio;
 assert(rms>100,'real decoded audio must be non-silent');assert(tonePowerRatio>0.5,'decoded PCM must contain injected 440Hz tone, not unrelated audio');
 report.observations={actual_handler_child:true,publishing_callback:true,resource_generation_present:true,started_updated_timestamp_present:true,decoded_frames:tone.frames,decoded_samples:tone.samples,rms,tone_power_ratio:tonePowerRatio,content_window:contentWindow,decoded_content:tone.snapshot()};
 const deleted=await client.deleteIngress(ingress.ingressId);assert.equal(deleted.ingressId,ingress.ingressId);
 await wait(async()=>!(await children()).includes(activeChild),'actual handler exits after active Delete',15000);
 await wait(()=>!Array.from(room.remoteParticipants.values()).some(p=>p.identity===publisher),'published participant leaves after active Delete',15000);
 await wait(()=>ffmpeg.exitCode!==null||ffmpeg.signalCode!==null,'actual RTMP producer connection closes after Delete',15000);
 report.observations.ffmpeg_after_delete=await ffmpegResult;report.observations.active_delete_stopped_handler=true;report.observations.active_delete_closed_rtmp=true;
 await assert.rejects(()=>client.listIngress({ingressId:ingress.ingressId}),error=>error?.code==='not_found','deleted List-by-ID must return source not_found');
 ingress=null;report.status='PASS';
}catch(error){report.status='FAIL';report.failure={name:error.name,message:sha(String(error.message))};}
finally{
 report.decoded_content_final=tone.snapshot();
 stopping=true;
 if(ffmpeg&&ffmpeg.exitCode===null&&ffmpeg.signalCode===null){ffmpeg.kill('SIGTERM');try{await wait(()=>ffmpeg.exitCode!==null||ffmpeg.signalCode!==null,'owned ffmpeg cleanup',5000);}catch{ffmpeg.kill('SIGKILL');report.cleanup_errors.push('owned ffmpeg required forced termination');}}
 try{if(reader)await reader.cancel();if(audioTask)await audioTask;}catch{report.cleanup_errors.push('decoded audio reader cleanup failed');}
 try{await room.disconnect();}catch{report.cleanup_errors.push('RTC disconnect failed');}
 try{if(ingress)await client.deleteIngress(ingress.ingressId);}catch{report.cleanup_errors.push('owned ingress deletion failed');}
 try{await rooms.deleteRoom(roomName);}catch{report.cleanup_errors.push('owned room deletion failed');}
 try{await rtc.dispose();}catch{report.cleanup_errors.push('public native RTC runtime dispose failed');}
 report.ffmpeg_stderr_sha256=stderrHash.digest('hex');if(report.cleanup_errors.length)report.status='FAIL';
 await writeFile(env.INGRESS_MEDIA_OUTPUT,JSON.stringify(report,null,2)+'\n',{mode:0o600});if(report.status!=='PASS')process.exitCode=1;
}
