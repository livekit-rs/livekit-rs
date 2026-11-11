// Official SDK media recovery under real packet impairment, one server per process (pinned Go or Rust).
// A publishes video (PROFILE: vp8 | vp8-simulcast | vp9-svc | h264 | av1) + audio, B subscribes; each goes through its own TURN/UDP impairment proxy
// (forced relay). Phases impair B's downlink or A's uplink with loss or a blackout; per-phase getStats deltas
// and post-blackout decode recovery times are recorded. Every decoded frame's embedded counter is read
// back (validity, order, lag behind the publisher) and the proxy records the RTP headers B actually received.
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {readFile,writeFile,mkdir} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {dirname,join,isAbsolute} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
import {LossyUdpProxy} from './lossy-proxy.mjs';
const here=dirname(fileURLToPath(import.meta.url));
const deps=process.env.INTEROP_DEPENDENCY_ROOT;assert.ok(deps&&isAbsolute(deps));
const require=createRequire(join(deps,'package.json'));
const {chromium}=require('playwright');const {AccessToken,RoomServiceClient}=require('livekit-server-sdk');
const out=process.env.INTEROP_OUTPUT_DIR;assert.ok(out&&isAbsolute(out));await mkdir(out,{recursive:false});
const url=process.env.LIVEKIT_URL,turnPort=Number(process.env.TURN_PORT),single=process.env.SINGLE_PC!=='false';
const profileName=process.env.PROFILE||'vp8';
const e2ee=process.env.E2EE==='true';
const layerSwitch=process.env.LK_REC_LAYER_SWITCH==='1';
const vp9PliControl=process.env.LK_REC_VP9_PLI_CONTROL==='1';
const traceFrames=process.env.LK_REC_FRAME_TRACE==='1';
const stableSource=layerSwitch||process.env.LK_REC_STABLE_SOURCE==='1';
if(traceFrames)assert.ok(e2ee&&profileName==='vp9-svc','frame trace requires encrypted VP9');
// RELAY=false: diagnostic only, direct ICE without the impairment proxy (faults have no effect).
const relay=process.env.RELAY!=='false';
const api=new RoomServiceClient(url.replace(/^ws/,'http'),'devkey','secret');
const sdk=await readFile(require.resolve('livekit-client'));const page=await readFile(join(here,'client.html'));
const worker=e2ee?await readFile(require.resolve('livekit-client/e2ee-worker')):null;
const traceWorker=traceFrames?await readFile(join(here,'frame-trace-worker.mjs')):null;
const lock=await readFile(join(deps,'package-lock.json'));
const sdkVersion=JSON.parse(lock).packages['node_modules/livekit-client'].version;
const sha=b=>createHash('sha256').update(b).digest('hex');
const host=createServer((q,r)=>{const b=q.url==='/'?page:q.url==='/sdk.js'?sdk:q.url==='/e2ee-worker.mjs'?worker:q.url==='/frame-trace-worker.mjs'?traceWorker:null;if(!b){r.writeHead(404);r.end();return;}r.writeHead(200,{'content-type':q.url==='/'?'text/html':'text/javascript','cache-control':'no-store'});r.end(b);});
await new Promise(r=>host.listen(0,'127.0.0.1',r));
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
const proxyA=await new LossyUdpProxy({upstreamPort:turnPort,seed:7}).start(),proxyB=await new LossyUdpProxy({upstreamPort:turnPort,seed:11}).start();
const browser=await chromium.launch({channel:process.env.BROWSER_CHANNEL||'chromium',headless:true,args:['--autoplay-policy=no-user-gesture-required']});
const fixtureNames=['client.html','run.mjs','lossy-proxy.mjs'];
if(layerSwitch)fixtureNames.push('layer-switch.mjs');
if(traceFrames)fixtureNames.push('frame-trace-worker.mjs');
const fixtureHashes=Object.fromEntries(await Promise.all(fixtureNames.map(async name=>[name,sha(await readFile(join(here,name)))])));
const evidence={sdkVersion,lockSha256:sha(lock),sdkSha256:sha(sdk),workerSha256:worker?sha(worker):null,fixtureHashes,single,e2ee,profile:profileName,url,browser:browser.version(),phases:[]};
if(traceFrames)evidence.frameTrace={stableSource,clock:'performance.timeOrigin + performance.now (local Unix epoch)'};
const room=`rec-${Date.now()}`;
async function client(identity,proxy){const c=await browser.newContext();const p=await c.newPage();const errors=[];p.on('pageerror',e=>errors.push(String(e)));
 await p.goto(`http://127.0.0.1:${host.address().port}/`);
 const t=new AccessToken('devkey','secret',{identity,ttl:'10m'});t.addGrant({roomJoin:true,room,canPublish:true,canSubscribe:true});
 await p.evaluate(o=>rec.connect(o),{url,token:await t.toJwt(),single,proxyPort:proxy.port,turnPort,relay,e2ee,frameTrace:traceFrames});return {c,p,errors};}
const delta=(a,b)=>Object.fromEntries(Object.keys(b).map(k=>[k,(b[k]||0)-(a?.[k]||0)]));
// Decoded-frame codes: -1 = check failed (corrupted picture), otherwise the publisher's frame counter.
function codeFacts(frames){
 let valid=0,invalid=0,regressions=0,last=null,firstValidAt=null;
 for(const f of frames){if(f.code===null)continue;if(f.code<0){invalid++;continue;}valid++;firstValidAt??=f.at;
  if(last!==null){const d=(f.code-last+65536)%65536;if(d>=32768)regressions++;}last=f.code;}
 return {decoded:frames.length,valid,invalid,regressions,lastCode:last,firstValidAt,sizes:[...new Set(frames.filter(f=>f.w).map(f=>`${f.w}x${f.h}`))]};}
async function lagFrames(){const pub=await A.p.evaluate(()=>rec.publisherFrame());const recent=await B.p.evaluate(()=>rec.decodedSince(performance.now()-400));
 const codes=recent.filter(f=>f.code>=0).map(f=>f.code);if(!codes.length)return null;return ((pub&0xffff)-codes.at(-1)+65536)%65536;}
let A,B,roomCreated=false;
try{
 await api.createRoom({name:room,emptyTimeout:30,departureTimeout:5});
 roomCreated=true;
 A=await client('a',proxyA);B=await client('b',proxyB);
 await A.p.evaluate(({profile,stableSource})=>rec.publish(profile,stableSource),{profile:profileName,stableSource});
 const ready=async()=>{for(let i=0;i<150;i++){const s=await B.p.evaluate(()=>rec.stats());if(s.inbound.framesDecoded>30)return s;await sleep(200);}throw Error('subscriber never decoded video');};
 await ready();
 // Session start: receive-side estimate ramp and the one-way delay of the first packets.
 evidence.start={remb:[]};
 for(let i=0;i<32;i++){const l=await B.p.evaluate(()=>rec.layers());evidence.start.remb.push([i*250,l.subBwe,l.sub?.w??null]);await sleep(250);}
 evidence.start.downRtp=proxyB.rtpSummary('down',0,proxyB.now());
 evidence.relay={a:(await A.p.evaluate(()=>rec.stats())).pairs,b:(await B.p.evaluate(()=>rec.stats())).pairs};
 if(relay)assert.ok(Object.values(evidence.relay).every(pairs=>pairs.length&&pairs.every(p=>p.type==='relay')),'both clients must select TURN through the proxy');
 evidence.feedback={a:await A.p.evaluate(()=>rec.feedback()),b:await B.p.evaluate(()=>rec.feedback())};
 const layered=profileName==='vp8-simulcast'||profileName==='vp9-svc';
 if(layered)evidence.videoLossSchema=1;
 const videoStats=async()=>{
  const [publisher,subscriber]=await Promise.all([A.p.evaluate(()=>rec.layers()),B.p.evaluate(()=>rec.layers())]);
  return {publisher:publisher.rids,subscriber:subscriber.inbound};
 };
 async function phase(name,{proxy,direction,profile,ms,blackoutMs}){
  const a0=await A.p.evaluate(()=>rec.stats()),b0=await B.p.evaluate(()=>rec.stats()),p0=proxy?.stats();
  const pageStart=b0.now,proxyStart=proxyB.now();
  let recoveryMs=null,afterRestore=null,restoreRtp=null,blackoutTiming=null,videoPath=null,videoLoss=null,videoFeedback=null,frameTrace=null;var timeline;
  if(blackoutMs){
   timeline=[];const t0=await B.p.evaluate(()=>performance.now());
   const sample=async tag=>{const [a,b,la,lb]=await Promise.all([
    A.p.evaluate(()=>rec.stats()),B.p.evaluate(()=>rec.stats()),A.p.evaluate(()=>rec.layers()),B.p.evaluate(()=>rec.layers()),
   ]);
    timeline.push({tag,ms:Math.round(b.now-t0),pubBwe:la.pubBwe,rids:la.rids.map(r=>`${r.rid??'-'}:${r.active===false?'off':r.w}@${Math.round(r.target/1000)}k/${r.limit}`).join(' '),
     publisherVideo:la.rids,subscriberVideo:lb.inbound,
     subW:lb.sub?.w??null,subFps:lb.sub?.fps??null,subBwe:lb.subBwe,subPubBwe:lb.pubBwe,rtt:{a:la.rtcpRtt,b:lb.rtcpRtt,aStun:la.stunRtt,bStun:lb.stunRtt},aPli:a.outbound.pliCount,aFir:a.outbound.firCount,aKey:a.outbound.keyFramesEncoded,aNack:a.outbound.nackCount,bDecoded:b.inbound.framesDecoded,bPli:b.inbound.pliCount,bKey:b.inbound.keyFramesDecoded,bLost:b.inbound.packetsLost,bRtx:b.inbound.retransmittedPacketsReceived});};
   const sampleUntil=async (tag,deadline,interval)=>{
    while(proxyB.now()<deadline){const started=proxyB.now();await sample(tag);
     const wait=Math.min(started+interval,deadline)-proxyB.now();if(wait>0)await sleep(wait);}
   };
   const captureAudio=async (peer,origin)=>{
    const requested=proxyB.now()-origin;
    const snapshot=await peer.p.evaluate(()=>rec.audioSnapshot());
    return {proxyRequestedMs:requested,proxyCompletedMs:proxyB.now()-origin,...snapshot};
   };
   const blackoutStart=proxyB.now();
   proxy.set(direction,{blackout:true});
   // The release timer is independent of how long any browser getStats takes.
   const window=sleep(Math.max(0,blackoutStart+blackoutMs-proxyB.now())).then(async()=>{
    proxy.clean();
    const origin=proxyB.now(),end=origin+3000;
    const initial=captureAudio(B,origin),sender=captureAudio(A,origin);
    const restoredTimeline=sampleUntil('restored',end,100);
    await sleep(Math.max(0,end-proxyB.now()));
    const final=captureAudio(B,origin);
    const [first,last,source]=await Promise.all([initial,final,sender,restoredTimeline]);
    return {origin,first,last,source};
   });
   const [{origin:restoredProxy,first,last,source}]=await Promise.all([window,sampleUntil('blackout',blackoutStart+blackoutMs,200)]);
   const restoredPage=first.requested;
   // Keep the later lag observations outside the audio measurement window.
   // Lag behind the publisher and decoded size every 500 ms after recovery.
   var lagTimeline=[];for(let i=0;i<4;i++){
    const [f,encoder,decoder]=await Promise.all([B.p.evaluate(()=>rec.decodedSince(performance.now()-300)),
     A.p.evaluate(()=>rec.layers()),B.p.evaluate(()=>rec.layers())]);
    lagTimeline.push({lag:await lagFrames(),size:f.at(-1)?`${f.at(-1).w}x${f.at(-1).h}`:null,
     publisherVideo:encoder.rids,subscriberVideo:decoder.inbound});await sleep(500);
   }
   // First 3 s after the impairment ended: decoded picture facts, audio and received RTP headers.
   const frames=await B.p.evaluate(t=>rec.decodedSince(t),restoredPage);
   const firstWindow=frames.filter(f=>f.at<=restoredPage+3000);
   afterRestore={...codeFacts(firstWindow),firstValidMs:null,lagFrames:await lagFrames(),lagTimeline,
    audio:delta(first.audio,last.audio)};
   if(afterRestore.firstValidAt!==null)afterRestore.firstValidMs=Math.round(afterRestore.firstValidAt-restoredPage);
   delete afterRestore.firstValidAt;
   const decoded=frames.find(f=>f.at>=restoredPage);
   if(decoded)recoveryMs=Math.max(0,Math.round(decoded.at-restoredPage));
   restoreRtp={outage:proxyB.rtpSummary('down',blackoutStart-500,restoredProxy),afterRestore:proxyB.rtpSummary('down',restoredProxy,restoredProxy+3000)};
   const trace=(proxy,side,streams)=>proxy.streamRtp(side,streams,blackoutStart-500,restoredProxy+3000,restoredProxy);
   blackoutTiming={actualBlackoutMs:restoredProxy-blackoutStart,receiverFirst:first,receiverLast:last,publisher:source,
    publisherRtp:trace(proxyA,'up',source.streams.filter(s=>s.direction==='outbound')),
    receiverRtp:trace(proxyB,'down',last.streams.filter(s=>s.direction==='inbound'))};
   if(direction==='down'){
    const feedback=(proxy,direction)=>proxy.rtcpTimeline(direction,blackoutStart,restoredProxy+3000,restoredProxy)
     .filter(({pt,fmt})=>(pt===205&&fmt===1)||(pt===206&&[1,4].includes(fmt)));
    videoFeedback={subscriberSent:feedback(proxyB,'up'),publisherReceived:feedback(proxyA,'down')};
    if(traceFrames){
     const [publisher,subscriber]=await Promise.all([A,B].map(peer=>peer.p.evaluate(()=>rec.frameTrace())));
     const start=performance.timeOrigin+blackoutStart-500,release=performance.timeOrigin+restoredProxy,end=release+3000;
     const offset=time=>+(time-release).toFixed(3);
     const keyframes=peer=>peer.keyframes.filter(frame=>frame.unixMs>=start&&frame.unixMs<=end)
      .map(({unixMs,...frame})=>({...frame,ms:offset(unixMs)}));
     const pictures=subscriber.pictures.filter(frame=>subscriber.timeOrigin+frame.at>=start&&subscriber.timeOrigin+frame.at<=end)
      .map(({at,receiveTime,...frame})=>({...frame,ms:offset(subscriber.timeOrigin+at),
       receiveMs:receiveTime===undefined?null:offset(subscriber.timeOrigin+receiveTime)}));
     const first=pictures.findIndex(frame=>frame.ms>=0);
     const before=pictures.filter(frame=>frame.ms<0).at(-1)??null;
     const after=first===-1?[]:pictures.slice(first,first+3);
     const tracePeer=(proxy,direction,frames)=>proxy.frameRtp(direction,frames.map(frame=>frame.rtpTimestamp),
      blackoutStart-500,restoredProxy+3000,restoredProxy);
     const source=keyframes(publisher),received=keyframes(subscriber);
     frameTrace={baselineKeyframes:{publisher:publisher.keyframes.filter(frame=>frame.unixMs<start).length,
      subscriber:subscriber.keyframes.filter(frame=>frame.unixMs<start).length},
      publisherKeyframes:source,subscriberKeyframes:received,lastPictureBeforeRelease:before,firstPicturesAfterRelease:after,
      publisherWire:tracePeer(proxyA,'up',source),subscriberWire:tracePeer(proxyB,'down',[...source,...received,...after])};
    }
   }
   if(direction==='up'){
    const outbound=timeline.flatMap(sample=>sample.publisherVideo);
    const inbound=timeline.flatMap(sample=>sample.subscriberVideo);
    videoPath={publisherRtp:trace(proxyA,'up',outbound),subscriberRtp:trace(proxyB,'down',inbound),
     publisherFeedbackHeaders:proxyA.rtcpTimeline('down',blackoutStart-500,restoredProxy+3000,restoredProxy)};
   }
  }else if(layered&&profile?.loss>0){
   const before=await videoStats(),pageStart=await B.p.evaluate(()=>performance.now()),from=proxy.now();
   proxy.set(direction,profile);
   await sleep(ms);
   proxy.clean();
   const to=proxy.now(),pageEnd=await B.p.evaluate(()=>performance.now()),after=await videoStats();
   const side=direction==='up'?'publisher':'subscriber';
   const streams=[...new Map([...before[side],...after[side]].map(s=>[`${s.ssrc}:${s.pt}`,s])).values()];
   const frames=await B.p.evaluate(t=>rec.decodedSince(t),pageStart);
   await sleep(Math.max(1000,profile.jitterMs??0));
   videoLoss={before,after,wire:proxy.lossRtp(direction,streams,from,to),
    pictures:codeFacts(frames.filter(frame=>frame.at<=pageEnd))};
  }else{if(proxy)proxy.set(direction,profile);await sleep(ms);proxy?.clean();await sleep(1000);}
  const a1=await A.p.evaluate(()=>rec.stats()),b1=await B.p.evaluate(()=>rec.stats());
  const phaseFrames=await B.p.evaluate(t=>rec.decodedSince(t),pageStart);
  const row={name,direction:direction??null,profile:profile??(blackoutMs?{blackoutMs}:null),publisher:delta(a0.outbound,a1.outbound),subscriber:delta(b0.inbound,b1.inbound),
   audio:delta(b0.audio,b1.audio),pictures:codeFacts(phaseFrames),lagFrames:await lagFrames(),codecs:b1.codecs,fmtp:b1.fmtp,
   downRtp:proxyB.rtpSummary('down',proxyStart),recoveryMs,afterRestore,restoreRtp,blackoutTiming,videoPath,videoLoss,videoFeedback,frameTrace,
   proxy:proxy?{before:p0,after:proxy.stats()}:null,timeline:typeof timeline==='undefined'?null:timeline};
  evidence.phases.push(row);await writeFile(join(out,'evidence.json'),JSON.stringify(evidence,null,2));
  console.log(name,JSON.stringify({publisher:row.publisher,subscriber:row.subscriber,pictures:row.pictures,lagFrames:row.lagFrames,recoveryMs,afterRestore}));
 }
 if(layerSwitch){
  assert.ok(layered,'explicit layer switching requires a layered profile');
  const {exerciseLayerSwitch}=await import('./layer-switch.mjs');
  evidence.layerSwitch={stages:[]};
  await exerciseLayerSwitch({profileName,subscriber:B.p,proxy:proxyB,videoStats,codeFacts,report:evidence.layerSwitch});
 }else if(vp9PliControl){
  await phase('baseline',{ms:5000});
  await phase('down-blackout-3s',{proxy:proxyB,direction:'down',blackoutMs:3000});
 }else{
  await phase('baseline',{ms:5000});
  await phase('down-loss-5',{proxy:proxyB,direction:'down',profile:{loss:0.05},ms:10000});
  await phase('down-jitter-30',{proxy:proxyB,direction:'down',profile:{jitterMs:30},ms:8000});
  await phase('down-blackout-3s',{proxy:proxyB,direction:'down',blackoutMs:3000});
  await phase('up-loss-5',{proxy:proxyA,direction:'up',profile:{loss:0.05},ms:10000});
  await phase('up-jitter-30',{proxy:proxyA,direction:'up',profile:{jitterMs:30},ms:8000});
  await phase('up-blackout-3s',{proxy:proxyA,direction:'up',blackoutMs:3000});
 }
 if(e2ee){
  const [sender,receiver]=await Promise.all([A,B].map(x=>x.p.evaluate(()=>rec.stats())));
  const tracks=receiver.events.filter(e=>e.name==='TrackSubscribed'&&e.encrypted).map(e=>e.kind);
  const errors=[...sender.events,...receiver.events].filter(e=>e.name==='EncryptionError');
  evidence.encryption={tracks,errors,audioSamples:receiver.audio.totalSamplesReceived};
  assert.deepEqual([...new Set(tracks)].sort(),['audio','video']);
  assert.deepEqual(errors,[]);
  assert.ok(evidence.encryption.audioSamples>0);
  const initial=layerSwitch?evidence.layerSwitch.stages[0]:evidence.phases[0];
  assert.ok(initial.pictures.valid>0,'subscriber must decode valid encrypted video');
 }
 evidence.pageErrors=[...A.errors,...B.errors];assert.deepEqual(evidence.pageErrors,[]);evidence.status='DONE';
}catch(e){evidence.status='FAIL';evidence.error=String(e.stack||e);console.log('ERROR',e);}
finally{
 const cleanupErrors=[];
 async function clean(action){try{await action();}catch(error){cleanupErrors.push(String(error));}}
 for(const x of [A,B])if(x){await clean(()=>x.p.evaluate(()=>rec.close()));await clean(()=>x.c.close());}
 if(roomCreated)await clean(async()=>{
  await api.deleteRoom(room);
  assert.deepEqual(await api.listRooms([room]),[],'owned room must be removed');
 });
 await clean(()=>browser.close());await clean(()=>proxyA.close());await clean(()=>proxyB.close());
 await clean(()=>new Promise((resolve,reject)=>host.close(error=>error?reject(error):resolve())));
 evidence.cleanupErrors=cleanupErrors;
 if(cleanupErrors.length)evidence.status='FAIL';
 await writeFile(join(out,'evidence.json'),JSON.stringify(evidence,null,2));}
process.exit(evidence.status==='DONE'?0:1);
