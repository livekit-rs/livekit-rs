/** Draft, NOT_RUN. Requires actual Rust/Go LiveKit server and unmodified SIP worker. */
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {createServer} from 'node:http';
import {access,readFile,writeFile,mkdir} from 'node:fs/promises';
import {dirname,join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {randomBytes} from 'node:crypto';
import {LocalSipPeer} from './peer.mjs';
const here=dirname(fileURLToPath(import.meta.url));
const packageFile=resolve(process.env.INTEROP_PACKAGE||join(here,'../package.json'));
const require=createRequire(packageFile),{chromium}=require('playwright'),{AccessToken,RoomServiceClient,SipClient}=require('livekit-server-sdk');
const sdk=await readFile(require.resolve('livekit-client')),html=await readFile(join(here,'client.html'));
const url=process.env.LIVEKIT_URL||'ws://127.0.0.1:7880',key=process.env.LIVEKIT_API_KEY||'devkey',secret=process.env.LIVEKIT_API_SECRET||'secret';
const output=resolve(process.env.SIP_MEDIA_OUTPUT||join(here,'output','sip-media.json'));
const topologyBarrier=process.env.SIP_MEDIA_TOPOLOGY_BARRIER;
const workerHealth=process.env.SIP_MEDIA_WORKER_HEALTH_URL;
const roomName=process.env.SIP_MEDIA_ROOM_NAME||`f07-local-sip-${randomBytes(5).toString('hex')}`,sipIdentity='f07-sip-caller';
const api=new RoomServiceClient(url.replace(/^ws/,'http'),key,secret),sip=new SipClient(url.replace(/^ws/,'http'),key,secret);
function safeError(error){return String(error.message).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<token>').replaceAll(secret,'<api-secret>');}
const evidence={profile:'f07-local-sip-media',status:'RUNNING',scope:'Unmodified official SDK Create/Transfer with real SIP worker, local digest-authenticated PCMU UA and browser RTC Opus; genuine target INVITE/ACK before final REFER NOTIFY. Not PSTN/provider/TLS/DTMF/hold/transcoding matrix acceptance.',startedAt:new Date().toISOString(),steps:[],versions:{}};
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function until(check,label,timeout=15000){const end=Date.now()+timeout;while(Date.now()<end){if(await check())return;await sleep(50);}throw Error(label);}
let peer,target,host,browser,page,trunk,roomCreated=false;
const failures=[];
try{
 for(const name of ['playwright','livekit-client','livekit-server-sdk']){try{evidence.versions[name]=require(`${name}/package.json`).version;}catch{evidence.versions[name]='see locked package.json; package exports restrict metadata';}}
 target=await new LocalSipPeer({authenticate:false,media:false}).start();peer=await new LocalSipPeer({authenticate:true,media:true}).start();
 host=createServer((req,res)=>{if(!['/','/sdk.js'].includes(req.url)){res.writeHead(404);res.end();return;}res.writeHead(200,{'content-type':req.url==='/'?'text/html':'text/javascript'});res.end(req.url==='/'?html:sdk);});await new Promise(r=>host.listen(0,'127.0.0.1',r));
 await api.createRoom({name:roomName,emptyTimeout:30});roomCreated=true;
 browser=await chromium.launch({channel:process.env.BROWSER_CHANNEL||'chrome',headless:true,args:['--no-sandbox','--autoplay-policy=no-user-gesture-required']});evidence.versions.browser=browser.version();page=await browser.newPage();await page.goto(`http://127.0.0.1:${host.address().port}`);
 const token=new AccessToken(key,secret,{identity:'f07-rtc-observer'});token.addGrant({roomJoin:true,room:roomName,canPublish:true,canSubscribe:true});
 await page.evaluate(args=>fixture.connect(args),{url,token:await token.toJwt(),sipIdentity});evidence.steps.push('official-client-connected-and-real-880hz-published');
 // Numeric transport0 is fixed protocol SIP_TRANSPORT_AUTO; force UDP1 by source enum below.
 const {SIPTransport}=require('@livekit/protocol');
 assert.equal(SIPTransport.SIP_TRANSPORT_UDP,1,'locked SDK protocol enum drift');
 trunk=await sip.createSipOutboundTrunk('f07-local-only',`127.0.0.1:${peer.port}`,['+12025550123'],{transport:SIPTransport.SIP_TRANSPORT_UDP,authUsername:peer.username,authPassword:peer.password});
 if(workerHealth){
  await until(async()=>{
   const response=await fetch(workerHealth,{signal:AbortSignal.timeout(1000)}).catch(()=>null);
   return response?.ok&&await response.text()==='OK';
  },'original SIP worker stayed under load before admission',45000);
  evidence.steps.push('original-SIP-worker-HealthOK-before-admission');
 }
 evidence.fixtureResources={room:roomName,trunk:trunk.sipTrunkId};
 const result=await sip.createSipParticipant(trunk.sipTrunkId,'15550001',roomName,{participantIdentity:sipIdentity,participantName:'Local SIP interoperability',waitUntilAnswered:true,ringingTimeout:10,maxCallDuration:topologyBarrier?180:45,timeout:45});
 assert.equal(result.participantIdentity,sipIdentity);assert.equal(result.roomName,roomName);assert.match(result.sipCallId,/^SCL_/);
 await until(async()=>{const participants=await api.listParticipants(roomName);return participants.some(p=>p.identity===sipIdentity&&p.attributes['sip.callID']===result.sipCallId);},'actual SIP RTC participant missing');
 evidence.steps.push('actual-worker-call-created-and-participant-verified');
 await until(async()=>{const p=peer.snapshot(),b=await page.evaluate(()=>fixture.snapshot());return p.authenticatedInvites>=1&&p.acks>=1&&p.rtpSent>=75&&p.rtpReceived>=75&&p.rtpTone880.ready&&p.rtpTone880.rms>.02&&p.rtpTone880.toneAmplitude>.025&&p.rtpTone880.toneAmplitude>p.rtpTone880.reference440Amplitude*5&&b.subscribed>=1&&b.tone440?.rms>.02&&b.tone440?.toneAmplitude>.025;},'two-way decoded audio content absent',20000);
 evidence.mediaBeforeTransfer={peer:peer.snapshot(),browser:await page.evaluate(()=>fixture.snapshot())};assert.deepEqual(evidence.mediaBeforeTransfer.peer.errors,[]);assert.equal(evidence.mediaBeforeTransfer.peer.overflow,false);evidence.steps.push('actual-PCMU-440-to-RTC-and-RTC-880-to-PCMU-content-proved');
 if(topologyBarrier){
  await writeFile(join(topologyBarrier,'call-ready.json'),JSON.stringify({callId:result.sipCallId,room:roomName,participant:sipIdentity}));
  await until(async()=>access(join(topologyBarrier,'promoted')).then(()=>true,()=>false),'owned topology promotion did not finish',25000);
  const before=peer.snapshot();
  const browserBefore=await page.evaluate(()=>fixture.snapshot());
  const received=browser=>browser.receivers.flatMap(r=>r.inbound).reduce((sum,r)=>sum+(r.packetsReceived||0),0);
  await until(async()=>{
   const after=peer.snapshot();
   const browserAfter=await page.evaluate(()=>fixture.snapshot());
   if(after.rtpSent-before.rtpSent<200||after.rtpReceived-before.rtpReceived<200||
      received(browserAfter)-received(browserBefore)<150||browserAfter.disconnected!==0||
      browserAfter.subscribed!==browserBefore.subscribed||after.rtpTone880.rms<=.02||
      after.rtpTone880.toneAmplitude<=.025||after.rtpTone880.toneAmplitude<=after.rtpTone880.reference440Amplitude*5||
      (browserAfter.tone440?.rms??0)<=.02||(browserAfter.tone440?.toneAmplitude??0)<=.025)return false;
   evidence.mediaAfterTopology={peer:after,browser:browserAfter,receivedSincePromotion:{
    sip:after.rtpReceived-before.rtpReceived,rtc:received(browserAfter)-received(browserBefore),
   }};
   return true;
  },'existing call did not decode two-way audio after topology change');
  evidence.steps.push('existing-call-media-after-live-topology-promotion');
 }
 if(topologyBarrier)await sleep(Number(process.env.SIP_MEDIA_SETTLE_SECONDS||0)*1000);
 evidence.transfer={issuedUnix:Date.now()/1000,ok:false};
 try {
  await sip.transferSipParticipant(roomName,sipIdentity,target.uri,{playDialtone:false,ringingTimeout:10});
  evidence.transfer.ok=true;
 } catch(error) {
  evidence.transfer.error=safeError(error);
  throw error;
 } finally {
  evidence.transfer.finishedUnix=Date.now()/1000;
 }
 await until(()=>{const a=peer.snapshot(),b=target.snapshot();return a.referReceived===1&&a.targetInviteAccepted===1&&a.notifySent>=2&&a.notifyAccepted>=2&&b.acceptedInvites===1&&b.acks>=1;},'REFER must reach actual local answered target and accepted final NOTIFY');
 await until(()=>peer.snapshot().byeReceived>=1,'worker did not actually terminate original SIP dialog');
 await until(async()=>!(await api.listParticipants(roomName)).some(p=>p.identity===sipIdentity),'actual SIP participant did not leave RTC room after transfer');
 evidence.sipTransfer={rtc_left:true};
 evidence.afterTransfer={peer:peer.snapshot(),target:target.snapshot(),browser:await page.evaluate(()=>fixture.snapshot())};assert.deepEqual(peer.snapshot().errors,[]);assert.deepEqual(target.snapshot().errors,[]);evidence.steps.push('REFER-target-INVITE-ACK-NOTIFY-success-original-BYE-and-RTC-leave');evidence.status='PASS';
}catch(error){evidence.status='FAIL';evidence.failureSnapshots={peer:peer?.snapshot(),target:target?.snapshot()};try{evidence.failureSnapshots.browser=await page?.evaluate(()=>fixture.snapshot());}catch{evidence.failureSnapshots.browser='unavailable';}evidence.failure={name:error.name,message:safeError(error)};process.exitCode=1;}
finally{
 // Cleanup only task-owned room/trunk/browser/UA; never stop external worker/server.
 try{await page?.evaluate(()=>fixture.close());}catch(e){failures.push('browser-room-close');}
 try{await browser?.close();}catch(e){failures.push('browser-close');}
 const cleanup=async(operation,run)=>{
  const row={operation,startedUnix:Date.now()/1000,ok:false};(evidence.cleanup??=[]).push(row);
  try{await run();row.ok=true;}catch(error){row.error=safeError(error);failures.push(operation);}
  finally{row.finishedUnix=Date.now()/1000;}
 };
 if(trunk)await cleanup('owned-trunk-delete',()=>sip.deleteSipTrunk(trunk.sipTrunkId));
 if(roomCreated)await cleanup('owned-room-delete',()=>api.deleteRoom(roomName));
 if(topologyBarrier&&peer&&!evidence.sipTransfer?.rtc_left){
  try{evidence.fixtureOriginalDialogByeAccepted=await peer.endOriginalDialog();}
  catch(error){evidence.fixtureOriginalDialogByeError=safeError(error);}
 }
 try{await peer?.close();}catch(e){failures.push('peer-close');}
 try{await target?.close();}catch(e){failures.push('target-close');}
 if(host)await new Promise(r=>host.close(r));
 evidence.finishedAt=new Date().toISOString();if(failures.length){evidence.cleanupFailures=failures;evidence.status='FAIL';process.exitCode=1;}await mkdir(dirname(output),{recursive:true});await writeFile(output,JSON.stringify(evidence,null,2)+'\n');
}
