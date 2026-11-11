/** NOT_RUN: real inbound SIP worker/IO callback/RTC media acceptance only. */
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {createServer} from 'node:http';
import {readFile,writeFile,mkdir} from 'node:fs/promises';
import {dirname,join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {randomBytes} from 'node:crypto';
import {InboundSipUac} from './uac.mjs';
const here=dirname(fileURLToPath(import.meta.url));
const require=createRequire(resolve(process.env.INTEROP_PACKAGE||join(here,'..','package.json')));
const {chromium}=require('playwright'),{AccessToken,RoomServiceClient,SipClient}=require('livekit-server-sdk');
const sdk=await readFile(require.resolve('livekit-client')),html=await readFile(join(here,'client.html'));
const url=process.env.LIVEKIT_URL||'ws://127.0.0.1:7880',key=process.env.LIVEKIT_API_KEY||'devkey',secret=process.env.LIVEKIT_API_SECRET||'secret';
const workerPort=Number(process.env.SIP_WORKER_PORT||15060);assert.ok(Number.isInteger(workerPort)&&workerPort>0&&workerPort<65536);
const output=resolve(process.env.SIP_INBOUND_OUTPUT||join(here,'output','sip-inbound.json'));
const roomName=`f08-local-inbound-${randomBytes(5).toString('hex')}`,observerIdentity='f08-rtc-observer',calledNumber='+12025550222';
const username=`f08-${randomBytes(4).toString('hex')}`,password=randomBytes(20).toString('hex');
const api=new RoomServiceClient(url.replace(/^ws/,'http'),key,secret),sip=new SipClient(url.replace(/^ws/,'http'),key,secret);
const evidence={profile:'f08-official-sip-inbound',status:'RUNNING',scope:'Unmodified Server SDK inbound trunk/direct dispatch CRUD; real local UAC Digest407→official SIP worker→Rust IOAuth/dispatch→RTC; bidirectional decoded440/880 content and actual BYE→room leave. No PSTN/provider/TLS/SRTP/PIN/DTMF acceptance.',steps:[],startedAt:new Date().toISOString(),versions:{}};
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function until(check,label,timeout=15000){const end=Date.now()+timeout;while(Date.now()<end){if(await check())return;await sleep(50);}throw Error(label);}
const owned=[];let host,browser,page,trunk,rule,roomCreated=false,good,bad;const cleanupFailures=[];
try{
 for(const name of ['playwright','livekit-client','livekit-server-sdk']){try{evidence.versions[name]=require(`${name}/package.json`).version;}catch{evidence.versions[name]='locked package metadata';}}
 await api.createRoom({name:roomName,emptyTimeout:30});roomCreated=true;
 trunk=await sip.createSipInboundTrunk('f08-loopback-authenticated',[calledNumber],{allowedAddresses:['127.0.0.1/32'],authUsername:username,authPassword:password,ringingTimeout:10});
 rule=await sip.createSipDispatchRule({type:'direct',roomName},{name:'f08-direct-owned-room',trunkIds:[trunk.sipTrunkId]});
 assert.match(trunk.sipTrunkId,/^ST_/);assert.match(rule.sipDispatchRuleId,/^SDR_/);evidence.steps.push('official-sdk-inbound-trunk-and-direct-dispatch-persisted');
 host=createServer((req,res)=>{if(!['/','/sdk.js'].includes(req.url)){res.writeHead(404);res.end();return;}res.writeHead(200,{'content-type':req.url==='/'?'text/html':'text/javascript'});res.end(req.url==='/'?html:sdk);});await new Promise(r=>host.listen(0,'127.0.0.1',r));
 browser=await chromium.launch({headless:true,args:['--no-sandbox','--autoplay-policy=no-user-gesture-required']});evidence.versions.browser=browser.version();page=await browser.newPage();await page.goto(`http://127.0.0.1:${host.address().port}`);
 const token=new AccessToken(key,secret,{identity:observerIdentity});token.addGrant({roomJoin:true,room:roomName,canPublish:true,canSubscribe:true});
 await page.evaluate(args=>fixture.connect(args),{url,token:await token.toJwt(),trunkId:trunk.sipTrunkId});evidence.steps.push('unchanged-official-browser-sdk-publishes880hz');
 const uri=`sip:${calledNumber}@127.0.0.1:${workerPort}`;
 // Rejected real network credentials must not silently create a room participant.
 bad=await new InboundSipUac({username,password:`${password}-wrong`}).start();owned.push(bad);
 const rejected=await bad.dial(uri,{expectAuthReject:true});assert.equal(rejected.rejected,true);assert.equal(bad.snapshot().proxyChallenges,1);assert.equal(bad.snapshot().authRejected,1);assert.equal(bad.snapshot().acceptedAnswers,0);
 for(let i=0;i<20;i++){assert.ok(!(await api.listParticipants(roomName)).some(p=>p.attributes['sip.trunkID']===trunk.sipTrunkId),'bad Digest credentials created an RTC participant');await sleep(50);}
 evidence.rejectedCredentialProbe={proxyChallenges:bad.snapshot().proxyChallenges,authorizedInviteSent:bad.snapshot().authorizedInviteSent,authRejected:bad.snapshot().authRejected,acceptedAnswers:bad.snapshot().acceptedAnswers};evidence.steps.push('real407-and-bad-Digest-rejection-no-RTC-participant');
 await bad.close();owned.splice(owned.indexOf(bad),1);bad=null;
 good=await new InboundSipUac({username,password}).start();owned.push(good);await good.dial(uri);
 let actualParticipant;
 await until(async()=>{actualParticipant=(await api.listParticipants(roomName)).find(p=>p.attributes['sip.trunkID']===trunk.sipTrunkId&&p.attributes['sip.ruleID']===rule.sipDispatchRuleId);return !!actualParticipant&&!!actualParticipant.attributes['sip.callID'];},'actual inbound SIP participant did not carry matched trunk/dispatch attributes');
 const sipIdentity=actualParticipant.identity;
 evidence.participantAuthority={actualPresent:true,trunkMatches:true,dispatchMatches:true,callIdPresent:true};evidence.steps.push('worker-inbound-auth-and-real-direct-dispatch-RTC-join');
 await until(async()=>{const p=good.snapshot(),b=await page.evaluate(()=>fixture.snapshot());return p.proxyChallenges>=1&&p.authorizedInviteSent===1&&p.acceptedAnswers===1&&p.ackSent>=2&&p.rtpSent>=75&&p.rtpReceived>=75&&p.rtpTone880.ready&&p.rtpTone880.rms>.02&&p.rtpTone880.toneAmplitude>.025&&p.rtpTone880.toneAmplitude>p.rtpTone880.reference440Amplitude*5&&b.subscribed>=1&&b.tone440?.rms>.02&&b.tone440?.toneAmplitude>.025;},'real inbound bidirectional decoded audio absent',20000);
 evidence.media={uac:good.snapshot(),browser:await page.evaluate(()=>fixture.snapshot())};assert.deepEqual(evidence.media.uac.errors,[]);assert.equal(evidence.media.uac.overflow,false);assert.deepEqual(evidence.media.browser.errors,[]);assert.equal(evidence.media.browser.diagnosticOverflow,false);evidence.steps.push('real-PCMU440-to-RTC440-and-RTC880-to-PCMU880-content');
 await good.hangup();assert.equal(good.snapshot().byeAccepted,1);
 await until(async()=>!(await api.listParticipants(roomName)).some(p=>p.identity===sipIdentity),'actual inbound BYE did not remove SIP RTC participant');
 assert.ok((await api.listParticipants(roomName)).some(p=>p.identity===observerIdentity),'test removed whole room instead of observing actual call leave');
 evidence.afterBye={byeSent:good.snapshot().byeSent,byeAccepted:good.snapshot().byeAccepted,rtcSipParticipantAbsent:true,rtcObserverStillPresent:true,browser:await page.evaluate(()=>fixture.snapshot())};evidence.steps.push('real-UAC-BYE200-and-RTC-call-participant-leave');evidence.status='PASS';
}catch(error){evidence.status='FAIL';let message=String(error.message);for(const value of [secret,password,username])message=message.replaceAll(value,'<credential>');evidence.failure={name:error.name,message:message.replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<token>')};process.exitCode=1;}
finally{
 for(const peer of owned)try{await peer.close();}catch{cleanupFailures.push('owned-UA-close');}
 try{await page?.evaluate(()=>fixture.close());}catch{cleanupFailures.push('browser-room-close');}
 try{await browser?.close();}catch{cleanupFailures.push('browser-close');}
 if(rule)try{await sip.deleteSipDispatchRule(rule.sipDispatchRuleId);}catch{cleanupFailures.push('owned-rule-delete');}
 if(trunk)try{await sip.deleteSipTrunk(trunk.sipTrunkId);}catch{cleanupFailures.push('owned-trunk-delete');}
 if(roomCreated)try{await api.deleteRoom(roomName);}catch{cleanupFailures.push('owned-room-delete');}
 if(host)await new Promise(r=>host.close(r));
 if(cleanupFailures.length){evidence.cleanupFailures=cleanupFailures;evidence.status='FAIL';process.exitCode=1;}
 evidence.finishedAt=new Date().toISOString();await mkdir(dirname(output),{recursive:true});await writeFile(output,JSON.stringify(evidence,null,2)+'\n');
}
