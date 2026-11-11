/** NOT_RUN. Real native WHIP peer + unchanged LiveKit SDK decoded media gate. */
import assert from 'node:assert/strict';import {createRequire} from 'node:module';import {createServer} from 'node:http';import {readFile,writeFile} from 'node:fs/promises';import {randomBytes} from 'node:crypto';import {dirname,join} from 'node:path';import {fileURLToPath} from 'node:url';
const here=dirname(fileURLToPath(import.meta.url)),require=createRequire(join(here,'../package.json'));
const{chromium}=require('playwright'),{AccessToken,RoomServiceClient}=require('livekit-server-sdk');
const url=process.env.LIVEKIT_URL||'ws://127.0.0.1:7880',key=process.env.LIVEKIT_API_KEY||'devkey',secret=process.env.LIVEKIT_API_SECRET||'secret';
const roomName=`native-whip-${randomBytes(5).toString('hex')}`,identity='native-whip-publisher',observer='native-whip-observer';
const api=new RoomServiceClient(url.replace(/^ws/,'http'),key,secret);const sdk=await readFile(require.resolve('livekit-client')),html=await readFile(join(here,'client.html')),native=await readFile(join(here,'native.js'));
const evidence={status:'RUNNING',scope:'Native WHIP POST+actual candidate PATCH+samePC ICE restart+generation-safe DELETE, no data channel, decoded440Hz into unchangedSDK and880Hz back; not external ingress substitute.',steps:[]};let host,browser,observation,peer,roomCreated=false;const cleanup=[];
const sleep=ms=>new Promise(r=>setTimeout(r,ms));async function until(check,label,timeout=20000){const end=Date.now()+timeout;while(Date.now()<end){if(await check())return;await sleep(50);}throw Error(label);}
async function jwt(id){const token=new AccessToken(key,secret,{identity:id});token.addGrant({roomJoin:true,room:roomName,canPublish:true,canSubscribe:true});return token.toJwt();}
try{
 await api.createRoom({name:roomName,emptyTimeout:30});roomCreated=true;
 host=createServer((req,res)=>{const value=req.url==='/sdk.js'?sdk:req.url==='/native.js'?native:req.url==='/'?html:null;if(!value){res.writeHead(404);res.end();return;}res.writeHead(200,{'content-type':req.url==='/'?'text/html':'text/javascript'});res.end(value);});await new Promise(r=>host.listen(0,'127.0.0.1',r));
 browser=await chromium.launch({channel:process.env.BROWSER_CHANNEL||'chrome',headless:true,args:['--no-sandbox','--autoplay-policy=no-user-gesture-required']});
 observation=await browser.newPage();await observation.goto(`http://127.0.0.1:${host.address().port}`);await observation.evaluate(args=>fixture.connect(args),{url,token:await jwt(observer),sipIdentity:identity});
 peer=await browser.newPage();await peer.goto(`http://127.0.0.1:${host.address().port}`);await peer.addScriptTag({url:'/native.js'});
 const created=await peer.evaluate(args=>nativeWhip.start(args),{url,token:await jwt(identity),observerIdentity:observer});assert.deepEqual(created,{postStatus:201,patchStatus:204,locationIsParticipant:true,etagPresent:true});evidence.steps.push('real-native-offer-answer-and-real-candidate-trickle');
 await until(async()=>{const a=await observation.evaluate(()=>fixture.snapshot()),b=await peer.evaluate(()=>nativeWhip.snapshot());return a.tone440?.rms>.02&&a.tone440?.toneAmplitude>.025&&b.connected&&b.receiverPackets>=75&&b.totalSamplesReceived>=8000&&b.tone880?.rms>.02&&b.tone880?.toneAmplitude>.025&&b.tone880?.toneAmplitude>b.tone880?.reference440Amplitude*5;},'actual bidirectional native WHIP decoded440/880 audio absent');
 const participants=await api.listParticipants(roomName),before=participants.find(p=>p.identity===identity);assert.ok(before);assert.ok(before.tracks.length>0);const sid=before.sid,tracks=before.tracks.map(t=>t.sid).sort();
 evidence.beforeRestart={native:await peer.evaluate(()=>nativeWhip.snapshot()),sdk:await observation.evaluate(()=>fixture.snapshot())};
 assert.deepEqual(evidence.beforeRestart.native.errors,[]);assert.deepEqual(evidence.beforeRestart.sdk.errors,[]);
 const restart=await peer.evaluate(()=>nativeWhip.restart());assert.deepEqual(restart,{restartStatus:200,etagChanged:true,oldTagStatus:500});
 await until(async()=>{const after=await peer.evaluate(()=>nativeWhip.snapshot());return after.connected&&after.totalSamplesReceived>evidence.beforeRestart.native.totalSamplesReceived+8000&&after.tone880?.rms>.02&&after.tone880?.toneAmplitude>.025;},'actual post-restart decoded media did not resume');
 const after=(await api.listParticipants(roomName)).find(p=>p.identity===identity);assert.equal(after.sid,sid);assert.deepEqual(after.tracks.map(t=>t.sid).sort(),tracks);evidence.restart={...restart,sameParticipant:true,samePublishedTracks:true,native:await peer.evaluate(()=>nativeWhip.snapshot()),sdk:await observation.evaluate(()=>fixture.snapshot())};evidence.steps.push('same-real-PC-native-ICE-restart-new-etag-real-audio-progress');
 assert.equal(await peer.evaluate(()=>nativeWhip.remove()),200);
 await until(async()=>!(await api.listParticipants(roomName)).some(p=>p.identity===identity),'real DELETE did not remove native RTC participant');assert.ok((await api.listParticipants(roomName)).some(p=>p.identity===observer));evidence.steps.push('actual-native-DELETE-roomleave-observer-retained');evidence.status='PASS';
}catch(error){evidence.status='FAIL';evidence.failureSnapshots={};
 try{if(peer)evidence.failureSnapshots.native=await peer.evaluate(()=>nativeWhip.snapshot());}catch{evidence.failureSnapshots.nativeUnavailable=true;}
 try{if(observation)evidence.failureSnapshots.sdk=await observation.evaluate(()=>fixture.snapshot());}catch{evidence.failureSnapshots.sdkUnavailable=true;}
 try{evidence.failureSnapshots.participants=(await api.listParticipants(roomName)).map(p=>({role:p.identity===identity?'native':p.identity===observer?'observer':'other',state:p.state,kind:p.kind,tracks:p.tracks.map(t=>({kind:t.type,mimeType:t.mimeType,muted:t.muted,source:t.source}))}));}catch{evidence.failureSnapshots.apiUnavailable=true;}
 evidence.failure={name:error.name,message:String(error.message).replaceAll(secret,'<secret>').replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<token>')};process.exitCode=1;}
finally{
 try{await peer?.evaluate(()=>nativeWhip.close());}catch{cleanup.push('native-client-close');}try{await observation?.evaluate(()=>fixture.close());}catch{cleanup.push('sdk-close');}try{await browser?.close();}catch{cleanup.push('owned-browser-close');}if(roomCreated)try{await api.deleteRoom(roomName);}catch{cleanup.push('owned-room-delete');}if(host)await new Promise(r=>host.close(r));if(cleanup.length){evidence.cleanupFailures=cleanup;evidence.status='FAIL';process.exitCode=1;}
 await writeFile(process.env.WHIP_MEDIA_OUTPUT||join(here,'actual-native-whip.json'),JSON.stringify(evidence,null,2)+'\n');
}
