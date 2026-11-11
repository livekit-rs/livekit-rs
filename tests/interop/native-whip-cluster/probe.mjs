import {closeOwnedBrowser,observeChildStderr} from '../owned-browser-exit.mjs';
/** NOT_RUN. Real native WHIP peer + unchanged LiveKit SDK decoded media gate. */
import assert from 'node:assert/strict';import {execFileSync} from 'node:child_process';import {createRequire} from 'node:module';import {createServer} from 'node:http';import {readFile,writeFile} from 'node:fs/promises';import {randomBytes} from 'node:crypto';import {dirname,join} from 'node:path';import {fileURLToPath} from 'node:url';
const here=dirname(fileURLToPath(import.meta.url)),require=createRequire(join(here,'../package.json'));
const{chromium}=require('playwright'),{AccessToken,RoomServiceClient}=require('livekit-server-sdk');
const url=process.env.LIVEKIT_URL||'ws://127.0.0.1:7880',key=process.env.LIVEKIT_API_KEY||'devkey',secret=process.env.LIVEKIT_API_SECRET||'secret';
const ownerUrl=process.env.WHIP_OWNER_URL,ownerNode=process.env.WHIP_OWNER_NODE_ID,ownerDebug=process.env.WHIP_OWNER_DEBUG_URL,frontDebug=process.env.WHIP_FRONT_DEBUG_URL;
assert.ok(ownerUrl&&ownerNode&&ownerDebug&&frontDebug,'explicit remote owner and both node debug listeners required');
const roomName=`native-whip-${randomBytes(5).toString('hex')}`,identity='native-whip-publisher',observer='native-whip-observer';
const ownerApi=new RoomServiceClient(ownerUrl,key,secret);
const api=new RoomServiceClient(url.replace(/^ws/,'http'),key,secret);const sdk=await readFile(require.resolve('livekit-client')),html=await readFile(join(here,'../native-whip/client.html')),native=await readFile(join(here,'../native-whip/native.js'));
const evidence={status:'RUNNING',scope:'Cross-node remote owner: Native WHIP POST+actual candidate PATCH+samePC ICE restart+generation-safe DELETE, no data channel, decoded440Hz into unchangedSDK and880Hz back; not external ingress substitute.',steps:[]};let host,browser,browserServer,finishBrowserStderr,observation,peer,roomCreated=false;const cleanup=[];
for(const signal of ['SIGINT','SIGTERM'])process.once(signal,()=>{browserServer?.close().finally(()=>process.exit(1));});
const sleep=ms=>new Promise(r=>setTimeout(r,ms));async function until(check,label,timeout=20000){const end=Date.now()+timeout;while(Date.now()<end){if(await check())return;await sleep(50);}throw Error(label);}
async function jwt(id){const token=new AccessToken(key,secret,{identity:id});token.addGrant({roomJoin:true,room:roomName,canPublish:true,canSubscribe:true});return token.toJwt();}
async function debug(base){const r=await fetch(`${base}/debug/rooms`);assert.equal(r.status,200);return r.json();}
function clean(value){return String(value).replaceAll(secret,'<secret>').replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<token>');}
async function deleteOwnedRoom(){
 const observation={wire:[]};evidence.cleanupDelete=observation;
 const originalFetch=globalThis.fetch;
 // Observe the unchanged SDK's exact DeleteRoom response without replacing
 // its request, status/error decoding, retries or authentication.
 globalThis.fetch=async(input,init)=>{
  const response=await originalFetch(input,init);
  const requestUrl=new URL(typeof input==='string'||input instanceof URL?input:input.url);
  if(requestUrl.origin===new URL(url.replace(/^ws/,'http')).origin&&requestUrl.pathname==='/twirp/livekit.RoomService/DeleteRoom'){
   const body=await response.clone().text();observation.wire.push({status:response.status,body:clean(body.slice(0,8192)),truncated:body.length>8192});
  }
  return response;
 };
 let originalError;
 try{await api.deleteRoom(roomName);observation.outcome='deleted';}
 catch(error){originalError=error;observation.error={name:error.name,status:error.status,code:error.code,message:clean(error.message)};}
 finally{globalThis.fetch=originalFetch;}
 const wire=observation.wire.length===1?observation.wire[0]:null;let body;try{body=JSON.parse(wire?.body||'null');}catch{}
 // Every original failure remains visible. Only the exact original Go
 // RoomExists=false result may be classified as already absent.
 if(originalError&&(originalError.status!==404||originalError.code!=='not_found'||originalError.message!=='requested room does not exist'||wire?.status!==404||wire?.truncated||body?.code!=='not_found'||body?.msg!=='requested room does not exist'))throw originalError;
 try{
  observation.absenceAttempts=0;
  await until(async()=>{
   const frontRooms=await api.listRooms([roomName]),ownerRooms=await ownerApi.listRooms([roomName]);
   const frontLocal=await debug(process.env.WHIP_FRONT_DEBUG_URL),ownerLocal=await debug(process.env.WHIP_OWNER_DEBUG_URL);
   const redisPort=process.env.WHIP_REDIS_PORT;assert.match(redisPort||'',/^\d+$/,'private campaign Redis port is required');
   const mapped=execFileSync('redis-cli',['--raw','-h','127.0.0.1','-p',redisPort,'HEXISTS','room_node_map',roomName],{encoding:'utf8',timeout:5000}).trim();assert.match(mapped,/^[01]$/);
   observation.absenceAttempts++;
   observation.absence={frontListed:frontRooms.some(r=>r.name===roomName),ownerListed:ownerRooms.some(r=>r.name===roomName),frontLocal:frontLocal.some(r=>r.Name===roomName),ownerLocal:ownerLocal.some(r=>r.Name===roomName),redisMapped:mapped==='1'};
   return Object.values(observation.absence).every(value=>value===false);
  },'DeleteRoom must remove both catalogs/local owner maps and actual Redis route',20000);
  if(originalError)observation.outcome='already-absent-before-client-close';
 }catch(error){observation.confirmationError={name:error.name,message:clean(error.message)};throw error;}
}
try{
 await ownerApi.createRoom({name:roomName,emptyTimeout:30,nodeId:ownerNode});roomCreated=true;
 host=createServer((req,res)=>{const value=req.url==='/sdk.js'?sdk:req.url==='/native.js'?native:req.url==='/'?html:null;if(!value){res.writeHead(404);res.end();return;}res.writeHead(200,{'content-type':req.url==='/'?'text/html':'text/javascript'});res.end(value);});await new Promise(r=>host.listen(0,'127.0.0.1',r));
 browserServer=await chromium.launchServer({channel:process.env.BROWSER_CHANNEL||'chrome',headless:true,args:['--no-sandbox','--autoplay-policy=no-user-gesture-required']});
 finishBrowserStderr=observeChildStderr(browserServer.process());
 if(process.env.WHIP_BROWSER_OWNERSHIP)await writeFile(process.env.WHIP_BROWSER_OWNERSHIP,JSON.stringify({browserPid:browserServer.process().pid,nodePid:process.pid}));
 browser=await chromium.connect(browserServer.wsEndpoint());
 observation=await browser.newPage();await observation.goto(`http://127.0.0.1:${host.address().port}`);await observation.evaluate(args=>fixture.connect(args),{url,token:await jwt(observer),sipIdentity:identity});
 peer=await browser.newPage();await peer.goto(`http://127.0.0.1:${host.address().port}`);await peer.addScriptTag({url:'/native.js'});
 const created=await peer.evaluate(args=>nativeWhip.start(args),{url,token:await jwt(identity),observerIdentity:observer});assert.deepEqual(created,{postStatus:201,patchStatus:204,locationIsParticipant:true,etagPresent:true});evidence.steps.push('real-native-offer-answer-and-real-candidate-trickle');
 await until(async()=>{const a=await observation.evaluate(()=>fixture.snapshot()),b=await peer.evaluate(()=>nativeWhip.snapshot());return a.tone440?.rms>.02&&a.tone440?.toneAmplitude>.025&&b.connected&&b.receiverPackets>=75&&b.totalSamplesReceived>=8000&&b.tone880?.rms>.02&&b.tone880?.toneAmplitude>.025&&b.tone880?.toneAmplitude>b.tone880?.reference440Amplitude*5;},'actual bidirectional native WHIP decoded440/880 audio absent');
 const frontRooms=await debug(frontDebug),ownerRooms=await debug(ownerDebug);
 assert.ok(!frontRooms.some(r=>r.Name===roomName),'WHIP HTTP front must not create shadow room');
 const ownedRoom=ownerRooms.find(r=>r.Name===roomName);assert.ok(ownedRoom);assert.ok(ownedRoom.Participants[identity]);assert.ok(ownedRoom.Participants[observer]);
 evidence.ownerRouting={frontHasShadow:false,ownerParticipantSid:ownedRoom.Participants[identity].ID,ownerNode};
 const participants=await api.listParticipants(roomName),before=participants.find(p=>p.identity===identity);assert.ok(before);assert.ok(before.tracks.length>0);const sid=before.sid,tracks=before.tracks.map(t=>t.sid).sort();
 evidence.beforeRestart={native:await peer.evaluate(()=>nativeWhip.snapshot()),sdk:await observation.evaluate(()=>fixture.snapshot())};
 assert.deepEqual(evidence.beforeRestart.native.errors,[]);assert.deepEqual(evidence.beforeRestart.sdk.errors,[]);
 const restart=await peer.evaluate(()=>nativeWhip.restart());assert.deepEqual(restart,{restartStatus:200,etagChanged:true,oldTagStatus:500});
 await until(async()=>{const after=await peer.evaluate(()=>nativeWhip.snapshot());return after.connected&&after.totalSamplesReceived>evidence.beforeRestart.native.totalSamplesReceived+8000&&after.tone880?.rms>.02&&after.tone880?.toneAmplitude>.025;},'actual post-restart decoded media did not resume');
 const after=(await api.listParticipants(roomName)).find(p=>p.identity===identity);assert.equal(after.sid,sid);assert.deepEqual(after.tracks.map(t=>t.sid).sort(),tracks);evidence.restart={...restart,sameParticipant:true,samePublishedTracks:true,native:await peer.evaluate(()=>nativeWhip.snapshot()),sdk:await observation.evaluate(()=>fixture.snapshot())};evidence.steps.push('same-real-PC-native-ICE-restart-new-etag-real-audio-progress');
 assert.equal(await peer.evaluate(()=>nativeWhip.remove()),200);
 await until(async()=>!(await api.listParticipants(roomName)).some(p=>p.identity===identity),'real DELETE did not remove native RTC participant');assert.ok((await api.listParticipants(roomName)).some(p=>p.identity===observer));await until(async()=>{const room=(await debug(ownerDebug)).find(r=>r.Name===roomName);return room&&!room.Participants[identity]&&room.Participants[observer];},'owner generation not released after remote DELETE');assert.ok(!(await debug(frontDebug)).some(r=>r.Name===roomName));evidence.steps.push('actual-native-DELETE-roomleave-observer-retained');evidence.status='PASS';
}catch(error){evidence.status='FAIL';evidence.failureSnapshots={};
 try{if(peer)evidence.failureSnapshots.native=await peer.evaluate(()=>nativeWhip.snapshot());}catch{evidence.failureSnapshots.nativeUnavailable=true;}
 try{if(observation)evidence.failureSnapshots.sdk=await observation.evaluate(()=>fixture.snapshot());}catch{evidence.failureSnapshots.sdkUnavailable=true;}
 try{evidence.failureSnapshots.participants=(await api.listParticipants(roomName)).map(p=>({role:p.identity===identity?'native':p.identity===observer?'observer':'other',state:p.state,kind:p.kind,tracks:p.tracks.map(t=>({kind:t.type,mimeType:t.mimeType,muted:t.muted,source:t.source}))}));}catch{evidence.failureSnapshots.apiUnavailable=true;}
 evidence.failure={name:error.name,message:String(error.message).replaceAll(secret,'<secret>').replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<token>')};process.exitCode=1;}
finally{
 // Explicit room teardown happens while the SDK observer is still connected;
 // never race its departure timer with a later public DeleteRoom.
 evidence.cleanupRoomDeletePhase='before-sdk-and-browser-close';
 if(roomCreated)try{await deleteOwnedRoom();}catch{cleanup.push('owned-room-delete');}
 try{await peer?.evaluate(()=>nativeWhip.close());}catch{cleanup.push('native-client-close');}try{await observation?.evaluate(()=>fixture.close());}catch{cleanup.push('sdk-close');}{const closed=await closeOwnedBrowser(browser,browserServer);evidence.browserExit=closed.exit;evidence.browserCleanup=closed;cleanup.push(...closed.failures);if(finishBrowserStderr){const stderr=finishBrowserStderr();evidence.browserStderr=stderr.metadata;try{await writeFile(process.env.WHIP_BROWSER_STDERR,stderr.bytes,{flag:'wx'});}catch{cleanup.push('owned-browser-stderr-persist');}}}if(host)await new Promise(r=>host.close(r));if(cleanup.length){evidence.cleanupFailures=cleanup;evidence.status='FAIL';process.exitCode=1;}
 await writeFile(process.env.WHIP_MEDIA_OUTPUT||join(here,'actual-native-whip.json'),JSON.stringify(evidence,null,2)+'\n');
}
