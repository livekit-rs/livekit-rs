/** Additional actual lifecycle leaf. No simulated reconnect or SDK patch. */
import assert from 'node:assert/strict';
import {randomUUID,createHash} from 'node:crypto';
import {AccessToken} from 'livekit-server-sdk';
import {SignalResponse,DisconnectReason,RequestResponse_Reason} from '@livekit/protocol';
export async function exerciseBlobResumeReplacement({context,hostUrl,url,apiKey,secret,roomName,single,api,readerRequest}){
 const identity=`blob-resume-${randomUUID()}`,key={key:{case:'generic',value:'actual-resume-key'}};
 const original=Uint8Array.from([0,255,3,7]),replacement=Uint8Array.from([9,0,8]);
 const report={status:'RUNNING',scope:'actual SDK signal-only resume retains participant blob; real duplicate identity replaces participant and frees old blob; no public BlobAPI',single,cleanupErrors:[]};
 const owned=[],routes=[];let routeError;let next=2000000;const pending=new Map();const responses={reconnect:0,join:0};
 const safe=error=>String(error).replaceAll(secret,'<secret>').replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<token>');
 const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
 async function wait(check,label){const until=Date.now()+10000;while(Date.now()<until){if(routeError)throw routeError;const value=await check();if(value)return value;await sleep(50);}throw Error(label);}
 async function token(media){const value=new AccessToken(apiKey,secret,{identity});value.addGrant({roomJoin:true,room:roomName,canPublish:media,canSubscribe:false,canPublishData:false});return value.toJwt();}
 function observe(page,ordinal){page.on('websocket',socket=>socket.on('framereceived',frame=>{try{const response=typeof frame.payload==='string'?SignalResponse.fromJsonString(frame.payload):SignalResponse.fromBinary(frame.payload);if(ordinal===0&&['reconnect','join'].includes(response.message.case))responses[response.message.case]++;if(['storeDataBlobResponse','getDataBlobResponse','requestResponse'].includes(response.message.case)){const value=response.message.value;pending.get(`${ordinal}:${value.requestId}`)?.({type:response.message.case,value});}}catch(error){routeError=Error(safe(error));}}));}
 async function send(page,ordinal,type,value){const requestId=next++,id=`${ordinal}:${requestId}`;let timer;const result=new Promise((resolve,reject)=>{pending.set(id,resolve);timer=setTimeout(()=>reject(Error('actual lifecycle Blob response timeout')),10000);});try{await page.evaluate(message=>blobFixture.signal(message),{case:type,value:{...value,requestId}});return await result;}finally{clearTimeout(timer);pending.delete(id);}}
 try{
  const publisher=await context.newPage();owned.push(publisher);observe(publisher,0);
  await publisher.routeWebSocket(/\/rtc(?:\/v1)?(?:\?|$)/,route=>{if(routes.length>=8){routeError=Error('bounded signal reconnect route overflow');void route.close({code:1011,reason:'fixture route bound'});return;}const server=route.connectToServer();routes.push({route,server});});
  await publisher.goto(hostUrl);await publisher.evaluate(args=>blobFixture.connect(args),{url,token:await token(true),single,lifecycleMedia:true});
  const before=await wait(async()=>{const state=await publisher.evaluate(()=>blobFixture.lifecycleSnapshot());return state.state==='connected'&&state.outboundPackets>=10?state:null;},'real connected publisher RTP required before signal-only interruption');
  report.initial={sidPresent:!!before.sid,trackCount:before.trackSids.length,outboundPackets:before.outboundPackets,joinFrames:responses.join,reconnectFrames:responses.reconnect,signalConnections:routes.length};
  assert.ok(before.sid);assert.equal(before.trackSids.length,1);assert.equal(before.eventOverflow,false);assert.equal(responses.join,1);assert.equal(responses.reconnect,0);
  const stored=await send(publisher,0,'storeDataBlobRequest',{blob:{key,contents:original}});assert.equal(stored.type,'storeDataBlobResponse');
  const got=await readerRequest('getDataBlobRequest',{participantIdentity:identity,key});assert.equal(got.type,'getDataBlobResponse');assert.deepEqual(Buffer.from(got.value.blob.contents),Buffer.from(original));
  assert.equal(routes.length,1);await Promise.all([routes[0].route.close({code:1011,reason:'fixture actual signal interruption'}),routes[0].server.close({code:1011,reason:'fixture actual signal interruption'})]);
  const resumed=await wait(async()=>{const state=await publisher.evaluate(()=>blobFixture.lifecycleSnapshot());return routes.length>=2&&responses.reconnect>=1&&state.events.includes('reconnected')&&state.state==='connected'?state:null;},'actual unchanged SDK resume response/event absent');
  assert.equal(resumed.eventOverflow,false);assert.equal(resumed.sid,before.sid,'full reconnect/new participant cannot substitute for resume');assert.equal(responses.join,1,'new JoinResponse is not resume');
  assert.deepEqual(resumed.trackSids,before.trackSids);assert.ok(resumed.outboundPackets>=before.outboundPackets);
  const retained=await readerRequest('getDataBlobRequest',{participantIdentity:identity,key});assert.equal(retained.type,'getDataBlobResponse');assert.deepEqual(Buffer.from(retained.value.blob.contents),Buffer.from(original));
  report.resume={sameParticipant:true,samePublishedTrackSids:true,realReconnectResponse:true,realSdkReconnectedEvent:true,signalConnections:routes.length,contentsSha256:createHash('sha256').update(original).digest('hex')};
  const fresh=await context.newPage();owned.push(fresh);observe(fresh,1);await fresh.goto(hostUrl);await fresh.evaluate(args=>blobFixture.connect(args),{url,token:await token(false),single});
  const newInfo=await wait(async()=>{const p=(await api.listParticipants(roomName)).find(p=>p.identity===identity);return p&&p.sid!==before.sid?p:null;},'duplicate identity did not create real new participant');
  const oldClosed=await wait(async()=>{const state=await publisher.evaluate(()=>blobFixture.lifecycleSnapshot());return state.state==='disconnected'?state:null;},'old SDK session did not close after real duplicate identity');
  assert.equal(oldClosed.disconnectedReason,DisconnectReason.DUPLICATE_IDENTITY);
  const absent=await readerRequest('getDataBlobRequest',{participantIdentity:identity,key});assert.equal(absent.type,'requestResponse');assert.equal(absent.value.reason,RequestResponse_Reason.NOT_FOUND);assert.equal(absent.value.message,'data blob not found');
  const freshStored=await send(fresh,1,'storeDataBlobRequest',{blob:{key,contents:replacement}});assert.equal(freshStored.type,'storeDataBlobResponse');
  await publisher.evaluate(()=>blobFixture.close());
  assert.equal((await api.listParticipants(roomName)).find(p=>p.identity===identity)?.sid,newInfo.sid,'old handle cleanup must not remove replacement');
  const freshGot=await readerRequest('getDataBlobRequest',{participantIdentity:identity,key});assert.equal(freshGot.type,'getDataBlobResponse');assert.deepEqual(Buffer.from(freshGot.value.blob.contents),Buffer.from(replacement));
  await fresh.evaluate(()=>blobFixture.close());await wait(async()=>!(await api.listParticipants(roomName)).some(p=>p.identity===identity),'owned replacement participant cleanup absent');
  report.replacement={actualNewParticipant:true,oldDuplicateIdentityClosed:true,oldBlobNotInherited:true,oldHandleCannotRemoveNew:true,freshBlobDelivered:true,trueLeaveRemovesParticipant:true};report.status='PASS';
 }catch(error){report.status='FAIL';report.error=safe(error);}
 finally{for(const page of owned){try{await page.evaluate(()=>blobFixture.close());}catch(error){report.cleanupErrors.push(safe(error));}try{await page.close();}catch(error){report.cleanupErrors.push(safe(error));}}if(report.cleanupErrors.length)report.status='FAIL';}
 if(report.status!=='PASS'){const error=Error(report.error||'owned lifecycle cleanup failed');error.lifecycleReport=report;throw error;}return report;
}
