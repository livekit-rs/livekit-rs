/** Real authenticated local WebSocket Blob protocol actors, with NO SDK decoder.
 * No SDP/RTC/media claims. All requests wait for the actual server Join response.
 */
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import {AccessToken} from 'livekit-server-sdk';
import {field,integer,readResponse,comparePayload} from './raw-protocol.mjs';
const TIMEOUT=10000,MAX_MESSAGES=2048;
function sanitize(error){return String(error).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<token>').replace(/([?&](?:access_token|token|api_secret)=)[^&\s]+/gi,'$1<redacted>');}
export class RawBlobPeer {
 constructor({url,token,identity,diagnostics}){
  this.identity=identity;this.diagnostics=diagnostics;this.pending=new Map();this.messages=0;this.closing=false;this.joined=false;this.failure=undefined;this.requestCount=0;
  const endpoint=new URL(url);endpoint.pathname=endpoint.pathname.replace(/\/$/,'')+'/rtc';endpoint.search='';
  endpoint.searchParams.set('access_token',token);endpoint.searchParams.set('protocol','17');endpoint.searchParams.set('sdk','unknown');endpoint.searchParams.set('version','f40-raw-protocol');endpoint.searchParams.set('auto_subscribe','0');endpoint.searchParams.set('auto_subscribe_data_track','0');
  // Node's built-in WebSocket is a real network transport; no additional package,
  // SDK request writer, decoder replacement or synthetic server is involved.
  assert.equal(typeof WebSocket,'function','Node runtime must provide its real WebSocket');
  this.socket=new WebSocket(endpoint);this.socket.binaryType='arraybuffer';
  this.ready=new Promise((resolve,reject)=>{this.joinResolve=resolve;this.joinReject=reject;});
  this.joinTimer=setTimeout(()=>this.fail(new Error('actual raw WebSocket Join timeout')),TIMEOUT);
  this.closed=new Promise(resolve=>{this.closeResolve=resolve;});
  this.socket.addEventListener('message',event=>{
   try {
    assert.ok(event.data instanceof ArrayBuffer,'raw peer requires actual binary protobuf response');
    assert.ok(++this.messages<=MAX_MESSAGES,'raw peer message resource bound');
    const response=readResponse(Buffer.from(event.data));
    if(response.type==='join'){
     assert.ok(!this.joined,'unexpected duplicate Join');
     const participant=response.tree.values.participant?.values;
     assert.ok(participant?.sid,'actual server participant SID required');assert.equal(participant.identity,this.identity,'Join must authenticate the requested actor');
     this.joined=true;clearTimeout(this.joinTimer);this.joinResolve();
     this.ping=setInterval(()=>{if(this.socket.readyState===WebSocket.OPEN)this.socket.send(field(16,integer(1,BigInt(Date.now()))));},5000);this.ping.unref?.();return;
    }
    if(response.type==='leave')throw new Error('actual raw peer received server Leave');
    if(['storeDataBlobResponse','getDataBlobResponse','requestResponse'].includes(response.type)){
     const resolve=this.pending.get(response.requestId);
     assert.ok(resolve,'uncorrelated/duplicate actual raw response');
     this.pending.delete(response.requestId);resolve.resolve(response);this.requestCount++;
     this.diagnostics.push({event:'response',type:response.type,requestId:response.requestId});
    }
   }catch(error){this.fail(error);}
  });
  this.socket.addEventListener('error',()=>this.fail(new Error('actual raw WebSocket transport error')));
  this.socket.addEventListener('close',event=>{
   clearTimeout(this.joinTimer);clearInterval(this.ping);this.closeResolve({code:event.code,clean:event.wasClean});
   if(!this.closing)this.fail(new Error('actual raw WebSocket closed unexpectedly'));
  });
 }
 fail(error){
  if(!this.failure){this.failure=new Error(sanitize(error));this.diagnostics.push({event:'error',error:sanitize(error)});}
  clearTimeout(this.joinTimer);clearInterval(this.ping);this.joinReject(this.failure);
  for(const operation of this.pending.values())operation.reject(this.failure);
  this.pending.clear();
 }
 async join(){await this.ready;if(this.failure)throw this.failure;assert.ok(this.joined);}
 async request(id,kind,body){
  await this.join();assert.equal(this.socket.readyState,WebSocket.OPEN);assert.ok(!this.pending.has(id));
  let timer;const response=new Promise((resolve,reject)=>{this.pending.set(id,{resolve,reject});timer=setTimeout(()=>{this.pending.delete(id);reject(new Error('actual raw Blob response timeout'));},TIMEOUT);});
  this.diagnostics.push({event:'request',kind,requestId:id});
  try{this.socket.send(field(kind,Buffer.concat([integer(1,id),body])));return await response;}
  finally{clearTimeout(timer);this.pending.delete(id);}
 }
 async close(){
  this.closing=true;clearTimeout(this.joinTimer);clearInterval(this.ping);
  if(this.socket.readyState!==WebSocket.CLOSED)this.socket.close(1000,'fixture complete');
  let timer;try {return await Promise.race([this.closed,new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error('owned raw WebSocket close timeout')),TIMEOUT);})]);}finally{clearTimeout(timer);}
 }
}

export async function runRawBlobCases({url,apiKey,secret,room,result}){
 assert.ok(process.env.BLOB_GO_CASES&&process.env.BLOB_GO_RESULTS,'actual Go component oracle files required');
 const cases=JSON.parse(await readFile(process.env.BLOB_GO_CASES,'utf8'));
 const go=(await readFile(process.env.BLOB_GO_RESULTS,'utf8')).split('\n').filter(Boolean).map(s=>JSON.parse(s));
 assert.equal(cases.length,26);assert.equal(go.length,26);
 const report={scope:'authenticated raw WebSocket binary conformance; no SDK Blob API or RTC/media claim',status:'RUNNING',node:process.version,cases:[],cleanupErrors:[],requestIdBoundaryObserved:[]};
 result.rawProtocol=report;let nextId=100,valid=0,invalid=0;
 for(const [index,c] of cases.entries()){
  const expected=go[index];assert.equal(c.id,expected.id);const events=[];
  const row={index,id:c.id,status:'RUNNING',events,cleanup:[]};report.cases.push(row);
  let writer,reader,error;
  try {
   const nonce=randomUUID();
   async function connect(role){const identity=`blob-wire-${role}-${nonce}`;const token=new AccessToken(apiKey,secret,{identity});token.addGrant({roomJoin:true,room,canPublish:false,canSubscribe:false,canPublishData:false});const peer=new RawBlobPeer({url,token:await token.toJwt(),identity,diagnostics:events});if(role==='writer')writer=peer;else reader=peer;await peer.join();return peer;}
   await connect('writer');await connect('reader');
   row.authenticatedJoinCount=2;
   const key=Buffer.from(c.left,'base64');
   const blob=Buffer.concat([field(1,key),field(2,Buffer.from([0,255,1])),Buffer.from(c.blob_unknown,'base64')]);
   const storeId=nextId++,store=await writer.request(storeId,22,field(2,blob));
   if(storeId===127||storeId===128)report.requestIdBoundaryObserved.push(storeId);
   if(expected.left_valid){
    comparePayload(store,readResponse(Buffer.from(expected.store_response,'base64')),storeId);
    const getId=nextId++,got=await reader.request(getId,23,Buffer.concat([field(2,Buffer.from(writer.identity)),field(3,Buffer.from(c.right,'base64'))]));
    if(getId===127||getId===128)report.requestIdBoundaryObserved.push(getId);
    if(expected.get)comparePayload(got,readResponse(Buffer.from(expected.get_response,'base64')),getId);
    else {assert.equal(got.type,'requestResponse');assert.equal(got.requestId,getId);assert.equal(got.tree.values.reason,1);assert.equal(got.tree.values.message,'data blob not found');assert.deepEqual(got.unknown,[]);assert.deepEqual(got.tree.unknown,[]);}
    valid++;
   } else {
    assert.equal(store.type,'requestResponse');assert.equal(store.requestId,storeId);assert.equal(store.tree.values.reason,11);assert.equal(store.tree.values.message,expected.left_invalid_message);assert.deepEqual(store.unknown,[]);assert.deepEqual(store.tree.unknown,[]);
    const getId=nextId++,got=await reader.request(getId,23,Buffer.concat([field(2,Buffer.from(writer.identity)),field(3,key)]));
    if(getId===127||getId===128)report.requestIdBoundaryObserved.push(getId);
    assert.equal(got.type,'requestResponse');assert.equal(got.requestId,getId);assert.equal(got.tree.values.reason,1);assert.equal(got.tree.values.message,'data blob not found','rejected Store must not create participant Blob');assert.deepEqual(got.unknown,[]);assert.deepEqual(got.tree.unknown,[]);
    invalid++;
   }
   assert.ok(!writer.failure&&!reader.failure,'no raw peer decode/transport errors');row.status='PASS';
  } catch(cause){error=cause;row.status='FAIL';row.error=sanitize(cause);}
  finally {
   for(const [role,peer] of [['writer',writer],['reader',reader]]){
    if(!peer)continue;
    try {const closed=await peer.close();assert.equal(closed.code,1000,'owned raw peer close handshake');assert.equal(closed.clean,true,'owned raw peer clean close');row.cleanup.push({role,closed:true,code:closed.code,clean:closed.clean});}
    catch(cause){row.status='FAIL';report.cleanupErrors.push({index,role,error:sanitize(cause)});error??=cause;}
   }
  }
  if(error){report.status='FAIL';throw error;}
 }
 assert.equal(valid+invalid,26);assert.ok(valid>=20&&invalid>0);assert.deepEqual(report.requestIdBoundaryObserved,[127,128]);
 report.status='PASS';report.validStoreCases=valid;report.invalidStoreCases=invalid;
 result.rawKeyOracle={total:26,validStoreCases:valid,sourceInvalidStoreCases:invalid,all26ActualAuthenticatedStoreRequests:true,all26ActualSourcePayloadComparisons:true,transport:'independent-authenticated-raw-WebSocket'};
}
