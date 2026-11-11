/** Root-run authenticated RFC7692 RTC comparison against fixed Go. No mock server.
 * Bounded raw RFC6455 frames, fresh dictionaries and distinct wire/decoded caps.
 * Official unmodified server SDK signs JWTs; official protocol encodes/decodes.
 */
import assert from 'node:assert/strict';
import net from 'node:net';
import {deflateRawSync,inflateRawSync,constants as zlib} from 'node:zlib';
import {randomBytes,randomUUID,createHash} from 'node:crypto';
import {writeFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {resolve,isAbsolute} from 'node:path';
assert.ok(process.env.WS_CAP_INTEROP_ROOT&&isAbsolute(process.env.WS_CAP_INTEROP_ROOT),'absolute existing locked tests/interop root required');
const require=createRequire(resolve(process.env.WS_CAP_INTEROP_ROOT,'package.json'));
const {AccessToken,RoomServiceClient}=require('livekit-server-sdk');
const {SignalRequest,SignalResponse,Ping}=require('@livekit/protocol');
const MiB=1<<20, RESPONSE_MAX=2*MiB, RECEIVE_BUFFER_MAX=3*MiB, ROW_MAX=29;
const limit=Number(process.env.WS_CAP_LIMIT);
assert.ok(Number.isSafeInteger(limit)&&[-1,0,1024].includes(limit),'select actualconfigured -1/0/1024 profile');
const apiKey=process.env.LIVEKIT_API_KEY||'devkey',secret=process.env.LIVEKIT_API_SECRET||'secret';
const targets={official:process.env.WS_CAP_OFFICIAL_URL,rust:process.env.WS_CAP_RUST_URL};
for(const value of Object.values(targets)){const u=new URL(value);assert.equal(u.protocol,'ws:','this bounded local profile uses actual ws, not TLS bypass');}
const safe=e=>String(e).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<token>').slice(0,1024);
const hash=b=>createHash('sha256').update(b).digest('hex');
function varint(n){n=BigInt(n);const bytes=[];while(n>=128n){bytes.push(Number(n&127n)|128);n>>=7n;}bytes.push(Number(n));return Buffer.from(bytes);}
function unknownMessage(total){
 const tag=varint((50000n<<3n)|2n);let count=total-tag.length-1;
 for(let i=0;i<5;i++){const prefix=varint(count);const next=total-tag.length-prefix.length;if(next===count){return Buffer.concat([tag,prefix,Buffer.alloc(count)]);}count=next;}
 throw new Error('unknown message exactlength did not converge');
}
function compressed(bytes){const wire=deflateRawSync(bytes,{finishFlush:zlib.Z_SYNC_FLUSH});assert.ok(wire.subarray(-4).equals(Buffer.from([0,0,255,255])));return wire.subarray(0,-4);}
function frameHeader(opcode,length,mask,fin,rsv1=false,extendedLength){const small=extendedLength??(length<126?length:length<=65535?126:127);const h=Buffer.alloc(2+(small===126?2:small===127?8:0)+4);h[0]=(fin?128:0)|(rsv1?64:0)|opcode;h[1]=128|small;let at=2;if(small===126){h.writeUInt16BE(length,at);at+=2;}if(small===127){h.writeBigUInt64BE(BigInt(length),at);at+=8;}mask.copy(h,at);return h;}
class Peer {
 constructor(url,offer=true){this.url=url;this.offer=offer;this.compressedFrames=0;this.fragmentCompressed=false;this.buffer=Buffer.alloc(0);this.frames=[];this.waiters=[];this.serial=Promise.resolve();this.closeFrame=null;this.closeAckSent=false;this.receivedClose=false;this.sentClose=false;this.ended=false;this.forcedDestroy=false;this.error=null;this.frameCount=0;this.upgraded=false;this.fragments=[];this.fragmentBytes=0;this.fragmentOpcode=0;}
 async connect(){
  const u=new URL(this.url);const key=randomBytes(16).toString('base64');this.expectedAccept=createHash('sha1').update(key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  this.socket=net.createConnection({host:u.hostname,port:Number(u.port||80)});
  this.socket.on('data',b=>{try{this.receive(b);}catch(e){this.error=safe(e);this.socket.destroy();this.wake();}});
  this.socket.on('error',e=>{this.error=e.code||safe(e);this.wake();});
  this.socket.on('close',()=>{this.ended=true;this.wake();});
  await new Promise((resolve,reject)=>{this.socket.once('connect',resolve);this.socket.once('error',reject);});
  await this.write(Buffer.from(`GET ${u.pathname}${u.search} HTTP/1.1\r\nHost: ${u.host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: ${key}\r\n${this.offer?'Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits\r\n':''}\r\n`));
  await this.until(()=>this.upgraded,'actual HTTP101 upgrade');
 }
 write(bytes){return new Promise((resolve,reject)=>{if(this.socket.destroyed){reject(new Error('actual socket alreadyclosed'));return;}this.socket.write(bytes,e=>e?reject(e):resolve());});}
 send(opcode,bytes,fin=true,rsv1=false,extraReservedBits=0,extendedLength){
  const run=async()=>{const mask=randomBytes(4),payload=Buffer.allocUnsafe(bytes.length);for(let i=0;i<bytes.length;i++)payload[i]=bytes[i]^mask[i&3];const header=frameHeader(opcode,payload.length,mask,fin,rsv1,extendedLength);header[0]|=extraReservedBits;await this.write(header);await this.write(payload);if(opcode===8)this.sentClose=true;};
  const pending=this.serial.then(run);this.serial=pending.catch(()=>{});return pending;
 }
 async message(bytes,fragment,useCompression=this.offer){
  if(useCompression)bytes=compressed(bytes);
  if(!fragment){await this.send(2,bytes,true,useCompression);return;}
  for(let i=0;i<bytes.length;i+=fragment){await this.send(i===0?2:0,bytes.subarray(i,Math.min(i+fragment,bytes.length)),i+fragment>=bytes.length,useCompression&&i===0);}
 }
 wake(){const rows=this.waiters.splice(0);for(const resolve of rows)resolve();}
 async until(predicate,label,timeout=20000){const deadline=Date.now()+timeout;while(!predicate()){
  if(this.error||this.ended)throw new Error(`${label}: transportended ${this.error||'EOF'}`);
  const left=deadline-Date.now();if(left<=0)throw new Error(`${label}: deadline`);
  let timer;await new Promise(resolve=>{const done=()=>{clearTimeout(timer);resolve();};this.waiters.push(done);timer=setTimeout(()=>{const i=this.waiters.indexOf(done);if(i>=0)this.waiters.splice(i,1);resolve();},left);});
 }}
 async next(){await this.until(()=>this.frames.length>0,'actual nextframe');return this.frames.shift();}
 receive(chunk){
  assert.ok(this.buffer.length+chunk.length<=RECEIVE_BUFFER_MAX,'explicit receiver overflow, not silent truncation');this.buffer=Buffer.concat([this.buffer,chunk]);
  if(!this.upgraded){const end=this.buffer.indexOf('\r\n\r\n');if(end<0){assert.ok(this.buffer.length<=65536,'HTTPheader bound');return;}const header=this.buffer.subarray(0,end).toString('latin1');assert.match(header,/^HTTP\/1\.[01] 101 /);const fields=Object.fromEntries(header.split('\r\n').slice(1).map(l=>{const at=l.indexOf(':');return[l.slice(0,at).toLowerCase(),l.slice(at+1).trim()];}));assert.equal(fields['sec-websocket-accept'],this.expectedAccept);if(this.offer)assert.equal(fields['sec-websocket-extensions'],'permessage-deflate; server_no_context_takeover; client_no_context_takeover');else assert.equal(fields['sec-websocket-extensions'],undefined);this.buffer=this.buffer.subarray(end+4);this.upgraded=true;this.wake();}
  while(this.buffer.length>=2){const a=this.buffer[0],b=this.buffer[1];assert.equal(a&48,0);const compressedFrame=!!(a&64);if(compressedFrame){assert.ok(this.offer);this.compressedFrames++;}assert.equal(b&128,0,'serverframes unmasked');const opcode=a&15,fin=!!(a&128);let n=b&127,at=2;if(n===126){if(this.buffer.length<4)return;n=this.buffer.readUInt16BE(2);at=4;}if(n===127){if(this.buffer.length<10)return;const size=this.buffer.readBigUInt64BE(2);assert.ok(size<=BigInt(RESPONSE_MAX));n=Number(size);at=10;}assert.ok(n<=RESPONSE_MAX);if(this.buffer.length<at+n)return;const body=Buffer.from(this.buffer.subarray(at,at+n));this.buffer=this.buffer.subarray(at+n);this.frameCount++;assert.ok(this.frameCount<=256,'serverframe count bound');
   if(opcode===9){assert.ok(!compressedFrame&&fin&&n<=125);this.send(10,body).catch(e=>{this.error=safe(e);this.wake();});continue;}
   if(opcode===8){assert.ok(!compressedFrame&&fin&&n<=125&&n!==1);this.receivedClose=true;this.closeFrame={code:n>=2?body.readUInt16BE(0):1005,reason:n>=2?new TextDecoder('utf8',{fatal:true}).decode(body.subarray(2)):''};this.serial.then(async()=>{if(!this.sentClose)await this.send(8,body);this.closeAckSent=true;this.wake();}).catch(()=>this.wake());this.wake();continue;}
   if(opcode===0){assert.ok(!compressedFrame&&this.fragmentOpcode!==0);this.fragments.push(body);this.fragmentBytes+=n;assert.ok(this.fragmentBytes<=RESPONSE_MAX);if(!fin)continue;this.push(this.fragmentOpcode,Buffer.concat(this.fragments),this.fragmentCompressed);this.fragments=[];this.fragmentBytes=0;this.fragmentOpcode=0;continue;}
   if(!fin){assert.ok(opcode===1||opcode===2);assert.equal(this.fragmentOpcode,0);this.fragmentOpcode=opcode;this.fragmentCompressed=compressedFrame;this.fragments=[body];this.fragmentBytes=n;continue;}
   this.push(opcode,body,compressedFrame);
  }
 }
 push(opcode,body,deflated=false){if(deflated)body=inflateRawSync(Buffer.concat([body,Buffer.from([0,0,255,255,1,0,0,255,255])]),{maxOutputLength:RESPONSE_MAX});assert.ok(this.frames.length<64,'explicit framequeue overflow');this.frames.push({opcode,body});this.wake();}
 async pong(marker){let control=false,signal=false;for(let i=0;i<32;i++){const frame=await this.next();if(frame.opcode===10&&frame.body.equals(Buffer.from(`cap-${marker}`)))control=true;if(frame.opcode===2){const response=SignalResponse.fromBinary(frame.body);if(response.message.case==='pongResp'&&Number(response.message.value.lastPingTimestamp)===marker)signal=true;}if(control&&signal)return true;}throw new Error('correlated controlPong and actual next SignalPong both required');}
 summary(){return{receivedClose:this.receivedClose,close:this.closeFrame,clean:this.receivedClose&&this.closeAckSent&&this.ended&&!this.error,transportError:this.error,serverEnded:this.ended,forcedDestroy:this.forcedDestroy};}
 async finish(){if(!this.socket)return;if(!this.receivedClose&&!this.ended){const body=Buffer.alloc(2);body.writeUInt16BE(1000);await this.send(8,body).catch(()=>{});}const stop=Date.now()+5000;while(!this.ended&&Date.now()<stop){await new Promise(r=>setTimeout(r,20));}if(!this.ended){this.forcedDestroy=true;this.socket.destroy();await new Promise(r=>this.socket.once('close',r));} }
}
const vectors=[
 {id:'close-empty',bytes:0,closePayload:[],accept:false,close:1005},
 {id:'close-one-byte',bytes:0,closePayload:[0x03],accept:false,close:1005},
 {id:'close-invalid-status',bytes:0,closePayload:[0x07,0xd0],accept:false,close:1002,reason:'bad close code 2000'},
 {id:'close-invalid-status-and-utf8',bytes:0,closePayload:[0x03,0xf6,0xff],accept:false,close:1002,reason:'bad close code 1014'},
 {id:'close-invalid-utf8',bytes:0,closePayload:[0x03,0xe8,0xff],accept:false,close:1002,reason:'invalid utf8 payload in close frame'},
 {id:'close-library-status',bytes:0,closePayload:[0x0f,0x9f,0x61],accept:false,close:3999},
 {id:'close-normal-with-reason',bytes:0,closePayload:[0x03,0xe8,0x61],accept:false,close:1000},
 {id:'compressed-valid',bytes:256,accept:true},
 {id:'compressed-byte-fragments',bytes:256,fragment:1,accept:true},
 {id:'fresh-context-second-message',bytes:256,repeat:true,accept:true},
 {id:'decoded-overflow',bytes:2048,accept:limit<=0,close:1000},
 {id:'wire-overflow',bytes:2048,noise:true,accept:limit<=0,close:1009},
 {id:'invalid-deflate-block',bytes:0,invalid:true,accept:false,close:1000},
 {id:'no-offer-plain',bytes:256,offer:false,accept:true},
 {id:'negotiated-rsv2',bytes:256,reserved:32,accept:false,close:1002,reason:'RSV2 set'},
 {id:'unnegotiated-rsv1',bytes:256,offer:false,reserved:64,accept:false,close:1002,reason:'RSV1 set'},
 {id:'negotiated-rsv1-continuation',bytes:256,reservedContinuation:true,accept:true},
 {id:'no-context-invalid-distance',bytes:256,crossContext:true,accept:false,close:1000},
 {id:'control-length-fin-mask-before-extended-length',bytes:0,header:[0x29,126],accept:false,close:1002,reason:'RSV2 set, len > 125 for control, FIN not set on control, bad MASK'},
 {id:'reserved-opcode-before-mask-key',bytes:0,header:[0xb3,128],accept:false,close:1002,reason:'RSV2 set, RSV3 set, bad opcode 3'},
 {id:'unnegotiated-rsv1-reserved-opcode-mask',bytes:0,offer:false,header:[0xcf,0],accept:false,close:1002,reason:'RSV1 set, bad opcode 15, bad MASK'},
 {id:'data-before-final-fragment',bytes:0,fragmentStart:true,header:[0x92,0],accept:false,close:1002,reason:'RSV3 set, data before FIN, bad MASK'},
 {id:'continuation-after-final-fragment',bytes:0,header:[0x80,128],accept:false,close:1002,reason:'continuation after FIN'},
 {id:'non-minimal-16-bit-data-length',bytes:16,extendedLength:126,accept:true},
 {id:'non-minimal-64-bit-data-length',bytes:16,extendedLength:127,accept:true},
 {id:'signed-64-bit-length-before-mask',bytes:0,rawHeader:'82ff8000000000000000',accept:false,close:1000},
 {id:'signed-64-bit-length-with-mask',bytes:0,rawHeader:'82ffffffffff00000012345678',accept:false,close:1000},
 {id:'signed-aggregate-length-overflow',bytes:0,fragmentStart:true,rawHeader:'80ff7fffffffffffffff12345678',accept:false,close:1000},
 {id:'unmasked-data-before-payload',bytes:0,header:[0x82,125],accept:false,close:1002,reason:'bad MASK'},
];
assert.ok(vectors.length<=ROW_MAX);
const report={status:'RUNNING',scope:'Authenticated RTC permessage-deflate, no-context-takeover, raw fragmented frames and actual signal consumer limits',configuredLimit:limit,sourcePolicy:'rtcservice wire cap + wsprotocol decoded LimitReader; wire1009 vs decoded1000; zero/negative unlimited',maxInputMessage:2048,rows:[],cleanupErrors:[]};
for(const [side,url]of Object.entries(targets)){
 const api=new RoomServiceClient(url.replace(/^ws/,'http'),apiKey,secret);const room=`f65-cap-${randomUUID()}`;let peer;
 try{
  await api.createRoom({name:room});
  for(const [index,v]of vectors.entries()){
   const row={side,id:v.id,status:'RUNNING',messageBytes:v.bytes,largestSentFrame:v.fragment||v.bytes,fragment:v.fragment||null,expectedAccepted:v.accept};report.rows.push(row);
   try{
    const token=new AccessToken(apiKey,secret,{identity:`f65-${randomUUID()}`});token.addGrant({roomJoin:true,room,canPublish:false,canSubscribe:false,canPublishData:false});
    const endpoint=new URL(url);endpoint.pathname='/rtc';endpoint.search='';endpoint.searchParams.set('access_token',await token.toJwt());endpoint.searchParams.set('protocol','17');endpoint.searchParams.set('sdk','unknown');
    peer=new Peer(endpoint,v.offer!==false);await peer.connect();const first=await peer.next();assert.equal(first.opcode,2);assert.equal(SignalResponse.fromBinary(first.body).message.case,'join');row.join=true;
    const bytes=v.invalid||v.header||v.rawHeader||v.closePayload?Buffer.alloc(0):unknownMessage(v.bytes);if(v.noise)randomBytes(bytes.length-6).copy(bytes,6);assert.equal(bytes.length,v.bytes);row.inputSha256=hash(bytes);// Fixed DEFLATE block: 5 literal bytes of a protobuf unknown-field header,
    // then length251/distance256, which cannot refer to this message's history.
    const wire=v.crossContext?Buffer.from('6bda2af19b7104fb1f00','hex'):v.invalid?Buffer.from([6]):v.reserved?bytes:peer.offer?compressed(bytes):bytes;row.wireBytes=wire.length;if(v.id==='decoded-overflow')assert.ok(wire.length<1024);if(v.noise)assert.ok(wire.length>1024);
    // Unknown binary field is structurally valid; no request-size normalizer or malformed sentinel.
    row.writeError=null;try{if(v.closePayload){await peer.send(8,Buffer.from(v.closePayload));row.sentClosePayload=Buffer.from(v.closePayload).toString('hex');}else if(v.rawHeader){if(v.fragmentStart)await peer.send(2,unknownMessage(16),false);const raw=Buffer.from(v.rawHeader,'hex');await peer.write(raw);row.sentHeader=v.rawHeader;await peer.until(()=>peer.receivedClose,'close after invalid signed length',1500);}else if(v.extendedLength){await peer.send(2,wire,true,peer.offer,0,v.extendedLength);}else if(v.header){if(v.fragmentStart)await peer.send(2,unknownMessage(16),false);await peer.write(Buffer.from(v.header));row.sentHeader=Buffer.from(v.header).toString('hex');await peer.until(()=>peer.receivedClose,'close after the first two header bytes',1500);}else if(v.crossContext){await peer.message(bytes);await peer.send(2,wire,true,true);}else if(v.invalid)await peer.send(2,wire,true,true);else if(v.reserved)await peer.send(2,wire,true,false,v.reserved);else if(v.reservedContinuation){await peer.send(2,wire.subarray(0,2),false,true);await peer.send(0,wire.subarray(2),true,true);}else{await peer.message(bytes,v.fragment);if(v.repeat)await peer.message(bytes,v.fragment);}}catch(e){row.writeError=safe(e);}
    if(v.header||v.rawHeader)assert.equal(row.writeError,null);
    const marker=810000+index;
    if(v.accept){assert.equal(row.writeError,null);await peer.send(9,Buffer.from(`cap-${marker}`));
     // A real subsequent typed request must be processed, not merely buffered.
     await peer.message(new SignalRequest({message:{case:'pingReq',value:new Ping({timestamp:BigInt(marker),rtt:0n})}}).toBinary());
     await peer.pong(marker);row.correlatedSignalPong=true;row.correlatedControlPong=true;await peer.finish();
    }else{await peer.until(()=>peer.receivedClose,'actual overlimit close');await peer.finish();assert.equal(peer.closeFrame.code,v.close);assert.equal(peer.closeFrame.reason,v.reason||'');}
    row.serverCompressedFrames=peer.compressedFrames;if(peer.offer)assert.ok(peer.compressedFrames>0,'actual server data compressed');else assert.equal(peer.compressedFrames,0);row.actual=peer.summary();assert.equal(row.actual.receivedClose,true,'actual closeframe required');assert.equal(row.actual.serverEnded,true,'server must end actual transport');assert.equal(row.actual.forcedDestroy,false,'test must not manufacture transport close');assert.ok([null,'EPIPE','ECONNRESET'].includes(row.actual.transportError),'unexpected transport failure');if(v.accept){assert.equal(row.actual.clean,true,'actual accepted connection close handshake');assert.deepEqual(row.actual.close,{code:1000,reason:''});}row.status='PASS';peer=null;
   }catch(error){row.status='FAIL';row.error=safe(error);if(peer){await peer.finish().catch(()=>{});row.actual=peer.summary();peer=null;}}
  }
 }catch(error){report.cleanupErrors.push(`${side}: ${safe(error)}`);}
 finally{if(peer)await peer.finish().catch(e=>report.cleanupErrors.push(safe(e)));try{await api.deleteRoom(room);}catch(e){report.cleanupErrors.push(`${side}: roomcleanup ${safe(e)}`);}}
}
// Fixed wsprotocol.go CloseWithReason writes Close then immediately closes TCP,
// without awaiting ACK. Actual Go/Go control01 and original mixed receipts retain
// EPIPE/ECONNRESET/EOF races *after* the same correct rejection Close. Keep exact
// equality visible; separately compare the deterministic protocol contract.
report.transportTailBasis='fixed Go wsprotocol.go99 immediate conn.Close; real compression-go-go-control01';
for(const v of vectors){
 const pair=report.rows.filter(r=>r.id===v.id);if(pair.length!==2)continue;
 const observation=r=>({status:r.status,join:r.join||false,pong:r.correlatedSignalPong||false,controlPong:r.correlatedControlPong||false,actual:r.actual});
 const equality=JSON.stringify(observation(pair[0]))===JSON.stringify(observation(pair[1]));
 const contract=r=>({...observation(r),actual:v.accept?r.actual:{receivedClose:r.actual?.receivedClose,close:r.actual?.close,serverEnded:r.actual?.serverEnded,forcedDestroy:r.actual?.forcedDestroy}});
 const protocolEqual=JSON.stringify(contract(pair[0]))===JSON.stringify(contract(pair[1]));
 for(const r of pair){r.strictPairEqual=equality;r.protocolContractEqual=protocolEqual;}
}
report.status=report.rows.length===vectors.length*2&&report.rows.every(r=>r.status==='PASS'&&r.protocolContractEqual)&&report.cleanupErrors.length===0?'PASS_SCOPED':'FAIL';
await writeFile(process.env.WS_CAP_OUTPUT||'ws-compression-results.json',JSON.stringify(report,null,2)+'\n');
if(report.status!=='PASS_SCOPED')process.exitCode=1;
