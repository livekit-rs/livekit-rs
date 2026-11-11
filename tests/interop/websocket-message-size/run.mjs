/** Root-run actual authenticated RTC WebSocket size conformance. No mock server.
 * Raw RFC6455 framing guarantees one17MiB frame, without any client fragmentation.
 * Official unmodified server SDK signs JWTs; official protocol encodes/decodes.
 */
import assert from 'node:assert/strict';
import net from 'node:net';
import {randomBytes,randomUUID,createHash} from 'node:crypto';
import {writeFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {resolve,isAbsolute} from 'node:path';
assert.ok(process.env.WS_CAP_INTEROP_ROOT&&isAbsolute(process.env.WS_CAP_INTEROP_ROOT),'absolute existing locked tests/interop root required');
const require=createRequire(resolve(process.env.WS_CAP_INTEROP_ROOT,'package.json'));
const {AccessToken,RoomServiceClient}=require('livekit-server-sdk');
const {SignalRequest,SignalResponse,Ping}=require('@livekit/protocol');
const MiB=1<<20, RESPONSE_MAX=2*MiB, RECEIVE_BUFFER_MAX=3*MiB, ROW_MAX=8;
const limit=Number(process.env.WS_CAP_LIMIT);
assert.ok(Number.isSafeInteger(limit)&&[-1,0,1024,20*MiB].includes(limit),'select actualconfigured -1/0/1024/20MiB profile');
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
function frameHeader(opcode,length,mask,fin){const small=length<126?length:length<=65535?126:127;const h=Buffer.alloc(2+(small===126?2:small===127?8:0)+4);h[0]=(fin?128:0)|opcode;h[1]=128|small;let at=2;if(small===126){h.writeUInt16BE(length,at);at+=2;}if(small===127){h.writeBigUInt64BE(BigInt(length),at);at+=8;}mask.copy(h,at);return h;}
class Peer {
 constructor(url){this.url=url;this.buffer=Buffer.alloc(0);this.frames=[];this.waiters=[];this.serial=Promise.resolve();this.closeFrame=null;this.closeAckSent=false;this.receivedClose=false;this.sentClose=false;this.ended=false;this.error=null;this.frameCount=0;this.upgraded=false;this.fragments=[];this.fragmentBytes=0;this.fragmentOpcode=0;}
 async connect(){
  const u=new URL(this.url);const key=randomBytes(16).toString('base64');this.expectedAccept=createHash('sha1').update(key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  this.socket=net.createConnection({host:u.hostname,port:Number(u.port||80)});
  this.socket.on('data',b=>{try{this.receive(b);}catch(e){this.error=safe(e);this.socket.destroy();this.wake();}});
  this.socket.on('error',e=>{this.error=e.code||safe(e);this.wake();});
  this.socket.on('close',()=>{this.ended=true;this.wake();});
  await new Promise((resolve,reject)=>{this.socket.once('connect',resolve);this.socket.once('error',reject);});
  await this.write(Buffer.from(`GET ${u.pathname}${u.search} HTTP/1.1\r\nHost: ${u.host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: ${key}\r\n\r\n`));
  await this.until(()=>this.upgraded,'actual HTTP101 upgrade');
 }
 write(bytes){return new Promise((resolve,reject)=>{if(this.socket.destroyed){reject(new Error('actual socket alreadyclosed'));return;}this.socket.write(bytes,e=>e?reject(e):resolve());});}
 send(opcode,bytes,fin=true){
  const run=async()=>{const mask=randomBytes(4),payload=Buffer.allocUnsafe(bytes.length);for(let i=0;i<bytes.length;i++)payload[i]=bytes[i]^mask[i&3];await this.write(frameHeader(opcode,payload.length,mask,fin));await this.write(payload);if(opcode===8)this.sentClose=true;};
  const pending=this.serial.then(run);this.serial=pending.catch(()=>{});return pending;
 }
 async message(bytes,fragment){
  if(!fragment){await this.send(2,bytes);return;}
  for(let i=0;i<bytes.length;i+=fragment){await this.send(i===0?2:0,bytes.subarray(i,Math.min(i+fragment,bytes.length)),i+fragment>=bytes.length);}
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
  if(!this.upgraded){const end=this.buffer.indexOf('\r\n\r\n');if(end<0){assert.ok(this.buffer.length<=65536,'HTTPheader bound');return;}const header=this.buffer.subarray(0,end).toString('latin1');assert.match(header,/^HTTP\/1\.[01] 101 /);const fields=Object.fromEntries(header.split('\r\n').slice(1).map(l=>{const at=l.indexOf(':');return[l.slice(0,at).toLowerCase(),l.slice(at+1).trim()];}));assert.equal(fields['sec-websocket-accept'],this.expectedAccept);assert.equal(fields['sec-websocket-extensions'],undefined,'no compression negotiated');this.buffer=this.buffer.subarray(end+4);this.upgraded=true;this.wake();}
  while(this.buffer.length>=2){const a=this.buffer[0],b=this.buffer[1];assert.equal(a&112,0);assert.equal(b&128,0,'serverframes unmasked');const opcode=a&15,fin=!!(a&128);let n=b&127,at=2;if(n===126){if(this.buffer.length<4)return;n=this.buffer.readUInt16BE(2);at=4;}if(n===127){if(this.buffer.length<10)return;const size=this.buffer.readBigUInt64BE(2);assert.ok(size<=BigInt(RESPONSE_MAX));n=Number(size);at=10;}assert.ok(n<=RESPONSE_MAX);if(this.buffer.length<at+n)return;const body=Buffer.from(this.buffer.subarray(at,at+n));this.buffer=this.buffer.subarray(at+n);this.frameCount++;assert.ok(this.frameCount<=256,'serverframe count bound');
   if(opcode===9){assert.ok(fin&&n<=125);this.send(10,body).catch(e=>{this.error=safe(e);this.wake();});continue;}
   if(opcode===8){assert.ok(fin&&n<=125&&n!==1);this.receivedClose=true;this.closeFrame={code:n>=2?body.readUInt16BE(0):1005,reason:n>=2?new TextDecoder('utf8',{fatal:true}).decode(body.subarray(2)):''};this.serial.then(async()=>{if(!this.sentClose)await this.send(8,body);this.closeAckSent=true;this.wake();}).catch(()=>this.wake());this.wake();continue;}
   if(opcode===0){assert.ok(this.fragmentOpcode!==0);this.fragments.push(body);this.fragmentBytes+=n;assert.ok(this.fragmentBytes<=RESPONSE_MAX);if(!fin)continue;this.push(this.fragmentOpcode,Buffer.concat(this.fragments));this.fragments=[];this.fragmentBytes=0;this.fragmentOpcode=0;continue;}
   if(!fin){assert.ok(opcode===1||opcode===2);assert.equal(this.fragmentOpcode,0);this.fragmentOpcode=opcode;this.fragments=[body];this.fragmentBytes=n;continue;}
   this.push(opcode,body);
  }
 }
 push(opcode,body){assert.ok(this.frames.length<64,'explicit framequeue overflow');this.frames.push({opcode,body});this.wake();}
 async pong(marker){let control=false,signal=false;for(let i=0;i<32;i++){const frame=await this.next();if(frame.opcode===10&&frame.body.equals(Buffer.from(`cap-${marker}`)))control=true;if(frame.opcode===2){const response=SignalResponse.fromBinary(frame.body);if(response.message.case==='pongResp'&&Number(response.message.value.lastPingTimestamp)===marker)signal=true;}if(control&&signal)return true;}throw new Error('correlated controlPong and actual next SignalPong both required');}
 summary(){return{receivedClose:this.receivedClose,close:this.closeFrame,clean:this.receivedClose&&this.closeAckSent&&this.ended&&!this.error,transportError:this.error};}
 async finish(){if(!this.socket)return;if(!this.receivedClose&&!this.ended){const body=Buffer.alloc(2);body.writeUInt16BE(1000);await this.send(8,body).catch(()=>{});}const stop=Date.now()+5000;while(!this.ended&&Date.now()<stop){await new Promise(r=>setTimeout(r,20));}if(!this.ended){this.socket.destroy();await new Promise(r=>this.socket.once('close',r));} }
}
const vectors=limit===20*MiB?[
 {id:'single17MiB',bytes:17*MiB,fragment:0,accept:true},
 {id:'fragmented17MiB',bytes:17*MiB,fragment:MiB,accept:true},
 {id:'exact20MiB',bytes:20*MiB,fragment:0,accept:true},
 {id:'single20MiBplus1',bytes:20*MiB+1,fragment:0,accept:false},
 {id:'fragmented20MiBplus1',bytes:20*MiB+1,fragment:MiB,accept:false},
]:limit>0?[
 {id:'exact1024',bytes:1024,fragment:0,accept:true},
 {id:'single1025',bytes:1025,fragment:0,accept:false},
 {id:'fragmented1025',bytes:1025,fragment:512,accept:false},
]:[
 {id:'single17MiB-unlimited',bytes:17*MiB,fragment:0,accept:true},
 {id:'fragmented65MiB-unlimited',bytes:65*MiB,fragment:MiB,accept:true},
];
assert.ok(vectors.length<=ROW_MAX);
const report={status:'RUNNING',scope:'Actual RTC WebSocket binary cap, no compression/media/worker/profiling claims',configuredLimit:limit,sourcePolicy:'Only positive limits apply; zero/negative unbounded, observed here up to65MiB',maxInputMessage:65*MiB,rows:[],cleanupErrors:[]};
for(const [side,url]of Object.entries(targets)){
 const api=new RoomServiceClient(url.replace(/^ws/,'http'),apiKey,secret);const room=`f65-cap-${randomUUID()}`;let peer;
 try{
  await api.createRoom({name:room});
  for(const [index,v]of vectors.entries()){
   const row={side,id:v.id,status:'RUNNING',messageBytes:v.bytes,largestSentFrame:v.fragment||v.bytes,fragment:v.fragment||null,expectedAccepted:v.accept};report.rows.push(row);
   try{
    const token=new AccessToken(apiKey,secret,{identity:`f65-${randomUUID()}`});token.addGrant({roomJoin:true,room,canPublish:false,canSubscribe:false,canPublishData:false});
    const endpoint=new URL(url);endpoint.pathname='/rtc';endpoint.search='';endpoint.searchParams.set('access_token',await token.toJwt());endpoint.searchParams.set('protocol','17');endpoint.searchParams.set('sdk','unknown');
    peer=new Peer(endpoint);await peer.connect();const first=await peer.next();assert.equal(first.opcode,2);assert.equal(SignalResponse.fromBinary(first.body).message.case,'join');row.join=true;
    const bytes=unknownMessage(v.bytes);assert.equal(bytes.length,v.bytes);row.inputSha256=hash(bytes);
    // Unknown binary field is structurally valid; no request-size normalizer or malformed sentinel.
    row.writeError=null;try{await peer.message(bytes,v.fragment);}catch(e){row.writeError=safe(e);}
    const marker=810000+index;
    if(v.accept){assert.equal(row.writeError,null);await peer.send(9,Buffer.from(`cap-${marker}`));
     // A real subsequent typed request must be processed, not merely buffered.
     await peer.message(new SignalRequest({message:{case:'pingReq',value:new Ping({timestamp:BigInt(marker),rtt:0n})}}).toBinary());
     await peer.pong(marker);row.correlatedSignalPong=true;row.correlatedControlPong=true;await peer.finish();
    }else{await peer.until(()=>peer.receivedClose,'actual overlimit close');await peer.finish();assert.equal(peer.closeFrame.code,1009);assert.equal(peer.closeFrame.reason,'');}
    row.actual=peer.summary();assert.equal(row.actual.receivedClose,true,'actual closeframe required');if(v.accept){assert.equal(row.actual.clean,true,'actual accepted connection close handshake');assert.deepEqual(row.actual.close,{code:1000,reason:''});}row.status='PASS';peer=null;
   }catch(error){row.status='FAIL';row.error=safe(error);if(peer){await peer.finish().catch(()=>{});row.actual=peer.summary();peer=null;}}
  }
 }catch(error){report.cleanupErrors.push(`${side}: ${safe(error)}`);}
 finally{if(peer)await peer.finish().catch(e=>report.cleanupErrors.push(safe(e)));try{await api.deleteRoom(room);}catch(e){report.cleanupErrors.push(`${side}: roomcleanup ${safe(e)}`);}}
}
for(const v of vectors){const pair=report.rows.filter(r=>r.id===v.id);if(pair.length!==2)continue;const observation=r=>({status:r.status,join:r.join||false,pong:r.correlatedSignalPong||false,controlPong:r.correlatedControlPong||false,actual:r.actual});const equality=JSON.stringify(observation(pair[0]))===JSON.stringify(observation(pair[1]));for(const r of pair)r.strictPairEqual=equality;}
report.status=report.rows.length===vectors.length*2&&report.rows.every(r=>r.status==='PASS'&&r.strictPairEqual)&&report.cleanupErrors.length===0?'PASS_SCOPED':'FAIL';
await writeFile(process.env.WS_CAP_OUTPUT||'ws-capacity-results.json',JSON.stringify(report,null,2)+'\n');
if(report.status!=='PASS_SCOPED')process.exitCode=1;
