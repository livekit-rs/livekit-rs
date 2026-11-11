/** Local standards-facing SIP/PCMU UA. Not a LiveKit/PSRPC worker substitute. */
import dgram from 'node:dgram';
import {randomBytes,createHash,timingSafeEqual} from 'node:crypto';
const hash=x=>createHash('md5').update(x).digest('hex');
const tag=()=>randomBytes(8).toString('hex');
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
export function parseMessage(buffer){
 if(buffer.length>65535)throw Error('oversize SIP datagram');
 const text=buffer.toString('utf8'),split=text.indexOf('\r\n\r\n');if(split<0)throw Error('missing SIP header delimiter');
 const lines=text.slice(0,split).split('\r\n'),start=lines.shift(),headers=new Map();
 for(const line of lines){const colon=line.indexOf(':');if(colon<1)throw Error('malformed SIP header');const key=line.slice(0,colon).toLowerCase();if(headers.has(key))headers.get(key).push(line.slice(colon+1).trim());else headers.set(key,[line.slice(colon+1).trim()]);}
 const body=buffer.subarray(Buffer.byteLength(text.slice(0,split))+4);
 if(Number(headers.get('content-length')?.[0]||0)!==body.length)throw Error('SIP Content-Length mismatch');
 return {start,headers,body:body.toString('utf8'),get:k=>headers.get(k.toLowerCase())?.[0]||''};
}
export function muEncode(value){
 let n=Math.round(value),sign=0;if(n<0){sign=0x80;n=-n;}n=Math.min(32635,n)+0x84;let exponent=7;
 for(let mask=0x4000;exponent>0&&(n&mask)===0;mask>>=1)exponent--;
 return (~(sign|(exponent<<4)|((n>>(exponent+3))&15)))&255;
}
export function muDecode(byte){const n=(~byte)&255,mag=(((n&15)<<3)+0x84)<<((n>>4)&7);return (n&128)?0x84-mag:mag-0x84;}
export function toneEvidence(samples,rate,frequency){
 if(samples.length<rate/2)return {samples:samples.length,ready:false};
 let sum=0,re=0,im=0,otherRe=0,otherIm=0;
 for(let i=0;i<samples.length;i++){const v=samples[i]/32768;sum+=v*v;re+=v*Math.cos(2*Math.PI*frequency*i/rate);im+=v*Math.sin(2*Math.PI*frequency*i/rate);otherRe+=v*Math.cos(2*Math.PI*440*i/rate);otherIm+=v*Math.sin(2*Math.PI*440*i/rate);}
 return {samples:samples.length,ready:true,rms:Math.sqrt(sum/samples.length),toneAmplitude:2*Math.hypot(re,im)/samples.length,reference440Amplitude:2*Math.hypot(otherRe,otherIm)/samples.length};
}
function authFields(text){const fields={};if(!text.startsWith('Digest '))return fields;for(const m of text.slice(7).matchAll(/([a-zA-Z0-9_-]+)=(?:"([^"]*)"|([^,\s]+))/g))fields[m[1]]=m[2]??m[3];return fields;}
function endpoint(uri){const m=uri.match(/^sip:[^@<>\s]+@(127\.0\.0\.1):(\d+)(?:;transport=udp)?$/);if(!m)throw Error('fixture only allows explicit loopback SIP endpoints');return {address:m[1],port:Number(m[2])};}
function rtpPayload(packet){
 if(packet.length<12||(packet[0]>>6)!==2)return null;let offset=12+(packet[0]&15)*4;
 if(packet[0]&16){if(offset+4>packet.length)return null;offset+=4+packet.readUInt16BE(offset+2)*4;}
 let end=packet.length;if(packet[0]&32)end-=packet.at(-1);if(end<offset)return null;
 return {pt:packet[1]&127,seq:packet.readUInt16BE(2),payload:packet.subarray(offset,end)};
}
export class LocalSipPeer {
 constructor({username='fixture-user',password='fixture-password',authenticate=true,media=true,onRefer}={}){
  Object.assign(this,{username,password,authenticate,media,onRefer});this.realm='f07.local';this.nonce=tag();this.localTag=tag();this.sip=dgram.createSocket('udp4');this.rtp=dgram.createSocket('udp4');this.rtcp=dgram.createSocket('udp4');this.dialog=null;this.outbound=new Map();this.cache=new Map();this.samples=[];this.timers=new Set();
  this.stats={challenges:0,authenticatedInvites:0,acceptedInvites:0,acks:0,rtpSent:0,rtpReceived:0,rtpPayloadBytes:0,referReceived:0,notifySent:0,notifyAccepted:0,byeReceived:0,byeSent:0,targetInviteAccepted:0,errors:[],overflow:false};
  this.sip.on('message',(b,r)=>{try{this.message(parseMessage(b),r);}catch(e){this.error(e);}});this.rtp.on('message',(b,r)=>{try{this.receiveRtp(b,r);}catch(e){this.error(e);}});for(const socket of [this.sip,this.rtp,this.rtcp])socket.on('error',e=>this.error(e));
 }
 error(error){if(this.stats.errors.length<16)this.stats.errors.push(String(error.message));else this.stats.overflow=true;}
 async start(){for(const socket of [this.sip,this.rtp,this.rtcp])await new Promise((resolve,reject)=>{socket.once('error',reject);socket.bind(0,'127.0.0.1',resolve);});this.port=this.sip.address().port;return this;}
 get uri(){return `sip:peer@127.0.0.1:${this.port}`;}
 send(raw,remote){this.sip.send(Buffer.from(raw),remote.port,remote.address);}
 response(req,remote,status,reason,extra=[],body=''){
  const to=req.get('to').includes(';tag=')?req.get('to'):`${req.get('to')};tag=${this.localTag}`;
  const lines=[`SIP/2.0 ${status} ${reason}`,...(req.headers.get('via')||[]).map(v=>`Via: ${v}`),`From: ${req.get('from')}`,`To: ${to}`,`Call-ID: ${req.get('call-id')}`,`CSeq: ${req.get('cseq')}`,...extra,`Content-Length: ${Buffer.byteLength(body)}`,'',body];const raw=lines.join('\r\n');this.send(raw,remote);return raw;
 }
 sdp(){return ['v=0',`o=f07 1 1 IN IP4 127.0.0.1`,'s=Local SIP interoperability','c=IN IP4 127.0.0.1','t=0 0',`m=audio ${this.rtp.address().port} RTP/AVP 0`,`a=rtcp:${this.rtcp.address().port} IN IP4 127.0.0.1`,'a=rtpmap:0 PCMU/8000','a=ptime:20','a=sendrecv',''].join('\r\n');}
 authenticated(req){
  const a=authFields(req.get('authorization')||req.get('proxy-authorization'));if(!a.response)return false;
  if(a.username!==this.username||a.realm!==this.realm||a.nonce!==this.nonce||a.algorithm&&a.algorithm.toUpperCase()!=='MD5'||a.uri!==req.start.split(' ')[1])throw Error('Digest challenge identity mismatch');
  const first=hash(`${this.username}:${this.realm}:${this.password}`),last=hash(`INVITE:${a.uri}`);
  if(a.qop&&a.qop!=='auth')throw Error('unsupported Digest qop');
  const expected=a.qop?hash(`${first}:${this.nonce}:${a.nc}:${a.cnonce}:auth:${last}`):hash(`${first}:${this.nonce}:${last}`);
  if(!/^[0-9a-f]{32}$/i.test(a.response)||!timingSafeEqual(Buffer.from(expected,'hex'),Buffer.from(a.response,'hex')))throw Error('Digest verification failed');return true;
 }
 message(req,remote){
  if(req.start.startsWith('SIP/2.0')){const pending=this.outbound.get(req.get('call-id'));if(pending)pending(req,remote);if(req.get('cseq').endsWith('NOTIFY')&&req.start.startsWith('SIP/2.0 200'))this.stats.notifyAccepted++;return;}
  if(this.cache.size>=64){this.stats.overflow=true;throw Error('bounded SIP transaction cache overflow');}
  const method=req.start.split(' ')[0];
  if(method==='OPTIONS'){this.response(req,remote,200,'OK');return;}
  if(method==='INVITE'){
   const key=`${req.get('call-id')}/${req.get('cseq')}/${req.get('via')}`;if(this.cache.has(key)){this.send(this.cache.get(key),remote);return;}
   if(this.authenticate&&!this.authenticated(req)){this.stats.challenges++;this.cache.set(key,this.response(req,remote,401,'Unauthorized',[`WWW-Authenticate: Digest realm="${this.realm}", nonce="${this.nonce}", algorithm=MD5, qop="auth"`]));return;}
   if(this.authenticate)this.stats.authenticatedInvites++;
   const audio=req.body.match(/^m=audio (\d+) RTP\/AVP ([^\r\n]+)/m);const connection=[...req.body.matchAll(/^c=IN IP4 ([^\r\n]+)/gm)].at(-1);
   if(!audio||!audio[2].split(' ').includes('0')||!connection||connection[1]!=='127.0.0.1')throw Error('PCMU loopback offer required; no codec fallback');
   this.response(req,remote,100,'Trying');this.stats.acceptedInvites++;
   const ok=this.response(req,remote,200,'OK',[`Contact: <${this.uri}>`,'Content-Type: application/sdp','Allow: INVITE, ACK, BYE, CANCEL, OPTIONS, REFER, NOTIFY'],this.sdp());this.cache.set(key,ok);
   this.dialog={invite:req,remote,rtp:{address:connection[1],port:Number(audio[1])},to:`${req.get('to')};tag=${this.localTag}`,from:req.get('from'),callId:req.get('call-id')};return;
  }
  if(method==='ACK'){if(this.dialog&&req.get('call-id')===this.dialog.callId&&req.get('cseq').split(' ')[0]===this.dialog.invite.get('cseq').split(' ')[0]){this.stats.acks++;if(this.media&&!this.audioTimer)this.startRtp();}return;}
  if(method==='REFER'){if(!this.dialog||req.get('call-id')!==this.dialog.callId)throw Error('REFER outside actual dialog');const key=`REFER/${req.get('call-id')}/${req.get('cseq')}/${req.get('via')}`;if(this.cache.has(key)){this.send(this.cache.get(key),remote);return;}this.stats.referReceived++;const referTo=req.get('refer-to').replace(/^<|>$/g,'');endpoint(referTo);this.cache.set(key,this.response(req,remote,202,'Accepted'));this.refer(req,referTo).catch(e=>this.error(e));return;}
  if(method==='BYE'){this.stats.byeReceived++;this.response(req,remote,200,'OK');this.stopRtp();return;}
  if(method==='CANCEL'){this.response(req,remote,200,'OK');return;}
  this.response(req,remote,501,'Not Implemented');
 }
 startRtp(){let sequence=1000,timestamp=0,index=0;const ssrc=0x07070001;this.audioTimer=setInterval(()=>{
  const packet=Buffer.alloc(172);packet[0]=0x80;packet[1]=0;packet.writeUInt16BE(sequence++&65535,2);packet.writeUInt32BE(timestamp>>>0,4);packet.writeUInt32BE(ssrc,8);timestamp+=160;
  for(let i=0;i<160;i++)packet[12+i]=muEncode(7000*Math.sin(2*Math.PI*440*index++/8000));
  this.rtp.send(packet,this.dialog.rtp.port,this.dialog.rtp.address);this.stats.rtpSent++;
 },20);}
 stopRtp(){if(this.audioTimer){clearInterval(this.audioTimer);this.audioTimer=null;}}
 receiveRtp(buffer,remote){if(remote.address!=='127.0.0.1')throw Error('nonlocal RTP');const packet=rtpPayload(buffer);if(!packet||packet.pt!==0)return;this.stats.rtpReceived++;this.stats.rtpPayloadBytes+=packet.payload.length;for(const b of packet.payload){if(this.samples.length===64000)this.samples.splice(0,8000);this.samples.push(muDecode(b));}}
 request(method,uri,headers,body='',remote=endpoint(uri)){const raw=[`${method} ${uri} SIP/2.0`,`Via: SIP/2.0/UDP 127.0.0.1:${this.port};branch=z9hG4bK${tag()};rport`,'Max-Forwards: 70',...headers,`Content-Length: ${Buffer.byteLength(body)}`,'',body].join('\r\n');this.send(raw,remote);}
 async dialLocal(uri){const remote=endpoint(uri),id=`f07-target-${tag()}`,from=`<${this.uri}>;tag=${tag()}`;return new Promise((resolve,reject)=>{
  const timer=setTimeout(()=>{this.outbound.delete(id);reject(Error('actual transfer target INVITE unanswered'));},4000);this.timers.add(timer);
  this.outbound.set(id,res=>{const status=Number(res.start.split(' ')[1]);if(status<200)return;if(status!==200){reject(Error('actual transfer target rejected'));return;}
   this.request('ACK',uri,[`From: ${from}`,`To: ${res.get('to')}`,`Call-ID: ${id}`,'CSeq: 1 ACK'], '',remote);clearTimeout(timer);this.outbound.delete(id);this.stats.targetInviteAccepted++;this.targetDialog={id,from,to:res.get('to'),uri,remote};resolve();
  });
  this.request('INVITE',uri,[`From: ${from}`,`To: <${uri}>`,`Call-ID: ${id}`,'CSeq: 1 INVITE',`Contact: <${this.uri}>`,'Content-Type: application/sdp'],this.sdp(),remote);
 });}
 notify(refer,status,reason,terminal){const d=this.dialog;this.request('NOTIFY',this.workerContact(),[`From: ${d.to}`,`To: ${d.from}`,`Call-ID: ${d.callId}`,`CSeq: ${this.notifySeq=(this.notifySeq||100)+1} NOTIFY`,`Event: refer;id=${refer.get('cseq').split(' ')[0]}`,`Subscription-State: ${terminal?'terminated;reason=noresource':'active;expires=30'}`,'Content-Type: message/sipfrag;version=2.0'],`SIP/2.0 ${status} ${reason}\r\n`,d.remote);this.stats.notifySent++;}
 workerContact(){const contact=this.dialog.invite.get('contact'),match=contact.match(/<([^>]+)>/);return match?match[1]:contact.split(';')[0];}
 async refer(req,uri){await sleep(30);this.notify(req,100,'Trying',false);await this.dialLocal(uri);await sleep(50);this.notify(req,200,'OK',true);/* Worker source closes old call after REFER success (1sec fallback). */}
 async endOriginalDialog(){
  const dialog=this.dialog;
  if(!dialog||this.stats.byeReceived||!this.stats.acks)return false;
  const seq=(this.notifySeq||100)+1;
  return new Promise(resolve=>{
   const finish=ok=>{clearTimeout(timer);this.outbound.delete(dialog.callId);resolve(ok);};
   const timer=setTimeout(()=>finish(false),1500);
   this.outbound.set(dialog.callId,response=>{
    if(response.get('cseq')!==`${seq} BYE`)return;
    const status=Number(response.start.split(' ')[1]);
    if(status>=200)finish(status===200);
   });
   this.request('BYE',this.workerContact(),[`From: ${dialog.to}`,`To: ${dialog.from}`,
    `Call-ID: ${dialog.callId}`,`CSeq: ${seq} BYE`],'',dialog.remote);
   this.stats.byeSent++;
  });
 }
 snapshot(){return {...this.stats,rtpTone880:toneEvidence(this.samples.slice(-32000),8000,880)};}
 async close(){this.stopRtp();for(const timer of this.timers)clearTimeout(timer);if(this.targetDialog){const d=this.targetDialog;this.request('BYE',d.uri,[`From: ${d.from}`,`To: ${d.to}`,`Call-ID: ${d.id}`,'CSeq: 2 BYE'],'',d.remote);this.stats.byeSent++;await sleep(30);}for(const socket of [this.sip,this.rtp,this.rtcp])await new Promise(resolve=>socket.close(resolve));}
}
