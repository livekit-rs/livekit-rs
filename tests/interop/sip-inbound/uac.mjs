/** Actual UDP SIP UAC toward the unmodified official worker. No PSRPC code. */
import {LocalSipPeer} from '../sip-media/peer.mjs';
import {randomBytes,createHash} from 'node:crypto';
const tag=()=>randomBytes(8).toString('hex');
const md5=value=>createHash('md5').update(value).digest('hex');
export function parseDigest(value){
 if(!value.startsWith('Digest '))throw Error('worker did not issue Digest authentication');
 const out={};for(const field of value.slice(7).matchAll(/([A-Za-z0-9_-]+)=(?:"([^"]*)"|([^,\s]+))/g))out[field[1]]=field[2]??field[3];
 if(!out.realm||!out.nonce||out.algorithm&&out.algorithm.toUpperCase()!=='MD5')throw Error('unsupported worker Digest challenge');
 if(out.qop&&!out.qop.split(',').map(s=>s.trim()).includes('auth'))throw Error('worker Digest challenge has no supported auth qop');
 return out;
}
export function digestAuthorization({challenge,username,password,uri,cnonce,nc='00000001'}){
 const first=md5(`${username}:${challenge.realm}:${password}`),last=md5(`INVITE:${uri}`);
 const response=challenge.qop?md5(`${first}:${challenge.nonce}:${nc}:${cnonce}:auth:${last}`):md5(`${first}:${challenge.nonce}:${last}`);
 const q=value=>`"${String(value).replaceAll('\\','\\\\').replaceAll('"','\\"')}"`;
 let value=`Digest username=${q(username)}, realm=${q(challenge.realm)}, nonce=${q(challenge.nonce)}, uri=${q(uri)}, response=${q(response)}, algorithm=MD5`;
 if(challenge.qop)value+=`, qop=auth, nc=${nc}, cnonce=${q(cnonce)}`;
 if(challenge.opaque)value+=`, opaque=${q(challenge.opaque)}`;
 return value;
}
function endpoint(uri){
 const match=uri.match(/^sip:(?:[^@<>\s]+@)?(127\.0\.0\.1):(\d+)(?:;transport=udp)?$/);
 if(!match||Number(match[2])<1||Number(match[2])>65535)throw Error('only explicit loopback UDP SIP destination is allowed');
 return {address:match[1],port:Number(match[2])};
}
export class InboundSipUac extends LocalSipPeer {
 constructor(options={}){
  super({...options,authenticate:false,media:true});
  Object.assign(this.stats,{inviteSent:0,proxyChallenges:0,authorizedInviteSent:0,acceptedAnswers:0,ackSent:0,byeAccepted:0,authRejected:0});
  this.pendingInvite=null;this.sentTransactions=new Map();this.terminating=null;
 }
 transaction(method,uri,cseq,branch,from,to,extra=[],body=''){
  return [`${method} ${uri} SIP/2.0`,`Via: SIP/2.0/UDP 127.0.0.1:${this.port};branch=${branch};rport`,'Max-Forwards: 70',`From: ${from}`,`To: ${to}`,`Call-ID: ${this.callId}`,`CSeq: ${cseq} ${method}`,`Contact: <${this.uri}>`,...extra,`Content-Length: ${Buffer.byteLength(body)}`,'',body].join('\r\n');
 }
 async dial(uri,{fromNumber='+12025550111',expectAuthReject=false,timeout=15000}={}){
  if(this.pendingInvite||this.dialog)throw Error('one inbound call per fixture UA');
  this.destination=endpoint(uri);this.requestUri=uri;this.callId=`f08-${tag()}`;this.from=`<sip:${fromNumber}@127.0.0.1:${this.port}>;tag=${this.localTag}`;
  this.initialTo=`<${uri}>`;this.authAttempted=false;this.expectAuthReject=expectAuthReject;
  return new Promise((resolve,reject)=>{
   const timer=setTimeout(()=>{this.stopInviteRetransmit();this.pendingInvite=null;reject(Error('actual official worker INVITE deadline elapsed'));},timeout);this.timers.add(timer);
   this.pendingInvite={resolve,reject,timer};this.sendInvite(1,null);
  });
 }
 sendInvite(cseq,authorization){
  this.stopInviteRetransmit();this.inviteCseq=cseq;this.inviteBranch=`z9hG4bK${tag()}`;
  const extra=['Content-Type: application/sdp','Allow: INVITE, ACK, BYE, CANCEL, OPTIONS'];
  if(authorization)extra.push(`Proxy-Authorization: ${authorization}`);
  this.inviteRaw=this.transaction('INVITE',this.requestUri,cseq,this.inviteBranch,this.from,this.initialTo,extra,this.sdp());
  this.send(this.inviteRaw,this.destination);this.stats.inviteSent++;if(authorization)this.stats.authorizedInviteSent++;
  this.retransmitDelay=500;
  const retry=()=>{if(!this.pendingInvite)return;this.send(this.inviteRaw,this.destination);this.retransmitDelay=Math.min(4000,this.retransmitDelay*2);this.inviteRetry=setTimeout(retry,this.retransmitDelay);};
  this.inviteRetry=setTimeout(retry,this.retransmitDelay);
 }
 stopInviteRetransmit(){if(this.inviteRetry){clearTimeout(this.inviteRetry);this.inviteRetry=null;}}
 ack(response,success){
  const uri=success?this.workerUri:this.requestUri;
  const raw=this.transaction('ACK',uri,this.inviteCseq,success?`z9hG4bK${tag()}`:this.inviteBranch,this.from,response.get('to'));
  this.send(raw,this.destination);this.stats.ackSent++;
 }
 message(message,remote){
  if(!message.start.startsWith('SIP/2.0'))return super.message(message,remote);
  if(message.get('call-id')!==this.callId)return;
  if(remote.address!==this.destination?.address||remote.port!==this.destination?.port)throw Error('SIP response from unexpected worker endpoint');
  const status=Number(message.start.split(' ')[1]),[sequence,method]=message.get('cseq').split(' ');
  if(method==='BYE'&&this.terminating){
   if(Number(sequence)!==this.byeCseq)return;
   if(status<200)return;
   const pending=this.terminating;this.terminating=null;clearTimeout(pending.timer);if(this.byeRetry){clearInterval(this.byeRetry);this.byeRetry=null;}
   if(status===200){this.stats.byeAccepted++;this.stopRtp();this.dialog=null;pending.resolve();}else pending.reject(Error('worker rejected actual BYE'));
   return;
  }
  if(method!=='INVITE'||Number(sequence)!==this.inviteCseq)return;
  if(status<200){this.stopInviteRetransmit();return;}
  if(this.dialog&&status===200){this.ack(message,true);return;}
  if(!this.pendingInvite)return;
  if(status===407||status===401){
   this.stopInviteRetransmit();this.ack(message,false);
   if(this.authAttempted){
    if(this.expectAuthReject){const pending=this.pendingInvite;this.pendingInvite=null;clearTimeout(pending.timer);this.stats.authRejected++;pending.resolve({rejected:true,status});return;}
    throw Error('official SIP worker rejected configured Digest credentials');
   }
   if(status!==407)throw Error('expected official worker Proxy-Authenticate407 challenge');
   const challenge=parseDigest(message.get('proxy-authenticate'));this.stats.proxyChallenges++;
   this.authAttempted=true;
   this.sendInvite(this.inviteCseq+1,digestAuthorization({challenge,username:this.username,password:this.password,uri:this.requestUri,cnonce:tag()}));return;
  }
  if(status!==200){this.stopInviteRetransmit();this.ack(message,false);const pending=this.pendingInvite;this.pendingInvite=null;clearTimeout(pending.timer);pending.reject(Error(`official SIP worker final INVITE status ${status}`));return;}
  if(this.expectAuthReject)throw Error('wrong credentials unexpectedly established real SIP call');
  if(!this.authAttempted||this.stats.proxyChallenges<1)throw Error('SIP call succeeded without actual Digest challenge');
  const audio=message.body.match(/^m=audio (\d+) RTP\/AVP ([^\r\n]+)/m),connection=[...message.body.matchAll(/^c=IN IP4 ([^\r\n]+)/gm)].at(-1);
  if(!audio||!audio[2].split(' ').includes('0')||connection?.[1]!=='127.0.0.1')throw Error('actual worker did not answer loopback PCMU');
  if(message.headers.has('record-route'))throw Error('fixture does not claim routed SIP dialogs');
  const contact=message.get('contact');this.workerUri=contact.match(/<([^>]+)>/)?.[1]||contact;endpoint(this.workerUri);
  this.dialog={rtp:{address:connection[1],port:Number(audio[1])},remote,to:message.get('to'),from:this.from,callId:this.callId};
  this.stats.acceptedAnswers++;this.ack(message,true);this.startRtp();this.stopInviteRetransmit();
  const pending=this.pendingInvite;this.pendingInvite=null;clearTimeout(pending.timer);pending.resolve({accepted:true});
 }
 async hangup(){
  if(!this.dialog)return;
  if(this.terminating)throw Error('BYE already pending');
  this.byeCseq=this.inviteCseq+1;this.stats.byeSent++;
  return new Promise((resolve,reject)=>{
   const timer=setTimeout(()=>{this.terminating=null;if(this.byeRetry){clearInterval(this.byeRetry);this.byeRetry=null;}reject(Error('actual worker did not acknowledge BYE'));},4000);this.timers.add(timer);
   this.terminating={resolve,reject,timer};
   const raw=this.transaction('BYE',this.workerUri,this.byeCseq,`z9hG4bK${tag()}`,this.from,this.dialog.to);
   this.send(raw,this.destination);this.byeRetry=setInterval(()=>this.send(raw,this.destination),500);
  });
 }
 async close(){
  this.stopInviteRetransmit();
  try{if(this.dialog&&!this.terminating)await this.hangup();}
  finally{if(this.byeRetry){clearInterval(this.byeRetry);this.byeRetry=null;}await super.close();}
 }
}
