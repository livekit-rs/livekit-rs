/** Genuine local SIP UAS: verify Digest, then reject the actual INVITE. */
import {LocalSipPeer} from './peer-base.mjs';
export class RejectingSipPeer extends LocalSipPeer {
 constructor(status){super({authenticate:true,media:false});if(![486,503].includes(status))throw Error('only explicit genuine SIP failure profiles');this.status=status;this.reason=status===486?'Busy Here':'Service Unavailable';this.rejected=new Map();this.rejectStats={finalResponses:0,finalAcks:0,retransmittedInvites:0};}
 message(req,remote){
  const method=req.start.split(' ')[0];
  if(method==='ACK'){
   const key=`${req.get('call-id')}/${req.get('cseq').split(' ')[0]}`;
   if(this.rejected.has(key)){this.rejectStats.finalAcks++;return;}
  }
  if(method==='INVITE'&&this.authenticated(req)){
   const key=`${req.get('call-id')}/${req.get('cseq').split(' ')[0]}`;
   if(this.rejected.has(key)){this.rejectStats.retransmittedInvites++;this.send(this.rejected.get(key),remote);return;}
   if(this.rejected.size>=8){this.stats.overflow=true;throw Error('bounded failure transaction overflow');}
   this.stats.authenticatedInvites++;this.rejectStats.finalResponses++;
   const headers=this.status===503?['Retry-After: 1']:[];
   this.rejected.set(key,this.response(req,remote,this.status,this.reason,headers));return;
  }
  // Includes the real unauthenticated INVITE challenge/its ACK; no fake RPC.
  return super.message(req,remote);
 }
 snapshot(){return {...super.snapshot(),rejection:{status:this.status,reason:this.reason,...this.rejectStats}};}
}
