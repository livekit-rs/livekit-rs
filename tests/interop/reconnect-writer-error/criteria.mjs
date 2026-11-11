// Shared by the actual runner and pure counterexample tests. No SDK or server
// dependency and no invented media events. Inputs are real observer snapshots.
export function topologyValid(topology,single){
 if(!topology||!Number.isInteger(topology.publisherId)||topology.publisherId<=0)return false;
 if(!Array.isArray(topology.nativeIds)||new Set(topology.nativeIds).size!==topology.nativeIds.length)return false;
 if(single)return topology.mode==='publisher-only'&&topology.subscriberId===null&&topology.nativeIds.length===1&&topology.nativeIds[0]===topology.publisherId;
 return ['subscriber-primary','publisher-primary'].includes(topology.mode)&&Number.isInteger(topology.subscriberId)&&topology.subscriberId>0&&topology.subscriberId!==topology.publisherId&&topology.nativeIds.length===2&&topology.nativeIds.includes(topology.publisherId)&&topology.nativeIds.includes(topology.subscriberId);
}
export function baseline(report){
 return {localSessionEpoch:report.localSessionEpoch,trackEpochMax:report.trackEpochMax,pcIds:[...report.topology.nativeIds],remoteEpochs:{...report.remoteEpochs}};
}
export function newMedia(report,before,identity,direction){
 if(!report.topology)return {videoFrames:0,audibleSamples:0,hasExpectedSize:false,trackEpochs:[]};
 const belongs=state=>{
  if(state.identity!==identity||!state.active||state.ended||!state.bindingValid||!state.participantCurrent||!Number.isInteger(state.trackEpoch)||state.trackEpoch<=before.trackEpochMax||!report.topology.nativeIds.includes(state.pcId)||state.localSessionEpoch!==report.localSessionEpoch)return false;
  if(direction==='local-rebuilt')return report.localSessionEpoch>before.localSessionEpoch&&!before.pcIds.includes(state.pcId)&&state.participantEpoch>=(before.remoteEpochs[identity]??0);
  if(direction==='remote-rebuilt')return report.localSessionEpoch===before.localSessionEpoch&&before.pcIds.includes(state.pcId)&&state.participantEpoch>(before.remoteEpochs[identity]??0);
  throw Error('unknown generation direction');
 };
 const videos=report.videos.filter(belongs),audio=report.audio.filter(belongs);
 return {videoFrames:videos.reduce((sum,v)=>sum+v.frames,0),audibleSamples:audio.reduce((sum,a)=>sum+a.audible,0),hasExpectedSize:videos.some(v=>v.width>=640&&v.height>=360),trackEpochs:[...videos,...audio].map(t=>t.trackEpoch)};
}
export function recovered(media){return media.hasExpectedSize&&media.videoFrames>=30&&media.audibleSamples>=10;}
export function assertDeadline(now,deadline,message='deadline exceeded'){if(!Number.isFinite(now)||!Number.isFinite(deadline)||now>deadline)throw Error(message);}
export async function callBeforeDeadline(fn,deadline,now=()=>performance.now(),message='deadline exceeded'){
 assertDeadline(now(),deadline,message);
 let timer;
 try{
  const value=await Promise.race([Promise.resolve().then(fn),new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error(message)),Math.max(0,deadline-now()));})]);
  assertDeadline(now(),deadline,message);return value;
 }finally{clearTimeout(timer);}
}
export async function pollUntil(fn,deadline,message,{now=()=>performance.now(),sleep=ms=>new Promise(r=>setTimeout(r,ms))}={}){
 while(now()<=deadline){
  const result=await callBeforeDeadline(fn,deadline,now,message);
  if(result)return;
  const left=deadline-now();if(left<=0)break;
  await sleep(Math.min(100,left));
 }
 throw Error(message);
}
