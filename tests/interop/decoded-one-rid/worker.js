// Standard WebRTC Encoded Transform; official SDK bytes are never modified.
const workerCapabilities={rtcTransformEvent:typeof self.RTCTransformEvent!=='undefined',handlerIDL:'onrtctransform' in self};
let pipelineStarted=0;let policy='all', generation=0, total=0, dropped=0, passed=0;
let layers={}, missing=0, dependencyViolation=0, highDependencyRejected=0, highPassed=0, droppedIds=new Set(), recent=[];
function snapshot(){return {workerCapabilities,pipelineStarted,policy,generation,total,dropped,passed,layers,missing,dependencyViolation,highDependencyRejected,highPassed,recent:recent.slice(-80)};}
onmessage=e=>{if(e.data.policy){policy=e.data.policy;generation=e.data.generation;droppedIds.clear();} if(e.data.snapshot)postMessage({id:e.data.id,snapshot:snapshot()});};
self.onrtctransform=e=>{
 pipelineStarted++;
 e.transformer.readable.pipeThrough(new TransformStream({transform(frame,controller){
  const m=frame.getMetadata();total++;
  const valid=Number.isInteger(m.spatialIndex)&&Number.isInteger(m.temporalIndex)&&Number.isInteger(m.frameId)&&Array.isArray(m.dependencies);
  if(!valid)missing++;
  const key=`${m.spatialIndex}/${m.temporalIndex}`;layers[key]=(layers[key]||0)+1;
  let keep=true;
  if(policy==='high-only'){
   keep=valid&&m.spatialIndex===2;
   if(!keep&&Number.isInteger(m.frameId))droppedIds.add(m.frameId);
   if(keep&&m.dependencies.some(id=>droppedIds.has(id))){highDependencyRejected++;keep=false;}
   // A rejected high frame retires its own ID as well. Descendants cannot be
   // released merely because their direct parent was high rather than low.
   if(!keep&&Number.isInteger(m.frameId))droppedIds.add(m.frameId);
   if(keep)highPassed++;
  }
  recent.push({generation,spatial:m.spatialIndex,temporal:m.temporalIndex,frameId:m.frameId,ssrc:m.synchronizationSource,rtpTimestamp:m.rtpTimestamp,dependencies:m.dependencies,type:frame.type,bytes:frame.data.byteLength,keep});
  if(recent.length>400)recent.splice(0,200);
  if(droppedIds.size>4096)droppedIds=new Set([...droppedIds].slice(-2048));
  if(keep){passed++;controller.enqueue(frame);}else dropped++;
 }})).pipeTo(e.transformer.writable).catch(error=>postMessage({fatal:String(error)}));
};
