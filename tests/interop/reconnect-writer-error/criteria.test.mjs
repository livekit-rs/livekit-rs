import test from 'node:test';
import assert from 'node:assert/strict';
import{topologyValid,baseline,newMedia,recovered,assertDeadline,callBeforeDeadline,pollUntil}from'./criteria.mjs';
const single={mode:'publisher-only',publisherId:1,subscriberId:null,nativeIds:[1]};
const dual={mode:'subscriber-primary',publisherId:1,subscriberId:2,nativeIds:[1,2]};
const before={localSessionEpoch:1,trackEpochMax:4,topology:single,remoteEpochs:{source:1,victim:1},videos:[],audio:[]};
const base=baseline(before);
const video=(props={})=>({identity:'source',active:true,ended:false,bindingValid:true,participantCurrent:true,trackEpoch:5,localSessionEpoch:2,participantEpoch:1,pcId:3,width:640,height:360,frames:30,...props});
const audio=(props={})=>({identity:'source',active:true,bindingValid:true,participantCurrent:true,trackEpoch:6,localSessionEpoch:2,participantEpoch:1,pcId:3,audible:10,...props});
const rebuilt=(props={})=>({...before,localSessionEpoch:2,trackEpochMax:6,topology:{...single,publisherId:3,nativeIds:[3]},videos:[video()],audio:[audio()],...props});
test('requested single/dual must match actual physical topology',()=>{
 assert.ok(topologyValid(single,true));assert.ok(topologyValid(dual,false));
 assert.equal(topologyValid(single,false),false);assert.equal(topologyValid(dual,true),false);
 assert.equal(topologyValid({...dual,subscriberId:1,nativeIds:[1]},false),false);
 assert.equal(topologyValid({...single,mode:'subscriber-primary'},true),false);
 assert.equal(topologyValid(null,true),false);
});
test('old-session30frames/10samples plus new1/1 cannot satisfy recovery',()=>{
 const report=rebuilt({videos:[video({trackEpoch:3,localSessionEpoch:1,pcId:1,frames:300}),video({frames:1})],audio:[audio({trackEpoch:4,localSessionEpoch:1,pcId:1,audible:100}),audio({audible:1})]});
 const actual=newMedia(report,base,'source','local-rebuilt');
 assert.equal(actual.videoFrames,1);assert.equal(actual.audibleSamples,1);assert.equal(recovered(actual),false);
});
test('real new local session+nativePC+track generations allow exact30/10',()=>{
 const actual=newMedia(rebuilt(),base,'source','local-rebuilt');assert.equal(actual.videoFrames,30);assert.equal(actual.audibleSamples,10);assert.ok(recovered(actual));
});
test('new labels alone with old nativePC cannot credit media',()=>{
 const report=rebuilt({topology:single,videos:[video({pcId:1})],audio:[audio({pcId:1})]});assert.equal(recovered(newMedia(report,base,'source','local-rebuilt')),false);
});
test('new nativePC without new participant session cannot credit media',()=>{
 const report=rebuilt({localSessionEpoch:1,videos:[video({localSessionEpoch:1})],audio:[audio({localSessionEpoch:1})]});assert.equal(recovered(newMedia(report,base,'source','local-rebuilt')),false);
});
test('new PC/session cannot credit old subscribed track generation',()=>{
 const report=rebuilt({videos:[video({trackEpoch:3})],audio:[audio({trackEpoch:4})]});assert.equal(recovered(newMedia(report,base,'source','local-rebuilt')),false);
});
test('receiver/nativePC binding is mandatory',()=>{
 assert.equal(recovered(newMedia(rebuilt({videos:[video({bindingValid:false})]}),base,'source','local-rebuilt')),false);
 assert.equal(recovered(newMedia(rebuilt({topology:null}),base,'source','local-rebuilt')),false);
 assert.equal(recovered(newMedia(rebuilt({videos:[video({participantCurrent:false})]}),base,'source','local-rebuilt')),false);
});
test('witness requires real new remote participant, not its own local reconnect',()=>{
 const state={...before,videos:[video({identity:'victim',localSessionEpoch:1,participantEpoch:2,pcId:1})],audio:[audio({identity:'victim',localSessionEpoch:1,participantEpoch:2,pcId:1})]};
 assert.ok(recovered(newMedia(state,base,'victim','remote-rebuilt')));
 const old={...state,videos:[{...state.videos[0],participantEpoch:1}],audio:[{...state.audio[0],participantEpoch:1}]};assert.equal(recovered(newMedia(old,base,'victim','remote-rebuilt')),false);
 assert.equal(recovered(newMedia({...state,localSessionEpoch:2},base,'victim','remote-rebuilt')),false);
});
test('inactive/ended and undersized frames cannot satisfy new media gate',()=>{
 for(const properties of[{active:false},{ended:true},{width:320},{height:180},{frames:29}])assert.equal(recovered(newMedia(rebuilt({videos:[video(properties)]}),base,'source','local-rebuilt')),false);
 assert.equal(recovered(newMedia(rebuilt({audio:[audio({audible:9})]}),base,'source','local-rebuilt')),false);
});
test('late true predicate fails after await, rather than resetting deadline',async()=>{
 let now=0;await assert.rejects(pollUntil(async()=>{now=30001;return true;},30000,'late',{now:()=>now}),/late/);
});
test('late writer receipt fails the same absolute deadline',async()=>{
 let now=0;await assert.rejects(callBeforeDeadline(async()=>{now=30001;return 'ACK';},30000,()=>now,'late receipt'),/late receipt/);
});
test('boundary and early-success deadline semantics are explicit',async()=>{
 assert.doesNotThrow(()=>assertDeadline(30000,30000));assert.throws(()=>assertDeadline(30001,30000));
 let now=1;await pollUntil(async()=>{now=2;return true;},30000,'unexpected',{now:()=>now});
});
