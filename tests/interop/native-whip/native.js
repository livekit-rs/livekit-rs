// Standards-facing WHIP client, not a LiveKit server/SFU substitute. NOT_RUN.
let whipPc,whipContext,whipOscillator,whipElement,whipRemote,whipAnalyser,whipTimer,whipLocation,whipEtag,whipUrl,whipToken;
const whipState={candidatePatchCount:0,connected:false,remoteAudioTracks:0,tone880:null,errors:[],receiverPackets:0};
const pendingCandidates=[];
function boundedError(error){if(whipState.errors.length<16)whipState.errors.push(String(error.message));}
function iceParts(sdp){const lines=sdp.split(/\r?\n/),m=lines.findIndex(s=>s.startsWith('m='));const media=lines.slice(m).slice(0,lines.slice(m+1).findIndex(s=>s.startsWith('m='))<0?undefined:lines.slice(m+1).findIndex(s=>s.startsWith('m='))+1);const take=key=>media.find(l=>l.startsWith(key))||lines.slice(0,m).find(l=>l.startsWith(key));return{group:lines.find(s=>s.startsWith('a=group:')),info:media[0],mid:media.find(s=>s.startsWith('a=mid:')),ufrag:take('a=ice-ufrag:'),pwd:take('a=ice-pwd:')};}
function fragment(sdp,candidates){const p=iceParts(sdp);if(!p.mid||!p.ufrag||!p.pwd)throw Error('actual SDP must provide MID and ICE credentials');return [p.group,p.info,p.mid,p.ufrag,p.pwd,...candidates.map(s=>`a=${s}`)].filter(Boolean).join('\r\n')+'\r\n';}
function patchAnswer(sdp,frag){
 const p=iceParts(frag),candidates=frag.split(/\r?\n/).filter(s=>s.startsWith('a=candidate:'));
 if(!p.ufrag||!p.pwd)throw Error('actual restart answer fragment missing native credentials');
 const lines=sdp.split(/\r?\n/),out=[];let inMedia=false;
 for(const line of lines){if(!line)continue;if(line.startsWith('m=')){if(inMedia)out.push(...candidates);inMedia=true;out.push(line);continue;}if(line.startsWith('a=candidate:')||line==='a=end-of-candidates')continue;if(line.startsWith('a=ice-ufrag:'))out.push(p.ufrag);else if(line.startsWith('a=ice-pwd:'))out.push(p.pwd);else out.push(line);}
 if(inMedia)out.push(...candidates);return out.join('\r\n')+'\r\n';
}
async function waitGather(pc,timeout=10000){if(pc.iceGatheringState==='complete')return;await new Promise((resolve,reject)=>{const end=setTimeout(()=>{pc.removeEventListener('icegatheringstatechange',check);reject(Error('actual ICE gathering did not complete'));},timeout);function check(){if(pc.iceGatheringState==='complete'){clearTimeout(end);pc.removeEventListener('icegatheringstatechange',check);resolve();}}pc.addEventListener('icegatheringstatechange',check);});}
function measure(samples,rate,freq){let sum=0,re=0,im=0,refRe=0,refIm=0;for(let i=0;i<samples.length;i++){const v=samples[i];sum+=v*v;re+=v*Math.cos(2*Math.PI*freq*i/rate);im+=v*Math.sin(2*Math.PI*freq*i/rate);refRe+=v*Math.cos(2*Math.PI*440*i/rate);refIm+=v*Math.sin(2*Math.PI*440*i/rate);}return{samples:samples.length,rms:Math.sqrt(sum/samples.length),toneAmplitude:2*Math.hypot(re,im)/samples.length,reference440Amplitude:2*Math.hypot(refRe,refIm)/samples.length};}
window.nativeWhip={
 async start({url,token,observerIdentity}){
  whipUrl=url.replace(/^ws/,'http');whipToken=token;whipContext=new AudioContext();await whipContext.resume();whipPc=new RTCPeerConnection({iceServers:[]});
  whipPc.onicecandidate=event=>{if(event.candidate){if(pendingCandidates.length>=64)throw Error('bounded actual ICE candidate overflow');pendingCandidates.push(event.candidate.candidate);}};
  whipPc.onconnectionstatechange=()=>{whipState.connected=whipPc.connectionState==='connected';};
  whipPc.ontrack=async event=>{try{
   if(event.track.kind!=='audio')return;if(whipState.remoteAudioTracks++>0)throw Error('unexpected second incoming WHIP audio track');
   whipElement=document.createElement('audio');whipElement.autoplay=true;whipElement.volume=0;whipElement.srcObject=new MediaStream([event.track]);document.body.append(whipElement);await whipElement.play();
   whipRemote=whipContext.createMediaStreamSource(new MediaStream([event.track]));whipAnalyser=whipContext.createAnalyser();whipAnalyser.fftSize=8192;const zero=whipContext.createGain();zero.gain.value=0;whipRemote.connect(whipAnalyser);whipAnalyser.connect(zero);zero.connect(whipContext.destination);
   whipTimer=setInterval(()=>{const samples=new Float32Array(whipAnalyser.fftSize);whipAnalyser.getFloatTimeDomainData(samples);whipState.tone880=measure(samples,whipContext.sampleRate,880);},100);
  }catch(error){boundedError(error);}};
  const sink=whipContext.createMediaStreamDestination(),gain=whipContext.createGain();gain.gain.value=.2;whipOscillator=whipContext.createOscillator();whipOscillator.frequency.value=440;whipOscillator.connect(gain);gain.connect(sink);whipOscillator.start();whipPc.addTrack(sink.stream.getAudioTracks()[0],sink.stream);
  const offer=await whipPc.createOffer();await whipPc.setLocalDescription(offer);
  // Use the original real offer before candidate gathering; successful transport must use actual PATCH.
  const response=await fetch(`${whipUrl}/whip/v1`,{method:'POST',headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/sdp','X-LiveKit-ClientInfo':JSON.stringify({subscribedParticipantTrackNames:{[observerIdentity]:['f10-rtc-880hz']}})},body:offer.sdp});
  if(response.status!==201)throw Error(`actual WHIP POST returned ${response.status}`);whipLocation=new URL(response.headers.get('Location'),whipUrl).href;whipEtag=response.headers.get('ETag');if(!whipEtag)throw Error('actual local ICE ETag absent');
  const answer=await response.text();await whipPc.setRemoteDescription({type:'answer',sdp:answer});await waitGather(whipPc);
  if(!pendingCandidates.length)throw Error('no actual gathered candidates to test trickle');
  const patch=await fetch(whipLocation,{method:'PATCH',headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/trickle-ice-sdpfrag','If-Match':whipEtag},body:fragment(offer.sdp,pendingCandidates.splice(0))});
  if(patch.status!==204)throw Error(`actual WHIP candidate PATCH returned ${patch.status}`);whipState.candidatePatchCount++;
  return{postStatus:201,patchStatus:204,locationIsParticipant:/\/whip\/v1\/PA_[A-Za-z0-9]+$/.test(whipLocation),etagPresent:true};
 },
 async restart(){
  const oldTag=whipEtag,oldUfrag=iceParts(whipPc.localDescription.sdp).ufrag;whipPc.restartIce();const offer=await whipPc.createOffer({iceRestart:true});await whipPc.setLocalDescription(offer);
  const expectedUfrag=iceParts(offer.sdp).ufrag;if(!expectedUfrag||expectedUfrag===oldUfrag)throw Error('real restart must change local ICE credentials');
  // A browser may reuse real candidate addresses without emitting new candidate events.
  // Observe the current real local SDP generation, never recycle the old event queue.
  const end=Date.now()+10000;let candidates=[];
  while(Date.now()<end){const current=whipPc.localDescription.sdp;if(iceParts(current).ufrag===expectedUfrag){candidates=current.split(/\r?\n/).filter(l=>l.startsWith('a=candidate:')).map(l=>l.slice(2));if(candidates.length&&whipPc.iceGatheringState==='complete')break;}await new Promise(r=>setTimeout(r,20));}
  pendingCandidates.length=0;if(!candidates.length||whipPc.iceGatheringState!=='complete')throw Error('restart did not gather actual current-generation ICE candidates');
  const response=await fetch(whipLocation,{method:'PATCH',headers:{Authorization:`Bearer ${whipToken}`,'Content-Type':'application/trickle-ice-sdpfrag','If-Match':'*'},body:fragment(whipPc.localDescription.sdp,candidates)});
  if(response.status!==200)throw Error(`actual ICE restart returned ${response.status}`);const answer=await response.text(),newTag=response.headers.get('ETag');if(!newTag||newTag===oldTag)throw Error('actual ICE restart did not change native local ufrag ETag');
  await whipPc.setRemoteDescription({type:'answer',sdp:patchAnswer(whipPc.remoteDescription.sdp,answer)});whipEtag=newTag;
  const stale=await fetch(whipLocation,{method:'PATCH',headers:{Authorization:`Bearer ${whipToken}`,'Content-Type':'application/trickle-ice-sdpfrag','If-Match':oldTag},body:fragment(offer.sdp,[])});
  return{restartStatus:200,etagChanged:true,oldTagStatus:stale.status};
 },
 async remove(){const response=await fetch(whipLocation,{method:'DELETE',headers:{Authorization:`Bearer ${whipToken}`}});return response.status;},
 async snapshot(){const value=structuredClone(whipState);value.connectionState=whipPc?.connectionState;value.iceConnectionState=whipPc?.iceConnectionState;value.dataChannelsCreated=0;if(whipPc){for(const stat of(await whipPc.getStats()).values())if(stat.type==='inbound-rtp'&&stat.kind==='audio'){value.receiverPackets+=stat.packetsReceived||0;value.totalSamplesReceived=stat.totalSamplesReceived;}}return value;},
 async close(){clearInterval(whipTimer);whipOscillator?.stop();whipPc?.close();if(whipElement){whipElement.pause();whipElement.srcObject=null;whipElement.remove();}whipRemote?.disconnect();whipAnalyser?.disconnect();await whipContext?.close();}
};
