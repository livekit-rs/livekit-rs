(()=>{
// Actual WHIP publisher only. Original native.js candidate/restart gate remains unchanged.
let pc, context, oscillator;const state={connected:false,postStatus:null,etagPresent:false,locationPresent:false,remoteAudioTracks:0};
window.proxyWhip={
 async start({endpoint,streamKey}){
  pc=new RTCPeerConnection({iceServers:[]});context=new AudioContext();await context.resume();
  pc.onconnectionstatechange=()=>{state.connected=pc.connectionState==='connected';};pc.ontrack=()=>state.remoteAudioTracks++;
  const sink=context.createMediaStreamDestination(),gain=context.createGain();gain.gain.value=.2;oscillator=context.createOscillator();oscillator.frequency.value=440;oscillator.connect(gain);gain.connect(sink);oscillator.start();
  pc.addTransceiver(sink.stream.getAudioTracks()[0],{direction:'sendonly',streams:[sink.stream]});
  await pc.setLocalDescription(await pc.createOffer());
  if(pc.iceGatheringState!=='complete')await new Promise((resolve,reject)=>{const timer=setTimeout(()=>{pc.removeEventListener('icegatheringstatechange',check);reject(Error('full ICE gathering deadline'));},10000);function check(){if(pc.iceGatheringState==='complete'){clearTimeout(timer);pc.removeEventListener('icegatheringstatechange',check);resolve();}}pc.addEventListener('icegatheringstatechange',check);check();});
  if(!pc.localDescription.sdp.includes('a=candidate:'))throw Error('no actual ICE candidates');
  const response=await fetch(endpoint,{method:'POST',headers:{'Authorization':`Bearer ${streamKey}`,'Content-Type':'application/sdp'},body:pc.localDescription.sdp});
  state.postStatus=response.status;state.etagPresent=!!response.headers.get('ETag');state.locationPresent=!!response.headers.get('Location');
  if(response.status!==201)throw Error(`worker proxy POST status ${response.status}`);
  await pc.setRemoteDescription({type:'answer',sdp:await response.text()});return structuredClone(state);
 },
 async snapshot(){const out=structuredClone(state);out.outbound={packetsSent:0,bytesSent:0};for(const s of(await pc.getStats()).values())if(s.type==='outbound-rtp'&&(s.kind==='audio'||s.mediaType==='audio')){out.outbound.packetsSent+=s.packetsSent||0;out.outbound.bytesSent+=s.bytesSent||0;}return out;},
 async close(){oscillator?.stop();pc?.close();await context?.close();}
};

})();
