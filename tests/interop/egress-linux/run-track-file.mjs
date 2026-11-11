// Run INSIDE owned full-stack Linux guest, with unchanged actual external Egress.
import assert from'node:assert/strict';import{createRequire}from'node:module';import{resolve}from'node:path';import{pathToFileURL}from'node:url';import{createServer}from'node:http';import{readFile,writeFile,stat,rm,rename}from'node:fs/promises';import{execFile}from'node:child_process';import{promisify}from'node:util';import{randomUUID,createHash}from'node:crypto';
const env=process.env;for(const k of ['INTEROP_PROJECT','AGENT_SDK_ROOT','LIVEKIT_URL','EGRESS_WORKER_PID','EGRESS_OUTPUT_FILE','EGRESS_MEDIA_REPORT'])assert(env[k],`${k} required`);assert.equal(process.platform,'linux');assert.equal(process.arch,'arm64');
const require=createRequire(resolve(env.INTEROP_PROJECT,'package.json'));const{AccessToken,RoomServiceClient,EgressClient,DirectFileOutput,WebhookReceiver}=require('livekit-server-sdk');assert.equal(JSON.parse(await readFile(resolve(env.INTEROP_PROJECT,'node_modules/livekit-server-sdk/package.json'),'utf8')).version,'2.19.2');const rtc=await import(pathToFileURL(resolve(env.AGENT_SDK_ROOT,'node_modules/@livekit/rtc-node/dist/index.js')));
const exec=promisify(execFile),sha=x=>createHash('sha256').update(x).digest('hex'),sleep=ms=>new Promise(r=>setTimeout(r,ms));const key=env.LIVEKIT_API_KEY||'devkey',secret=env.LIVEKIT_API_SECRET||'secret';const rooms=new RoomServiceClient(env.LIVEKIT_URL,key,secret),egress=new EgressClient(env.LIVEKIT_URL,key,secret),receiver=new WebhookReceiver(key,secret);const roomName=`f11-track-${randomUUID()}`,room=new rtc.Room();let source,track,producer,producerError,stop=false,n=0,frames=0,id,child,completed=false;const events=[],errors=[],sockets=new Set();const report={status:'RUNNING',scope:'Linux ARM64 actual original Egress v1.15.0 TrackDirectFile OGG; unchanged RTC SDK real PCM->RTP->worker->actual file and decode',observations:{},cleanup_errors:[]};

const topologyBarrier = env.EGRESS_TOPOLOGY_BARRIER;
let frequency = 440;
function tone(pcm, start, end, hz) {
  let square = 0, sin = 0, cos = 0;
  const count = end - start;
  for (let i = start; i < end; i++) {
    const value = pcm.readInt16LE(i * 2);
    square += value * value;
    sin += value * Math.sin(2 * Math.PI * hz * i / 48000);
    cos += value * Math.cos(2 * Math.PI * hz * i / 48000);
  }
  const rms = Math.sqrt(square / count);
  const amplitude = 2 * Math.hypot(sin, cos) / count;
  const ratio = amplitude * amplitude / (2 * rms * rms);
  assert(rms > 100 && ratio > 0.5, `decoded ${hz} Hz segment has RMS ${rms}, tone ratio ${ratio}`);
  return { rms, ratio };
}

async function wait(check,label,ms=45000){const until=Date.now()+ms;while(Date.now()<until){const v=await check();if(v)return v;await sleep(100);}throw Error(`${label} deadline`);}
async function children(){const{stdout}=await exec('/bin/ps',['-axo','pid=,ppid=,comm='],{maxBuffer:1024*1024});return stdout.split('\n').map(s=>s.trim().match(/^(\d+)\s+(\d+)\s+(.+)$/)).filter(Boolean).filter(m=>+m[2]===+env.EGRESS_WORKER_PID&&m[3].split('/').at(-1)==='egress').map(m=>+m[1]);}
const sink=createServer(async(req,res)=>{try{let bytes=0;const parts=[];for await(const b of req){bytes+=b.length;assert(bytes<=1024*1024);parts.push(b);}events.push(await receiver.receive(Buffer.concat(parts).toString(),req.headers.authorization));res.end('ok');}catch{errors.push('webhook authentication or bounded decode failed');res.writeHead(401).end();}});sink.on('connection',s=>{sockets.add(s);s.on('close',()=>sockets.delete(s));});
try{
 assert(env.EGRESS_OUTPUT_FILE.startsWith('/home/fixture/fixture/output/'),'owned Linux output directory required');await assert.rejects(()=>stat(env.EGRESS_OUTPUT_FILE),e=>e.code==='ENOENT','must not overwrite an existing recording');assert.equal((await children()).length,0);
 await new Promise((r,j)=>{sink.once('error',j);sink.listen(0,'127.0.0.1',r);});await rooms.createRoom({name:roomName});const token=new AccessToken(key,secret,{identity:'f11-real-publisher'});token.addGrant({roomJoin:true,room:roomName,canPublish:true});await room.connect(env.LIVEKIT_URL.replace(/^http/,'ws'),await token.toJwt());source=new rtc.AudioSource(48000,1);track=rtc.LocalAudioTrack.createAudioTrack('real sine',source);const pub=await room.localParticipant.publishTrack(track,new rtc.TrackPublishOptions({source:rtc.TrackSource.SOURCE_MICROPHONE,dtx:false,red:false}));assert(pub.sid);
 // captureFrame already waits for the native bounded AudioSource queue.
 // A second relative20ms sleep underfeeds its real-time consumer and can underfeed the real-time consumer.
 producer=(async()=>{while(!stop){const pcm=new Int16Array(960);for(let i=0;i<960;i++)pcm[i]=Math.round(10000*Math.sin(2*Math.PI*frequency*n++/48000));await source.captureFrame(new rtc.AudioFrame(pcm,48000,1,960));frames++;}})();producer.catch(e=>{producerError=e;});
 const outbound=await wait(async()=>{if(producerError)throw producerError;const stats=await room.getRtcStats();let packets=0n,bytes=0n;for(const s of stats.publisherStats)if(s.stats.case==='outboundRtp'){packets+=s.stats.value.sent?.packetsSent??0n;bytes+=s.stats.value.sent?.bytesSent??0n;}return packets>0n&&bytes>0n?{packets:Number(packets),bytes:Number(bytes)}:null;},'actual publisher outbound RTP');
 // IMPORTANT: SDK string output means websocketUrl. Real DirectFileOutput is required.
 const started=await egress.startTrackEgress(roomName,new DirectFileOutput({filepath:env.EGRESS_OUTPUT_FILE,disableManifest:true}),pub.sid,[{url:`http://127.0.0.1:${sink.address().port}/egress`,signingKey:key}]);id=started.egressId;assert.match(id,/^EG_/);child=await wait(async()=>{const ids=await children();assert(ids.length<=1);return ids[0];},'actual original egress run-handler child');await wait(async()=>{const item=(await egress.listEgress({egressId:id}))[0];if(item&&[4,5,6].includes(item.status))throw Error('actual Egress failed or aborted');return item?.status===1?item:null;},'actual ACTIVE IOInfo callback');

 if (topologyBarrier) {
   await sleep(3000);
   const staged = resolve(topologyBarrier, 'recording-ready.tmp');
   await writeFile(staged, JSON.stringify({ egressId: id, child, timestamp: Date.now() }));
   await rename(staged, resolve(topologyBarrier, 'recording-ready.json'));
   await wait(async () => {
     assert((await children()).includes(child), 'original handler exited during Redis fault');
     if (producerError) throw producerError;
     return stat(resolve(topologyBarrier, 'promoted')).catch(error => {
       if (error.code !== 'ENOENT') throw error;
     });
   }, 'actual Redis topology promotion and worker subscription recovery', 30000);
   frequency = 880;
   report.topology = { child_before: child, fault_recovered_at: Date.now(), published_hz: [440, 880] };
   await sleep(5500);
   assert((await children()).includes(child), 'original handler exited before public StopEgress');
   report.topology.child_before_stop = child;
 } else {
   await sleep(8000);
 }
 if(producerError)throw producerError;await egress.stopEgress(id);const terminal=await wait(async()=>{const item=(await egress.listEgress({egressId:id}))[0];if(item&&[4,5,6].includes(item.status))throw Error('real worker terminal error');return item?.status===3?item:null;},'actual COMPLETE callback');completed=true;assert.equal(terminal.error,'');assert.equal(terminal.errorCode,0);assert(BigInt(terminal.endedAt)>BigInt(terminal.startedAt));assert.equal(terminal.fileResults.length,1);const result=terminal.fileResults[0];assert(BigInt(result.size)>0n);assert(BigInt(result.duration)>0n);assert.equal(result.filename,env.EGRESS_OUTPUT_FILE);
 await wait(async()=>!(await children()).includes(child),'actual handler exit');const file=await readFile(env.EGRESS_OUTPUT_FILE);assert.equal(BigInt(file.length),BigInt(result.size));assert(file.length<=16*1024*1024);
 const probed=await exec('ffprobe',['-v','error','-show_entries','stream=codec_name,sample_rate,codec_type:format=format_name,duration','-of','json',env.EGRESS_OUTPUT_FILE],{maxBuffer:65536,timeout:15000});const probe=JSON.parse(probed.stdout);assert.equal(probe.streams.length,1);assert.equal(probe.streams[0].codec_type,'audio');assert.equal(probe.streams[0].codec_name,'opus');assert.equal(probe.streams[0].sample_rate,'48000');assert.equal(probe.format.format_name,'ogg');assert(Number(probe.format.duration)>=5);
 const decoded=await exec('ffmpeg',['-nostdin','-v','error','-i',env.EGRESS_OUTPUT_FILE,'-f','s16le','-ac','1','-ar','48000','pipe:1'],{encoding:'buffer',maxBuffer:16*1024*1024,timeout:15000});const pcm=decoded.stdout;assert.equal(pcm.length%2,0);const count=pcm.length/2;assert(count>=240000);let toneResult;
 if (topologyBarrier) {
   const before = tone(pcm, 48000, 96000, 440);
   const after = tone(pcm, count - 96000, count - 48000, 880);
   report.topology.decoded_before = before;
   report.topology.decoded_after = after;
   toneResult = after;
 } else {
   toneResult = tone(pcm, 0, count, 440);
 }
 await wait(()=>events.some(e=>e.event==='egress_started'&&e.egressInfo?.egressId===id)&&events.some(e=>e.event==='egress_ended'&&e.egressInfo?.egressId===id),'actual signed started and ended callbacks');assert.deepEqual(errors,[]);report.observations={actual_handler:true,publisher_outbound_rtp:outbound,submitted_pcm_frames:frames,active_complete:true,handler_exited:true,file_size:file.length,file_sha256:sha(file),format:probe.format.format_name,codec:'opus',duration_seconds:Number(probe.format.duration),decoded_samples:count,rms:toneResult.rms,tone_power_ratio:toneResult.ratio,signed_events:events.filter(e=>e.egressInfo?.egressId===id).map(e=>e.event)};report.status='PASS';
}catch(e){report.status='FAIL';report.failure={name:e.name,code:e.code??null,message_sha256:sha(String(e.message))};if(topologyBarrier)console.error(e);}
finally{
 if(id&&!completed)try{await egress.stopEgress(id);}catch{report.cleanup_errors.push('owned Egress stop failed');}stop=true;try{if(producer)await producer;}catch{report.cleanup_errors.push('PCM producer exit failed');}try{await room.disconnect();}catch{report.cleanup_errors.push('RTC disconnect failed');}try{if(track)await track.close(false);if(source)await source.close();}catch{report.cleanup_errors.push('RTC audio handles failed');}try{await rtc.dispose();}catch{report.cleanup_errors.push('RTC runtime dispose failed');}try{await rooms.deleteRoom(roomName);}catch{report.cleanup_errors.push('owned room delete failed');}for(const s of sockets)s.destroy();await new Promise(r=>sink.close(r));if(report.cleanup_errors.length)report.status='FAIL';await writeFile(env.EGRESS_MEDIA_REPORT,JSON.stringify(report,null,2)+'\n',{mode:0o600});if(report.status==='PASS')await rm(env.EGRESS_OUTPUT_FILE,{force:true});if(report.status!=='PASS')process.exitCode=1;
}
