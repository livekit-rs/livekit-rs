// Public wire observation only. Never classify invalid source config as parity.
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {resolve} from 'node:path';
import {writeFile} from 'node:fs/promises';
import {randomUUID,createHash} from 'node:crypto';
const env=process.env,require=createRequire(resolve(env.INTEROP_PROJECT,'package.json'));
const {RoomServiceClient,AccessToken}=require('livekit-server-sdk');
const WebSocket=require('ws');
const service=new RoomServiceClient(env.SIGNAL_OWNER_URL,'devkey','secret');
const room='signal-zero-observer-'+randomUUID(),result={status:'RUNNING',scope:'one actual public WebSocket with zero/negative connect_attempts; OBSERVATION ONLY, no expected success invented',room};
let ws,timer;
try{
 const created=await service.createRoom({name:room,nodeId:env.SIGNAL_OWNER_NODE});result.room_sid=created.sid;
 await writeFile(env.SIGNAL_CONTROL,JSON.stringify({room})+'\n');
 const token=new AccessToken('devkey','secret',{identity:'zero-observer'});token.addGrant({roomJoin:true,room,canSubscribe:true});
 result.frames=[];const start=Date.now();const jwt=await token.toJwt();
 await new Promise(resolve=>{
  let finished=false;const finish=reason=>{if(finished)return;finished=true;result.finish_reason=reason;resolve();};
  ws=new WebSocket(env.LIVEKIT_URL.replace(/^http/,'ws')+'/rtc?protocol=17&auto_subscribe=1&sdk=js&access_token='+encodeURIComponent(jwt));
  ws.once('upgrade',response=>{result.http_status=response.statusCode;});
  ws.on('message',(data,binary)=>{const bytes=Buffer.from(data);if(result.frames.length<10)result.frames.push({binary,length:bytes.length,sha256:createHash('sha256').update(bytes).digest('hex'),first64_hex:bytes.subarray(0,64).toString('hex'),elapsed_ms:Date.now()-start});});
  ws.once('unexpected-response',(_,response)=>{
   result.http_status=response.statusCode;let bytes=Buffer.alloc(0);
   response.on('data',part=>{bytes=Buffer.concat([bytes,part]);if(bytes.length>65536){ws.terminate();finish('bounded-http-body');}});
   response.once('end',()=>{result.http_body_sha256=createHash('sha256').update(bytes).digest('hex');result.http_body=bytes.subarray(0,1024).toString();finish('http-response');});
  });
  ws.once('error',error=>{result.websocket_error=String(error.message);});
  ws.once('close',(code,reason)=>{result.websocket_close={code,reason:reason.toString(),elapsed_ms:Date.now()-start};finish('websocket-close');});
  timer=setTimeout(()=>{result.observation_deadline_ms=5000;ws.terminate();finish('bounded-observation');},5000);
 });
 assert(result.http_status!==undefined,'no actual HTTP handshake observed; cannot classify setup failure as source behavior');
 result.status='OBSERVED_NOT_ACCEPTANCE';
}catch(error){result.status='FAIL';result.error=String(error.message).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'[JWT]');}
finally{
 clearTimeout(timer);if(ws&&ws.readyState!==WebSocket.CLOSED)ws.terminate();
 try{await service.deleteRoom(room);}catch(error){result.cleanup_error=String(error.message);}
 await writeFile(env.SIGNAL_RESULT,JSON.stringify(result,null,2)+'\n');
 await writeFile(env.SIGNAL_PHASE,JSON.stringify({phase:'zero-observed',room,room_sid:result.room_sid})+'\n');
}
if(result.status!=='OBSERVED_NOT_ACCEPTANCE')process.exitCode=1;
