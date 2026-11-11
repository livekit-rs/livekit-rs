/** Real generated Go IOInfo caller → actual main server → Redis → unchanged SDK
 * and authenticated HTTP WebhookReceiver. Synthetic callback data is explicit:
 * this proves no recording, external media worker, or real media output. */
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {createRequire} from 'node:module';
import {writeFile} from 'node:fs/promises';
const require=createRequire(new URL('../../interop/package.json',import.meta.url));
const {RoomServiceClient,EgressClient,WebhookReceiver}=require('livekit-server-sdk');
assert.equal(process.env.IO_DEDICATED_BROKERS,'1','requires coordinator-owned disposable Redis brokers; callback records persist until broker shutdown');
const receiver=new WebhookReceiver('devkey','secret');
const events=[],errors=[],runs=[];
const sink=createServer(async(req,res)=>{
 try {const parts=[];for await(const b of req)parts.push(b);const body=Buffer.concat(parts).toString();
  const event=await receiver.receive(body,req.headers.authorization);
  events.push(event);res.end('ok');
 } catch(error){errors.push(error.message);res.writeHead(401).end('invalid');}
});
await new Promise(resolve=>sink.listen(0,'127.0.0.1',resolve));
const sinkUrl=`http://127.0.0.1:${sink.address().port}/worker`;
const oracle=process.env.IO_GO_ORACLE;assert.ok(oracle);
const urls=[process.env.REFERENCE_URL,process.env.LIVEKIT_URL];assert.ok(urls.every(Boolean));
const redis=[process.env.IO_REFERENCE_REDIS_URL,process.env.IO_RUST_REDIS_URL];assert.ok(redis.every(Boolean));
async function callback(index,command){
 // execFile does not provide stdin; the genuine generated caller consumes JSONL.
 const {spawn}=await import('node:child_process');
 const child=spawn(oracle,['--mode','call'],{env:{...process.env,IO_REDIS_URL:redis[index]},stdio:['pipe','pipe','pipe']});
 let stdout='',stderr='';child.stdout.on('data',b=>stdout+=b);child.stderr.on('data',b=>stderr+=b);
 const closed=new Promise((resolve,reject)=>{child.once('error',reject);child.once('close',(code,signal)=>resolve({code,signal}));});
 child.stdin.end(JSON.stringify(command)+'\n');
 const timer=setTimeout(()=>child.kill('SIGTERM'),15000);
 const status=await closed;clearTimeout(timer);assert.equal(status.code,0,`real caller failed (${status.signal})`);
 const rows=stdout.trim().split('\n').map(JSON.parse);assert.equal(rows.length,1);assert.equal(rows[0].status,'OK');
 return {case:command.case,actualExitCode:status.code,status:rows[0].status};
}
try {
 for(let index=0;index<2;index++){
  const roomName=`f06-main-io-${process.pid}-${index}`,egressId=`EG_F06_main_${process.pid}_${index}`;
  const rooms=new RoomServiceClient(urls[index],'devkey','secret');
  const egress=new EgressClient(urls[index],'devkey','secret');
  const room=await rooms.createRoom({name:roomName});const row={server:index===0?'reference':'rust',steps:[]};runs.push(row);
  try {
   const info={egressId,roomName,roomId:room.sid,status:'EGRESS_STARTING',track:{roomName,trackId:'TR_synthetic_callback_only',webhooks:[{url:sinkUrl,signingKey:'devkey'}]}};
   row.steps.push(await callback(index,{case:'create',op:'CreateEgress',message:info}));
   let listed=await egress.listEgress({egressId});assert.equal(listed.length,1);assert.equal(listed[0].status,0);assert.equal(listed[0].roomId,room.sid);
   row.steps.push({case:'official-sdk-list-starting',status:'PASS'});
   for(const [status,label]of [['EGRESS_ACTIVE','active'],['EGRESS_COMPLETE','complete']]){
    row.steps.push(await callback(index,{case:label,op:'UpdateEgress',message:{...info,status}}));
    listed=await egress.listEgress({egressId});assert.equal(listed.length,1);assert.equal(listed[0].status,label==='active'?1:3);
   }
   const deadline=Date.now()+10000;
   while(events.filter(e=>e.egressInfo?.egressId===egressId).length<3 && Date.now()<deadline)await new Promise(r=>setTimeout(r,20));
   const matched=events.filter(e=>e.egressInfo?.egressId===egressId);
   assert.deepEqual(matched.map(e=>e.event),['egress_started','egress_updated','egress_ended']);
   for(const e of matched){assert.equal(e.room.sid,room.sid);assert.equal(e.room.name,roomName);assert.ok(e.id);assert.ok(e.createdAt>0n);}
   row.steps.push({case:'actual-official-webhook-auth-ordered',status:'PASS',events:matched.map(e=>e.event)});
  } finally {await rooms.deleteRoom(roomName);}
 }
 assert.deepEqual(errors,[]);
 const report={status:'PASS',scope:'Synthetic genuine IOInfo callbacks through actual main server and Redis, unchanged SDK list and signed HTTP webhooks; no real recording/media inferred',runs,webhookAuthFailures:errors};
 await writeFile(process.env.IO_MAIN_OUTPUT,JSON.stringify(report,null,2)+'\n');console.log('PASS actual main IOInfo/SDK/HTTP webhook graph');
} finally {await new Promise(resolve=>sink.close(resolve));}
