/** Real process restart with persistent native Redis SIP configuration. */
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {createRequire} from 'node:module';
import {mkdtemp,writeFile,mkdir,open} from 'node:fs/promises';
import {join,dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
import {tmpdir} from 'node:os';
import {createHash,randomBytes} from 'node:crypto';
const here=dirname(fileURLToPath(import.meta.url));
const {AccessToken}=createRequire(join(here,'package.json'))('livekit-server-sdk');
const apiKey='f04-persist-'+process.pid, apiSecret=randomBytes(32).toString('base64url');
const token=new AccessToken(apiKey,apiSecret);token.addSIPGrant({admin:true});const jwt=await token.toJwt();
const crash=process.env.SIP_PERSIST_STOP_MODE==='crash';
assert.ok(!process.env.SIP_PERSIST_STOP_MODE||['crash','graceful'].includes(process.env.SIP_PERSIST_STOP_MODE));
const evidence={profile:crash?'functional-sip-crash-persistence':'functional-sip-process-persistence',status:'RUNNING',scope:'Actual native Redis-backed SIP config survives an actual server process restart. Broker restart/HA/room routing/calls are not covered.',cases:[]};
const directory=await mkdtemp(join(tmpdir(),'livekit-rs-f04-persist-'));
const digest=value=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
assert.ok(!process.env.SIP_PERSIST_ONLY||['reference','rust'].includes(process.env.SIP_PERSIST_ONLY));
const definitions=[['reference',process.env.SIP_PERSIST_GO_BIN,process.env.REFERENCE_REDIS_URL,18990],['rust',process.env.SIP_PERSIST_RUST_BIN,process.env.LIVEKIT_REDIS_URL,28990]];
assert.ok(definitions.every(([,bin,redis])=>bin&&redis),'actual binaries and dedicated Redis URLs required');
for(const [name,bin,redisUrl,port] of definitions.filter(([name])=>!process.env.SIP_PERSIST_ONLY||name===process.env.SIP_PERSIST_ONLY)){
 let child,log,exit;const row={server:name,exits:[]};evidence.cases.push(row);
 const redis=new URL(redisUrl);assert.equal(redis.protocol,'redis:');
 const config=join(directory,name+'.yaml');
 await writeFile(config,`port: ${port}\nbind_addresses: [127.0.0.1]\nkeys:\n  ${JSON.stringify(apiKey)}: ${JSON.stringify(apiSecret)}\nredis:\n  address: ${JSON.stringify(redis.hostname+':'+(redis.port||'6379'))}\n  username: ${JSON.stringify(decodeURIComponent(redis.username))}\n  password: ${JSON.stringify(decodeURIComponent(redis.password))}\n  db: ${Number(redis.pathname.slice(1)||0)}\nrtc:\n  node_ip: 127.0.0.1\n  tcp_port: ${port+1}\n  udp_port: ${port+2}\n  use_external_ip: false\nlogging:\n  level: info\n`);
 const call=async(method,body)=>{const response=await fetch(`http://127.0.0.1:${port}/twirp/livekit.SIP/${method}`,{method:'POST',headers:{authorization:'Bearer '+jwt,'content-type':'application/json'},body:JSON.stringify(body)});return {status:response.status,body:await response.json()}};
 async function stop(){
  if(!child)return;
  const process=child;child=undefined;
  if(process.exitCode===null&&process.signalCode===null){const done=new Promise(resolve=>process.once('exit',(code,signal)=>resolve({code,signal})));process.kill(crash?'SIGKILL':'SIGINT');const timer=setTimeout(()=>process.kill('SIGKILL'),10000);exit=await done;clearTimeout(timer);}else exit={code:process.exitCode,signal:process.signalCode};
  row.exits.push(exit);await log?.close();log=undefined;if(crash){assert.equal(exit.signal,'SIGKILL','explicit crash profile requires actual crash');}else{assert.equal(exit.code,0,'server must stop normally');}
 }
 async function start(index){log=await open(join(directory,`${name}-${index}.log`),'w');child=spawn(bin,['--dev','--config',config],{stdio:['ignore',log.fd,log.fd]});let ready=false;const deadline=Date.now()+15000;while(Date.now()<deadline&&child.exitCode===null){try{const r=await fetch(`http://127.0.0.1:${port}/`);if(r.status===200){ready=true;break}}catch{}await new Promise(resolve=>setTimeout(resolve,100));}assert.ok(ready,'actual process must become ready');}
 let trunk,rule;
 try{
  await start(1);
  const before=await call('ListSIPInboundTrunk',{});assert.equal(before.status,200);assert.deepEqual(before.body.items,[],'dedicated fresh Redis namespace required');
  trunk=await call('CreateSIPInboundTrunk',{trunk:{name:'persisted-fixture',numbers:['+12025550790'],metadata:'{"persistent":true}'}});assert.equal(trunk.status,200);assert.ok(trunk.body.sip_trunk_id);
  rule=await call('CreateSIPDispatchRule',{dispatch_rule:{name:'persisted-rule',trunk_ids:[trunk.body.sip_trunk_id],rule:{dispatch_rule_direct:{room_name:'persistent-room'}}}});assert.equal(rule.status,200);assert.ok(rule.body.sip_dispatch_rule_id);
  await stop();await start(2);
  const loaded=await call('GetSIPInboundTrunk',{sip_trunk_id:trunk.body.sip_trunk_id});assert.equal(loaded.status,200);assert.deepEqual(loaded.body.trunk,trunk.body);
  const rules=await call('ListSIPDispatchRule',{dispatch_rule_ids:[rule.body.sip_dispatch_rule_id]});assert.equal(rules.status,200);assert.deepEqual(rules.body.items,[rule.body]);
  row.same_trunk_after_restart=true;row.same_rule_after_restart=true;row.stored_trunk_sha256=digest(trunk.body);row.stored_rule_sha256=digest(rule.body);
 }catch(error){evidence.status='FAIL';row.error={name:error.name,message:error.message};process.exitCode=1;}
 finally{
  if(child){try{if(rule?.status===200){const r=await call('DeleteSIPDispatchRule',{sip_dispatch_rule_id:rule.body.sip_dispatch_rule_id});assert.equal(r.status,200)}if(trunk?.status===200){const r=await call('DeleteSIPTrunk',{sip_trunk_id:trunk.body.sip_trunk_id});assert.equal(r.status,200)}row.owned_records_removed=true;}catch(error){evidence.status='FAIL';row.cleanup_error=error.name;process.exitCode=1;}
   try{await stop()}catch(error){evidence.status='FAIL';row.stop_error=error.name;process.exitCode=1;}}
 }
}
if(evidence.status!=='FAIL')evidence.status='PASS';
const output=process.env.SIP_PERSIST_EVIDENCE||join(here,'output/functional-sip-persistence.json');await mkdir(dirname(output),{recursive:true});await writeFile(output,JSON.stringify(evidence,null,2)+'\n');console.log(JSON.stringify({status:evidence.status,servers:evidence.cases.length}));
