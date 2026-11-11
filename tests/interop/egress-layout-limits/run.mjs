// Actual Go/Rust public HTTP fixture. No service startup, browser, SDK patch,
// recording worker success or media assertion is manufactured.
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {readFile,writeFile,realpath} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {resolve,dirname,join,basename} from 'node:path';
import {fileURLToPath} from 'node:url';
const here=dirname(fileURLToPath(import.meta.url));
const root=resolve(process.env.SOURCE_ROOT||'.');
const require=createRequire(join(root,'tests/interop/package.json'));
const {AccessToken,RoomServiceClient}=require('livekit-server-sdk');
const {EgressInfo}=require('@livekit/protocol');
const {RawBlobPeer}=await import(join(root,'tests/interop/participant-data-blob/raw-peer.mjs'));
const execute=promisify(execFile),url=process.env.LIVEKIT_URL,key=process.env.LIVEKIT_API_KEY,secret=process.env.LIVEKIT_API_SECRET;
assert(url&&key&&secret&&process.env.LAYOUT_PROFILE&&process.env.LAYOUT_REPORT&&process.env.LAYOUT_REDIS_PORT);
assert.equal(process.env.LAYOUT_REDIS_ISOLATED,'true','owned isolated source/Rust broker only');
const profiles={negative_name:{name:-1,metadata:64000},negative_name_and_metadata:{name:-1,metadata:3},unlimited_name:{name:0,metadata:64000},positive_name:{name:1,metadata:64000},metadata_only:{name:0,metadata:3}};
const profile=profiles[process.env.LAYOUT_PROFILE];assert(profile);
const room='layout-preflight-owned',identity='EG_layout_owned',missing='EG_layout_missing';
const api=new RoomServiceClient(url.replace(/^ws/,'http'),key,secret,{requestTimeout:10});
let peer,created=false;const report={status:'RUNNING',scope:'real public Egress.UpdateLayout -> stored fixture EgressInfo -> actual participant metadata; no StartEgress/recording/media claim',profile:process.env.LAYOUT_PROFILE,declaredLimits:profile,observations:[],cleanupErrors:[]};
const diag=[];
async function ownerProfileProvenance(){
 const names=['LAYOUT_SERVER_PID','LAYOUT_SERVER_BIN','LAYOUT_SERVER_CONFIG','LAYOUT_EXECUTION_RECEIPT'];for(const name of names)assert(process.env[name],`required ${name}`);
 const pid=Number(process.env.LAYOUT_SERVER_PID);assert(Number.isSafeInteger(pid)&&pid>1);process.kill(pid,0);
 const binary=await realpath(process.env.LAYOUT_SERVER_BIN),config=await realpath(process.env.LAYOUT_SERVER_CONFIG),digest=bytes=>createHash('sha256').update(bytes).digest('hex');
 let executable,argv;try{executable=await realpath(`/proc/${pid}/exe`);assert.equal(executable,binary);argv=(await readFile(`/proc/${pid}/cmdline`)).toString('utf8').split('\0');}
 catch(error){if(error.code!=='ENOENT')throw error;const {stdout}=await execute('/bin/ps',['-p',String(pid),'-o','comm='],{timeout:10000,maxBuffer:65536});executable=stdout.trim();assert(executable);if(executable.startsWith('/'))assert.equal(await realpath(executable),binary);else assert.equal(executable,basename(binary));argv=(await execute('/bin/ps',['-p',String(pid),'-o','command='],{timeout:10000,maxBuffer:65536})).stdout.trim().split(/\s+/);}
 // Actual argv is private; no tokens/config-body/credentials enter the report.
 assert(!argv.some(arg=>arg==='--config-body'||arg.startsWith('--config-body=')),'config-body would override file');
 const overridden=['--api.enable_psrpc_for_get_list_participants','--limit.max_participant_name_length','--limit.max_metadata_size'];
 assert(!argv.some(arg=>overridden.some(flag=>arg===flag||arg.startsWith(flag+'='))),'no generated profile flag overrides');
 const configFlags=argv.filter(arg=>arg==='--config'||arg.startsWith('--config='));assert.equal(configFlags.length,1);
 assert(argv.some((arg,i)=>arg==='--config'&&argv[i+1]===config||arg==='--config='+config),'actual process config must match');
 const configBytes=await readFile(config),lines=configBytes.toString('utf8').split(/\r?\n/);
 function section(name){const indexes=lines.flatMap((line,i)=>line===name+':'?[i]:[]);assert.equal(indexes.length,1,'canonical unique section '+name);const out=[];for(let i=indexes[0]+1;i<lines.length;i++){const line=lines[i];if(line&&!line.startsWith(' ')&&!line.startsWith('#'))break;if(line.trim()&&!line.trim().startsWith('#'))out.push(line);}return out;}
 assert.deepEqual(section('api'),['  enable_psrpc_for_get_list_participants: true'],'live owner API profile required');
 const limits=section('limit');for(const [name,value]of [['max_participant_name_length',profile.name],['max_metadata_size',profile.metadata]]){assert.equal(limits.filter(line=>line.trimStart().startsWith(name+':')).length,1);assert(limits.includes('  '+name+': '+value),'actual canonical limit value '+name);}
 const receiptBytes=await readFile(process.env.LAYOUT_EXECUTION_RECEIPT),receipt=JSON.parse(receiptBytes),binarySha=digest(await readFile(binary)),configSha=digest(configBytes);
 const endpoint=new URL(url);assert(['http:','https:','ws:','wss:'].includes(endpoint.protocol));assert(!endpoint.username&&!endpoint.password&&!endpoint.search&&!endpoint.hash);assert.equal(endpoint.pathname,'/');
 const listenerPort=Number(endpoint.port||(endpoint.protocol==='https:'||endpoint.protocol==='wss:'?443:80));const portLines=lines.filter(line=>line.startsWith('port:'));assert.equal(portLines.length,1);assert.equal(portLines[0],'port: '+listenerPort,'actual startup config API port matches target URL');
 assert.equal(receipt.livekit_url,url);assert.equal(receipt.listener_port,listenerPort);
 assert.equal(receipt.pid,pid);assert.equal(receipt.binary_sha256,binarySha);assert.equal(receipt.startup_config_sha256,configSha);
 assert.equal(receipt.api_owner_get,true);assert.equal(receipt.no_api_config_overrides,true);assert.equal(receipt.no_limit_config_overrides,true);assert.equal(receipt.environment_api_override_absent,true);assert.equal(receipt.environment_limit_overrides_absent,true);
 assert.deepEqual(receipt.effective_limits,{max_participant_name_length:profile.name,max_metadata_size:profile.metadata});
 return {pid_alive:true,pid,livekit_url:url,listener_port:listenerPort,actual_executable:executable,binary_sha256:binarySha,startup_config_sha256:configSha,coordinator_execution_receipt_sha256:digest(receiptBytes),owner_get_profile:true,root_receipted_env_override_absence:true};
}

async function redis(mode,id,wire){
 const args=[join(here,'redis-op.py'),'--mode',mode,'--port',process.env.LAYOUT_REDIS_PORT,'--db',process.env.LAYOUT_REDIS_DB||'2','--egress-id',id,'--room',room];
 if(wire)args.push('--wire-base64',Buffer.from(wire).toString('base64'));
 await execute(process.env.PYTHON||'python3',args,{timeout:5000,env:process.env});
}
async function update(id,layout){
 const token=new AccessToken(key,secret);token.addGrant({roomRecord:true});
 const response=await fetch(url.replace(/^ws/,'http').replace(/\/$/,'')+'/twirp/livekit.Egress/UpdateLayout',{method:'POST',headers:{Authorization:'Bearer '+await token.toJwt(),'Content-Type':'application/json'},body:JSON.stringify({egressId:id,layout}),signal:AbortSignal.timeout(10000)});
 const text=await response.text();const body=JSON.parse(text);const contentType=response.headers.get('content-type');
 if(response.status===200){
  // This exact isolated metadata row contains only synthetic fixture values.
  // Persist actual source bytes before field assertions; redact only the known
  // task-owned generated room SID, preserving raw SHA and original spelling.
  report.successCaptures??=[];report.successCaptures.push({status:response.status,contentType,rawBodySha256:createHash('sha256').update(text).digest('hex'),rawBodyWithOwnedRoomSidPlaceholder:text.replaceAll(report.privateSid,'<actual-owned-room-sid>')});
  const safeReport={...report};delete safeReport.privateSid;await writeFile(process.env.LAYOUT_REPORT,JSON.stringify(safeReport,null,2)+'\n',{mode:0o600});
  assert.equal(body.egress_id,identity);assert.equal(body.room_name,room);assert.equal(body.room_id,report.privateSid);body.room_id='<actual-owned-room-sid>';
 }
 return {status:response.status,contentType,body,rawBodySha256:createHash('sha256').update(text).digest('hex')};
}
try{
 report.provenance=await ownerProfileProvenance();
 const createdRoom=await api.createRoom({name:room});created=true;report.privateSid=createdRoom.sid;
 await redis('cleanup',missing);
 const noStore=await update(missing,'grid');assert.notEqual(noStore.status,200);report.observations.push({case:'missing_store_first',http:noStore});
 const row=new EgressInfo({egressId:identity,roomId:createdRoom.sid,roomName:room});
 await redis('write',identity,row.toBinary());
 // Raw Join is a real participant lifecycle; no SDK/RTC/media proof is claimed.
 const jwt=new AccessToken(key,secret,{identity});jwt.addGrant({roomJoin:true,room,canPublish:false,canSubscribe:false,canPublishData:false});
 peer=new RawBlobPeer({url:url.replace(/^http/,'ws'),token:await jwt.toJwt(),identity,diagnostics:diag});await peer.join();
 const before=await api.getParticipant(room,identity);
 const result=await update(identity,'grid');const after=await api.getParticipant(room,identity);
 assert.equal(after.sid,before.sid,'same actual participant generation');
 if(profile.name<0){assert.equal(result.status,400);assert.equal(result.body.code,'invalid_argument');assert.equal(result.body.msg,'name length exceeds limits -1');assert.equal(result.body.meta?.argument,'name length exceeds limits');assert.equal(after.metadata,before.metadata);assert.equal(after.name,before.name);assert.deepEqual(after.attributes,before.attributes);}
 else if(profile.metadata===3){assert.equal(result.status,400);assert.equal(result.body.code,'invalid_argument');assert.equal(result.body.meta?.argument,'metadata size exceeds limits');assert.equal(after.metadata,before.metadata);}
 else{assert.equal(result.status,200);assert.equal(after.metadata,JSON.stringify({layout:'grid'}));assert.equal(after.name,before.name);assert.deepEqual(after.attributes,before.attributes);}
 report.observations.push({case:'actual_participant',http:result,participant:{sameSid:true,nameUnchanged:after.name===before.name,attributesUnchanged:JSON.stringify(after.attributes)===JSON.stringify(before.attributes),metadata:after.metadata}});
 await peer.close();peer=undefined;
 // Current participant is actually absent; source name preflight still precedes
 // existence when configured signed negative, and must not invent a write.
 if(profile.name<0){
  if((await api.listParticipants(room)).some(p=>p.identity===identity))await api.removeParticipant(room,identity);
  const end=Date.now()+10000;let absentNow=false;
  while(Date.now()<end){if(!(await api.listParticipants(room)).some(p=>p.identity===identity)){absentNow=true;break;}await new Promise(r=>setTimeout(r,50));}
  assert(absentNow,'real participant must be absent before missing-identity preflight');
  const absent=await update(identity,'grid');assert.equal(absent.status,400);assert.equal(absent.body.meta?.argument,'name length exceeds limits');report.observations.push({case:'missing_participant_name_first',http:absent});}
 report.status='PASS_SCOPED';
}catch(error){report.status='FAIL';report.error=String(error).replace(/eyJ[^\s]+/g,'<token>');}
finally{
 if(peer)try{await peer.close();}catch{report.cleanupErrors.push('raw participant close');}
 try{await redis('cleanup',identity);await redis('cleanup',missing);}catch{report.cleanupErrors.push('owned egress store cleanup');}
 if(created)try{await api.deleteRoom(room);}catch{report.cleanupErrors.push('owned room delete');}
 delete report.privateSid;
 if(report.cleanupErrors.length)report.status='FAIL';
 await writeFile(process.env.LAYOUT_REPORT,JSON.stringify(report,null,2)+'\n',{mode:0o600});
 if(report.status!=='PASS_SCOPED')process.exitCode=1;
}
