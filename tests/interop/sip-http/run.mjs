/** NOT_RUN isolated F07 HTTP source-order differential. Never starts a SIP worker. */
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {createServer} from 'node:http';
import {readFile,mkdir,writeFile} from 'node:fs/promises';
import {dirname,join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {randomBytes} from 'node:crypto';
const here=dirname(fileURLToPath(import.meta.url));
const require=createRequire(resolve(process.env.INTEROP_PACKAGE||join(here,'../package.json')));
const {AccessToken}=require('livekit-server-sdk'),{CreateSIPParticipantRequest,TransferSIPParticipantRequest}=require('@livekit/protocol');
const urls=[process.env.REFERENCE_URL,process.env.LIVEKIT_URL];assert.ok(urls.every(Boolean),'two actual separately configured HTTP servers required');
const backend=process.env.SIP_HTTP_BACKEND;assert.ok(['none','redis'].includes(backend),'explicit SIP_HTTP_BACKEND=none|redis required');
const keys=[process.env.REFERENCE_API_KEY||process.env.LIVEKIT_API_KEY||'devkey',process.env.LIVEKIT_API_KEY||'devkey'];
const secrets=[process.env.REFERENCE_API_SECRET||process.env.LIVEKIT_API_SECRET||'secret',process.env.LIVEKIT_API_SECRET||'secret'];
const run= randomBytes(5).toString('hex'),room=`f07-sip-http-${run}`,missingRoom=`${room}-missing`,identity='f07-real-nonsip';
const output=resolve(process.env.SIP_HTTP_OUTPUT||join(here,'output','sip-http.json'));
const withParticipant=process.env.SIP_HTTP_WITH_PARTICIPANT!=='false';
const evidence={profile:'f07-sip-http',status:'RUNNING',backend,startedAt:new Date().toISOString(),scope:'Actual public JSON/protobuf decode+auth+validation+real room/participant lookups. No backend worker success, SIP media or synthetic PSRPC worker. Exact official errors are measured rather than prescribed.',cases:[],setup:[],differences:[],coverage:{normalParticipant:'NOT_RUN'},cleanup:[]};
const ownedRooms=[false,false],pages=[],contexts=[];let host,browser;
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
const redact=text=>{let out=String(text).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<jwt>');for(const s of secrets)if(s)out=out.replaceAll(s,'<secret>');return out;};
const json=(value)=>JSON.stringify(value,(_,v)=>typeof v==='bigint'?String(v):v);
async function token(server,kind,scope=room){
 if(kind==='none')return undefined;
 const t=new AccessToken(keys[server],secrets[server],{identity:kind==='join'?identity:undefined});
 if(kind==='sipadmin')t.addSIPGrant({admin:true});
 if(['call','calladmin','wrongroom'].includes(kind))t.addSIPGrant({call:true});
 if(['video','calladmin','wrongroom','roomcrud'].includes(kind))t.addGrant({room:kind==='wrongroom'?`${scope}-wrong`:scope,roomAdmin:true,roomCreate:true,roomList:true});
 if(kind==='join')t.addGrant({room:scope,roomJoin:true,canSubscribe:false,canPublish:false});
 return t.toJwt();
}
async function request(server,service,method,body,{codec='json',auth='none',scope=room,timeout=12000,headers:extra={}}={}){
 const jwt=await token(server,auth,scope),headers={'content-type':codec==='protobuf'?'application/protobuf':'application/json',...(jwt?{authorization:`Bearer ${jwt}`} :{}),...extra};
 const start=performance.now();
 const res=await fetch(`${urls[server].replace(/\/$/,'')}/twirp/${service}/${method}`,{method:'POST',headers,body,signal:AbortSignal.timeout(timeout)});
 const bytes=new Uint8Array(await res.arrayBuffer()),text=new TextDecoder('utf-8',{fatal:true}).decode(bytes);let parsed;try{parsed=JSON.parse(text);}catch{parsed=null;}
 return {status:res.status,contentType:res.headers.get('content-type'),body:parsed,rawText:text,elapsedMs:Math.round(performance.now()-start)};
}
const comparable=r=>({status:r.status,contentType:r.contentType,body:r.body,rawText:r.body===null?r.rawText:undefined});
function compare(label,reference,rust){try{assert.deepEqual(comparable(rust),comparable(reference));}catch{evidence.differences.push(label);}}
async function observe(id,method,payload,{auth='none',codecs=['json','protobuf'],scope=room,...options}={}){
 const Type=method==='CreateSIPParticipant'?CreateSIPParticipantRequest:TransferSIPParticipantRequest;
 for(const codec of codecs){const body=codec==='json'?json(payload):new Type(payload).toBinary();const result={id:`${id}/${codec}`,method,codec,auth,reference:null,rust:null};evidence.cases.push(result);
  for(let server=0;server<2;server++){try{result[server===0?'reference':'rust']=await request(server,'livekit.SIP',method,body,{codec,auth,scope,...options});}catch(e){result[server===0?'reference':'rust']={transportFailure:{name:e.name,message:redact(e.message)}};}}
  if(result.reference.transportFailure||result.rust.transportFailure)evidence.differences.push(`${result.id}:transport failure (including accidental worker wait)`);else{compare(result.id,result.reference,result.rust);if(result.reference.status===200||result.rust.status===200)evidence.differences.push(`${result.id}:unexpected successful call without worker`);}
 }
}
async function rawObserve(id,method,body,codec,auth='none'){
 const row={id,method,codec,auth,reference:await request(0,'livekit.SIP',method,body,{codec,auth}),rust:await request(1,'livekit.SIP',method,body,{codec,auth})};evidence.cases.push(row);compare(id,row.reference,row.rust);assert.notEqual(row.reference.status,200,'malformed fixture cannot be successful');assert.notEqual(row.rust.status,200,'malformed fixture cannot be successful');
}
async function roomCall(server,method,payload){return request(server,'livekit.RoomService',method,json(payload),{auth:'roomcrud'});}
try{
 // Decode order must still be exercised with both absent and fully valid grants.
 for(const method of ['CreateSIPParticipant','TransferSIPParticipant']){
  for(const auth of ['none','calladmin'])for(const [name,body,codec] of [['truncated-json','{','json'],['scalar-json','42','json'],['invalid-protobuf',new Uint8Array([255]),'protobuf'],['invalid-utf8',new Uint8Array(method==='CreateSIPParticipant'?[0x12,1,0xff]:[0x0a,1,0xff]),'protobuf']])await rawObserve(`${method}/${name}/${auth}`,method,body,codec,auth);
  await observe(`${method}/empty-noauth`,method,{});
  await observe(`${method}/empty-calladmin`,method,{}, {auth:'calladmin'});
 }
 const createBase={sipNumber:'123',sipCallTo:'456',roomName:room};
 for(const auth of ['none','video','sipadmin'])await observe(`create/grant-${auth}`,'CreateSIPParticipant',createBase,{auth});
 // With Redis these fail validation or actual trunk lookup before any worker RPC.
 // Without Redis, source store presence precedes the same validation failures.
 for(const [name,payload] of [['missing-all',{}],['missing-callee',{sipNumber:'123',roomName:room}],['missing-room',{sipNumber:'123',sipCallTo:'456'}],['callee-full-uri',{...createBase,sipCallTo:'sip:x@example.invalid'}],['bad-inline-host',{...createBase,trunk:{hostname:'sip:invalid'}}],['bad-header',{...createBase,headers:{'bad key':'v'}}],['bad-display',{...createBase,displayName:'"'}],['missing-real-trunk',{...createBase,sipTrunkId:'ST_f07_missing'}]])await observe(`create/${name}`,'CreateSIPParticipant',payload,{auth:'calladmin'});
 if(backend==='none')await observe('create/valid-disconnected-source-store-preflight','CreateSIPParticipant',createBase,{auth:'calladmin'});
 const transferBase={roomName:room,participantIdentity:identity,transferTo:'sip:target@example.invalid'};
 for(const auth of ['none','video','sipadmin','call','wrongroom'])await observe(`transfer/grant-${auth}`,'TransferSIPParticipant',transferBase,{auth});
 await observe('transfer/missing-room-pre-auth','TransferSIPParticipant',{}, {auth:'none'});
 await observe('transfer/missing-identity-pre-auth','TransferSIPParticipant',{roomName:room},{auth:'none'});
 for(const [name,payload] of [['empty-target',{...transferBase,transferTo:''}],['unsupported-target',{...transferBase,transferTo:'https://example.invalid'}],['bad-header-before-owner',{...transferBase,headers:{'bad key':'v'}}]])await observe(`transfer/${name}`,'TransferSIPParticipant',payload,{auth:'calladmin'});
 // These old unit cases intentionally prescribe no Internal/NotFound outcome.
 // Actual official/Rust code, message and metadata decide the disputed order.
 for(const uri of ['sip:p@example.invalid','<sips:p@example.invalid>','tel:+123','<tel:+123>'])await observe(`transfer/absent-room/${uri}`,'TransferSIPParticipant',{...transferBase,roomName:missingRoom,transferTo:uri},{auth:'calladmin',scope:missingRoom});
 for(let server=0;server<2;server++){const created=await roomCall(server,'CreateRoom',{name:room});evidence.setup.push({server,step:'actual-create-room',response:created});assert.equal(created.status,200);ownedRooms[server]=true;}
 await observe('transfer/existing-room-missing-participant','TransferSIPParticipant',transferBase,{auth:'calladmin'});
 if(withParticipant){
  const {chromium}=require('playwright'),sdk=await readFile(require.resolve('livekit-client')),html=await readFile(join(here,'participant.html'));
  host=createServer((req,res)=>{if(!['/','/sdk.js'].includes(req.url)){res.writeHead(404);res.end();return;}res.writeHead(200,{'content-type':req.url==='/'?'text/html':'text/javascript'});res.end(req.url==='/'?html:sdk);});await new Promise(r=>host.listen(0,'127.0.0.1',r));browser=await chromium.launch({channel:process.env.BROWSER_CHANNEL||'chrome',headless:true,args:['--no-sandbox']});
  for(let server=0;server<2;server++){const context=await browser.newContext();contexts.push(context);const page=await context.newPage();pages.push(page);await page.goto(`http://127.0.0.1:${host.address().port}`);const jwt=await token(server,'join');await page.evaluate(args=>peer.join(args),{url:urls[server].replace(/^http/,'ws'),token:jwt});
   let found;const end=Date.now()+10000;while(Date.now()<end){found=await roomCall(server,'GetParticipant',{room,identity});if(found.status===200)break;await sleep(50);}assert.equal(found?.status,200,'real normal RTC participant must exist');assert.ok(!Object.hasOwn(found.body.attributes||{},'sip.callID'),'fixture must be an actual nonSIP participant');evidence.setup.push({server,step:'actual-normal-RTC-participant',response:found});}
  evidence.coverage.normalParticipant='ACTUAL_CONNECTED_BOTH_SERVERS';
  await observe('transfer/real-normal-participant-no-sip-session','TransferSIPParticipant',transferBase,{auth:'calladmin'});
  await observe('transfer/normal-participant-wrong-admin-room','TransferSIPParticipant',transferBase,{auth:'wrongroom'});
  for(const page of pages)await page.evaluate(()=>peer.close());
 }
 for(let server=0;server<2;server++){const removed=await roomCall(server,'DeleteRoom',{room});evidence.setup.push({server,step:'actual-delete-room',response:removed});assert.equal(removed.status,200);ownedRooms[server]=false;}
 await observe('transfer/after-real-room-delete','TransferSIPParticipant',transferBase,{auth:'calladmin'});
 evidence.status=evidence.differences.length?'FAIL':withParticipant?'PASS':'PARTIAL';if(evidence.status==='FAIL'||evidence.status==='PARTIAL')process.exitCode=1;
}catch(e){evidence.status='FAIL';evidence.error={name:e.name,message:redact(e.message)};process.exitCode=1;}
finally{
 for(const page of pages)try{await page.evaluate(()=>peer.close());}catch{}
 for(const context of contexts)try{await context.close();}catch(e){evidence.cleanup.push('browser-context-close-failed');}
 try{await browser?.close();}catch(e){evidence.cleanup.push('browser-close-failed');}
 if(host)await new Promise(r=>host.close(r));
 for(let server=0;server<2;server++)if(ownedRooms[server])try{const r=await roomCall(server,'DeleteRoom',{room});if(r.status!==200)evidence.cleanup.push(`owned-room-${server}-delete-status-${r.status}`);}catch{evidence.cleanup.push(`owned-room-${server}-delete-transport-failed`);}
 if(evidence.cleanup.length){evidence.status='FAIL';process.exitCode=1;}
 evidence.finishedAt=new Date().toISOString();await mkdir(dirname(output),{recursive:true});await writeFile(output,json(evidence)+'\n');console.log(json({status:evidence.status,cases:evidence.cases.length,differences:evidence.differences,coverage:evidence.coverage}));
}
