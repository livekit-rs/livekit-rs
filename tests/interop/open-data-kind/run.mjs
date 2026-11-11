// Public binary Twirp SendData -> actual unchanged browser SDK/native channels.
import assert from 'node:assert/strict';import {createRequire} from 'node:module';import {createServer} from 'node:http';import {readFile,writeFile,realpath} from 'node:fs/promises';import {randomUUID,createHash} from 'node:crypto';import {dirname,join,basename} from 'node:path';import {fileURLToPath} from 'node:url';import {execFile} from 'node:child_process';import {promisify} from 'node:util';
const exec=promisify(execFile);
const here=dirname(fileURLToPath(import.meta.url));const require=createRequire(join(process.env.INTEROP_ROOT??join(here,'..'),'package.json'));
const {chromium}=require('playwright'),{AccessToken,RoomServiceClient}=require('livekit-server-sdk'),{SendDataRequest,DataPacket}=require('@livekit/protocol');
const url=process.env.LIVEKIT_URL,key=process.env.LIVEKIT_API_KEY,secret=process.env.LIVEKIT_API_SECRET,single=process.env.SINGLE_PC!=='false';assert.ok(url&&key&&secret&&process.env.RESULT);
const api=new RoomServiceClient(url.replace(/^ws/,'http'),key,secret,{requestTimeout:10});const room='open-data-'+randomUUID();let browser,server,created=false,resultOwned=false;const pages=[];const report={status:'RUNNING',single,observations:[],cleanupErrors:[]};const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function until(fn,msg,ms=10000){const deadline=Date.now()+ms;while(Date.now()<deadline){if(await fn()){assert.ok(Date.now()<=deadline,msg);return;}await sleep(20);}throw Error(msg);}
const snapshot=page=>page.evaluate(()=>fixture.snapshot());
async function packageIdentity(name,version){
 const entry=require.resolve(name);let parent=dirname(entry),file,bytes,data;
 for(;;){const candidate=join(parent,'package.json');try{const b=await readFile(candidate),d=JSON.parse(b);if(d.name===name){file=candidate;bytes=b;data=d;break;}}catch(error){if(error.code!=='ENOENT')throw error;}const next=dirname(parent);assert.notEqual(next,parent,'actual resolved package missing');parent=next;}
 assert.equal(data.version,version,'actual installed package version');return {name,version:data.version,entry,package:file,entrySha256:createHash('sha256').update(await readFile(entry)).digest('hex'),packageSha256:createHash('sha256').update(bytes).digest('hex')};
}
async function processProof(){
 const pid=Number(process.env.SERVER_PID);assert(Number.isSafeInteger(pid)&&pid>1);process.kill(pid,0);
 const binary=await realpath(process.env.SERVER_BIN),config=await realpath(process.env.SERVER_CONFIG);let argv,observed;
 try{observed=await realpath(`/proc/${pid}/exe`);assert.equal(observed,binary);argv=(await readFile(`/proc/${pid}/cmdline`)).toString().split('\0').filter(Boolean);}
 catch(error){if(error.code!=='ENOENT')throw error;const ps=(await exec('/bin/ps',['-p',String(pid),'-o','comm='],{timeout:10000,maxBuffer:65536})).stdout.trim();assert(ps);if(ps.startsWith('/'))assert.equal(await realpath(ps),binary);else assert.equal(ps,basename(binary));observed=ps;argv=(await exec('/bin/ps',['-p',String(pid),'-o','command='],{timeout:10000,maxBuffer:65536})).stdout.trim().split(/\s+/);}
 // Canonical owned launch only; never persist argv or possible credentials.
 const args=argv.slice(1);assert(args.length===2&&args[0]==='--config'&&args[1]===config||args.length===1&&args[0]==='--config='+config,'only actual canonical --config launch is accepted');
 const bytes=await readFile(config),origin=new URL(url);assert(['http:','https:','ws:','wss:'].includes(origin.protocol));assert(['127.0.0.1','localhost','[::1]'].includes(origin.hostname));
 const port=Number(origin.port||(['https:','wss:'].includes(origin.protocol)?443:80));assert.deepEqual(bytes.toString().split(/\r?\n/).filter(l=>l.startsWith('port:')),['port: '+port]);
 const rb=await readFile(process.env.SERVER_EXECUTION_RECEIPT),receipt=JSON.parse(rb),digest=b=>createHash('sha256').update(b).digest('hex');
 const binarySha=digest(await readFile(binary)),configSha=digest(bytes);assert.equal(receipt.pid,pid);assert.equal(receipt.livekit_url,url);assert.equal(receipt.listener_port,port);assert.equal(receipt.binary_sha256,binarySha);assert.equal(receipt.startup_config_sha256,configSha);assert.equal(receipt.livekit_environment_cleared,true);assert.equal(receipt.no_config_overrides,true);
 const sockets=(await exec(process.env.LSOF||'lsof',['-a','-p',String(pid),'-iTCP:'+port,'-sTCP:LISTEN','-Fpn'],{timeout:10000,maxBuffer:65536})).stdout;assert(sockets.split('\n').includes('p'+pid));assert(sockets.split('\n').some(l=>l.startsWith('n')&&new RegExp(':'+port+'$').test(l)));
 return {pid,executableObserved:observed,binarySha256:binarySha,configSha256:configSha,receiptSha256:digest(rb),listenerSha256:digest(sockets),listenerPort:port};
}
try{
 await writeFile(process.env.RESULT,'',{flag:'wx',mode:0o600});resultOwned=true;
 report.processBefore=await processProof();report.packages=await Promise.all([packageIdentity('livekit-client','2.23.0'),packageIdentity('livekit-server-sdk','2.19.2'),packageIdentity('@livekit/protocol','1.52.1'),packageIdentity('playwright','1.64.1')]);
 const entry=require.resolve('livekit-client'),sdk=await readFile(entry),html=await readFile(join(here,'client.html'));report.sdk={entrySha256:createHash('sha256').update(sdk).digest('hex'),lockSha256:createHash('sha256').update(await readFile(join(process.env.INTEROP_ROOT??join(here,'..'),'package-lock.json'))).digest('hex')};
 server=createServer((req,res)=>{res.writeHead(200,{'content-type':req.url==='/sdk.js'?'text/javascript':'text/html'});res.end(req.url==='/sdk.js'?sdk:html);});await new Promise(r=>server.listen(0,'127.0.0.1',r));await api.createRoom({name:room});created=true;
 browser=await chromium.launch({channel:process.env.BROWSER_CHANNEL??'chrome',headless:true,args:['--no-sandbox']});
 for(const identity of ['target','non-target']){const page=await browser.newPage();pages.push(page);await page.goto('http://127.0.0.1:'+server.address().port);const token=new AccessToken(key,secret,{identity});token.addGrant({roomJoin:true,room,canPublishData:true});await page.evaluate(a=>fixture.connect(a),{url,token:await token.toJwt(),single});}
 await until(async()=>{const bound=await Promise.all(pages.map(p=>p.evaluate(()=>fixture.bind())));return bound.every(Boolean);},'actual native reliable+lossy downstream channels missing',30000);
 for(const page of pages){const t=await snapshot(page);assert.equal(t.connection,'connected');assert.equal(t.publisherNative,true);assert.equal(t.unique,single?1:2);assert.equal(t.subscriberNative,!single);if(!single)assert.equal(t.same,false);}
 const admin=new AccessToken(key,secret,{ttl:'10m'});admin.addGrant({roomAdmin:true,room});const auth='Bearer '+await admin.toJwt();
 // Sequential producer; each target packet must be observed on its actual
 // channel before next call. No reliable-vs-lossy cross-channel order claim.
 for(const [seq,kind] of [0,1,99,-1,-2147483648,2147483647].entries()){
  const payload=new TextEncoder().encode(JSON.stringify({seq,kind})),topic='controlled-open-kind';
  const req=new SendDataRequest({room,kind,data:payload,destinationIdentities:['target'],topic});
  const response=await fetch(url.replace(/^ws/,'http')+'/twirp/livekit.RoomService/SendData',{method:'POST',headers:{authorization:auth,'content-type':'application/protobuf'},body:req.toBinary(),signal:AbortSignal.timeout(10000)});assert.equal(response.status,200);await response.arrayBuffer();
  await until(async()=>{const s=await snapshot(pages[0]);return s.native.length>=seq+1&&s.received.length>=seq+1;},'public API packet not actually received by SDK');
  const a=await snapshot(pages[0]),b=await snapshot(pages[1]);assert.equal(a.native.length,seq+1);assert.equal(a.received.length,seq+1);assert.equal(b.native.length,0);assert.equal(b.received.length,0);
  const n=a.native[seq],packet=DataPacket.fromBinary(Buffer.from(n.base64,'base64')),d=a.received[seq];assert.equal(n.label,kind===0?'_reliable':'_lossy');assert.equal(n.ordered,kind===0);assert.equal(n.maxRetransmits,kind===0?null:0);assert.equal(packet.kind,kind);assert.equal(d.kind,kind);assert.equal(d.fromServer,true);assert.equal(d.topic,topic);assert.deepEqual(d.value,{seq,kind});
  report.observations.push({kind,label:n.label,ordered:n.ordered,maxRetransmits:n.maxRetransmits,wireKind:packet.kind,sdkKind:d.kind,targetReceipts:1,otherReceipts:0});
 }
 // Bounded late-duplicate/untargeted observation, not an unlimited guarantee.
 await sleep(1000);for(const [index,page]of pages.entries()){const s=await snapshot(page);assert.equal(s.errors.length,0);assert.equal(s.received.length,index===0?6:0);assert.equal(s.native.length,index===0?6:0);assert.equal(s.connection,'connected');}
 report.processAfter=await processProof();assert.equal(report.processAfter.binarySha256,report.processBefore.binarySha256);assert.equal(report.processAfter.configSha256,report.processBefore.configSha256);
 if(process.env.REFERENCE_RESULTS){const ref=JSON.parse(await readFile(process.env.REFERENCE_RESULTS,'utf8'));assert.equal(ref.status,'REFERENCE_CAPTURED');assert.equal(ref.single,single);assert.deepEqual(report.observations,ref.observations);report.status='PASS';}else report.status='REFERENCE_CAPTURED';
}catch(error){report.status='FAIL';report.failure={name:error.name,message:String(error.message)};process.exitCode=1;}
finally{for(const page of pages){try{await page.evaluate(()=>fixture.close());}catch{report.cleanupErrors.push('disconnect-peer');}}if(browser){try{await browser.close();}catch{report.cleanupErrors.push('browser-close');}}if(created){try{await api.deleteRoom(room);}catch{report.cleanupErrors.push('delete-room');}}if(server){try{await new Promise((r,j)=>server.close(e=>e?j(e):r()));}catch{report.cleanupErrors.push('fixture-http-close');}}if(report.cleanupErrors.length){report.status='FAIL';process.exitCode=1;}if(resultOwned)await writeFile(process.env.RESULT,JSON.stringify(report,null,2)+'\n',{mode:0o600});}
