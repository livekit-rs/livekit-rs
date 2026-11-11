/** Actual authenticated unchanged-SDK known-fields session + separate raw protocol actors.
 * NOT a public SDK Blob API acceptance claim; no compiled results are supplied.
 * Put alongside the existing interop scripts before coordinator execution.
 */
import assert from 'node:assert/strict';
import {createHash,randomUUID} from 'node:crypto';
import {createServer} from 'node:http';
import {readFile,writeFile,mkdir} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {dirname,join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {chromium} from 'playwright';
import {AccessToken,RoomServiceClient} from 'livekit-server-sdk';
import {SignalResponse} from '@livekit/protocol';
import {runRawBlobCases} from './raw-peer.mjs';
import {exerciseBlobResumeReplacement} from './resume-replacement.mjs';
const root=dirname(fileURLToPath(import.meta.url)),require=createRequire(import.meta.url);
const url=process.env.LIVEKIT_URL||'ws://127.0.0.1:7880';
const apiKey=process.env.LIVEKIT_API_KEY||'devkey',secret=process.env.LIVEKIT_API_SECRET||'secret';
assert.ok(['true','false'].includes(process.env.PARTICIPANT_BLOB_ENABLED),'declare the actual prestarted server flag');
const enabled=process.env.PARTICIPANT_BLOB_ENABLED==='true';
const sdk=await readFile(require.resolve('livekit-client'));
const version=JSON.parse(await readFile(join(dirname(require.resolve('livekit-client')),'../package.json'))).version;
assert.equal(version,'2.23.0');
const html=await readFile(join(root,'client.html'));
const host=createServer((req,res)=>{res.writeHead(200,{'content-type':req.url==='/sdk.js'?'text/javascript':'text/html'});res.end(req.url==='/sdk.js'?sdk:html);});
await new Promise(resolve=>host.listen(0,'127.0.0.1',resolve));
const api=new RoomServiceClient(url.replace(/^ws/,'http'),apiKey,secret);
const output=process.env.BLOB_OUTPUT||join(root,'output','data-blob.json');
const evidence={status:'RUNNING',profile:'authenticated-signaling-data-blob-known-fields',sdk:version,enabled,scope:'Known fields: unchanged SDK connection/internal sendRequest seam. Raw26: separate authenticated real WebSocket peers and lossless descriptor-specific conformance reader. No public SDK Blob API, HTTP Blob route, RTC/media or congestion proof.',cases:[]};
let browser,context,currentRoom;let nextId=100;
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
function safeError(error){return String(error).replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<token>').replace(/([?&](?:access_token|token|api_secret)=)[^&\s]+/gi,'$1<redacted>');}
try {
 browser=await chromium.launch({channel:process.env.BROWSER_CHANNEL||'chrome',headless:true,args:['--no-sandbox','--autoplay-policy=no-user-gesture-required']});
 evidence.browser=browser.version();
 for(const single of [false,true]) {
  const result={single,steps:[],status:'RUNNING'};evidence.cases.push(result);
  currentRoom=`blob-conformance-${randomUUID()}`;await api.createRoom({name:currentRoom});
  context=await browser.newContext();const pages=await Promise.all([0,1,2].map(()=>context.newPage()));
  const pending=new Map();const errors=[];
  for(const [client,page] of pages.entries()) page.on('websocket',socket=>socket.on('framereceived',frame=>{
   try {
    const response=typeof frame.payload==='string'?SignalResponse.fromJsonString(frame.payload):SignalResponse.fromBinary(frame.payload);
    if(!['storeDataBlobResponse','getDataBlobResponse','requestResponse'].includes(response.message.case))return;
    const value=response.message.value,id=value.requestId,key=`${client}:${id}`;
    if(pending.has(key))pending.get(key)({type:response.message.case,value});
   } catch(error){errors.push(safeError(error));}
  }));
  await Promise.all(pages.map(async(page,i)=>{
   await page.goto(`http://127.0.0.1:${host.address().port}`);
   const token=new AccessToken(apiKey,secret,{identity:`blob-${i}`});
   // Source has no extra data-publish or subscribe permission gate for Blob.
   token.addGrant({roomJoin:true,room:currentRoom,canPublish:false,canSubscribe:false,canPublishData:false});
   await page.evaluate(args=>blobFixture.connect(args),{url,token:await token.toJwt(),single});
  }));
  async function request(client,type,value) {
   const requestId=nextId++,key=`${client}:${requestId}`;
   let timer;
   const result=new Promise((resolve,reject)=>{pending.set(key,resolve);timer=setTimeout(()=>reject(new Error('actual Blob response timeout')),10000);});
   try {await pages[client].evaluate(message=>blobFixture.signal(message),{case:type,value:{...value,requestId}});return await result;}
   finally {clearTimeout(timer);pending.delete(key);}
  }
  const generic=value=>({key:{case:'generic',value}});
  async function expectError(client,type,value,reason,message) {
   const response=await request(client,type,value);assert.equal(response.type,'requestResponse');assert.equal(response.value.reason,reason);assert.equal(response.value.message,message);return response;
  }
  // RequestResponse enum values are obtained from actual generated protocol, not
  // HTTP/Twirp code names or a new endpoint's authorization semantics.
  const {RequestResponse_Reason:R}=await import('@livekit/protocol');
  await expectError(1,'getDataBlobRequest',{participantIdentity:'blob-0'},R.INVALID_REQUEST,'data blob key is required');
  await expectError(1,'getDataBlobRequest',{participantIdentity:'absent',key:generic('absent')},R.NOT_FOUND,'participant not found');
  await expectError(1,'getDataBlobRequest',{participantIdentity:'blob-0',key:generic('absent')},R.NOT_FOUND,'data blob not found');
  result.steps.push('Get is available independently of Store flag and validates key before participant lookup');
  if(!enabled) {
   await expectError(0,'storeDataBlobRequest',{},R.NOT_ALLOWED,'data blob not enabled');
   result.steps.push('disabled Store default is not advertised as Blob support');
  } else {
   const key=generic('schema-定义'),contents=Uint8Array.from({length:257},(_,i)=>(i*31)%256);
   const stored=await request(0,'storeDataBlobRequest',{blob:{key,contents}});
   assert.equal(stored.type,'storeDataBlobResponse');assert.deepEqual(stored.value.key.key,key.key);
   let got=await request(1,'getDataBlobRequest',{participantIdentity:'blob-0',key});assert.equal(got.type,'getDataBlobResponse');assert.deepEqual(Buffer.from(got.value.blob.contents),Buffer.from(contents));
   const replacement=Uint8Array.from([255,0,7]);await request(0,'storeDataBlobRequest',{blob:{key,contents:replacement}});
   got=await request(1,'getDataBlobRequest',{participantIdentity:'blob-0',key});assert.deepEqual(Buffer.from(got.value.blob.contents),Buffer.from(replacement));
   const schemaKey={key:{case:'schemaId',value:{name:'typed-schema',encoding:{value:{case:'custom',value:'fixture/custom'}}}}};
   const schemaStore=await request(0,'storeDataBlobRequest',{blob:{key:schemaKey,contents}});assert.equal(schemaStore.type,'storeDataBlobResponse');
   got=await request(1,'getDataBlobRequest',{participantIdentity:'blob-0',key:schemaKey});assert.deepEqual(Buffer.from(got.value.blob.contents),Buffer.from(contents));
   result.payload={length:contents.length,sha256:createHash('sha256').update(contents).digest('hex')};
   result.steps.push('cross-participant exact binary get, overwrite, generic/schema key separation without publish/subscribe grants');
   try {result.lifecycle=await exerciseBlobResumeReplacement({context,hostUrl:`http://127.0.0.1:${host.address().port}`,url,apiKey,secret,roomName:currentRoom,single,api,readerRequest:(type,value)=>request(1,type,value)});}
   catch(error){if(error.lifecycleReport)result.lifecycle=error.lifecycleReport;throw error;}
   // Fresh third participant proves the official DEFAULT 64000B key+value quota.
   // A different configured limit needs its own declared profile, not a softened assertion.
   const quotaKey=generic('quota'),quota=Uint8Array.from({length:63995},(_,i)=>i%251);
   const quotaStore=await request(2,'storeDataBlobRequest',{blob:{key:quotaKey,contents:quota}});assert.equal(quotaStore.type,'storeDataBlobResponse');
   await expectError(2,'storeDataBlobRequest',{blob:{key:quotaKey,contents:Uint8Array.from([1])}},R.LIMIT_EXCEEDED,'async attribute definition exceeds limit');
   got=await request(1,'getDataBlobRequest',{participantIdentity:'blob-2',key:quotaKey});assert.deepEqual(Buffer.from(got.value.blob.contents),Buffer.from(quota));
   result.steps.push('exact default key+value quota, old-value-inclusive overwrite rejection preserves prior content');
   await pages[0].evaluate(()=>blobFixture.close());
   const deadline=Date.now()+10000;while((await api.listParticipants(currentRoom)).some(p=>p.identity==='blob-0')){assert.ok(Date.now()<deadline);await sleep(20);}
   await expectError(1,'getDataBlobRequest',{participantIdentity:'blob-0',key},R.NOT_FOUND,'participant not found');
   result.steps.push('actual publisher leave removes Blob visibility');
   await runRawBlobCases({url,apiKey,secret,room:currentRoom,result});
   result.steps.push('all26 raw binary cases use separate actual authenticated WebSocket peers, including invalid/no-write cases and exact nested unknown cohorts');

  }
  assert.deepEqual(errors,[]);result.status='PASS';
  await Promise.all(pages.map(p=>p.evaluate(()=>blobFixture.close())));
  await context.close();context=undefined;await api.deleteRoom(currentRoom);currentRoom=undefined;
 }
 evidence.status='PASS_SCOPED';
} catch(error){evidence.status='FAIL';evidence.error=safeError(error);process.exitCode=1;}
finally {
 evidence.cleanupErrors=[];
 async function cleanup(name,operation){try{await operation();}catch(error){evidence.cleanupErrors.push({operation:name,error:safeError(error)});evidence.status='FAIL';process.exitCode=1;}}
 if(context)await cleanup('owned-browser-context',()=>context.close());
 if(currentRoom)await cleanup('owned-fixture-room',()=>api.deleteRoom(currentRoom));
 if(browser)await cleanup('owned-browser',()=>browser.close());
 await cleanup('owned-http-host',()=>new Promise((resolve,reject)=>host.close(error=>error?reject(error):resolve())));
 await mkdir(dirname(output),{recursive:true});await writeFile(output,JSON.stringify(evidence,null,2)+'\n');
 console.log(JSON.stringify({status:evidence.status,cases:evidence.cases.length,output}));
}
