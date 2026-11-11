/** Positive Run-only HTTPS ENV consumer. Coordinator starts/stops the real server. */
import assert from 'node:assert/strict';
import https from 'node:https';
import http from 'node:http';
import {readFile,writeFile} from 'node:fs/promises';
import {X509Certificate} from 'node:crypto';
import {createRequire} from 'node:module';
const require=createRequire(new URL('../package.json',import.meta.url));
const {AccessToken}=require('livekit-server-sdk');
for(const key of ['HTTPS_CONSUMER_URL','HTTPS_CONSUMER_HTTP_URL','HTTPS_CONSUMER_CA','HTTPS_CONSUMER_CERT','HTTPS_CONSUMER_OUTPUT'])assert.ok(process.env[key],key);
const ca=await readFile(process.env.HTTPS_CONSUMER_CA);
const expected=new X509Certificate(await readFile(process.env.HTTPS_CONSUMER_CERT)).fingerprint256;
function request(url,body,token){return new Promise((resolve,reject)=>{
 const secure=new URL(url).protocol==='https:';
 const req=(secure?https:http).request(url,{method:body?'POST':'GET',...(secure?{ca}:{}),headers:body?{'content-type':'application/json',authorization:`Bearer ${token}`}:{},timeout:8000},res=>{
  const peer=secure?{authorized:res.socket.authorized,peer:res.socket.getPeerCertificate().fingerprint256,protocol:res.socket.getProtocol()}:{};
  const chunks=[];let size=0;
  res.on('data',b=>{size+=b.length;if(size>1048576){res.destroy(new Error('response bound'));return;}chunks.push(b);});
  res.on('error',reject);
  res.on('end',()=>resolve({status:res.statusCode,body:Buffer.concat(chunks).toString(),...peer}));
 });req.on('error',reject);req.on('timeout',()=>req.destroy(new Error('actual HTTPS consumer deadline')));req.end(body);
});}
const plain=await request(process.env.HTTPS_CONSUMER_HTTP_URL);
assert.equal(plain.status,200);assert.equal(plain.body,'OK');
const native=await request(process.env.HTTPS_CONSUMER_URL);
assert.equal(native.status,200);assert.equal(native.body,'OK');assert.equal(native.authorized,true);assert.equal(native.peer,expected);
const token=new AccessToken(process.env.LIVEKIT_API_KEY||'devkey',process.env.LIVEKIT_API_SECRET||'secret');token.addGrant({roomList:true});
const api=await request(new URL('/twirp/livekit.RoomService/ListRooms',process.env.HTTPS_CONSUMER_URL),'{}',await token.toJwt());
assert.equal(api.status,200);assert.ok(Array.isArray(JSON.parse(api.body).rooms));assert.equal(api.authorized,true);assert.equal(api.peer,expected);
const result={status:'PASS',scope:'Actual Run invocation consuming registered HTTPS ENV, nondefault bound port, certificate/key files, HTTP enabled and authenticated HTTPS API. No browser/WSS/TURN/performance or arbitrary raw cert path claim.',http_status:plain.status,https_status:native.status,https_api_status:api.status,verified_peer_sha256:expected,tls_protocol:native.protocol,certificate_verification:'normal CA/name validation; no bypass'};
await writeFile(process.env.HTTPS_CONSUMER_OUTPUT,JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result));
