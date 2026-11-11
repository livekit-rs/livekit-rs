/** Real prestarted local Go/Rust configured-limit profiles. No SDK Blob API claim. */
import assert from 'node:assert/strict';
import {randomUUID,createHash} from 'node:crypto';
import {writeFile} from 'node:fs/promises';
import {AccessToken,RoomServiceClient} from 'livekit-server-sdk';
import {RawBlobPeer} from './raw-peer.mjs';
import {field,readKey,readBlob} from './raw-protocol.mjs';
const profiles={
 'key-negative':{enable_participant_data_blob:true,limit:{max_data_blob_key_length:-1}},
 'key-utf8-three':{enable_participant_data_blob:true,limit:{max_data_blob_key_length:3}},
 'key-unlimited':{enable_participant_data_blob:true,limit:{max_data_blob_key_length:0}},
 'custom-negative':{enable_participant_data_blob:true,limit:{max_data_blob_key_length:0,max_data_track_custom_encoding_length:-1}},
 'custom-unlimited':{enable_participant_data_blob:true,limit:{max_data_blob_key_length:0,max_data_track_custom_encoding_length:0}},
 'quota-three':{enable_participant_data_blob:true,limit:{max_data_blobs_size:3}},
 'quota-unlimited':{enable_participant_data_blob:true,limit:{max_data_blobs_size:0}},
 'store-disabled':{enable_participant_data_blob:false},
};
const profile=process.env.BLOB_LIMIT_PROFILE;assert.ok(Object.hasOwn(profiles,profile),'declare exactly one source profile');
const url=process.env.LIVEKIT_URL||'ws://127.0.0.1:7880',apiKey=process.env.LIVEKIT_API_KEY||'devkey',secret=process.env.LIVEKIT_API_SECRET||'secret';
const api=new RoomServiceClient(url.replace(/^ws/,'http'),apiKey,secret),room=`blob-limits-${randomUUID()}`;
const report={status:'RUNNING',profile,declaredConfig:profiles[profile],scope:'real authenticated WebSocket Blob Store/Get configured limits; no public SDK BlobAPI/media claim',steps:[],events:[],cleanup:[],cleanupErrors:[]};
if(process.env.BLOB_SERVER_CONFIG_SHA256){assert.match(process.env.BLOB_SERVER_CONFIG_SHA256,/^[0-9a-f]{64}$/);report.declaredServerConfigSha256=process.env.BLOB_SERVER_CONFIG_SHA256;}
const sha=b=>createHash('sha256').update(b).digest('hex'),safe=e=>String(e).replaceAll(secret,'<secret>').replace(/blob-limit-(?:writer|reader)-[0-9a-f-]+/g,'<owned-actor>').replace(/blob-limits-[0-9a-f-]+/g,'<owned-room>').replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g,'<token>');
const generic=s=>field(1,Buffer.from(s)),custom=s=>field(2,Buffer.concat([field(1,Buffer.from('x')),field(2,field(2,Buffer.from(s)))]));
let writer,reader,nextId=100,created=false;
async function connect(role){const identity=`blob-limit-${role}-${randomUUID()}`;const token=new AccessToken(apiKey,secret,{identity});token.addGrant({roomJoin:true,room,canPublish:false,canSubscribe:false,canPublishData:false});const peer=new RawBlobPeer({url,token:await token.toJwt(),identity,diagnostics:report.events});if(role==='writer')writer=peer;else reader=peer;await peer.join();return peer;}
function observed(label,response){assert.deepEqual(response.unknown,[]);assert.deepEqual(response.tree.unknown,[]);report.steps.push({label,type:response.type,requestId:response.requestId,...(response.type==='requestResponse'?{reason:response.tree.values.reason||0,message:response.tree.values.message||''}:{})});}
function error(label,response,reason,message){observed(label,response);assert.equal(response.type,'requestResponse',label);assert.equal(response.tree.values.reason,reason,label);assert.equal(response.tree.values.message,message,label);}
async function store(key,contents){const id=nextId++,blob=Buffer.concat([field(1,key),field(2,contents)]);const response=await writer.request(id,22,field(2,blob));assert.equal(response.requestId,id);return response;}
async function get(key,identity=writer.identity){const id=nextId++,response=await reader.request(id,23,Buffer.concat([field(2,Buffer.from(identity)),field(3,key)]));assert.equal(response.requestId,id);return response;}
async function absent(label,key){error(label,await get(key),1,'data blob not found');}
async function positive(label,key,contents){const stored=await store(key,contents);observed(label+'-store',stored);assert.equal(stored.type,'storeDataBlobResponse');assert.deepEqual(stored.tree.values.key,readKey(key));await retained(label,key,contents);}
async function retained(label,key,contents){const got=await get(key);observed(label+'-get',got);assert.equal(got.type,'getDataBlobResponse');assert.deepEqual(got.tree.values.blob,readBlob(Buffer.concat([field(1,key),field(2,contents)])));report.steps.at(-1).payload={length:contents.length,sha256:sha(contents),keyWireSha256:sha(key)};}
async function closePeer(role,peer){if(!peer)return;const closed=await peer.close();assert.equal(closed.code,1000);assert.equal(closed.clean,true);report.cleanup.push({role,closed:true,code:closed.code,clean:closed.clean});}
async function waitAbsent(){const deadline=Date.now()+10000;while((await api.listParticipants(room)).some(p=>p.identity===writer.identity)){assert.ok(Date.now()<deadline,'owned writer participant cleanup deadline');await new Promise(r=>setTimeout(r,20));}}
try{
 await api.createRoom({name:room});created=true;await connect('writer');await connect('reader');report.authenticatedJoins=2;
 const one=Buffer.from([1]),x=generic('x');
 switch(profile){
  case 'key-negative':error('signed-negative-key',await store(x,one),11,'data blob is invalid');await absent('rejected-key-no-write',x);error('key-before-empty-content',await store(x,Buffer.alloc(0)),11,'data blob is invalid');break;
  case 'key-utf8-three':{const key=generic('汉');assert.equal(Buffer.byteLength('汉'),3);await positive('three-UTF8-bytes',key,one);const bad=generic('汉a');error('four-UTF8-bytes-rejected',await store(bad,one),11,'data blob is invalid');await absent('rejected-long-key-no-write',bad);await retained('existing-key-retained',key,one);break;}
  case 'key-unlimited':{const value='汉'.repeat(86);assert.equal(Buffer.byteLength(value),258);await positive('zero-key-limit-258-bytes',generic(value),one);break;}
  case 'custom-negative':{const key=custom('a');error('signed-negative-custom',await store(key,one),11,'encoding identifier is empty or exceeds the maximum length');await absent('rejected-custom-no-write',key);break;}
  case 'custom-unlimited':{const value='é'.repeat(17);assert.equal(Buffer.byteLength(value),34);const key=custom(value);await positive('zero-custom-limit-34-bytes',key,one);const empty=custom('');error('explicit-empty-custom-still-invalid',await store(empty,one),11,'encoding identifier is empty or exceeds the maximum length');await absent('empty-custom-no-write',empty);await retained('valid-custom-retained',key,one);break;}
  case 'quota-three':{const initial=Buffer.from([1,2]);await positive('exact-three-byte-quota',x,initial);error('old-inclusive-replacement-rejected',await store(x,Buffer.from([3])),3,'async attribute definition exceeds limit');await retained('rejected-overwrite-retains-old-content',x,initial);break;}
  case 'quota-unlimited':{const contents=Buffer.from(Array.from({length:64001},(_,i)=>i%251));await positive('zero-quota-64002-combined-bytes',x,contents);break;}
  case 'store-disabled':{
   error('flag-before-absent-blob',await writer.request(nextId++,22,Buffer.alloc(0)),2,'data blob not enabled');
   error('flag-before-invalid-empty-key',await store(generic(''),one),2,'data blob not enabled');
   error('flag-before-invalid-custom',await store(custom(''),one),2,'data blob not enabled');
   error('get-key-before-missing-participant',await reader.request(nextId++,23,field(2,Buffer.from('absent'))),11,'data blob key is required');
   error('get-missing-participant-independent-flag',await get(x,'absent'),1,'participant not found');await absent('get-existing-participant-missing-entry',x);break;
  }
 }
 assert.ok(!writer.failure&&!reader.failure);
 await closePeer('writer',writer);await waitAbsent();report.cleanup.push({role:'writer',participantAbsent:true});error('writer-private-state-not-visible-after-close',await get(x),1,'participant not found');writer=undefined;
 await closePeer('reader',reader);reader=undefined;await api.deleteRoom(room);created=false;report.cleanup.push({roomDeleted:true});report.status='PASS_SCOPED';
}catch(error){report.status='FAIL';report.error=safe(error);process.exitCode=1;}
finally{
 for(const [role,peer] of [['writer',writer],['reader',reader]])if(peer){try{await closePeer(role,peer);}catch(e){report.cleanupErrors.push({role,error:safe(e)});report.status='FAIL';process.exitCode=1;}}
 if(created)try{await api.deleteRoom(room);report.cleanup.push({roomDeleted:true});}catch(e){report.cleanupErrors.push({role:'room',error:safe(e)});report.status='FAIL';process.exitCode=1;}
 for(const event of report.events)if(event.error)event.error=safe(event.error);
 await writeFile(process.env.BLOB_LIMIT_OUTPUT||'blob-limits.json',JSON.stringify(report,null,2)+'\n');
}
