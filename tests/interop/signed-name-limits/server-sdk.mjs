/** Unchanged pinned Server SDK calls against two actual normal-limit servers. */
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {writeFile} from 'node:fs/promises';
import {RoomServiceClient} from 'livekit-server-sdk';

const urls=[process.env.REFERENCE_URL,process.env.LIVEKIT_URL];
assert.ok(urls.every(Boolean));assert.ok(process.env.SIGNED_SDK_OUTPUT);
const name=`signed-sdk-${randomUUID()}`,badName='é'.repeat(129);
const report={status:'RUNNING',scope:'Actual unchanged Server SDK2.19.2 RoomService CRUD and signed-limit envelope; not browser/media/full API compatibility.',observations:[],cleanupErrors:[]};
async function scenario(url){
 const api=new RoomServiceClient(url,process.env.LIVEKIT_API_KEY||'devkey',process.env.LIVEKIT_API_SECRET||'secret');
 let owned=false,badOwned=false;const rows=[];
 try{
  assert.deepEqual(await api.listRooms([name]),[]);assert.deepEqual(await api.listRooms([badName]),[]);
  const room=await api.createRoom({name,metadata:'before',emptyTimeout:30});owned=true;
  assert.match(room.sid,/^RM_/);assert.equal(room.name,name);assert.equal(room.metadata,'before');rows.push({step:'create',metadata:room.metadata});
  const updated=await api.updateRoomMetadata(name,'after');assert.equal(updated.sid,room.sid);assert.equal(updated.metadata,'after');
  const listed=await api.listRooms([name]);assert.equal(listed.length,1);assert.equal(listed[0].sid,room.sid);assert.equal(listed[0].metadata,'after');rows.push({step:'update-list',metadata:listed[0].metadata});
  let rejected=false;try{await api.createRoom({name:badName});badOwned=true;}catch(error){rejected=true;assert.equal(error.status,400);assert.equal(error.code,'invalid_argument');assert.equal(error.message,'room name length exceeds limits: max length 256');assert.equal(error.metadata?.argument,undefined);rows.push({step:'rejected-name',status:error.status,code:error.code,message:error.message,metadata:error.metadata??{}});}
  assert.ok(rejected);assert.deepEqual(await api.listRooms([badName]),[]);
  await api.deleteRoom(name);owned=false;assert.deepEqual(await api.listRooms([name]),[]);rows.push({step:'delete-absence'});return rows;
 }finally{for(const target of [...(owned?[name]:[]),...(badOwned?[badName]:[])])try{await api.deleteRoom(target);assert.deepEqual(await api.listRooms([target]),[]);}catch(error){report.cleanupErrors.push(String(error.message));}}
}
try{const go=await scenario(urls[0]),rust=await scenario(urls[1]);assert.deepEqual(rust,go);assert.deepEqual(report.cleanupErrors,[]);report.observations={go,rust};report.status='PASS_SCOPED';}
catch(error){report.status='FAIL';report.error={name:error.name,message:error.message};process.exitCode=1;}
finally{await writeFile(process.env.SIGNED_SDK_OUTPUT,JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify({status:report.status}));}
