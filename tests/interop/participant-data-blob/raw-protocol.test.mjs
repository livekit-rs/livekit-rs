/** Source-bound unit checks; actual server acceptance remains the runner gate. */
import test from 'node:test';
import assert from 'node:assert/strict';
import {SignalResponse} from '@livekit/protocol';
import {varint,field,integer,fields,readKey,readBlob,readResponse,comparePayload} from './raw-protocol.mjs';
const bytes=(...parts)=>Buffer.concat(parts),text=s=>Buffer.from(s);
const store=(id,key)=>field(30,bytes(integer(1,id),field(2,key)));
test('actual source case13 wrong-kind key remains unknown instead of Buf premature EOF',()=>{
 const key=Buffer.from('0a01670807','hex'),wire=store(128,key);
 assert.throws(()=>SignalResponse.fromBinary(wire),/premature EOF/);
 const response=readResponse(wire);assert.equal(response.requestId,128);
 assert.equal(response.tree.values.key.values.generic,'g');
 assert.deepEqual(response.tree.values.key.unknown,['0807']);
 comparePayload(response,readResponse(store(71,key)),128);
});
test('unknown cohorts retain wire kind, original varint bytes, group ownership and order',()=>{
 const a=readKey(Buffer.from('0a0167a0068100ad0601000000a9060100000000000000b3060801b406','hex'));
 assert.deepEqual(a.unknown,['a0068100','ad0601000000','a9060100000000000000','b3060801b406']);
 const fixed32=readKey(Buffer.from('0a0167ad0601000000','hex'));
 const fixed64=readKey(Buffer.from('0a0167a9060100000000000000','hex'));
 assert.notDeepEqual(fixed32.unknown,fixed64.unknown,'Go identity can alias fixed numeric text but emitted wire cohorts remain distinct');
 assert.notDeepEqual(readKey(bytes(integer(100,1),integer(101,2))).unknown,readKey(bytes(integer(101,2),integer(100,1))).unknown);
});
test('same singular message merges, explicit defaults clear, oneof switches discard old child',()=>{
 const schemaName=field(1,text('name')),encoding=field(2,field(2,text('custom')));
 const merged=readKey(bytes(field(2,schemaName),field(2,bytes(encoding,field(1,Buffer.alloc(0))))));
 assert.equal(merged.oneofs.key,'schemaId');assert.equal(merged.values.schemaId.values.name,undefined);
 assert.equal(merged.values.schemaId.values.encoding.values.custom,'custom');
 const switched=readKey(bytes(field(2,bytes(schemaName,integer(100,9))),field(1,text('g')),field(2,Buffer.alloc(0))));
 assert.deepEqual(switched.values.schemaId,{values:{},oneofs:{},unknown:[]});assert.equal(switched.values.generic,undefined);
 const defaultOneof=readKey(field(1,Buffer.alloc(0)));assert.equal(defaultOneof.oneofs.key,'generic');assert.equal(defaultOneof.values.generic,'');
});
test('nested unknowns and repeated response branch merge are compared without erasure',()=>{
 const key=bytes(field(2,bytes(field(1,text('n')),integer(90,3))),integer(91,4));
 const blob=bytes(field(1,key),field(2,Buffer.from([0,255,1])),integer(92,5));
 const tree=readBlob(blob);assert.deepEqual(tree.unknown,[integer(92,5).toString('hex')]);assert.deepEqual(tree.values.key.unknown,[integer(91,4).toString('hex')]);
 assert.deepEqual(tree.values.key.values.schemaId.unknown,[integer(90,3).toString('hex')]);
 const merged=readResponse(bytes(store(71,key),field(30,integer(1,0))));assert.equal(merged.requestId,0);assert.ok(merged.tree.values.key);
 assert.throws(()=>comparePayload(readResponse(store(128,key)),readResponse(store(71,field(1,text('different')))),128));
 assert.throws(()=>comparePayload(readResponse(store(128,key)),readResponse(store(71,key)),127));
});
test('bounded parser rejects truncation, invalid groups, tags and excessive fields',()=>{
 for(const hex of ['00','0a07','800000','0b140c','0b0801','0c','0e','ffffffffffffffffff02'])assert.throws(()=>fields(Buffer.from(hex,'hex')),hex);
 assert.throws(()=>fields(Buffer.alloc((2<<20)+1)));
 assert.throws(()=>fields(Buffer.concat(Array.from({length:8193},()=>integer(100,1)))));
 let group=Buffer.alloc(0);for(let i=0;i<101;i++)group=bytes(Buffer.from([11]),group,Buffer.from([12]));assert.throws(()=>fields(group));
 assert.equal(varint(128).toString('hex'),'8001');
});
