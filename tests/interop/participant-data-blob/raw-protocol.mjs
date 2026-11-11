/** Bounded descriptor-specific wire reader for fixed LiveKit Blob responses.
 * Known-number/wrong-kind fields are UNKNOWN, as fixed generated Go requires.
 * Unknown original tag+value bytes, order and nested message ownership are kept.
 * This is a protocol conformance consumer, never a modified SDK decoder.
 */
import assert from 'node:assert/strict';
const LIMIT=2<<20,MAX_FIELDS=8192,MAX_DEPTH=100;
const utf8=new TextDecoder('utf-8',{fatal:true});
export function varint(value){value=BigInt(value);assert.ok(value>=0n&&value<=0xffffffffffffffffn);const out=[];while(value>=128n){out.push(Number((value&127n)|128n));value>>=7n;}out.push(Number(value));return Buffer.from(out);}
export function field(number,bytes){return Buffer.concat([varint((number<<3)|2),varint(bytes.length),bytes]);}
export function integer(number,value){return Buffer.concat([varint(number<<3),varint(value)]);}
function readVarint(buffer,state){let value=0n;for(let shift=0n;shift<70n;shift+=7n){assert.ok(state.at<buffer.length,'truncated wire varint');const b=buffer[state.at++];assert.ok(shift!==63n||b<=1,'wire varint overflow');value|=BigInt(b&127)<<shift;if(b<128)return value;}throw new Error('wire varint overflow');}
function take(buffer,state,length){assert.ok(Number.isSafeInteger(length)&&length>=0&&state.at+length<=buffer.length,'truncated wire value');const value=buffer.subarray(state.at,state.at+length);state.at+=length;return value;}
function scan(buffer,state,depth){
 assert.ok(depth>0,'wire group recursion limit');assert.ok(++state.fields<=MAX_FIELDS,'wire field count limit');const start=state.at,tag=readVarint(buffer,state),number=Number(tag>>3n),wire=Number(tag&7n);assert.ok(number>0&&number<=0x1fffffff,'illegal field number');const valueStart=state.at;let value,payload;
 if(wire===0)value=readVarint(buffer,state);
 else if(wire===1)value=take(buffer,state,8);
 else if(wire===2){const n=readVarint(buffer,state);assert.ok(n<=BigInt(LIMIT),'wire bytes length bound');payload=take(buffer,state,Number(n));value=payload;}
 else if(wire===3){const begin=state.at;for(;;){assert.ok(state.at<buffer.length,'unterminated group');const end=state.at,child=scan(buffer,state,depth-1);if(child.wire===4){assert.equal(child.number,number,'group end mismatch');payload=buffer.subarray(begin,end);break;}}}
 else if(wire===4){}else if(wire===5)value=take(buffer,state,4);else throw new Error('illegal wire kind');
 return {number,wire,value,payload,raw:buffer.subarray(start,state.at),rawValue:buffer.subarray(valueStart,state.at)};
}
export function fields(bytes,depth=MAX_DEPTH){const buffer=Buffer.from(bytes);assert.ok(buffer.length<=LIMIT,'response frame size bound');const state={at:0,fields:0},out=[];while(state.at<buffer.length){const f=scan(buffer,state,depth);assert.notEqual(f.wire,4,'unexpected group end');out.push(f);}return out;}
const string=(name,oneof)=>({name,type:'string',oneof}),message=(name,schema,oneof)=>({name,type:'message',schema,oneof}),number=(name,type='u32',oneof)=>({name,type,oneof});
const Encoding={1:number('wellKnown','i32','value'),2:string('custom','value')};
const Schema={1:string('name'),2:message('encoding',Encoding)};
const Key={1:string('generic','key'),2:message('schemaId',Schema,'key')};
const Blob={1:message('key',Key),2:{name:'contents',type:'bytes'}};
const Store={1:number('requestId'),2:message('key',Key)};
const Get={1:number('requestId'),2:message('blob',Blob)};
const ErrorResponse={1:number('requestId'),2:number('reason','i32'),3:string('message')};
const Participant={1:string('sid'),2:string('identity')};
const Join={2:message('participant',Participant)};
function empty(){return {values:{},oneofs:{},unknown:[]};}
function accepted(spec,wire){return wire===(spec.type==='u32'||spec.type==='i32'?0:2);}
function fill(schema,bytes,target,depth){assert.ok(depth>0,'message recursion limit');for(const f of fields(bytes,depth)){
 const spec=schema[f.number];
 if(!spec||!accepted(spec,f.wire)){target.unknown.push(f.raw.toString('hex'));continue;}
 if(spec.oneof){const old=target.oneofs[spec.oneof];if(old!==spec.name&&old!==undefined)delete target.values[old];target.oneofs[spec.oneof]=spec.name;}
 if(spec.type==='message'){
  const child=target.values[spec.name]||empty();fill(spec.schema,f.payload,child,depth-1);target.values[spec.name]=child;
 }else{
  let value;
  if(spec.type==='string')value=utf8.decode(f.payload);
  else if(spec.type==='bytes')value=f.payload.toString('hex');
  else if(spec.type==='u32')value=Number(BigInt.asUintN(32,f.value));
  else value=Number(BigInt.asIntN(32,f.value));
  const zero=value===0||value==='';
  if(!spec.oneof&&zero)delete target.values[spec.name];else target.values[spec.name]=value;
 }
 }return target;}
export function readKey(bytes){return fill(Key,bytes,empty(),MAX_DEPTH);}
export function readBlob(bytes){return fill(Blob,bytes,empty(),MAX_DEPTH);}
export function readResponse(bytes){
 let selected;const unknown=[];
 const known=new Set([1,2,3,4,5,6,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31]);
 for(const f of fields(bytes)){
  const wire=f.number===18?0:2;
  if(!known.has(f.number)||f.wire!==wire){unknown.push(f.raw.toString('hex'));continue;}
  // Every accepted different root oneof replaces the old branch. Repeated
  // occurrences of the SAME singular message merge, including explicit clears.
  const schema=f.number===1?Join:f.number===22?ErrorResponse:f.number===30?Store:f.number===31?Get:undefined;
  const previous=selected?.number===f.number?selected.tree:undefined;
  selected={number:f.number,type:f.number===1?'join':f.number===8?'leave':f.number===22?'requestResponse':f.number===30?'storeDataBlobResponse':f.number===31?'getDataBlobResponse':'other',tree:schema?fill(schema,f.payload,previous||empty(),MAX_DEPTH):undefined};
 }
 assert.ok(selected,'signal frame has no accepted response oneof');return {...selected,unknown,requestId:selected.tree?.values.requestId||0};
}
export function comparePayload(actual,expected,actualId){
 assert.equal(actual.type,expected.type);assert.equal(actual.requestId,actualId,'real request/response ID');
 assert.deepEqual(actual.unknown,expected.unknown,'SignalResponse source unknown cohort');
 const left=structuredClone(actual.tree),right=structuredClone(expected.tree);
 // The independent Go component oracle uses fixed IDs71/72; live transactions
 // allocate unique IDs. Assert actual identity first, then compare every other
 // known value/presence and unknown byte cohort, including response root.
 delete left.values.requestId;delete right.values.requestId;
 assert.deepEqual(left,right,'all known semantics/presence + original unknown cohorts');
}
