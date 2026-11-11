// Actual prestarted HTTP servers only. Never launch/mutate the server or SDK.
import assert from 'node:assert/strict';
import {readFile,writeFile,mkdir} from 'node:fs/promises';
import {dirname,join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash,createHmac,randomUUID} from 'node:crypto';
const root=dirname(fileURLToPath(import.meta.url));
const output=process.env.F109_OUTPUT;assert.ok(output,'F109_OUTPUT required');
const cases=(await readFile(join(root,'fixtures/go25-cases.jsonl'),'utf8')).trimEnd().split('\n');
const observations=(await readFile(join(root,'fixtures/go25-observations.jsonl'),'utf8')).trimEnd().split('\n').map(JSON.parse);
assert.equal(cases.length,25);assert.equal(observations.length,25);
const key=process.env.LIVEKIT_API_KEY??'devkey',secret=process.env.LIVEKIT_API_SECRET??'secret';
const sides=[{label:'official',url:process.env.F109_OFFICIAL_URL,binary:process.env.F109_OFFICIAL_BINARY},{label:'rust',url:process.env.F109_RUST_URL,binary:process.env.F109_RUST_BINARY}];
const evidence={status:'RUNNING',scope:'Actual TCP HTTP /rtc/validate auth middleware + route, no SDK lifecycle claim',rows:[],cleanupErrors:[],binaries:{}};
const hash=bytes=>createHash('sha256').update(bytes).digest('hex');
const enc=value=>Buffer.from(value).toString('base64url');
function signed(raw){const input=enc('{"alg":"HS256","typ":"JWT"}')+'.'+enc(raw);return input+'.'+createHmac('sha256',secret).update(input).digest('base64url');}
function claims(room,config){return `{"iss":${JSON.stringify(key)},"sub":"f109-http-actor","exp":${Math.floor(Date.now()/1000)+600},"video":{"roomJoin":true,"room":${JSON.stringify(room)}},"roomConfig":${config}}`;}
const admin=signed(JSON.stringify({iss:key,sub:'f109-owned-admin',exp:Math.floor(Date.now()/1000)+600,video:{roomCreate:true,roomList:true}}));
async function api(side,method,body){const response=await fetch(new URL('/twirp/livekit.RoomService/'+method,side.url),{method:'POST',headers:{authorization:'Bearer '+admin,'content-type':'application/json'},body:JSON.stringify(body),signal:AbortSignal.timeout(10000),redirect:'manual'});const text=await response.text();assert.equal(response.status,200,'owned '+method+' failed: '+response.status+' '+text);return text;}
// Inputs retain raw same-name duplicates and raw whitespace. Only extracting the
// config member from the controlled JSONL envelope is done here. No JSON.parse
// of that member may erase its lexical duplicates or escapes.
function input(line){const row=JSON.parse(line);if(Object.hasOwn(row,'config_raw'))return{id:row.id,raw:row.config_raw};const prefix='"config":';const at=line.indexOf(prefix);assert.ok(at>=0);assert.ok(line.endsWith('}'));return{id:row.id,raw:line.slice(at+prefix.length,-1)};}
try{
 for(const side of sides){assert.ok(side.url&&side.binary,'actual URL+binary file path required for each prestarted side');assert.ok(['http:','https:'].includes(new URL(side.url).protocol));evidence.binaries[side.label]={sha256:hash(await readFile(side.binary))};}
 for(const side of sides){
  side.room='f109-'+side.label+'-'+randomUUID();side.created=false;
  await api(side,'CreateRoom',{name:side.room,emptyTimeout:600});side.created=true;
  for(let i=0;i<cases.length;i++){
   const c=input(cases[i]),go=observations[i];assert.equal(c.id,go.id);
   for(const auth of ['bearer','query']){
    const token=signed(claims(side.room,c.raw));const target=new URL('/rtc/validate',side.url);target.searchParams.set('protocol','15');target.searchParams.set('sdk','js');target.searchParams.set('version','2.23.0');
    const headers={origin:'http://f109.public.fixture'};if(auth==='bearer')headers.authorization='Bearer '+token;else target.searchParams.set('access_token',token);
    const response=await fetch(target,{headers,signal:AbortSignal.timeout(10000),redirect:'manual'});const body=Buffer.from(await response.arrayBuffer());
    const row={side:side.label,id:c.id,auth,status:response.status,bodyBase64:body.toString('base64'),bodySha256:hash(body),contentType:response.headers.get('content-type'),allowOrigin:response.headers.get('access-control-allow-origin')};evidence.rows.push(row);
    assert.equal(row.status,go.verified?200:401,'source acceptance status: '+c.id+'/'+auth);
    assert.equal(row.allowOrigin,'*','validate auth override required');
    if(go.verified)assert.equal(body.toString(),'success','real successful validate route');
    else assert.ok(body.toString().startsWith('invalid authorization token: token is malformed: could not JSON decode claim: '),'real auth diagnostic wrapper missing');
   }
  }
 }
 let differences=[];
 for(const go of evidence.rows.filter(r=>r.side==='official')){
  const rust=evidence.rows.find(r=>r.side==='rust'&&r.id===go.id&&r.auth===go.auth);assert.ok(rust);
  for(const field of ['status','bodyBase64','contentType','allowOrigin'])if(go[field]!==rust[field])differences.push({id:go.id,auth:go.auth,field,official:go[field],rust:rust[field]});
 }
 evidence.differences=differences;evidence.exactPairEqual=differences.length===0;
 assert.equal(evidence.rows.length,100);assert.deepEqual(differences,[],'actual fixed-binary HTTP parity differs; raw error bytes MUST remain unnormalized');evidence.status='PASS_SCOPED';
}catch(error){evidence.status='FAIL';evidence.error=String(error);process.exitCode=1;}
finally{
 for(const side of sides)if(side.created){try{await api(side,'DeleteRoom',{room:side.room});}catch(error){evidence.cleanupErrors.push(side.label+': '+String(error));}}
 if(evidence.cleanupErrors.length){evidence.status='FAIL';process.exitCode=1;}
 await mkdir(dirname(output),{recursive:true});await writeFile(output,JSON.stringify(evidence,null,2)+'\n');
}
