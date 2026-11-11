/** Unchanged official SipClient against real public config RPCs and Redis. */
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {dirname,join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {mkdir,writeFile} from 'node:fs/promises';
const here=dirname(fileURLToPath(import.meta.url));
const {SipClient}=createRequire(join(here,'package.json'))('livekit-server-sdk');
const urls=[process.env.REFERENCE_URL,process.env.LIVEKIT_URL];assert.ok(urls.every(Boolean));
const evidence={profile:'functional-sip-official-sdk',status:'RUNNING',scope:'Unmodified locked livekit-server-sdk SipClient configuration helpers; no phone calls or transfers.',servers:[]};
for(let index=0;index<urls.length;index++){
 const client=new SipClient(urls[index],index===0?process.env.REFERENCE_API_KEY||'devkey':process.env.LIVEKIT_API_KEY||'devkey',index===0?process.env.REFERENCE_API_SECRET||'secret':process.env.LIVEKIT_API_SECRET||'secret');
 const ids=new Map(), owned=[];const row={server:index===0?'reference':'rust',steps:[]};evidence.servers.push(row);
 const normalize=v=>{if(v?.toJson)v=v.toJson();if(Array.isArray(v))return v.map(normalize);if(v&&typeof v==='object')return Object.fromEntries(Object.entries(v).map(([k,x])=>[k,normalize(x)]));if(typeof v==='string')for(const [id,label] of ids)v=v.replaceAll(id,label);return v};
 const record=(step,value)=>row.steps.push({step,value:normalize(value)});
 try{
  assert.deepEqual(await client.listSipInboundTrunk(),[]);assert.deepEqual(await client.listSipOutboundTrunk(),[]);assert.deepEqual(await client.listSipDispatchRule(),[]);
  let inbound=await client.createSipInboundTrunk('sdk-inbound',['+12025550761'],{includeHeaders:999,metadata:'sdk-fixture'});ids.set(inbound.sipTrunkId,'INBOUND');owned.push(['trunk',inbound.sipTrunkId]);assert.equal(inbound.includeHeaders,999);record('createInbound',inbound);
  let outbound=await client.createSipOutboundTrunk('sdk-outbound','sip.example.invalid',['+12025550762'],{transport:0});ids.set(outbound.sipTrunkId,'OUTBOUND');owned.push(['trunk',outbound.sipTrunkId]);record('createOutbound',outbound);
  record('listInbound',await client.listSipInboundTrunk({trunkIds:[inbound.sipTrunkId]}));record('listOutbound',await client.listSipOutboundTrunk({trunkIds:[outbound.sipTrunkId]}));
  inbound=await client.updateSipInboundTrunkFields(inbound.sipTrunkId,{metadata:'updated',numbers:{add:['+12025550763'],remove:['+12025550761']}});assert.deepEqual(inbound.numbers,['+12025550763']);record('updateInboundFields',inbound);
  inbound.name='replacement-inbound';inbound=await client.updateSipInboundTrunk(inbound.sipTrunkId,inbound);assert.equal(inbound.name,'replacement-inbound');record('replaceInbound',inbound);
  outbound=await client.updateSipOutboundTrunkFields(outbound.sipTrunkId,{metadata:'updated',numbers:{set:['+12025550764']}});assert.deepEqual(outbound.numbers,['+12025550764']);record('updateOutboundFields',outbound);
  outbound.name='replacement-outbound';outbound=await client.updateSipOutboundTrunk(outbound.sipTrunkId,outbound);record('replaceOutbound',outbound);
  let rule=await client.createSipDispatchRule({type:'direct',roomName:'sdk-fixture-room'},{name:'sdk-rule',trunkIds:[inbound.sipTrunkId]});ids.set(rule.sipDispatchRuleId,'RULE');owned.push(['rule',rule.sipDispatchRuleId]);record('createRule',rule);
  rule=await client.updateSipDispatchRuleFields(rule.sipDispatchRuleId,{metadata:'sdk-rule-updated',attributes:{locale:'zh'}});record('updateRuleFields',rule);
  rule.name='replacement-rule';rule=await client.updateSipDispatchRule(rule.sipDispatchRuleId,rule);record('replaceRule',rule);
  record('listRule',await client.listSipDispatchRule({dispatchRuleIds:[rule.sipDispatchRuleId]}));
 }catch(error){evidence.status='FAIL';row.error={name:error.name,message:error.message};process.exitCode=1;}
 finally{for(const [kind,id] of owned.toReversed()){try{if(kind==='rule')record('deleteRule',await client.deleteSipDispatchRule(id));else record('deleteTrunk',await client.deleteSipTrunk(id));}catch(error){evidence.status='FAIL';row.cleanup_error=error.name;process.exitCode=1;}}}
}
try{assert.deepEqual(evidence.servers[0].steps,evidence.servers[1].steps)}catch{evidence.status='FAIL';evidence.differential_error='Official SDK decoded state/output mismatch';process.exitCode=1;}
if(evidence.status!=='FAIL')evidence.status='PASS';
const output=process.env.SIP_SDK_EVIDENCE||join(here,'output/functional-sip-sdk.json');await mkdir(dirname(output),{recursive:true});await writeFile(output,JSON.stringify(evidence,null,2)+'\n');console.log(JSON.stringify({status:evidence.status,steps:evidence.servers.map(x=>x.steps.length)}));
