/** Actual public endpoint recorder, same cases against fixed Go then Rust. NOT_RUN. */
import {createHmac} from 'node:crypto';
import {writeFile} from 'node:fs/promises';
const url=process.env.LIVEKIT_HTTP_URL||'http://127.0.0.1:7880',key=process.env.LIVEKIT_API_KEY||'devkey',secret=process.env.LIVEKIT_API_SECRET||'secret';
function token(claims){const b=value=>Buffer.from(JSON.stringify(value)).toString('base64url');const h=b({alg:'HS256',typ:'JWT'}),p=b({iss:key,exp:Math.floor(Date.now()/1000)+600,nbf:0,...claims});return `${h}.${p}.${createHmac('sha256',secret).update(`${h}.${p}`).digest('base64url')}`;}
const claims={sub:'whip-oracle-public',video:{roomJoin:true,room:'whip-oracle-room'}};
const cases=[];for(const path of ['/whip/v1','/whip/v1/PA_public_oracle'])for(const method of ['GET','HEAD','OPTIONS','PUT'])cases.push({id:`${path.endsWith('v1')?'root':'resource'}-${method}`,path,method});
cases.push({id:'cors-preflight',path:'/whip/v1',method:'OPTIONS',headers:{Origin:'http://public.fixture','Access-Control-Request-Method':'POST','Access-Control-Request-Headers':'authorization,content-type'}});
for(const method of ['POST','PATCH','DELETE']){
 const path=method==='POST'?'/whip/v1':'/whip/v1/PA_public_oracle';
 const headers=method==='POST'?{'Content-Type':'application/sdp'}:method==='PATCH'?{'Content-Type':'application/trickle-ice-sdpfrag','If-Match':'*'}:{};
 for(const [name,c]of [['absent',null],['no-video',{sub:claims.sub}],['no-join',{sub:claims.sub,video:{room:claims.video.room}}],['empty-room',{sub:claims.sub,video:{roomJoin:true}}],['empty-identity',{video:claims.video}],['valid',claims]])cases.push({id:`${method}-${name}`,path,method,headers,claims:c,body:method==='DELETE'?undefined:''});
 if(method!=='DELETE')for(const type of ['', 'application/sdp; charset=utf-8','Application/SDP','text/plain'])cases.push({id:`${method}-type-${type||'absent'}`,path,method,headers:{...headers,'Content-Type':type},claims,body:''});
}
for(const [name,headers,body]of [
 ['missing-tag',{'Content-Type':'application/trickle-ice-sdpfrag'},''],
 ['wrong-type-and-missing-tag',{'Content-Type':'text/plain'},''],
 ['trickle-missing-resource',{'Content-Type':'application/trickle-ice-sdpfrag','If-Match':'fixture-tag'},'m=audio 9 RTP/AVP 0\na=mid:0\n'],
 ['oversize-before-missing-resource',{'Content-Type':'application/trickle-ice-sdpfrag','If-Match':'*'},'x'.repeat((1<<20)+1)]
])cases.push({id:`PATCH-${name}`,path:'/whip/v1/PA_public_oracle',method:'PATCH',headers,body,claims});
for(const [name,headers,body]of [
 ['invalid-sdp',{},'bad SDP'],['oversize',{},'x'.repeat((1<<20)+1)],
 ['malformed-client-info',{'X-LiveKit-ClientInfo':'{'},''],
 ['client-info-null',{'X-LiveKit-ClientInfo':'null'},''],
 ['client-info-trailing-document',{'X-LiveKit-ClientInfo':'{} {}'},''],
 ['client-info-wrong-type',{'X-LiveKit-ClientInfo':'{"clientIp":7}'},'']
])cases.push({id:`POST-${name}`,path:'/whip/v1',method:'POST',headers:{'Content-Type':'application/sdp',...headers},body,claims});
const results=[];
for(const c of cases){const headers={...c.headers};if(c.claims)headers.Authorization=`Bearer ${token(c.claims)}`;
 const response=await fetch(url+c.path,{method:c.method,headers,body:c.body,redirect:'manual',signal:AbortSignal.timeout(15000)});const body=await response.text();
 const selected={};for(const k of ['content-type','allow','location','etag','access-control-allow-origin','access-control-allow-headers','access-control-allow-methods','access-control-expose-headers','vary']){const v=response.headers.get(k);if(v!==null)selected[k]=v;}
 results.push({id:c.id,status:response.status,headers:selected,body});
}
await writeFile(process.env.WHIP_HTTP_OUTPUT||new URL('../output/native-whip-http.json',import.meta.url),JSON.stringify({status:'RECORDED_NOT_AUTOMATIC_PASS',count:results.length,results},null,2)+'\n');
