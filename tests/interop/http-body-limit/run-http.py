#!/usr/bin/env python3
"""Actual HTTP differential only. Coordinator starts fixedGo/Rust owned servers.
No fake worker/RPC, no service launch, no change to SDP input. All full responses
are compared in memory; evidence saves only safe errors/counts/hashes.
"""
import argparse,base64,hashlib,hmac,http.client,json,os,time,urllib.parse

def digest(x):return hashlib.sha256(x).hexdigest()
def token(key,secret,room):
 now=int(time.time());part=lambda x:base64.urlsafe_b64encode(json.dumps(x,separators=(',',':')).encode()).rstrip(b'=')
 head=part({'alg':'HS256','typ':'JWT'});body=part({'iss':key,'sub':'f67-owned-observer','nbf':now-5,'exp':now+600,'video':{'room':room,'roomJoin':True,'roomCreate':True,'roomAdmin':True,'roomList':True,'canPublish':True}})
 return b'.'.join([head,body,base64.urlsafe_b64encode(hmac.new(secret.encode(),head+b'.'+body,hashlib.sha256).digest()).rstrip(b'=')]).decode()

def execute(url,method,path,content,body,framing,chunk_bytes,auth,timeout,extra=None):
 parsed=urllib.parse.urlsplit(url)
 if parsed.scheme!='http'or parsed.hostname not in ['localhost','127.0.0.1','::1']or parsed.path not in ['', '/']or parsed.query or parsed.fragment:raise ValueError('owned loopback HTTP base URL required')
 connection=http.client.HTTPConnection(parsed.hostname,parsed.port or 80,timeout=timeout);sent=0;send_error=None
 try:
  connection.putrequest(method,path);connection.putheader('Content-Type',content)
  if auth:connection.putheader('Authorization','Bearer '+auth)
  for k,v in (extra or {}).items():connection.putheader(k,v)
  connection.putheader('Connection','close')
  if framing=='length':connection.putheader('Content-Length',str(len(body)))
  elif framing=='chunked':connection.putheader('Transfer-Encoding','chunked')
  else:raise ValueError('framing must be exactlength or actual chunked')
  connection.endheaders()
  try:
   if framing=='length':connection.send(body);sent=1 if body else 0
   else:
    for at in range(0,len(body),chunk_bytes):
     block=body[at:at+chunk_bytes];connection.send(('%x\r\n'%len(block)).encode()+block+b'\r\n');sent+=1
    connection.send(b'0\r\n\r\n')
  except OSError as e:send_error=type(e).__name__
  response=connection.getresponse();data=response.read(2<<20)
  if len(data)==2<<20:raise ValueError('response bound exceeded')
  return {'status':response.status,'content_type':response.getheader('Content-Type',''),'body':data,'request_chunks_sent':sent,'send_error_class':send_error}
 finally:connection.close()

def comparison(value):
 raw=value['body']
 try:parsed=json.loads(raw)
 except (ValueError,UnicodeError):parsed=None
 return value['status'],value['content_type'],parsed if parsed is not None else raw

def safe(value):
 body=value['body'];result={k:v for k,v in value.items()if k!='body'};result.update(body_bytes=len(body),body_sha256=digest(body))
 try:decoded=json.loads(body)
 except (ValueError,UnicodeError):decoded=None
 allowed={'request body too large','the json request could not be decoded','failed to read request body','permission denied','missing entity tag'}
 if isinstance(decoded,dict):
  for key in ['error','msg']:
   text=decoded.get(key)
   if isinstance(text,str)and(text in allowed or (text.startswith('request body exceeds ')and text.endswith(' bytes')and text[21:-6].isdigit())):result[key]=text
  if decoded.get('code')in ['malformed','unauthenticated','permission_denied','invalid_argument','bad_route']:result['code']=decoded['code']
 elif body==b'request body too large':result['plain_error']='request body too large'
 return result

def varint(n):
 out=bytearray()
 while n>127:out.append(n&127|128);n>>=7
 out.append(n);return bytes(out)

def main():
 p=argparse.ArgumentParser();p.add_argument('--official',required=True);p.add_argument('--rust',required=True);p.add_argument('--offer-file',required=True);p.add_argument('--limit',type=int,required=True);p.add_argument('--out',required=True);p.add_argument('--timeout',type=float,default=15);a=p.parse_args()
 if not 32<=a.limit<1<<20 or not 0<a.timeout<=30:raise ValueError('requires explicit configured32<=limit<1MiB and bounded timeout')
 key=os.environ['API_KEY'];secret=os.environ['API_SECRET'];room='f67-owned-'+str(os.getpid())+'-'+str(time.time_ns());bearer=token(key,secret,room);offer=open(a.offer_file,'rb').read()
 if not a.limit<len(offer)<1<<20:raise ValueError('unaltered actual SDP input must exceed configured limit and be below native1MiB cap')
 urls={'official':a.official,'rust':a.rust};path='/twirp/livekit.RoomService/ListRooms';small=json.dumps({'names':[room]},separators=(',',':')).encode()
 if len(small)>a.limit:raise ValueError('choose limit large enough for actual minimal authorized ListRooms request')
 # All sent bytes and framing are identical for both real servers. The JSON
 # prefix/read-ahead cases have no authored Go status: actualGo decides them.
 inside=b'{"names":["'+b'x'*(a.limit+20)+b'"]}'
 proto=varint(100<<3|2)+varint(a.limit+10)+b'x'*(a.limit+10)
 cases=[
  ('configured-limit-exact-positive','POST',path,'application/json',small+b' '*(a.limit-len(small)),'length',a.limit,True,{},200),
  ('configured-limit-plus-one-preauth','POST',path,'application/json',b' '* (a.limit+1),'length',a.limit+1,False,{},413),
  ('whip-declared-plus-one-before-auth','POST','/whip/v1','application/sdp',offer,'length',len(offer),False,{},413),
  ('chunked-json-value-exceeds-limit','POST',path,'application/json',inside,'chunked',13,True,{},400),
  ('chunked-protobuf-value-exceeds-limit','POST',path,'application/protobuf',proto,'chunked',13,True,{},400),
  ('chunked-whip-unaltered-offer','POST','/whip/v1','application/sdp',offer,'chunked',13,True,{},413),
  ('chunked-whip-auth-before-reader','POST','/whip/v1','application/sdp',offer,'chunked',13,False,{},401),
  ('chunked-whip-fragment-before-missing-owner','PATCH','/whip/v1/PA_f67_absent','application/trickle-ice-sdpfrag',offer,'chunked',13,True,{'If-Match':'*'},413),
  ('chunked-whip-missing-tag-before-reader','PATCH','/whip/v1/PA_f67_absent','application/trickle-ice-sdpfrag',offer,'chunked',13,True,{},428),
 ]
 for chunk in [1,13,512,a.limit+20]:
  cases.append(('json-first-value-trailing-overlimit-chunk'+str(chunk),'POST',path,'application/json',small+b' '*(a.limit+100),'chunked',chunk,True,{},None))
 report={'status':'RUNNING','scope':'real fixedGo/Rust HTTP request-reader contract; no RTC/media content acceptance','configured_limit':a.limit,'offer_bytes':len(offer),'offer_sha256':digest(offer),'input_bytes_unchanged':True,'base_url_sha256':{k:digest(v.encode())for k,v in urls.items()},'cases':[],'cleanup':[],'not_claimed':['No SDP rewriting or mock worker/callback','Read-ahead behavior decided by actual paired Go, not an authored first-value success','Source limits disabled/negative/native1MiB still need separate server profile']}
 failed=False
 try:
  for name,method,route,content,body,framing,chunk,authorized,extra,expected in cases:
   row={'id':name,'method':method,'path':route,'content_type':content,'request_bytes':len(body),'request_sha256':digest(body),'framing':framing,'chunk_size':chunk,'auth_present':authorized,'go_status_expectation':expected,'actual':{}};raw={}
   for side,url in urls.items():
    try:raw[side]=execute(url,method,route,content,body,framing,chunk,bearer if authorized else None,a.timeout,extra);row['actual'][side]=safe(raw[side])
    except Exception as e:row['actual'][side]={'transport_exception_class':type(e).__name__}
   equal=len(raw)==2 and comparison(raw['official'])==comparison(raw['rust'])
   if expected is not None:equal=equal and raw.get('official',{}).get('status')==expected and raw.get('rust',{}).get('status')==expected
   row['status']='PASS'if equal else'FAIL';failed=failed or not equal;report['cases'].append(row)
 finally:
  # List the exact owned name, delete only actual returned owned rooms. Do not
  # flush Redis, delete others, suppress an actual cleanup error or log SDP.
  for side,url in urls.items():
   item={'side':side,'status':'NOT_RUN'}
   try:
    response=execute(url,'POST',path,'application/json',small,'length',len(small),bearer,a.timeout);decoded=json.loads(response['body'])
    if response['status']!=200:raise ValueError('owned cleanup list failed')
    rooms=decoded.get('rooms',[]);assert all(x.get('name')==room for x in rooms)
    for _ in rooms:
     body=json.dumps({'room':room},separators=(',',':')).encode();deleted=execute(url,'POST','/twirp/livekit.RoomService/DeleteRoom','application/json',body,'length',len(body),bearer,a.timeout)
     if deleted['status']!=200:raise ValueError('actual owned room delete failed')
    final=execute(url,'POST',path,'application/json',small,'length',len(small),bearer,a.timeout)
    if final['status']!=200 or json.loads(final['body']).get('rooms'):raise ValueError('owned room remains')
    item.update(status='PASS',owned_rooms_deleted=len(rooms),owned_rooms_remaining=0)
   except Exception as e:item.update(status='FAIL',exception_class=type(e).__name__);failed=True
   report['cleanup'].append(item)
  report['status']='FAIL'if failed else'PASS';report['case_count']=len(report['cases']);report['passed']=sum(x['status']=='PASS'for x in report['cases']);report['failed']=sum(x['status']=='FAIL'for x in report['cases'])
  with open(a.out,'w')as f:json.dump(report,f,indent=2);f.write('\n')
 return 1 if failed else 0
if __name__=='__main__':raise SystemExit(main())
