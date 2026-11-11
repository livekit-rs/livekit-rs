#!/usr/bin/env python3
"""Write private worker/SFU config overlays. Does not start any process."""
import argparse, json, os
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--out',required=True);a=p.parse_args();out=Path(a.out);out.mkdir(mode=0o700,parents=True,exist_ok=True)
required=['LIVEKIT_URL','LIVEKIT_API_KEY','LIVEKIT_API_SECRET','REDIS_ADDRESS','WHIP_PROXY_PORT','WHIP_PROXY_RTC_PORT','INGRESS_RELAY_PORT','INGRESS_HEALTH_PORT']
for k in required:
    if not os.environ.get(k): raise SystemExit('missing '+k)
e=os.environ
for k in required[4:]:
    if not 1024<=int(e[k])<=65535: raise SystemExit('invalid owned nonprivileged port '+k)
if len({int(e[k]) for k in required[4:]})!=4: raise SystemExit('ports must be distinct')
# JSON is valid YAML. No external templater, logged secret or public key fixture.
worker={'redis':{'address':e['REDIS_ADDRESS']},'api_key':e['LIVEKIT_API_KEY'],'api_secret':e['LIVEKIT_API_SECRET'],'ws_url':e['LIVEKIT_URL'].replace('http://','ws://').replace('https://','wss://'),'whip_port':int(e['WHIP_PROXY_PORT']),'rtc_config':{'udp_port':int(e['WHIP_PROXY_RTC_PORT'])},'rtmp_port':-1,'http_relay_port':int(e['INGRESS_RELAY_PORT']),'health_port':int(e['INGRESS_HEALTH_PORT']),'development':True,'whip_proxy_enabled':True,'logging':{'level':'info'}}
# Merge this exact subtree into the ROOT-OWNED working server config. Do not replace
# its tested RTC/auth/Redis/HTTP settings with an incomplete new server config.
overlay={'ingress':{'whip_base_url':'http://127.0.0.1:'+e['WHIP_PROXY_PORT']+'/whip'},'psrpc':{'compression':{'quality':0}}}
for name,value in [('worker.yaml',worker),('server-overlay.yaml',overlay)]:
    path=out/name
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w') as f: json.dump(value,f,indent=2);f.write('\n')
