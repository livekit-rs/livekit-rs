#!/usr/bin/env python3
"""Read actual output only; no process launch/rewrites or partial campaign PASS."""
import sys,json,pathlib,hashlib
root=pathlib.Path(sys.argv[1]);plan=json.loads(pathlib.Path(__file__).with_name('PLAN.json').read_text());total=0;records=[]
for profile in plan['required_profiles']:
    if profile.startswith('room-'):
        expected=['CreateRoom-'+x for x in ['', '61','c3a961','c3a9c3a9']]+[x+'-'+y for y in ['empty-identity','room-only','empty-room']for x in ['rtc','whip']]
        limits={'room':-1 if profile.endswith('negative')else 0 if profile.endswith('zero')else 3,'identity':256,'name':256}
    elif profile.startswith('identity-'):
        expected=['rtc-identity-'+str(i)for i in range(5)]
        limits={'room':256,'identity':-1 if profile.endswith('negative')else 0 if profile.endswith('zero')else 3,'name':256}
    else:
        expected=['connect-display-name-not-a-limit']+['signal-'+str(flag).lower()+'-'+str(i)for flag in [False,True]for i in range(4)]+['api-prepermission-'+str(i)for i in range(3)]
        limits={'room':256,'identity':256,'name':-1 if profile.endswith('negative')else 0 if profile.endswith('zero')else 3}
    assert len(expected)==plan['expected_observations'][profile]
    actual=[]
    for side in ['go','rust']:
        path=root/(side+'-'+profile+'.json');data=json.loads(path.read_text());assert data['status']=='PASS_SCOPED',(side,profile,data['status']);assert data['profile']==profile;assert data['limits']==limits;assert data['cleanupErrors']==[]
        observations=data['observations'];assert [x['case']for x in observations]==expected,(side,profile,'exact ordered inventory');assert len({x['case']for x in observations})==len(expected)
        if profile.startswith('name-'):
            assert data['transportClosures']==[{'identity':identity,'closing':True,'code':1000,'clean':True,'reason':''}for identity in ['denied','permitted']],(side,profile,'actual clean peer close')
        if profile.startswith('room-'):
            for x in observations[:4]:
                if x['status']!=200:
                    assert x['status']==400 and x['error']['code']=='invalid_argument' and x['error']['msg']==f"room name length exceeds limits: max length {limits['room']}",(side,profile,x['case'])
        actual.append(data);records.append({'side':side,'profile':profile,'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'observations':len(observations)})
    assert actual[0]['observations']==actual[1]['observations'],(profile,'exact Go/Rust paired outcomes')
    pairpath=root/('pair-'+profile+'.json');pair=json.loads(pairpath.read_text());assert pair['status']=='PASS_SCOPED'and pair['profile']==profile and pair['cases']==len(expected)
    total+=len(expected)
assert total==81 and len(records)==18
print(json.dumps({'status':'PASS_SCOPED_ACTUAL_9_PAIRS','actual_runs':18,'pairs':9,'paired_observations':81,'records':records,'scope':'Configured HTTP/SIG only; server identities/config/exit statuses require parent execution manifest.'},indent=2))
