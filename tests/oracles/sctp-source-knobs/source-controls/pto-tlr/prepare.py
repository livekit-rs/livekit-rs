from pathlib import Path
import argparse,json,hashlib,shutil
p=argparse.ArgumentParser();p.add_argument('--staged-source',type=Path,required=True);p.add_argument('--source-reference',type=Path,required=True);a=p.parse_args();here=Path(__file__).resolve().parent;lock=json.loads((here/'pion-source-inputs.json').read_text())
for name,sha in lock['file_sha256'].items():
 if not name.endswith('.go')or name.endswith('_test.go'):continue
 src=a.source_reference/name;assert hashlib.sha256(src.read_bytes()).hexdigest()==sha,name
 dst=a.staged_source/'source'/name;assert not dst.exists()or hashlib.sha256(dst.read_bytes()).hexdigest()==sha,name
 if not dst.exists():dst.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(src,dst)
q=a.staged_source/'source/association.go';s=q.read_text();replacements=[]
for name,endname in [('movePendingDataChunkToInflightQueue','popPendingDataChunksToSend'),('onRackAfterSACK','schedulePTOAfterSendLocked'),('startRackTimer','stopRackTimer'),('startPTOTimer','stopPTOTimer'),('processSelectiveAck','streamForAcknowledgedChunk'),('getDataPacketsToRetransmit','generateNextTSN'),('onPTOTimerLocked','rackInsert'),('tlrCurrentBurstUnitsLocked','tlrCurrentBurstBudgetScaledLocked'),('tlrBeginLocked','tlrApplyAdditionalLossLocked'),('gatherOutboundFastRetransmissionPackets','gatherOutboundSackPackets')]:
 start=s.index('func (a *Association) '+name+'(');end=s.index('func (a *Association) '+endname+'(',start);part=s[start:end];changed=part.replace('time.Now()','sourceRackObserverNow()').replace('time.Since(a.rackDeliveredTime)','sourceRackObserverNow().Sub(a.rackDeliveredTime)');assert part!=changed,name
 replacements.append({'method':name,'before_sha256':hashlib.sha256(part.encode()).hexdigest(),'after_sha256':hashlib.sha256(changed.encode()).hexdigest()});s=s[:start]+changed+s[end:]
q.chmod(q.stat().st_mode|0o200);q.write_text(s)
dst=a.staged_source/'source/source_pto_tlr_test.go';assert not dst.exists();shutil.copyfile(here/'pto_tlr_test.go',dst)
(a.staged_source/'ack-rtt-observer-replacements.json').write_text(json.dumps(replacements,indent=2)+'\n')
