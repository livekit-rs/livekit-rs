# is 0.11.1 mapped-host physical base extension

Original published identity and exact original/modified hashes are in SOURCE.json. CandidateBuilder::mapped_host(addr,base) retains Host kind. Advertised-IP validation is unchanged; genuine physical bases separately permit IPv4 link-local, while unspecified/broadcast/multicast/family mismatch remain invalid. Host binding-request lookup uses base (ordinary host base=addr unchanged). Candidate Eq/dedup/foundation/pair pruning/nomination algorithms remain byte-identical. Upstream Pion gather policy is enforced separately by runtime shared-mux candidate admission, not by changing this general native ICE capability. New units and real ICE/DTLS/SRTP integration tests are NOT_RUN here. Original MIT license retained.

## Sequential WARP/SPED append

`warp-sped-dtls-in-stun.patch` follows the unchanged mapped-host patch. It adds authenticated0xc070/0xc071 attributes and the fixed LiveKit ICEv4.4.0-warp.3 DTLS flight/CRC32-ACK controller, selected-pair check wakeups for ICE-lite, plain peer fallback, and owned pending-flight cleanup. Source license is preserved separately. The consuming str0m adapter must inject only after MESSAGE-INTEGRITY validation, and preserve actual DTLS certificate/fingerprint verification. New controller/wire/SCTP tests are implementation source pending execution, not a network acceptance certificate.

## Nomination acceptance minimum waits

`nomination-acceptance-min-waits.patch` adds opt-in per-kind controlling nomination
ages, mapped local kind selection, role/restart age reset and pending deadline
wakeups; actual authenticated-STUN tests are included. Generic defaults stay zero.
Fixed Go defaults are installed by the application. SOURCE ONLY, not executed here.

## Dedicated server-reflexive socket bases

`server-reflexive-socket-base.patch` allows a server-reflexive candidate on its
own socket to receive authenticated connectivity checks against the physical
base. Existing host and relayed candidates retain precedence. The related
address builder sets SDP metadata without changing socket ownership.
Discarded server-reflexive candidates are pruned when ICE restarts, after
the checklist and selected pair have been cleared.
