from pathlib import Path
import hashlib,json,difflib
root=Path('/Users/zoujiaqing/projects/livekit-rs');out=Path(__file__).resolve().parent
h=lambda b:hashlib.sha256(b).hexdigest();base='livekit-rs-server/crates/livecloud/src/'
cli=base+'cli_nodes.rs';lib=base+'lib.rs';helper=base+'redis_address.rs';tests=base+'cli_redis_address_tests.rs'
bases={n:(root/n).read_bytes() if (root/n).exists() else None for n in [cli,lib,helper,tests]};assert bases[helper] is None and bases[tests] is None
s=bases[cli].decode();start=s.index('        let info = redis::IntoConnectionInfo::into_connection_info(format!(');end=s.index('        let client = redis::Client::open(',start)
s=s[:start]+'''        let address = redis_address::parse(&settings.address)
            .await
            .map_err(|error| format!("unable to connect to redis: {error}"))?;
        let info = redis::IntoConnectionInfo::into_connection_info(address.connection.clone())
            .map_err(|error| format!("unable to connect to redis: {}", redis_message(&error)))?;
        // Fixed go-redis only issues SELECT when DB > 0; negative DB stays DB0.
        let mut native = info.redis_settings().clone().set_db(settings.db.max(0));
        if !settings.username.is_empty() {
            native = native.set_username(&settings.username);
        }
        if !settings.password.is_empty() {
            native = native.set_password(&settings.password);
        }
'''+s[end:]
s=s.replace('connection_message(&e, &endpoint)','address_connection_message(&e, &address)');assert '&endpoint' not in s
needle='#[cfg(test)]\nuse prost::Message;\n';assert s.count(needle)==1;s=s.replace(needle,needle+'\n#[path = "redis_address.rs"]\nmod redis_address;\n',1)
needle='fn connection_message(error: &redis::RedisError, endpoint: &str) -> String {';assert s.count(needle)==1
new='''fn address_connection_message(error: &redis::RedisError, address: &redis_address::Address) -> String {
    if address.network == "tcp" || error.kind() == redis::ErrorKind::AuthenticationFailed {
        return connection_message(error, &address.endpoint);
    }
    // Only a real UNIX dial IO cause authorizes the net.OpError-style wrapper.
    // A textual Redis server/client error must never be converted into a dial.
    if error.is_io_error() {
        let kind = std::error::Error::source(error).and_then(|cause| {
            cause.downcast_ref::<std::io::Error>().map(std::io::Error::kind)
                .or_else(|| cause.downcast_ref::<std::sync::Arc<dyn std::error::Error + Send + Sync>>()
                    .and_then(|shared| shared.downcast_ref::<std::io::Error>())
                    .map(std::io::Error::kind))
        });
        let cause = match kind {
            Some(std::io::ErrorKind::NotFound) => Some("no such file or directory"),
            Some(std::io::ErrorKind::ConnectionRefused) => Some("connection refused"),
            Some(std::io::ErrorKind::PermissionDenied) => Some("permission denied"),
            Some(std::io::ErrorKind::NotADirectory) => Some("not a directory"),
            Some(std::io::ErrorKind::InvalidInput) => Some("invalid argument"),
            _ => None,
        };
        if let Some(cause) = cause {
            return format!("dial unix {}: connect: {cause}", address.endpoint);
        }
    }
    redis_message(error)
}

'''
s=s.replace(needle,new+needle,1)
payload={cli:s.encode(),helper:(out/'redis_address.rs').read_bytes(),tests:(out/'new-tests.rs').read_bytes(),lib:bases[lib]+b'\n#[cfg(test)]\nmod cli_redis_address_tests;\n'}
targets=[];hunks={};patch=[];phasepatch={'production':[],'tests-only':[]}
for n in [cli,helper,lib,tests]:
 old=(bases[n] or b'').decode().splitlines(True);new=payload[n].decode().splitlines(True);phase='production' if n in [cli,helper] else 'tests-only';q=out/'payload'/n;q.parent.mkdir(parents=True,exist_ok=True);q.write_bytes(payload[n]);targets.append({'path':n,'phase':phase,'base_sha256':h(bases[n]) if bases[n] is not None else None,'payload_sha256':h(payload[n])});hunks[n]=[{'old_start':a,'old_end':b,'old_lines':old[a:b],'new_lines':new[c:d]} for tag,a,b,c,d in difflib.SequenceMatcher(None,old,new,autojunk=False).get_opcodes() if tag!='equal'];diff=''.join(difflib.unified_diff(old,new,fromfile='a/'+n if bases[n] is not None else '/dev/null',tofile='b/'+n));patch.append(diff);phasepatch[phase].append(diff)
(out/'all.patch').write_text(''.join(patch))
for phase,v in phasepatch.items():(out/(phase+'.patch')).write_text(''.join(v))
context=['livekit-rs-server/crates/lk-config/src/load.rs','livekit-rs-server/crates/lk-config/src/unsupported.rs','livekit-rs-server/crates/livecloud/Cargo.toml','livekit-rs-server/Cargo.lock','livekit-rs-server/tests/oracles/list-nodes-wire/cases.jsonl']
fixed=['references/protocol-stable/redis/redis.go','references/livekit-stable/cmd/server/commands.go','references/livekit-stable/pkg/service/wire_gen.go','references/livekit-stable/pkg/routing/redisrouter.go','references/go-modules/go-redis-v9.22.0/options.go','references/go-modules/go-redis-v9.22.0/redis.go','references/go-modules/go-redis-v9.22.0/commands.go','references/go-stdlib-go1.26.3/src/net/net.go','references/go-modules/go-redis-v9.22.0/universal.go','references/go-stdlib-go1.26.3/src/net/ipsock.go','references/go-stdlib-go1.26.3/src/net/port.go','references/go-stdlib-go1.26.3/src/net/lookup.go','references/go-stdlib-go1.26.3/src/net/port_unix.go','references/go-stdlib-go1.26.3/src/net/cgo_unix.go','references/protocol-stable/go.mod','references/protocol-stable/go.sum']
j={'status':'CANDIDATE_NOT_COMPILED_NOT_RUN','targets':targets,'line_hunks':hunks,'context_sha256':{n:h((root/n).read_bytes()) for n in context},'fixed_source_root':str(root),'fixed_source_sha256':{n:h((root/n).read_bytes()) for n in fixed},'assets_sha256':{}}
(out/'SOURCE.json').write_text(json.dumps(j,indent=2)+'\n')
