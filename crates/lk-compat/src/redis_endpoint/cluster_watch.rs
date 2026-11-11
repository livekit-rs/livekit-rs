//! A WATCH transaction keeps a dedicated physical slot-master connection.
//! Generic ClusterConnection can route/retry individual commands, so it cannot
//! own WATCH, the read and EXEC as one connection-bound operation.
use super::*;
use redis::cluster_routing::Slot;

impl RedisEndpoint {
    pub(crate) async fn watch_connection(
        &self,
        config: &AsyncConnectionConfig,
        key: &str,
    ) -> redis::RedisResult<MultiplexedConnection> {
        let EndpointKind::Cluster(cluster) = &self.kind else {
            let mut connection = self.dial(config).await?;
            redis::cmd("WATCH")
                .arg(key)
                .query_async::<()>(&mut connection)
                .await?;
            return Ok(connection);
        };
        let configured = cluster
            .timeouts
            .map(|timeouts| timeouts.apply(config.clone()));
        let config = configured.as_ref().unwrap_or(config);
        let slot = Slot::for_key(key);
        let tls = self.tls_override.as_deref();
        let mut info = slot_master(cluster, config, tls, slot).await?;
        // Retry only admission to WATCH. Once this function returns a
        // connection, the callback and EXEC can never be retried here.
        let redirects = cluster.max_redirects;
        for attempt in 0..=redirects {
            if attempt > 0 {
                tokio::time::sleep(retry_backoff(attempt)?).await;
            }
            let error = match dial_with(info.clone(), config, tls).await {
                Ok(mut connection) => match redis::cmd("WATCH")
                    .arg(key)
                    .query_async::<()>(&mut connection)
                    .await
                {
                    Ok(()) => return Ok(connection),
                    Err(error) => error,
                },
                Err(error) => error,
            };
            if attempt == redirects {
                return Err(error);
            }
            if let Some((address, reported)) = error.redirect_node() {
                if Slot::new(reported) != Some(slot) {
                    return Err(error);
                }
                let Some((host, port)) = address.rsplit_once(':') else {
                    return Err(error);
                };
                let Ok(port) = port.parse::<u16>() else {
                    return Err(error);
                };
                let host = host
                    .strip_prefix('[')
                    .and_then(|host| host.strip_suffix(']'))
                    .unwrap_or(host);
                info = retarget(&info, host, port)?;
            } else if error.kind() == ErrorKind::Server(redis::ServerErrorKind::ReadOnly) {
                info = slot_master(cluster, config, tls, slot).await?;
            } else if !retry_watch_admission(&error) {
                return Err(error);
            }
        }
        unreachable!("inclusive redirect loop always returns on its final attempt")
    }
}

// go-redis v9.22.0 ClusterClient.Watch applies the cluster retry policy only
// before a transaction callback is entered. Definite auth/ACL, parse and
// certificate verification failures do not become a new fallback route.
fn retry_watch_admission(error: &RedisError) -> bool {
    use redis::ServerErrorKind::{BusyLoading, ClusterDown, MasterDown, TryAgain};
    matches!(
        error.kind(),
        ErrorKind::Server(BusyLoading | ClusterDown | MasterDown | TryAgain)
    ) || error.code() == Some("NOREPLICAS")
        || (error.code() == Some("ERR") && error.detail() == Some("max number of clients reached"))
        || error.is_timeout()
        // The Redis classifier checks the wrapped I/O kind. A synthetic Io
        // error (for example, failed peer verification) has no error source.
        || (error.is_connection_dropped() && std::error::Error::source(error).is_some())
}

// ClusterOptions defaults are 10 ms minimum and 1 s maximum. The fixed
// RetryBackoff uses min + uniform(0, min << attempt), capped at max.
pub(super) fn retry_backoff(attempt: u64) -> redis::RedisResult<Duration> {
    const MIN_NS: u64 = 10_000_000;
    const MAX_NS: u64 = 1_000_000_000;
    let Some(span) = u32::try_from(attempt)
        .ok()
        .and_then(|shift| 1u64.checked_shl(shift))
        .and_then(|multiplier| MIN_NS.checked_mul(multiplier))
        .filter(|span| *span <= i64::MAX as u64)
    else {
        return Ok(Duration::from_nanos(MAX_NS));
    };
    let mut randomness = [0u8; 8];
    getrandom::fill(&mut randomness).map_err(|error| {
        RedisError::from((
            ErrorKind::Client,
            "could not choose Redis retry delay",
            error.to_string(),
        ))
    })?;
    let offset = ((u128::from(u64::from_le_bytes(randomness)) * u128::from(span)) >> 64) as u64;
    Ok(Duration::from_nanos((MIN_NS + offset).min(MAX_NS)))
}

async fn slot_master(
    cluster: &ClusterShared,
    config: &AsyncConnectionConfig,
    tls: Option<&TlsSniOverride>,
    slot: Slot,
) -> redis::RedisResult<ConnectionInfo> {
    let mut first_error = None;
    for seed in &cluster.initial_nodes {
        let result = async {
            let mut connection = dial_with(seed.clone(), config, tls).await?;
            let slots = redis::cmd("CLUSTER")
                .arg("SLOTS")
                .query_async::<Value>(&mut connection)
                .await?;
            parse_slot_master(slots, seed, slot)
        }
        .await;
        match result {
            Ok(info) => return Ok(info),
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
    }
    Err(first_error.unwrap_or_else(|| {
        RedisError::from((
            ErrorKind::InvalidClientConfig,
            "cluster endpoint has no seeds",
        ))
    }))
}

fn invalid_slots() -> RedisError {
    RedisError::from((
        ErrorKind::UnexpectedReturnType,
        "CLUSTER SLOTS returned no usable WATCH slot master",
    ))
}

fn parse_slot_master(
    slots: Value,
    seed: &ConnectionInfo,
    slot: Slot,
) -> redis::RedisResult<ConnectionInfo> {
    let Value::Array(rows) = slots else {
        return Err(invalid_slots());
    };
    for row in rows {
        let Value::Array(columns) = row else { continue };
        let [Value::Int(start), Value::Int(end), Value::Array(master), ..] = columns.as_slice()
        else {
            continue;
        };
        let (Ok(start), Ok(end)) = (u16::try_from(*start), u16::try_from(*end)) else {
            continue;
        };
        if Slot::new(end).is_none() || start > end {
            continue;
        }
        // redis-rs exposes Slot equality but keeps its numeric value private.
        // This bounded local scan avoids an extra ACL-sensitive KEYSLOT RPC.
        if !(start..=end).any(|number| Slot::new(number) == Some(slot)) {
            continue;
        }
        let [address, Value::Int(port), ..] = master.as_slice() else {
            return Err(invalid_slots());
        };
        let port = u16::try_from(*port).map_err(|_| invalid_slots())?;
        let host = match address {
            Value::Nil => "",
            Value::BulkString(bytes) => std::str::from_utf8(bytes).map_err(|_| invalid_slots())?,
            Value::SimpleString(host) => host,
            _ => return Err(invalid_slots()),
        };
        return retarget(seed, host, port);
    }
    Err(invalid_slots())
}

fn retarget(seed: &ConnectionInfo, host: &str, port: u16) -> redis::RedisResult<ConnectionInfo> {
    if host == "?" || port == 0 {
        return Err(invalid_slots());
    }
    let target = match seed.addr().clone() {
        ConnectionAddr::Tcp(seed_host, _) => ConnectionAddr::Tcp(
            if host.is_empty() {
                seed_host
            } else {
                host.into()
            },
            port,
        ),
        ConnectionAddr::TcpTls {
            host: seed_host,
            insecure,
            tls_params,
            ..
        } => ConnectionAddr::TcpTls {
            host: if host.is_empty() {
                seed_host
            } else {
                host.into()
            },
            port,
            insecure,
            tls_params,
        },
        _ => return Err(invalid_slots()),
    };
    Ok(seed.clone().set_addr(target))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires the owned WATCH fault proxy and its real three-primary Redis Cluster"]
    async fn real_cluster_watch_retries_one_transient_admission_before_actual_exec() {
        assert_eq!(
            std::env::var("WATCH_FAULT_CLUSTER_ISOLATED").as_deref(),
            Ok("1")
        );
        let info = redis::IntoConnectionInfo::into_connection_info(
            std::env::var("WATCH_FAULT_REDIS_URL").expect("WATCH_FAULT_REDIS_URL"),
        )
        .unwrap();
        let endpoint = RedisEndpoint::cluster(vec![info], 2);
        let config = AsyncConnectionConfig::new()
            .set_connection_timeout(Some(Duration::from_secs(3)))
            .set_response_timeout(Some(Duration::from_secs(3)));
        let key = "{ingress}_watch_fault_test";
        let mut transaction = endpoint.watch_connection(&config, key).await.expect(
            "WATCH fault oracle: a transient admission refusal must be retried before the callback",
        );
        let before: Option<Vec<u8>> = redis::cmd("GET")
            .arg(key)
            .query_async(&mut transaction)
            .await
            .unwrap();
        assert_eq!(before, None);
        let committed: Option<Vec<Value>> = redis::pipe()
            .atomic()
            .cmd("SET")
            .arg(key)
            .arg("actual committed callback")
            .query_async(&mut transaction)
            .await
            .unwrap();
        assert_eq!(committed.unwrap(), vec![Value::Okay]);
        let after: String = redis::cmd("GET")
            .arg(key)
            .query_async(&mut transaction)
            .await
            .unwrap();
        assert_eq!(after, "actual committed callback");
        assert_eq!(
            redis::cmd("DEL")
                .arg(key)
                .query_async::<i64>(&mut transaction)
                .await
                .unwrap(),
            1
        );
    }

    #[test]
    fn watch_retries_only_transient_admission_failures_and_caps_delay() {
        use redis::ServerErrorKind::{BusyLoading, ClusterDown, MasterDown, NoPerm, TryAgain};
        for kind in [BusyLoading, ClusterDown, MasterDown, TryAgain] {
            let error = RedisError::from((ErrorKind::Server(kind), "fixture"));
            assert!(retry_watch_admission(&error));
        }
        for kind in [
            std::io::ErrorKind::UnexpectedEof,
            std::io::ErrorKind::ConnectionRefused,
            std::io::ErrorKind::TimedOut,
        ] {
            assert!(retry_watch_admission(&RedisError::from(
                std::io::Error::from(kind)
            )));
        }
        for kind in [
            ErrorKind::Server(NoPerm),
            ErrorKind::AuthenticationFailed,
            ErrorKind::Parse,
        ] {
            assert!(!retry_watch_admission(&RedisError::from((kind, "fixture"))));
        }
        let tls = RedisError::from((
            ErrorKind::Io,
            "Redis TLS handshake (server_name override) failed",
            "invalid certificate".to_owned(),
        ));
        assert!(!retry_watch_admission(&tls));
        let first = retry_backoff(1).unwrap();
        assert!((Duration::from_millis(10)..Duration::from_millis(30)).contains(&first));
        assert_eq!(retry_backoff(u64::MAX).unwrap(), Duration::from_secs(1));
    }

    #[test]
    fn slots_use_the_covering_master_not_a_replica_and_retain_tls() {
        let seed = redis::IntoConnectionInfo::into_connection_info(
            "rediss://watch-user:watch-password@seed.example:6379/",
        )
        .unwrap();
        let slot = Slot::for_key("ingress");
        assert_eq!(Slot::for_key("{ingress}_stream_key"), slot);
        assert_eq!(Slot::for_key("{ingress}_state:IN_fixture"), slot);
        assert_eq!(Slot::for_key("room_{ingress}:fixture"), slot);
        let row = |start, end, host, port| {
            Value::Array(vec![
                Value::Int(start),
                Value::Int(end),
                Value::Array(vec![host, Value::Int(port)]),
                Value::Array(vec![
                    Value::BulkString(b"replica.example".to_vec()),
                    Value::Int(6400),
                ]),
            ])
        };
        let target = parse_slot_master(
            Value::Array(vec![
                row(0, 5460, Value::BulkString(b"first.example".to_vec()), 6380),
                row(
                    5461,
                    10922,
                    Value::BulkString(b"second.example".to_vec()),
                    6381,
                ),
                row(10923, 16383, Value::Nil, 6382),
            ]),
            &seed,
            slot,
        )
        .unwrap();
        assert!(matches!(
            target.addr(),
            ConnectionAddr::TcpTls { host, port: 6382, insecure: false, .. }
                if host == "seed.example"
        ));
        assert_eq!(target.redis_settings().username(), Some("watch-user"));
        assert_eq!(target.redis_settings().password(), Some("watch-password"));
        assert!(
            parse_slot_master(
                Value::Array(vec![row(
                    10923,
                    16383,
                    Value::SimpleString("?".into()),
                    6382
                )]),
                &seed,
                slot,
            )
            .is_err()
        );
    }
}
