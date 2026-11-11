//! Fixed service/redisstore.go IO state consumer. Dedicated connection:
//! WATCH may not share its connection with unrelated command users.
use super::{error::IoError, preserved::WireValue};
use crate::redis_endpoint::{RedisEndpoint, SharedConnection};
use lk_protocol::livekit as pb;
use prost::Message;
use redis::aio::MultiplexedConnection;
use std::time::Duration;
use tokio::sync::Mutex;

const EGRESS: &str = "egress";
const ENDED: &str = "ended_egress";
const INGRESS: &str = "ingress";
const STREAM_KEY: &str = "{ingress}_stream_key";
const INGRESS_STATE: &str = "{ingress}_state:";
const ROOM_INGRESS: &str = "room_{ingress}:";
const ROOM_EGRESS: &str = "egress:room:";

pub struct IoStore {
    connection: Mutex<SharedConnection>,
    endpoint: RedisEndpoint,
    timeout: Duration,
    started: Mutex<bool>,
}
impl IoStore {
    pub async fn connect(endpoint: RedisEndpoint, timeout: Duration) -> Result<Self, IoError> {
        let config = redis::AsyncConnectionConfig::new()
            .set_connection_timeout(Some(timeout))
            .set_response_timeout(Some(timeout));
        let connection = endpoint.connect(&config).await.map_err(IoError::redis)?;
        Ok(Self {
            connection: Mutex::new(connection),
            endpoint,
            timeout,
            started: Mutex::new(false),
        })
    }
    // Each WATCH transaction owns a fresh connection. Cancelling its future
    // drops that connection rather than leaking WATCH state into another call.
    // A Sentinel endpoint resolves the current master for every transaction.
    async fn transaction_connection(&self, key: &str) -> Result<MultiplexedConnection, IoError> {
        let config = redis::AsyncConnectionConfig::new()
            .set_connection_timeout(Some(self.timeout))
            .set_response_timeout(Some(self.timeout));
        self.endpoint
            .watch_connection(&config, key)
            .await
            .map_err(IoError::redis)
    }
    /// Run before starting the source sweeper. Only a newer actual server
    /// version overwrites the shared version marker; an existing newer marker
    /// is retained. Malformed stored values fail startup instead of Go's nil panic.
    pub async fn start(&self, current_version: &str) -> Result<(), IoError> {
        let mut started = self.started.lock().await;
        if *started {
            return Ok(());
        }
        // Go marks done before its first Redis read, including error cases.
        *started = true;
        let mut c = self.connection.lock().await;
        let existing: Option<String> = redis::cmd("GET")
            .arg("livekit_version")
            .query_async(&mut *c)
            .await
            .map_err(IoError::redis)?;
        if super::version::should_update(existing.as_deref().unwrap_or(""), current_version)
            .map_err(IoError::ordinary)?
        {
            redis::cmd("SET")
                .arg("livekit_version")
                .arg(current_version)
                .query_async::<()>(&mut *c)
                .await
                .map_err(IoError::redis)?;
        }
        Ok(())
    }
    pub async fn store_egress(&self, info: &pb::EgressInfo) -> Result<(), IoError> {
        self.store_egress_wire(&WireValue::from_message(info.clone()))
            .await
    }
    pub async fn store_egress_wire(
        &self,
        value: &WireValue<pb::EgressInfo>,
    ) -> Result<(), IoError> {
        let info = &value.message;
        let c = self.connection.lock().await;
        // Source uses Pipeline (not MULTI) for this pair.
        let mut pipe = redis::pipe();
        pipe.cmd("HSET")
            .arg(EGRESS)
            .arg(&info.egress_id)
            .arg(value.encode());
        pipe.cmd("SADD")
            .arg(format!("{ROOM_EGRESS}{}", info.room_name))
            .arg(&info.egress_id);
        c.ordinary_pipeline(&pipe)
            .await
            .map_err(|e| IoError::redis(e).context("could not store egress info"))?;
        Ok(())
    }
    pub async fn load_egress(&self, id: &str) -> Result<pb::EgressInfo, IoError> {
        self.load_egress_wire(id).await.map(|v| v.message)
    }
    pub async fn load_egress_wire(&self, id: &str) -> Result<WireValue<pb::EgressInfo>, IoError> {
        let mut c = self.connection.lock().await;
        let raw: Option<Vec<u8>> = redis::cmd("HGET")
            .arg(EGRESS)
            .arg(id)
            .query_async(&mut *c)
            .await
            .map_err(IoError::redis)?;
        WireValue::decode(&raw.ok_or_else(IoError::egress_not_found)?)
    }
    pub async fn list_egress(
        &self,
        room: &str,
        active: bool,
    ) -> Result<Vec<pb::EgressInfo>, IoError> {
        self.list_egress_wire(room, active)
            .await
            .map(|values| values.into_iter().map(|v| v.message).collect())
    }
    pub async fn list_egress_wire(
        &self,
        room: &str,
        active: bool,
    ) -> Result<Vec<WireValue<pb::EgressInfo>>, IoError> {
        let mut c = self.connection.lock().await;
        let raw: Vec<Option<Vec<u8>>> = if room.is_empty() {
            // HVALS preserves the source's unspecified hash iteration order.
            let values: Vec<Vec<u8>> = redis::cmd("HVALS")
                .arg(EGRESS)
                .query_async(&mut *c)
                .await
                .map_err(IoError::redis)?;
            values.into_iter().map(Some).collect()
        } else {
            let ids: Vec<String> = redis::cmd("SMEMBERS")
                .arg(format!("{ROOM_EGRESS}{room}"))
                .query_async(&mut *c)
                .await
                .map_err(IoError::redis)?;
            if ids.is_empty() {
                return Ok(Vec::new());
            }
            // Fixed source intentionally ignores HMGET command error in this branch.
            redis::cmd("HMGET")
                .arg(EGRESS)
                .arg(ids)
                .query_async(&mut *c)
                .await
                .unwrap_or_default()
        };
        raw.into_iter()
            .flatten()
            .map(|raw| WireValue::<pb::EgressInfo>::decode(&raw))
            .filter_map(|r| match r {
                Ok(info) if active && info.message.status >= 3 => None,
                other => Some(other),
            })
            .collect()
    }
    pub async fn update_egress(&self, info: &pb::EgressInfo) -> Result<(), IoError> {
        self.update_egress_wire(&WireValue::from_message(info.clone()))
            .await
    }
    pub async fn update_egress_wire(
        &self,
        value: &WireValue<pb::EgressInfo>,
    ) -> Result<(), IoError> {
        let info = &value.message;
        let mut c = self.connection.lock().await;
        if info.ended_at != 0 {
            let mut pipe = redis::pipe();
            pipe.cmd("HSET")
                .arg(EGRESS)
                .arg(&info.egress_id)
                .arg(value.encode());
            pipe.cmd("HSET")
                .arg(ENDED)
                .arg(&info.egress_id)
                .arg(format!("{}|{}", info.room_name, info.ended_at));
            c.ordinary_pipeline(&pipe)
                .await
                .map_err(|e| IoError::redis(e).context("could not update egress info"))?;
        } else {
            redis::cmd("HSET")
                .arg(EGRESS)
                .arg(&info.egress_id)
                .arg(value.encode())
                .query_async::<i64>(&mut *c)
                .await
                .map_err(|e| IoError::redis(e).context("could not update egress info"))?;
        }
        Ok(())
    }
    /// Caller-owned 30min ticker invokes this with actual wall-clock Unix ns.
    pub async fn clean_ended_egress(&self, now_ns: i64) -> Result<(), IoError> {
        let mut c = self.connection.lock().await;
        let values: Vec<(String, String)> = redis::cmd("HGETALL")
            .arg(ENDED)
            .query_async(&mut *c)
            .await
            .map_err(IoError::redis)?;
        let expiry = now_ns.saturating_sub(24 * 60 * 60 * 1_000_000_000);
        for (id, value) in values {
            let (room, ended) = parse_ended(&value)?;
            if ended < expiry {
                let mut pipe = redis::pipe();
                pipe.cmd("SREM")
                    .arg(format!("{ROOM_EGRESS}{room}"))
                    .arg(&id);
                pipe.cmd("HDEL").arg(EGRESS).arg(&id);
                pipe.cmd("HDEL").arg(ENDED).arg(&id);
                c.ordinary_pipeline(&pipe).await.map_err(IoError::redis)?;
            }
        }
        Ok(())
    }
    pub async fn load_ingress(&self, id: &str) -> Result<pb::IngressInfo, IoError> {
        self.load_ingress_wire(id).await.map(|v| v.message)
    }
    pub async fn load_ingress_wire(&self, id: &str) -> Result<WireValue<pb::IngressInfo>, IoError> {
        let mut c = self.connection.lock().await;
        let raw: Option<Vec<u8>> = redis::cmd("HGET")
            .arg(INGRESS)
            .arg(id)
            .query_async(&mut *c)
            .await
            .map_err(IoError::redis)?;
        let mut value =
            WireValue::<pb::IngressInfo>::decode(&raw.ok_or_else(IoError::ingress_not_found)?)?;
        let state: Option<Vec<u8>> = redis::cmd("GET")
            .arg(format!("{INGRESS_STATE}{id}"))
            .query_async(&mut *c)
            .await
            .map_err(IoError::redis)?;
        if let Some(raw) = state {
            let state = WireValue::<pb::IngressState>::decode(&raw)?;
            value.message.state = Some(state.message);
            value.wire.replace_nested("state", Some(&state.wire));
        }
        // Missing external state leaves any legacy inline state intact, like Go.
        Ok(value)
    }
    pub async fn load_ingress_from_stream_key(
        &self,
        key: &str,
    ) -> Result<pb::IngressInfo, IoError> {
        self.load_ingress_from_stream_key_wire(key)
            .await
            .map(|v| v.message)
    }
    pub async fn load_ingress_from_stream_key_wire(
        &self,
        key: &str,
    ) -> Result<WireValue<pb::IngressInfo>, IoError> {
        let id: Option<String> = {
            let mut c = self.connection.lock().await;
            redis::cmd("HGET")
                .arg(STREAM_KEY)
                .arg(key)
                .query_async(&mut *c)
                .await
                .map_err(IoError::redis)?
        };
        self.load_ingress_wire(&id.ok_or_else(IoError::ingress_not_found)?)
            .await
    }
    pub async fn list_ingress(&self, room: &str) -> Result<Vec<pb::IngressInfo>, IoError> {
        self.list_ingress_wire(room)
            .await
            .map(|values| values.into_iter().map(|v| v.message).collect())
    }
    pub async fn list_ingress_wire(
        &self,
        room: &str,
    ) -> Result<Vec<WireValue<pb::IngressInfo>>, IoError> {
        let mut c = self.connection.lock().await;
        let raw: Vec<Option<Vec<u8>>> = if room.is_empty() {
            let values: Vec<(String, Vec<u8>)> = redis::cmd("HGETALL")
                .arg(INGRESS)
                .query_async(&mut *c)
                .await
                .map_err(IoError::redis)?;
            values.into_iter().map(|(_, v)| Some(v)).collect()
        } else {
            let ids: Vec<String> = redis::cmd("SMEMBERS")
                .arg(format!("{ROOM_INGRESS}{room}"))
                .query_async(&mut *c)
                .await
                .map_err(IoError::redis)?;
            if ids.is_empty() {
                return Ok(Vec::new());
            }
            // This error is deliberately suppressed by fixed Go room filtering.
            redis::cmd("HMGET")
                .arg(INGRESS)
                .arg(ids)
                .query_async(&mut *c)
                .await
                .unwrap_or_default()
        };
        let mut values = Vec::new();
        for raw in raw.into_iter().flatten() {
            let mut value = WireValue::<pb::IngressInfo>::decode(&raw)?;
            let state: Option<Vec<u8>> = redis::cmd("GET")
                .arg(format!("{INGRESS_STATE}{}", value.message.ingress_id))
                .query_async(&mut *c)
                .await
                .map_err(IoError::redis)?;
            if let Some(raw) = state {
                let state = WireValue::<pb::IngressState>::decode(&raw)?;
                value.message.state = Some(state.message);
                value.wire.replace_nested("state", Some(&state.wire));
            }
            values.push(value);
        }
        Ok(values)
    }
    pub async fn store_ingress(&self, info: &pb::IngressInfo) -> Result<(), IoError> {
        self.store_ingress_wire(&WireValue::from_message(info.clone()))
            .await
    }
    pub async fn store_ingress_wire(
        &self,
        value: &WireValue<pb::IngressInfo>,
    ) -> Result<(), IoError> {
        let info = &value.message;
        self.update_ingress_wire(value).await?;
        self.update_ingress_state(&info.ingress_id, &pb::IngressState::default())
            .await
    }
    pub async fn update_ingress(&self, info: &pb::IngressInfo) -> Result<(), IoError> {
        self.update_ingress_wire(&WireValue::from_message(info.clone()))
            .await
    }
    pub async fn update_ingress_wire(
        &self,
        value: &WireValue<pb::IngressInfo>,
    ) -> Result<(), IoError> {
        let info = &value.message;
        if info.ingress_id.is_empty() {
            return Err(IoError::ordinary("Missing IngressId"));
        }
        if info.stream_key.is_empty() && info.input_type != 2 {
            return Err(IoError::ordinary("Missing StreamKey"));
        }
        let mut copy = info.clone();
        copy.state = None;
        let raw = value.wire.overlay(&copy).encode_to_vec();
        for _ in 0..5 {
            let mut c = self.transaction_connection(INGRESS).await?;
            let old = match load_info(&mut c, &info.ingress_id).await {
                Ok(v) => v,
                Err(e) if e.is_ingress_not_found() => pb::IngressInfo::default(),
                Err(e) => {
                    unwatch(&mut c).await;
                    return Err(e);
                }
            };
            let mut pipe = redis::pipe();
            pipe.atomic()
                .cmd("HSET")
                .arg(INGRESS)
                .arg(&info.ingress_id)
                .arg(&raw);
            if !info.stream_key.is_empty() {
                pipe.cmd("HSET")
                    .arg(STREAM_KEY)
                    .arg(&info.stream_key)
                    .arg(&info.ingress_id);
            }
            if old.room_name != info.room_name {
                if !old.room_name.is_empty() {
                    pipe.cmd("SREM")
                        .arg(format!("{ROOM_INGRESS}{}", old.room_name))
                        .arg(&info.ingress_id);
                }
                if !info.room_name.is_empty() {
                    pipe.cmd("SADD")
                        .arg(format!("{ROOM_INGRESS}{}", info.room_name))
                        .arg(&info.ingress_id);
                }
            }
            let result: Option<Vec<redis::Value>> =
                pipe.query_async(&mut c).await.map_err(IoError::redis)?;
            if result.is_some() {
                return Ok(());
            }
        }
        // Fixed Go exhausts optimistic retries with nil (do not claim a write).
        Ok(())
    }
    pub async fn update_ingress_state(
        &self,
        id: &str,
        state: &pb::IngressState,
    ) -> Result<(), IoError> {
        self.update_ingress_state_wire(id, &WireValue::from_message(state.clone()))
            .await
    }
    pub async fn update_ingress_state_wire(
        &self,
        id: &str,
        value: &WireValue<pb::IngressState>,
    ) -> Result<(), IoError> {
        let state = &value.message;
        if id.is_empty() {
            return Err(IoError::ordinary("Missing IngressId"));
        }
        let key = format!("{INGRESS_STATE}{id}");
        for _ in 0..5 {
            let mut c = self.transaction_connection(&key).await?;
            let old = match load_state_optional(&mut c, id).await {
                Ok(v) => v.unwrap_or_default(),
                Err(e) => {
                    unwatch(&mut c).await;
                    return Err(e);
                }
            };
            match state_order(&old, state) {
                StateOrder::OlderSession => {
                    unwatch(&mut c).await;
                    return Err(IoError::out_of_date());
                }
                StateOrder::OlderUpdate => {
                    unwatch(&mut c).await;
                    return Ok(());
                }
                StateOrder::Write => {}
            }
            let result: Option<Vec<redis::Value>> = redis::pipe()
                .atomic()
                .cmd("SET")
                .arg(&key)
                .arg(value.encode())
                .query_async(&mut c)
                .await
                .map_err(IoError::redis)?;
            if result.is_some() {
                return Ok(());
            }
        }
        Ok(())
    }
    pub async fn delete_ingress(&self, info: &pb::IngressInfo) -> Result<(), IoError> {
        let mut c = self.connection.lock().await;
        let mut pipe = redis::pipe();
        pipe.atomic()
            .cmd("SREM")
            .arg(format!("{ROOM_INGRESS}{}", info.room_name))
            .arg(&info.ingress_id);
        if !info.stream_key.is_empty() {
            pipe.cmd("HDEL").arg(STREAM_KEY).arg(&info.stream_key);
        }
        pipe.cmd("HDEL")
            .arg(INGRESS)
            .arg(&info.ingress_id)
            .cmd("DEL")
            .arg(format!("{INGRESS_STATE}{}", info.ingress_id));
        pipe.query_async::<Vec<redis::Value>>(&mut *c)
            .await
            .map_err(|e| IoError::redis(e).context("could not delete ingress info"))?;
        Ok(())
    }
}
async fn unwatch(c: &mut MultiplexedConnection) {
    let _ = redis::cmd("UNWATCH").query_async::<()>(c).await;
}
async fn load_info(c: &mut MultiplexedConnection, id: &str) -> Result<pb::IngressInfo, IoError> {
    let raw: Option<Vec<u8>> = redis::cmd("HGET")
        .arg(INGRESS)
        .arg(id)
        .query_async(c)
        .await
        .map_err(IoError::redis)?;
    decode(raw.ok_or_else(IoError::ingress_not_found)?)
}
async fn load_state_optional(
    c: &mut MultiplexedConnection,
    id: &str,
) -> Result<Option<pb::IngressState>, IoError> {
    let raw: Option<Vec<u8>> = redis::cmd("GET")
        .arg(format!("{INGRESS_STATE}{id}"))
        .query_async(c)
        .await
        .map_err(IoError::redis)?;
    raw.map(decode).transpose()
}
fn decode<T: Message + Default + prost::Name>(raw: Vec<u8>) -> Result<T, IoError> {
    // Transaction reloads obey the same Go-compatible known projection as
    // ordinary reads. A legal unknown with a known field's number but another
    // wire kind must not make a previously accepted stored object uneditable.
    lk_protocol::wire::decode::<T>(&raw).map_err(|_| {
        use lk_protocol::json::wire_error::{WireDecodeFailure, classify};
        if classify(&T::full_name(), &raw) == WireDecodeFailure::InvalidUtf8 {
            IoError::ordinary("string field contains invalid UTF-8")
        } else {
            IoError::ordinary("proto: cannot parse invalid wire-format data")
        }
    })
}
#[derive(Debug, PartialEq, Eq)]
enum StateOrder {
    OlderSession,
    OlderUpdate,
    Write,
}
fn state_order(old: &pb::IngressState, new: &pb::IngressState) -> StateOrder {
    if new.started_at < old.started_at {
        StateOrder::OlderSession
    } else if new.started_at == old.started_at && new.updated_at < old.updated_at {
        StateOrder::OlderUpdate
    } else {
        StateOrder::Write
    }
}
fn parse_ended(value: &str) -> Result<(&str, i64), IoError> {
    let parts: Vec<_> = value.split('|').collect();
    if parts.len() != 2 {
        return Err(IoError::ordinary("invalid egressEnded value"));
    }
    let at = parts[1]
        .parse()
        .map_err(|_| IoError::ordinary("invalid endedAt"))?;
    Ok((parts[0], at))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn transactional_projection_accepts_legal_known_number_wrong_wire_unknowns() {
        let expected = pb::IngressInfo {
            ingress_id: "IN_transaction_fixture".into(),
            audio: Some(pb::IngressAudioOptions {
                name: "current audio name".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut child = expected.audio.as_ref().unwrap().encode_to_vec();
        // AudioOptions.name#1 is a string. Go treats a varint with that same
        // field number as a legal unknown, including in transaction reloads.
        child.extend([0x08, 7]);
        let mut value = WireValue::from_message(expected.clone());
        value.wire.replace_nested(
            "audio",
            Some(
                &crate::sip::wire::PreservedMessage::decode("livekit.IngressAudioOptions", &child)
                    .unwrap(),
            ),
        );
        assert_eq!(decode::<pb::IngressInfo>(value.encode()).unwrap(), expected);
        // Filtering legal unknowns never permits corrupt framing or UTF-8.
        assert!(decode::<pb::IngressInfo>(vec![0x0a, 4, b'x']).is_err());
        assert_eq!(
            decode::<pb::IngressInfo>(vec![0x0a, 1, 0xff])
                .unwrap_err()
                .message,
            "string field contains invalid UTF-8"
        );
    }
    #[test]
    fn state_order_does_not_overwrite_new_session_or_newer_update() {
        let old = pb::IngressState {
            started_at: 9,
            updated_at: 20,
            ..Default::default()
        };
        assert_eq!(
            state_order(
                &old,
                &pb::IngressState {
                    started_at: 8,
                    updated_at: 50,
                    ..Default::default()
                }
            ),
            StateOrder::OlderSession
        );
        assert_eq!(
            state_order(
                &old,
                &pb::IngressState {
                    started_at: 9,
                    updated_at: 19,
                    ..Default::default()
                }
            ),
            StateOrder::OlderUpdate
        );
        assert_eq!(state_order(&old, &old), StateOrder::Write);
        assert_eq!(parse_ended("room|123").unwrap(), ("room", 123));
        assert!(parse_ended("room|a|123").is_err());
    }
}

#[cfg(test)]
#[path = "store_cluster_tests.rs"]
mod cluster_tests;
