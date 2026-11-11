//! Persistent SIP configuration catalog, matching the pinned Go RedisStore.
//! Domain validation and public authorization belong to the caller.
use lk_protocol::livekit::*;
use prost::Message;
use redis::aio::MultiplexedConnection;
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;

use crate::redis_endpoint::{RedisEndpoint, SharedConnection};

mod go_sort;
mod preserved;

const LEGACY: &str = "sip_trunk";
const INBOUND: &str = "sip_inbound_trunk";
const OUTBOUND: &str = "sip_outbound_trunk";
const RULE: &str = "sip_dispatch_rule";

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("id is not set")]
    EmptyId,
    #[error("requested sip trunk does not exist")]
    TrunkNotFound,
    #[error("requested sip dispatch rule does not exist")]
    RuleNotFound,
    #[error("Redis SIP catalog operation failed")]
    Redis(#[source] redis::RedisError),
    #[error("stored SIP protobuf is invalid")]
    Decode(#[source] prost::DecodeError),
    #[error("stored SIP protobuf contains invalid UTF-8")]
    DecodeUtf8(#[source] prost::DecodeError),
    #[error("stored SIP protobuf provenance cannot be retained")]
    Provenance(#[source] crate::sip::wire::WireError),
    #[error("invalid SIP catalog connection configuration")]
    Configuration,
    #[error("Redis SIP catalog operation timed out")]
    Timeout,
}

impl CatalogError {
    /// Fixed-Go public boundary text for stored data and actual Redis server replies.
    /// Local transport-policy errors retain their own honest local descriptions.
    pub fn compatible_public_message(&self) -> String {
        match self {
            Self::Decode(_) => "proto: cannot parse invalid wire-format data".into(),
            Self::DecodeUtf8(_) => "string field contains invalid UTF-8".into(),
            Self::Redis(error) => match (error.code(), error.detail()) {
                (Some(code), Some(detail)) if !detail.is_empty() => format!("{code} {detail}"),
                (Some(code), _) => code.to_owned(),
                // A non-server I/O/parser error is not a Redis error reply. Do
                // not invent a server code or claim Go socket text equivalence.
                _ => self.to_string(),
            },
            _ => self.to_string(),
        }
    }
}

#[derive(Clone)]
pub struct SipCatalog {
    connection: Arc<SharedConnection>,
    commands: Arc<Semaphore>,
    timeout: Duration,
}

impl SipCatalog {
    pub async fn connect(
        url: &str,
        timeout: Duration,
        max_commands: usize,
    ) -> Result<Self, CatalogError> {
        let info =
            redis::IntoConnectionInfo::into_connection_info(url).map_err(CatalogError::Redis)?;
        Self::connect_endpoint(RedisEndpoint::simple(info), timeout, max_commands).await
    }

    /// Native Redis endpoint plus separate credentials, never interpolated into logs.
    pub async fn connect_native(
        address: &str,
        username: &str,
        password: &str,
        db: i64,
        timeout: Duration,
        max_commands: usize,
    ) -> Result<Self, CatalogError> {
        let info = redis::IntoConnectionInfo::into_connection_info(format!("redis://{address}/"))
            .map_err(CatalogError::Redis)?;
        let mut settings = info.redis_settings().clone().set_db(db);
        if !username.is_empty() {
            settings = settings.set_username(username);
        }
        if !password.is_empty() {
            settings = settings.set_password(password);
        }
        Self::connect_endpoint(
            RedisEndpoint::simple(info.set_redis_settings(settings)),
            timeout,
            max_commands,
        )
        .await
    }

    /// SPEC-0074 C11: connect through a Redis endpoint. A Sentinel endpoint
    /// resolves the current master for each dial and re-resolves after a failed
    /// command connection/READONLY reply. Proactive `+switch-master` migration
    /// remains open (SPEC0077 G07); successful reads can still use the old replica.
    pub async fn connect_endpoint(
        endpoint: RedisEndpoint,
        timeout: Duration,
        max_commands: usize,
    ) -> Result<Self, CatalogError> {
        if timeout.is_zero() || max_commands == 0 || max_commands > Semaphore::MAX_PERMITS {
            return Err(CatalogError::Configuration);
        }
        let config = redis::AsyncConnectionConfig::new()
            .set_connection_timeout(Some(timeout))
            .set_response_timeout(Some(timeout))
            .set_pipeline_buffer_size(max_commands)
            .set_concurrency_limit(max_commands);
        let connection = endpoint
            .connect(&config)
            .await
            .map_err(CatalogError::Redis)?;
        Ok(Self {
            connection: Arc::new(connection),
            commands: Arc::new(Semaphore::new(max_commands)),
            timeout,
        })
    }

    /// Supplied connections retain their own Redis driver policy. The facade
    /// bounds concurrent commands and includes permit wait in the timeout.
    pub fn from_connection(
        connection: MultiplexedConnection,
        timeout: Duration,
        max_commands: usize,
    ) -> Result<Self, CatalogError> {
        if timeout.is_zero() || max_commands == 0 || max_commands > Semaphore::MAX_PERMITS {
            return Err(CatalogError::Configuration);
        }
        Ok(Self {
            connection: Arc::new(SharedConnection::from_connection(connection)),
            commands: Arc::new(Semaphore::new(max_commands)),
            timeout,
        })
    }

    async fn query<T: redis::FromRedisValue>(
        &self,
        command: &mut redis::Cmd,
    ) -> Result<T, CatalogError> {
        tokio::time::timeout(self.timeout, async {
            let _permit = self
                .commands
                .acquire()
                .await
                .map_err(|_| CatalogError::Configuration)?;
            command
                .query_async(&mut self.connection.begin())
                .await
                .map_err(CatalogError::Redis)
        })
        .await
        .map_err(|_| CatalogError::Timeout)?
    }

    async fn store<T: Message>(&self, key: &str, id: &str, value: &T) -> Result<(), CatalogError> {
        if id.is_empty() {
            return Err(CatalogError::EmptyId);
        }
        let _: usize = self
            .query(
                redis::cmd("HSET")
                    .arg(key)
                    .arg(id)
                    .arg(value.encode_to_vec()),
            )
            .await?;
        Ok(())
    }

    async fn load<T: Message + prost::Name + Default>(
        &self,
        key: &str,
        id: &str,
    ) -> Result<Option<T>, CatalogError> {
        let data: Option<Vec<u8>> = self.query(redis::cmd("HGET").arg(key).arg(id)).await?;
        data.map(|v| decode_stored::<T>(&v)).transpose()
    }

    async fn page<T: Message + prost::Name + Default>(
        &self,
        key: &str,
        page: Option<&Pagination>,
    ) -> Result<Vec<T>, CatalogError> {
        let values: Vec<Vec<u8>> = match page {
            None => self.query(redis::cmd("HVALS").arg(key)).await?,
            Some(page) => {
                let mut ids: Vec<String> = self.query(redis::cmd("HKEYS").arg(key)).await?;
                ids.sort();
                let start = if page.after_id.is_empty() {
                    0
                } else {
                    ids.partition_point(|id| id <= &page.after_id)
                };
                let limit = if page.limit > 0 {
                    page.limit as usize
                } else {
                    1000
                };
                let ids: Vec<_> = ids.into_iter().skip(start).take(limit).collect();
                if ids.is_empty() {
                    return Ok(Vec::new());
                }
                let data: Vec<Option<Vec<u8>>> =
                    self.query(redis::cmd("HMGET").arg(key).arg(ids)).await?;
                data.into_iter().flatten().collect()
            }
        };
        values.into_iter().map(|v| decode_stored::<T>(&v)).collect()
    }

    pub async fn store_legacy(&self, value: &SipTrunkInfo) -> Result<(), CatalogError> {
        self.store(LEGACY, &value.sip_trunk_id, value).await
    }
    pub async fn store_inbound(&self, value: &SipInboundTrunkInfo) -> Result<(), CatalogError> {
        self.store(INBOUND, &value.sip_trunk_id, value).await
    }
    pub async fn store_outbound(&self, value: &SipOutboundTrunkInfo) -> Result<(), CatalogError> {
        self.store(OUTBOUND, &value.sip_trunk_id, value).await
    }
    pub async fn store_rule(&self, value: &SipDispatchRuleInfo) -> Result<(), CatalogError> {
        self.store(RULE, &value.sip_dispatch_rule_id, value).await
    }
    pub async fn load_legacy(&self, id: &str) -> Result<SipTrunkInfo, CatalogError> {
        if let Some(v) = self.load(LEGACY, id).await? {
            return Ok(v);
        }
        if let Some(v) = self.load(INBOUND, id).await? {
            return Ok(inbound_as_legacy(&v));
        }
        if let Some(v) = self.load(OUTBOUND, id).await? {
            return Ok(outbound_as_legacy(&v));
        }
        Err(CatalogError::TrunkNotFound)
    }
    // None specifically preserves Go's (nil,nil) for opposite-kind legacy trunks.
    // Missing Redis entries instead return TrunkNotFound.
    pub async fn load_inbound(
        &self,
        id: &str,
    ) -> Result<Option<SipInboundTrunkInfo>, CatalogError> {
        if let Some(v) = self.load(INBOUND, id).await? {
            return Ok(Some(v));
        }
        let v = self
            .load(LEGACY, id)
            .await?
            .ok_or(CatalogError::TrunkNotFound)?;
        Ok(legacy_as_inbound(&v))
    }
    pub async fn load_outbound(
        &self,
        id: &str,
    ) -> Result<Option<SipOutboundTrunkInfo>, CatalogError> {
        if let Some(v) = self.load(OUTBOUND, id).await? {
            return Ok(Some(v));
        }
        let v = self
            .load(LEGACY, id)
            .await?
            .ok_or(CatalogError::TrunkNotFound)?;
        Ok(legacy_as_outbound(&v))
    }
    pub async fn load_rule(&self, id: &str) -> Result<SipDispatchRuleInfo, CatalogError> {
        self.load(RULE, id).await?.ok_or(CatalogError::RuleNotFound)
    }
    pub async fn delete_trunk(&self, id: &str) -> Result<(), CatalogError> {
        // Go issues all three commands even when the first fails, then returns
        // the first error. This intentionally is not an invented transaction.
        let mut first = None;
        for key in [LEGACY, INBOUND, OUTBOUND] {
            let result: Result<usize, CatalogError> =
                self.query(redis::cmd("HDEL").arg(key).arg(id)).await;
            if let Err(error) = result
                && first.is_none()
            {
                first = Some(error);
            }
        }
        first.map_or(Ok(()), Err)
    }
    pub async fn delete_rule(&self, id: &str) -> Result<(), CatalogError> {
        let _: usize = self.query(redis::cmd("HDEL").arg(RULE).arg(id)).await?;
        Ok(())
    }
    pub async fn list_legacy(
        &self,
        req: &ListSipTrunkRequest,
    ) -> Result<Vec<SipTrunkInfo>, CatalogError> {
        let page = req.page.as_ref();
        let mut values: Vec<SipTrunkInfo> = self.page(LEGACY, page).await?;
        values.extend(
            self.page::<SipInboundTrunkInfo>(INBOUND, page)
                .await?
                .iter()
                .map(inbound_as_legacy),
        );
        values.extend(
            self.page::<SipOutboundTrunkInfo>(OUTBOUND, page)
                .await?
                .iter()
                .map(outbound_as_legacy),
        );
        values.retain(|v| after(page, &v.sip_trunk_id));
        go_sort::sort_by(&mut values, |a, b| a.sip_trunk_id.cmp(&b.sip_trunk_id));
        truncate(&mut values, page);
        Ok(values)
    }
    pub async fn list_inbound(
        &self,
        req: &ListSipInboundTrunkRequest,
    ) -> Result<Vec<Option<SipInboundTrunkInfo>>, CatalogError> {
        let page = req.page.as_ref();
        let mut values: Vec<_> = self
            .page::<SipInboundTrunkInfo>(INBOUND, page)
            .await?
            .into_iter()
            .map(Some)
            .collect();
        values.extend(
            self.page::<SipTrunkInfo>(LEGACY, page)
                .await?
                .iter()
                .map(legacy_as_inbound),
        );
        values.retain(|v| {
            v.as_ref()
                .is_none_or(|v| inbound_filter(req, v) && after(page, &v.sip_trunk_id))
        });
        go_sort::sort_by(&mut values, |a, b| {
            optional_trunk_id(a.as_ref().map(|v| v.sip_trunk_id.as_str())).cmp(optional_trunk_id(
                b.as_ref().map(|v| v.sip_trunk_id.as_str()),
            ))
        });
        truncate(&mut values, page);
        Ok(values)
    }
    pub async fn list_outbound(
        &self,
        req: &ListSipOutboundTrunkRequest,
    ) -> Result<Vec<Option<SipOutboundTrunkInfo>>, CatalogError> {
        let page = req.page.as_ref();
        let mut values: Vec<_> = self
            .page::<SipOutboundTrunkInfo>(OUTBOUND, page)
            .await?
            .into_iter()
            .map(Some)
            .collect();
        values.extend(
            self.page::<SipTrunkInfo>(LEGACY, page)
                .await?
                .iter()
                .map(legacy_as_outbound),
        );
        values.retain(|v| {
            v.as_ref()
                .is_none_or(|v| outbound_filter(req, v) && after(page, &v.sip_trunk_id))
        });
        go_sort::sort_by(&mut values, |a, b| {
            optional_trunk_id(a.as_ref().map(|v| v.sip_trunk_id.as_str())).cmp(optional_trunk_id(
                b.as_ref().map(|v| v.sip_trunk_id.as_str()),
            ))
        });
        truncate(&mut values, page);
        Ok(values)
    }
    pub async fn list_rules(
        &self,
        req: &ListSipDispatchRuleRequest,
    ) -> Result<Vec<SipDispatchRuleInfo>, CatalogError> {
        let page = req.page.as_ref();
        let mut values: Vec<SipDispatchRuleInfo> = self.page(RULE, page).await?;
        values.retain(|v| rule_filter(req, v) && after(page, &v.sip_dispatch_rule_id));
        go_sort::sort_by(&mut values, |a, b| {
            a.sip_dispatch_rule_id.cmp(&b.sip_dispatch_rule_id)
        });
        truncate(&mut values, page);
        Ok(values)
    }
}

fn decode_stored<T: Message + prost::Name + Default>(bytes: &[u8]) -> Result<T, CatalogError> {
    lk_protocol::wire::decode(bytes).map_err(|error| {
        let source = match error {
            lk_protocol::wire::WireError::KnownValue(source) => source,
            error => prost::DecodeError::new(error.to_string()),
        };
        match lk_protocol::json::wire_error::classify(&T::full_name(), bytes) {
            lk_protocol::json::wire_error::WireDecodeFailure::InvalidUtf8 => {
                CatalogError::DecodeUtf8(source)
            }
            lk_protocol::json::wire_error::WireDecodeFailure::Malformed => {
                CatalogError::Decode(source)
            }
        }
    })
}

fn optional_trunk_id(id: Option<&str>) -> &str {
    id.unwrap_or("")
}
fn after(page: Option<&Pagination>, id: &str) -> bool {
    page.is_none_or(|p| p.after_id.is_empty() || id.is_empty() || id > p.after_id.as_str())
}
fn truncate<T>(values: &mut Vec<T>, page: Option<&Pagination>) {
    if let Some(p) = page
        && p.limit > 0
    {
        values.truncate(p.limit as usize);
    }
}

pub fn legacy_as_inbound(v: &SipTrunkInfo) -> Option<SipInboundTrunkInfo> {
    if v.kind == 2 {
        return None;
    }
    Some(SipInboundTrunkInfo {
        sip_trunk_id: v.sip_trunk_id.clone(),
        name: v.name.clone(),
        metadata: v.metadata.clone(),
        numbers: if v.outbound_number.is_empty() {
            vec![]
        } else {
            vec![v.outbound_number.clone()]
        },
        allowed_addresses: v.inbound_addresses.clone(),
        allowed_numbers: v.inbound_numbers.clone(),
        auth_username: v.inbound_username.clone(),
        auth_password: v.inbound_password.clone(),
        ..Default::default()
    })
}
pub fn legacy_as_outbound(v: &SipTrunkInfo) -> Option<SipOutboundTrunkInfo> {
    if v.kind == 1 {
        return None;
    }
    Some(SipOutboundTrunkInfo {
        sip_trunk_id: v.sip_trunk_id.clone(),
        name: v.name.clone(),
        metadata: v.metadata.clone(),
        numbers: if v.outbound_number.is_empty() {
            vec![]
        } else {
            vec![v.outbound_number.clone()]
        },
        address: v.outbound_address.clone(),
        transport: v.transport,
        auth_username: v.outbound_username.clone(),
        auth_password: v.outbound_password.clone(),
        ..Default::default()
    })
}
pub fn inbound_as_legacy(v: &SipInboundTrunkInfo) -> SipTrunkInfo {
    SipTrunkInfo {
        sip_trunk_id: v.sip_trunk_id.clone(),
        kind: 1,
        name: v.name.clone(),
        metadata: v.metadata.clone(),
        outbound_number: v.numbers.first().cloned().unwrap_or_default(),
        inbound_addresses: v.allowed_addresses.clone(),
        inbound_numbers: v.allowed_numbers.clone(),
        inbound_username: v.auth_username.clone(),
        inbound_password: v.auth_password.clone(),
        ..Default::default()
    }
}
pub fn outbound_as_legacy(v: &SipOutboundTrunkInfo) -> SipTrunkInfo {
    SipTrunkInfo {
        sip_trunk_id: v.sip_trunk_id.clone(),
        kind: 2,
        name: v.name.clone(),
        metadata: v.metadata.clone(),
        outbound_number: v.numbers.first().cloned().unwrap_or_default(),
        outbound_address: v.address.clone(),
        transport: v.transport,
        outbound_username: v.auth_username.clone(),
        outbound_password: v.auth_password.clone(),
        ..Default::default()
    }
}

pub fn normalize_number(value: &str) -> String {
    let digits = value.strip_prefix('+').unwrap_or(value);
    if digits.is_empty()
        || !digits
            .bytes()
            .all(|b| b.is_ascii_digit() || b"- ()".contains(&b))
    {
        return value.to_owned();
    }
    let stripped: String = value
        .chars()
        .filter(|c| !matches!(c, ' ' | '-' | '(' | ')'))
        .collect();
    if stripped.starts_with('+') {
        stripped
    } else {
        format!("+{stripped}")
    }
}
pub fn inbound_filter(req: &ListSipInboundTrunkRequest, v: &SipInboundTrunkInfo) -> bool {
    (req.trunk_ids.is_empty() || req.trunk_ids.contains(&v.sip_trunk_id))
        && (req.numbers.is_empty()
            || v.numbers.is_empty()
            || v.numbers.iter().any(|n| {
                req.numbers
                    .iter()
                    .any(|r| r == n || normalize_number(r) == normalize_number(n))
            }))
}
pub fn outbound_filter(req: &ListSipOutboundTrunkRequest, v: &SipOutboundTrunkInfo) -> bool {
    (req.trunk_ids.is_empty() || req.trunk_ids.contains(&v.sip_trunk_id))
        && (req.numbers.is_empty()
            || v.numbers.is_empty()
            || v.numbers.iter().any(|n| req.numbers.contains(n)))
}
pub fn rule_filter(req: &ListSipDispatchRuleRequest, v: &SipDispatchRuleInfo) -> bool {
    (req.dispatch_rule_ids.is_empty() || req.dispatch_rule_ids.contains(&v.sip_dispatch_rule_id))
        && (req.trunk_ids.is_empty()
            || v.trunk_ids.is_empty()
            || v.trunk_ids.iter().any(|id| req.trunk_ids.contains(id)))
}

#[cfg(test)]
mod tests;
