//! Fixed LiveKit public SIP call gateway over real SIPInternal PSRPC.
//! No retries: fixed
//! service/wire.go intentionally strips generic timeout/retry for SIP calls.
#![allow(deprecated)]
use super::preserved::WireValue;
use crate::{
    auth::{
        VerifiedToken,
        permissions::{ensure_admin, ensure_sip_call},
    },
    service_bus::rpc::{
        CallOptions, MethodSpec, RawRequest, RpcClient, RpcError, RpcErrorOrigin, SelectionOptions,
    },
    sip::{
        catalog::{CatalogError, SipCatalog},
        domain,
        wire::{PreservedMessage, Stored},
    },
};
use futures_util::future::BoxFuture;
use lk_protocol::{livekit as pb, rpc};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
mod countries;
mod validation;

pub trait SipRooms: Send + Sync {
    /// Actual RoomService owner lookup after the gateway checked room-admin.
    fn participant(
        &self,
        room: &str,
        identity: &str,
    ) -> BoxFuture<'_, Result<pb::ParticipantInfo, RpcError>>;
}
pub struct SipGateway {
    client: Option<Arc<RpcClient>>,
    catalog: Option<SipCatalog>,
    rooms: Arc<dyn SipRooms>,
}
impl SipGateway {
    pub fn new(
        client: Option<Arc<RpcClient>>,
        catalog: Option<SipCatalog>,
        rooms: Arc<dyn SipRooms>,
    ) -> Self {
        Self {
            client,
            catalog,
            rooms,
        }
    }
    pub async fn create_wire(
        &self,
        auth: Option<&VerifiedToken>,
        mut value: WireValue<pb::CreateSipParticipantRequest>,
        mut options: CallOptions,
    ) -> Result<WireValue<pb::SipParticipantInfo>, RpcError> {
        call_permission(auth)?;
        let catalog = self.catalog.as_ref().ok_or_else(disconnected)?;
        upgrade(&mut value.message);
        let call_id = rtc_core::ids::new_sid("SCL_");
        validation::create(&value.message)?;
        let mut trunk = if value.message.sip_trunk_id.is_empty() {
            None
        } else {
            caller_io(&options, async {
                catalog
                    .load_outbound_preserved(&value.message.sip_trunk_id)
                    .await
                    .map_err(store_error)
            })
            .await?
        };
        if let Some(t) = trunk.as_mut() {
            domain::upgrade_outbound(&mut t.value);
        }
        let request = map_create(&value, trunk.as_ref(), call_id, choose_number)?;
        let timeout = if value.message.wait_until_answered {
            80
        } else {
            30
        };
        resolve_create_timeout(&mut options, Duration::from_secs(timeout), Instant::now())?;
        // Fixed getRequestOpts for a non-affinity queued method accepts the
        // first eligible claim; waiting for the overall call deadline never
        // grants the real SIP worker and would prevent INVITE/REFER execution.
        options.selection = SelectionOptions {
            accept_first: true,
            affinity_timeout: Some(Duration::from_secs(1)),
            ..Default::default()
        };
        let client = self.client.as_deref().ok_or_else(disconnected)?;
        let raw = RawRequest {
            message: request.message.clone(),
            raw: Arc::from(request.encode()),
        };
        let result = client
            .unary_raw_request::<_, rpc::InternalCreateSipParticipantResponse>(
                &method("CreateSIPParticipant", ""),
                &raw,
                options,
            )
            .await?;
        // Fixed source deliberately constructs the public response, not a clone
        // of the internal response (including a peer's differing SipCallID).
        Ok(WireValue::from_message(pb::SipParticipantInfo {
            participant_id: result.message.participant_id,
            participant_identity: result.message.participant_identity,
            room_name: value.message.room_name,
            sip_call_id: request.message.sip_call_id,
        }))
    }
    pub async fn transfer_wire(
        &self,
        auth: Option<&VerifiedToken>,
        mut value: WireValue<pb::TransferSipParticipantRequest>,
        mut options: CallOptions,
    ) -> Result<WireValue<pb::TransferSipParticipantResponse>, RpcError> {
        transfer_preflight(auth, &value.message)?;
        validation::transfer(&mut value.message)?;
        let participant = caller_io(
            &options,
            self.rooms.participant(
                &value.message.room_name,
                &value.message.participant_identity,
            ),
        )
        .await?;
        let call_id = participant
            .attributes
            .get("sip.callID")
            .ok_or_else(|| {
                RpcError::new(
                    "invalid_argument",
                    "no SIP session associated with participant",
                )
            })?
            .clone();
        let mut request = WireValue::from_message(rpc::InternalTransferSipParticipantRequest {
            sip_call_id: call_id.clone(),
            transfer_to: value.message.transfer_to,
            play_dialtone: value.message.play_dialtone,
            headers: value.message.headers,
            ringing_timeout: value.message.ringing_timeout,
            ..Default::default()
        });
        // Only a genuine same-type child may inherit provenance. The effective
        // timeout itself is intentionally replaced below by source contract.
        let timeout =
            signed_duration(request.message.ringing_timeout.as_ref()).unwrap_or(30_000_000_000);
        let timeout = resolve_transfer_timeout(&mut options, timeout, Instant::now())?;
        request.message.ringing_timeout = Some(pbjson_types::Duration {
            seconds: (timeout / 1_000_000_000) as i64,
            nanos: (timeout % 1_000_000_000) as i32,
        });
        // Fixed getRequestOpts for a non-affinity queued method accepts the
        // first eligible claim; waiting for the overall call deadline never
        // grants the real SIP worker and would prevent INVITE/REFER execution.
        options.selection = SelectionOptions {
            accept_first: true,
            affinity_timeout: Some(Duration::from_secs(1)),
            ..Default::default()
        };
        let client = self.client.as_deref().ok_or_else(disconnected)?;
        let raw = RawRequest {
            message: request.message.clone(),
            raw: Arc::from(request.encode()),
        };
        let result = client
            .unary_raw_request::<_, rpc::InternalTransferSipParticipantResponse>(
                &method("TransferSIPParticipant", &call_id),
                &raw,
                options,
            )
            .await?;
        let internal: WireValue<rpc::InternalTransferSipParticipantResponse> =
            WireValue::decode(&result.raw).map_err(RpcError::from)?;
        let mut response = WireValue::from_message(pb::TransferSipParticipantResponse {
            transfer_id: internal.message.transfer_id,
            status: internal.message.status,
            reason: internal.message.reason,
            sip_status: internal.message.sip_status,
        });
        response
            .wire
            .replace_nested("sip_status", internal.wire.nested("sip_status").as_ref());
        Ok(response)
    }
}
fn call_permission(auth: Option<&VerifiedToken>) -> Result<(), RpcError> {
    ensure_sip_call(auth).map_err(|e| RpcError::new("unauthenticated", e.to_string()))
}
fn transfer_preflight(
    auth: Option<&VerifiedToken>,
    req: &pb::TransferSipParticipantRequest,
) -> Result<(), RpcError> {
    if req.room_name.is_empty() {
        return Err(RpcError::new("invalid_argument", "Missing room name"));
    }
    if req.participant_identity.is_empty() {
        return Err(RpcError::new(
            "invalid_argument",
            "Missing participant identity",
        ));
    }
    call_permission(auth)?;
    ensure_admin(auth, &req.room_name).map_err(|e| RpcError::new("unauthenticated", e.to_string()))
}
fn method(name: &str, topic: &str) -> MethodSpec {
    MethodSpec {
        service: "SIPInternal".into(),
        method: name.into(),
        topics: vec![topic.into()],
        require_claim: true,
        queue: true,
    }
}
fn disconnected() -> RpcError {
    RpcError::new("internal", "sip not connected (redis required)")
}
fn store_error(error: CatalogError) -> RpcError {
    RpcError::new(
        if matches!(error, CatalogError::TrunkNotFound) {
            "not_found"
        } else {
            "unknown"
        },
        error.compatible_public_message(),
    )
}
fn upgrade(req: &mut pb::CreateSipParticipantRequest) {
    let media = req.media.get_or_insert_with(Default::default);
    if media.encryption.is_none() && req.media_encryption != 0 {
        media.encryption = Some(req.media_encryption);
    }
}
/// Rejection sampling, not biased modulo selection and not first-number fallback.
fn choose_number(count: usize) -> Result<usize, RpcError> {
    let count = count as u64;
    let minimum = count.wrapping_neg() % count;
    loop {
        let mut bytes = [0u8; 8];
        getrandom::fill(&mut bytes).map_err(|e| RpcError::new("unknown", e.to_string()))?;
        let n = u64::from_le_bytes(bytes);
        if n >= minimum {
            return Ok((n % count) as usize);
        }
    }
}
fn map_create(
    value: &WireValue<pb::CreateSipParticipantRequest>,
    trunk: Option<&Stored<pb::SipOutboundTrunkInfo>>,
    call_id: String,
    pick: impl FnOnce(usize) -> Result<usize, RpcError>,
) -> Result<WireValue<rpc::InternalCreateSipParticipantRequest>, RpcError> {
    let hostname = trunk
        .filter(|t| !t.from_host.is_empty())
        .map(|t| t.from_host.clone())
        .or_else(|| {
            value
                .message
                .trunk
                .as_ref()
                .filter(|t| !t.from_host.is_empty())
                .map(|t| t.from_host.clone())
        })
        .unwrap_or_default();
    map_create_with_hostname(value, trunk, call_id, &hostname, pick)
}
/// Protocol mapper context is independent of service-level trunk/inline selection.
/// Fixed rpc.NewCreateSIPParticipantRequestResult receives fromHostname explicitly.
fn map_create_with_hostname(
    value: &WireValue<pb::CreateSipParticipantRequest>,
    trunk: Option<&Stored<pb::SipOutboundTrunkInfo>>,
    call_id: String,
    from_hostname: &str,
    pick: impl FnOnce(usize) -> Result<usize, RpcError>,
) -> Result<WireValue<rpc::InternalCreateSipParticipantRequest>, RpcError> {
    let req = &value.message;
    validation::create(req)?; // NewCreateSIPParticipantRequestResult revalidates.
    let mut out = rpc::InternalCreateSipParticipantRequest::default();
    if let Some(t) = trunk {
        out.address = t.address.clone();
        out.transport = t.transport;
        out.destination_country = t.destination_country.clone();
        out.username = t.auth_username.clone();
        out.password = t.auth_password.clone();
        out.headers = t.headers.clone();
        out.include_headers = t.include_headers;
        out.headers_to_attributes = t.headers_to_attributes.clone();
        out.attributes_to_headers = t.attributes_to_headers.clone();
        out.media = t.media.clone();
    } else if let Some(t) = &req.trunk {
        // The fixed mapping intentionally does not inherit inline media/headers.
        out.address = t.hostname.clone();
        out.transport = t.transport;
        out.destination_country = t.destination_country.clone();
        out.username = t.auth_username.clone();
        out.password = t.auth_password.clone();
        out.headers_to_attributes = t.headers_to_attributes.clone();
        out.attributes_to_headers = t.attributes_to_headers.clone();
    }
    out.hostname = from_hostname.to_owned();
    out.number = req.sip_number.clone();
    if out.number.is_empty() {
        let numbers = trunk
            .map(|t| t.numbers.as_slice())
            .filter(|n| !n.is_empty())
            .ok_or_else(|| RpcError::new("failed_precondition", "no numbers on outbound trunk"))?;
        out.number = numbers[pick(numbers.len())?].clone();
    }
    if out.address.ends_with("twilio.com") && !out.number.starts_with('+') {
        out.number.insert(0, '+');
    }
    out.sip_call_id = call_id;
    out.sip_trunk_id = if req.sip_trunk_id.is_empty() {
        trunk.map(|t| t.sip_trunk_id.clone()).unwrap_or_default()
    } else {
        req.sip_trunk_id.clone()
    };
    out.participant_attributes = req.participant_attributes.clone();
    out.participant_attributes
        .insert("sip.callID".into(), out.sip_call_id.clone());
    out.participant_attributes
        .insert("sip.trunkID".into(), out.sip_trunk_id.clone());
    if !req.hide_phone_number {
        out.participant_attributes
            .insert("sip.phoneNumber".into(), req.sip_call_to.clone());
        out.participant_attributes
            .insert("sip.hostname".into(), out.address.clone());
        out.participant_attributes
            .insert("sip.trunkPhoneNumber".into(), out.number.clone());
    }
    out.headers.extend(req.headers.clone());
    if req.include_headers != 0 {
        out.include_headers = req.include_headers;
    }
    if req.krisp_enabled {
        out.enabled_features.push(1);
    }
    out.participant_identity = if req.participant_identity.is_empty() {
        format!("sip_{}", req.sip_call_to)
    } else {
        req.participant_identity.clone()
    };
    out.media = domain::merge_media(out.media.as_ref(), req.media.as_ref());
    out.media_encryption = out
        .media
        .as_ref()
        .and_then(|m| m.encryption)
        .unwrap_or_default();
    out.sip_request_uri = req.sip_request_uri.clone();
    out.sip_from_header = req.sip_from_header.clone();
    out.sip_to_header = req.sip_to_header.clone();
    out.to_user_override = req.to_user_override.clone();
    out.call_to = req.sip_call_to.clone();
    out.room_name = req.room_name.clone();
    out.participant_name = req.participant_name.clone();
    out.participant_metadata = req.participant_metadata.clone();
    out.dtmf = req.dtmf.clone();
    out.play_dialtone = req.play_dialtone || req.play_ringtone;
    out.ringing_timeout = req.ringing_timeout;
    out.max_call_duration = req.max_call_duration;
    out.wait_until_answered = req.wait_until_answered;
    out.display_name = req.display_name.clone();
    out.destination = req.destination.clone();
    let mut result = WireValue::from_message(out);
    for child in [
        "sip_request_uri",
        "sip_from_header",
        "sip_to_header",
        "ringing_timeout",
        "max_call_duration",
        "destination",
    ] {
        result
            .wire
            .replace_nested(child, value.wire.nested(child).as_ref());
    }
    if let Some(media) = &result.message.media {
        let mut wire = value
            .wire
            .nested("media")
            .unwrap_or_else(|| PreservedMessage::from_message(&pb::SipMediaConfig::default()));
        if let Some(t) = trunk.and_then(|t| t.wire.nested("media")) {
            if req.media.as_ref().is_none_or(|m| m.media_timeout.is_none()) {
                wire.replace_nested("media_timeout", t.nested("media_timeout").as_ref());
            }
            if req.media.as_ref().is_none_or(|m| m.codecs.is_empty()) {
                wire.replace_repeated("codecs", t.repeated("codecs"));
            }
        }
        result
            .wire
            .replace_nested("media", Some(&wire.overlay(media)));
    }
    Ok(result)
}
fn signed_duration(value: Option<&pbjson_types::Duration>) -> Option<i128> {
    value.map(|v| {
        (v.seconds as i128 * 1_000_000_000 + v.nanos as i128)
            .clamp(i64::MIN as i128, i64::MAX as i128)
    })
}
fn set_timeout(options: &mut CallOptions, nanos: i128, now: Instant) -> Result<u64, RpcError> {
    if options.cancel.is_cancelled() {
        let mut error = RpcError::new("canceled", "context canceled");
        error.origin = RpcErrorOrigin::Canceled;
        return Err(error);
    }
    if nanos <= 0 {
        let mut error = RpcError::new("deadline_exceeded", "context deadline exceeded");
        error.origin = RpcErrorOrigin::Deadline;
        return Err(error);
    }
    let nanos = nanos as u64;
    options.timeout = Duration::from_nanos(nanos);
    if options.deadline.is_none() {
        options.deadline = now.checked_add(options.timeout);
    }
    Ok(nanos)
}
fn resolve_create_timeout(
    options: &mut CallOptions,
    default: Duration,
    now: Instant,
) -> Result<u64, RpcError> {
    let nanos = options
        .deadline
        .map(|d| d.saturating_duration_since(now).as_nanos() as i128)
        .unwrap_or(default.as_nanos() as i128);
    set_timeout(options, nanos.min(i64::MAX as i128), now)
}
fn resolve_transfer_timeout(
    options: &mut CallOptions,
    requested: i128,
    now: Instant,
) -> Result<u64, RpcError> {
    let nanos = options
        .deadline
        .map(|d| requested.min(d.saturating_duration_since(now).as_nanos() as i128))
        .unwrap_or(requested);
    set_timeout(options, nanos, now)
}
#[cfg(test)]
mod tests;

#[cfg(test)]
mod oracle_tests;

async fn caller_io<T>(
    options: &CallOptions,
    future: impl std::future::Future<Output = Result<T, RpcError>>,
) -> Result<T, RpcError> {
    let deadline = async {
        match options.deadline {
            Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
            None => std::future::pending::<()>().await,
        }
    };
    tokio::pin!(deadline);
    tokio::select! {
        biased;
        _=options.cancel.cancelled()=>{let mut e=RpcError::new("canceled","context canceled");e.origin=RpcErrorOrigin::Canceled;Err(e)},
        _=&mut deadline=>{let mut e=RpcError::new("deadline_exceeded","context deadline exceeded");e.origin=RpcErrorOrigin::Deadline;Err(e)},
        result=future=>result,
    }
}
