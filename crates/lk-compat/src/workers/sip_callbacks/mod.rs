// Copyright 2023 LiveKit, Inc. SPDX-License-Identifier: Apache-2.0
//! Fixed IOInfo SIP callbacks backed by the real persistent SIP catalog.
//! Matching follows protocol/sip/sip.go; typed response ownership carries only
//! cloned child provenance, never unknown root fields across message types.
#![allow(deprecated)]
use super::{io_service::SipCallbacks, preserved::WireValue};
use crate::{
    service_bus::rpc::RpcError,
    sip::{
        catalog::{CatalogError, SipCatalog},
        domain,
        wire::{PreservedMessage, Stored},
    },
};
use futures_util::future::BoxFuture;
use lk_protocol::{livekit as pb, rpc};
use sha2::{Digest, Sha256};
mod netip;

/// Real callbacks. Absence of a Redis catalog preserves the upstream explicit
/// not-connected error; it never produces a successful dispatch placeholder.
pub struct CatalogSipCallbacks {
    catalog: Option<SipCatalog>,
}
impl CatalogSipCallbacks {
    pub fn new(catalog: Option<SipCatalog>) -> Self {
        Self { catalog }
    }
    fn catalog(&self) -> Result<&SipCatalog, RpcError> {
        self.catalog
            .as_ref()
            .ok_or_else(|| RpcError::new("internal", "sip not connected (redis required)"))
    }
    async fn match_trunk(
        &self,
        id: &str,
        call: &rpc::SipCall,
    ) -> Result<Option<Stored<pb::SipInboundTrunkInfo>>, RpcError> {
        let catalog = self.catalog()?;
        if call.to.is_none() {
            return Err(nil_catalog());
        }
        if !id.is_empty()
            && let Ok(trunk) = catalog.load_inbound_preserved(id).await
        {
            let trunk = trunk.ok_or_else(nil_catalog)?;
            // A valid optimized lookup returning no match is still final in
            // the fixed Go implementation; only its errors trigger list retry.
            if let Ok(result) = match_trunks(&[trunk], call) {
                return Ok(result);
            }
        }
        let items = catalog
            .list_inbound_preserved(&pb::ListSipInboundTrunkRequest {
                numbers: vec![to(call).user.clone()],
                ..Default::default()
            })
            .await
            .map_err(store_error)?;
        let items = items
            .into_iter()
            .map(|v| v.ok_or_else(nil_catalog))
            .collect::<Result<Vec<_>, _>>()?;
        match_trunks(&items, call)
    }
    async fn authentication(
        &self,
        req: rpc::GetSipTrunkAuthenticationRequest,
    ) -> Result<WireValue<rpc::GetSipTrunkAuthenticationResponse>, RpcError> {
        let call = auth_call(&req);
        netip::validate_source(&call.source_ip)?;
        let Some(trunk) = self.match_trunk("", &call).await? else {
            return Ok(WireValue::from_message(Default::default()));
        };
        Ok(WireValue::from_message(auth_prompt(&trunk.value)))
    }
    async fn evaluation(
        &self,
        req: rpc::EvaluateSipDispatchRulesRequest,
    ) -> Result<WireValue<rpc::EvaluateSipDispatchRulesResponse>, RpcError> {
        let call = dispatch_call(&req);
        netip::validate_source(&call.source_ip)?;
        let trunk = self.match_trunk(&req.sip_trunk_id, &call).await?;
        let rules = self
            .catalog()?
            .list_rules_preserved(&pb::ListSipDispatchRuleRequest {
                trunk_ids: trunk
                    .as_ref()
                    .map(|v| vec![v.sip_trunk_id.clone()])
                    .unwrap_or_default(),
                ..Default::default()
            })
            .await
            .map_err(store_error)?;
        let Some(rule) = match_rule(trunk.as_ref().map(|v| &v.value), &rules, &req)? else {
            return Ok(WireValue::from_message(
                rpc::EvaluateSipDispatchRulesResponse {
                    result: rpc::SipDispatchResult::Reject as i32,
                    sip_trunk_id: trunk
                        .as_ref()
                        .map(|v| v.sip_trunk_id.clone())
                        .unwrap_or_default(),
                    ..Default::default()
                },
            ));
        };
        let mut output =
            evaluate_selected(trunk.as_ref(), rule, &req, || rtc_core::ids::new_sid(""))?;
        // IOInfo replaces the evaluator's request-ID fallback with the actual
        // matched trunk ID. Attributes retain the evaluator's own source value.
        output.message.sip_trunk_id = trunk
            .as_ref()
            .map(|v| v.sip_trunk_id.clone())
            .unwrap_or_default();
        Ok(output)
    }
}
impl SipCallbacks for CatalogSipCallbacks {
    fn authenticate(
        &self,
        request: rpc::GetSipTrunkAuthenticationRequest,
    ) -> BoxFuture<'_, Result<WireValue<rpc::GetSipTrunkAuthenticationResponse>, RpcError>> {
        Box::pin(self.authentication(request))
    }
    fn evaluate(
        &self,
        request: rpc::EvaluateSipDispatchRulesRequest,
    ) -> BoxFuture<'_, Result<WireValue<rpc::EvaluateSipDispatchRulesResponse>, RpcError>> {
        Box::pin(self.evaluation(request))
    }
}
fn nil_catalog() -> RpcError {
    RpcError::new("internal", "Internal service panic")
}
fn store_error(error: CatalogError) -> RpcError {
    RpcError::new("unknown", error.compatible_public_message())
}
fn twirp_error(code: &str, message: impl std::fmt::Display) -> RpcError {
    // psrpc.GetErrorCode does not interpret Twirp errors as psrpc.Error.
    RpcError::new("unknown", format!("twirp error {code}: {message}"))
}
fn auth_prompt(trunk: &pb::SipInboundTrunkInfo) -> rpc::GetSipTrunkAuthenticationResponse {
    rpc::GetSipTrunkAuthenticationResponse {
        sip_trunk_id: trunk.sip_trunk_id.clone(),
        username: trunk.auth_username.clone(),
        password: trunk.auth_password.clone(),
        realm: trunk.auth_realm.clone(),
        provider_info: Some(pb::ProviderInfo {
            id: trunk.sip_trunk_id.clone(),
            name: trunk.name.clone(),
            r#type: pb::ProviderType::External as i32,
            ..Default::default()
        }),
        ..Default::default()
    }
}
fn split_legacy_address(value: &str) -> String {
    // Go net.SplitHostPort strips a syntactically valid port (not necessarily a
    // numeric port). A raw IPv6 address is retained, not split at its last colon.
    if let Some(rest) = value.strip_prefix('[') {
        if let Some((host, port)) = rest.split_once("]:")
            && !host.contains(['[', ']'])
            && !port.contains([':', '[', ']'])
        {
            return host.into();
        }
    } else if let Some((host, port)) = value.split_once(':')
        && !host.contains(['[', ']'])
        && !port.contains([':', '[', ']'])
    {
        return host.into();
    }
    value.into()
}
fn legacy_call(
    id: &str,
    source: &str,
    caller: &str,
    caller_host: &str,
    called: &str,
    called_host: &str,
) -> rpc::SipCall {
    let to = pb::SipUri {
        user: called.into(),
        host: called_host.into(),
        ..Default::default()
    };
    rpc::SipCall {
        lk_call_id: id.into(),
        source_ip: split_legacy_address(source),
        from: Some(pb::SipUri {
            user: caller.into(),
            host: caller_host.into(),
            ..Default::default()
        }),
        to: Some(to.clone()),
        address: Some(to),
        ..Default::default()
    }
}
fn auth_call(req: &rpc::GetSipTrunkAuthenticationRequest) -> rpc::SipCall {
    req.call.clone().unwrap_or_else(|| {
        legacy_call(
            &req.sip_call_id,
            &req.src_address,
            &req.from,
            &req.from_host,
            &req.to,
            &req.to_host,
        )
    })
}
fn dispatch_call(req: &rpc::EvaluateSipDispatchRulesRequest) -> rpc::SipCall {
    req.call.clone().unwrap_or_else(|| {
        legacy_call(
            &req.sip_call_id,
            &req.src_address,
            &req.calling_number,
            &req.calling_host,
            &req.called_number,
            &req.called_host,
        )
    })
}
fn from(call: &rpc::SipCall) -> &pb::SipUri {
    call.from.as_ref().unwrap_or(&EMPTY_URI)
}
fn to(call: &rpc::SipCall) -> &pb::SipUri {
    call.to.as_ref().unwrap_or(&EMPTY_URI)
}
static EMPTY_URI: pb::SipUri = pb::SipUri {
    user: String::new(),
    host: String::new(),
    ip: String::new(),
    port: 0,
    transport: 0,
};
fn match_trunks(
    items: &[Stored<pb::SipInboundTrunkInfo>],
    call: &rpc::SipCall,
) -> Result<Option<Stored<pb::SipInboundTrunkInfo>>, RpcError> {
    let mut selected = None;
    let mut defaults = Vec::new();
    for trunk in items {
        if call.from.is_none() {
            return Err(nil_catalog());
        }
        if !domain::match_numbers(&from(call).user, &trunk.allowed_numbers)
            || !domain::match_address_masks(
                &call.source_ip,
                &from(call).host,
                &trunk.allowed_addresses,
            )
        {
            continue;
        }
        if trunk.numbers.is_empty() {
            defaults.push(trunk);
        } else if domain::match_numbers(&to(call).user, &trunk.numbers) {
            if selected.is_some() {
                return Err(twirp_error(
                    "failed_precondition",
                    format!(
                        "Multiple SIP Trunks matched for {}",
                        netip::go_quote(&to(call).user)
                    ),
                ));
            }
            selected = Some(trunk);
        }
    }
    if let Some(selected) = selected {
        return Ok(Some(selected.clone()));
    }
    if defaults.len() > 1 {
        return Err(twirp_error(
            "failed_precondition",
            format!(
                "Multiple default SIP Trunks matched for {}",
                netip::go_quote(&to(call).user)
            ),
        ));
    }
    Ok(defaults.first().map(|v| (*v).clone()))
}
fn pin_room(info: &pb::SipDispatchRuleInfo) -> Result<(&str, &str), RpcError> {
    match info.rule.as_ref().and_then(|v| v.rule.as_ref()) {
        Some(pb::sip_dispatch_rule::Rule::DispatchRuleDirect(v)) => Ok((&v.room_name, &v.pin)),
        Some(pb::sip_dispatch_rule::Rule::DispatchRuleIndividual(v)) => {
            Ok((&v.room_prefix, &v.pin))
        }
        Some(pb::sip_dispatch_rule::Rule::DispatchRuleCallee(v)) => Ok((&v.room_prefix, &v.pin)),
        None => Err(RpcError::new(
            "invalid_argument",
            "unsupported SIP Dispatch Rule: <nil>",
        )),
    }
}
fn match_rule<'a>(
    trunk: Option<&pb::SipInboundTrunkInfo>,
    rules: &'a [Stored<pb::SipDispatchRuleInfo>],
    req: &rpc::EvaluateSipDispatchRulesRequest,
) -> Result<Option<&'a Stored<pb::SipDispatchRuleInfo>>, RpcError> {
    // The fixed iterator validates every rule before number/PIN filtering.
    domain::validate_dispatch_conflicts(&rules.iter().map(|v| v.value.clone()).collect::<Vec<_>>())
        .map_err(|e| {
            // GetPinAndRoom emits a typed psrpc error, unlike the validator
            // conflict (a Twirp error). This fixed variant has no user data.
            if e == "unsupported SIP Dispatch Rule: <nil>" {
                RpcError::new("invalid_argument", e)
            } else {
                twirp_error("invalid_argument", e)
            }
        })?;
    let mut specific: Option<&Stored<pb::SipDispatchRuleInfo>> = None;
    let mut default: Option<&Stored<pb::SipDispatchRuleInfo>> = None;
    for rule in rules {
        // These filters intentionally use the legacy request fields, just like
        // fixed MatchDispatchRuleIter; SIPCall normalization does not mutate req.
        if (!rule.inbound_numbers.is_empty() && !rule.inbound_numbers.contains(&req.calling_number))
            || (!rule.numbers.is_empty() && !rule.numbers.contains(&req.called_number))
        {
            continue;
        }
        let (room, pin) = pin_room(rule)?;
        if req.no_pin && !pin.is_empty() {
            continue;
        }
        if !req.no_pin && !req.pin.is_empty() && (pin.is_empty() || req.pin != pin) {
            continue;
        }
        let slot = if rule.trunk_ids.is_empty() {
            &mut default
        } else if trunk.is_some_and(|t| rule.trunk_ids.contains(&t.sip_trunk_id)) {
            &mut specific
        } else {
            continue;
        };
        let replace = match slot {
            None => true,
            Some(old) => {
                let p = domain::dispatch_priority(rule);
                let old_p = domain::dispatch_priority(old);
                p < old_p || (p == old_p && room < pin_room(old)?.0)
            }
        };
        if replace {
            *slot = Some(rule);
        }
    }
    Ok(specific.or(default))
}
fn merged_media_wire(
    trunk: Option<&Stored<pb::SipInboundTrunkInfo>>,
    rule: &Stored<pb::SipDispatchRuleInfo>,
    media: &pb::SipMediaConfig,
) -> PreservedMessage {
    // UpgradeWith always makes both input media pointers non-nil. Merge clones
    // rule media; fallback fields are explicitly borrowed from trunk, not its
    // root unknown fields or a previous response with a matching ID.
    let mut wire = rule
        .wire
        .nested("media")
        .unwrap_or_else(|| PreservedMessage::from_message(&pb::SipMediaConfig::default()));
    if let Some(trunk) = trunk
        && let Some(source) = trunk.wire.nested("media")
    {
        if rule
            .media
            .as_ref()
            .is_none_or(|m| m.media_timeout.is_none())
        {
            wire.replace_nested("media_timeout", source.nested("media_timeout").as_ref());
        }
        if rule.media.as_ref().is_none_or(|m| m.codecs.is_empty()) {
            wire.replace_repeated("codecs", source.repeated("codecs"));
        }
    }
    wire.overlay(media)
}
fn evaluate_selected(
    trunk: Option<&Stored<pb::SipInboundTrunkInfo>>,
    rule: &Stored<pb::SipDispatchRuleInfo>,
    req: &rpc::EvaluateSipDispatchRulesRequest,
    random: impl FnOnce() -> String,
) -> Result<WireValue<rpc::EvaluateSipDispatchRulesResponse>, RpcError> {
    let mut upgraded_trunk = trunk.map(|v| v.value.clone());
    if let Some(t) = &mut upgraded_trunk {
        domain::upgrade_inbound(t);
    }
    let mut upgraded_rule = rule.value.clone();
    domain::upgrade_dispatch(&mut upgraded_rule);
    let call = dispatch_call(req);
    if call.to.is_none() || call.from.is_none() {
        return Err(nil_catalog());
    }
    let trunk_id = upgraded_trunk
        .as_ref()
        .map(|v| v.sip_trunk_id.clone())
        .unwrap_or_else(|| req.sip_trunk_id.clone());
    let media = domain::merge_media(
        upgraded_trunk.as_ref().and_then(|v| v.media.as_ref()),
        upgraded_rule.media.as_ref(),
    )
    .unwrap_or_default();
    let (room, pin) = pin_room(&upgraded_rule)?;
    let mut result = rpc::EvaluateSipDispatchRulesResponse {
        sip_trunk_id: trunk_id.clone(),
        sip_dispatch_rule_id: rule.sip_dispatch_rule_id.clone(),
        media_encryption: media.encryption.unwrap_or(0),
        media: Some(media.clone()),
        ..Default::default()
    };
    if !pin.is_empty() {
        if req.pin.is_empty() {
            result.result = rpc::SipDispatchResult::RequestPin as i32;
            result.request_pin = true;
            let mut output = WireValue::from_message(result);
            output
                .wire
                .replace_nested("media", Some(&merged_media_wire(trunk, rule, &media)));
            return Ok(output);
        }
        if req.pin != pin {
            return Err(twirp_error(
                "permission_denied",
                "Incorrect PIN for SIP room",
            ));
        }
    }
    let mut attrs = rule.attributes.clone();
    attrs.extend(req.extra_attributes.clone());
    attrs.insert("sip.callID".into(), call.lk_call_id.clone());
    attrs.insert("sip.trunkID".into(), trunk_id);
    let mut caller = from(&call).user.clone();
    let identity = if rule.hide_phone_number {
        let hash = Sha256::digest(caller.as_bytes());
        let identity = format!(
            "sip_{}",
            hash[..8]
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        );
        let n = if caller.len() <= 4 {
            caller.len().min(1)
        } else {
            4
        };
        caller = std::str::from_utf8(&caller.as_bytes()[caller.len() - n..])
            .map_err(|_| RpcError::new("unknown", "string field contains invalid UTF-8"))?
            .into();
        identity
    } else {
        attrs.insert("sip.phoneNumber".into(), from(&call).user.clone());
        attrs.insert("sip.hostname".into(), from(&call).host.clone());
        attrs.insert("sip.trunkPhoneNumber".into(), to(&call).user.clone());
        format!("sip_{caller}")
    };
    let room = match upgraded_rule.rule.as_ref().and_then(|r| r.rule.as_ref()) {
        Some(pb::sip_dispatch_rule::Rule::DispatchRuleIndividual(v)) => {
            let mut r = if v.room_prefix.is_empty() {
                caller.clone()
            } else {
                format!("{}_{}", v.room_prefix, caller)
            };
            if !v.no_randomness {
                r.push('_');
                r.push_str(&random());
            }
            r
        }
        Some(pb::sip_dispatch_rule::Rule::DispatchRuleCallee(v)) => {
            let mut r = if v.room_prefix.is_empty() {
                to(&call).user.clone()
            } else {
                format!("{}_{}", v.room_prefix, to(&call).user)
            };
            if v.randomize {
                r.push('_');
                r.push_str(&random());
            }
            r
        }
        _ => room.into(),
    };
    attrs.insert("sip.ruleID".into(), rule.sip_dispatch_rule_id.clone());
    result.result = rpc::SipDispatchResult::Accept as i32;
    result.room_name = room;
    result.participant_identity = identity;
    result.participant_name = format!("Phone {caller}");
    result.participant_metadata = rule.metadata.clone();
    result.participant_attributes = attrs;
    result.room_preset = rule.room_preset.clone();
    result.room_config = rule.room_config.clone();
    if let Some(t) = &upgraded_trunk {
        result.headers = t.headers.clone();
        result.headers_to_attributes = t.headers_to_attributes.clone();
        result.attributes_to_headers = t.attributes_to_headers.clone();
        result.include_headers = t.include_headers;
        result.ringing_timeout = t.ringing_timeout;
        result.ringing_timeout_status = t.ringing_timeout_status;
        result.max_call_duration = t.max_call_duration;
    }
    if upgraded_trunk.as_ref().is_some_and(|v| v.krisp_enabled) || rule.krisp_enabled {
        result
            .enabled_features
            .push(pb::SipFeature::KrispEnabled as i32);
    }
    let mut output = WireValue::from_message(result);
    output
        .wire
        .replace_nested("media", Some(&merged_media_wire(trunk, rule, &media)));
    output
        .wire
        .replace_nested("room_config", rule.wire.nested("room_config").as_ref());
    if let Some(trunk) = trunk {
        for field in ["ringing_timeout", "max_call_duration"] {
            output
                .wire
                .replace_nested(field, trunk.wire.nested(field).as_ref());
        }
    }
    Ok(output)
}
#[cfg(test)]
mod tests;

#[cfg(test)]
mod oracle_tests;
