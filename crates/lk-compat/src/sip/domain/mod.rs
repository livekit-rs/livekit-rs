// Derived from LiveKit protocol 9bb708cdb81918a6c63dd25e915b4644310b45b6.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// See SOURCE.json and LICENSE-APACHE in this module's distribution.
#![allow(deprecated)]
use lk_protocol::livekit as pb;
use std::collections::HashMap;
mod go_print;

/// Shared Go Unicode printability table for source-compatible error quoting.
pub(crate) fn is_go_print(c: char) -> bool {
    go_print::is_print(c)
}

mod headers;
mod matching;
pub use headers::{validate_header_name, validate_header_value};
pub use matching::{
    dispatch_priority, match_address_masks, match_numbers, normalize_number,
    validate_dispatch_conflicts, validate_inbound_conflicts,
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ValidationResult {
    pub hard: Option<String>,
    pub soft: Vec<String>,
    // Preserve psrpc.InvalidArgument origin for post-load Apply errors.
    invalid_argument: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DomainFailure {
    pub message: String,
    pub invalid_argument: bool,
}
impl DomainFailure {
    fn invalid(message: String) -> Self {
        Self {
            message,
            invalid_argument: true,
        }
    }
}
impl std::fmt::Display for DomainFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for DomainFailure {}
impl From<String> for DomainFailure {
    fn from(message: String) -> Self {
        Self {
            message,
            invalid_argument: false,
        }
    }
}
impl From<&str> for DomainFailure {
    fn from(message: &str) -> Self {
        message.to_owned().into()
    }
}

impl ValidationResult {
    pub fn ok(&self) -> bool {
        self.hard.is_none()
    }
    pub fn fail(error: impl Into<String>) -> Self {
        Self {
            hard: Some(error.into()),
            soft: vec![],
            invalid_argument: false,
        }
    }
    fn fail_invalid(error: impl Into<String>) -> Self {
        Self {
            hard: Some(error.into()),
            soft: vec![],
            invalid_argument: true,
        }
    }
    fn failure(&self) -> Option<DomainFailure> {
        self.hard.clone().map(|message| DomainFailure {
            message,
            invalid_argument: self.invalid_argument,
        })
    }
    pub fn with_error(mut self, error: impl Into<String>) -> Self {
        self.hard = Some(error.into());
        self.invalid_argument = false;
        self
    }
    pub fn combine(mut self, other: Self) -> Self {
        self.invalid_argument |= other.invalid_argument;
        self.hard = match (self.hard, other.hard) {
            (Some(a), Some(b)) => Some(format!("{a}\n{b}")),
            (a, b) => a.or(b),
        };
        self.soft.extend(other.soft);
        self
    }
}
fn maps(
    headers: &HashMap<String, String>,
    attrs: &HashMap<String, String>,
    reverse: &HashMap<String, String>,
) -> ValidationResult {
    let mut result = ValidationResult::default();
    for (name, value) in headers {
        if let Err(e) = validate_header_name(name, true) {
            return result.with_error(format!("invalid header name: {e}"));
        }
        if let Some(hint) = match name.to_lowercase().as_str() {
            "to" => Some("; use CreateSIPParticipantRequest.to_user_override instead"),
            "via" => Some(""),
            _ => None,
        } {
            result.soft.push(format!(
                "header {name} is not supported and will be rejected in a future release{hint}"
            ));
        }
        let validated = validate_header_value(name, value);
        if let Some(error) = validated.hard {
            return result.with_error(format!("invalid header value for {name}: {error}"));
        }
        result = result.combine(validated);
    }
    for name in attrs.values().chain(reverse.keys()) {
        if let Err(e) = validate_header_name(name, false) {
            return result.with_error(format!("invalid header name: {e}"));
        }
    }
    result
}
pub fn validate_hostname(value: &str, name: &str) -> Result<(), String> {
    if value.contains("transport=") {
        return Err(format!("{name} should not contain transport parameter"));
    }
    if value.contains(['@', ';']) || value.starts_with("sip:") || value.starts_with("sips:") {
        return Err(format!("{name} should be a hostname or IP, not SIP URI"));
    }
    Ok(())
}
pub fn validate_list_update(value: Option<&pb::ListUpdate>) -> Result<(), String> {
    let Some(v) = value else { return Ok(()) };
    let change = !v.set.is_empty() || !v.add.is_empty() || !v.remove.is_empty();
    if !v.clear && !change {
        return Err("unsupported list update operation".into());
    }
    if v.clear && change {
        return Err("cannot clear and change the list at the same time".into());
    }
    if !v.set.is_empty() && (!v.add.is_empty() || !v.remove.is_empty()) {
        return Err("cannot set and change the list at the same time".into());
    }
    if v.set
        .iter()
        .chain(&v.add)
        .chain(&v.remove)
        .any(String::is_empty)
    {
        return Err("empty element in the list".into());
    }
    Ok(())
}
pub fn apply_list_update(
    values: &mut Vec<String>,
    value: Option<&pb::ListUpdate>,
) -> Result<(), String> {
    validate_list_update(value)?;
    let Some(v) = value else { return Ok(()) };
    if v.clear {
        values.clear();
        return Ok(());
    }
    if !v.set.is_empty() {
        *values = v.set.clone();
        return Ok(());
    }
    for x in &v.remove {
        if let Some(i) = values.iter().position(|a| a == x) {
            values.remove(i);
        }
    }
    for x in &v.add {
        if !values.contains(x) {
            values.push(x.clone());
        }
    }
    Ok(())
}
fn set<T: Clone>(to: &mut T, from: &Option<T>) {
    if let Some(v) = from {
        *to = v.clone();
    }
}
pub fn validate_media(media: Option<&pb::SipMediaConfig>) -> Result<(), String> {
    let Some(m) = media else { return Ok(()) };
    for codec in &m.codecs {
        if codec.name.is_empty() {
            return Err("missing codec name".into());
        }
        if codec.name.contains([' ', '\t', '\n', '\r']) {
            return Err("invalid codec name".into());
        }
        if codec.name.contains('/') {
            return Err("codec name must not contain '/'".into());
        }
    }
    if let Some(d) = &m.media_timeout {
        let nanos = (i128::from(d.seconds) * 1_000_000_000 + i128::from(d.nanos))
            .clamp(i128::from(i64::MIN), i128::from(i64::MAX));
        if nanos < 0 {
            return Err("media_timeout must not be negative".into());
        }
        if nanos > 600_000_000_000 {
            return Err("media_timeout must not exceed 10m0s".into());
        }
    }
    Ok(())
}
fn upgrade_media(media: &mut Option<pb::SipMediaConfig>, legacy: i32) {
    let m = media.get_or_insert_with(Default::default);
    if m.encryption.is_none() && legacy != 0 {
        m.encryption = Some(legacy);
    }
}
pub fn upgrade_inbound(info: &mut pb::SipInboundTrunkInfo) {
    upgrade_media(&mut info.media, info.media_encryption);
}
pub fn upgrade_outbound(info: &mut pb::SipOutboundTrunkInfo) {
    upgrade_media(&mut info.media, info.media_encryption);
}
pub fn upgrade_dispatch(info: &mut pb::SipDispatchRuleInfo) {
    upgrade_media(&mut info.media, info.media_encryption);
}
fn update_media(
    media: &mut Option<pb::SipMediaConfig>,
    legacy: &mut i32,
    new: &Option<pb::SipMediaConfig>,
    enc: &Option<i32>,
) {
    if new.is_some() {
        *media = new.clone();
    }
    set(legacy, enc);
    upgrade_media(media, *legacy);
    *legacy = media.as_ref().and_then(|m| m.encryption).unwrap_or(0);
}
pub fn validate_inbound(info: &pb::SipInboundTrunkInfo) -> ValidationResult {
    if info.auth_username.is_empty()
        && info.auth_password.is_empty()
        && info.allowed_addresses.is_empty()
        && info.numbers.is_empty()
    {
        return ValidationResult::fail_invalid(
            "for security, one of the fields must be set: AuthUsername+AuthPassword, AllowedAddresses or Numbers",
        );
    }
    if let Err(e) = validate_media(info.media.as_ref()) {
        return ValidationResult::fail(e);
    }
    if info.ringing_timeout_status != 0 && !(400..=699).contains(&info.ringing_timeout_status) {
        return ValidationResult::fail_invalid(format!(
            "ringing timeout status must be 4xx-6xx, got {}",
            info.ringing_timeout_status
        ));
    }
    maps(
        &info.headers,
        &info.attributes_to_headers,
        &info.headers_to_attributes,
    )
}
pub fn validate_outbound(info: &pb::SipOutboundTrunkInfo) -> ValidationResult {
    if info.numbers.is_empty() {
        return ValidationResult::fail("no trunk numbers specified");
    }
    if info.address.is_empty() {
        return ValidationResult::fail("no outbound address specified");
    }
    if let Err(e) = validate_hostname(&info.address, "trunk address") {
        return ValidationResult::fail(e);
    }
    if let Err(e) = validate_hostname(&info.from_host, "from_host") {
        return ValidationResult::fail(e);
    }
    let result = maps(
        &info.headers,
        &info.attributes_to_headers,
        &info.headers_to_attributes,
    );
    if !result.ok() {
        return result;
    }
    if let Err(e) = validate_media(info.media.as_ref()) {
        return result.with_error(e);
    }
    result
}
pub fn validate_create_inbound(request: &pb::CreateSipInboundTrunkRequest) -> ValidationResult {
    let Some(t) = &request.trunk else {
        return ValidationResult::fail("missing trunk");
    };
    if !t.sip_trunk_id.is_empty() {
        return ValidationResult::fail("trunk id must not be set");
    }
    validate_inbound(t)
}
pub fn validate_create_outbound(request: &pb::CreateSipOutboundTrunkRequest) -> ValidationResult {
    let Some(t) = &request.trunk else {
        return ValidationResult::fail("missing trunk");
    };
    if !t.sip_trunk_id.is_empty() {
        return ValidationResult::fail("trunk id must not be set");
    }
    validate_outbound(t)
}
fn validate_inbound_patch(p: &pb::SipInboundTrunkUpdate) -> Result<(), String> {
    validate_list_update(p.numbers.as_ref())?;
    validate_list_update(p.allowed_addresses.as_ref())?;
    validate_list_update(p.allowed_numbers.as_ref())?;
    validate_media(p.media.as_ref())
}
fn validate_outbound_patch(p: &pb::SipOutboundTrunkUpdate) -> Result<(), String> {
    validate_list_update(p.numbers.as_ref())?;
    validate_media(p.media.as_ref())
}

pub fn validate_update_inbound(request: &pb::UpdateSipInboundTrunkRequest) -> ValidationResult {
    if request.sip_trunk_id.is_empty() {
        return ValidationResult::fail("trunk id must be set");
    }
    match &request.action {
        None => ValidationResult::fail("missing or unsupported update action"),
        Some(pb::update_sip_inbound_trunk_request::Action::Replace(info)) => {
            if !info.sip_trunk_id.is_empty() && info.sip_trunk_id != request.sip_trunk_id {
                return ValidationResult::fail(
                    "trunk id in the info must be empty or match the id in the update",
                );
            }
            validate_inbound(info)
        }
        Some(pb::update_sip_inbound_trunk_request::Action::Update(diff)) => {
            match validate_inbound_patch(diff) {
                Ok(()) => ValidationResult::default(),
                Err(e) => ValidationResult::fail(e),
            }
        }
    }
}
pub fn apply_inbound_request(
    request: &pb::UpdateSipInboundTrunkRequest,
    existing: &pb::SipInboundTrunkInfo,
) -> Result<(pb::SipInboundTrunkInfo, ValidationResult), DomainFailure> {
    let validated = validate_update_inbound(request);
    if let Some(e) = validated.hard {
        return Err(DomainFailure::invalid(e));
    }
    let mut value = existing.clone();
    match request
        .action
        .as_ref()
        .ok_or("missing or unsupported update action")?
    {
        pb::update_sip_inbound_trunk_request::Action::Replace(info) => {
            value = info.clone();
            if !existing.sip_trunk_id.is_empty() {
                value.sip_trunk_id = existing.sip_trunk_id.clone();
            }
        }
        pb::update_sip_inbound_trunk_request::Action::Update(diff) => {
            apply_inbound_patch(&mut value, diff)?;
        }
    }
    let result = validate_inbound(&value);
    if let Some(error) = result.failure() {
        return Err(error);
    }
    let result = ValidationResult::default();
    Ok((value, result))
}

pub fn validate_update_outbound(request: &pb::UpdateSipOutboundTrunkRequest) -> ValidationResult {
    if request.sip_trunk_id.is_empty() {
        return ValidationResult::fail("trunk id must be set");
    }
    match &request.action {
        None => ValidationResult::fail("missing or unsupported update action"),
        Some(pb::update_sip_outbound_trunk_request::Action::Replace(info)) => {
            if !info.sip_trunk_id.is_empty() && info.sip_trunk_id != request.sip_trunk_id {
                return ValidationResult::fail(
                    "trunk id in the info must be empty or match the id in the update",
                );
            }
            validate_outbound(info)
        }
        Some(pb::update_sip_outbound_trunk_request::Action::Update(diff)) => {
            match validate_outbound_patch(diff) {
                Ok(()) => ValidationResult::default(),
                Err(e) => ValidationResult::fail(e),
            }
        }
    }
}
pub fn apply_outbound_request(
    request: &pb::UpdateSipOutboundTrunkRequest,
    existing: &pb::SipOutboundTrunkInfo,
) -> Result<(pb::SipOutboundTrunkInfo, ValidationResult), DomainFailure> {
    let validated = validate_update_outbound(request);
    if let Some(e) = validated.hard {
        return Err(DomainFailure::invalid(e));
    }
    let mut value = existing.clone();
    match request
        .action
        .as_ref()
        .ok_or("missing or unsupported update action")?
    {
        pb::update_sip_outbound_trunk_request::Action::Replace(info) => {
            value = info.clone();
            if !existing.sip_trunk_id.is_empty() {
                value.sip_trunk_id = existing.sip_trunk_id.clone();
            }
        }
        pb::update_sip_outbound_trunk_request::Action::Update(diff) => {
            apply_outbound_patch(&mut value, diff)?;
        }
    }
    let result = validate_outbound(&value);
    if let Some(error) = result.failure() {
        return Err(error);
    }
    let result = if matches!(
        request.action.as_ref(),
        Some(pb::update_sip_outbound_trunk_request::Action::Replace(_))
    ) {
        result
    } else {
        ValidationResult::default()
    };
    Ok((value, result))
}

fn apply_inbound_patch(
    info: &mut pb::SipInboundTrunkInfo,
    diff: &pb::SipInboundTrunkUpdate,
) -> Result<(), String> {
    validate_inbound_patch(diff)?;
    apply_list_update(&mut info.numbers, diff.numbers.as_ref())?;
    apply_list_update(&mut info.allowed_addresses, diff.allowed_addresses.as_ref())?;
    apply_list_update(&mut info.allowed_numbers, diff.allowed_numbers.as_ref())?;
    set(&mut info.auth_username, &diff.auth_username);
    set(&mut info.auth_password, &diff.auth_password);
    set(&mut info.auth_realm, &diff.auth_realm);
    set(&mut info.name, &diff.name);
    set(&mut info.metadata, &diff.metadata);
    update_media(
        &mut info.media,
        &mut info.media_encryption,
        &diff.media,
        &diff.media_encryption,
    );
    upgrade_inbound(info);
    Ok(())
}
fn apply_outbound_patch(
    info: &mut pb::SipOutboundTrunkInfo,
    diff: &pb::SipOutboundTrunkUpdate,
) -> Result<(), String> {
    validate_outbound_patch(diff)?;
    set(&mut info.address, &diff.address);
    set(&mut info.transport, &diff.transport);
    set(&mut info.destination_country, &diff.destination_country);
    apply_list_update(&mut info.numbers, diff.numbers.as_ref())?;
    set(&mut info.auth_username, &diff.auth_username);
    set(&mut info.auth_password, &diff.auth_password);
    set(&mut info.name, &diff.name);
    set(&mut info.metadata, &diff.metadata);
    set(&mut info.from_host, &diff.from_host);
    update_media(
        &mut info.media,
        &mut info.media_encryption,
        &diff.media,
        &diff.media_encryption,
    );
    upgrade_outbound(info);
    Ok(())
}
pub fn dispatch_info(request: &pb::CreateSipDispatchRuleRequest) -> pb::SipDispatchRuleInfo {
    request
        .dispatch_rule
        .clone()
        .unwrap_or_else(|| pb::SipDispatchRuleInfo {
            rule: request.rule.clone(),
            trunk_ids: request.trunk_ids.clone(),
            inbound_numbers: request.inbound_numbers.clone(),
            hide_phone_number: request.hide_phone_number,
            name: request.name.clone(),
            metadata: request.metadata.clone(),
            attributes: request.attributes.clone(),
            room_preset: request.room_preset.clone(),
            room_config: request.room_config.clone(),
            ..Default::default()
        })
}
pub fn validate_dispatch(info: &pb::SipDispatchRuleInfo) -> Result<(), String> {
    if info.rule.is_none() {
        return Err("missing rule".into());
    }
    validate_media(info.media.as_ref())
}
pub fn validate_create_dispatch(request: &pb::CreateSipDispatchRuleRequest) -> Result<(), String> {
    if request
        .dispatch_rule
        .as_ref()
        .is_some_and(|r| !r.sip_dispatch_rule_id.is_empty())
    {
        return Err("rule id must not be set".into());
    }
    validate_dispatch(&dispatch_info(request))
}
fn validate_dispatch_patch(diff: &pb::SipDispatchRuleUpdate) -> Result<(), String> {
    validate_list_update(diff.trunk_ids.as_ref())?;
    validate_media(diff.media.as_ref())
}
pub fn validate_update_dispatch(request: &pb::UpdateSipDispatchRuleRequest) -> Result<(), String> {
    if request.sip_dispatch_rule_id.is_empty() {
        return Err("rule id must be set".into());
    }
    match &request.action {
        None => Err("missing or unsupported update action".into()),
        Some(pb::update_sip_dispatch_rule_request::Action::Replace(info)) => {
            if !info.sip_dispatch_rule_id.is_empty()
                && info.sip_dispatch_rule_id != request.sip_dispatch_rule_id
            {
                return Err(
                    "rule id in the info must be empty or match the id in the update".into(),
                );
            }
            validate_dispatch(info)
        }
        Some(pb::update_sip_dispatch_rule_request::Action::Update(diff)) => {
            validate_dispatch_patch(diff)
        }
    }
}
pub fn apply_dispatch_request(
    request: &pb::UpdateSipDispatchRuleRequest,
    existing: &pb::SipDispatchRuleInfo,
) -> Result<pb::SipDispatchRuleInfo, DomainFailure> {
    validate_update_dispatch(request).map_err(DomainFailure::invalid)?;
    let mut value = existing.clone();
    match request
        .action
        .as_ref()
        .ok_or("missing or unsupported update action")?
    {
        pb::update_sip_dispatch_rule_request::Action::Replace(info) => {
            value = info.clone();
            if !existing.sip_dispatch_rule_id.is_empty() {
                value.sip_dispatch_rule_id = existing.sip_dispatch_rule_id.clone();
            }
        }
        pb::update_sip_dispatch_rule_request::Action::Update(diff) => {
            apply_list_update(&mut value.trunk_ids, diff.trunk_ids.as_ref())?;
            if diff.rule.is_some() {
                value.rule = diff.rule.clone();
            }
            set(&mut value.name, &diff.name);
            set(&mut value.metadata, &diff.metadata);
            for (k, v) in &diff.attributes {
                if v.is_empty() {
                    value.attributes.remove(k);
                } else {
                    value.attributes.insert(k.clone(), v.clone());
                }
            }
            update_media(
                &mut value.media,
                &mut value.media_encryption,
                &diff.media,
                &diff.media_encryption,
            );
            upgrade_dispatch(&mut value);
        }
    }
    validate_dispatch(&value)?;
    Ok(value)
}

pub fn merge_media(
    first: Option<&pb::SipMediaConfig>,
    second: Option<&pb::SipMediaConfig>,
) -> Option<pb::SipMediaConfig> {
    match (first, second) {
        (None, b) => b.cloned(),
        (a, None) => a.cloned(),
        (Some(a), Some(b)) => {
            let mut result = b.clone();
            result.encryption = match (a.encryption, b.encryption) {
                (None, b) => b,
                (a, None) => a,
                (Some(a), Some(b)) => Some(if b == 0 { a } else { b }),
            };
            if b.media_timeout.is_none() {
                result.media_timeout = a.media_timeout;
            }
            if b.codecs.is_empty() {
                result.only_listed_codecs = a.only_listed_codecs;
                result.codecs = a.codecs.clone();
            }
            Some(result)
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod oracle_tests;

#[cfg(test)]
mod edge_tests;

#[cfg(test)]
mod error_code_tests;
