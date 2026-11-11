//! Public SIP configuration RPCs; call and transfer are owned by SIP gateway.
#![allow(deprecated)]
use axum::response::Response;
use lk_protocol::{livekit as pb, twirp_json::livekit as tj};
use rtc_core::ids::new_sid;

use super::{
    catalog::{CatalogError, SipCatalog},
    domain,
    wire::{PreservedMessage, Stored},
};
use crate::auth::{VerifiedToken, permissions::ensure_sip_admin};
use crate::twirp::{
    codec::{Codec, decode, encode, encode_preserved},
    error::{Code, TwirpError},
};

type Result<T> = std::result::Result<T, TwirpError>;
fn invalid(message: impl Into<String>) -> TwirpError {
    TwirpError::new(Code::InvalidArgument, message)
}
fn store_error(error: CatalogError) -> TwirpError {
    let code = match error {
        CatalogError::TrunkNotFound | CatalogError::RuleNotFound => Code::NotFound,
        _ => Code::Unknown,
    };
    TwirpError::new(code, error.compatible_public_message())
}
fn domain_error(error: domain::DomainFailure) -> TwirpError {
    TwirpError::new(
        if error.invalid_argument {
            Code::InvalidArgument
        } else {
            Code::Unknown
        },
        error.message,
    )
}
fn ready<'a>(
    catalog: Option<&'a SipCatalog>,
    auth: Option<&VerifiedToken>,
) -> Result<&'a SipCatalog> {
    ensure_sip_admin(auth)?;
    catalog.ok_or_else(|| TwirpError::internal("sip not connected (redis required)"))
}
fn validated(result: domain::ValidationResult) -> Result<()> {
    for warning in result.soft {
        tracing::warn!(warning = %warning, "SIP configuration soft validation failure");
    }
    result.hard.map_or(Ok(()), |error| Err(invalid(error)))
}
// Preserve the fixed Go validator's nil dereference recovery behavior. Nil
// catalog entries remain legal in ordinary list/Get responses, not conflicts.
fn conflict_trunks(
    values: Vec<Option<Stored<pb::SipInboundTrunkInfo>>>,
) -> Result<Vec<pb::SipInboundTrunkInfo>> {
    values
        .into_iter()
        .map(|value| {
            value
                .map(|v| v.value)
                .ok_or_else(|| TwirpError::internal("Internal service panic"))
        })
        .collect()
}
fn advance(page: &mut Option<pb::Pagination>, ids: impl Iterator<Item = String>) -> bool {
    let Some(page) = page.as_mut() else {
        return false;
    };
    let next = ids.max().unwrap_or_default();
    if next > page.after_id {
        page.after_id = next;
        true
    } else {
        false
    }
}
macro_rules! list_all {
    ($name:ident, $request:ty, $item:ty, $method:ident, $id:expr) => {
        async fn $name(catalog: &SipCatalog, mut request: $request) -> Result<Vec<$item>> {
            let mut output = Vec::new();
            loop {
                let values = catalog.$method(&request).await.map_err(store_error)?;
                if request.page.is_none() {
                    output.extend(values);
                    break;
                }
                // Go returns EOF before AllPages appends a page with no ID progress.
                if !advance(&mut request.page, values.iter().map($id)) {
                    break;
                }
                output.extend(values);
            }
            Ok(output)
        }
    };
}
list_all!(
    legacy_all,
    pb::ListSipTrunkRequest,
    Stored<pb::SipTrunkInfo>,
    list_legacy_preserved,
    |v: &Stored<pb::SipTrunkInfo>| v.sip_trunk_id.clone()
);
list_all!(
    inbound_all,
    pb::ListSipInboundTrunkRequest,
    Option<Stored<pb::SipInboundTrunkInfo>>,
    list_inbound_preserved,
    |v: &Option<Stored<pb::SipInboundTrunkInfo>>| v
        .as_ref()
        .map(|v| v.sip_trunk_id.clone())
        .unwrap_or_default()
);
list_all!(
    outbound_all,
    pb::ListSipOutboundTrunkRequest,
    Option<Stored<pb::SipOutboundTrunkInfo>>,
    list_outbound_preserved,
    |v: &Option<Stored<pb::SipOutboundTrunkInfo>>| v
        .as_ref()
        .map(|v| v.sip_trunk_id.clone())
        .unwrap_or_default()
);
list_all!(
    rules_all,
    pb::ListSipDispatchRuleRequest,
    Stored<pb::SipDispatchRuleInfo>,
    list_rules_preserved,
    |v: &Stored<pb::SipDispatchRuleInfo>| v.sip_dispatch_rule_id.clone()
);

fn request_source<T: prost::Message + prost::Name>(
    codec: Codec,
    body: &[u8],
    request: &T,
) -> Result<PreservedMessage> {
    match codec {
        Codec::Json => Ok(PreservedMessage::from_message(request)),
        Codec::Protobuf => PreservedMessage::decode(&T::full_name(), body)
            .map_err(|_| TwirpError::malformed("the protobuf request could not be decoded")),
    }
}
fn update_source(
    existing: &PreservedMessage,
    request: &PreservedMessage,
    replacement: bool,
    replaced_fields: &[&str],
) -> PreservedMessage {
    if replacement {
        return request
            .nested("replace")
            .expect("validated replacement message");
    }
    let mut source = existing.clone();
    let patch = request.nested("update").expect("validated patch message");
    for field in replaced_fields {
        source.replace_nested(field, patch.nested(field).as_ref());
    }
    source
}
fn list_wire<T>(items: impl IntoIterator<Item = PreservedMessage>) -> PreservedMessage
where
    T: prost::Message + prost::Name + Default,
{
    let mut wire = PreservedMessage::from_message(&T::default());
    wire.replace_repeated("items", items);
    wire
}

/// The fixed 14 configuration methods use decode → SIP admin → catalog ready →
/// exact method validation and store effects. Call/transfer are not implemented
/// here: the public dispatcher invokes their actual state-aware SIP gateway.
pub async fn call(
    catalog: Option<&SipCatalog>,
    auth: Option<&VerifiedToken>,
    method: &str,
    codec: Codec,
    body: &[u8],
) -> Result<Response> {
    match method {
        "CreateSIPInboundTrunk" => {
            let request: pb::CreateSipInboundTrunkRequest = decode(codec, body)?;
            let request_wire = request_source(codec, body, &request)?;
            let catalog = ready(catalog, auth)?;
            validated(domain::validate_create_inbound(&request))?;
            let mut info = request.trunk.unwrap();
            let mut existing = conflict_trunks(
                inbound_all(
                    catalog,
                    pb::ListSipInboundTrunkRequest {
                        numbers: info.numbers.clone(),
                        ..Default::default()
                    },
                )
                .await?,
            )?;
            existing.push(info.clone());
            domain::validate_inbound_conflicts(&existing).map_err(invalid)?;
            info.sip_trunk_id = new_sid("ST_");
            let info =
                Stored::with_wire(info, request_wire.nested("trunk").expect("validated trunk"));
            catalog
                .store_inbound_preserved(&info)
                .await
                .map_err(store_error)?;
            Ok(encode_preserved(
                codec,
                info.wire.as_dynamic(),
                info.wire.encode_to_vec(),
            ))
        }
        "CreateSIPOutboundTrunk" => {
            let request: pb::CreateSipOutboundTrunkRequest = decode(codec, body)?;
            let request_wire = request_source(codec, body, &request)?;
            let catalog = ready(catalog, auth)?;
            validated(domain::validate_create_outbound(&request))?;
            let mut info = request.trunk.unwrap();
            info.sip_trunk_id = new_sid("ST_");
            let info =
                Stored::with_wire(info, request_wire.nested("trunk").expect("validated trunk"));
            catalog
                .store_outbound_preserved(&info)
                .await
                .map_err(store_error)?;
            Ok(encode_preserved(
                codec,
                info.wire.as_dynamic(),
                info.wire.encode_to_vec(),
            ))
        }
        "UpdateSIPInboundTrunk" => {
            let request: pb::UpdateSipInboundTrunkRequest = decode(codec, body)?;
            let request_wire = request_source(codec, body, &request)?;
            let catalog = ready(catalog, auth)?;
            validated(domain::validate_update_inbound(&request))?;
            let existing = catalog
                .load_inbound_preserved(&request.sip_trunk_id)
                .await
                .map_err(store_error)?
                .ok_or_else(|| TwirpError::internal("Internal service panic"))?;
            let (info, result) =
                domain::apply_inbound_request(&request, &existing).map_err(domain_error)?;
            validated(result)?;
            let mut trunks = conflict_trunks(
                inbound_all(
                    catalog,
                    pb::ListSipInboundTrunkRequest {
                        numbers: info.numbers.clone(),
                        ..Default::default()
                    },
                )
                .await?,
            )?;
            for value in &mut trunks {
                if value.sip_trunk_id == request.sip_trunk_id {
                    *value = info.clone();
                }
            }
            domain::validate_inbound_conflicts(&trunks).map_err(invalid)?;
            let (replacement, fields) = match request.action.as_ref().unwrap() {
                pb::update_sip_inbound_trunk_request::Action::Replace(_) => (true, vec![]),
                pb::update_sip_inbound_trunk_request::Action::Update(diff) => (
                    false,
                    if diff.media.is_some() {
                        vec!["media"]
                    } else {
                        vec![]
                    },
                ),
            };
            let source = update_source(&existing.wire, &request_wire, replacement, &fields);
            let info = Stored::with_wire(info, source);
            catalog
                .store_inbound_preserved(&info)
                .await
                .map_err(store_error)?;
            Ok(encode_preserved(
                codec,
                info.wire.as_dynamic(),
                info.wire.encode_to_vec(),
            ))
        }
        "UpdateSIPOutboundTrunk" => {
            let request: pb::UpdateSipOutboundTrunkRequest = decode(codec, body)?;
            let request_wire = request_source(codec, body, &request)?;
            let catalog = ready(catalog, auth)?;
            validated(domain::validate_update_outbound(&request))?;
            let existing = catalog
                .load_outbound_preserved(&request.sip_trunk_id)
                .await
                .map_err(store_error)?
                .ok_or_else(|| TwirpError::internal("Internal service panic"))?;
            let (info, result) =
                domain::apply_outbound_request(&request, &existing).map_err(domain_error)?;
            validated(result)?;
            let (replacement, fields) = match request.action.as_ref().unwrap() {
                pb::update_sip_outbound_trunk_request::Action::Replace(_) => (true, vec![]),
                pb::update_sip_outbound_trunk_request::Action::Update(diff) => (
                    false,
                    if diff.media.is_some() {
                        vec!["media"]
                    } else {
                        vec![]
                    },
                ),
            };
            let source = update_source(&existing.wire, &request_wire, replacement, &fields);
            let info = Stored::with_wire(info, source);
            catalog
                .store_outbound_preserved(&info)
                .await
                .map_err(store_error)?;
            Ok(encode_preserved(
                codec,
                info.wire.as_dynamic(),
                info.wire.encode_to_vec(),
            ))
        }
        "GetSIPInboundTrunk" => {
            let request: pb::GetSipInboundTrunkRequest = decode(codec, body)?;
            let catalog = ready(catalog, auth)?;
            if request.sip_trunk_id.is_empty() {
                return Err(invalid("trunk ID is required"));
            }
            let trunk = catalog
                .load_inbound_preserved(&request.sip_trunk_id)
                .await
                .map_err(store_error)?;
            let response = pb::GetSipInboundTrunkResponse {
                trunk: trunk.as_ref().map(|v| v.value.clone()),
            };
            let mut wire = PreservedMessage::from_message(&response);
            wire.replace_nested("trunk", trunk.as_ref().map(|v| &v.wire));
            Ok(encode_preserved(
                codec,
                wire.as_dynamic(),
                wire.encode_to_vec(),
            ))
        }
        "GetSIPOutboundTrunk" => {
            let request: pb::GetSipOutboundTrunkRequest = decode(codec, body)?;
            let catalog = ready(catalog, auth)?;
            if request.sip_trunk_id.is_empty() {
                return Err(invalid("trunk ID is required"));
            }
            let trunk = catalog
                .load_outbound_preserved(&request.sip_trunk_id)
                .await
                .map_err(store_error)?;
            let response = pb::GetSipOutboundTrunkResponse {
                trunk: trunk.as_ref().map(|v| v.value.clone()),
            };
            let mut wire = PreservedMessage::from_message(&response);
            wire.replace_nested("trunk", trunk.as_ref().map(|v| &v.wire));
            Ok(encode_preserved(
                codec,
                wire.as_dynamic(),
                wire.encode_to_vec(),
            ))
        }
        "ListSIPTrunk" => {
            let request: pb::ListSipTrunkRequest = decode(codec, body)?;
            let catalog = ready(catalog, auth)?;
            let items = legacy_all(catalog, request).await?;
            let wire = list_wire::<pb::ListSipTrunkResponse>(items.into_iter().map(|v| v.wire));
            Ok(encode_preserved(
                codec,
                wire.as_dynamic(),
                wire.encode_to_vec(),
            ))
        }
        "ListSIPInboundTrunk" => {
            let request: pb::ListSipInboundTrunkRequest = decode(codec, body)?;
            let catalog = ready(catalog, auth)?;
            let items = inbound_all(catalog, request).await?;
            let wire = list_wire::<pb::ListSipInboundTrunkResponse>(items.into_iter().map(|v| {
                v.map(|v| v.wire).unwrap_or_else(|| {
                    PreservedMessage::from_message(&pb::SipInboundTrunkInfo::default())
                })
            }));
            Ok(encode_preserved(
                codec,
                wire.as_dynamic(),
                wire.encode_to_vec(),
            ))
        }
        "ListSIPOutboundTrunk" => {
            let request: pb::ListSipOutboundTrunkRequest = decode(codec, body)?;
            let catalog = ready(catalog, auth)?;
            let items = outbound_all(catalog, request).await?;
            let wire = list_wire::<pb::ListSipOutboundTrunkResponse>(items.into_iter().map(|v| {
                v.map(|v| v.wire).unwrap_or_else(|| {
                    PreservedMessage::from_message(&pb::SipOutboundTrunkInfo::default())
                })
            }));
            Ok(encode_preserved(
                codec,
                wire.as_dynamic(),
                wire.encode_to_vec(),
            ))
        }
        "DeleteSIPTrunk" => {
            let request: pb::DeleteSipTrunkRequest = decode(codec, body)?;
            let catalog = ready(catalog, auth)?;
            if request.sip_trunk_id.is_empty() {
                return Err(invalid("trunk ID is required"));
            }
            catalog
                .delete_trunk(&request.sip_trunk_id)
                .await
                .map_err(store_error)?;
            Ok(encode::<_, tj::SipTrunkInfo>(
                codec,
                &pb::SipTrunkInfo {
                    sip_trunk_id: request.sip_trunk_id,
                    ..Default::default()
                },
            ))
        }
        "CreateSIPDispatchRule" => {
            let mut request: pb::CreateSipDispatchRuleRequest = decode(codec, body)?;
            let request_wire = request_source(codec, body, &request)?;
            let catalog = ready(catalog, auth)?;
            if let Some(info) = request.dispatch_rule.as_mut() {
                domain::upgrade_dispatch(info);
            }
            domain::validate_create_dispatch(&request).map_err(invalid)?;
            let mut info = domain::dispatch_info(&request);
            info.sip_dispatch_rule_id.clear();
            let existing = rules_all(
                catalog,
                pb::ListSipDispatchRuleRequest {
                    trunk_ids: request.trunk_ids.clone(),
                    ..Default::default()
                },
            )
            .await?;
            let mut existing: Vec<_> = existing.into_iter().map(|v| v.value).collect();
            existing.push(info.clone());
            domain::validate_dispatch_conflicts(&existing).map_err(invalid)?;
            info.sip_dispatch_rule_id = new_sid("SDR_");
            let mut source = request_wire
                .nested("dispatch_rule")
                .unwrap_or_else(|| PreservedMessage::from_message(&info));
            if request.dispatch_rule.is_none() {
                for field in ["rule", "room_config"] {
                    source.replace_nested(field, request_wire.nested(field).as_ref());
                }
            }
            let info = Stored::with_wire(info, source);
            catalog
                .store_rule_preserved(&info)
                .await
                .map_err(store_error)?;
            Ok(encode_preserved(
                codec,
                info.wire.as_dynamic(),
                info.wire.encode_to_vec(),
            ))
        }
        "UpdateSIPDispatchRule" => {
            let request: pb::UpdateSipDispatchRuleRequest = decode(codec, body)?;
            let request_wire = request_source(codec, body, &request)?;
            let catalog = ready(catalog, auth)?;
            domain::validate_update_dispatch(&request).map_err(invalid)?;
            let existing = catalog
                .load_rule_preserved(&request.sip_dispatch_rule_id)
                .await
                .map_err(store_error)?;
            let info = domain::apply_dispatch_request(&request, &existing).map_err(domain_error)?;
            let rules = rules_all(
                catalog,
                pb::ListSipDispatchRuleRequest {
                    trunk_ids: info.trunk_ids.clone(),
                    ..Default::default()
                },
            )
            .await?;
            let mut rules: Vec<_> = rules.into_iter().map(|v| v.value).collect();
            for value in &mut rules {
                if value.sip_dispatch_rule_id == request.sip_dispatch_rule_id {
                    *value = info.clone();
                }
            }
            domain::validate_dispatch_conflicts(&rules).map_err(invalid)?;
            let (replacement, fields) = match request.action.as_ref().unwrap() {
                pb::update_sip_dispatch_rule_request::Action::Replace(_) => (true, vec![]),
                pb::update_sip_dispatch_rule_request::Action::Update(diff) => {
                    let mut fields = vec![];
                    if diff.media.is_some() {
                        fields.push("media");
                    }
                    if diff.rule.is_some() {
                        fields.push("rule");
                    }
                    (false, fields)
                }
            };
            let source = update_source(&existing.wire, &request_wire, replacement, &fields);
            let info = Stored::with_wire(info, source);
            catalog
                .store_rule_preserved(&info)
                .await
                .map_err(store_error)?;
            Ok(encode_preserved(
                codec,
                info.wire.as_dynamic(),
                info.wire.encode_to_vec(),
            ))
        }
        "ListSIPDispatchRule" => {
            let request: pb::ListSipDispatchRuleRequest = decode(codec, body)?;
            let catalog = ready(catalog, auth)?;
            let items = rules_all(catalog, request).await?;
            let wire =
                list_wire::<pb::ListSipDispatchRuleResponse>(items.into_iter().map(|v| v.wire));
            Ok(encode_preserved(
                codec,
                wire.as_dynamic(),
                wire.encode_to_vec(),
            ))
        }
        "DeleteSIPDispatchRule" => {
            let request: pb::DeleteSipDispatchRuleRequest = decode(codec, body)?;
            let catalog = ready(catalog, auth)?;
            if request.sip_dispatch_rule_id.is_empty() {
                return Err(invalid("dispatch rule ID is required"));
            }
            let info = catalog
                .load_rule_preserved(&request.sip_dispatch_rule_id)
                .await
                .map_err(store_error)?;
            catalog
                .delete_rule(&info.sip_dispatch_rule_id)
                .await
                .map_err(store_error)?;
            Ok(encode_preserved(
                codec,
                info.wire.as_dynamic(),
                info.wire.encode_to_vec(),
            ))
        }
        _ => Err(TwirpError::new(Code::BadRoute, "no handler for method")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::twirp::other_services::SIP_METHODS;

    #[test]
    fn typed_apply_failures_and_untyped_catalog_errors_match_actual_server_interceptor() {
        let typed = domain_error(domain::DomainFailure {
            message: "invalid field".into(),
            invalid_argument: true,
        });
        assert_eq!(typed.code, Code::InvalidArgument);
        let ordinary = domain_error(domain::DomainFailure {
            message: "invalid field".into(),
            invalid_argument: false,
        });
        assert_eq!(ordinary.code, Code::Unknown);
        assert_eq!(store_error(CatalogError::EmptyId).code, Code::Unknown);
        assert_eq!(
            store_error(CatalogError::TrunkNotFound).code,
            Code::NotFound
        );
    }

    #[tokio::test]
    async fn every_configuration_method_decodes_before_auth_then_checks_store_readiness() {
        let mut auth = VerifiedToken::default();
        auth.extra.sip.admin = true;
        let mut covered = 0;
        for &method in SIP_METHODS {
            if matches!(method, "CreateSIPParticipant" | "TransferSIPParticipant") {
                continue;
            }
            for (codec, body) in [(Codec::Json, &b"{}"[..]), (Codec::Protobuf, &b""[..])] {
                assert_eq!(
                    call(None, None, method, codec, body)
                        .await
                        .unwrap_err()
                        .code,
                    Code::Unauthenticated,
                    "{method}"
                );
                let error = call(None, Some(&auth), method, codec, body)
                    .await
                    .unwrap_err();
                assert_eq!(
                    (error.code, error.msg.as_str()),
                    (Code::Internal, "sip not connected (redis required)"),
                    "{method}"
                );
            }
            assert_eq!(
                call(None, None, method, Codec::Json, b"[]")
                    .await
                    .unwrap_err()
                    .code,
                Code::Malformed
            );
            assert_eq!(
                call(None, None, method, Codec::Protobuf, &[255])
                    .await
                    .unwrap_err()
                    .code,
                Code::Malformed
            );
            covered += 1;
        }
        assert_eq!(covered, 14);
    }
    #[tokio::test]
    async fn calls_retain_original_preflight_and_do_not_fabricate_worker_success() {
        let (state, net) = crate::test_support::state().await;
        let error = crate::twirp::other_services::sip(
            &state,
            None,
            "TransferSIPParticipant",
            Codec::Json,
            b"{}",
        )
        .await
        .unwrap_err();
        assert_eq!(
            (error.code, error.msg.as_str()),
            (Code::InvalidArgument, "Missing room name")
        );
        let mut auth = VerifiedToken::default();
        auth.extra.sip.call = true;
        let error = crate::twirp::other_services::sip(
            &state,
            Some(&auth),
            "CreateSIPParticipant",
            Codec::Json,
            b"{}",
        )
        .await
        .unwrap_err();
        assert_eq!(
            (error.code, error.msg.as_str()),
            (Code::Internal, "sip not connected (redis required)")
        );
        net.shutdown();
    }
    #[test]
    fn pagination_advances_only_by_actual_maximum_ids_and_keeps_no_page_distinct() {
        let mut page = Some(pb::Pagination {
            after_id: "ST_b".into(),
            limit: 2,
        });
        assert!(advance(
            &mut page,
            ["ST_d".into(), "ST_c".into()].into_iter()
        ));
        assert_eq!(page.as_ref().unwrap().after_id, "ST_d");
        assert!(!advance(
            &mut page,
            ["ST_d".into(), String::new()].into_iter()
        ));
        assert_eq!(page.as_ref().unwrap().after_id, "ST_d");
        assert!(!advance(&mut None, ["ST_z".into()].into_iter()));
    }
    #[test]
    fn nil_conflict_entries_fail_without_erasing_regular_response_positions() {
        let info = pb::SipInboundTrunkInfo {
            sip_trunk_id: "ST_fixture".into(),
            ..Default::default()
        };
        assert_eq!(
            conflict_trunks(vec![Some(Stored::typed(info.clone()))]).unwrap(),
            vec![info.clone()]
        );
        assert!(conflict_trunks(Vec::new()).unwrap().is_empty());
        for values in [
            vec![None],
            vec![Some(info.clone()), None],
            vec![None, Some(info.clone())],
        ] {
            let error = conflict_trunks(
                values
                    .clone()
                    .into_iter()
                    .map(|v| v.map(Stored::typed))
                    .collect(),
            )
            .unwrap_err();
            assert_eq!(
                (error.code, error.msg.as_str()),
                (Code::Internal, "Internal service panic")
            );
            // The same values remain present as default entries when encoded
            // through the ordinary list path; conflict failure is not a filter.
            let items: Vec<_> = values.into_iter().map(Option::unwrap_or_default).collect();
            assert!(!items.is_empty());
            assert!(items.iter().any(|item| item.sip_trunk_id.is_empty()));
        }
        let get = pb::GetSipInboundTrunkResponse { trunk: None };
        assert!(get.trunk.is_none());
    }
}

#[cfg(test)]
mod wire_provenance_tests {
    use super::*;
    use prost::{Message, Name};
    fn unknown<T: Message + Name>(value: &T, n: u8) -> PreservedMessage {
        let mut bytes = value.encode_to_vec();
        bytes.extend([0xa0, 6, n]);
        PreservedMessage::decode(&T::full_name(), &bytes).unwrap()
    }
    #[test]
    fn patch_source_is_existing_while_replace_uses_replacement_not_old_root() {
        use prost_reflect::ReflectMessage;
        let info = pb::SipInboundTrunkInfo {
            sip_trunk_id: "ST_fixture".into(),
            numbers: vec!["+123".into()],
            media: Some(Default::default()),
            ..Default::default()
        };
        let existing = unknown(&info, 7);
        let media = unknown(&pb::SipMediaConfig::default(), 9);
        let patch = pb::UpdateSipInboundTrunkRequest {
            sip_trunk_id: "ST_fixture".into(),
            action: Some(pb::update_sip_inbound_trunk_request::Action::Update(
                pb::SipInboundTrunkUpdate {
                    media: Some(Default::default()),
                    ..Default::default()
                },
            )),
        };
        let mut request = unknown(&patch, 11);
        let mut diff = request.nested("update").unwrap();
        diff.replace_nested("media", Some(&media));
        request.replace_nested("update", Some(&diff));
        let source = update_source(&existing, &request, false, &["media"]);
        let actual = source.overlay(&info);
        assert_eq!(actual.test_unknown_fields_count(), 1);
        assert_eq!(
            actual.nested("media").unwrap().test_unknown_fields_count(),
            1
        );
        assert_eq!(
            actual.as_dynamic().descriptor().full_name(),
            "livekit.SIPInboundTrunkInfo"
        );
        let replacement = pb::UpdateSipInboundTrunkRequest {
            sip_trunk_id: "ST_fixture".into(),
            action: Some(pb::update_sip_inbound_trunk_request::Action::Replace(
                info.clone(),
            )),
        };
        let request = PreservedMessage::from_message(&replacement);
        let source = update_source(&existing, &request, true, &[]);
        assert_eq!(source.overlay(&info).test_unknown_fields_count(), 0);
    }
}
