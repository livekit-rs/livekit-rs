use super::*;
use prost::{Message, Name};
fn trunk(id: &str, numbers: &[&str]) -> Stored<pb::SipInboundTrunkInfo> {
    Stored::typed(pb::SipInboundTrunkInfo {
        sip_trunk_id: id.into(),
        numbers: numbers.iter().map(|v| (*v).into()).collect(),
        ..Default::default()
    })
}
fn direct(id: &str, room: &str, pin: &str) -> Stored<pb::SipDispatchRuleInfo> {
    Stored::typed(pb::SipDispatchRuleInfo {
        sip_dispatch_rule_id: id.into(),
        rule: Some(pb::SipDispatchRule {
            rule: Some(pb::sip_dispatch_rule::Rule::DispatchRuleDirect(
                pb::SipDispatchRuleDirect {
                    room_name: room.into(),
                    pin: pin.into(),
                },
            )),
        }),
        ..Default::default()
    })
}
fn request() -> rpc::EvaluateSipDispatchRulesRequest {
    rpc::EvaluateSipDispatchRulesRequest {
        sip_call_id: "SCL_test".into(),
        calling_number: "+12345678".into(),
        calling_host: "example.test".into(),
        called_number: "+1800".into(),
        src_address: "192.0.2.7:5060".into(),
        ..Default::default()
    }
}
#[test]
fn source_cidr_caller_filter_and_specific_over_default_have_real_conflicts() {
    let req = request();
    let call = dispatch_call(&req);
    let mut specific = trunk("ST_specific", &["1 (800)"]);
    specific.value.allowed_addresses = vec!["192.0.2.0/24".into()];
    specific.value.allowed_numbers = vec!["1 234-5678".into()];
    let fallback = trunk("ST_default", &[]);
    assert_eq!(
        match_trunks(&[fallback.clone(), specific.clone()], &call)
            .unwrap()
            .unwrap()
            .sip_trunk_id,
        "ST_specific"
    );
    specific.value.allowed_addresses = vec!["198.51.100.0/24".into()];
    assert_eq!(
        match_trunks(&[fallback.clone(), specific], &call)
            .unwrap()
            .unwrap()
            .sip_trunk_id,
        "ST_default"
    );
    let error = match_trunks(&[fallback, trunk("ST_other", &[])], &call).unwrap_err();
    assert_eq!(error.code, "unknown");
    assert_eq!(
        error.message,
        "twirp error failed_precondition: Multiple default SIP Trunks matched for \"+1800\""
    );
    assert!(
        match_trunks(
            &[trunk("ST_a", &["+1800"]), trunk("ST_b", &["1800"])],
            &call
        )
        .is_err()
    );
}
#[test]
fn pin_is_prioritized_and_wrong_pin_cannot_fall_back_to_open_rule() {
    let rules = vec![
        direct("open", "open", ""),
        direct("pin", "protected", "1234"),
    ];
    let mut req = request();
    assert_eq!(
        match_rule(None, &rules, &req)
            .unwrap()
            .unwrap()
            .sip_dispatch_rule_id,
        "pin"
    );
    let output = evaluate_selected(None, &rules[1], &req, || "random".into()).unwrap();
    assert_eq!(
        output.message.result,
        rpc::SipDispatchResult::RequestPin as i32
    );
    assert!(output.message.participant_attributes.is_empty());
    assert!(output.message.room_config.is_none());
    req.pin = "wrong".into();
    assert!(match_rule(None, &rules, &req).unwrap().is_none());
    req.no_pin = true;
    assert_eq!(
        match_rule(None, &rules, &req)
            .unwrap()
            .unwrap()
            .sip_dispatch_rule_id,
        "open"
    );
}
#[test]
fn specific_dispatch_dominates_wildcard_and_all_rules_are_validated_before_filter() {
    let mut specific = direct("specific", "z", "");
    specific.value.trunk_ids = vec!["ST_a".into()];
    let rules = vec![direct("wild", "a", ""), specific];
    let req = request();
    let t = trunk("ST_a", &[]);
    assert_eq!(
        match_rule(Some(&t), &rules, &req)
            .unwrap()
            .unwrap()
            .sip_dispatch_rule_id,
        "specific"
    );
    assert_eq!(
        match_rule(None, &rules, &req)
            .unwrap()
            .unwrap()
            .sip_dispatch_rule_id,
        "wild"
    );
    let mut conflict = direct("duplicate", "elsewhere", "");
    conflict.value.numbers = vec!["unrelated".into()];
    let mut other = conflict.clone();
    other.value.sip_dispatch_rule_id = "other".into();
    assert_eq!(
        match_rule(None, &[conflict, other], &req).unwrap_err().code,
        "unknown"
    );
}
#[test]
fn modern_call_wins_legacy_normalization_but_dispatch_filters_use_real_legacy_fields() {
    let mut req = request();
    req.call = Some(rpc::SipCall {
        lk_call_id: "modern".into(),
        source_ip: "2001:db8::1".into(),
        from: Some(pb::SipUri {
            user: "modern-from".into(),
            ..Default::default()
        }),
        to: Some(pb::SipUri {
            user: "modern-to".into(),
            ..Default::default()
        }),
        ..Default::default()
    });
    assert_eq!(dispatch_call(&req).lk_call_id, "modern");
    let mut rule = direct("specific", "r", "");
    rule.value.numbers = vec!["modern-to".into()];
    assert!(match_rule(None, &[rule.clone()], &req).unwrap().is_none());
    rule.value.numbers = vec![req.called_number.clone()];
    assert!(match_rule(None, &[rule], &req).unwrap().is_some());
    assert_eq!(split_legacy_address("[fe80::1%eth0]:sip"), "fe80::1%eth0");
    assert_eq!(split_legacy_address("2001:db8::1"), "2001:db8::1");
}
#[test]
fn accepted_rule_preserves_attributes_privacy_headers_and_room_config() {
    let mut t = trunk("ST_test", &[]);
    t.value.auth_username = "u".into();
    t.value.auth_password = "p".into();
    t.value.auth_realm = "realm".into();
    let auth = auth_prompt(&t);
    assert_eq!(auth.realm, "realm");
    assert_eq!(auth.provider_info.unwrap().r#type, 2);
    t.value.headers.insert("X-Result".into(), "value".into());
    t.value.krisp_enabled = true;
    t.value.media_encryption = 1;
    let mut rule = direct("SDR_test", "room", "");
    rule.value.hide_phone_number = true;
    rule.value.attributes.insert("user".into(), "rule".into());
    rule.value
        .attributes
        .insert("sip.callID".into(), "wrong".into());
    rule.value.room_config = Some(pb::RoomConfiguration {
        name: "configured".into(),
        ..Default::default()
    });
    let mut req = request();
    req.extra_attributes.insert("user".into(), "request".into());
    let out = evaluate_selected(Some(&t), &rule, &req, || "random".into())
        .unwrap()
        .message;
    assert_eq!(out.participant_name, "Phone 5678");
    assert_ne!(out.participant_identity, "sip_+12345678");
    assert_eq!(out.participant_attributes["user"], "request");
    assert_eq!(out.participant_attributes["sip.callID"], "SCL_test");
    assert!(!out.participant_attributes.contains_key("sip.phoneNumber"));
    assert_eq!(out.headers["X-Result"], "value");
    assert_eq!(out.room_config.unwrap().name, "configured");
    assert_eq!(out.enabled_features, vec![1]);
    assert_eq!(out.media.unwrap().encryption, Some(1));
}
#[test]
fn room_generation_uses_caller_mask_for_individual_and_called_number_for_callee() {
    let mut r = direct("id", "", " ");
    r.value.rule = Some(pb::SipDispatchRule {
        rule: Some(pb::sip_dispatch_rule::Rule::DispatchRuleIndividual(
            pb::SipDispatchRuleIndividual {
                room_prefix: "in".into(),
                no_randomness: false,
                pin: String::new(),
            },
        )),
    });
    let mut req = request();
    let out = evaluate_selected(None, &r, &req, || "fixed".into()).unwrap();
    assert_eq!(out.message.room_name, "in_+12345678_fixed");
    r.value.rule = Some(pb::SipDispatchRule {
        rule: Some(pb::sip_dispatch_rule::Rule::DispatchRuleCallee(
            pb::SipDispatchRuleCallee {
                room_prefix: "callee".into(),
                randomize: true,
                pin: String::new(),
            },
        )),
    });
    req.called_number = "+999".into();
    let out = evaluate_selected(None, &r, &req, || "fixed".into()).unwrap();
    assert_eq!(out.message.room_name, "callee_+999_fixed");
}
#[test]
fn derived_media_and_room_config_clone_only_real_source_unknown_subtrees() {
    // Root unknown#100 is not copied across types; independent media roots
    // and each fallback codec/duration retain their own source provenance.
    let mut t = trunk("ST_test", &[]);
    let codec =
        PreservedMessage::decode(&pb::SipCodec::full_name(), &[0x0a, 1, b'a', 0xa0, 6, 7]).unwrap();
    let mut media = PreservedMessage::from_message(&pb::SipMediaConfig::default());
    media.replace_repeated("codecs", [codec]);
    let duration =
        PreservedMessage::decode("google.protobuf.Duration", &[8, 1, 0xa0, 6, 8]).unwrap();
    media.replace_nested("media_timeout", Some(&duration));
    t.wire.replace_nested("media", Some(&media));
    t.value = t.wire.project();
    let mut r = direct("id", "r", "");
    let rule_media =
        PreservedMessage::decode(&pb::SipMediaConfig::full_name(), &[0xa0, 6, 9]).unwrap();
    r.wire.replace_nested("media", Some(&rule_media));
    let room =
        PreservedMessage::decode(&pb::RoomConfiguration::full_name(), &[0xa0, 6, 10]).unwrap();
    r.wire.replace_nested("room_config", Some(&room));
    r.value = r.wire.project();
    let response = evaluate_selected(Some(&t), &r, &request(), || "random".into()).unwrap();
    let retained = PreservedMessage::decode(
        &rpc::EvaluateSipDispatchRulesResponse::full_name(),
        &response.encode(),
    )
    .unwrap();
    let merged = retained.nested("media").unwrap();
    assert!(merged.encode_to_vec().ends_with(&[0xa0, 6, 9]));
    assert!(
        merged.repeated("codecs")[0]
            .encode_to_vec()
            .ends_with(&[0xa0, 6, 7])
    );
    assert!(
        merged
            .nested("media_timeout")
            .unwrap()
            .encode_to_vec()
            .ends_with(&[0xa0, 6, 8])
    );
    assert!(
        retained
            .nested("room_config")
            .unwrap()
            .encode_to_vec()
            .ends_with(&[0xa0, 6, 10])
    );
    // Replacing codecs with fresh rule entries must discard the fallback marker.
    r.value.media.as_mut().unwrap().codecs = vec![pb::SipCodec {
        name: "fresh".into(),
        ..Default::default()
    }];
    r.wire.replace_nested(
        "media",
        Some(&PreservedMessage::from_message(
            r.value.media.as_ref().unwrap(),
        )),
    );
    let output = evaluate_selected(Some(&t), &r, &request(), String::new).unwrap();
    let wire = PreservedMessage::decode(
        &rpc::EvaluateSipDispatchRulesResponse::full_name(),
        &output.encode(),
    )
    .unwrap();
    assert_eq!(
        wire.nested("media").unwrap().repeated("codecs")[0].encode_to_vec(),
        pb::SipCodec {
            name: "fresh".into(),
            ..Default::default()
        }
        .encode_to_vec()
    );
}
#[test]
fn fixed_netip_error_validation_and_zones() {
    for ip in ["192.0.2.1", "::1", "fe80::1%eth0", "::ffff:192.0.2.1", ""] {
        assert!(netip::validate_source(ip).is_ok(), "{ip}");
    }
    for ip in [
        "host",
        "192.00.2.1",
        "256.1.2.3",
        "1..2.3",
        "1.2.3",
        "::1%",
        "::1::2",
        "1:2:3:4:5:6:7:8::",
        "[::1]",
    ] {
        assert!(netip::validate_source(ip).is_err(), "{ip}");
    }
    assert_eq!(
        netip::parse("192.00.2.1").unwrap_err(),
        "ParseAddr(\"192.00.2.1\"): IPv4 field has octet with leading zero"
    );
}
#[tokio::test]
async fn absent_catalog_is_an_actual_not_connected_error_not_an_empty_success() {
    let handler = CatalogSipCallbacks::new(None);
    assert_eq!(
        handler
            .authenticate(Default::default())
            .await
            .unwrap_err()
            .message,
        "sip not connected (redis required)"
    );
    assert_eq!(
        handler.evaluate(request()).await.unwrap_err().code,
        "internal"
    );
}

#[test]
fn trunk_conflict_quotes_non_printable_unicode_with_fixed_go_strconv_rules() {
    let mut req = request();
    req.called_number = "called\u{200d}number".into();
    let call = dispatch_call(&req);
    let error = match_trunks(&[trunk("ST_a", &[]), trunk("ST_b", &[])], &call).unwrap_err();
    assert_eq!(error.code, "unknown");
    assert_eq!(
        error.message,
        "twirp error failed_precondition: Multiple default SIP Trunks matched for \"called\\u200dnumber\""
    );
    let error = match_trunks(
        &[
            trunk("ST_a", &["called\u{200d}number"]),
            trunk("ST_b", &["called\u{200d}number"]),
        ],
        &call,
    )
    .unwrap_err();
    assert_eq!(
        error.message,
        "twirp error failed_precondition: Multiple SIP Trunks matched for \"called\\u200dnumber\""
    );
}
