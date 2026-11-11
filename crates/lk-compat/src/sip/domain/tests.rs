use super::*;

fn inbound(id: &str) -> pb::SipInboundTrunkInfo {
    pb::SipInboundTrunkInfo {
        sip_trunk_id: id.into(),
        numbers: vec!["+123".into()],
        ..Default::default()
    }
}
fn outbound() -> pb::SipOutboundTrunkInfo {
    pb::SipOutboundTrunkInfo {
        sip_trunk_id: "ST_original".into(),
        numbers: vec!["+123".into()],
        address: "example.org".into(),
        ..Default::default()
    }
}
fn direct(id: &str, pin: &str) -> pb::SipDispatchRuleInfo {
    pb::SipDispatchRuleInfo {
        sip_dispatch_rule_id: id.into(),
        rule: Some(pb::SipDispatchRule {
            rule: Some(pb::sip_dispatch_rule::Rule::DispatchRuleDirect(
                pb::SipDispatchRuleDirect {
                    room_name: "room".into(),
                    pin: pin.into(),
                },
            )),
        }),
        ..Default::default()
    }
}

#[test]
fn list_patch_distinguishes_presence_clear_and_first_duplicate_removal() {
    assert_eq!(
        validate_list_update(Some(&pb::ListUpdate::default())).unwrap_err(),
        "unsupported list update operation"
    );
    assert_eq!(
        validate_list_update(Some(&pb::ListUpdate {
            clear: true,
            add: vec!["x".into()],
            ..Default::default()
        }))
        .unwrap_err(),
        "cannot clear and change the list at the same time"
    );
    let mut value = vec!["a".into(), "a".into(), "b".into()];
    apply_list_update(
        &mut value,
        Some(&pb::ListUpdate {
            remove: vec!["a".into()],
            add: vec!["b".into(), "c".into()],
            ..Default::default()
        }),
    )
    .unwrap();
    assert_eq!(value, vec!["a", "b", "c"]);
    apply_list_update(
        &mut value,
        Some(&pb::ListUpdate {
            set: vec!["z".into(), "z".into()],
            ..Default::default()
        }),
    )
    .unwrap();
    assert_eq!(value, vec!["z", "z"]);
    apply_list_update(
        &mut value,
        Some(&pb::ListUpdate {
            clear: true,
            ..Default::default()
        }),
    )
    .unwrap();
    assert!(value.is_empty());
}
#[test]
fn hard_header_errors_and_soft_name_addr_warnings_stay_separate() {
    let mut info = inbound("");
    info.headers.insert("To".into(), "not-a-uri".into());
    let result = validate_inbound(&info);
    assert!(result.ok());
    assert_eq!(result.soft.len(), 2);
    assert!(result.soft[0].contains("to_user_override"));
    assert!(result.soft[1].contains("uri: scheme"));
    info.headers.clear();
    info.headers
        .insert("From".into(), "sip:a@example.org".into());
    assert_eq!(
        validate_inbound(&info).hard.as_deref(),
        Some("invalid header name: header name From not supported")
    );
    assert!(validate_header_value("X-Test", "東京").ok());
    assert!(
        validate_header_value("X-Test", "ok\r\nInjected")
            .hard
            .unwrap()
            .contains("char 13 not allowed")
    );
    assert!(
        validate_header_value("P-Asserted-Identity", r#""Name\x20Here" <sip:a@host>"#)
            .soft
            .is_empty()
    );
    assert_eq!(
        validate_header_value("P-Asserted-Identity", "<sip:a@host><sip:b@host>")
            .soft
            .len(),
        1
    );
}
#[test]
fn security_presence_unknown_status_and_hostname_validation_match_fixed_source() {
    assert!(!validate_inbound(&pb::SipInboundTrunkInfo::default()).ok());
    assert!(
        validate_inbound(&pb::SipInboundTrunkInfo {
            auth_username: "only-one-half".into(),
            ..Default::default()
        })
        .ok()
    );
    // No extra parser invented for masks or E.164 numbers in trunk info validation.
    assert!(
        validate_inbound(&pb::SipInboundTrunkInfo {
            allowed_addresses: vec!["arbitrary-mask".into()],
            ..Default::default()
        })
        .ok()
    );
    let mut info = inbound("");
    info.ringing_timeout_status = 399;
    assert_eq!(
        validate_inbound(&info).hard.as_deref(),
        Some("ringing timeout status must be 4xx-6xx, got 399")
    );
    for valid in ["", "HOST:5060", "sIp:host", "host?x"] {
        validate_hostname(valid, "from_host").unwrap();
    }
    assert_eq!(
        validate_hostname("sip:a@host;transport=tcp", "from_host").unwrap_err(),
        "from_host should not contain transport parameter"
    );
    assert_eq!(
        validate_hostname("sip:host", "trunk address").unwrap_err(),
        "trunk address should be a hostname or IP, not SIP URI"
    );
}
#[test]
fn update_is_clone_before_write_and_preserves_optional_empty_values() {
    let original = outbound();
    let invalid = pb::UpdateSipOutboundTrunkRequest {
        sip_trunk_id: "ST_original".into(),
        action: Some(pb::update_sip_outbound_trunk_request::Action::Update(
            pb::SipOutboundTrunkUpdate {
                address: Some(String::new()),
                ..Default::default()
            },
        )),
    };
    assert_eq!(
        apply_outbound_request(&invalid, &original)
            .unwrap_err()
            .message,
        "no outbound address specified"
    );
    assert_eq!(original.address, "example.org");
    let update = pb::UpdateSipOutboundTrunkRequest {
        sip_trunk_id: "ST_original".into(),
        action: Some(pb::update_sip_outbound_trunk_request::Action::Update(
            pb::SipOutboundTrunkUpdate {
                name: Some(String::new()),
                media_encryption: Some(2),
                ..Default::default()
            },
        )),
    };
    let (value, _) = apply_outbound_request(&update, &original).unwrap();
    assert_eq!(value.sip_trunk_id, "ST_original");
    assert_eq!(value.media.as_ref().unwrap().encryption, Some(2));
    assert_eq!(value.media_encryption, 2);
    assert!(original.media.is_none());
    let wrong = pb::UpdateSipOutboundTrunkRequest {
        sip_trunk_id: "different".into(),
        action: Some(pb::update_sip_outbound_trunk_request::Action::Replace(
            original.clone(),
        )),
    };
    assert_eq!(
        validate_update_outbound(&wrong).hard.as_deref(),
        Some("trunk id in the info must be empty or match the id in the update")
    );
}
#[test]
fn media_upgrade_allocates_optional_message_but_zero_legacy_stays_unset() {
    let mut zero = inbound("");
    upgrade_inbound(&mut zero);
    assert!(zero.media.is_some());
    assert_eq!(zero.media.unwrap().encryption, None);
    let mut old = inbound("");
    old.media_encryption = 1;
    upgrade_inbound(&mut old);
    assert_eq!(old.media.unwrap().encryption, Some(1));
    let first = pb::SipMediaConfig {
        encryption: Some(2),
        codecs: vec![pb::SipCodec {
            name: "opus".into(),
            rate: 48000,
        }],
        only_listed_codecs: true,
        ..Default::default()
    };
    let next = pb::SipMediaConfig {
        encryption: Some(0),
        ..Default::default()
    };
    let merged = merge_media(Some(&first), Some(&next)).unwrap();
    assert_eq!(merged.encryption, Some(2));
    assert!(merged.only_listed_codecs);
    assert_eq!(merged.codecs.len(), 1);
    let invalid = pb::SipMediaConfig {
        codecs: vec![pb::SipCodec {
            name: "audio/opus".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    assert_eq!(
        validate_media(Some(&invalid)).unwrap_err(),
        "codec name must not contain '/'"
    );
}
#[test]
fn conflict_buckets_normalize_numbers_without_overrejecting_specific_rules() {
    let mut a = inbound("a");
    let mut b = inbound("b");
    a.numbers = vec!["+1 (23)".into(), "123".into()];
    b.numbers = vec!["123".into()];
    assert!(validate_inbound_conflicts(&[a.clone(), b.clone()]).is_err());
    a.allowed_numbers = vec!["+999".into()];
    validate_inbound_conflicts(&[a.clone(), b.clone()]).unwrap();
    b.allowed_numbers = vec!["999".into()];
    assert!(validate_inbound_conflicts(&[a, b]).is_err());
    let a = direct("first", "");
    let mut b = direct("second", "");
    assert!(validate_dispatch_conflicts(&[a.clone(), b.clone()]).is_err());
    b.trunk_ids = vec!["specific".into()];
    validate_dispatch_conflicts(&[a, b]).unwrap();
    let duplicate = pb::SipDispatchRuleInfo {
        trunk_ids: vec!["same".into(), "same".into()],
        ..direct("duplicate", "")
    };
    assert!(validate_dispatch_conflicts(&[duplicate]).is_err());
}
#[test]
fn number_and_address_matching_preserve_permissive_source_edges() {
    for (input, out) in [
        ("", ""),
        ("+", "+"),
        ("( )", "+"),
        ("1 (23)-4", "+1234"),
        ("١٢٣", "١٢٣"),
        ("+123x", "+123x"),
    ] {
        assert_eq!(normalize_number(input), out);
    }
    assert!(match_address_masks(
        "invalid-peer",
        "host",
        &["192.0.2.0/24".into()]
    ));
    assert!(match_address_masks(
        "192.0.2.1",
        "host",
        &["bad mask".into()]
    ));
    assert!(!match_address_masks("192.0.2.1", "host", &[String::new()]));
    assert!(match_address_masks(
        "192.0.2.1",
        "same-host",
        &["same-host".into()]
    ));
    assert!(!match_address_masks(
        "2001:db8::1",
        "host",
        &["192.0.2.0/24".into()]
    ));
    assert!(match_address_masks(
        "2001:db8::1%eth0",
        "host",
        &["2001:db8::1%eth0".into()]
    ));
    assert!(!match_address_masks(
        "2001:db8::1%eth0",
        "host",
        &["2001:db8::/32".into()]
    ));
}

#[test]
fn post_load_apply_preserves_typed_psrpc_error_origin_and_does_not_mutate_store_value() {
    let original = inbound("ST_original");
    let request = pb::UpdateSipInboundTrunkRequest {
        sip_trunk_id: original.sip_trunk_id.clone(),
        action: Some(pb::update_sip_inbound_trunk_request::Action::Update(
            pb::SipInboundTrunkUpdate {
                numbers: Some(pb::ListUpdate {
                    clear: true,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )),
    };
    let failure = apply_inbound_request(&request, &original).unwrap_err();
    assert!(failure.invalid_argument);
    assert_eq!(
        failure.message,
        "for security, one of the fields must be set: AuthUsername+AuthPassword, AllowedAddresses or Numbers"
    );
    assert_eq!(original.numbers, vec!["+123"]);
    let original = outbound();
    let request = pb::UpdateSipOutboundTrunkRequest {
        sip_trunk_id: original.sip_trunk_id.clone(),
        action: Some(pb::update_sip_outbound_trunk_request::Action::Update(
            pb::SipOutboundTrunkUpdate {
                numbers: Some(pb::ListUpdate {
                    clear: true,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )),
    };
    let failure = apply_outbound_request(&request, &original).unwrap_err();
    assert!(!failure.invalid_argument);
    assert_eq!(failure.message, "no trunk numbers specified");
    assert_eq!(original.numbers, vec!["+123"]);
}
