use super::*;
#[test]
fn legacy_conversion_preserves_go_direction_and_first_number_only() {
    let legacy = SipTrunkInfo {
        sip_trunk_id: "ST_fixture".into(),
        kind: 0,
        outbound_number: "+123".into(),
        inbound_addresses: vec!["192.0.2.1".into()],
        inbound_numbers: vec!["456".into()],
        inbound_username: "synthetic".into(),
        outbound_address: "example.invalid".into(),
        transport: 2,
        ..Default::default()
    };
    let inbound = legacy_as_inbound(&legacy).unwrap();
    assert_eq!(inbound.numbers, ["+123"]);
    assert_eq!(inbound.allowed_addresses, legacy.inbound_addresses);
    assert_eq!(inbound.auth_username, "synthetic");
    assert_eq!(legacy_as_outbound(&legacy).unwrap().transport, 2);
    let mut outbound_only = legacy.clone();
    outbound_only.kind = 2;
    assert!(legacy_as_inbound(&outbound_only).is_none());
    let mut inbound_only = legacy;
    inbound_only.kind = 1;
    assert!(legacy_as_outbound(&inbound_only).is_none());
    let inbound = SipInboundTrunkInfo {
        numbers: vec!["one".into(), "two".into()],
        ..inbound
    };
    assert_eq!(inbound_as_legacy(&inbound).outbound_number, "one");
}
#[test]
fn inbound_normalizes_but_outbound_uses_literal_number_filter() {
    let req = ListSipInboundTrunkRequest {
        numbers: vec!["+1 (234) 5-6".into()],
        ..Default::default()
    };
    let inbound = SipInboundTrunkInfo {
        numbers: vec!["123456".into()],
        ..Default::default()
    };
    assert!(inbound_filter(&req, &inbound));
    let outbound = SipOutboundTrunkInfo {
        numbers: inbound.numbers,
        ..Default::default()
    };
    assert!(!outbound_filter(
        &ListSipOutboundTrunkRequest {
            numbers: req.numbers.clone(),
            ..Default::default()
        },
        &outbound
    ));
    assert!(inbound_filter(&req, &SipInboundTrunkInfo::default()));
}
#[test]
fn normalize_exact_ascii_regex_including_formatting_only() {
    for (input, want) in [
        ("", ""),
        ("+", "+"),
        (" ()-", "+"),
        ("+ ()-", "+"),
        ("123", "+123"),
        ("１２", "１２"),
        ("1\t2", "1\t2"),
        ("++12", "++12"),
        ("a12", "a12"),
    ] {
        assert_eq!(normalize_number(input), want);
    }
}
#[test]
fn rule_wildcards_and_ids_are_conjunctive() {
    let req = ListSipDispatchRuleRequest {
        dispatch_rule_ids: vec!["SD_a".into()],
        trunk_ids: vec!["ST_a".into()],
        ..Default::default()
    };
    let mut rule = SipDispatchRuleInfo {
        sip_dispatch_rule_id: "SD_a".into(),
        ..Default::default()
    };
    assert!(rule_filter(&req, &rule));
    rule.trunk_ids = vec!["ST_b".into()];
    assert!(!rule_filter(&req, &rule));
    rule.trunk_ids = vec!["ST_a".into()];
    rule.sip_dispatch_rule_id = "SD_b".into();
    assert!(!rule_filter(&req, &rule));
}
#[test]
fn page_filter_keeps_nil_id_but_excludes_cursor_and_truncate_zero_is_unlimited() {
    let page = Pagination {
        after_id: "b".into(),
        limit: 0,
    };
    assert!(after(Some(&page), ""));
    assert!(!after(Some(&page), "a"));
    assert!(!after(Some(&page), "b"));
    assert!(after(Some(&page), "c"));
    let mut v = vec![1, 2, 3];
    truncate(&mut v, Some(&page));
    assert_eq!(v.len(), 3);
    truncate(
        &mut v,
        Some(&Pagination {
            limit: 1,
            ..Default::default()
        }),
    );
    assert_eq!(v, [1]);
}

// Explicit external-service test, not silently skipped inside a passing test.
// Coordinator runs it only against a task-owned Redis URL and records actual result.
#[tokio::test]
#[ignore = "requires task-owned LIVEKIT_SIP_CATALOG_REDIS_URL"]
async fn actual_redis_preserves_catalog_lifetime_fallback_pages_and_delete() {
    let url = std::env::var("LIVEKIT_SIP_CATALOG_REDIS_URL").expect("explicit isolated Redis URL");
    let store = SipCatalog::connect(&url, Duration::from_secs(3), 8)
        .await
        .unwrap();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let prefix = format!("ST_test_{nonce}_");
    let legacy = SipTrunkInfo {
        sip_trunk_id: format!("{prefix}a"),
        kind: 0,
        outbound_number: "+123456".into(),
        ..Default::default()
    };
    store.store_legacy(&legacy).await.unwrap();
    let modern = SipInboundTrunkInfo {
        sip_trunk_id: legacy.sip_trunk_id.clone(),
        name: "modern-wins".into(),
        numbers: vec!["123456".into()],
        ..Default::default()
    };
    store.store_inbound(&modern).await.unwrap();
    assert_eq!(
        store
            .load_inbound(&legacy.sip_trunk_id)
            .await
            .unwrap()
            .unwrap(),
        modern
    );
    assert_eq!(
        store.load_legacy(&legacy.sip_trunk_id).await.unwrap(),
        legacy
    );
    let second = SipOutboundTrunkInfo {
        sip_trunk_id: format!("{prefix}b"),
        name: "survives-new-client".into(),
        ..Default::default()
    };
    store.store_outbound(&second).await.unwrap();
    let reconnect = SipCatalog::connect(&url, Duration::from_secs(3), 8)
        .await
        .unwrap();
    assert_eq!(
        reconnect
            .load_outbound(&second.sip_trunk_id)
            .await
            .unwrap()
            .unwrap(),
        second
    );
    let page = store
        .list_inbound(&ListSipInboundTrunkRequest {
            trunk_ids: vec![legacy.sip_trunk_id.clone()],
            page: Some(Pagination {
                after_id: prefix.clone(),
                limit: 0,
            }),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(page.len(), 2); // Same ID modern+legacy are both kept, no invented dedup.
    assert!(
        page.iter()
            .all(|v| v.as_ref().unwrap().sip_trunk_id == legacy.sip_trunk_id)
    );
    let opposite = SipTrunkInfo {
        sip_trunk_id: format!("{prefix}c"),
        kind: 2,
        ..Default::default()
    };
    store.store_legacy(&opposite).await.unwrap();
    assert_eq!(
        store.load_inbound(&opposite.sip_trunk_id).await.unwrap(),
        None
    );
    let rule = SipDispatchRuleInfo {
        sip_dispatch_rule_id: format!("SD_test_{nonce}"),
        name: "actual-storage".into(),
        ..Default::default()
    };
    store.store_rule(&rule).await.unwrap();
    assert_eq!(
        store.load_rule(&rule.sip_dispatch_rule_id).await.unwrap(),
        rule
    );
    store.delete_trunk(&legacy.sip_trunk_id).await.unwrap();
    store.delete_trunk(&legacy.sip_trunk_id).await.unwrap();
    assert!(matches!(
        store.load_legacy(&legacy.sip_trunk_id).await,
        Err(CatalogError::TrunkNotFound)
    ));
    assert_eq!(
        store.load_rule(&rule.sip_dispatch_rule_id).await.unwrap(),
        rule
    ); // Trunk delete never cascades rules.
    store.delete_rule(&rule.sip_dispatch_rule_id).await.unwrap();
    store.delete_rule(&rule.sip_dispatch_rule_id).await.unwrap();
    assert!(matches!(
        store.load_rule(&rule.sip_dispatch_rule_id).await,
        Err(CatalogError::RuleNotFound)
    ));
    store.delete_trunk(&second.sip_trunk_id).await.unwrap();
    store.delete_trunk(&opposite.sip_trunk_id).await.unwrap();
}

#[tokio::test]
#[ignore = "requires empty task-owned LIVEKIT_SIP_CATALOG_EDGE_REDIS_URL database"]
async fn actual_redis_page_cap_filter_hole_corruption_and_wrongtype() {
    let url =
        std::env::var("LIVEKIT_SIP_CATALOG_EDGE_REDIS_URL").expect("dedicated EMPTY Redis DB");
    let store = SipCatalog::connect(&url, Duration::from_secs(3), 8)
        .await
        .unwrap();
    for key in [LEGACY, INBOUND, OUTBOUND, RULE] {
        let exists: bool = store.query(redis::cmd("EXISTS").arg(key)).await.unwrap();
        assert!(!exists, "edge test requires isolated empty database");
    }
    let mut pipeline = redis::pipe();
    for i in 0..1005 {
        let value = SipInboundTrunkInfo {
            sip_trunk_id: format!("ST_{i:04}"),
            numbers: vec![if i == 1004 { "+2" } else { "+1" }.into()],
            ..Default::default()
        };
        pipeline
            .cmd("HSET")
            .arg(INBOUND)
            .arg(&value.sip_trunk_id)
            .arg(value.encode_to_vec())
            .ignore();
    }
    let _: () = pipeline
        .query_async(&mut store.connection.begin())
        .await
        .unwrap();
    let page = store
        .list_inbound(&ListSipInboundTrunkRequest {
            page: Some(Pagination::default()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(page.len(), 1000);
    assert_eq!(
        page.last().unwrap().as_ref().unwrap().sip_trunk_id,
        "ST_0999"
    );
    // Actual Store semantics fetch IDs first and then filter. An empty filtered
    // first page is not rescued by scanning every record for a later match.
    let hole = store
        .list_inbound(&ListSipInboundTrunkRequest {
            numbers: vec!["+2".into()],
            page: Some(Pagination::default()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(hole.is_empty());
    let tail = store
        .list_inbound(&ListSipInboundTrunkRequest {
            numbers: vec!["+2".into()],
            page: Some(Pagination {
                after_id: "ST_0999".into(),
                limit: 0,
            }),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].as_ref().unwrap().sip_trunk_id, "ST_1004");
    let missing_cursor = store
        .list_inbound(&ListSipInboundTrunkRequest {
            page: Some(Pagination {
                after_id: "ST_1000x".into(),
                limit: 1,
            }),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(missing_cursor[0].as_ref().unwrap().sip_trunk_id, "ST_1001");
    let _: usize = store
        .query(
            redis::cmd("HSET")
                .arg(INBOUND)
                .arg("ST_bad")
                .arg(vec![0x80u8]),
        )
        .await
        .unwrap();
    assert!(matches!(
        store.load_inbound("ST_bad").await,
        Err(CatalogError::Decode(_))
    ));
    let _: usize = store
        .query(redis::cmd("HDEL").arg(INBOUND).arg("ST_bad"))
        .await
        .unwrap();
    let _: () = store
        .query(redis::cmd("SET").arg(RULE).arg("wrong-type-fixture"))
        .await
        .unwrap();
    assert!(matches!(
        store.load_rule("SD_missing").await,
        Err(CatalogError::Redis(_))
    ));
    assert!(matches!(
        store
            .store_rule(&SipDispatchRuleInfo {
                sip_dispatch_rule_id: "SD_fixture".into(),
                ..Default::default()
            })
            .await,
        Err(CatalogError::Redis(_))
    ));
    let _: usize = store
        .query(redis::cmd("DEL").arg(INBOUND).arg(RULE))
        .await
        .unwrap();
}

#[test]
fn pinned_go_sort_exact_equal_key_and_nil_group_permutations() {
    #[derive(Clone, serde::Deserialize)]
    struct Item {
        key: String,
        ordinal: usize,
    }
    #[derive(serde::Deserialize)]
    struct Fixture {
        name: String,
        input: Vec<Item>,
        expected_ordinals: Vec<usize>,
    }
    #[derive(serde::Deserialize)]
    struct Fixtures {
        go_version: String,
        fixtures: Vec<Fixture>,
    }
    let fixtures: Fixtures = serde_json::from_str(include_str!("go-sort-fixtures.json")).unwrap();
    assert_eq!(fixtures.go_version, "go1.26.0");
    assert_eq!(fixtures.fixtures.len(), 57);
    let mut observed_unstable = false;
    for fixture in fixtures.fixtures {
        let mut stable = fixture.input.clone();
        stable.sort_by(|a, b| a.key.cmp(&b.key));
        observed_unstable |=
            stable.iter().map(|v| v.ordinal).collect::<Vec<_>>() != fixture.expected_ordinals;
        let mut actual = fixture.input;
        go_sort::sort_by(&mut actual, |a, b| a.key.cmp(&b.key));
        assert_eq!(
            actual.iter().map(|v| v.ordinal).collect::<Vec<_>>(),
            fixture.expected_ordinals,
            "{}",
            fixture.name
        );
    }
    assert!(
        observed_unstable,
        "fixture must actually distinguish stable sort from Go PDQsort"
    );
}

#[test]
fn public_store_error_preserves_actual_redis_code_and_detail() {
    let error = CatalogError::Redis(redis::make_extension_error(
        "WRONGTYPE".into(),
        Some("Operation against a key holding the wrong kind of value".into()),
    ));
    assert_eq!(
        error.compatible_public_message(),
        "WRONGTYPE Operation against a key holding the wrong kind of value"
    );
    let other = CatalogError::Redis(redis::make_extension_error(
        "CUSTOM_TEST".into(),
        Some("specific fixture detail".into()),
    ));
    assert_eq!(
        other.compatible_public_message(),
        "CUSTOM_TEST specific fixture detail"
    );
    let no_detail = CatalogError::Redis(redis::make_extension_error("CUSTOM_TEST".into(), None));
    assert_eq!(no_detail.compatible_public_message(), "CUSTOM_TEST");
    let source = SipInboundTrunkInfo::decode([0xffu8].as_slice()).unwrap_err();
    assert_eq!(
        CatalogError::Decode(source).compatible_public_message(),
        "proto: cannot parse invalid wire-format data"
    );
}

#[test]
fn schema_bound_utf8_failure_has_real_utf8_source_and_order() {
    let utf8 = decode_stored::<SipInboundTrunkInfo>(&[0x0a, 1, 0xff]).unwrap_err();
    assert!(matches!(utf8, CatalogError::DecodeUtf8(_)));
    assert_eq!(
        utf8.compatible_public_message(),
        "string field contains invalid UTF-8"
    );
    let earlier = decode_stored::<SipInboundTrunkInfo>(&[0, 0x0a, 1, 0xff]).unwrap_err();
    assert!(matches!(earlier, CatalogError::Decode(_)));
    assert_eq!(
        earlier.compatible_public_message(),
        "proto: cannot parse invalid wire-format data"
    );
}

#[test]
fn first_store_decode_accepts_go_known_number_wrong_wire_without_fabricating_known_values() {
    let input = [0x10, 7, 0xa2, 1, 2, 0x10, 7];
    let actual: SipInboundTrunkInfo = decode_stored(&input).unwrap();
    assert!(actual.name.is_empty());
    assert!(actual.media.unwrap().codecs.is_empty());
}
