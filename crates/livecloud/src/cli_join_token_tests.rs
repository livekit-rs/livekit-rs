//! Real command configuration and token consumer; no parser-only success claim.
use super::*;
use crate::COMPAT_VERSION;
use lk_config::{EntryAction, Env, ProgramInfo};

fn command_config(body: &str) -> Box<lk_config::CommandConfig> {
    let action = lk_config::parse(
        vec![
            "livecloud",
            "--node-ip",
            "127.0.0.1",
            "--config-body",
            body,
            "create-join-token",
            "--room",
            "owned-cli-room",
            "--identity",
            "owned-cli-person",
        ]
        .into_iter()
        .map(Into::into)
        .collect(),
        &Env::default(),
        &ProgramInfo {
            binary_name: "livecloud".into(),
            product_version: "test".into(),
            compat_version: COMPAT_VERSION,
        },
    )
    .unwrap();
    match action {
        EntryAction::CreateJoinToken {
            config,
            room,
            identity,
            recorder,
        } => {
            assert_eq!(room, "owned-cli-room");
            assert_eq!(identity, "owned-cli-person");
            assert!(!recorder);
            config
        }
        _ => panic!("real create-join-token configuration consumer required"),
    }
}

#[test]
fn create_join_token_rejects_empty_selected_key_or_secret_with_source_error() {
    for body in [
        r#"keys: {"": "owned-cli-not-production-secret"}"#,
        r#"keys: {"APIownedcli": ""}"#,
        r#"keys: {"": ""}"#,
    ] {
        let cfg = command_config(body);
        assert_eq!(
            create_join_token(&cfg, "owned-cli-room", "owned-cli-person", false).unwrap_err(),
            "missing API key or secret key"
        );
    }
}

#[test]
fn create_join_token_valid_selected_pair_keeps_real_signed_recorder_claims() {
    let cfg = command_config(r#"keys: {"APIownedcli": "owned-cli-not-production-secret"}"#);
    for recorder in [false, true] {
        let token =
            create_join_token(&cfg, "owned-cli-room", "owned-cli-person", recorder).unwrap();
        let verified = lk_compat::auth::verify_token(
            &KeyStore::new(cfg.official.keys.0.clone()),
            &token,
            rtc_core::util::unix_s() as f64,
        )
        .unwrap();
        assert_eq!(verified.grants.api_key, "APIownedcli");
        assert_eq!(verified.grants.identity, "owned-cli-person");
        assert!(verified.grants.video.room_join);
        assert_eq!(verified.grants.video.room, "owned-cli-room");
        assert_eq!(verified.grants.video.recorder, recorder);
        assert_eq!(verified.grants.video.hidden, recorder);
        assert_eq!(verified.grants.video.can_publish, recorder.then_some(false));
        assert_eq!(
            verified.grants.video.can_publish_data,
            recorder.then_some(false)
        );
    }
}

#[test]
fn public_token_command_runtime_irrelevant_http_port_still_mints_verified_recorder_token() {
    let action = lk_config::parse(
        [
            "livecloud",
            "--node-ip",
            "127.0.0.1",
            "--config-body",
            "port: 70000\nkeys: {APIownedcli: owned-cli-not-production-secret}\n",
            "create-join-token",
            "--room",
            "owned-cli-room",
            "--identity",
            "owned-cli-person",
            "--recorder",
        ]
        .into_iter()
        .map(Into::into)
        .collect(),
        &Env::default(),
        &ProgramInfo {
            binary_name: "livecloud".into(),
            product_version: "test".into(),
            compat_version: COMPAT_VERSION,
        },
    );
    assert!(
        action.is_ok(),
        "actual token command cannot require an HTTP listener: {action:?}"
    );
    let EntryAction::CreateJoinToken {
        config,
        room,
        identity,
        recorder,
    } = action.unwrap()
    else {
        panic!("actual token action")
    };
    let token = create_join_token(&config, &room, &identity, recorder).unwrap();
    let verified = lk_compat::auth::verify_token(
        &KeyStore::new(vec![(
            "APIownedcli".into(),
            "owned-cli-not-production-secret".into(),
        )]),
        &token,
        rtc_core::util::unix_s() as f64,
    )
    .unwrap();
    assert_eq!(verified.grants.api_key, "APIownedcli");
    assert_eq!(verified.grants.identity, "owned-cli-person");
    assert!(verified.grants.video.room_join);
    assert_eq!(verified.grants.video.room, "owned-cli-room");
    assert!(verified.grants.video.hidden && verified.grants.video.recorder);
    assert_eq!(verified.grants.video.can_publish, Some(false));
    assert_eq!(verified.grants.video.can_publish_data, Some(false));
}
