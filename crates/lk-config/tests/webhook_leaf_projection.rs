//! C98-D config_projection legs for the two `webhook` group parents whose leaves
//! reach a runtime carrier: `YAML:webhook.filter_params` and
//! `YAML:webhook.resource_url_notifier`.
//!
//! Tier rule (SPEC-0075 §2, same harness as `rtc_residual_leaf_projection.rs`):
//! drive `parse_with_probe(argv, env, ProgramInfo, NoNetwork)` and assert on the
//! projected `rtc_core::config::WebhookConfig` (`loaded.server.webhook`), which is
//! the structure the notifier is built from at
//! `crates/livecloud/src/server.rs:488-495` ->
//! `lk_compat::webhook::WebhookNotifier::new` (`webhook/mod.rs:151-206`). The raw
//! official tree is only consulted to prove where a group default came from.
//!
//! A *group* row must claim something a child row cannot: the container rules —
//! absent group vs. empty map vs. one-leaf map, an unrecognised child key, a
//! group-level flag, a group-level env name. Go oracle:
//! references/protocol-stable @ webhook/notifier.go:37-41 (`DefaultWebHookConfig`),
//! :194-197 (`FilterParams`), resource_url_notifier.go:48-56
//! (`ResourceURLNotifierConfig`, `DefaultResourceURLNotifierConfig` 30s/200),
//! references/livekit-stable pkg/config/config.go:663 (`WebHook:
//! webhook.DefaultWebHookConfig`).
use lk_config::{EntryAction, Env, ProgramInfo, netdetect::NetProbe, parse_with_probe};
use std::{ffi::OsString, net::IpAddr, time::Duration};

struct NoNetwork;
impl NetProbe for NoNetwork {
    fn local_addresses(&self) -> Vec<IpAddr> {
        vec!["127.0.0.1".parse().expect("fixture loopback")]
    }
    fn external_ip(&self, _: &[String], _: bool) -> Result<IpAddr, String> {
        panic!("fixture must not discover external IP")
    }
    fn sleep(&self, _: Duration) {
        panic!("fixture must not retry networking")
    }
}

fn parse_ex(args: &[&str], env: &Env) -> Result<Box<lk_config::LoadedConfig>, String> {
    let info = ProgramInfo {
        binary_name: "livecloud".into(),
        product_version: "fixture".into(),
        compat_version: "1.13.10",
    };
    let mut argv: Vec<OsString> = ["livecloud", "--dev"]
        .into_iter()
        .map(OsString::from)
        .collect();
    argv.extend(args.iter().map(OsString::from));
    match parse_with_probe(argv, env, &info, &NoNetwork) {
        Ok(EntryAction::RunServer(loaded)) => Ok(loaded),
        Ok(other) => Err(format!("entry action {other:?}")),
        Err(e) => Err(format!("parse error: {e}")),
    }
}

fn webhook(args: &[&str], env: &Env) -> rtc_core::config::WebhookConfig {
    match parse_ex(args, env) {
        Ok(loaded) => loaded.server.webhook.clone(),
        Err(e) => panic!("expected a server config, got {e}"),
    }
}

fn load_error(args: &[&str], env: &Env) -> String {
    match parse_ex(args, env) {
        Err(e) => e,
        Ok(_) => panic!("expected the load to refuse {args:?}, got a server config"),
    }
}

fn yaml(body: &'static str) -> [&'static str; 2] {
    ["--config-body", body]
}

/// `YAML:webhook.filter_params` — the container, not its two lists.
#[test]
fn webhook_filter_params_group_keeps_defaults_when_absent_empty_or_partial() {
    // Absent group: Go's `FilterParams{}` (notifier.go:40) is two empty lists, and
    // the carrier must show them as empty rather than as "everything excluded".
    let absent = webhook(&[], &Env::default());
    assert!(
        absent.include_events.is_empty() && absent.exclude_events.is_empty(),
        "C98D-01 an absent filter_params projects two empty lists, got {:?}",
        (absent.include_events.clone(), absent.exclude_events.clone())
    );

    // Group present as an empty mapping. Go unmarshals onto the defaults
    // (config.go:670-677), so an empty map assigns nothing and both lists stay
    // empty -- the group node must not be read as "null out the struct".
    let empty = webhook(&yaml("webhook:\n  filter_params: {}\n"), &Env::default());
    assert!(
        empty.include_events.is_empty() && empty.exclude_events.is_empty(),
        "C98D-02 filter_params: {{}} must not invent events, got {:?}",
        (empty.include_events.clone(), empty.exclude_events.clone())
    );

    // One leaf set, the sibling keeps its container default. This is the rule a
    // child row cannot state: it holds only while both leaves live in one struct.
    let partial = webhook(
        &yaml("webhook:\n  filter_params:\n    include_events: [room_started]\n"),
        &Env::default(),
    );
    assert_eq!(
        vec!["room_started".to_string()],
        partial.include_events,
        "C98D-03 the set leaf must project"
    );
    assert!(
        partial.exclude_events.is_empty(),
        "C98D-03 the untouched sibling keeps the empty-list default, got {:?}",
        partial.exclude_events
    );

    // The container's CLI surface is the real group-level rule: both leaves are
    // `[]string`, and Go's flag generator `continue`s on `reflect.Slice`
    // (references/livekit-stable pkg/config/config.go:1033-1035), so the group has
    // no flag for its lists at all -- only YAML can reach them. The fixture flag
    // table confirms it: `webhook` contributes `api_key`, the two notifier ints
    // and the duration, and nothing else (tests/fixtures/cli-verbose-generated-
    // v1.13.10.txt:274-278).
    for slice_flag in [
        "--webhook.filter_params.include_events=participant_joined",
        "--webhook.filter_params.exclude_events=room_finished",
        "--webhook.urls=https://example.invalid/hook",
    ] {
        let text = load_error(&[slice_flag], &Env::default());
        assert!(
            text.contains("not defined"),
            "C98D-03b {slice_flag} must have no flag (config.go:1033-1035 skips slices), got {text}"
        );
    }
    // A string leaf in the same parent does have a flag, so the refusal above is
    // the slice rule and not "webhook has no flags".
    // One parent feeds the carrier three separate lists (`urls` and the two
    // filter lists), so a leg has to pin that filling one leaves the others alone.
    let with_urls = webhook(
        &yaml(
            "webhook:\n  urls: [https://example.invalid/hook]\n  filter_params:\n    include_events: [room_started]\n",
        ),
        &Env::default(),
    );
    assert_eq!(
        vec!["https://example.invalid/hook".to_string()],
        with_urls.urls,
        "C98D-03d the url list must project on its own"
    );
    assert_eq!(
        vec!["room_started".to_string()],
        with_urls.include_events,
        "C98D-03e the inclusion list must project on its own"
    );
    assert!(
        with_urls.exclude_events.is_empty(),
        "C98D-03f neither url nor inclusion must leak into the exclusion list, got {:?}",
        with_urls.exclude_events
    );

    let keyed = webhook(&["--webhook.api_key=c98key"], &Env::default());
    assert_eq!(
        "c98key", keyed.api_key,
        "C98D-03c the sibling string leaf keeps a flag while the slices do not"
    );
    assert!(
        keyed.include_events.is_empty() && keyed.urls.is_empty(),
        "C98D-03c an unrelated flag must not populate the slice container, got {:?}",
        (keyed.include_events.clone(), keyed.urls.clone())
    );
}

/// `YAML:webhook.resource_url_notifier` — 30s/200 come from the group default,
/// not from a per-leaf rewrite.
#[test]
fn webhook_resource_url_notifier_group_seeds_30s_and_200_and_partial_keeps_them() {
    let absent = webhook(&[], &Env::default());
    assert_eq!(
        30_000_000_000,
        absent.max_age.as_nanos_i64(),
        "C98D-04 DefaultResourceURLNotifierConfig.MaxAge is 30s (resource_url_notifier.go:53-56)"
    );
    assert_eq!(
        200, absent.max_depth,
        "C98D-04 DefaultResourceURLNotifierConfig.MaxDepth is 200 (resource_url_notifier.go:53-56)"
    );

    let empty = webhook(
        &yaml("webhook:\n  resource_url_notifier: {}\n"),
        &Env::default(),
    );
    assert_eq!(
        (absent.max_age.as_nanos_i64(), absent.max_depth),
        (empty.max_age.as_nanos_i64(), empty.max_depth),
        "C98D-05 an empty group node keeps the 30s/200 defaults, got {}/{}",
        empty.max_age.as_nanos_i64(),
        empty.max_depth
    );

    // One leaf set: the sibling keeps its group default. Only max_depth is sent
    // downstream as an override here, so the pair must not collapse to zero.
    let partial = webhook(
        &yaml("webhook:\n  resource_url_notifier:\n    max_depth: 5\n"),
        &Env::default(),
    );
    assert_eq!(
        30_000_000_000,
        partial.max_age.as_nanos_i64(),
        "C98D-06 setting max_depth must not reset max_age"
    );
    assert_eq!(5, partial.max_depth, "C98D-06 the set leaf wins");

    // Zero spelling: the group default is *replaced* by an explicit zero, and the
    // consumer -- not the loader -- is what turns 0 back into 30s/200. The carrier
    // therefore has to show the zero, otherwise the consumer rule is untestable.
    let zeros = webhook(
        &yaml("webhook:\n  resource_url_notifier:\n    max_age: 0s\n    max_depth: 0\n"),
        &Env::default(),
    );
    assert_eq!(
        0,
        zeros.max_age.as_nanos_i64(),
        "C98D-07 an explicit 0s max_age must reach the carrier so webhook/mod.rs:191-196 can apply the default"
    );
    assert_eq!(
        0, zeros.max_depth,
        "C98D-07 an explicit 0 max_depth must reach the carrier"
    );
}

/// Both groups are containers: a child key Go does not declare must fail the
/// load, and neither group name is itself a flag or an env leaf.
#[test]
fn webhook_group_nodes_reject_unknown_children_and_group_level_surfaces() {
    let unknown = load_error(
        &yaml("webhook:\n  filter_params:\n    include_event: [room_started]\n"),
        &Env::default(),
    );
    assert!(
        unknown.contains("include_event"),
        "C98D-08 a near-miss child key must fail the load naming the key, got {unknown}"
    );
    let unknown_resource = load_error(
        &yaml("webhook:\n  resource_url_notifier:\n    maxages: 5s\n"),
        &Env::default(),
    );
    assert!(
        unknown_resource.contains("maxages"),
        "C98D-08b the resource group must reject an unknown child the same way, got {unknown_resource}"
    );

    // Go registers flags for the leaves only, so the group spelling is not a flag:
    // `load_error` itself is the claim -- if the loader accepted the group node it
    // would return a config and this leg aborts instead of passing quietly.
    load_error(&["--webhook.filter_params=room_started"], &Env::default());
    load_error(&["--webhook=room_started"], &Env::default());

    // The group env names are inert: `LIVEKIT_WEBHOOK_FILTER_PARAMS` binds to
    // nothing, so the container keeps its defaults instead of half-parsing.
    let env = Env::from_pairs([("LIVEKIT_WEBHOOK_FILTER_PARAMS", "room_started")]);
    let inert = webhook(&[], &env);
    assert!(
        inert.include_events.is_empty() && inert.exclude_events.is_empty(),
        "C98D-10 a group-level env name must not fill either list, got {:?}",
        (inert.include_events.clone(), inert.exclude_events.clone())
    );
}
