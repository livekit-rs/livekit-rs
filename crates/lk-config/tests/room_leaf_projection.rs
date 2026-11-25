//! C96-room config_projection legs for the `room.*` leaf family.
//! Every leg drives YAML / ENV / CLI through the public lk-config loading API
//! (parse_with_probe) and asserts the value that lands in the projected
//! ServerConfig the room service consumes, not merely the raw parsed tree.
//!
//! Production consumers of the projected fields:
//! - rtc-core/src/config.rs:95-100 (RoomPolicy::from copies enabled_codecs,
//!   max_participants, empty_timeout, departure_timeout, playout_delay).
//! - rtc-core/src/room/mod.rs:181 (empty_timeout.as_secs()).
//! - lk-compat/src/room_control/create_policy.rs:16-21 reads
//!   room.create_room_attempts / create_room_timeout, called from
//!   lk-compat/src/room_control/mod.rs:578 and room_create.rs:49.
//! - lk-compat/src/room_control/catalog.rs:746-761 (global room defaults) and
//!   :770-786 + rtc-core/src/permission.rs:162-178 (named preset inheritance).
use lk_config::{EntryAction, Env, GoDuration, ProgramInfo, netdetect::NetProbe, parse_with_probe};
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

fn parse(args: &[&str], env: &Env) -> Box<lk_config::LoadedConfig> {
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
    let result = match parse_with_probe(argv, env, &info, &NoNetwork) {
        Ok(r) => r,
        Err(e) => panic!("parse_with_probe error: {}", e),
    };
    match result {
        EntryAction::RunServer(config) => config,
        other => panic!("expected RunServer, got other entry action: {other:?}"),
    }
}

/// Preset lookup helper: a named preset must exist and be non-nil.
fn preset<'a>(cfg: &'a lk_config::LoadedConfig, key: &str) -> &'a rtc_core::permission::RoomConfig {
    cfg.server
        .room
        .presets
        .get(key)
        .unwrap_or_else(|| panic!("preset {key} missing from projection"))
        .as_ref()
        .unwrap_or_else(|| panic!("preset {key} projected as nil"))
}

// ---------------------------------------------------------------------------
// YAML:room.empty_timeout (normalize.rs:662 `empty_timeout: secs(...)`)
// ---------------------------------------------------------------------------

#[test]
fn room_empty_timeout_yaml_env_cli_projection_and_units() {
    // Go default: pkg/config/config.go:614 EmptyTimeout: 5 * 60 (seconds).
    let base = parse(&[], &Env::default());
    assert_eq!(base.official.room.empty_timeout, 300);
    assert_eq!(base.server.room.empty_timeout, Duration::from_secs(300));

    // Explicit YAML 45 must project as 45 SECONDS, not 45 ms or 45 min.
    let cfg = parse(
        &["--config-body", "room:\n  empty_timeout: 45\n"],
        &Env::default(),
    );
    assert_eq!(cfg.server.room.empty_timeout, Duration::from_secs(45));
    assert_ne!(
        cfg.server.room.empty_timeout, base.server.room.empty_timeout,
        "the projected duration must move with the YAML value"
    );

    // ENV override: generated env name LIVEKIT_ROOM_EMPTY_TIMEOUT.
    let env = Env::from_pairs([("LIVEKIT_ROOM_EMPTY_TIMEOUT", "90")]);
    let cfg = parse(&[], &env);
    assert_eq!(cfg.server.room.empty_timeout, Duration::from_secs(90));

    // CLI beats ENV in both directions.
    let cfg = parse(&["--room.empty_timeout=7"], &env);
    assert_eq!(cfg.server.room.empty_timeout, Duration::from_secs(7));
    let env_off = Env::from_pairs([("LIVEKIT_ROOM_EMPTY_TIMEOUT", "0")]);
    let cfg = parse(&["--room.empty_timeout=11"], &env_off);
    assert_eq!(cfg.server.room.empty_timeout, Duration::from_secs(11));

    // Boundary: explicit 0 stays 0 (Go treats 0 as "no timeout" downstream,
    // pkg/service/roomallocator.go:95 only overrides a request value > 0).
    let cfg = parse(
        &["--config-body", "room:\n  empty_timeout: 0\n"],
        &Env::default(),
    );
    assert_eq!(cfg.official.room.empty_timeout, 0);
    assert_eq!(cfg.server.room.empty_timeout, Duration::ZERO);

    // Large boundary survives the u32 -> u64 widening without truncation.
    let cfg = parse(&["--room.empty_timeout=4294967295"], &Env::default());
    assert_eq!(
        cfg.server.room.empty_timeout,
        Duration::from_secs(4_294_967_295)
    );
}

// ---------------------------------------------------------------------------
// YAML:room.departure_timeout (normalize.rs:663)
// ---------------------------------------------------------------------------

#[test]
fn room_departure_timeout_yaml_env_cli_projection_and_units() {
    // Go default: pkg/config/config.go:615 DepartureTimeout: 20 (seconds).
    let base = parse(&[], &Env::default());
    assert_eq!(base.official.room.departure_timeout, 20);
    assert_eq!(base.server.room.departure_timeout, Duration::from_secs(20));

    let cfg = parse(
        &["--config-body", "room:\n  departure_timeout: 60\n"],
        &Env::default(),
    );
    assert_eq!(cfg.server.room.departure_timeout, Duration::from_secs(60));
    assert_ne!(
        cfg.server.room.departure_timeout,
        base.server.room.departure_timeout
    );

    let env = Env::from_pairs([("LIVEKIT_ROOM_DEPARTURE_TIMEOUT", "33")]);
    let cfg = parse(&[], &env);
    assert_eq!(cfg.server.room.departure_timeout, Duration::from_secs(33));
    let cfg = parse(&["--room.departure_timeout=3"], &env);
    assert_eq!(cfg.server.room.departure_timeout, Duration::from_secs(3));

    // Boundary: an explicit 0 is distinct from an absent key only in that it
    // also projects to zero; the unit must stay seconds either way.
    let cfg = parse(&["--room.departure_timeout=0"], &Env::default());
    assert_eq!(cfg.server.room.departure_timeout, Duration::ZERO);
}

// ---------------------------------------------------------------------------
// YAML:room.max_participants (normalize.rs:661)
// ---------------------------------------------------------------------------

#[test]
fn room_max_participants_yaml_env_cli_projection() {
    // Go default: RoomLimits has no MaxParticipants entry, so the zero value
    // (unlimited) applies; pkg/types/room.go:299-301 only enforces > 0.
    let base = parse(&[], &Env::default());
    assert_eq!(base.official.room.max_participants, 0);
    assert_eq!(base.server.room.max_participants, 0);

    let cfg = parse(
        &["--config-body", "room:\n  max_participants: 7\n"],
        &Env::default(),
    );
    assert_eq!(cfg.server.room.max_participants, 7);
    assert_ne!(
        cfg.server.room.max_participants,
        base.server.room.max_participants
    );

    let env = Env::from_pairs([("LIVEKIT_ROOM_MAX_PARTICIPANTS", "12")]);
    let cfg = parse(&[], &env);
    assert_eq!(cfg.server.room.max_participants, 12);
    let cfg = parse(&["--room.max_participants=4"], &env);
    assert_eq!(cfg.server.room.max_participants, 4);

    // Boundary: u32::MAX projects unchanged (no clamping to i32).
    let cfg = parse(&["--room.max_participants=4294967295"], &Env::default());
    assert_eq!(cfg.server.room.max_participants, u32::MAX);
}

// ---------------------------------------------------------------------------
// YAML:room.enabled_codecs + .mime + .fmtp_line (normalize.rs:653-660)
// ---------------------------------------------------------------------------

#[test]
fn room_enabled_codecs_yaml_env_cli_projection_of_mime_and_fmtp() {
    // Go default: pkg/config/config.go:602-613 lists 10 pion mime specs with
    // empty FmtpLine; the projection preserves order and the empty fmtp line.
    let base = parse(&[], &Env::default());
    assert_eq!(base.server.room.enabled_codecs.len(), 10);
    assert_eq!(base.server.room.enabled_codecs[0].mime, "audio/PCMU");
    assert_eq!(base.server.room.enabled_codecs[3].mime, "audio/red");
    assert_eq!(base.server.room.enabled_codecs[9].mime, "video/rtx");
    assert!(
        base.server
            .room
            .enabled_codecs
            .iter()
            .all(|c| c.fmtp_line.is_empty()),
        "Go default CodecSpec entries carry no fmtp_line"
    );

    // Explicit list replaces the default wholesale, carrying both leaves.
    let yaml = concat!(
        "room:\n",
        "  enabled_codecs:\n",
        "    - mime: audio/opus\n",
        "      fmtp_line: minptime=10;useinbandfec=1\n",
        "    - mime: video/H264\n",
    );
    let cfg = parse(&["--config-body", yaml], &Env::default());
    assert_eq!(cfg.server.room.enabled_codecs.len(), 2);
    assert_eq!(cfg.server.room.enabled_codecs[0].mime, "audio/opus");
    assert_eq!(
        cfg.server.room.enabled_codecs[0].fmtp_line,
        "minptime=10;useinbandfec=1"
    );
    assert_eq!(cfg.server.room.enabled_codecs[1].mime, "video/H264");
    assert_eq!(cfg.server.room.enabled_codecs[1].fmtp_line, "");

    // Boundary: an empty list is NOT the same as an absent key (absent keeps
    // the 10-codec default); the projection must be empty, not defaulted.
    let cfg = parse(
        &["--config-body", "room:\n  enabled_codecs: []\n"],
        &Env::default(),
    );
    assert!(cfg.official.room.enabled_codecs.is_empty());
    assert!(cfg.server.room.enabled_codecs.is_empty());

    // A codec entry with no mime projects the empty string, not a dropped row.
    let cfg = parse(
        &[
            "--config-body",
            "room:\n  enabled_codecs:\n    - fmtp_line: a=1\n",
        ],
        &Env::default(),
    );
    assert_eq!(cfg.server.room.enabled_codecs.len(), 1);
    assert_eq!(cfg.server.room.enabled_codecs[0].mime, "");
    assert_eq!(cfg.server.room.enabled_codecs[0].fmtp_line, "a=1");
}

// ---------------------------------------------------------------------------
// YAML:room.playout_delay.min / .max (normalize.rs:665-669)
// ---------------------------------------------------------------------------

#[test]
fn room_playout_delay_min_max_yaml_env_cli_projection_and_gating() {
    // Go default: config.PlayoutDelayConfig{Enabled:false,Min:0,Max:0}
    // (pkg/config/config.go:250-254, no DefaultConfig entry) projects to None.
    let base = parse(&[], &Env::default());
    assert_eq!(base.server.room.playout_delay, None);

    // Explicit pair on an enabled gate projects both values as milliseconds.
    let cfg = parse(
        &[
            "--config-body",
            "room:\n  playout_delay:\n    enabled: true\n    min: 120\n    max: 900\n",
        ],
        &Env::default(),
    );
    assert_eq!(cfg.server.room.playout_delay, Some((120, 900)));
    assert_ne!(
        cfg.server.room.playout_delay,
        base.server.room.playout_delay
    );

    // ENV surface: LIVEKIT_ROOM_PLAYOUT_DELAY_{ENABLED,MIN,MAX}.
    let env = Env::from_pairs([
        ("LIVEKIT_ROOM_PLAYOUT_DELAY_ENABLED", "true"),
        ("LIVEKIT_ROOM_PLAYOUT_DELAY_MIN", "50"),
        ("LIVEKIT_ROOM_PLAYOUT_DELAY_MAX", "150"),
    ]);
    let cfg = parse(&[], &env);
    assert_eq!(cfg.server.room.playout_delay, Some((50, 150)));

    // CLI beats ENV, including flipping the gate off.
    let cfg = parse(
        &[
            "--room.playout_delay.enabled=false",
            "--room.playout_delay.min=999",
        ],
        &env,
    );
    assert_eq!(cfg.server.room.playout_delay, None);
    let cfg = parse(
        &[
            "--room.playout_delay.enabled=true",
            "--room.playout_delay.min=10",
            "--room.playout_delay.max=20",
        ],
        &Env::default(),
    );
    assert_eq!(cfg.server.room.playout_delay, Some((10, 20)));

    // Pathological pair: min > max is passed through unclamped, exactly like
    // applyDefaultRoomConfig (pkg/service/roomallocator.go:195-199), which
    // stores Min/Max verbatim; the runtime consumer owns any ordering rule.
    let cfg = parse(
        &[
            "--config-body",
            "room:\n  playout_delay:\n    enabled: true\n    min: 500\n    max: 100\n",
        ],
        &Env::default(),
    );
    assert_eq!(cfg.server.room.playout_delay, Some((500, 100)));

    // Gating boundary: an enabled gate with an unset pair projects Some((0,0)),
    // which is distinguishable from the disabled None.
    let cfg = parse(
        &[
            "--config-body",
            "room:\n  playout_delay:\n    enabled: true\n",
        ],
        &Env::default(),
    );
    assert_eq!(cfg.server.room.playout_delay, Some((0, 0)));
}

// ---------------------------------------------------------------------------
// YAML:room.create_room_timeout (normalize.rs:672)
// ---------------------------------------------------------------------------

#[test]
fn room_create_room_timeout_yaml_env_cli_projection_as_nanoseconds() {
    // Go default: pkg/config/config.go:617 CreateRoomTimeout: 10 * time.Second.
    let base = parse(&[], &Env::default());
    assert_eq!(
        base.official.room.create_room_timeout,
        GoDuration::from_secs(10)
    );
    assert_eq!(
        base.server.room.create_room_timeout.as_nanos_i64(),
        10_000_000_000
    );

    // A Go-duration string projects to exact nanoseconds, not whole seconds.
    let cfg = parse(
        &["--config-body", "room:\n  create_room_timeout: 1500ms\n"],
        &Env::default(),
    );
    assert_eq!(
        cfg.server.room.create_room_timeout.as_nanos_i64(),
        1_500_000_000
    );
    assert_ne!(
        cfg.server.room.create_room_timeout,
        base.server.room.create_room_timeout
    );

    let env = Env::from_pairs([("LIVEKIT_ROOM_CREATE_ROOM_TIMEOUT", "2m")]);
    let cfg = parse(&[], &env);
    assert_eq!(
        cfg.server.room.create_room_timeout.as_nanos_i64(),
        120_000_000_000
    );
    let cfg = parse(&["--room.create_room_timeout=25s"], &env);
    assert_eq!(
        cfg.server.room.create_room_timeout.as_nanos_i64(),
        25_000_000_000
    );

    // Sub-microsecond fidelity: a nanosecond value that is not a whole number
    // of microseconds must survive the projection intact.
    let cfg = parse(
        &["--config-body", "room:\n  create_room_timeout: 1500001ns\n"],
        &Env::default(),
    );
    assert_eq!(
        cfg.server.room.create_room_timeout.as_nanos_i64(),
        1_500_001
    );

    // Boundary: 0 stays 0. The consumer turns a zero/absent budget into its
    // own fallback instead of a zero deadline (create_policy.rs:18-26).
    let cfg = parse(
        &["--config-body", "room:\n  create_room_timeout: 0s\n"],
        &Env::default(),
    );
    assert_eq!(cfg.server.room.create_room_timeout.as_nanos_i64(), 0);
}

// ---------------------------------------------------------------------------
// YAML:room.create_room_attempts (normalize.rs:673)
// ---------------------------------------------------------------------------

#[test]
fn room_create_room_attempts_yaml_env_cli_projection() {
    // Go default: pkg/config/config.go:618 CreateRoomAttempts: 3.
    let base = parse(&[], &Env::default());
    assert_eq!(base.official.room.create_room_attempts, 3);
    assert_eq!(base.server.room.create_room_attempts, 3);

    let cfg = parse(
        &["--config-body", "room:\n  create_room_attempts: 7\n"],
        &Env::default(),
    );
    assert_eq!(cfg.server.room.create_room_attempts, 7);
    assert_ne!(
        cfg.server.room.create_room_attempts,
        base.server.room.create_room_attempts
    );

    let env = Env::from_pairs([("LIVEKIT_ROOM_CREATE_ROOM_ATTEMPTS", "5")]);
    let cfg = parse(&[], &env);
    assert_eq!(cfg.server.room.create_room_attempts, 5);
    let cfg = parse(&["--room.create_room_attempts=1"], &env);
    assert_eq!(cfg.server.room.create_room_attempts, 1);

    // Boundary: a negative attempt count is preserved verbatim; the retry
    // loop compares it literally (create_policy.rs:30-39) rather than
    // silently disabling retries.
    let cfg = parse(&["--room.create_room_attempts=-2"], &Env::default());
    assert_eq!(cfg.server.room.create_room_attempts, -2);
    let cfg = parse(&["--room.create_room_attempts=0"], &Env::default());
    assert_eq!(cfg.server.room.create_room_attempts, 0);
}

// ---------------------------------------------------------------------------
// YAML:room.max_metadata_size (legacy alias, normalize.rs:802)
// ---------------------------------------------------------------------------

#[test]
fn room_max_metadata_size_legacy_alias_projection() {
    // Go default: Room.MaxMetadataSize is 0 (deprecated, config.go:279) and
    // Limit.MaxMetadataSize is 512 * 1024 (config.go:621). The port projects
    // the limit into server.limits.max_metadata_size (normalize.rs:802), read
    // by lk-compat/src/twirp/room_service.rs:219 and whip/mod.rs:241.
    let base = parse(&[], &Env::default());
    assert_eq!(base.official.room.max_metadata_size, 0);
    assert_eq!(base.official.limit.max_metadata_size, 512 * 1024);
    assert_eq!(base.server.limits.max_metadata_size, Some(512 * 1024));

    // The legacy room key reaches the SAME projected field the server reads.
    let cfg = parse(
        &["--config-body", "room:\n  max_metadata_size: 1024\n"],
        &Env::default(),
    );
    assert_eq!(cfg.official.room.max_metadata_size, 1024);
    assert_eq!(cfg.server.limits.max_metadata_size, Some(1024));
    assert_ne!(
        cfg.server.limits.max_metadata_size,
        base.server.limits.max_metadata_size
    );

    // ENV and CLI drive the identical projection.
    let env = Env::from_pairs([("LIVEKIT_ROOM_MAX_METADATA_SIZE", "2048")]);
    let cfg = parse(&[], &env);
    assert_eq!(cfg.server.limits.max_metadata_size, Some(2048));
    let cfg = parse(&["--room.max_metadata_size=64"], &env);
    assert_eq!(cfg.server.limits.max_metadata_size, Some(64));

    // Boundary: explicit 0 means "unset" and falls back to the limit default.
    let cfg = parse(&["--room.max_metadata_size=0"], &Env::default());
    assert_eq!(cfg.server.limits.max_metadata_size, Some(512 * 1024));
    // A negative legacy value is rejected by the loader before projection: both
    // room.max_metadata_size and limit.max_metadata_size are uint32 in Go
    // (pkg/config/config.go:279 and :373), so no aliasing branch can see one.
    let info = ProgramInfo {
        binary_name: "livecloud".into(),
        product_version: "fixture".into(),
        compat_version: "1.13.10",
    };
    for body in [
        "room:\n  max_metadata_size: -1\n",
        "limit:\n  max_metadata_size: -1\n",
    ] {
        let argv: Vec<OsString> = ["livecloud", "--dev", "--config-body", body]
            .into_iter()
            .map(OsString::from)
            .collect();
        assert!(
            parse_with_probe(argv, &Env::default(), &info, &NoNetwork).is_err(),
            "negative uint32 metadata limit must be rejected: {body}"
        );
    }
}

// ---------------------------------------------------------------------------
// YAML:room.room_configurations.{key}.name / metadata / tags / egress
// ---------------------------------------------------------------------------

#[test]
fn room_configurations_name_metadata_tags_egress_projection() {
    // Absent presets map: the projection is an empty map, not a default entry.
    let base = parse(&[], &Env::default());
    assert!(base.server.room.presets.is_empty());

    let yaml = concat!(
        "room:\n",
        "  room_configurations:\n",
        "    tagged:\n",
        "      name: ignored-id\n",
        "      metadata: preset-meta\n",
        "      tags:\n",
        "        tier: gold\n",
        "        region: ap-east\n",
        "      egress:\n",
        "        tracks:\n",
        "          filepath: out/tracks\n",
        "          disable_manifest: true\n",
        "        participant:\n",
        "          preset: 1\n",
        "    blank:\n",
        "      metadata: \"\"\n",
        "      tags: {}\n",
        "    nil_entry: ~\n",
    );
    let cfg = parse(&["--config-body", yaml], &Env::default());

    let tagged = preset(&cfg, "tagged");
    assert_eq!(tagged.metadata.as_deref(), Some("preset-meta"));
    assert_eq!(tagged.tags.get("tier").map(String::as_str), Some("gold"));
    assert_eq!(
        tagged.tags.get("region").map(String::as_str),
        Some("ap-east")
    );
    let recording = tagged
        .automatic_recording
        .as_ref()
        .expect("egress must project into automatic_recording");
    let tracks = recording
        .tracks
        .as_ref()
        .expect("tracks egress must project");
    assert_eq!(tracks.filepath, "out/tracks");
    assert!(tracks.disable_manifest);
    assert!(recording.participant.is_some());

    // Boundary: an empty metadata string and an empty tag map are indistinguishable
    // from absent after projection (None / empty), unlike a raw YAML presence check.
    let blank = preset(&cfg, "blank");
    assert_eq!(blank.metadata, None);
    assert!(blank.tags.is_empty());
    assert!(blank.automatic_recording.is_none());

    // A nil entry (`name: ~`) stays None in the projection; Go dereferences it
    // at pkg/service/roomallocator.go:211-219, so the distinction matters.
    assert!(
        matches!(cfg.server.room.presets.get("nil_entry"), Some(None)),
        "nil preset entry must project as Some(None)"
    );
}

// ---------------------------------------------------------------------------
// YAML:room.room_configurations.{key}.emptytimeout / maxparticipants /
// minplayoutdelay / maxplayoutdelay -- preset-over-global merge order.
// ---------------------------------------------------------------------------

#[test]
fn room_configurations_numeric_presets_override_global_defaults() {
    // The global block carries the Go defaults (300/20/0 playout); the preset
    // carries different values. A preset that does not override a field must
    // fall back to the global value, so each assertion names one authority.
    let yaml = concat!(
        "room:\n",
        "  empty_timeout: 300\n",
        "  departure_timeout: 20\n",
        "  max_participants: 9\n",
        "  playout_delay:\n",
        "    enabled: true\n",
        "    min: 100\n",
        "    max: 400\n",
        "  room_configurations:\n",
        "    override:\n",
        "      empty_timeout: 60\n",
        "      max_participants: 3\n",
        "      min_playout_delay: 20\n",
        "      max_playout_delay: 80\n",
        "    zeros:\n",
        "      empty_timeout: 0\n",
        "      max_participants: 0\n",
        "      min_playout_delay: 0\n",
        "      max_playout_delay: 0\n",
    );
    let cfg = parse(&["--config-body", yaml], &Env::default());

    // Global authority itself.
    assert_eq!(cfg.server.room.empty_timeout, Duration::from_secs(300));
    assert_eq!(cfg.server.room.departure_timeout, Duration::from_secs(20));
    assert_eq!(cfg.server.room.max_participants, 9);
    assert_eq!(cfg.server.room.playout_delay, Some((100, 400)));

    let over = preset(&cfg, "override");
    assert_eq!(over.empty_timeout_s, Some(60));
    assert_eq!(over.max_participants, Some(3));
    assert_eq!(over.min_playout_delay, Some(20));
    assert_eq!(over.max_playout_delay, Some(80));
    // The preset says nothing about departure_timeout: it stays unset here and
    // only inherits the global value when the allocator merges (permission.rs:164).
    assert_eq!(over.departure_timeout_s, None);

    // Boundary: every zero in a preset projects to None, i.e. "inherit", never
    // an explicit zero cap (normalize.rs:347 nz()).
    let zeros = preset(&cfg, "zeros");
    assert_eq!(zeros.empty_timeout_s, None);
    assert_eq!(zeros.departure_timeout_s, None);
    assert_eq!(zeros.max_participants, None);
    assert_eq!(zeros.min_playout_delay, None);
    assert_eq!(zeros.max_playout_delay, None);
}

#[test]
fn room_configurations_departuretimeout_preset_projection() {
    let yaml = concat!(
        "room:\n",
        "  departure_timeout: 20\n",
        "  room_configurations:\n",
        "    long:\n",
        "      departure_timeout: 15\n",
        "    unset: {}\n",
    );
    let cfg = parse(&["--config-body", yaml], &Env::default());
    assert_eq!(cfg.server.room.departure_timeout, Duration::from_secs(20));
    assert_eq!(preset(&cfg, "long").departure_timeout_s, Some(15));
    assert_eq!(preset(&cfg, "unset").departure_timeout_s, None);
}

// ---------------------------------------------------------------------------
// Consumed merge order: preset fills only what the request left unset.
// Mirrors pkg/service/roomallocator.go:211-247 (clone-first-wins) plus
// applyDefaultRoomConfig (:188-201) for the global fallback.
// ---------------------------------------------------------------------------

#[test]
fn room_preset_inherit_keeps_request_authority_over_the_preset() {
    use rtc_core::permission::RoomConfig;
    let yaml = concat!(
        "room:\n",
        "  room_configurations:\n",
        "    p:\n",
        "      empty_timeout: 60\n",
        "      departure_timeout: 15\n",
        "      max_participants: 3\n",
        "      metadata: preset-meta\n",
        "      min_playout_delay: 20\n",
        "      max_playout_delay: 80\n",
    );
    let cfg = parse(&["--config-body", yaml], &Env::default());
    let p = preset(&cfg, "p").clone();

    // An unset request inherits every projected preset field.
    let mut unset = RoomConfig::default();
    unset.inherit(&p);
    assert_eq!(
        (
            unset.empty_timeout_s,
            unset.departure_timeout_s,
            unset.max_participants,
            unset.metadata.as_deref(),
            unset.min_playout_delay,
            unset.max_playout_delay
        ),
        (
            Some(60),
            Some(15),
            Some(3),
            Some("preset-meta"),
            Some(20),
            Some(80)
        )
    );

    // A set request value wins: this is the leg that fails if the merge order
    // is inverted to `p.or(self)` (roomallocator.go:218-247).
    let mut explicit = RoomConfig {
        empty_timeout_s: Some(1),
        departure_timeout_s: Some(2),
        max_participants: Some(5),
        metadata: Some("request-meta".into()),
        min_playout_delay: Some(7),
        max_playout_delay: Some(9),
        ..Default::default()
    };
    explicit.inherit(&p);
    assert_eq!(explicit.empty_timeout_s, Some(1));
    assert_eq!(explicit.departure_timeout_s, Some(2));
    assert_eq!(explicit.max_participants, Some(5));
    assert_eq!(explicit.metadata.as_deref(), Some("request-meta"));
    assert_eq!(explicit.min_playout_delay, Some(7));
    assert_eq!(explicit.max_playout_delay, Some(9));
}
