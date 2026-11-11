//! Public Room mutation + controlled model receipts. NOT native/RoomTask/SDK proof.
use super::*;

struct SourceFixture {
    r: Room,
    at: Instant,
    publisher: Joined,
    subscriber: Joined,
    publisher_transport: TransportId,
    track: TrackSid,
    cumulative_errors: usize,
    close_receipts: usize,
}
impl SourceFixture {
    fn new(one_shot: bool, present: bool) -> Self {
        let (mut r, at) = room();
        Arc::make_mut(&mut r.policy).reconnect_on_subscription_error = false;
        let publisher = r.join(params("publisher", 17, true), at).unwrap();
        let effects = r.take_effects();
        let publisher_transport = transport_of(&effects, &publisher.sid, PcRole::Single);
        r.handle_media_event(
            MediaEvent::TransportConnected {
                participant: publisher.sid.clone(),
                transport: publisher_transport,
            },
            at,
        );
        r.handle_request(
            &publisher.sid,
            publisher.generation,
            SessionRequest::AddTrack(add_track(
                "source-oracle-audio",
                TrackKind::Audio,
                TrackSource::Microphone,
            )),
            at,
        );
        let ack = r.take_effects();
        let track = signals_to(&ack, &publisher.sid)
            .into_iter()
            .find_map(|s| match s {
                SignalEvent::TrackPublished { track, .. } => Some(track.sid),
                _ => None,
            })
            .expect("real public AddTrack ACK supplies SID");
        let mut bp = params("subscriber", 17, true);
        bp.auto_subscribe = false;
        bp.capabilities.one_shot = one_shot;
        let subscriber = r.join(bp, at).unwrap();
        let effects = r.take_effects();
        let bt = transport_of(&effects, &subscriber.sid, PcRole::Single);
        r.handle_media_event(
            MediaEvent::TransportConnected {
                participant: subscriber.sid.clone(),
                transport: bt,
            },
            at,
        );
        r.take_effects();
        let mut f = Self {
            r,
            at,
            publisher,
            subscriber,
            publisher_transport,
            track,
            cumulative_errors: 0,
            close_receipts: 0,
        };
        if present {
            f.promote(at);
        }
        f
    }
    fn promote(&mut self, now: Instant) {
        self.r.media_arrives(
            RemoteMedia {
                registration: crate::media_api::PublicationRegistration::Pending,
                participant: self.publisher.sid.clone(),
                transport: self.publisher_transport,
                mid: "source-oracle-mid".into(),
                kind: TrackKind::Audio,
                msid_track_id: "source-oracle-audio".into(),
                rids: vec![],
                mime_type: "audio/opus".into(),
            },
            now,
        );
        self.consume(now);
    }
    fn consume(&mut self, now: Instant) {
        // Same controlled-boundary role as Go's real AddSubscriber returning its
        // original fake track. Inputs are labelled MODEL, never authenticated RTP.
        let effects = self.r.take_effects();
        let mut prepared = 0;
        for effect in effects {
            match effect {
                Effect::Media(MediaCommand::Subscribe(spec))
                    if spec.subscriber == self.subscriber.sid && spec.track_sid == self.track =>
                {
                    prepared += 1
                }
                Effect::Signal {
                    to,
                    event: SignalEvent::SubscriptionResponse { track_sid, error },
                } if to == self.subscriber.sid && track_sid == self.track => {
                    assert_eq!(
                        error,
                        SubscriptionError::Unknown,
                        "this permission source corpus has only prepared/unbound diagnostic"
                    );
                    self.cumulative_errors += 1;
                }
                _ => {}
            }
        }
        for _ in 0..prepared {
            self.r.handle_media_event(
                MediaEvent::SubscriptionPrepared {
                    subscriber: self.subscriber.sid.clone(),
                    track_sid: self.track.clone(),
                },
                now,
            );
        }
        // Prepared is an owner receipt, not a native negotiation or bind.
        let follow = self.r.take_effects();
        assert!(
            !follow
                .iter()
                .any(|e| matches!(e, Effect::Media(MediaCommand::Subscribe(_)))),
            "prepared receipt must not manufacture another admission"
        );
    }
    fn step(&mut self, op: &str, ns: u64) {
        let now = self.at + Duration::from_nanos(ns);
        match op {
            "settings" => self.r.handle_request(
                &self.subscriber.sid,
                self.subscriber.generation,
                SessionRequest::TrackSettings {
                    track_sids: vec![self.track.clone()],
                    settings: TrackSettings {
                        disabled: true,
                        ..Default::default()
                    },
                },
                now,
            ),
            "subscribe" | "unsubscribe" => self
                .r
                .api_update_subscriptions(
                    &Identity::from("subscriber"),
                    std::slice::from_ref(&self.track),
                    op == "subscribe",
                    now,
                )
                .unwrap(),
            "bind" => {
                assert!(
                    self.r.participants[&self.subscriber.sid].subscriptions[&self.track].requested,
                    "bind must follow real owner admission"
                );
                self.r.handle_media_event(
                    MediaEvent::SubscriptionBound {
                        subscriber: self.subscriber.sid.clone(),
                        track_sid: self.track.clone(),
                        mid: "source-oracle-down".into(),
                    },
                    now,
                );
            }
            "close" => {
                let s = &self.r.participants[&self.subscriber.sid].subscriptions[&self.track];
                assert!(
                    s.requested || s.closing,
                    "controlled close must refer to prior real admission"
                );
                self.r.handle_media_event(
                    MediaEvent::SubscriptionRemoved {
                        subscriber: self.subscriber.sid.clone(),
                        track_sid: self.track.clone(),
                    },
                    now,
                );
                self.close_receipts += 1;
            }
            "deny" | "allow" => self.r.handle_request(
                &self.publisher.sid,
                self.publisher.generation,
                SessionRequest::SubscriptionPermission(SubscriptionPermission {
                    all_participants: op == "allow",
                    track_permissions: vec![],
                }),
                now,
            ),
            "missing" => assert!(
                !self
                    .r
                    .get_participant(&Identity::from("publisher"))
                    .unwrap()
                    .tracks
                    .iter()
                    .any(|t| t.sid == self.track),
                "known ACK remains pending, no fabricated source track"
            ),
            "present" => {
                self.promote(now);
                return;
            }
            "tick" => {
                self.r.tick(now);
            }
            // Original source fake IsDisconnected is not Room.signal_closed:
            // real room lifetime transitions must not be replaced by a state write.
            "disconnect" | "connect" => {
                panic!("source-only predicate control is not a public Room operation")
            }
            _ => panic!("undeclared original source operation"),
        }
        self.consume(now);
    }
}
fn boolean(s: &str) -> bool {
    match s {
        "true" => true,
        "false" => false,
        _ => panic!("invalid boolean cell"),
    }
}
fn optional_bool(s: &str) -> Option<bool> {
    if s == "-" { None } else { Some(boolean(s)) }
}
fn valid_hex(s: &str) {
    assert_eq!(s.len() % 2, 0);
    assert!(
        s.bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    );
}
fn rows(kind: &str, env: &str, count: usize, sha: &str) -> Vec<Vec<String>> {
    let text =
        std::fs::read_to_string(std::env::var_os(env).expect("strict normalizer output required"))
            .unwrap();
    let mut lines = text.lines();
    assert_eq!(
        lines.next().unwrap(),
        format!("#GO_OWNER_OBSERVABLE_V1\t{kind}\t{count}\t{sha}")
    );
    let rows: Vec<_> = lines
        .map(|l| l.split('\t').map(str::to_owned).collect::<Vec<_>>())
        .collect();
    assert_eq!(rows.len(), count);
    rows
}
const PERMISSION_ACTIONS: &[(&str, usize, u64, &str, bool)] = &[
    (
        "bound-denied-restored-held-then-bind",
        0,
        0,
        "subscribe",
        false,
    ),
    ("bound-denied-restored-held-then-bind", 1, 0, "bind", false),
    (
        "bound-denied-restored-held-then-bind",
        2,
        20000000000,
        "tick",
        false,
    ),
    (
        "bound-denied-restored-held-then-bind",
        3,
        21000000000,
        "deny",
        false,
    ),
    (
        "bound-denied-restored-held-then-bind",
        4,
        40000000000,
        "tick",
        false,
    ),
    (
        "bound-denied-restored-held-then-bind",
        5,
        41000000000,
        "allow",
        false,
    ),
    (
        "bound-denied-restored-held-then-bind",
        6,
        45000000000,
        "tick",
        false,
    ),
    (
        "bound-denied-restored-held-then-bind",
        7,
        46000000000,
        "bind",
        false,
    ),
    (
        "bound-denied-restored-held-then-bind",
        8,
        70000000000,
        "tick",
        false,
    ),
    (
        "restored-held-over15-and-repeated-allow",
        0,
        0,
        "subscribe",
        false,
    ),
    (
        "restored-held-over15-and-repeated-allow",
        1,
        0,
        "bind",
        false,
    ),
    (
        "restored-held-over15-and-repeated-allow",
        2,
        21000000000,
        "deny",
        false,
    ),
    (
        "restored-held-over15-and-repeated-allow",
        3,
        41000000000,
        "allow",
        false,
    ),
    (
        "restored-held-over15-and-repeated-allow",
        4,
        45000000000,
        "tick",
        false,
    ),
    (
        "restored-held-over15-and-repeated-allow",
        5,
        50000000000,
        "allow",
        false,
    ),
    (
        "restored-held-over15-and-repeated-allow",
        6,
        56000000000,
        "tick",
        false,
    ),
    (
        "restored-held-over15-and-repeated-allow",
        7,
        57000000000,
        "tick",
        false,
    ),
    ("already-allowed-does-not-renew", 0, 0, "subscribe", false),
    (
        "already-allowed-does-not-renew",
        1,
        10000000000,
        "allow",
        false,
    ),
    (
        "already-allowed-does-not-renew",
        2,
        16000000000,
        "tick",
        false,
    ),
];
const SETTINGS_ACTIONS: &[(&str, usize, u64, &str, bool)] = &[
    ("settings-only-exact-expiry", 0, 0, "settings", false),
    ("settings-only-exact-expiry", 1, 59000000000, "tick", false),
    ("settings-only-exact-expiry", 2, 60000000000, "tick", false),
    (
        "settings-only-exact-expiry",
        3,
        61000000000,
        "subscribe",
        false,
    ),
    ("undesired-settings-renew", 0, 0, "settings", false),
    (
        "undesired-settings-renew",
        1,
        50000000000,
        "settings",
        false,
    ),
    ("undesired-settings-renew", 2, 60000000000, "tick", false),
    ("undesired-settings-renew", 3, 109000000000, "tick", false),
    ("undesired-settings-renew", 4, 111000000000, "tick", false),
    (
        "undesired-settings-renew",
        5,
        112000000000,
        "subscribe",
        false,
    ),
    ("subscribe-clears-keep-no-downtrack", 0, 0, "missing", false),
    (
        "subscribe-clears-keep-no-downtrack",
        1,
        0,
        "settings",
        false,
    ),
    (
        "subscribe-clears-keep-no-downtrack",
        2,
        30000000000,
        "subscribe",
        false,
    ),
    (
        "subscribe-clears-keep-no-downtrack",
        3,
        40000000000,
        "unsubscribe",
        false,
    ),
    (
        "subscribe-clears-keep-no-downtrack",
        4,
        41000000000,
        "present",
        false,
    ),
    (
        "subscribe-clears-keep-no-downtrack",
        5,
        42000000000,
        "subscribe",
        false,
    ),
    (
        "unsubscribe-waits-close-no-new-ttl",
        0,
        0,
        "settings",
        false,
    ),
    (
        "unsubscribe-waits-close-no-new-ttl",
        1,
        1000000000,
        "subscribe",
        false,
    ),
    (
        "unsubscribe-waits-close-no-new-ttl",
        2,
        1000000000,
        "bind",
        false,
    ),
    (
        "unsubscribe-waits-close-no-new-ttl",
        3,
        2000000000,
        "unsubscribe",
        false,
    ),
    (
        "unsubscribe-waits-close-no-new-ttl",
        4,
        90000000000,
        "tick",
        false,
    ),
    (
        "unsubscribe-waits-close-no-new-ttl",
        5,
        91000000000,
        "close",
        false,
    ),
    (
        "unsubscribe-waits-close-no-new-ttl",
        6,
        92000000000,
        "subscribe",
        false,
    ),
    ("settings-after-unsubscribe-keep", 0, 0, "subscribe", false),
    (
        "settings-after-unsubscribe-keep",
        1,
        1000000000,
        "unsubscribe",
        false,
    ),
    (
        "settings-after-unsubscribe-keep",
        2,
        2000000000,
        "settings",
        false,
    ),
    (
        "settings-after-unsubscribe-keep",
        3,
        3000000000,
        "close",
        false,
    ),
    (
        "settings-after-unsubscribe-keep",
        4,
        60000000000,
        "tick",
        false,
    ),
    (
        "settings-after-unsubscribe-keep",
        5,
        63000000000,
        "tick",
        false,
    ),
    (
        "settings-after-unsubscribe-keep",
        6,
        64000000000,
        "subscribe",
        false,
    ),
    (
        "redesire-before-close-retains-settings",
        0,
        0,
        "settings",
        false,
    ),
    (
        "redesire-before-close-retains-settings",
        1,
        1000000000,
        "subscribe",
        false,
    ),
    (
        "redesire-before-close-retains-settings",
        2,
        2000000000,
        "unsubscribe",
        false,
    ),
    (
        "redesire-before-close-retains-settings",
        3,
        3000000000,
        "subscribe",
        false,
    ),
    (
        "redesire-before-close-retains-settings",
        4,
        4000000000,
        "close",
        false,
    ),
    (
        "redesire-before-close-retains-settings",
        5,
        6000000000,
        "tick",
        false,
    ),
    (
        "already-undesired-unsubscribe-does-not-renew",
        0,
        0,
        "settings",
        false,
    ),
    (
        "already-undesired-unsubscribe-does-not-renew",
        1,
        20000000000,
        "unsubscribe",
        false,
    ),
    (
        "already-undesired-unsubscribe-does-not-renew",
        2,
        60000000000,
        "tick",
        false,
    ),
    (
        "already-undesired-unsubscribe-does-not-renew",
        3,
        61000000000,
        "subscribe",
        false,
    ),
    ("desired-settings-do-not-arm-keep", 0, 0, "subscribe", false),
    (
        "desired-settings-do-not-arm-keep",
        1,
        1000000000,
        "bind",
        false,
    ),
    (
        "desired-settings-do-not-arm-keep",
        2,
        20000000000,
        "settings",
        false,
    ),
    (
        "desired-settings-do-not-arm-keep",
        3,
        21000000000,
        "unsubscribe",
        false,
    ),
    (
        "desired-settings-do-not-arm-keep",
        4,
        22000000000,
        "close",
        false,
    ),
    (
        "desired-settings-do-not-arm-keep",
        5,
        23000000000,
        "subscribe",
        false,
    ),
    ("disconnected-defers-cleanup", 0, 0, "settings", false),
    (
        "disconnected-defers-cleanup",
        1,
        2000000000,
        "disconnect",
        false,
    ),
    ("disconnected-defers-cleanup", 2, 60000000000, "tick", false),
    (
        "disconnected-defers-cleanup",
        3,
        65000000000,
        "connect",
        false,
    ),
    ("disconnected-defers-cleanup", 4, 66000000000, "tick", false),
    (
        "disconnected-defers-cleanup",
        5,
        67000000000,
        "subscribe",
        false,
    ),
    (
        "one-shot-unsubscribe-immediate-delete",
        0,
        0,
        "settings",
        true,
    ),
    (
        "one-shot-unsubscribe-immediate-delete",
        1,
        1000000000,
        "unsubscribe",
        true,
    ),
    (
        "one-shot-unsubscribe-immediate-delete",
        2,
        2000000000,
        "subscribe",
        true,
    ),
];
#[test]
#[ignore = "requires original fixed Go20 actual bytes and strict complete-schema normalizer"]
fn actual_go20_permission_public_owner_observable_subscope() {
    let input = rows(
        "permission",
        "GO_PERMISSION_OWNER_TSV",
        20,
        "96dd60e361be3322a6222d00e1fa59737241f6a7dbe2716b9fe88da52c50c227",
    );
    assert_eq!(input.len(), PERMISSION_ACTIONS.len());
    let mut state: Option<SourceFixture> = None;
    for (row, (id, index, ns, op, _)) in input.iter().zip(PERMISSION_ACTIONS) {
        assert_eq!(row.len(), 13);
        assert_eq!(row[0], *id);
        assert_eq!(row[1].parse::<usize>().unwrap(), *index);
        assert_eq!(row[2].parse::<u64>().unwrap(), *ns);
        assert_eq!(row[3], *op);
        if *index == 0 {
            state = Some(SourceFixture::new(false, true));
        }
        let f = state.as_mut().unwrap();
        f.step(op, *ns);
        let sub = &f.r.participants[&f.subscriber.sid].subscriptions[&f.track];
        assert_eq!(sub.desired, boolean(&row[4]), "{id} {index} desired");
        assert_eq!(sub.bound, boolean(&row[5]), "{id} {index} bound");
        assert_eq!(
            sub.requested && !sub.bound,
            boolean(&row[6]),
            "{id} {index} owner admitted but unbound"
        );
        let permitted = f.r.participants[&f.publisher.sid].allows_subscriber(
            &f.track,
            &f.subscriber.sid,
            &Identity::from("subscriber"),
        );
        assert_eq!(
            permitted,
            boolean(&row[7]),
            "{id} {index} actual publisher policy"
        );
        assert_eq!(
            f.cumulative_errors,
            row[8].parse::<usize>().unwrap(),
            "{id} {index} real owner diagnostic count"
        );
        // Preserve/parse Go's internal timestamps and complete raw errors/row;
        // DO NOT fill invented clocks or fatal/message fields into Rust output.
        let _: i64 = row[9].parse().unwrap();
        let _: i64 = row[10].parse().unwrap();
        valid_hex(&row[11]);
        valid_hex(&row[12]);
    }
    std::eprintln!(
        "PASS_OBSERVABLE_SUBSCOPE permission rows20: owner desired/bound/admitted-unbound/policy/typed diagnostic count; source-only internal started_ns/subscribe_ns and errors fatal/text/rawTrackID NOT equality claims"
    );
}
#[test]
#[ignore = "requires original fixed Go55 actual bytes and strict complete-schema normalizer"]
fn actual_go55_settings_public_owner_observable_subscope() {
    let input = rows(
        "settings",
        "GO_SETTINGS_OWNER_TSV",
        55,
        "47c5817430b77beafc81d4530151259f25ba899b5d36684085827a7ee2d5aec3",
    );
    assert_eq!(input.len(), SETTINGS_ACTIONS.len());
    let mut state: Option<SourceFixture> = None;
    let mut compared = 0;
    let mut source_only = 0;
    for (row, (id, index, ns, op, one_shot)) in input.iter().zip(SETTINGS_ACTIONS) {
        assert_eq!(row.len(), 15);
        assert_eq!(row[0], *id);
        assert_eq!(row[1].parse::<usize>().unwrap(), *index);
        assert_eq!(row[2].parse::<u64>().unwrap(), *ns);
        assert_eq!(row[3], *op);
        assert_eq!(boolean(&row[4]), *one_shot);
        let entry = boolean(&row[5]);
        for cell in [6, 7, 8, 10, 11] {
            let _ = optional_bool(&row[cell]);
        }
        if row[9] != "-" {
            let _: i64 = row[9].parse().unwrap();
        }
        let _: usize = row[12].parse().unwrap();
        valid_hex(&row[13]);
        valid_hex(&row[14]);
        if *id == "disconnected-defers-cleanup" {
            // All6 original rows retained and full-schema-validated, explicitly
            // outside public Room domain: fake IsDisconnected toggles independently
            // of participant lifetime. No direct info.state overwrite is permitted.
            source_only += 1;
            continue;
        }
        if *index == 0 {
            state = Some(SourceFixture::new(*one_shot, *op != "missing"));
        }
        let f = state.as_mut().unwrap();
        f.step(op, *ns);
        let p = &f.r.participants[&f.subscriber.sid];
        let sub = p.subscriptions.get(&f.track);
        assert_eq!(sub.is_some(), entry, "{id} {index} entry");
        if let Some(sub) = sub {
            assert_eq!(
                Some(sub.desired),
                optional_bool(&row[6]),
                "{id} {index} desired"
            );
            assert_eq!(
                Some(sub.requested || sub.closing),
                optional_bool(&row[7]),
                "{id} {index} admitted/teardown-pending owner slot (not actual native DownTrack)"
            );
            let kept = sub
                .keep_until
                .is_some_and(|until| f.at + Duration::from_nanos(*ns) < until);
            assert_eq!(
                Some(kept),
                optional_bool(&row[8]),
                "{id} {index} actual keep predicate"
            );
            let raw = row[9].parse::<i64>().unwrap();
            // Go zero time Sub(origin).Nanoseconds saturates MIN; map that
            // documented unset sentinel to Option None, not an invented deadline.
            let expected = if raw == i64::MIN {
                None
            } else {
                Some(f.at + Duration::from_nanos(u64::try_from(raw).unwrap()))
            };
            assert_eq!(
                sub.keep_until, expected,
                "{id} {index} optional keep deadline"
            );
            let settings = p.track_settings.get(&f.track);
            assert_eq!(
                Some(settings.is_some()),
                optional_bool(&row[10]),
                "{id} {index} settings presence"
            );
            assert_eq!(
                settings.map(|s| s.disabled),
                optional_bool(&row[11]),
                "{id} {index} actual settings value"
            );
        } else {
            for cell in &row[6..12] {
                assert_eq!(*cell, "-");
            }
        }
        assert_eq!(
            f.close_receipts,
            row[12].parse::<usize>().unwrap(),
            "{id} {index} accepted controlled owner removal receipt"
        );
        compared += 1;
    }
    assert_eq!((compared, source_only), (49, 6));
    std::eprintln!(
        "PASS_OBSERVABLE_SUBSCOPE settings: full source schema55, public Room49, source-only fake-disconnection-control6; internal actual_downtrack_setting_calls/immediate NOT native application claims"
    );
}

/// Standalone permission restoration lifecycle test.
/// Exercises the deny→allow→re-subscribe cycle without Go oracle dependency.
/// Validates monitor removal on deny, fresh monitor on allow, and 15s supervision.
#[test]
fn permission_deny_allow_restores_subscription_with_fresh_monitor() {
    // Scenario: subscribe → bind → deny (monitor removed) → allow (fresh monitor) → bind
    let mut f = SourceFixture::new(false, true);
    let origin = f.at;

    // Step 0: subscribe
    f.step("subscribe", 0);
    let sub = &f.r.participants[&f.subscriber.sid].subscriptions[&f.track];
    assert!(sub.desired, "subscribe must set desired");
    assert!(sub.requested, "subscribe with present track must request");

    // Step 1: bind
    f.step("bind", 0);
    let sub = &f.r.participants[&f.subscriber.sid].subscriptions[&f.track];
    assert!(sub.bound, "bind must set bound flag");

    // Step 2: advance 20s (past any threshold, subscription is healthy)
    f.step("tick", 20_000_000_000);

    // Step 3: deny → publisher revokes permission
    f.step("deny", 21_000_000_000);
    // Monitor must be removed on deny (subscribe.rs:441-443)
    assert!(
        !f.r.subscription_monitors
            .contains_key(&(f.subscriber.sid.clone(), f.track.clone())),
        "deny must remove subscription monitor"
    );
    let sub = &f.r.participants[&f.subscriber.sid].subscriptions[&f.track];
    assert!(!sub.requested, "deny must clear requested flag");
    assert!(!sub.bound, "deny must clear bound flag");

    // Step 4: advance to 40s, tick
    f.step("tick", 40_000_000_000);

    // Step 5: allow → publisher restores permission
    f.step("allow", 41_000_000_000);
    // Fresh monitor must be created on re-subscribe (subscribe.rs:187-193)
    let monitor =
        f.r.subscription_monitors
            .get(&(f.subscriber.sid.clone(), f.track.clone()));
    assert!(
        monitor.is_some(),
        "allow must create fresh subscription monitor"
    );
    let monitor = monitor.unwrap();
    // The monitor's started time must be at the allow time (41s), not the original subscribe time
    assert!(
        monitor.started >= origin + Duration::from_secs(40),
        "fresh monitor must have current timestamp, not original"
    );

    // Step 6: tick at 45s
    f.step("tick", 45_000_000_000);

    // Step 7: bind again at 46s
    f.step("bind", 46_000_000_000);
    let sub = &f.r.participants[&f.subscriber.sid].subscriptions[&f.track];
    assert!(sub.bound, "re-bind after allow must set bound flag");

    // Step 8: tick at 70s - no error should be reported since bound
    f.step("tick", 70_000_000_000);
    let monitor =
        f.r.subscription_monitors
            .get(&(f.subscriber.sid.clone(), f.track.clone()));
    // Bound subscription should have no failure
    if let Some(m) = monitor {
        assert!(
            m.failed.is_none(),
            "bound subscription must not have failure"
        );
    }
}

/// Permission restore after 15s supervision threshold: verify that a deny→allow
/// cycle where the deny period exceeds 15s still creates a fresh monitor and
/// does not carry over the old supervision epoch.
#[test]
fn permission_restore_after_15s_creates_fresh_epoch_not_carried_over() {
    let mut f = SourceFixture::new(false, true);
    let origin = f.at;

    // subscribe + bind
    f.step("subscribe", 0);
    f.step("bind", 0);

    // deny at 21s
    f.step("deny", 21_000_000_000);
    assert!(
        !f.r.subscription_monitors
            .contains_key(&(f.subscriber.sid.clone(), f.track.clone())),
        "deny must remove monitor"
    );

    // Allow at 41s (20s after deny, well past 15s threshold)
    f.step("allow", 41_000_000_000);
    let monitor =
        f.r.subscription_monitors
            .get(&(f.subscriber.sid.clone(), f.track.clone()));
    assert!(monitor.is_some(), "allow must recreate monitor");
    let m = monitor.unwrap();
    // Fresh monitor started must be at 41s, not the original 0s
    assert!(
        m.started >= origin + Duration::from_secs(40),
        "monitor epoch must be fresh, not carried from original subscription"
    );
    // No failure on the fresh monitor
    assert!(m.failed.is_none(), "fresh monitor must not have failure");

    // Repeated allow at 50s must not reset the monitor (idempotent grant)
    let monitor_before =
        f.r.subscription_monitors
            .get(&(f.subscriber.sid.clone(), f.track.clone()))
            .unwrap()
            .started;
    f.step("allow", 50_000_000_000);
    let monitor_after =
        f.r.subscription_monitors
            .get(&(f.subscriber.sid.clone(), f.track.clone()))
            .unwrap()
            .started;
    assert_eq!(
        monitor_before, monitor_after,
        "repeated allow must not reset existing monitor epoch"
    );
}
