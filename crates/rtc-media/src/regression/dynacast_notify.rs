use super::*;
use rtc_core::media_api::{DownTrackSpec, PublishedTrackSpec};
use rtc_core::model::{BackupCodecPolicy, TrackCodec, TrackKind, TrackSettings, TrackSource};
use serde::{Deserialize, Serialize};

const CODECS: [&str; 3] = ["vp8", "vp9", "h264"];
const SID: &str = "TR_notify_snapshot";

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Quality {
    quality: i32,
    enabled: bool,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CodecSnapshot {
    codec: String,
    qualities: Vec<Quality>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    codecs: Vec<CodecSnapshot>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Trace {
    codec_count: usize,
    batch: bool,
    events: Vec<Snapshot>,
}

fn fixture(count: usize, delay: Duration, now: Instant) -> (MediaEngine, Vec<(usize, usize)>) {
    fixture_with_demand(count, delay, now, true)
}

fn fixture_with_demand(
    count: usize,
    delay: Duration,
    now: Instant,
    initialize: bool,
) -> (MediaEngine, Vec<(usize, usize)>) {
    let mut engine = engine(now);
    let sid = TrackSid::from(SID);
    let spec = PublishedTrackSpec {
        backup_codec_policy: BackupCodecPolicy::Simulcast,
        sid: sid.clone(),
        kind: TrackKind::Video,
        source: TrackSource::Camera,
        cid: "notify".into(),
        layers: vec![],
        codecs: CODECS[..count]
            .iter()
            .map(|codec| TrackCodec {
                mime_type: format!("video/{codec}"),
                ..Default::default()
            })
            .collect(),
        stereo: false,
        disable_dtx: false,
        disable_red: false,
        encrypted: false,
        muted: false,
    };
    engine.published.insert(sid.clone(), spec.clone());
    let mut tracks = Vec::new();
    for (index, codec) in CODECS[..count].iter().enumerate() {
        let mime = format!("video/{codec}");
        let ui = engine.ups.insert(crate::uptrack::UpTrack::new(
            spec.clone(),
            "PA_notify".into(),
            TransportId::next(),
            Mid::from(index.to_string().as_str()),
            TrackKind::Video,
            mime.clone(),
            vec!["q".into(), "h".into(), "f".into()],
            &engine.audio_params,
            delay,
            now,
        ));
        engine.up_by_sid.entry(sid.clone()).or_insert(ui);
        engine
            .codec_receivers
            .entry(sid.clone())
            .or_default()
            .bind(&mime, ui);
        let mut down = crate::downtrack::DownTrack::new(
            DownTrackSpec {
                subscriber: format!("PA_sub_{index}").into(),
                publisher: "PA_notify".into(),
                track_sid: sid.clone(),
                stream_id: "notify".into(),
                settings: TrackSettings::default(),
                adaptive_stream: false,
            },
            TrackKind::Video,
            ui,
            false,
            &mime,
            now,
        );
        down.max_spatial = -1;
        let di = engine.downs.insert(down);
        engine.ups.get_mut(ui).unwrap().subscribers.push(di);
        if initialize {
            engine.update_dynacast(ui, now);
        }
        tracks.push((ui, di));
    }
    engine.events.clear();
    (engine, tracks)
}

fn demand(engine: &mut MediaEngine, track: (usize, usize), quality: i32, now: Instant) {
    engine.downs.get_mut(track.1).unwrap().max_spatial = if quality == 3 { -1 } else { quality };
    engine.update_dynacast(track.0, now);
}

fn drain(engine: &mut MediaEngine, snapshots: &mut Vec<Snapshot>) {
    let mut events = Vec::new();
    engine.drain_events(&mut events);
    for event in events {
        let MediaEvent::SubscribedQuality {
            publisher,
            track_sid,
            codecs,
        } = event
        else {
            panic!("unexpected event {event:?}");
        };
        assert_eq!(publisher, ParticipantSid::from("PA_notify"));
        assert_eq!(track_sid, TrackSid::from(SID));
        let mut codecs: Vec<_> = codecs
            .into_iter()
            .map(|codec| CodecSnapshot {
                codec: codec.codec,
                qualities: codec
                    .qualities
                    .into_iter()
                    .map(|quality| Quality {
                        quality: quality.quality.as_i32(),
                        enabled: quality.enabled,
                    })
                    .collect(),
            })
            .collect();
        codecs.sort_by(|a, b| a.codec.cmp(&b.codec));
        snapshots.push(Snapshot { codecs });
    }
}

fn expected(qualities: &[i32]) -> Snapshot {
    let mut codecs: Vec<_> = qualities
        .iter()
        .enumerate()
        .map(|(index, max)| CodecSnapshot {
            codec: CODECS[index].into(),
            qualities: (0..3)
                .map(|quality| Quality {
                    quality,
                    enabled: *max != 3 && quality <= *max,
                })
                .collect(),
        })
        .collect();
    codecs.sort_by(|a, b| a.codec.cmp(&b.codec));
    Snapshot { codecs }
}

fn trace(count: usize, batch: bool) -> Trace {
    let now = Instant::now();
    let (mut engine, tracks) = fixture(count, Duration::ZERO, now);
    let mut events = Vec::new();
    for quality in [2, 1, 0, 3, 2, 2] {
        for track in &tracks {
            demand(&mut engine, *track, quality, now);
            if !batch {
                drain(&mut engine, &mut events);
            }
        }
    }
    drain(&mut engine, &mut events);
    Trace {
        codec_count: count,
        batch,
        events,
    }
}

fn assert_trace(trace: Trace) {
    let mut qualities = vec![3; trace.codec_count];
    let mut events = Vec::new();
    for quality in [2, 1, 0, 3, 2] {
        for index in 0..trace.codec_count {
            qualities[index] = quality;
            events.push(expected(&qualities));
        }
    }
    assert_eq!(
        trace.events, events,
        "count={} batch={}",
        trace.codec_count, trace.batch
    );
}

#[test]
fn single_codec_snapshot_control() {
    for batch in [false, true] {
        assert_trace(trace(1, batch));
    }
}

#[test]
fn multi_codec_snapshots_preserve_each_commit_before_drain() {
    for count in [2, 3] {
        for batch in [false, true] {
            assert_trace(trace(count, batch));
        }
    }
}

#[test]
fn timer_commits_capture_all_receivers_at_shared_deadline() {
    let now = Instant::now();
    let (mut engine, tracks) = fixture(2, Duration::from_secs(1), now);
    for track in &tracks {
        demand(&mut engine, *track, 2, now);
    }
    engine.events.clear();
    for track in &tracks {
        demand(&mut engine, *track, 0, now);
    }
    assert!(engine.events.is_empty());
    engine.dynacast_timers(now + Duration::from_millis(999));
    assert!(engine.events.is_empty());
    engine.dynacast_timers(now + Duration::from_secs(1));
    let mut events = Vec::new();
    drain(&mut engine, &mut events);
    assert_eq!(events, vec![expected(&[0, 0])]);
}

#[test]
fn unmute_snapshot_survives_later_demand_before_drain() {
    let now = Instant::now();
    let (mut engine, tracks) = fixture(2, Duration::ZERO, now);
    for track in &tracks {
        demand(&mut engine, *track, 2, now);
    }
    engine.events.clear();
    engine.set_publisher_muted(&SID.into(), true, now);
    assert!(engine.events.is_empty());
    engine.set_publisher_muted(&SID.into(), false, now);
    demand(&mut engine, tracks[0], 3, now);
    let mut events = Vec::new();
    drain(&mut engine, &mut events);
    assert_eq!(events, vec![expected(&[2, 2]), expected(&[3, 2])]);
}

#[test]
fn queued_snapshot_survives_receiver_removal() {
    let now = Instant::now();
    let (mut engine, tracks) = fixture(2, Duration::ZERO, now);
    demand(&mut engine, tracks[0], 2, now);
    engine.ups.remove(tracks[0].0);
    engine.ups.remove(tracks[1].0);
    engine.codec_receivers.remove(&SID.into());
    engine.published.remove(&SID.into());
    let mut events = Vec::new();
    drain(&mut engine, &mut events);
    assert_eq!(events, vec![expected(&[2, 3])]);
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TimedObservation {
    at_ms: u64,
    events: Vec<Snapshot>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TimedTrace {
    scenario: String,
    codec_count: usize,
    pause_ms: u64,
    observations: Vec<TimedObservation>,
}

fn timed_trace(scenario: &str, count: usize) -> TimedTrace {
    let now = Instant::now();
    let pause_ms = if scenario == "zero-delay" { 0 } else { 1000 };
    let (mut engine, tracks) = fixture(count, Duration::from_millis(pause_ms), now);
    for (index, track) in tracks.iter().enumerate() {
        demand(&mut engine, *track, if index == 0 { 2 } else { 1 }, now);
    }
    engine.events.clear();
    let times: &[u64] = match scenario {
        "shared-expiry" => &[0, 200, 400, 999, 1000, 1200],
        "upgrade-flush" => &[0, 200, 400, 999, 1000, 1399, 1400],
        "return-keeps-deadline" => &[0, 200, 400, 999, 1000, 1400],
        "equal-expiry" => &[0, 200, 999, 1000],
        "zero-delay" => &[0, 200, 400, 1000],
        _ => panic!("unknown scenario"),
    };
    let mut observations = Vec::new();
    for &at_ms in times {
        let at = now + Duration::from_millis(at_ms);
        engine.dynacast_timers(at);
        let change = match (scenario, at_ms) {
            (_, 0) => Some((0, 0)),
            ("shared-expiry" | "zero-delay", 200) => Some((1, 3)),
            ("shared-expiry", 400) if count == 3 => Some((2, 0)),
            ("upgrade-flush", 200) => Some((1, 2)),
            ("upgrade-flush", 400) => Some((if count == 3 { 2 } else { 0 }, 3)),
            ("return-keeps-deadline" | "equal-expiry", 200) => Some((0, 2)),
            ("return-keeps-deadline", 400) => Some((0, 3)),
            ("zero-delay", 400) => Some((if count == 3 { 2 } else { 0 }, 2)),
            _ => None,
        };
        if let Some((codec, quality)) = change {
            demand(&mut engine, tracks[codec], quality, at);
        }
        let mut events = Vec::new();
        drain(&mut engine, &mut events);
        observations.push(TimedObservation { at_ms, events });
    }
    TimedTrace {
        scenario: scenario.into(),
        codec_count: count,
        pause_ms,
        observations,
    }
}

fn assert_timed_trace(scenario: &str, count: usize) {
    let trace = timed_trace(scenario, count);
    for observation in trace.observations {
        let state: Option<Vec<i32>> = match (scenario, observation.at_ms) {
            ("shared-expiry", 1000) => Some(vec![0, 3, 0][..count].to_vec()),
            ("upgrade-flush", 200) => Some(vec![0, 2, 1][..count].to_vec()),
            ("upgrade-flush", 1400) => Some(if count == 2 {
                vec![3, 2]
            } else {
                vec![0, 2, 3]
            }),
            ("return-keeps-deadline", 1000) => Some(vec![3, 1][..count].to_vec()),
            ("equal-expiry", 1000) => Some(vec![2, 1][..count].to_vec()),
            ("zero-delay", 0) => Some(vec![0, 1, 1][..count].to_vec()),
            ("zero-delay", 200) => Some(vec![0, 3, 1][..count].to_vec()),
            ("zero-delay", 400) => Some(if count == 2 {
                vec![2, 3]
            } else {
                vec![0, 3, 2]
            }),
            _ => None,
        };
        assert_eq!(
            observation.events,
            state.map(|s| vec![expected(&s)]).unwrap_or_default(),
            "scenario={scenario} codecs={count} at_ms={}",
            observation.at_ms
        );
    }
}

#[test]
fn shared_debounce_staggered_downgrades_expire_together() {
    for count in [2, 3] {
        assert_timed_trace("shared-expiry", count);
    }
}

#[test]
fn shared_debounce_upgrade_flushes_and_rearms() {
    for count in [2, 3] {
        assert_timed_trace("upgrade-flush", count);
    }
}

#[test]
fn shared_debounce_return_retains_original_deadline() {
    for count in [1, 2] {
        assert_timed_trace("return-keeps-deadline", count);
    }
}

#[test]
fn shared_debounce_equal_expiry_still_notifies() {
    for count in [1, 2] {
        assert_timed_trace("equal-expiry", count);
    }
}

#[test]
fn shared_debounce_zero_delay_control() {
    for count in [2, 3] {
        assert_timed_trace("zero-delay", count);
    }
}

#[test]
fn shared_debounce_deadline_reaches_engine_poll_and_dies_with_publication() {
    let now = Instant::now();
    let (mut engine, tracks) = fixture(2, Duration::from_secs(1), now);
    for track in &tracks {
        demand(&mut engine, *track, 2, now);
    }
    engine.events.clear();
    demand(&mut engine, tracks[0], 0, now);
    demand(&mut engine, tracks[1], 0, now + Duration::from_millis(200));
    assert_eq!(engine.poll_timeout(), Some(now + Duration::from_secs(1)));
    engine.unpublish(&SID.into(), now + Duration::from_millis(500));
    assert!(!engine.codec_receivers.contains_key(&SID.into()));
    assert!(engine.ups.iter().next().is_none());
    engine.events.clear();
    engine.dynacast_timers(now + Duration::from_secs(2));
    assert!(engine.events.is_empty());
    assert_eq!(engine.poll_timeout(), None);
}

#[test]
fn shared_debounce_muted_upgrade_commits_and_cancels_without_emission() {
    let now = Instant::now();
    let (mut engine, tracks) = fixture(2, Duration::from_secs(1), now);
    demand(&mut engine, tracks[0], 2, now);
    demand(&mut engine, tracks[1], 1, now);
    engine.events.clear();
    engine.set_publisher_muted(&SID.into(), true, now);
    demand(&mut engine, tracks[0], 0, now);
    assert_eq!(engine.poll_timeout(), Some(now + Duration::from_secs(1)));
    demand(&mut engine, tracks[1], 2, now + Duration::from_millis(200));
    assert!(
        engine.codec_receivers[&SID.into()]
            .dynacast_commit
            .deadline
            .is_none()
    );
    for (track, quality) in tracks.iter().zip([0, 2]) {
        let msg = engine
            .ups
            .get(track.0)
            .unwrap()
            .dynacast
            .as_ref()
            .unwrap()
            .committed_message()
            .unwrap();
        assert_eq!(
            msg[0].qualities.iter().filter(|q| q.enabled).count(),
            quality + 1
        );
    }
    engine.dynacast_timers(now + Duration::from_secs(1));
    assert!(engine.events.is_empty());
    assert_eq!(
        engine.ups.get(tracks[0].0).unwrap().max_expected_spatial(),
        0
    );
    assert_eq!(
        engine.ups.get(tracks[1].0).unwrap().max_expected_spatial(),
        2
    );
    engine.set_publisher_muted(&SID.into(), false, now + Duration::from_millis(1100));
    assert_eq!(
        engine.ups.get(tracks[0].0).unwrap().max_expected_spatial(),
        0
    );
    assert_eq!(
        engine.ups.get(tracks[1].0).unwrap().max_expected_spatial(),
        2
    );
    let mut snapshots = Vec::new();
    drain(&mut engine, &mut snapshots);
    assert_eq!(snapshots, vec![expected(&[0, 2])]);
}

#[test]
fn shared_debounce_survives_origin_invalidation() {
    let now = Instant::now();
    let (mut engine, tracks) = fixture(2, Duration::from_secs(1), now);
    for track in &tracks {
        demand(&mut engine, *track, 2, now);
    }
    engine.events.clear();
    demand(&mut engine, tracks[0], 0, now);
    demand(&mut engine, tracks[1], 1, now + Duration::from_millis(200));
    engine.invalidate_codec_receiver(tracks[0].0, now + Duration::from_millis(400));
    assert!(engine.ups.get(tracks[0].0).unwrap().dynacast.is_none());
    assert_eq!(engine.poll_timeout(), Some(now + Duration::from_secs(1)));
    engine.dynacast_timers(now + Duration::from_millis(999));
    assert!(engine.events.is_empty());
    engine.dynacast_timers(now + Duration::from_secs(1));
    let mut snapshots = Vec::new();
    drain(&mut engine, &mut snapshots);
    assert_eq!(snapshots, vec![expected(&[3, 1])]);
    assert_eq!(
        engine.ups.get(tracks[1].0).unwrap().max_expected_spatial(),
        1
    );
}

#[test]
fn shared_debounce_updates_each_receiver_expected_layer() {
    for upgrade in [false, true] {
        let now = Instant::now();
        let (mut engine, tracks) = fixture(2, Duration::from_secs(1), now);
        demand(&mut engine, tracks[0], 2, now);
        demand(&mut engine, tracks[1], 1, now);
        demand(&mut engine, tracks[0], 0, now);
        assert_eq!(
            engine.ups.get(tracks[0].0).unwrap().max_expected_spatial(),
            2
        );
        demand(
            &mut engine,
            tracks[1],
            if upgrade { 2 } else { 3 },
            now + Duration::from_millis(200),
        );
        if !upgrade {
            assert_eq!(
                engine.ups.get(tracks[1].0).unwrap().max_expected_spatial(),
                1
            );
            engine.dynacast_timers(now + Duration::from_secs(1));
        }
        assert_eq!(
            engine.ups.get(tracks[0].0).unwrap().max_expected_spatial(),
            0
        );
        assert_eq!(
            engine.ups.get(tracks[1].0).unwrap().max_expected_spatial(),
            if upgrade { 2 } else { -1 }
        );
    }
}

#[test]
fn shared_debounce_first_codec_demand_flushes_pending_peer() {
    let now = Instant::now();
    let (mut engine, tracks) = fixture(1, Duration::from_secs(1), now);
    demand(&mut engine, tracks[0], 2, now);
    engine.events.clear();
    demand(&mut engine, tracks[0], 0, now);
    let mut spec = engine.published[&SID.into()].clone();
    spec.codecs.push(TrackCodec {
        mime_type: "video/vp9".into(),
        ..Default::default()
    });
    engine.published.insert(SID.into(), spec.clone());
    engine.ups.get_mut(tracks[0].0).unwrap().spec = spec.clone();
    let at = now + Duration::from_millis(200);
    let ui = engine.ups.insert(crate::uptrack::UpTrack::new(
        spec,
        "PA_notify".into(),
        TransportId::next(),
        Mid::from("new"),
        TrackKind::Video,
        "video/vp9".into(),
        vec!["q".into(), "h".into(), "f".into()],
        &engine.audio_params,
        Duration::from_secs(1),
        at,
    ));
    engine
        .codec_receivers
        .get_mut(&SID.into())
        .unwrap()
        .bind("video/vp9", ui);
    engine.update_dynacast(ui, at);
    let mut snapshots = Vec::new();
    drain(&mut engine, &mut snapshots);
    assert_eq!(snapshots, vec![expected(&[0, 3])]);
    assert!(
        engine.codec_receivers[&SID.into()]
            .dynacast_commit
            .deadline
            .is_none()
    );
    assert_eq!(
        engine.ups.get(tracks[0].0).unwrap().max_expected_spatial(),
        0
    );
    assert_eq!(engine.ups.get(ui).unwrap().max_expected_spatial(), -1);
    engine.dynacast_timers(now + Duration::from_secs(1));
    assert!(engine.events.is_empty());
}

#[test]
fn publisher_unmute_flushes_pending_once_for_every_codec_count() {
    let now = Instant::now();
    for count in [1, 2, 3] {
        let (mut engine, tracks) = fixture(count, Duration::from_secs(2), now);
        for track in &tracks {
            demand(&mut engine, *track, 2, now);
        }
        engine.events.clear();
        engine.set_publisher_muted(&SID.into(), true, now);
        for track in &tracks {
            demand(&mut engine, *track, 0, now + Duration::from_millis(100));
        }
        assert_eq!(
            engine.poll_timeout(),
            Some(now + Duration::from_millis(2100))
        );
        engine.set_publisher_muted(&SID.into(), false, now + Duration::from_millis(200));
        let mut snapshots = Vec::new();
        drain(&mut engine, &mut snapshots);
        assert_eq!(snapshots, vec![expected(&vec![0; count])]);
        for track in &tracks {
            assert_eq!(engine.ups.get(track.0).unwrap().max_expected_spatial(), 0);
        }
        assert_eq!(engine.poll_timeout(), Some(now + Duration::from_secs(10)));
        engine.dynacast_timers(now + Duration::from_millis(2100));
        assert!(engine.events.is_empty());
    }
}

#[test]
fn publisher_unmute_without_aggregate_preserves_initial_deadline() {
    let now = Instant::now();
    let (mut engine, _) = fixture_with_demand(2, Duration::from_secs(2), now, false);
    for muted in [false, true, false, false] {
        engine.set_publisher_muted(&SID.into(), muted, now);
        assert!(engine.events.is_empty());
        assert_eq!(engine.poll_timeout(), Some(now + Duration::from_secs(10)));
    }
    engine.unpublish(&SID.into(), now);
    let mut removed = Vec::new();
    for event in engine.events.drain(..) {
        let MediaEvent::SubscriptionRemoved {
            subscriber,
            track_sid,
        } = event
        else {
            panic!("unexpected event after unpublish: {event:?}");
        };
        assert_eq!(track_sid, TrackSid::from(SID));
        removed.push(subscriber.to_string());
    }
    removed.sort();
    assert_eq!(removed, ["PA_sub_0", "PA_sub_1"]);
    engine.set_publisher_muted(&SID.into(), false, now);
    assert!(engine.events.is_empty());
    assert_eq!(engine.poll_timeout(), None);
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpectedLayer {
    codec: String,
    spatial: i32,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MuteObservation {
    at_ms: u64,
    events: Vec<Snapshot>,
    expected_layers: Vec<ExpectedLayer>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MuteTrace {
    scenario: String,
    codec_count: usize,
    pause_ms: u64,
    observations: Vec<MuteObservation>,
}

fn publisher_mute_trace(scenario: &str, count: usize) -> MuteTrace {
    let now = Instant::now();
    let pause_ms = if scenario == "zero-muted" { 0 } else { 1000 };
    let (mut engine, tracks) =
        fixture_with_demand(count, Duration::from_millis(pause_ms), now, false);
    for (index, track) in tracks.iter().enumerate() {
        demand(
            &mut engine,
            *track,
            if scenario == "muted-upgrade" && index == 1 {
                1
            } else {
                2
            },
            now,
        );
    }
    engine.events.clear();
    let mut observations = Vec::new();
    for at_ms in [
        0, 100, 200, 300, 1199, 1200, 1499, 1500, 1600, 1700, 1800, 1900, 2000,
    ] {
        let at = now + Duration::from_millis(at_ms);
        engine.dynacast_timers(at);
        match at_ms {
            100 if scenario != "unmuted-control" => {
                engine.set_publisher_muted(&SID.into(), true, at);
            }
            200 => demand(
                &mut engine,
                tracks[0],
                if scenario == "off-muted" { 3 } else { 0 },
                at,
            ),
            300 if scenario == "muted-upgrade" => demand(&mut engine, tracks[1], 2, at),
            1500 | 1600 | 1900 => engine.set_publisher_muted(&SID.into(), false, at),
            1700 | 1800 => engine.set_publisher_muted(&SID.into(), true, at),
            _ => {}
        }
        let mut events = Vec::new();
        drain(&mut engine, &mut events);
        let expected_layers = tracks
            .iter()
            .enumerate()
            .map(|(index, track)| ExpectedLayer {
                codec: CODECS[index].into(),
                spatial: engine.ups.get(track.0).unwrap().max_expected_spatial(),
            })
            .collect();
        observations.push(MuteObservation {
            at_ms,
            events,
            expected_layers,
        });
    }
    MuteTrace {
        scenario: scenario.into(),
        codec_count: count,
        pause_ms,
        observations,
    }
}

fn publisher_mute_matrix() -> Vec<MuteTrace> {
    let mut rows = Vec::new();
    for scenario in [
        "muted-downgrade",
        "muted-upgrade",
        "zero-muted",
        "off-muted",
        "unmuted-control",
    ] {
        for count in [1, 2] {
            if scenario != "muted-upgrade" || count == 2 {
                rows.push(publisher_mute_trace(scenario, count));
            }
        }
    }
    rows
}

fn assert_publisher_mute_trace(scenario: &str, count: usize) {
    let after = if scenario == "off-muted" { 3 } else { 0 };
    for observation in publisher_mute_trace(scenario, count).observations {
        let at = observation.at_ms;
        let committed = at
            >= match scenario {
                "zero-muted" => 200,
                "muted-upgrade" => 300,
                _ => 1200,
            };
        let states = if committed {
            vec![after, 2][..count].to_vec()
        } else if scenario == "muted-upgrade" {
            vec![2, 1]
        } else {
            vec![2; count]
        };
        assert_eq!(
            observation.expected_layers,
            states
                .iter()
                .enumerate()
                .map(|(index, quality)| ExpectedLayer {
                    codec: CODECS[index].into(),
                    spatial: if *quality == 3 { -1 } else { *quality },
                })
                .collect::<Vec<_>>(),
            "scenario={scenario} count={count} at={at} receiver layers"
        );
        let emit =
            matches!(at, 1500 | 1600 | 1900) || (scenario == "unmuted-control" && at == 1200);
        assert_eq!(
            observation.events,
            if emit {
                vec![expected(&states)]
            } else {
                vec![]
            },
            "scenario={scenario} count={count} at={at} publisher events"
        );
    }
}

#[test]
fn publisher_mute_commits_receiver_layers_without_notification() {
    for scenario in ["muted-downgrade", "zero-muted", "off-muted"] {
        for count in [1, 2] {
            assert_publisher_mute_trace(scenario, count);
        }
    }
    assert_publisher_mute_trace("muted-upgrade", 2);
}

#[test]
fn publisher_mute_unmuted_control_and_repeated_force() {
    for count in [1, 2] {
        assert_publisher_mute_trace("unmuted-control", count);
    }
}

#[test]
#[ignore = "requires C58_GO_JSONL and fresh C58_RUST_JSONL"]
fn fixed_go_publisher_mute_differential() {
    use std::io::Write;
    let actual = publisher_mute_matrix();
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var_os("C58_RUST_JSONL").expect("C58_RUST_JSONL"))
        .unwrap();
    for row in &actual {
        writeln!(output, "{}", serde_json::to_string(row).unwrap()).unwrap();
    }
    output.sync_all().unwrap();
    let input =
        std::fs::read_to_string(std::env::var_os("C58_GO_JSONL").expect("C58_GO_JSONL")).unwrap();
    let reference: Vec<MuteTrace> = input
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(reference.len(), 9);
    assert_eq!(actual, reference);
}

#[test]
fn initial_update_preserves_receiver_layer_and_unmute_force() {
    let now = Instant::now();
    let (mut engine, tracks) = fixture(2, Duration::from_secs(2), now);
    for track in &tracks {
        demand(&mut engine, *track, 2, now);
    }
    engine.events.clear();
    engine.set_publisher_muted(&SID.into(), true, now + Duration::from_millis(9400));
    demand(&mut engine, tracks[0], 0, now + Duration::from_millis(9500));
    engine.dynacast_timers(now + Duration::from_secs(10));
    assert!(engine.events.is_empty());
    assert_eq!(
        engine.poll_timeout(),
        Some(now + Duration::from_millis(11500))
    );
    assert_eq!(
        engine.ups.get(tracks[0].0).unwrap().max_expected_spatial(),
        2
    );
    let committed = engine
        .ups
        .get(tracks[0].0)
        .unwrap()
        .dynacast
        .as_ref()
        .unwrap()
        .committed_message()
        .unwrap();
    assert!(committed[0].qualities.iter().all(|q| q.enabled));
    engine.set_publisher_muted(&SID.into(), false, now + Duration::from_millis(10500));
    assert_eq!(
        engine.ups.get(tracks[0].0).unwrap().max_expected_spatial(),
        0
    );
    assert!(
        engine.codec_receivers[&SID.into()]
            .dynacast_commit
            .deadline
            .is_none()
    );
    let mut events = Vec::new();
    drain(&mut engine, &mut events);
    assert_eq!(events, vec![expected(&[0, 2])]);
    engine.dynacast_timers(now + Duration::from_millis(11500));
    assert!(engine.events.is_empty());
}

fn initial_trace(scenario: &str, count: usize) -> TimedTrace {
    let now = Instant::now();
    let pause_ms = if scenario == "zero-delay" { 0 } else { 2000 };
    let (mut engine, tracks) =
        fixture_with_demand(count, Duration::from_millis(pause_ms), now, false);
    for (index, track) in tracks.iter().enumerate() {
        if scenario == "no-demand" || (scenario == "new-codec-initial" && index == 1) {
            continue;
        }
        demand(
            &mut engine,
            *track,
            if scenario == "settled-low" { 0 } else { 2 },
            now,
        );
    }
    engine.events.clear();
    let mut observations = Vec::new();
    for at_ms in [0, 8000, 9000, 9500, 9999, 10000, 10001, 11499, 11500, 12000] {
        let at = now + Duration::from_millis(at_ms);
        engine.dynacast_timers(at);
        let quality = match (scenario, at_ms) {
            ("pending-codue" | "equal-codue", 8000) => Some(0),
            ("equal-codue", 9000) => Some(2),
            ("pending-late" | "new-codec-initial" | "zero-delay", 9500) => Some(0),
            _ => None,
        };
        if let Some(quality) = quality {
            demand(&mut engine, tracks[0], quality, at);
        }
        let mut events = Vec::new();
        drain(&mut engine, &mut events);
        observations.push(TimedObservation { at_ms, events });
    }
    TimedTrace {
        scenario: scenario.into(),
        codec_count: count,
        pause_ms,
        observations,
    }
}

fn assert_initial_trace(scenario: &str, count: usize) {
    for observation in initial_trace(scenario, count).observations {
        let state = match (scenario, observation.at_ms) {
            ("pending-late", 11500) | ("pending-codue", 10000) | ("zero-delay", 9500) => {
                Some(vec![0, 2][..count].to_vec())
            }
            ("equal-codue", 10000) => Some(vec![2; count]),
            ("no-demand", 10000) => Some(vec![3]),
            ("new-codec-initial", 10000) => Some(vec![0, 3]),
            _ => None,
        };
        assert_eq!(
            observation.events,
            state.map(|s| vec![expected(&s)]).unwrap_or_default(),
            "scenario={scenario} codecs={count} at_ms={}",
            observation.at_ms
        );
    }
}

#[test]
fn initial_update_suppresses_settled_demand() {
    for scenario in ["settled-high", "settled-low"] {
        for count in [1, 2] {
            assert_initial_trace(scenario, count);
        }
    }
}

#[test]
fn initial_update_does_not_shorten_pending_downgrade() {
    for count in [1, 2] {
        assert_initial_trace("pending-late", count);
    }
}

#[test]
fn initial_update_coincident_deadline_emits_once() {
    for scenario in ["pending-codue", "equal-codue"] {
        for count in [1, 2] {
            assert_initial_trace(scenario, count);
        }
    }
}

#[test]
fn initial_update_zero_delay_stays_silent() {
    for count in [1, 2] {
        assert_initial_trace("zero-delay", count);
    }
}

#[test]
fn initial_update_without_demand_emits_off() {
    assert_initial_trace("no-demand", 1);
}

#[test]
fn initial_update_new_codec_flushes_pending_peer() {
    assert_initial_trace("new-codec-initial", 2);
}

#[test]
#[ignore = "requires C57_GO_JSONL and fresh C57_RUST_JSONL"]
fn fixed_go_initial_update_differential() {
    use std::io::Write;
    let mut actual: Vec<_> = [
        "settled-high",
        "settled-low",
        "pending-late",
        "pending-codue",
        "equal-codue",
        "zero-delay",
    ]
    .into_iter()
    .flat_map(|scenario| [1, 2].map(move |count| initial_trace(scenario, count)))
    .collect();
    actual.push(initial_trace("no-demand", 1));
    actual.push(initial_trace("new-codec-initial", 2));
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var_os("C57_RUST_JSONL").expect("C57_RUST_JSONL"))
        .unwrap();
    for row in &actual {
        writeln!(output, "{}", serde_json::to_string(row).unwrap()).unwrap();
    }
    output.sync_all().unwrap();
    let input =
        std::fs::read_to_string(std::env::var_os("C57_GO_JSONL").expect("C57_GO_JSONL")).unwrap();
    let reference: Vec<TimedTrace> = input
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(reference.len(), 14);
    assert_eq!(actual, reference);
}

#[test]
#[ignore = "requires C56_GO_JSONL and fresh C56_RUST_JSONL"]
fn fixed_go_shared_debounce_differential() {
    use std::io::Write;
    let actual: Vec<_> = [
        "shared-expiry",
        "upgrade-flush",
        "return-keeps-deadline",
        "equal-expiry",
        "zero-delay",
    ]
    .into_iter()
    .flat_map(|scenario| {
        let counts = if matches!(scenario, "return-keeps-deadline" | "equal-expiry") {
            [1, 2]
        } else {
            [2, 3]
        };
        counts.map(move |count| timed_trace(scenario, count))
    })
    .collect();
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var_os("C56_RUST_JSONL").expect("C56_RUST_JSONL"))
        .unwrap();
    for row in &actual {
        writeln!(output, "{}", serde_json::to_string(row).unwrap()).unwrap();
    }
    output.sync_all().unwrap();
    let input =
        std::fs::read_to_string(std::env::var_os("C56_GO_JSONL").expect("C56_GO_JSONL")).unwrap();
    let reference: Vec<TimedTrace> = input
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(reference.len(), 10);
    assert_eq!(actual, reference);
}

#[test]
#[ignore = "requires C55_GO_JSONL and fresh C55_RUST_JSONL"]
fn fixed_go_notification_snapshot_differential() {
    use std::io::Write;
    let actual: Vec<_> = (1..=3)
        .flat_map(|count| [false, true].map(move |batch| trace(count, batch)))
        .collect();
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var_os("C55_RUST_JSONL").expect("C55_RUST_JSONL"))
        .unwrap();
    for row in &actual {
        writeln!(output, "{}", serde_json::to_string(row).unwrap()).unwrap();
    }
    output.sync_all().unwrap();
    let input =
        std::fs::read_to_string(std::env::var_os("C55_GO_JSONL").expect("C55_GO_JSONL")).unwrap();
    let reference: Vec<Trace> = input
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(reference.len(), 6);
    assert_eq!(actual, reference);
}
