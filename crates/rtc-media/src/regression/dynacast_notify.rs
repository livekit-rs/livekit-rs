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
        engine.update_dynacast(ui, now);
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
fn timer_commits_capture_each_receiver_before_the_next_tick() {
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
    assert_eq!(events, vec![expected(&[0, 2]), expected(&[0, 0])]);
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
    assert_eq!(
        events,
        vec![expected(&[2, 2]), expected(&[2, 2]), expected(&[3, 2])]
    );
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
