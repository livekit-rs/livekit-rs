// Included inside c61_video_quality_publish to reuse its strict input/model projections.
mod c63_publication_metadata {
    use super::*;
    use crate::events::EngineEvent;
    use crate::media_api::PublicationRegistration;

    strict_object!(PeerCodec {
        mime_type: String,
        clock_rate: u32,
        payload_type: u8,
    });
    strict_object!(Rtp {
        count: u32,
        sequence_start: u16,
        timestamp_start: u32,
        timestamp_step: u32,
        marker: bool,
        payload_hex: String,
        interval_ms: u32,
    });
    strict_object!(MetadataInput {
        fire_on_track_by_sdp: bool,
        request: Request,
        enabled_codecs: Vec<EnabledCodec>,
        browser: String,
        protocol_version: i32,
        peer_codec: PeerCodec,
        sdp_ssrc: u32,
        sdp_mid: String,
        sdp_rids: Vec<String>,
        sdp_simulcast: bool,
        rtp: Rtp,
    });
    strict_object!(MetadataCase {
        schema_version: u32,
        case: String,
        input: MetadataInput,
    });
    // Go uses protobuf names, not C61's mime/layer_mode aliases.
    strict_object!(MetadataCodec {
        mime_type: String,
        cid: String,
        mid: String,
        sdp_cid: String,
        video_layer_mode: i32,
        layers: Vec<Layer>,
    });
    strict_object!(MetadataTrack {
        name: String,
        r#type: i32,
        width: u32,
        height: u32,
        muted: bool,
        disable_dtx: bool,
        source: i32,
        layers: Vec<Layer>,
        stereo: bool,
        disable_red: bool,
        encryption: i32,
        stream: String,
        backup_codec_policy: i32,
        audio_features: Vec<i32>,
        packet_trailer_features: Vec<i32>,
        simulcast: bool,
        mime_type: String,
        mid: String,
        codecs: Vec<MetadataCodec>,
    });
    strict_object!(MetadataOutput {
        ack: MetadataTrack,
        callback: MetadataTrack,
        callback_before_rtp: bool,
        publication_count: u32,
        settled: MetadataTrack,
    });
    strict_object!(MetadataObservation {
        schema_version: u32,
        case: String,
        input: MetadataInput,
        output: MetadataOutput,
    });

    impl From<&TrackInfo> for MetadataTrack {
        fn from(track: &TrackInfo) -> Self {
            // Exhaustive C61 projection excludes only SID/version. Destructure without
            // '..' so newly projected deterministic fields cannot silently disappear.
            let Track {
                name,
                r#type,
                width,
                height,
                muted,
                disable_dtx,
                source,
                layers,
                stereo,
                disable_red,
                encryption,
                stream,
                backup_codec_policy,
                audio_features,
                packet_trailer_features,
                simulcast,
                mime_type,
                mid,
                codecs,
            } = Track::from(track);
            Self {
                name,
                r#type,
                width,
                height,
                muted,
                disable_dtx,
                source,
                layers,
                stereo,
                disable_red,
                encryption,
                stream,
                backup_codec_policy,
                audio_features,
                packet_trailer_features,
                simulcast,
                mime_type,
                mid,
                codecs: codecs
                    .into_iter()
                    .map(|codec| {
                        let Codec {
                            mime,
                            cid,
                            mid,
                            sdp_cid,
                            layer_mode,
                            layers,
                        } = codec;
                        MetadataCodec {
                            mime_type: mime,
                            cid,
                            mid,
                            sdp_cid,
                            video_layer_mode: layer_mode,
                            layers,
                        }
                    })
                    .collect(),
            }
        }
    }

    fn canonical_cases() -> [MetadataCase; 12] {
        let cases: [MetadataCase; 12] = serde_json::from_str(include_str!(
            "../../../../../tests/oracles/video-publication-metadata/cases.json"
        ))
        .expect("strict canonical C63 input rows");
        for (case, (name, sdp, rid)) in cases.iter().zip([
            ("sdp-signalled-f", true, "f"),
            ("sdp-empty-rid", true, ""),
            ("rtp-signalled-f", false, "f"),
            ("rtp-empty-rid", false, ""),
        ]) {
            assert_eq!(case.schema_version, 1);
            assert_eq!(case.case, name, "canonical identity/order");
            assert_eq!(case.input.fire_on_track_by_sdp, sdp);
            assert_eq!(case.input.request.layers.len(), 1);
            assert_eq!(case.input.request.layers[0].rid, rid);
            assert_eq!(case.input.request.simulcast_codecs.len(), 1);
            assert_eq!(case.input.request.simulcast_codecs[0].layers.len(), 1);
            assert_eq!(case.input.request.simulcast_codecs[0].layers[0].rid, rid);
            assert!(case.input.sdp_rids.is_empty());
            assert!(!case.input.sdp_simulcast);
            assert_ne!(case.input.sdp_ssrc, 0);
            assert_ne!(case.input.rtp.count, 0);
        }
        for (case, (name, early, qualities)) in cases[4..].iter().zip([
            ("c64-sdp-low-high", true, vec![0, 2]),
            ("c64-rtp-low-high", false, vec![0, 2]),
            ("c64-sdp-medium-high", true, vec![1, 2]),
            ("c64-rtp-medium-high", false, vec![1, 2]),
            ("c64-sdp-low-low-high", true, vec![0, 0, 2]),
            ("c64-rtp-low-low-high", false, vec![0, 0, 2]),
            ("c64-sdp-low-high-99", true, vec![0, 2, 99]),
            ("c64-rtp-low-high-99", false, vec![0, 2, 99]),
        ]) {
            assert_eq!(case.schema_version, 1);
            assert_eq!(case.case, name);
            assert_eq!(case.input.fire_on_track_by_sdp, early);
            assert_eq!(
                case.input
                    .request
                    .layers
                    .iter()
                    .map(|l| l.quality)
                    .collect::<Vec<_>>(),
                qualities
            );
            assert_eq!(case.input.request.simulcast_codecs.len(), 1);
            assert_eq!(
                case.input.request.layers,
                case.input.request.simulcast_codecs[0].layers
            );
            assert!(case.input.sdp_rids.is_empty());
            assert!(!case.input.sdp_simulcast);
            assert_ne!(case.input.sdp_ssrc, 0);
            assert_ne!(case.input.rtp.count, 0);
        }
        cases
    }

    fn publications(effects: &[Effect], publisher: &ParticipantSid) -> Vec<TrackInfo> {
        effects
            .iter()
            .filter_map(|effect| match effect {
                Effect::Event(EngineEvent::TrackPublished {
                    participant, track, ..
                }) => {
                    assert_eq!(&participant.sid, publisher);
                    Some(track.clone())
                }
                _ => None,
            })
            .collect()
    }

    // CORE CONSUMER differential only: no SDP parser, native receiver or RTP wire
    // is exercised here. The native producer is tested separately. Receiver events
    // below are derived solely from canonical INPUT, never from Go OUTPUT.
    fn observe_metadata(case: &MetadataCase) -> MetadataObservation {
        let input = &case.input;
        let mut policy = test_policy();
        policy.enabled_codecs = input
            .enabled_codecs
            .iter()
            .map(|codec| CodecSpec {
                mime: codec.mime.clone(),
                fmtp_line: codec.fmtp_line.clone(),
            })
            .collect();
        let now = Instant::now();
        let mut room = Room::new(
            RoomKey::new(ProjectId::local(), "r1"),
            Arc::new(policy),
            Arc::new(EchoData),
            &Default::default(),
            now,
        );
        room.take_effects();
        let mut join = params("c63", input.protocol_version, false);
        join.client.browser = input.browser.clone();
        join.capabilities.video_publication_requires_rtp = false;
        join.capabilities.incomplete_rtcp_sender_reports = input.browser.contains("OBS");
        join.grants.video.can_publish = Some(true);
        join.grants.video.can_subscribe = Some(false);
        join.grants.video.can_publish_data = Some(false);
        join.auto_subscribe = false;
        let publisher = room.join(join, now).expect("C63 publisher joins");
        let effects = room.take_effects();
        let transport = transport_of(&effects, &publisher.sid, PcRole::Publisher);
        assert!(publications(&effects, &publisher.sid).is_empty());
        let request = input.request.to_model();
        assert_eq!(
            Request::from(&request),
            input.request,
            "full input projection"
        );
        room.handle_request(
            &publisher.sid,
            publisher.generation,
            SessionRequest::AddTrack(request.clone()),
            now,
        );
        let effects = room.take_effects();
        let signals = signals_to(&effects, &publisher.sid);
        assert_eq!(signals.len(), 1, "{} AddTrack signal count", case.case);
        let SignalEvent::TrackPublished { cid, track: ack } = &signals[0] else {
            panic!("{} missing actual AddTrack ACK", case.case);
        };
        assert_eq!(cid, &request.cid);
        assert!(!ack.sid.to_string().is_empty());
        let expected = effects
            .iter()
            .filter_map(|effect| match effect {
                Effect::Media(MediaCommand::ExpectTrack { transport, track }) => {
                    Some((transport, track))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(expected.len(), 1);
        assert_eq!(*expected[0].0, transport);
        assert_eq!(expected[0].1.sid, ack.sid);
        assert_eq!(expected[0].1.cid, request.cid);
        assert!(publications(&effects, &publisher.sid).is_empty());
        assert_eq!(room.published_track_count(), 0, "ACK is not publication");
        assert!(room.participants[&publisher.sid].info.tracks.is_empty());
        let ack_projection = MetadataTrack::from(ack);

        room.handle_media_event(
            MediaEvent::RemoteMediaAdded(RemoteMedia {
                registration: if input.fire_on_track_by_sdp {
                    PublicationRegistration::SdpReceiver
                } else {
                    PublicationRegistration::Pending
                },
                receiver_ssrc: input.fire_on_track_by_sdp.then_some((input.sdp_ssrc, 0)),
                participant: publisher.sid.clone(),
                transport,
                mid: input.sdp_mid.clone(),
                kind: request.kind,
                msid_track_id: request.cid.clone(),
                rids: input.sdp_rids.clone(),
                mime_type: input.peer_codec.mime_type.clone(),
            }),
            now,
        );
        let effects = room.take_effects();
        assert!(effects.iter().any(|effect| matches!(
            effect,
            Effect::Media(MediaCommand::BindTrack { transport: bound, mid, track_sid })
                if *bound == transport && mid == &input.sdp_mid && track_sid == &ack.sid
        )));
        assert!(publications(&effects, &publisher.sid).is_empty());
        assert_eq!(
            room.published_track_count(),
            0,
            "not visible before connection"
        );
        room.handle_media_event(
            MediaEvent::TransportConnected {
                participant: publisher.sid.clone(),
                transport,
            },
            now,
        );
        let mut callbacks = publications(&room.take_effects(), &publisher.sid);
        let callback_before_rtp = !callbacks.is_empty();
        assert_eq!(callback_before_rtp, input.fire_on_track_by_sdp);
        assert_eq!(room.published_track_count(), callbacks.len());

        // Synthetic receiver metadata is derived from INPUT, not Go observations.
        let mut receiver_layers: Vec<_> =
            input.request.layers.iter().map(Layer::to_model).collect();
        let mut qualities: Vec<_> = receiver_layers
            .iter()
            .filter_map(|layer| layer.quality.spatial_index())
            .collect();
        qualities.sort_unstable();
        qualities.dedup();
        for layer in &mut receiver_layers {
            layer.rid.clear();
            layer.spatial_layer = layer
                .quality
                .spatial_index()
                .and_then(|quality| qualities.iter().position(|candidate| *candidate == quality))
                .map(|index| index as i32)
                .unwrap_or(-1);
        }
        if let Some(layer) = receiver_layers
            .iter_mut()
            .find(|layer| layer.spatial_layer == 0)
        {
            if layer.ssrc == 0 {
                layer.ssrc = input.sdp_ssrc;
            }
        }
        room.handle_media_event(
            MediaEvent::TrackMediaStarted {
                track_sid: ack.sid.clone(),
                mime_type: input.peer_codec.mime_type.clone(),
                layers: receiver_layers,
            },
            now + Duration::from_millis(u64::from(input.rtp.interval_ms)),
        );
        callbacks.extend(publications(&room.take_effects(), &publisher.sid));
        // Advance the core clock through the stimulus window, not synthetic RTP
        // packets: packet bytes/count and codec PT/clock rate belong to the producer.
        room.tick(
            now + Duration::from_millis(
                u64::from(input.rtp.interval_ms) * u64::from(input.rtp.count),
            ),
        );
        callbacks.extend(publications(&room.take_effects(), &publisher.sid));
        assert_eq!(
            callbacks.len(),
            1,
            "{} exactly one public callback",
            case.case
        );
        assert_eq!(callbacks[0].sid, ack.sid);
        assert_eq!(room.published_track_count(), 1);
        let settled = &room.participants[&publisher.sid].info.tracks;
        assert_eq!(settled.len(), 1);
        assert_eq!(settled[0].sid, ack.sid);
        assert_eq!(
            MetadataTrack::from(ack),
            ack_projection,
            "ACK snapshot is immutable"
        );
        assert_eq!(Request::from(&request), input.request);
        MetadataObservation {
            schema_version: case.schema_version,
            case: case.case.clone(),
            input: input.clone(),
            output: MetadataOutput {
                ack: ack_projection,
                callback: MetadataTrack::from(&callbacks[0]),
                callback_before_rtp,
                publication_count: u32::try_from(callbacks.len()).unwrap(),
                settled: MetadataTrack::from(&settled[0]),
            },
        }
    }

    fn compare_rows(
        rust: &[MetadataObservation],
        go: &[MetadataObservation],
        cases: &[MetadataCase; 12],
    ) {
        assert_eq!(rust.len(), cases.len(), "exact Rust membership");
        assert_eq!(go.len(), cases.len(), "exact Go membership");
        for (rows, label) in [(rust, "Rust"), (go, "Go")] {
            for (row, case) in rows.iter().zip(cases) {
                assert_eq!(row.schema_version, case.schema_version, "{label} schema");
                assert_eq!(row.case, case.case, "{label} identity/order");
                assert_eq!(row.input, case.input, "{label} full canonical input");
            }
        }
        assert_eq!(rust, go, "full actual core-consumer rows versus raw Go");
    }

    #[test]
    #[ignore = "core consumer only; requires raw C63_GO_RESULTS and a new absolute C63_RUST_RESULTS"]
    fn core_consumer_matches_raw_go_publication_metadata() {
        let cases = canonical_cases();
        let rust_path =
            PathBuf::from(std::env::var_os("C63_RUST_RESULTS").expect("C63_RUST_RESULTS required"));
        let go_path =
            PathBuf::from(std::env::var_os("C63_GO_RESULTS").expect("C63_GO_RESULTS required"));
        assert!(rust_path.is_absolute(), "C63_RUST_RESULTS must be absolute");
        assert!(go_path.is_absolute(), "C63_GO_RESULTS must be absolute");
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&rust_path)
            .expect("exclusively create C63_RUST_RESULTS");
        let rust = cases.iter().map(observe_metadata).collect::<Vec<_>>();
        let mut writer = BufWriter::new(file);
        for row in &rust {
            serde_json::to_writer(&mut writer, row).expect("write actual Rust row");
            writer.write_all(b"\n").expect("write JSONL newline");
        }
        writer
            .flush()
            .expect("flush ALL Rust rows before opening Go");
        writer
            .get_ref()
            .sync_all()
            .expect("persist actual Rust rows");
        drop(writer);

        // No Go file is opened until every actual Rust observation is persisted.
        let raw = std::fs::read_to_string(&go_path).expect("read external raw C63_GO_RESULTS");
        let go = raw
            .lines()
            .enumerate()
            .map(|(index, line)| {
                serde_json::from_str::<MetadataObservation>(line)
                    .unwrap_or_else(|error| panic!("C63 Go line {}: {error}", index + 1))
            })
            .collect::<Vec<_>>();
        compare_rows(&rust, &go, &cases);
    }

    // Exercise every concrete object reader, including reused nested C61 types.
    // Mutations are parser controls, not expected publication metadata.
    fn strict_controls<T: Serialize + serde::de::DeserializeOwned>(object: &T) {
        let value = serde_json::to_value(object).unwrap();
        let fields = value.as_object().unwrap();
        let encode = |value: &serde_json::Value| serde_json::to_string(value).unwrap();
        assert!(serde_json::from_str::<T>(&encode(&value)).is_ok());
        let rejects = |raw: &str| {
            assert!(
                serde_json::from_str::<T>(raw).is_err(),
                "accepted invalid object: {raw}"
            );
        };
        rejects("null");
        rejects(&encode(&serde_json::Value::Array(
            fields.values().cloned().collect(),
        )));
        let mut unknown = fields.clone();
        unknown.insert("unexpected".into(), serde_json::Value::Bool(true));
        rejects(&encode(&serde_json::Value::Object(unknown)));
        for (key, field) in fields {
            let mut missing = fields.clone();
            missing.remove(key);
            rejects(&encode(&serde_json::Value::Object(missing)));
            let raw = encode(&value);
            let duplicate = format!(
                "{{{}:{},{}",
                serde_json::to_string(key).unwrap(),
                encode(field),
                &raw[1..]
            );
            rejects(&duplicate);
            let mut wrong = fields.clone();
            wrong.insert(key.clone(), serde_json::Value::Null);
            rejects(&encode(&serde_json::Value::Object(wrong)));
            if field.is_number() {
                for replacement in [
                    serde_json::json!(true),
                    serde_json::json!("1"),
                    serde_json::json!(1.5),
                    serde_json::json!(u64::MAX),
                    serde_json::json!(i64::MIN),
                ] {
                    let mut wrong = fields.clone();
                    wrong.insert(key.clone(), replacement);
                    rejects(&encode(&serde_json::Value::Object(wrong)));
                }
            }
        }
    }

    #[test]
    fn core_lifecycle_and_strict_metadata_schema_without_go() {
        for case in canonical_cases() {
            let row = observe_metadata(&case);
            assert_eq!(
                row.output.callback, row.output.settled,
                "stable public snapshot"
            );
            strict_controls(&case);
            strict_controls(&row);
            strict_controls(&row.input);
            strict_controls(&row.input.peer_codec);
            strict_controls(&row.input.rtp);
            strict_controls(&row.input.request);
            for codec in &row.input.enabled_codecs {
                strict_controls(codec);
            }
            for layer in &row.input.request.layers {
                strict_controls(layer);
            }
            for codec in &row.input.request.simulcast_codecs {
                strict_controls(codec);
                for layer in &codec.layers {
                    strict_controls(layer);
                }
            }
            strict_controls(&row.output);
            for track in [&row.output.ack, &row.output.callback, &row.output.settled] {
                strict_controls(track);
                for layer in &track.layers {
                    strict_controls(layer);
                }
                for codec in &track.codecs {
                    strict_controls(codec);
                    for layer in &codec.layers {
                        strict_controls(layer);
                    }
                }
            }
            // Narrow protocol integer boundaries cannot be widened accidentally.
            assert!(
                serde_json::from_str::<PeerCodec>(
                    r#"{"mime_type":"video/VP8","clock_rate":90000,"payload_type":256}"#
                )
                .is_err()
            );
            let invalid_sequence = serde_json::to_string(&row.input.rtp)
                .unwrap()
                .replace("\"sequence_start\":1000", "\"sequence_start\":65536");
            assert!(serde_json::from_str::<Rtp>(&invalid_sequence).is_err());
        }
    }
}
