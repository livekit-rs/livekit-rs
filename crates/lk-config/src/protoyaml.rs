//! Protocol YAML is a distinct decoder boundary from ordinary server YAML.
//! Fixed protocol/livekit/types.go re-marshals each RoomConfiguration node;
//! buf.build/go/protoyaml then visits ordered entries, tags and descriptors.
mod decode;
pub(crate) mod float;
mod node;

use node::Value;
use serde_saphyr::granit_parser::ScalarStyle;
use std::collections::HashSet;

use crate::official::RoomConfiguration;
use crate::yamlv3::{self, Doc, Id, Kind};

fn first_unbound_alias(node: &node::Node, names: &mut HashSet<String>) -> Option<String> {
    if !node.anchor.is_empty() {
        names.insert(node.anchor.clone());
    }
    match &node.value {
        Value::Alias(name) if !names.contains(name) => Some(name.clone()),
        Value::Map(entries) => {
            for (k, v) in entries {
                if let Some(name) =
                    first_unbound_alias(k, names).or_else(|| first_unbound_alias(v, names))
                {
                    return Some(name);
                }
            }
            None
        }
        Value::Seq(values) => values.iter().find_map(|v| first_unbound_alias(v, names)),
        _ => None,
    }
}

/// The yaml.v3 node handed to `UnmarshalYAML`, in the protocol node model.
/// Aliases stay aliases (`yaml.Marshal` re-emits `*name`); anchors keep their
/// names. Positions are recomputed by `node::render`.
fn to_protocol_node(doc: &Doc, id: Id) -> node::Node {
    let n = doc.node(id);
    let explicit_tag = (n.style & yamlv3::node_style::TAGGED != 0).then(|| n.tag.clone());
    let value = match n.kind {
        Kind::Mapping => Value::Map(
            n.content
                .chunks(2)
                .map(|kv| (to_protocol_node(doc, kv[0]), to_protocol_node(doc, kv[1])))
                .collect(),
        ),
        Kind::Sequence => Value::Seq(
            n.content
                .iter()
                .map(|c| to_protocol_node(doc, *c))
                .collect(),
        ),
        Kind::Alias => Value::Alias(n.value.clone()),
        _ => {
            let style = if n.style & yamlv3::node_style::DOUBLE_QUOTED != 0 {
                ScalarStyle::DoubleQuoted
            } else if n.style & yamlv3::node_style::SINGLE_QUOTED != 0 {
                ScalarStyle::SingleQuoted
            } else if n.style & yamlv3::node_style::LITERAL != 0 {
                ScalarStyle::Literal
            } else if n.style & yamlv3::node_style::FOLDED != 0 {
                ScalarStyle::Folded
            } else {
                ScalarStyle::Plain
            };
            let tag = explicit_tag.clone().unwrap_or_else(|| {
                if style != ScalarStyle::Plain {
                    "!!str".into()
                } else if matches!(n.value.as_str(), "" | "~" | "null" | "Null" | "NULL") {
                    "!!null".into()
                } else if matches!(
                    n.value.as_str(),
                    "true" | "false" | "True" | "False" | "TRUE" | "FALSE"
                ) {
                    "!!bool".into()
                } else {
                    "!!str".into()
                }
            });
            Value::Scalar(n.value.clone(), style, tag)
        }
    };
    node::Node {
        value,
        line: n.line,
        column: n.column,
        flow: n.style & yamlv3::node_style::FLOW != 0,
        anchor: n.anchor.clone(),
        anchor_id: if n.anchor.is_empty() { 0 } else { id + 1 },
        explicit_tag,
    }
}

/// The validated descriptor projection as a yaml.v3 node tree (exact floats
/// carried as bits), so the ordinary typed decoder can fill the DTO.
fn from_projection(doc: &mut Doc, value: &node::Node) -> Id {
    let mut n = match &value.value {
        Value::Map(entries) => {
            let mut n = yamlv3::Node::new(Kind::Mapping, "!!map", "");
            n.content = entries
                .iter()
                .flat_map(|(k, v)| [from_projection(doc, k), from_projection(doc, v)])
                .collect();
            n
        }
        Value::Seq(values) => {
            let mut n = yamlv3::Node::new(Kind::Sequence, "!!seq", "");
            n.content = values.iter().map(|v| from_projection(doc, v)).collect();
            n
        }
        Value::Float(bits) => {
            let mut n = yamlv3::Node::new(Kind::Scalar, "!!float", "");
            n.float_bits = Some(*bits);
            n
        }
        Value::Scalar(text, _, tag) => yamlv3::Node::new(Kind::Scalar, tag, text),
        Value::Alias(..) => unreachable!("validated projection holds no alias"),
    };
    n.line = value.line;
    n.column = value.column;
    doc.push(n)
}

/// `(*RoomConfiguration).UnmarshalYAML(node)`: `yaml.Marshal(node)` then
/// `protoyaml.Unmarshal`. Errors are protoyaml's text, returned unwrapped.
pub(crate) fn decode_room_configuration(doc: &Doc, id: Id) -> Result<RoomConfiguration, String> {
    let mut resolved = to_protocol_node(doc, id);
    let source = node::render(&mut resolved);
    if let Some(name) = first_unbound_alias(&resolved, &mut HashSet::new()) {
        return Err(format!("yaml: unknown anchor '{name}' referenced"));
    }
    let descriptor = lk_protocol::json::descriptor_pool()
        .get_message_by_name("livekit.RoomConfiguration")
        .expect("fixed protocol descriptor");
    let mut decoder = decode::Decoder {
        errors: Vec::new(),
        source: &source,
    };
    let value = decoder.message(&resolved, &descriptor);
    if !decoder.errors.is_empty() {
        return Err(decoder.errors.join("\n"));
    }
    let mut projection = Doc::default();
    let root = from_projection(&mut projection, &value);
    let mut out = RoomConfiguration::default();
    yamlv3::decode_root(
        &projection,
        root,
        true,
        &mut out,
        "livekit.RoomConfiguration",
    )?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use crate::official::{Config, decode_yaml};

    #[test]
    fn ordinary_duplicate_policy_is_not_relaxed_by_protocol_projection() {
        let mut config = Config::default();
        assert!(decode_yaml("port: 1000\nport: 2000\n", true, &mut config).is_err());
        let yaml = "room:\n  room_configurations:\n    record:\n      egress:\n        room:\n          audio_only: false\n          audio_only: true\n";
        decode_yaml(yaml, true, &mut config).unwrap();
        assert!(
            config.room.room_configurations["record"]
                .as_ref()
                .unwrap()
                .egress
                .as_ref()
                .unwrap()
                .room
                .as_ref()
                .unwrap()
                .audio_only
        );
    }
    #[test]
    fn actual_custom_boundary_keeps_source_alias_enum_and_null_semantics() {
        let mut config = Config::default();
        decode_yaml("room:\n  room_configurations:\n    record:\n      5:\n        room:\n          audioMixing: DUAL_CHANNEL_ALTERNATE\n          layout: null\n",true,&mut config).unwrap();
        let room = config.room.room_configurations["record"]
            .as_ref()
            .unwrap()
            .egress
            .as_ref()
            .unwrap()
            .room
            .as_ref()
            .unwrap();
        assert_eq!(room.audio_mixing, 2);
        assert_eq!(room.layout, "null");
        for invalid in [
            "audio_only: \"true\"",
            "audio_mixing: null",
            "file_outputs: null",
        ] {
            let yaml = format!(
                "room:\n  room_configurations:\n    record:\n      egress:\n        room:\n          {invalid}\n"
            );
            assert!(decode_yaml(&yaml, false, &mut Config::default()).is_err());
        }
    }
    #[test]
    fn source_errors_keep_local_positions_and_order() {
        let yaml = "room:\n  room_configurations:\n    record:\n      egress:\n        room:\n          first_unknown: true\n          audio_only: nope\n";
        let err = decode_yaml(yaml, true, &mut Config::default()).unwrap_err();
        assert_eq!(
            err,
            ":3:9 unknown field \"first_unknown\", expected one of [room_name layout audio_only audio_mixing video_only custom_base_url file ...]\n   3 |         first_unknown: true\n   3 | ........^\n\n:4:21 expected bool, got \"nope\"\n   4 |         audio_only: nope\n   4 | ....................^\n"
        );
    }
}

#[cfg(test)]
mod v2_consumer_tests {
    use crate::netdetect::NetProbe;
    use crate::{EntryAction, Env, LoadedConfig, ProgramInfo};
    use rtc_core::auto_egress::{AdvancedEncoding, RecordingEncoding};
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::Duration;
    struct Local;
    impl NetProbe for Local {
        fn local_addresses(&self) -> Vec<IpAddr> {
            vec![Ipv4Addr::LOCALHOST.into()]
        }
        fn external_ip(&self, _: &[String], _: bool) -> Result<IpAddr, String> {
            Err("test must not discover external addresses".into())
        }
        fn sleep(&self, _: Duration) {
            panic!("test must not start discovery retries");
        }
    }
    fn load(yaml: &str) -> Result<LoadedConfig, crate::ConfigError> {
        let args = ["protoyaml-consumer", "--dev", "--config-body", yaml]
            .into_iter()
            .map(Into::into)
            .collect();
        let info = ProgramInfo {
            binary_name: "protoyaml-consumer".into(),
            product_version: "test".into(),
            compat_version: "1.13.10",
        };
        match crate::parse_with_probe(args, &Env::default(), &info, &Local)? {
            EntryAction::RunServer(config) => Ok(*config),
            _ => panic!("true loader must return server action"),
        }
    }
    fn advanced(config: &LoadedConfig) -> &AdvancedEncoding {
        match config.server.room.presets["record"]
            .as_ref()
            .unwrap()
            .automatic_recording
            .as_ref()
            .unwrap()
            .room
            .as_ref()
            .unwrap()
            .encoding
            .as_ref()
            .unwrap()
        {
            RecordingEncoding::Advanced(value) => value,
            _ => panic!("actual advanced policy required"),
        }
    }
    #[test]
    fn public_loader_neutral_floatbits_keep_nonfinite_and_negative_zero() {
        for (lexical, bits) in [
            ("NaN", 0x7ff8_0000_0000_0001),
            ("Inf", f64::INFINITY.to_bits()),
            ("-Inf", f64::NEG_INFINITY.to_bits()),
            ("-0.0", (-0.0f64).to_bits()),
            ("0.0", 0.0f64.to_bits()),
            ("0x1p-1", 0.5f64.to_bits()),
        ] {
            let yaml = format!(
                "room:\n  room_configurations:\n    record:\n      egress:\n        room:\n          advanced:\n            key_frame_interval: {lexical}\n            audio_quality: 12\n"
            );
            let config = load(&yaml).unwrap();
            assert_eq!(
                advanced(&config).key_frame_interval.0,
                bits,
                "actual neutral projection for {lexical}"
            );
            assert_eq!(advanced(&config).audio_quality, 12);
        }
        for lexical in [".nan", "1e999", "+NaN", "0x1p"] {
            let yaml = format!(
                "room:\n  room_configurations:\n    record:\n      egress:\n        room:\n          advanced:\n            key_frame_interval: {lexical}\n"
            );
            assert!(
                load(&yaml).is_err(),
                "source invalid float must not be accepted: {lexical}"
            );
        }
    }
    #[test]
    fn true_loader_tag_namespace_is_not_guessed_from_suffix() {
        for tag in ["!bool", "!<tag:example.invalid,2026:bool>", "!!str"] {
            let yaml = format!(
                "room:\n  room_configurations:\n    record:\n      egress:\n        room:\n          audio_only: {tag} true\n"
            );
            let error = match load(&yaml) {
                Err(error) => error,
                Ok(_) => panic!("foreign namespace must not grant core bool semantics"),
            };
            assert!(error.to_string().contains("expected tag !!bool, got"));
            assert!(
                error
                    .to_string()
                    .contains(&format!("audio_only: {tag} true")),
                "real node diagnostic retains explicit source tag"
            );
        }
        let config=load("room:\n  room_configurations:\n    record:\n      egress:\n        room:\n          audio_only: !!bool true\n").unwrap();
        assert!(
            config.server.room.presets["record"]
                .as_ref()
                .unwrap()
                .automatic_recording
                .as_ref()
                .unwrap()
                .room
                .as_ref()
                .unwrap()
                .audio_only
        );
    }
    #[test]
    fn outer_alias_resolution_and_external_anchor_consumers_remain_real() {
        let config=load("room:\n  room_configurations:\n    first: &preset\n      metadata: hello\n      egress: {}\n    record: *preset\n").unwrap();
        assert!(
            config.server.room.presets["first"]
                .as_ref()
                .unwrap()
                .automatic_recording
                .is_some()
        );
        assert!(
            config.server.room.presets["record"]
                .as_ref()
                .unwrap()
                .automatic_recording
                .is_some()
        );
        let config=load("room:\n  room_configurations:\n    record:\n      metadata: &ownedmeta hello\nregion: *ownedmeta\n").unwrap();
        assert_eq!(config.server.region, "hello");
        // Preserve original enum spelling for the unrelated ordinary consumer;
        // moving its anchor onto the normalized numeric node would corrupt it.
        let config=load("room:\n  room_configurations:\n    record:\n      egress:\n        participant:\n          preset: &original H264_720P_30\nregion: *original\n").unwrap();
        assert_eq!(config.server.region, "H264_720P_30");
        let invalid = "room:\n  room_configurations:\n    record:\n      egress:\n        participant:\n          file_outputs:\n            - &file {filepath: one}\n            - *file\n";
        assert!(
            match load(invalid) {
                Err(error) => error,
                Ok(_) => panic!("inner alias must be rejected"),
            }
            .to_string()
            .contains("got alias"),
            "inner alias remains protoyaml node semantics"
        );
    }
    #[test]
    fn true_loader_preserves_generated_empty_list_and_default_agent_elements() {
        for body in ["{}", "{agents: []}"] {
            let config = load(&format!(
                "room:\n  room_configurations:\n    record: {body}\n"
            ))
            .unwrap();
            assert!(
                config.server.room.presets["record"]
                    .as_ref()
                    .unwrap()
                    .agent_dispatches
                    .is_none()
            );
        }
        let config=load("room:\n  room_configurations:\n    record: {agents: [{agentName: assistant, restartPolicy: JRP_NEVER}, null]}\n").unwrap();
        let agents = config.server.room.presets["record"]
            .as_ref()
            .unwrap()
            .agent_dispatches
            .as_ref()
            .unwrap();
        assert_eq!(
            agents.len(),
            2,
            "anonymous default entry is a real dispatch specification"
        );
        assert_eq!(agents[0].agent_name, "assistant");
        assert_eq!(agents[0].restart_policy, 1);
        assert!(agents[1].agent_name.is_empty());
        assert_eq!(agents[1].restart_policy, 0);
        assert!(load("room:\n  room_configurations:\n    record: {agents: null}\n").is_err());
    }

    #[test]
    #[ignore = "requires coordinator actual fixed Go float consumer output"]
    fn actual_fixed_go_float_consumer_bits_match() {
        let path = std::env::var("F111_FLOAT_CONSUMER_RESULTS")
            .expect("actual Go consumer output required");
        let rows = std::fs::read_to_string(path).unwrap();
        let mut count = 0;
        for line in rows.lines() {
            let expected: serde_json::Value = serde_json::from_str(line).unwrap();
            let yaml = expected["yaml"].as_str().unwrap();
            let result = load(yaml);
            if let Some(error) = expected["error"].as_str() {
                let actual = match result {
                    Err(error) => error,
                    Ok(_) => panic!("actual Go rejects this same config"),
                };
                assert_eq!(actual.to_string(), error, "{}", expected["name"]);
            } else {
                let config = result.expect("actual Go accepted this same config");
                let value = advanced(&config);
                assert_eq!(
                    format!("{:016x}", value.key_frame_interval.0),
                    expected["bits"].as_str().unwrap(),
                    "{}",
                    expected["name"]
                );
                assert_eq!(
                    i64::from(value.audio_quality),
                    expected["audio_quality"].as_i64().unwrap()
                );
            }
            count += 1;
        }
        assert_eq!(
            count, 47,
            "the independent full input corpus must not be truncated"
        );
    }
}

#[cfg(test)]
mod outer_container_consumer_tests {
    use crate::official::{Config, decode_yaml};
    #[test]
    fn actual_outer_alias_and_merge_containers_keep_descriptor_projection() {
        for yaml in [
            "holder: &r\n  room_configurations:\n    record: {egress: {room: {audioMixing: DUAL_CHANNEL_ALTERNATE}}}\nroom: *r\n",
            "holder: &c\n  record: {egress: {room: {audioMixing: DUAL_CHANNEL_ALTERNATE}}}\nroom: {room_configurations: *c}\n",
            "holder: &r\n  room_configurations:\n    record: {egress: {room: {audioMixing: DUAL_CHANNEL_ALTERNATE}}}\nroom: {<<: *r}\n",
            "holder: &c\n  record: {egress: {room: {audioMixing: DUAL_CHANNEL_ALTERNATE}}}\nroom: {room_configurations: {<<: *c}}\n",
            "holder: &r\n  room:\n    room_configurations:\n      record: {egress: {room: {audioMixing: DUAL_CHANNEL_ALTERNATE}}}\n<<: *r\n",
            "holder: &c\n  record: {egress: {room: {audio_mixing: 0}}}\nroom:\n  room_configurations:\n    <<: *c\n    record: {egress: {room: {audioMixing: DUAL_CHANNEL_ALTERNATE}}}\n",
        ] {
            let mut config = Config::default();
            decode_yaml(yaml, false, &mut config).unwrap();
            let actual = config.room.room_configurations["record"]
                .as_ref()
                .unwrap()
                .egress
                .as_ref()
                .unwrap()
                .room
                .as_ref()
                .unwrap();
            assert_eq!(actual.audio_mixing, 2);
        }
    }
    #[test]
    fn null_collection_tags_and_custom_unmarshal_alias_parse_order_match_source() {
        for yaml in [
            "room: {room_configurations: {record: {egress: !!null {room: {audio_only: true}}}}}\n",
            "room: {room_configurations: {record: {egress: !!null [bad]}}}\n",
        ] {
            let mut config = Config::default();
            decode_yaml(yaml, true, &mut config).unwrap();
            let egress = config.room.room_configurations["record"]
                .as_ref()
                .unwrap()
                .egress
                .as_ref()
                .unwrap();
            assert!(
                egress.room.is_none() && egress.participant.is_none() && egress.tracks.is_none()
            );
        }
        let mut config = Config::default();
        assert_eq!(decode_yaml("holder: &x {audio_only: true}\nroom: {room_configurations: {record: {egress: {room: {<<: *x}}}}}\n",false,&mut config).unwrap_err(),"yaml: unknown anchor 'x' referenced");
        assert_eq!(decode_yaml("port: 1000\nroom: {room_configurations: {record: {egress: {room: {audio_only: nope}}}}}\nport: 2000\n",true,&mut Config::default()).unwrap_err(),"yaml: unmarshal errors:\n  line 3: mapping key \"port\" already defined at line 1");
    }
}
