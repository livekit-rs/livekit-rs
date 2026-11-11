use super::*;
use prost::Message;
fn meta(browser: &str, version: &str, os: &str, model: &str) -> ClientMeta {
    ClientMeta {
        browser: browser.into(),
        browser_version: version.into(),
        os: os.into(),
        device_model: model.into(),
        ..Default::default()
    }
}
fn publish_codecs(c: Option<pb::ClientConfiguration>) -> Vec<String> {
    c.and_then(|c| c.disabled_codecs)
        .map(|d| d.publish.into_iter().map(|c| c.mime).collect())
        .unwrap_or_default()
}
#[test]
fn old_public_static_config_safari_minor_threshold() {
    for version in ["18.4", "18.3.1", "18.30", "19.0.0"] {
        assert_eq!(
            publish_codecs(client_configuration(
                &meta("safari", version, "macos", ""),
                false
            )),
            vec!["video/VP9"]
        );
    }
    for version in ["18", "18.3", "18.3.0", "18.3.0-rc.1", "18.3.0+build.01"] {
        assert!(
            publish_codecs(client_configuration(
                &meta("safari", version, "macos", ""),
                false
            ))
            .is_empty()
        );
    }
}
#[test]
fn old_public_static_config_exact_browser_names() {
    for browser in [
        "vendor-safari",
        "safari preview",
        "vendor-firefox",
        "firefoxmobile",
    ] {
        assert!(client_configuration(&meta(browser, "18.4", "linux", ""), false).is_none());
    }
    assert!(client_configuration(&meta("SaFaRi", "18.4", "macos", ""), false).is_some());
}
#[test]
fn old_public_static_config_exact_android_xiaomi_rule() {
    for (browser, os, model) in [
        ("chrome", "linux", "xiaomi 2201117ti"),
        ("chrome", "android", "xiaomi other"),
        ("chrome", "android-x", "xiaomi 2201117ti"),
        ("vendor-firefox", "android", ""),
    ] {
        assert!(client_configuration(&meta(browser, "157", os, model), false).is_none());
    }
    assert_eq!(
        publish_codecs(client_configuration(
            &meta("chrome", "157", "ANDROID", "XIAOMI 2201117TI"),
            false
        )),
        vec!["video/H264"]
    );
    let config =
        client_configuration(&meta("firefox mobile", "156.0", "android", ""), false).unwrap();
    let codecs = config.disabled_codecs.unwrap();
    assert_eq!(codecs.codecs[0].mime, "video/AV1");
    assert_eq!(codecs.publish[0].mime, "video/H264");
}
#[test]
fn old_public_static_config_invalid_version_falls_back_to_original_string() {
    for version in [
        "157rc",
        "157.0rc",
        "999999999999999999999999999999999999",
        "v157.0",
    ] {
        assert!(client_configuration(&meta("firefox", version, "linux", ""), false).is_none());
    }
    assert!(client_configuration(&meta("firefox", "0157", "linux", ""), false).is_some());
}
#[test]
fn old_public_static_config_go_simple_lowercase() {
    assert_eq!(
        publish_codecs(client_configuration(
            &meta("SAFARİ", "18.4", "macos", ""),
            false
        )),
        vec!["video/VP9"]
    );
    assert_eq!(
        publish_codecs(client_configuration(
            &meta("chrome", "157", "ANDROİD", "XİAOMİ 2201117Tİ"),
            false
        )),
        vec!["video/H264"]
    );
}
#[test]
#[ignore = "requires actual original manager/script matrix STATIC_CLIENT_CONFIG_GO_RESULTS"]
fn old_public_static_config_actual_source_matrix() {
    let cases: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("static_client_configuration_cases.json")).unwrap();
    let rows = std::fs::read_to_string(
        std::env::var_os("STATIC_CLIENT_CONFIG_GO_RESULTS").expect("actual source result required"),
    )
    .unwrap();
    let rows: Vec<serde_json::Value> = rows
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    assert_eq!(rows.len(), cases.len());
    let mut seen = std::collections::BTreeSet::new();
    for (case, row) in cases.iter().zip(rows) {
        assert_eq!(
            row.as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>(),
            ["id", "input", "config_present", "config_wire_hex"]
                .into_iter()
                .collect()
        );
        assert_eq!(row["id"], case["id"]);
        assert!(seen.insert(row["id"].as_str().unwrap().to_owned()));
        assert_eq!(row["input"], case.clone());
        assert!(row["config_present"].is_boolean());
        assert!(row["config_wire_hex"].is_string());
        let c = client_configuration(
            &meta(
                case["browser"].as_str().unwrap(),
                case["version"].as_str().unwrap(),
                case["os"].as_str().unwrap(),
                case["model"].as_str().unwrap(),
            ),
            false,
        );
        assert_eq!(row["config_present"], c.is_some(), "case {}", case["id"]);
        let hex = c
            .map(|c| {
                c.encode_to_vec()
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>()
            })
            .unwrap_or_default();
        assert_eq!(
            row["config_wire_hex"], hex,
            "case {} full protobuf result",
            case["id"]
        );
    }
}
