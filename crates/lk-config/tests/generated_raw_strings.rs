//! Old public loader regression; no server/network service is launched.
#![cfg(unix)]
use lk_config::{EntryAction, Env, ProgramInfo, parse_with_probe};
use std::{ffi::OsString, net::IpAddr, os::unix::ffi::OsStringExt, time::Duration};
#[cfg(target_os = "linux")]
use std::{io::Write, os::unix::fs::OpenOptionsExt};
struct Local;
impl lk_config::netdetect::NetProbe for Local {
    fn local_addresses(&self) -> Vec<IpAddr> {
        vec!["127.0.0.1".parse().unwrap()]
    }
    fn external_ip(&self, _: &[String], _: bool) -> Result<IpAddr, String> {
        panic!("no external discovery expected")
    }
    fn sleep(&self, _: Duration) {
        panic!("no retry sleep expected")
    }
}
fn info() -> ProgramInfo {
    ProgramInfo {
        binary_name: "livekit-server".into(),
        product_version: "0.1.0".into(),
        compat_version: "1.13.10",
    }
}
#[test]
fn raw_generated_cli_and_environment_strings_do_not_fail_unused_ports_consumer() {
    for env_mode in [false, true] {
        let mut args = vec![OsString::from("livekit-server")];
        let env = if env_mode {
            Env::from_os_pairs([("LIVEKIT_LOG_LEVEL", OsString::from_vec(vec![0xff]))])
        } else {
            args.extend([
                OsString::from("--log_level"),
                OsString::from_vec(vec![0xff]),
            ]);
            Env::default()
        };
        args.push("ports".into());
        assert!(matches!(
            parse_with_probe(args, &env, &info(), &Local).unwrap(),
            EntryAction::Ports(_)
        ));
    }
}
#[cfg(target_os = "linux")]
#[test]
fn raw_generated_key_file_reaches_actual_run_key_loader_without_loss() {
    let dir = std::env::temp_dir().join(format!(
        "generated-raw-key-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&dir).unwrap();
    struct Owned(std::path::PathBuf);
    impl Drop for Owned {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let owner = Owned(dir);
    let key = owner.0.join(OsString::from_vec(b"key-\xff.yaml".to_vec()));
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&key)
        .unwrap();
    file.write_all(b"APIrawowned: unique-owned-raw-file-secret-at-least-32-bytes\n")
        .unwrap();
    drop(file);
    for env_mode in [false, true] {
        let mut args = vec![OsString::from("livekit-server")];
        let env = if env_mode {
            Env::from_os_pairs([("LIVEKIT_KEY_FILE", key.clone().into_os_string())])
        } else {
            args.extend([OsString::from("--key_file"), key.clone().into_os_string()]);
            Env::default()
        };
        let EntryAction::RunServer(config) = parse_with_probe(args, &env, &info(), &Local).unwrap()
        else {
            panic!("real Run consumer required")
        };
        assert_eq!(config.key_file.as_ref(), Some(&key));
        assert_eq!(
            config.server.keys,
            vec![(
                "APIrawowned".into(),
                "unique-owned-raw-file-secret-at-least-32-bytes".into()
            )]
        );
    }
}
