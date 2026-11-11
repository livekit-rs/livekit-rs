//! Public config boundary regression; no server or network started.
#[cfg(unix)]
#[test]
fn token_command_can_read_public_key_file_without_weakening_server_validation() {
    use lk_config::{EntryAction, Env, ProgramInfo};
    use std::ffi::OsString;
    use std::os::unix::fs::PermissionsExt;
    let directory = std::env::temp_dir().join(format!(
        "lk-token-mode-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&directory).unwrap();
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(directory.clone());
    let file = directory.join("keys.yaml");
    std::fs::write(&file, "fixture: fixture-secret-not-for-production\n").unwrap();
    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
    let info = ProgramInfo {
        binary_name: "livecloud".into(),
        product_version: "test".into(),
        compat_version: "1.13.10",
    };
    let parse = |command: bool, body: String| {
        let mut args = vec!["livecloud".to_owned()];
        if command {
            args.extend(
                [
                    "create-join-token",
                    "--room",
                    "fixture-room",
                    "--identity",
                    "fixture-user",
                ]
                .map(str::to_owned),
            );
        }
        args.extend(["--config-body".to_owned(), body]);
        lk_config::parse(
            args.into_iter().map(OsString::from).collect(),
            &Env::default(),
            &info,
        )
    };
    let body = format!("rtc:\n  node_ip: 127.0.0.1\nkey_file: {}\n", file.display());
    let EntryAction::CreateJoinToken {
        config,
        room,
        identity,
        recorder,
    } = parse(true, body.clone()).unwrap()
    else {
        panic!("token action required")
    };
    assert_eq!(
        config
            .official
            .keys
            .0
            .iter()
            .find(|(key, _)| key == "fixture")
            .unwrap()
            .1,
        "fixture-secret-not-for-production"
    );
    assert_eq!(
        (room.as_str(), identity.as_str(), recorder),
        ("fixture-room", "fixture-user", false)
    );
    assert_eq!(
        parse(false, body.clone()).unwrap_err().to_string(),
        "key file others permissions must be set to 0"
    );
    std::fs::write(&file, "").unwrap();
    assert_eq!(parse(true, body.clone()).unwrap_err().to_string(), "EOF");
    std::fs::write(&file, "{}").unwrap();
    assert_eq!(
        parse(true, body).unwrap_err().to_string(),
        "keys are not configured"
    );
    let body = format!(
        "rtc:\n  node_ip: 127.0.0.1\nkey_file: {}\nkeys:\n  inline: inline-fixture-secret\n",
        directory.join("absent").display()
    );
    let EntryAction::CreateJoinToken { config, .. } = parse(true, body).unwrap() else {
        panic!("token action required")
    };
    assert_eq!(
        config
            .official
            .keys
            .0
            .iter()
            .find(|(key, _)| key == "inline")
            .unwrap()
            .1,
        "inline-fixture-secret"
    );
}
