//! Lossless POSIX CLI input/path boundary. Missing HOME uses real source OS lookup.
//! Quote delegates valid Unicode to the unchanged F51 source-bound tables.
use super::*;
use std::ffi::OsStr;
use std::path::PathBuf;
#[cfg(any(target_os = "macos", target_os = "linux"))]
#[path = "home_dir.rs"]
mod home_dir;

pub(super) fn invalid_value(kind: crate::generated::Kind, raw: &[u8]) -> String {
    use crate::generated::Kind;
    let quoted = quote::quote_bytes(raw);
    match kind {
        Kind::Bool => "parse error".to_owned(),
        Kind::Int => format!("strconv.ParseInt: parsing {quoted}: invalid syntax"),
        Kind::Uint | Kind::Uint64 => format!("strconv.ParseUint: parsing {quoted}: invalid syntax"),
        Kind::Float => format!("strconv.ParseFloat: parsing {quoted}: invalid syntax"),
        Kind::Duration => format!("time: invalid duration {quoted}"),
        Kind::Str => "non-Unicode generated string projection is not implemented".to_owned(),
    }
}
pub(super) fn usage(info: &ProgramInfo, message: &[u8], command: Option<&str>) -> EntryAction {
    if let Ok(message) = std::str::from_utf8(message) {
        return flag_usage_error(info, message, command);
    }
    let EntryAction::PrintHelp { text, .. } = flag_usage_error(info, "", command) else {
        unreachable!()
    };
    let help = text.split_once("\n\n").map(|(_, help)| help).unwrap_or("");
    let mut stdout = help.as_bytes().to_vec();
    stdout.extend_from_slice(message);
    stdout.push(b'\n');
    let mut stderr = b"Incorrect Usage: ".to_vec();
    stderr.extend_from_slice(message);
    stderr.extend_from_slice(b"\n\n");
    EntryAction::PrintBytes {
        stdout,
        stderr,
        exit_code: 1,
    }
}
pub(super) fn preflight(args: &[OsString], info: &ProgramInfo) -> Result<(), EntryAction> {
    let boundary = child_boundary(args);
    let command = boundary
        .and_then(|(at, _)| args[at].to_str())
        .map(|c| if c == "h" { "help" } else { c });
    let mut at = boundary.map_or(1, |(at, _)| at + 1);
    let mut help_set = false;
    while at < args.len() {
        let original = args[at].as_encoded_bytes();
        // Go TrimSpace for valid UTF8; invalid bytes are not replacement characters.
        let token = args[at]
            .to_str()
            .map(str::trim)
            .map(str::as_bytes)
            .unwrap_or(original);
        if token == b"--" || token == b"-" {
            break;
        }
        if !token.starts_with(b"-") {
            at += 1;
            continue;
        }
        if !token.starts_with(b"--")
            && !token.get(1).is_some_and(|b| char::from(*b).is_alphabetic())
        {
            break;
        }
        let spelling = if token.starts_with(b"--") {
            &token[2..]
        } else {
            &token[1..]
        };
        let (name, inline) = match spelling.iter().position(|b| *b == b'=') {
            Some(n) => (&spelling[..n], Some(&spelling[n + 1..])),
            None => (spelling, None),
        };
        let valid_name = std::str::from_utf8(name).ok();
        let canonical = valid_name.map(|name| match name {
            "h" => "help",
            "v" => "version",
            v => v,
        });
        let builtin = canonical.is_some_and(|name| {
            (name == "help" && command != Some("help")) || (name == "version" && command.is_none())
        });
        let kind = if builtin {
            Some(crate::generated::Kind::Bool)
        } else {
            canonical.and_then(|name| {
                if command == Some("create-join-token") && name == "recorder" {
                    return Some(crate::generated::Kind::Bool);
                }
                BASE_FLAGS
                    .iter()
                    .find(|f| f.name == name)
                    .map(|f| {
                        if f.kind == BaseKind::Bool {
                            crate::generated::Kind::Bool
                        } else {
                            crate::generated::Kind::Str
                        }
                    })
                    .or_else(|| specs().iter().find(|s| s.path == name).map(|s| s.kind))
                    .or_else(|| {
                        (command == Some("create-join-token")
                            && matches!(name, "room" | "identity"))
                        .then_some(crate::generated::Kind::Str)
                    })
            })
        };
        let failure = if let Some(kind) = kind {
            let raw = if kind == crate::generated::Kind::Bool {
                inline.unwrap_or(b"true")
            } else if let Some(value) = inline {
                value
            } else if let Some(value) = args.get(at + 1) {
                at += 1;
                value.as_encoded_bytes()
            } else {
                let mut msg = b"flag needs an argument: ".to_vec();
                msg.extend_from_slice(original);
                return Err(if help_set {
                    stage_help(info, command)
                } else {
                    usage(info, &msg, command)
                });
            };
            if kind == crate::generated::Kind::Str {
                None
            } else {
                let parsed = match std::str::from_utf8(raw) {
                    Ok(raw) if kind == crate::generated::Kind::Bool => parse_cli_bool(raw)
                        .map(Value::Bool)
                        .map_err(|_| "parse error".to_owned()),
                    Ok(raw) => parse_value(kind, raw),
                    Err(_) => Err(invalid_value(kind, raw)),
                };
                match parsed {
                    Ok(Value::Bool(value)) if canonical == Some("help") => {
                        help_set = value;
                        None
                    }
                    Ok(_) => None,
                    Err(error) => Some(
                        format!(
                            "invalid value {} for flag -{}: {error}",
                            quote::quote_bytes(raw),
                            valid_name.expect("known name")
                        )
                        .into_bytes(),
                    ),
                }
            }
        } else {
            let mut msg = b"flag provided but not defined: -".to_vec();
            msg.extend_from_slice(name);
            Some(msg)
        };
        if let Some(message) = failure {
            return Err(if help_set {
                stage_help(info, command)
            } else {
                usage(info, &message, command)
            });
        }
        at += 1;
    }
    Ok(())
}

/// Go POSIX filepath.Join + filepath.Clean contract for home expansion.
/// Go source BSD-3-Clause attribution is retained in LICENSES/Go-BSD-3-Clause.txt. This is lexical:
/// no filesystem lookup, and backslash remains an ordinary filename byte.
#[cfg(unix)]
pub(crate) fn join_home_posix(home: &OsStr, suffix: &[u8]) -> PathBuf {
    use std::os::unix::ffi::OsStringExt;
    let home = home.as_encoded_bytes();
    let mut joined = home.to_vec();
    if !suffix.is_empty() {
        if !joined.is_empty() {
            joined.push(b'/');
        }
        joined.extend_from_slice(suffix);
    }
    if joined.is_empty() {
        return PathBuf::new();
    }
    let rooted = joined[0] == b'/';
    let mut parts: Vec<&[u8]> = Vec::new();
    for part in joined.split(|byte| *byte == b'/') {
        match part {
            b"" | b"." => {}
            b".." if parts.last().is_some_and(|last| *last != b"..") => {
                parts.pop();
            }
            b".." if rooted => {}
            _ => parts.push(part),
        }
    }
    let mut clean = Vec::new();
    if rooted {
        clean.push(b'/');
    }
    for part in parts {
        if !clean.is_empty() && clean.last() != Some(&b'/') {
            clean.push(b'/');
        }
        clean.extend_from_slice(part);
    }
    if clean.is_empty() {
        clean.push(b'.');
    }
    PathBuf::from(OsString::from_vec(clean))
}

#[cfg(unix)]
pub(crate) fn expand_path(path: &OsStr, env: &Env) -> Result<PathBuf, ConfigError> {
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    let raw = path.as_encoded_bytes();
    let mut out = Vec::new();
    let mut at = 0;
    while at < raw.len() {
        if raw[at] != b'$' || at + 1 == raw.len() {
            out.push(raw[at]);
            at += 1;
            continue;
        }
        at += 1;
        let begin = at;
        let (key, width) = if raw[at] == b'{' {
            if let Some(end) = raw[at + 1..].iter().position(|b| *b == b'}') {
                (&raw[at + 1..at + 1 + end], end + 2)
            } else {
                (&[][..], 1)
            }
        } else if matches!(
            raw[at],
            b'*' | b'#' | b'$' | b'@' | b'!' | b'?' | b'-' | b'0'..=b'9'
        ) {
            (&raw[at..at + 1], 1)
        } else {
            let width = raw[at..]
                .iter()
                .take_while(|b| b.is_ascii_alphanumeric() || **b == b'_')
                .count();
            (&raw[at..at + width], width)
        };
        at = begin + width;
        if key.is_empty() {
            if width == 0 {
                out.push(b'$');
            }
        } else if let Some(value) = env.get_os_name(OsStr::from_bytes(key)) {
            out.extend_from_slice(value.as_encoded_bytes());
        }
    }
    if out.first() == Some(&b'~') {
        if out.len() > 1 && !matches!(out[1], b'/' | b'\\') {
            return Err(ConfigError::msg("cannot expand user-specific home dir"));
        }
        #[cfg(any(target_os = "macos", target_os = "linux"))]
        let home = home_dir::resolve(env)?;
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        let home = env
            .get_os("HOME")
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                ConfigError::unsupported(
                    "HOME lookup",
                    "native fallback requires a declared platform source profile",
                )
            })?
            .to_os_string();
        return Ok(join_home_posix(&home, &out[1..]));
    }
    Ok(PathBuf::from(OsString::from_vec(out)))
}
#[cfg(not(unix))]
pub(crate) fn expand_path(_path: &OsStr, _env: &Env) -> Result<PathBuf, ConfigError> {
    Err(ConfigError::unsupported(
        "non-Unicode path",
        "POSIX byte-path profile only; Windows needs its native wide-string contract",
    ))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::ffi::OsStringExt;
    fn info() -> ProgramInfo {
        ProgramInfo {
            binary_name: "livekit-server".into(),
            product_version: "0.1.0".into(),
            compat_version: "1.13.10",
        }
    }
    fn args(values: &[&[u8]]) -> Vec<OsString> {
        std::iter::once(OsString::from("livekit-server"))
            .chain(values.iter().map(|v| OsString::from_vec(v.to_vec())))
            .collect()
    }
    #[test]
    fn invalid_bytes_are_not_replacement_runes_and_valid_f51_quote_is_unchanged() {
        assert_eq!(quote::quote_bytes(b"\xff\xc3\""), "\"\\xff\\xc3\\\"\"");
        assert_eq!(quote::quote_bytes("�".as_bytes()), quote::quote("�"));
        assert_ne!(quote::quote_bytes(b"\xff"), quote::quote("�"));
    }
    #[test]
    fn typed_cli_consumes_real_byte_values_in_source_order() {
        let error = preflight(&args(&[b"--port", b"\xff", b"--help"]), &info()).unwrap_err();
        assert!(
            matches!(error,EntryAction::PrintHelp{text,..} if text.starts_with("Incorrect Usage: invalid value \"\\xff\" for flag -port: strconv.ParseUint: parsing \"\\xff\": invalid syntax"))
        );
        assert!(preflight(&args(&[b"--config", b"\xff", b"--help"]), &info()).is_ok());
        let error = preflight(&args(&[b"--\xff", b"--help"]), &info()).unwrap_err();
        assert!(
            matches!(error,EntryAction::PrintBytes{stderr,..} if stderr==b"Incorrect Usage: flag provided but not defined: -\xff\n\n")
        );
    }
    #[test]
    fn known_environment_byte_values_survive_and_root_cli_alone_can_override() {
        let env = Env::from_os_pairs([(
            OsString::from("LIVEKIT_PORT"),
            OsString::from_vec(vec![255]),
        )]);
        assert_eq!(
            env.get_os("LIVEKIT_PORT").unwrap().as_encoded_bytes(),
            b"\xff"
        );
        assert!(
            parse_args(args(&[b"ports", b"--help"]), &env, &info())
                .err()
                .expect("raw ENV error")
                .stdout_bytes()
                .starts_with(b"could not parse \"\\xff\" as uint")
        );
        assert!(parse_args(args(&[b"ports", b"--port=43117", b"--help"]), &env, &info()).is_err());
        assert!(matches!(
            parse_args(args(&[b"--port=43117", b"ports", b"--help"]), &env, &info()).unwrap(),
            Parsed::Early(_)
        ));
    }
    #[test]
    fn paths_and_env_expansion_keep_all_original_bytes() {
        let env = Env::from_os_pairs([(
            OsString::from("HOME"),
            OsString::from_vec(b"/owned/home-\xff".to_vec()),
        )]);
        assert_eq!(
            expand_path(OsStr::new("~/keys"), &env)
                .unwrap()
                .as_os_str()
                .as_encoded_bytes(),
            b"/owned/home-\xff/keys"
        );
        assert_eq!(
            expand_path(&OsString::from_vec(b"/owned/key-\xfe".to_vec()), &env)
                .unwrap()
                .as_os_str()
                .as_encoded_bytes(),
            b"/owned/key-\xfe"
        );
    }
    #[test]
    fn yaml_reader_byte_failure_categories_do_not_coerce_text() {
        use crate::official::{Config, decode_yaml};

        for (input, error) in [
            (&b"x: \xff"[..], "yaml: invalid leading UTF-8 octet"),
            (&b"x: \xc3"[..], "yaml: incomplete UTF-8 octet sequence"),
            (&b"x: \xc3x"[..], "yaml: invalid trailing UTF-8 octet"),
            (
                &b"x: \xc0\x80"[..],
                "yaml: invalid length of a UTF-8 sequence",
            ),
        ] {
            assert_eq!(
                decode_yaml(input, true, &mut Config::default()).unwrap_err(),
                error
            );
        }
    }
    #[test]
    fn missing_non_unicode_config_diagnostic_retains_raw_path_bytes() {
        let path = std::env::temp_dir().join(OsString::from_vec(
            format!("f52-absent-{}-", std::process::id())
                .into_bytes()
                .into_iter()
                .chain([255])
                .collect(),
        ));
        let argv = vec![
            OsString::from("livekit-server"),
            OsString::from("--config"),
            path.into_os_string(),
            OsString::from("ports"),
        ];
        let error = crate::parse_with_probe(
            argv,
            &Env::default(),
            &info(),
            &crate::netdetect::tests::FakeProbe::local("127.0.0.1"),
        )
        .expect_err("actual read error");
        let bytes = error.stdout_bytes();
        assert!(bytes.starts_with(b"open "));
        assert!(bytes.contains(&255));
        assert!(!bytes.windows(3).any(|w| w == [239, 191, 189]));
    }
    #[test]
    fn invalid_config_and_keys_environment_bytes_reach_their_real_consumers() {
        let probe = crate::netdetect::tests::FakeProbe::local("127.0.0.1");
        for (name, value, expected) in [
            (
                "LIVEKIT_CONFIG",
                b"port: \xff\n".as_slice(),
                "could not parse config: yaml: invalid leading UTF-8 octet",
            ),
            (
                "LIVEKIT_KEYS",
                b"x: \xff".as_slice(),
                crate::keys::KEYS_PARSE_ERROR,
            ),
        ] {
            let env =
                Env::from_os_pairs([(OsString::from(name), OsString::from_vec(value.to_vec()))]);
            let error = crate::parse_with_probe(args(&[b"ports"]), &env, &info(), &probe)
                .expect_err("source consumer error");
            assert_eq!(error.stdout_bytes(), expected.as_bytes());
        }
    }
    #[cfg(target_os = "linux")]
    #[test]
    fn linux_positive_byte_filenames_reach_real_config_and_keyfile_consumers() {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        struct Owned(std::path::PathBuf);
        impl Drop for Owned {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let directory = std::env::temp_dir().join(format!(
            "f52-byte-paths-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&directory).unwrap();
        let owned = Owned(directory);
        let config = owned
            .0
            .join(OsString::from_vec(b"config-\xff.yaml".to_vec()));
        let key = owned.0.join(OsString::from_vec(b"key-\xfe.yaml".to_vec()));
        for (path, body) in [
            (&config, b"port: 43117\n".as_slice()),
            (&key, b"APIf52: owned-not-production-secret\n".as_slice()),
        ] {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path)
                .unwrap();
            file.write_all(body).unwrap();
        }
        let probe = crate::netdetect::tests::FakeProbe::local("127.0.0.1");
        let argv = vec![
            OsString::from("livekit-server"),
            OsString::from("--config"),
            config.into_os_string(),
            OsString::from("ports"),
        ];
        let result = crate::parse_with_probe(argv, &Env::default(), &info(), &probe).unwrap();
        assert!(matches!(result,EntryAction::Ports(config) if config.official.port==43117));
        let argv = vec![
            OsString::from("livekit-server"),
            OsString::from("--key-file"),
            key.clone().into_os_string(),
            OsString::from("create-join-token"),
            OsString::from("--room=f52-room"),
            OsString::from("--identity=f52-identity"),
        ];
        let result = crate::parse_with_probe(argv, &Env::default(), &info(), &probe).unwrap();
        assert!(
            matches!(result,EntryAction::CreateJoinToken{config,..} if config.key_file==Some(key.clone()) && config.official.keys.0==vec![("APIf52".to_owned(),"owned-not-production-secret".to_owned())])
        );
    }
    #[cfg(target_os = "linux")]
    #[test]
    fn ascii_keyfile_expansion_dependencies_preserve_byte_home_and_variable_in_real_loader() {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        struct Owned(std::path::PathBuf);
        impl Drop for Owned {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let directory = std::env::temp_dir().join(OsString::from_vec(
            format!(
                "f56-home-{}-{}-",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            )
            .into_bytes()
            .into_iter()
            .chain([255])
            .collect(),
        ));
        std::fs::create_dir(&directory).unwrap();
        let owned = Owned(directory);
        let key = owned.0.join("keys");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&key)
            .unwrap();
        file.write_all(b"APIf56: owned-not-production-secret\n")
            .unwrap();
        drop(file);
        let probe = crate::netdetect::tests::FakeProbe::local("127.0.0.1");
        for (variable, pattern, missing) in [
            ("HOME", "~/keys", "~/missing"),
            ("RAW_DIR", "$RAW_DIR/keys", "$RAW_DIR/missing"),
        ] {
            let env =
                Env::from_os_pairs([(OsString::from(variable), owned.0.clone().into_os_string())]);
            let argv = vec![
                "livekit-server".into(),
                "--key-file".into(),
                pattern.into(),
                "create-join-token".into(),
                "--room=f56-room".into(),
                "--identity=f56-identity".into(),
            ];
            let result = crate::parse_with_probe(argv, &env, &info(), &probe).unwrap();
            assert!(
                matches!(result,EntryAction::CreateJoinToken{config,..} if config.key_file==Some(key.clone())&&config.official.keys.0==vec![("APIf56".to_owned(),"owned-not-production-secret".to_owned())])
            );
            let argv = vec![
                "livekit-server".into(),
                "--key-file".into(),
                missing.into(),
                "create-join-token".into(),
                "--room=f56-room".into(),
                "--identity=f56-identity".into(),
            ];
            let bytes = crate::parse_with_probe(argv, &env, &info(), &probe)
                .err()
                .expect("actual missing key-file error")
                .stdout_bytes();
            let mut expected = b"stat ".to_vec();
            expected.extend_from_slice(owned.0.join("missing").as_os_str().as_encoded_bytes());
            expected.extend_from_slice(b": no such file or directory");
            assert_eq!(bytes, expected);
        }
    }

    #[test]
    fn declared_raw_https_environment_reaches_public_typed_consumer_and_cli_precedence() {
        let probe = crate::netdetect::tests::FakeProbe::local("127.0.0.1");
        for field in [
            "PORT",
            "HTTP_ENABLED",
            "MAX_CONNECTIONS",
            "HANDSHAKE_TIMEOUT_MS",
        ] {
            let env = Env::from_os_pairs([(
                OsString::from(format!("LIVECLOUD_HTTPS_{field}")),
                OsString::from_vec(vec![255]),
            )]);
            let error = crate::parse_with_probe(args(&[b"--dev"]), &env, &info(), &probe)
                .expect_err("declared invalid numeric/bool ENV cannot disappear");
            assert!(
                error
                    .stdout_bytes()
                    .starts_with(b"invalid livecloud.https.")
            );
            assert!(error.stdout_bytes().ends_with(b" value \"\\xff\""));
            assert!(matches!(
                crate::parse_with_probe(args(&[b"--help"]), &env, &info(), &probe).unwrap(),
                EntryAction::PrintHelp { exit_code: 0, .. }
            ));
        }
        let env = Env::from_os_pairs([("LIVECLOUD_HTTPS_PORT", OsString::from_vec(vec![255]))]);
        let parsed = crate::parse_with_probe(
            args(&[
                b"--livecloud-https-port=8443",
                b"--livecloud-https-cert-file=cert.pem",
                b"--livecloud-https-key-file=key.pem",
                b"--dev",
            ]),
            &env,
            &info(),
            &probe,
        )
        .unwrap();
        assert!(
            matches!(parsed,EntryAction::RunServer(config) if config.server.extension.as_ref().and_then(|e| e.https.as_ref()).is_some_and(|h|h.port==8443))
        );

        let invalid = Env::from_os_pairs([("LIVECLOUD_HTTPS_PORT", OsString::from_vec(vec![255]))]);
        for argv in [
            args(&[b"ports", b"-", b"--help"]),
            args(&[b"list-nodes"]),
            args(&[b"generate-keys"]),
            args(&[
                b"create-join-token",
                b"--keys=APIf61: secret",
                b"--room=r",
                b"--identity=i",
            ]),
        ] {
            crate::parse_with_probe(argv, &invalid, &info(), &probe)
                .expect("server-only extension ENV must not affect one-shot CLI consumers");
        }
        let env =
            Env::from_os_pairs([("LIVECLOUD_HTTPS_CERT_FILE", OsString::from_vec(vec![255]))]);
        assert!(
            matches!(
                crate::parse_with_probe(args(&[b"--dev"]), &env, &info(), &probe),
                Err(ConfigError::Unsupported { .. })
            ),
            "raw proprietary path remains explicitly OPEN, not successful support"
        );
    }

    #[test]
    fn posix_home_join_keeps_backslash_and_lexically_cleans_raw_parent() {
        for (home, suffix, expected) in [
            (
                b"/owned/home-\xff".as_slice(),
                b"/../keys".as_slice(),
                b"/owned/keys".as_slice(),
            ),
            (b"/owned/home", b"\\keys", b"/owned/home/\\keys"),
            (b"/owned/home", b"//a/./b/../../keys", b"/owned/home/keys"),
            (b"/", b"/../../keys", b"/keys"),
            (b"relative/home", b"/../keys", b"relative/keys"),
            (b"..", b"/../keys", b"../../keys"),
        ] {
            assert_eq!(
                join_home_posix(&OsString::from_vec(home.to_vec()), suffix)
                    .as_os_str()
                    .as_encoded_bytes(),
                expected
            );
        }
    }

    #[test]
    fn raw_nonexistent_home_parent_and_posix_backslash_reach_real_ascii_key_files() {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        struct Owned(PathBuf);
        impl Drop for Owned {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let path = std::env::temp_dir().join(format!(
            "f61-real-home-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        let owned = Owned(path);
        let ordinary = owned.0.join("keys-ascii.yaml");
        let backslash_name = OsString::from("\\keys-ascii.yaml");
        let backslash = owned.0.join(backslash_name);
        for file in [&ordinary, &backslash] {
            let mut output = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(file)
                .unwrap();
            output
                .write_all(b"APIf61: f61-owned-signing-secret\n")
                .unwrap();
        }
        let raw_home = owned
            .0
            .join(OsString::from_vec(b"absent-home-\xff".to_vec()));
        let probe = crate::netdetect::tests::FakeProbe::local("127.0.0.1");
        for (home, pattern, expected) in [
            (raw_home.into_os_string(), "~/../keys-ascii.yaml", ordinary),
            (
                owned.0.clone().into_os_string(),
                "~\\keys-ascii.yaml",
                backslash,
            ),
        ] {
            let env = Env::from_os_pairs([("HOME", home)]);
            let argv = [
                "livekit-server",
                "--key-file",
                pattern,
                "create-join-token",
                "--room=f61",
                "--identity=f61",
            ]
            .into_iter()
            .map(OsString::from)
            .collect();
            let result = crate::parse_with_probe(argv, &env, &info(), &probe).unwrap();
            assert!(matches!(result,EntryAction::CreateJoinToken { config,.. }
                if config.key_file==Some(expected.clone()) && config.official.keys.0==vec![("APIf61".into(),"f61-owned-signing-secret".into())]));
        }
    }

    #[test]
    #[ignore = "requires actual fixed go-homedir F61_HOME_GO_RESULTS and F61_HOME_CASES"]
    fn actual_go_homedir_posix_vectors() {
        fn unhex(value: &str) -> Vec<u8> {
            value
                .as_bytes()
                .as_chunks::<2>()
                .0
                .iter()
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect()
        }
        let cases =
            std::fs::read_to_string(std::env::var("F61_HOME_CASES").expect("actual oracle cases"))
                .unwrap();
        let cases: Vec<serde_json::Value> = serde_json::from_str(&cases).unwrap();
        let results = std::fs::read_to_string(
            std::env::var("F61_HOME_GO_RESULTS").expect("actual Go output"),
        )
        .unwrap();
        let results: Vec<serde_json::Value> = results
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(cases.len(), 8);
        assert_eq!(results.len(), cases.len());
        for (case, result) in cases.iter().zip(&results) {
            assert_eq!(case["name"], result["name"]);
            let home = OsString::from_vec(unhex(case["home_hex"].as_str().unwrap()));
            let path = OsString::from_vec(unhex(case["path_hex"].as_str().unwrap()));
            let value = expand_path(&path, &Env::from_os_pairs([("HOME", home)]));
            match value {
                Ok(path) => {
                    assert_eq!(result["error"], "");
                    assert_eq!(
                        path.as_os_str().as_encoded_bytes(),
                        unhex(result["path_hex"].as_str().unwrap())
                    );
                }
                Err(error) => assert_eq!(
                    error.stdout_bytes(),
                    result["error"].as_str().unwrap().as_bytes()
                ),
            }
        }
    }
    #[test]
    fn server_only_https_environment_consumes_run_but_not_official_one_shots() {
        let probe = crate::netdetect::tests::FakeProbe::local("127.0.0.1");
        let incomplete = Env::from_pairs([("LIVECLOUD_HTTPS_PORT", "9443")]);
        for argv in [
            args(&[b"ports", b"-", b"--help"]),
            args(&[b"list-nodes"]),
            args(&[b"generate-keys"]),
            args(&[
                b"create-join-token",
                b"--keys=APIf61: secret",
                b"--room=r",
                b"--identity=i",
            ]),
        ] {
            crate::parse_with_probe(argv, &incomplete, &info(), &probe).expect(
                "real one-shot CLI must not require server TLS cert/key from unrelated ENV",
            );
        }
        let complete = Env::from_pairs([
            ("LIVECLOUD_HTTPS_PORT", "9443"),
            ("LIVECLOUD_HTTPS_CERT_FILE", "cert.pem"),
            ("LIVECLOUD_HTTPS_KEY_FILE", "key.pem"),
        ]);
        let actual =
            crate::parse_with_probe(args(&[b"--dev"]), &complete, &info(), &probe).unwrap();
        assert!(matches!(actual,EntryAction::RunServer(config)
            if config.server.extension.as_ref().and_then(|e| e.https.as_ref()).is_some_and(|h|h.port==9443)));
    }

    #[test]
    fn actual_raw_environment_name_expands_only_at_explicit_path_reference() {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        struct Owned(PathBuf);
        impl Drop for Owned {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let path = std::env::temp_dir().join(format!(
            "f69-name-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        let owned = Owned(path);
        let nested = owned.0.join("raw-target");
        std::fs::create_dir(&nested).unwrap();
        let key = nested.join("keys.yaml");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&key)
            .unwrap();
        file.write_all(b"APIf69: owned-f69-not-production-secret\n")
            .unwrap();
        drop(file);
        let probe = crate::netdetect::tests::FakeProbe::local("127.0.0.1");
        for name in [b"F69_PATH\xff".as_slice(), b"LIVEKIT_PORT\xff".as_slice()] {
            let env = Env::from_os_pairs([(
                OsString::from_vec(name.to_vec()),
                OsString::from("raw-target"),
            )]);
            let mut expression = owned.0.as_os_str().as_encoded_bytes().to_vec();
            expression.extend_from_slice(b"/${");
            expression.extend_from_slice(name);
            expression.extend_from_slice(b"}/keys.yaml");
            let argv = vec![
                OsString::from("livekit-server"),
                OsString::from("--key-file"),
                OsString::from_vec(expression),
                OsString::from("--config-body"),
                OsString::from("keys: {}"),
                OsString::from("create-join-token"),
                OsString::from("--room=f69"),
                OsString::from("--identity=f69"),
            ];
            let actual = crate::parse_with_probe(argv, &env, &info(), &probe).unwrap();
            assert!(
                matches!(actual,EntryAction::CreateJoinToken{config,..}if config.key_file==Some(key.clone())&&config.official.keys.0==vec![("APIf69".to_string(),"owned-f69-not-production-secret".to_string())])
            );
            let ascii = crate::parse_with_probe(args(&[b"ports"]), &env, &info(), &probe).unwrap();
            assert!(
                matches!(ascii,EntryAction::Ports(config)if config.official.port==7880),
                "raw name must not alias declared ASCII flag ENV"
            );
        }
    }
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn actual_public_missing_home_empty_path_uses_go_lookup_error_not_decoy() {
        struct Owned(PathBuf);
        impl Drop for Owned {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let path = std::env::temp_dir().join(format!(
            "f84-red-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        let owned = Owned(path);
        let e = Env::from_os_pairs([
            (OsString::from("PATH"), OsString::new()),
            ("USERPROFILE".into(), owned.0.clone().into_os_string()),
        ]);
        let argv = args(&[
            b"--node-ip=127.0.0.1",
            b"--key-file",
            b"~/never-created-f84",
            b"--config-body",
            b"keys: {}",
            b"create-join-token",
            b"--room=f84",
            b"--identity=f84",
        ]);
        let error = crate::parse_with_probe(
            argv,
            &e,
            &info(),
            &crate::netdetect::tests::FakeProbe::local("127.0.0.1"),
        )
        .unwrap_err();
        #[cfg(target_os = "macos")]
        assert_eq!(
            error.stdout_bytes(),
            b"exec: \"sh\": executable file not found in $PATH"
        );
        #[cfg(target_os = "linux")]
        assert_eq!(
            error.stdout_bytes(),
            b"exec: \"getent\": executable file not found in $PATH"
        );
    }
}
