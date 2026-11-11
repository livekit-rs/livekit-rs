// Source behavior: references/deps/mitchellh--go-homedir v1.1.0 (MIT),
// references/deps/golang--go-1.26.3/src/os/exec (BSD-3-Clause).
// Notices: LICENSES/go-homedir-MIT.txt and LICENSES/Go-BSD-3-Clause.txt.
//! POSIX CLI key-file HOME resolver. No USERPROFILE and no global injected-Env cache.
use crate::{ConfigError, Env};
use std::ffi::{CString, OsStr, OsString};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

#[cfg(target_os = "macos")]
const AT_FDCWD: i32 = -2;
#[cfg(target_os = "macos")]
const AT_EACCESS: i32 = 0x10;
#[cfg(target_os = "macos")]
const ENOSYS: i32 = 78;
#[cfg(target_os = "linux")]
const AT_FDCWD: i32 = -100;
#[cfg(target_os = "linux")]
const AT_EACCESS: i32 = 0x200;
#[cfg(target_os = "linux")]
const ENOSYS: i32 = 38;
unsafe extern "C" {
    fn faccessat(dirfd: i32, path: *const std::ffi::c_char, mode: i32, flags: i32) -> i32;
    #[cfg(target_os = "linux")]
    fn getuid() -> u32;
}

/// Only source Go White_Space code points; invalid UTF-8 bytes remain non-space.
fn space(value: char) -> bool {
    matches!(value as u32, 0x09..=0x0d | 0x20 | 0x85 | 0xa0 | 0x1680 | 0x2000..=0x200a | 0x2028 | 0x2029 | 0x202f | 0x205f | 0x3000)
}
fn first(input: &[u8]) -> Option<(char, usize)> {
    let b = *input.first()?;
    let width = match b {
        0..=0x7f => 1,
        0xc2..=0xdf => 2,
        0xe0..=0xef => 3,
        0xf0..=0xf4 => 4,
        _ => return None,
    };
    Some((
        std::str::from_utf8(input.get(..width)?)
            .ok()?
            .chars()
            .next()?,
        width,
    ))
}
fn trim_space(mut input: &[u8]) -> &[u8] {
    while let Some((ch, width)) = first(input) {
        if !space(ch) {
            break;
        }
        input = &input[width..];
    }
    loop {
        let mut tail = None;
        for width in 1..=input.len().min(4) {
            if let Some((ch, actual)) = first(&input[input.len() - width..])
                && actual == width
            {
                tail = Some((ch, width));
                break;
            }
        }
        let Some((ch, width)) = tail else {
            break;
        };
        if !space(ch) {
            break;
        }
        input = &input[..input.len() - width];
    }
    input
}
fn exec_error(name: &str, cause: &str) -> ConfigError {
    ConfigError::msg(format!("exec: {}: {cause}", crate::cli::quote::quote(name)))
}
fn executable(path: &Path) -> bool {
    let Ok(meta) = std::fs::metadata(path) else {
        return false;
    };
    if meta.is_dir() {
        return false;
    }
    let Ok(path) = CString::new(path.as_os_str().as_bytes()) else {
        return false;
    };
    // SAFETY: live NUL-terminated pathname, scalar POSIX constants; no retained pointer.
    let result = unsafe { faccessat(AT_FDCWD, path.as_ptr(), 1, AT_EACCESS) };
    if result == 0 {
        return true;
    }
    let errno = std::io::Error::last_os_error().raw_os_error();
    (matches!(errno, Some(1)) || errno == Some(ENOSYS)) && meta.permissions().mode() & 0o111 != 0
}
fn allow_relative(env: &Env) -> bool {
    env.get_os("GODEBUG")
        .map(OsStrExt::as_bytes)
        .unwrap_or_default()
        .split(|b| *b == b',')
        .rev()
        .find_map(|v| v.strip_prefix(b"execerrdot="))
        == Some(b"0".as_slice())
}
/// Match source LookPath over captured ENV, including absent/empty PATH and ErrDot.
fn look_path(name: &str, env: &Env) -> Result<PathBuf, ConfigError> {
    let path = env
        .get_os("PATH")
        .map(OsStrExt::as_bytes)
        .unwrap_or_default();
    if !path.is_empty() {
        for dir in path.split(|b| *b == b':') {
            let dir = if dir.is_empty() { b".".as_slice() } else { dir };
            let file = super::join_home_posix(OsStr::from_bytes(dir), name.as_bytes());
            if executable(&file) {
                if !file.is_absolute() && !allow_relative(env) {
                    return Err(exec_error(
                        name,
                        "cannot run executable found relative to current directory",
                    ));
                }
                return Ok(file);
            }
        }
    }
    Err(exec_error(name, "executable file not found in $PATH"))
}
fn run(name: &str, args: &[&OsStr], env: &Env) -> Result<Vec<u8>, ConfigError> {
    let path = look_path(name, env)?;
    // Rust Command searches PATH again for a slash-free relative name. Go
    // already selected this exact relative file; force exec of the same file.
    let executable = if !path.is_absolute() && !path.as_os_str().as_bytes().contains(&b'/') {
        Path::new(".").join(&path)
    } else {
        path.clone()
    };
    let result = Command::new(&executable)
        .arg0(name)
        .args(args)
        .env_clear()
        .envs(env.command_env())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .map_err(|e| crate::keys::path_error("fork/exec", &path, &e))?;
    if result.status.success() {
        return Ok(result.stdout);
    }
    let text = if let Some(code) = result.status.code() {
        format!("exit status {code}")
    } else if let Some(signal) = result.status.signal() {
        format!("signal: {}", signal_name(signal))
    } else {
        "process status unavailable".to_owned()
    };
    let text = if result.status.core_dumped() {
        text + " (core dumped)"
    } else {
        text
    };
    Err(ConfigError::process_exit(
        text,
        result.status.code().unwrap_or(-1),
    ))
}
/// Same command order and actual process errors as source. CLI expansion is once;
/// no cache result can bleed between independent immutable Env snapshots.
pub(super) fn resolve(env: &Env) -> Result<OsString, ConfigError> {
    if let Some(home) = env.get_os("HOME").filter(|h| !h.is_empty()) {
        return Ok(home.to_os_string());
    }
    #[cfg(target_os = "macos")]
    {
        let script = r#"dscl -q . -read /Users/"$(whoami)" NFSHomeDirectory | sed 's/^[^ ]*: //'"#;
        if let Ok(output) = run("sh", &[OsStr::new("-c"), OsStr::new(script)], env) {
            let result = trim_space(&output);
            if !result.is_empty() {
                return Ok(OsString::from_vec(result.to_vec()));
            }
        }
    }
    #[cfg(target_os = "linux")]
    {
        // SAFETY: POSIX getuid has no inputs/pointers and returns real uid_t(u32).
        let uid = unsafe { getuid() }.to_string();
        // Source compares err directly to exec.ErrNotFound. LookPath failures
        // are *exec.Error wrappers, so real missing getent returns here, not sh.
        let output = run("getent", &[OsStr::new("passwd"), OsStr::new(&uid)], env)?;
        let result = trim_space(&output);
        if !result.is_empty() {
            let parts: Vec<_> = result.splitn(7, |b| *b == b':').collect();
            if parts.len() > 5 {
                return Ok(OsString::from_vec(parts[5].to_vec()));
            }
        }
    }
    let output = run("sh", &[OsStr::new("-c"), OsStr::new("cd && pwd")], env)?;
    let result = trim_space(&output);
    if result.is_empty() {
        return Err(ConfigError::msg("blank output when reading home directory"));
    }
    Ok(OsString::from_vec(result.to_vec()))
}

#[cfg(target_os = "macos")]
fn signal_name(signal: i32) -> String {
    match signal {
        1 => "hangup".to_owned(),
        2 => "interrupt".to_owned(),
        3 => "quit".to_owned(),
        4 => "illegal instruction".to_owned(),
        5 => "trace/BPT trap".to_owned(),
        6 => "abort trap".to_owned(),
        7 => "EMT trap".to_owned(),
        8 => "floating point exception".to_owned(),
        9 => "killed".to_owned(),
        10 => "bus error".to_owned(),
        11 => "segmentation fault".to_owned(),
        12 => "bad system call".to_owned(),
        13 => "broken pipe".to_owned(),
        14 => "alarm clock".to_owned(),
        15 => "terminated".to_owned(),
        16 => "urgent I/O condition".to_owned(),
        17 => "suspended (signal)".to_owned(),
        18 => "suspended".to_owned(),
        19 => "continued".to_owned(),
        20 => "child exited".to_owned(),
        21 => "stopped (tty input)".to_owned(),
        22 => "stopped (tty output)".to_owned(),
        23 => "I/O possible".to_owned(),
        24 => "cputime limit exceeded".to_owned(),
        25 => "filesize limit exceeded".to_owned(),
        26 => "virtual timer expired".to_owned(),
        27 => "profiling timer expired".to_owned(),
        28 => "window size changes".to_owned(),
        29 => "information request".to_owned(),
        30 => "user defined signal 1".to_owned(),
        31 => "user defined signal 2".to_owned(),
        _ => format!("signal {signal}"),
    }
}

#[cfg(target_os = "linux")]
fn signal_name(signal: i32) -> String {
    match signal {
        1 => "hangup".to_owned(),
        2 => "interrupt".to_owned(),
        3 => "quit".to_owned(),
        4 => "illegal instruction".to_owned(),
        5 => "trace/breakpoint trap".to_owned(),
        6 => "aborted".to_owned(),
        7 => "bus error".to_owned(),
        8 => "floating point exception".to_owned(),
        9 => "killed".to_owned(),
        10 => "user defined signal 1".to_owned(),
        11 => "segmentation fault".to_owned(),
        12 => "user defined signal 2".to_owned(),
        13 => "broken pipe".to_owned(),
        14 => "alarm clock".to_owned(),
        15 => "terminated".to_owned(),
        16 => "stack fault".to_owned(),
        17 => "child exited".to_owned(),
        18 => "continued".to_owned(),
        19 => "stopped (signal)".to_owned(),
        20 => "stopped".to_owned(),
        21 => "stopped (tty input)".to_owned(),
        22 => "stopped (tty output)".to_owned(),
        23 => "urgent I/O condition".to_owned(),
        24 => "CPU time limit exceeded".to_owned(),
        25 => "file size limit exceeded".to_owned(),
        26 => "virtual timer expired".to_owned(),
        27 => "profiling timer expired".to_owned(),
        28 => "window changed".to_owned(),
        29 => "I/O possible".to_owned(),
        30 => "power failure".to_owned(),
        31 => "bad system call".to_owned(),
        _ => format!("signal {signal}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Owned(PathBuf);
    impl Owned {
        fn new() -> Self {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let sequence = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "f84-{}-{sequence}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
        #[cfg(target_os = "macos")]
        fn only_sh(&self) -> OsString {
            let path = self.0.join("only-sh");
            std::fs::create_dir(&path).unwrap();
            std::os::unix::fs::symlink("/bin/sh", path.join("sh")).unwrap();
            path.into_os_string()
        }
    }
    impl Drop for Owned {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    #[cfg(target_os = "macos")]
    fn env(path: OsString) -> Env {
        Env::from_os_pairs([
            (OsString::from("PATH"), path),
            ("LC_ALL".into(), "C".into()),
            ("TZ".into(), "UTC".into()),
            ("USERPROFILE".into(), "/f84-decoy-never-a-posix-home".into()),
        ])
    }
    #[test]
    fn exact_trim_space_does_not_lossily_decode_paths() {
        assert_eq!(
            trim_space(b" \t\xc2\xa0\xffhome\xfe\xe3\x80\x80\n"),
            b"\xffhome\xfe"
        );
        assert_eq!(trim_space(b"\xff\t"), b"\xff");
        assert_eq!(trim_space(b"\0home\0"), b"\0home\0");
    }
    #[test]
    fn nonempty_raw_home_wins_without_search_or_userprofile() {
        let e = Env::from_os_pairs([
            (
                OsString::from("HOME"),
                OsString::from_vec(b"/f84-\xff".to_vec()),
            ),
            ("PATH".into(), "".into()),
            ("USERPROFILE".into(), "/wrong".into()),
        ]);
        assert_eq!(resolve(&e).unwrap().as_bytes(), b"/f84-\xff");
    }
    #[test]
    fn command_snapshot_preserves_empty_home_and_raw_names() {
        let e = Env::from_os_pairs([
            (OsString::from("HOME"), OsString::new()),
            (
                OsString::from_vec(b"RAW\xff".to_vec()),
                OsString::from_vec(b"VALUE\xfe".to_vec()),
            ),
        ]);
        let values = e.command_env();
        assert_eq!(values.len(), 2);
        assert!(values.iter().any(|(k, v)| k == "HOME" && v.is_empty()));
        assert!(
            values
                .iter()
                .any(|(k, v)| k.as_bytes() == b"RAW\xff" && v.as_bytes() == b"VALUE\xfe")
        );
    }
    #[test]
    fn actual_public_loader_empty_path_returns_source_lookup_error_not_decoy_file_error() {
        let owned = Owned::new();
        let e = Env::from_os_pairs([
            (OsString::from("PATH"), OsString::new()),
            ("USERPROFILE".into(), owned.0.clone().into_os_string()),
        ]);
        let args = [
            "livekit-server",
            "--node-ip=127.0.0.1",
            "--key-file",
            "~/never-created-f84",
            "--config-body",
            "keys: {}",
            "create-join-token",
            "--room=f84",
            "--identity=f84",
        ]
        .into_iter()
        .map(OsString::from)
        .collect();
        let info = crate::ProgramInfo {
            binary_name: "livekit-server".into(),
            product_version: "test".into(),
            compat_version: "1.13.10",
        };
        let error = crate::parse_with_probe(
            args,
            &e,
            &info,
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
    #[cfg(target_os = "macos")]
    #[test]
    fn real_only_sh_preserves_absent_versus_empty_home_source_behavior() {
        let owned = Owned::new();
        let mut e = env(owned.only_sh());
        assert_eq!(resolve(&e).unwrap_err().stdout_bytes(), b"exit status 1");
        e.0.insert("HOME".into(), String::new());
        assert_eq!(
            resolve(&e).unwrap(),
            std::env::current_dir().unwrap().into_os_string()
        );
    }
    #[cfg(target_os = "macos")]
    #[test]
    fn real_empty_home_shell_result_drives_owned_keyfile_consumer() {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let owned = Owned::new();
        let mut e = env(owned.only_sh());
        e.0.insert("HOME".into(), String::new());
        let key = owned.0.join("keys.yaml");
        let mut f = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&key)
            .unwrap();
        f.write_all(b"APIf84: f84-owned-source-secret\n").unwrap();
        drop(f);
        let mut raw = b"~/".to_vec();
        for c in std::env::current_dir().unwrap().components() {
            if matches!(c, std::path::Component::Normal(_)) {
                raw.extend_from_slice(b"../");
            }
        }
        raw.extend_from_slice(key.as_os_str().as_bytes().strip_prefix(b"/").unwrap());
        let mut args: Vec<_> = ["livekit-server", "--node-ip=127.0.0.1", "--key-file"]
            .into_iter()
            .map(OsString::from)
            .collect();
        args.push(OsString::from_vec(raw));
        args.extend(
            [
                "--config-body",
                "keys: {}",
                "create-join-token",
                "--room=f84",
                "--identity=f84",
            ]
            .into_iter()
            .map(OsString::from),
        );
        let info = crate::ProgramInfo {
            binary_name: "livekit-server".into(),
            product_version: "test".into(),
            compat_version: "1.13.10",
        };
        let crate::EntryAction::CreateJoinToken { config, .. } = crate::parse_with_probe(
            args,
            &e,
            &info,
            &crate::netdetect::tests::FakeProbe::local("127.0.0.1"),
        )
        .unwrap() else {
            panic!("real token command configuration");
        };
        assert_eq!(config.key_file, Some(key));
        assert_eq!(
            config.official.keys.0,
            vec![("APIf84".to_string(), "f84-owned-source-secret".to_string())]
        );
    }
}
