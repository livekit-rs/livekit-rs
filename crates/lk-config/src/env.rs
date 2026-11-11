//! Snapshot of the process environment. Everything in this crate reads the
//! environment through [`Env`] so tests can inject one.

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};

/// Environment snapshot. A variable that is present with an empty value counts
/// as *set* (urfave/cli semantics, impl-notes §1.1).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Env(
    pub BTreeMap<String, String>,
    BTreeMap<String, OsString>,
    BTreeMap<OsString, OsString>,
);

impl Env {
    /// Captures process values without discarding bytes of a known ASCII key.
    /// Non-Unicode names cannot name declared ASCII flags, but remain available
    /// to byte-preserving path expansion through [`Env::get_os_name`].
    pub fn from_process() -> Env {
        Self::from_os_pairs(std::env::vars_os())
    }

    /// Snapshot OS values without process-global side tables or lossy decoding.
    pub fn from_os_pairs<K: Into<OsString>, V: Into<OsString>>(
        pairs: impl IntoIterator<Item = (K, V)>,
    ) -> Self {
        let mut result = Self::default();
        for (key, value) in pairs {
            let key: OsString = key.into();
            // Go syscall.copyenv keeps the first actual key occurrence, including
            // an empty first value. Match byte names before Unicode projection.
            if result.get_os_name(&key).is_some() {
                continue;
            }
            let value = value.into();
            let key = match key.into_string() {
                Ok(key) => key,
                Err(key) => {
                    result.2.insert(key, value);
                    continue;
                }
            };
            result.0.remove(&key);
            result.1.remove(&key);
            match value.into_string() {
                Ok(value) => {
                    result.0.insert(key, value);
                }
                Err(value) => {
                    result.1.insert(key, value);
                }
            }
        }
        result
    }
    /// Raw value including non-Unicode data. Presence still counts as set.
    pub fn get_os(&self, k: &str) -> Option<&OsStr> {
        self.1
            .get(k)
            .map(OsString::as_os_str)
            .or_else(|| self.0.get(k).map(OsStr::new))
    }
    /// Exact OS-name lookup for shell-style ${name} expansion, including raw
    /// POSIX names. This does not declare any new ASCII flag environment source.
    pub fn get_os_name(&self, name: &OsStr) -> Option<&OsStr> {
        match name.to_str() {
            Some(name) => self.get_os(name),
            None => self.2.get(name).map(OsString::as_os_str),
        }
    }
    pub fn get(&self, k: &str) -> Option<&str> {
        self.0.get(k).map(String::as_str)
    }

    /// Full owned environment for source OS lookups. Preserve unset versus empty
    /// HOME and raw names/values; never read ambient globals after capture.
    pub(crate) fn command_env(&self) -> Vec<(OsString, OsString)> {
        self.0
            .iter()
            .map(|(k, v)| (OsString::from(k), OsString::from(v)))
            .chain(self.1.iter().map(|(k, v)| (OsString::from(k), v.clone())))
            .chain(self.2.iter().map(|(k, v)| (k.clone(), v.clone())))
            .collect()
    }

    /// Test helper: builds an environment from pairs.
    pub fn from_pairs<K: Into<String>, V: Into<String>>(
        pairs: impl IntoIterator<Item = (K, V)>,
    ) -> Env {
        Env(
            pairs
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            BTreeMap::new(),
            BTreeMap::new(),
        )
    }
}

#[cfg(all(test, unix))]
mod duplicate_process_snapshot_tests {
    use super::*;
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    /// Component regression only. The execve fixture supplies separate real
    /// process/CLI evidence; an injected map alone cannot establish that gate.
    #[test]
    fn duplicate_os_snapshot_keeps_first_empty_and_raw_values() {
        let raw = OsString::from_vec(b"F92_NAME\xff".to_vec());
        let e = Env::from_os_pairs([
            (OsString::from("LIVEKIT_PORT"), OsString::from("17881")),
            (OsString::from("LIVEKIT_PORT"), OsString::from("17882")),
            (OsString::from("EMPTY"), OsString::new()),
            (OsString::from("EMPTY"), OsString::from("later")),
            (raw.clone(), OsString::from_vec(b"first\xfe".to_vec())),
            (raw.clone(), OsString::from("later")),
            (OsString::from("livekit_port"), OsString::from("distinct")),
        ]);
        assert_eq!(e.get("LIVEKIT_PORT"), Some("17881"));
        assert_eq!(e.get_os("EMPTY"), Some(OsStr::new("")));
        assert_eq!(e.get_os_name(&raw).unwrap().as_bytes(), b"first\xfe");
        assert_eq!(e.get("livekit_port"), Some("distinct"));
        assert_eq!(e.command_env().len(), 4);
    }
}
