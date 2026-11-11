//! Identifiers. Internal ids, externally visible SIDs, SDP mids, SSRCs and data
//! channel ids are distinct types (§5.1).

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// Alphabet of `shortuuid.DefaultAlphabet`, used by the upstream guid package.
const SID_ALPHABET: &[u8] = b"23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
/// Random part length of upstream guids (`guid.Size`).
const SID_SIZE: usize = 12;

pub const ROOM_PREFIX: &str = "RM_";
pub const PARTICIPANT_PREFIX: &str = "PA_";
pub const TRACK_PREFIX: &str = "TR_";
pub const NODE_PREFIX: &str = "ND_";

/// `prefix` followed by 12 random characters of the shortuuid alphabet.
pub fn new_sid(prefix: &str) -> String {
    let mut out = String::with_capacity(prefix.len() + SID_SIZE);
    out.push_str(prefix);
    // Rejection sampling keeps the distribution uniform over 57 symbols.
    let limit = 256 - (256 % SID_ALPHABET.len());
    let mut buf = [0u8; 32];
    let mut filled = 0;
    while filled < SID_SIZE {
        crate::util::fill_random(&mut buf);
        for b in buf {
            if (b as usize) < limit {
                out.push(SID_ALPHABET[b as usize % SID_ALPHABET.len()] as char);
                filled += 1;
                if filled == SID_SIZE {
                    break;
                }
            }
        }
    }
    out
}

macro_rules! string_id {
    ($(#[$m:meta])* $name:ident) => {
        $(#[$m])*
        #[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
        pub struct $name(pub String);

        impl $name {
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self.0)
            }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                Self(s.to_string())
            }
        }

        impl From<String> for $name {
            fn from(s: String) -> Self {
                Self(s)
            }
        }
    };
}

string_id!(
    /// Tenant boundary. Standalone deployments use a single [`ProjectId::local`].
    ProjectId
);
string_id!(RoomName);
string_id!(RoomSid);
string_id!(Identity);
string_id!(ParticipantSid);
string_id!(TrackSid);
string_id!(NodeId);

impl ProjectId {
    pub fn local() -> Self {
        Self("local".into())
    }
}

impl RoomSid {
    pub fn generate() -> Self {
        Self(new_sid(ROOM_PREFIX))
    }
}

impl ParticipantSid {
    pub fn generate() -> Self {
        Self(new_sid(PARTICIPANT_PREFIX))
    }
}

impl TrackSid {
    pub fn generate() -> Self {
        Self(new_sid(TRACK_PREFIX))
    }
}

/// Every room-scoped key carries the project (§7.4, K07).
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RoomKey {
    pub project: ProjectId,
    pub name: RoomName,
}

impl RoomKey {
    pub fn new(project: ProjectId, name: impl Into<RoomName>) -> Self {
        Self {
            project,
            name: name.into(),
        }
    }
}

impl fmt::Debug for RoomKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RoomKey({}/{})", self.project, self.name)
    }
}

macro_rules! counter_id {
    ($(#[$m:meta])* $name:ident) => {
        $(#[$m])*
        #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
        pub struct $name(pub u64);

        impl $name {
            pub fn next() -> Self {
                static NEXT: AtomicU64 = AtomicU64::new(1);
                Self(NEXT.fetch_add(1, Ordering::Relaxed))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

counter_id!(
    /// Process-local id of one participant session (a join attempt). A reconnect that
    /// resumes keeps the participant but bumps [`SessionGeneration`].
    SessionId
);
counter_id!(
    /// Process-local id of one WebRTC transport (PeerConnection on the client).
    TransportId
);

/// Incremented on every signal resume; stale sockets carry an older generation and
/// must not mutate the current session (§3.3).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Default)]
pub struct SessionGeneration(pub u32);

impl SessionGeneration {
    pub fn bump(self) -> Self {
        Self(self.0.wrapping_add(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sid_has_prefix_and_alphabet() {
        let sid = new_sid(TRACK_PREFIX);
        assert!(sid.starts_with("TR_"));
        assert_eq!(sid.len(), 3 + SID_SIZE);
        assert!(sid[3..].bytes().all(|b| SID_ALPHABET.contains(&b)));
        assert_ne!(new_sid(TRACK_PREFIX), sid);
    }

    #[test]
    fn counter_ids_are_unique() {
        assert_ne!(TransportId::next(), TransportId::next());
    }
}
