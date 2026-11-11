//! Bounded per-generation authority, never a best-effort event history.
use super::super::catalog::CachedParticipantIds;
use rtc_core::{events::RoomProjection, model::RoomInfo};
use std::{collections::BTreeMap, sync::Arc};
pub(super) type Key = (String, String);
struct Record {
    projection: Arc<RoomProjection>,
    revision: u64,
    acknowledged: u64,
    started: bool,
    admitted: std::collections::BTreeSet<(String, String)>,
    admission_errors: BTreeMap<(String, String), crate::service_bus::rpc::RpcError>,
    possibly_cached: CachedParticipantIds,
}
#[derive(Clone)]
pub(super) struct Snapshot {
    pub key: Key,
    pub revision: u64,
    pub projection: Arc<RoomProjection>,
}
pub(super) struct Ledger {
    capacity: usize,
    records: BTreeMap<Key, Record>,
    pub failed: bool,
    pub fatal: bool,
}
impl Ledger {
    pub fn new(capacity: usize) -> Result<Self, String> {
        if capacity == 0 {
            return Err("routing authority capacity must be positive".into());
        }
        Ok(Self {
            capacity,
            records: BTreeMap::new(),
            failed: false,
            fatal: false,
        })
    }
    fn key(room: &RoomInfo) -> Key {
        (room.name.0.clone(), room.sid.0.clone())
    }
    pub fn reserve(&mut self, room: &RoomInfo) -> Result<(), String> {
        if self.fatal || self.failed {
            return Err("routing authority reconciliation is unavailable".into());
        }
        let key = Self::key(room);
        if self.records.contains_key(&key) {
            return Err("room generation is still owned by routing authority".into());
        }
        if self.records.len() >= self.capacity {
            return Err("routing authority generation capacity reached".into());
        }
        self.records.insert(
            key,
            Record {
                projection: Arc::new(RoomProjection {
                    room: room.clone(),
                    participants: vec![],
                    terminal: false,
                }),
                revision: 1,
                acknowledged: 0,
                started: false,
                admitted: Default::default(),
                admission_errors: Default::default(),
                possibly_cached: Default::default(),
            },
        );
        Ok(())
    }
    pub fn cancel(&mut self, room: &RoomInfo) {
        let key = Self::key(room);
        if self.records.get(&key).is_some_and(|record| !record.started) {
            self.records.remove(&key);
        }
    }
    pub fn start(&mut self, room: &RoomInfo) {
        match self.records.get_mut(&Self::key(room)) {
            Some(record) => record.started = true,
            None => self.fatal = true,
        }
    }
    pub fn project(&mut self, projection: RoomProjection) {
        let key = Self::key(&projection.room);
        let Some(record) = self.records.get_mut(&key) else {
            self.fatal = true;
            return;
        };
        if record.projection.terminal && !projection.terminal {
            return;
        }
        record.started = true;
        if *record.projection != projection {
            record.admitted.retain(|(identity, sid)| {
                projection
                    .participants
                    .iter()
                    .any(|p| p.identity.0 == *identity && p.sid.0 == *sid)
            });
            record.admission_errors.retain(|(identity, sid), _| {
                projection
                    .participants
                    .iter()
                    .any(|p| p.identity.0 == *identity && p.sid.0 == *sid)
            });
            record.projection = Arc::new(projection);
            match record.revision.checked_add(1) {
                Some(revision) => record.revision = revision,
                None => self.fatal = true,
            }
        }
    }
    pub fn finish(&mut self, room: &RoomInfo) {
        self.project(RoomProjection {
            room: room.clone(),
            participants: vec![],
            terminal: true,
        });
    }
    /// Topics are actually registered before this notification. A completed
    /// store attempt, including an I/O error, permits Go's RTC request worker.
    pub fn participant_store_attempted(&mut self, projection: &RoomProjection) {
        if let Some(record) = self.records.get_mut(&Self::key(&projection.room)) {
            if record.projection.terminal {
                return;
            }
            for p in &projection.participants {
                if record
                    .projection
                    .participants
                    .iter()
                    .any(|current| current.sid == p.sid && current.identity == p.identity)
                {
                    let key = (p.identity.0.clone(), p.sid.0.clone());
                    record.admission_errors.remove(&key);
                    record.admitted.insert(key);
                }
            }
        }
    }
    pub fn participant_topics_failed(
        &mut self,
        projection: &RoomProjection,
        error: &crate::service_bus::rpc::RpcError,
    ) {
        if let Some(record) = self.records.get_mut(&Self::key(&projection.room)) {
            if record.projection.terminal {
                return;
            }
            for p in &projection.participants {
                if record
                    .projection
                    .participants
                    .iter()
                    .any(|current| current.sid == p.sid && current.identity == p.identity)
                {
                    let key = (p.identity.0.clone(), p.sid.0.clone());
                    if !record.admitted.contains(&key) {
                        record.admission_errors.insert(key, error.clone());
                    }
                }
            }
        }
    }
    /// None means retired/replaced; false means initial store has not completed.
    pub fn participant_admitted(
        &self,
        room: &str,
        identity: &str,
        sid: &str,
    ) -> Option<Result<bool, crate::service_bus::rpc::RpcError>> {
        self.records.iter().find_map(|((name, _), record)| {
            if name != room || record.projection.terminal {
                return None;
            }
            record
                .projection
                .participants
                .iter()
                .any(|p| p.identity.0 == identity && p.sid.0 == sid)
                .then(|| {
                    let key = (identity.to_owned(), sid.to_owned());
                    if let Some(error) = record.admission_errors.get(&key) {
                        Err(error.clone())
                    } else {
                        Ok(record.admitted.contains(&key))
                    }
                })
        })
    }
    pub fn pending(&self) -> Vec<Snapshot> {
        self.records
            .iter()
            .filter(|(_, r)| r.started && r.revision != r.acknowledged)
            .map(|(key, r)| Snapshot {
                key: key.clone(),
                revision: r.revision,
                projection: r.projection.clone(),
            })
            .collect()
    }
    pub fn is_current(&self, snapshot: &Snapshot) -> bool {
        self.records
            .get(&snapshot.key)
            .is_some_and(|r| r.revision == snapshot.revision)
    }
    pub fn prepare_participant_cache(
        &mut self,
        snapshot: &Snapshot,
        observed: &CachedParticipantIds,
    ) -> Option<Vec<String>> {
        let record = self.records.get_mut(&snapshot.key)?;
        if record.revision != snapshot.revision {
            return None;
        }
        let active = &snapshot.projection.participants;
        let active_identities: std::collections::BTreeSet<_> =
            active.iter().map(|p| p.identity.0.as_str()).collect();
        // A failed write can have reached Redis. Keep only attempted rows
        // still observed plus this attempt's writes, never a join history.
        record
            .possibly_cached
            .retain(|pair| observed.contains(pair));
        let retired = record
            .possibly_cached
            .iter()
            .filter(|(identity, _)| !active_identities.contains(identity.as_str()))
            .map(|(identity, _)| identity.clone())
            .collect();
        record.possibly_cached.extend(
            active
                .iter()
                .map(|p| (p.identity.0.clone(), p.sid.0.clone())),
        );
        Some(retired)
    }
    pub fn acknowledge(&mut self, snapshot: &Snapshot) {
        if self.fatal {
            return;
        }
        if let Some(record) = self.records.get_mut(&snapshot.key)
            && record.revision == snapshot.revision
        {
            if record.projection.terminal {
                self.records.remove(&snapshot.key);
            } else {
                record.acknowledged = snapshot.revision;
                record.possibly_cached = snapshot
                    .projection
                    .participants
                    .iter()
                    .map(|p| (p.identity.0.clone(), p.sid.0.clone()))
                    .collect();
            }
        }
    }
    pub fn unresolved(&self) -> usize {
        self.records
            .values()
            .filter(|r| r.started && r.revision != r.acknowledged)
            .count()
    }
    pub fn quiescent(&self) -> bool {
        !self.fatal
            && self
                .records
                .values()
                .all(|r| r.started && r.revision == r.acknowledged)
    }
    pub fn len(&self) -> usize {
        self.records.len()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn room(name: &str, sid: &str) -> RoomInfo {
        RoomInfo {
            turn_password: String::new(),
            sid: sid.into(),
            name: name.into(),
            empty_timeout_s: 300,
            departure_timeout_s: 20,
            max_participants: 0,
            creation_time_ms: 1,
            metadata: String::new(),
            num_participants: 0,
            num_publishers: 0,
            active_recording: false,
            enabled_codecs: vec![],
            version: rtc_core::model::TimedVersion {
                unix_micro: 1,
                ticks: 0,
            },
        }
    }
    #[test]
    fn terminal_old_and_new_same_name_coexist_bounded_without_eviction() {
        let mut l = Ledger::new(2).unwrap();
        let old = room("same", "RM_old");
        let new = room("same", "RM_new");
        l.reserve(&old).unwrap();
        l.start(&old);
        l.finish(&old);
        l.reserve(&new).unwrap();
        l.start(&new);
        assert_eq!(l.len(), 2);
        assert_eq!(l.unresolved(), 2);
        assert!(l.reserve(&room("third", "RM_third")).is_err());
        let old_snapshot = l
            .pending()
            .into_iter()
            .find(|s| s.key.1 == "RM_old")
            .unwrap();
        l.acknowledge(&old_snapshot);
        assert_eq!(l.len(), 1);
        assert_eq!(l.pending()[0].key.1, "RM_new");
    }
    #[test]
    fn acknowledgement_never_clears_newer_mutation_or_failure() {
        let mut l = Ledger::new(1).unwrap();
        let r = room("a", "RM_a");
        l.reserve(&r).unwrap();
        l.start(&r);
        let stale = l.pending()[0].clone();
        let mut newer = stale.projection.as_ref().clone();
        newer.room.metadata = "new".into();
        l.project(newer);
        l.acknowledge(&stale);
        assert_eq!(l.unresolved(), 1);
        l.failed = true;
        assert!(l.reserve(&room("b", "RM_b")).is_err());
        let latest = l.pending()[0].clone();
        l.acknowledge(&latest);
        assert!(l.quiescent());
        assert_eq!(l.len(), 1, "live clean authority remains reserved");
    }
    #[test]
    fn repeated_same_projection_and_notify_coalescing_cannot_grow_history() {
        let mut l = Ledger::new(1).unwrap();
        let r = room("a", "RM_a");
        l.reserve(&r).unwrap();
        l.start(&r);
        let first = l.pending()[0].clone();
        for _ in 0..10000 {
            l.project((*first.projection).clone());
        }
        assert_eq!(l.len(), 1);
        assert_eq!(l.pending()[0].revision, first.revision);
        l.finish(&r);
        let terminal = l.pending()[0].clone();
        l.project((*first.projection).clone());
        assert!(l.pending()[0].projection.terminal);
        l.acknowledge(&terminal);
        assert_eq!(l.len(), 0);
    }
    #[test]
    fn failed_construction_releases_only_unstarted_reservation() {
        let mut l = Ledger::new(1).unwrap();
        let r = room("a", "RM_a");
        l.reserve(&r).unwrap();
        l.cancel(&r);
        assert_eq!(l.len(), 0);
        l.reserve(&r).unwrap();
        l.start(&r);
        l.cancel(&r);
        assert_eq!(l.len(), 1);
    }
    #[test]
    fn revision_overflow_fails_closed_without_acknowledging_unique_state() {
        let mut l = Ledger::new(1).unwrap();
        let r = room("a", "RM_a");
        l.reserve(&r).unwrap();
        l.start(&r);
        l.records
            .get_mut(&("a".into(), "RM_a".into()))
            .unwrap()
            .revision = u64::MAX;
        let old = l.pending()[0].clone();
        let mut changed = (*old.projection).clone();
        changed.room.metadata = "last".into();
        l.project(changed);
        assert!(l.fatal);
        l.acknowledge(&old);
        assert_eq!(l.unresolved(), 1);
        assert!(l.reserve(&room("b", "RM_b")).is_err());
    }
}
