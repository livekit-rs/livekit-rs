//! First-phase platform control client. Media admission is gated separately.

mod wire;

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use wire::Wire;

const REGISTER: u8 = 1;
const HEARTBEAT: u8 = 2;
const SNAPSHOT: u8 = 3;
const ACK: u8 = 4;
const BUDGET: Duration = Duration::from_secs(5);
const MAX_STATE: u64 = 17 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("control endpoint must be a WSS URL without user credentials")]
    Endpoint,
    #[error("control TLS configuration or peer verification failed")]
    Tls,
    #[error("control transport closed or failed")]
    Transport,
    #[error("control request timed out")]
    Timeout,
    #[error("invalid control frame")]
    Frame,
    #[error("invalid control JSON")]
    Json,
    #[error("platform rejected control operation (code {0})")]
    Rejected(i64),
    #[error("control state could not be read, locked or persisted")]
    Storage,
    #[error("invalid control registration or snapshot")]
    State,
}

#[derive(Clone, Deserialize, Serialize)]
struct Snapshot {
    contract_version: u8,
    region: String,
    config_version: u64,
    snapshot: bool,
    issued_at: u64,
    valid_until: u64,
    projects: Vec<Value>,
}

impl Snapshot {
    fn validate(&self, region: &str, now: u64) -> Result<(), Error> {
        if self.contract_version != 1
            || self.region != region
            || !self.snapshot
            || self.issued_at > now.saturating_add(30_000)
            || self.valid_until <= now
            || self.valid_until.saturating_sub(self.issued_at) > 3_600_000
            || self.issued_at >= self.valid_until
        {
            return Err(Error::State);
        }
        let mut ids = std::collections::HashSet::new();
        let mut keys = std::collections::HashSet::new();
        for project in &self.projects {
            let field = |name| {
                project
                    .get(name)
                    .and_then(Value::as_str)
                    .filter(|v| !v.is_empty())
            };
            if !ids.insert(field("project_id").ok_or(Error::State)?)
                || !keys.insert(field("api_key").ok_or(Error::State)?)
                || field("secret").is_none()
                || field("status").is_none()
            {
                return Err(Error::State);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Deserialize, Serialize)]
struct Stored {
    region: String,
    node_id: String,
    credential_file: PathBuf,
    generation: u64,
    server_epoch: u64,
    snapshot: Option<Snapshot>,
}

#[derive(Deserialize)]
struct Reply<T> {
    code: i64,
    data: Option<T>,
}

impl<T> Reply<T> {
    fn checked(self) -> Result<Option<T>, Error> {
        if self.code != 0 {
            return Err(Error::Rejected(self.code));
        }
        Ok(self.data)
    }
}

#[derive(Deserialize)]
struct Heartbeat {
    config_version: u64,
}

#[derive(Deserialize)]
struct Registration {
    generation: u64,
    config_version: u64,
    heartbeat_interval_ms: u64,
}

pub struct ControlNode {
    directory: PathBuf,
    _lock: File,
    stored: Stored,
    confirmed: bool,
}

impl ControlNode {
    pub fn open(
        directory: &Path,
        region: &str,
        node_id: &str,
        credential_file: &Path,
    ) -> Result<Self, Error> {
        if region.is_empty() || node_id.is_empty() || !credential_file.is_absolute() {
            return Err(Error::State);
        }
        let mut builder = std::fs::DirBuilder::new();
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
        if let Err(error) = builder.create(directory)
            && error.kind() != std::io::ErrorKind::AlreadyExists
        {
            return Err(Error::Storage);
        }
        let metadata = std::fs::symlink_metadata(directory).map_err(|_| Error::Storage)?;
        if !metadata.is_dir() || metadata.permissions().mode() & 0o077 != 0 {
            return Err(Error::Storage);
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(directory.join("lock"))
            .map_err(|_| Error::Storage)?;
        lock.try_lock().map_err(|_| Error::Storage)?;
        let stored = match File::open(directory.join("control.json")) {
            Ok(file) => {
                let bytes = read_bounded(file, MAX_STATE)?;
                let stored: Stored = serde_json::from_slice(&bytes).map_err(|_| Error::Storage)?;
                if stored.region != region
                    || stored.node_id != node_id
                    || stored.credential_file != credential_file
                {
                    return Err(Error::State);
                }
                stored
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Stored {
                region: region.to_owned(),
                node_id: node_id.to_owned(),
                credential_file: credential_file.to_owned(),
                generation: 0,
                server_epoch: 0,
                snapshot: None,
            },
            Err(_) => return Err(Error::Storage),
        };
        let mut node = Self {
            directory: directory.to_owned(),
            _lock: lock,
            stored,
            confirmed: false,
        };
        let mut next = node.stored.clone();
        next.server_epoch = next.server_epoch.checked_add(1).ok_or(Error::State)?;
        node.persist(next)?;
        Ok(node)
    }

    fn persist(&mut self, next: Stored) -> Result<(), Error> {
        let pending = self.directory.join("control.pending");
        match std::fs::remove_file(&pending) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(Error::Storage),
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&pending)
            .map_err(|_| Error::Storage)?;
        serde_json::to_writer(&mut file, &next).map_err(|_| Error::Storage)?;
        file.flush()
            .and_then(|()| file.sync_all())
            .map_err(|_| Error::Storage)?;
        std::fs::rename(pending, self.directory.join("control.json"))
            .map_err(|_| Error::Storage)?;
        // Update the local view after rename even if directory fsync fails.
        // The caller still receives the failure and cannot confirm this state.
        self.stored = next;
        File::open(&self.directory)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| Error::Storage)
    }

    fn apply(&mut self, mut incoming: Snapshot, now: u64) -> Result<(), Error> {
        incoming.validate(&self.stored.region, now)?;
        incoming
            .projects
            .sort_by(|a, b| a["project_id"].as_str().cmp(&b["project_id"].as_str()));
        if let Some(current) = &self.stored.snapshot {
            if incoming.config_version < current.config_version
                || (incoming.config_version == current.config_version
                    && (incoming.projects != current.projects
                        || incoming.issued_at < current.issued_at
                        || incoming.valid_until < current.valid_until))
            {
                return Err(Error::State);
            }
        }
        let mut next = self.stored.clone();
        next.snapshot = Some(incoming);
        self.persist(next)
    }

    pub fn applied_version(&self) -> Option<u64> {
        self.stored
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.config_version)
    }

    pub fn control_confirmed(&self) -> bool {
        self.confirmed
            && self
                .stored
                .snapshot
                .as_ref()
                .is_some_and(|snapshot| snapshot.valid_until > now_ms())
    }

    /// Maintain the control connection until the caller's shutdown resolves.
    pub async fn supervise(
        &mut self,
        endpoint: &str,
        ca_file: Option<&Path>,
        build_sha: &str,
        shutdown: impl Future<Output = ()>,
    ) {
        tokio::pin!(shutdown);
        let mut backoff = Duration::from_millis(250);
        loop {
            let started = tokio::time::Instant::now();
            let failure = tokio::select! {
                biased;
                () = &mut shutdown => break,
                failure = self.run_session(endpoint, ca_file, build_sha) => failure,
            };
            if let Err(error) = failure {
                tracing::warn!(%error, "cloud control session ended");
            }
            if started.elapsed() >= Duration::from_secs(30) {
                backoff = Duration::from_millis(250);
            }
            let jitter =
                Duration::from_millis(rand::random_range(0..=backoff.as_millis() as u64 / 2));
            tokio::select! {
                biased;
                () = &mut shutdown => break,
                () = tokio::time::sleep(backoff + jitter) => {}
            }
            backoff = (backoff * 2).min(Duration::from_secs(20));
        }
        self.confirmed = false;
    }

    /// Run one authenticated control session until transport failure.
    async fn run_session(
        &mut self,
        endpoint: &str,
        ca_file: Option<&Path>,
        build_sha: &str,
    ) -> Result<(), Error> {
        self.confirmed = false;
        let result = self.session(endpoint, ca_file, build_sha).await;
        self.confirmed = false;
        result
    }

    async fn session(
        &mut self,
        endpoint: &str,
        ca_file: Option<&Path>,
        build_sha: &str,
    ) -> Result<(), Error> {
        let mut wire = tokio::time::timeout(BUDGET, Wire::connect(endpoint, ca_file))
            .await
            .map_err(|_| Error::Timeout)??;
        let bytes = read_bounded(
            File::open(&self.stored.credential_file).map_err(|_| Error::Storage)?,
            4096,
        )?;
        let credential = std::str::from_utf8(&bytes)
            .map_err(|_| Error::State)?
            .trim();
        if credential.is_empty() {
            return Err(Error::State);
        }
        let identity = json!({
            "contract_version": 1, "region": self.stored.region, "node_id": self.stored.node_id,
            "credential": credential, "build_sha": build_sha, "capabilities": [],
            "applied_version": self.applied_version().unwrap_or(0), "server_epoch": self.stored.server_epoch,
        });
        let (reply, _) = wire
            .request::<_, Reply<Registration>>(REGISTER, &identity, BUDGET)
            .await?;
        let registered = reply.checked()?.ok_or(Error::State)?;
        if registered.generation <= self.stored.generation
            || !(1_000..=30_000).contains(&registered.heartbeat_interval_ms)
            || self
                .applied_version()
                .is_some_and(|version| registered.config_version < version)
        {
            return Err(Error::State);
        }
        let mut next = self.stored.clone();
        next.generation = registered.generation;
        self.persist(next)?;

        let mut changed = self
            .sync(&mut wire, build_sha, registered.config_version)
            .await?;
        let heartbeat_period = Duration::from_millis(registered.heartbeat_interval_ms);
        let mut heartbeat = tokio::time::interval(heartbeat_period);
        let sync_period = Duration::from_secs(30);
        let mut poll =
            tokio::time::interval_at(tokio::time::Instant::now() + sync_period, sync_period);
        loop {
            if changed {
                changed = self.sync(&mut wire, build_sha, 0).await?;
            }
            tokio::select! {
                _ = heartbeat.tick() => {
                    let (reply, pending) = wire.request::<_, Reply<Heartbeat>>(HEARTBEAT, &json!({
                        "contract_version": 1, "region": self.stored.region, "server_id": self.stored.node_id,
                        "build_sha": build_sha, "capabilities": [], "projects": [], "state": "STARTING",
                        "applied_version": self.applied_version(), "reported_at": now_ms(),
                    }), BUDGET).await?;
                    let heartbeat = reply.checked()?.ok_or(Error::State)?;
                    changed = pending || self.applied_version().is_none_or(|version| heartbeat.config_version > version);
                }
                _ = poll.tick() => changed = true,
                packet = wire.read() => {
                    let packet = packet?;
                    if packet.packet_type() != msgtrans::PacketType::OneWay || packet.biz_type() != wire::CONFIG_CHANGED {
                        return Err(Error::Frame);
                    }
                    changed = true;
                }
            }
        }
    }

    async fn sync(
        &mut self,
        wire: &mut Wire,
        build_sha: &str,
        minimum_version: u64,
    ) -> Result<bool, Error> {
        let (reply, pending) = wire
            .request::<_, Reply<Snapshot>>(
                SNAPSHOT,
                &json!({"since": self.applied_version()}),
                BUDGET,
            )
            .await?;
        let incoming = reply.checked()?.ok_or(Error::State)?;
        if incoming.config_version < minimum_version {
            return Err(Error::State);
        }
        self.apply(incoming, now_ms())?;
        let (reply, changed) = wire.request::<_, Reply<bool>>(ACK, &json!({
            "contract_version": 1, "region": self.stored.region, "server_id": self.stored.node_id,
            "build_sha": build_sha, "applied_version": self.applied_version(), "applied_at": now_ms(), "errors": [],
        }), BUDGET).await?;
        if reply.checked()? != Some(true) {
            return Err(Error::State);
        }
        self.confirmed = true;
        Ok(pending || changed)
    }
}

fn read_bounded(file: File, limit: u64) -> Result<Vec<u8>, Error> {
    let mut result = Vec::new();
    file.take(limit + 1)
        .read_to_end(&mut result)
        .map_err(|_| Error::Storage)?;
    if result.len() as u64 > limit {
        return Err(Error::Storage);
    }
    Ok(result)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(version: u64, issued_at: u64, valid_until: u64) -> Snapshot {
        Snapshot {
            contract_version: 1,
            region: "test-1".into(),
            config_version: version,
            snapshot: true,
            issued_at,
            valid_until,
            projects: vec![
                json!({"project_id": "a", "status": "ACTIVE", "api_key": "test-key", "secret": "test-secret"}),
            ],
        }
    }

    struct Temp(PathBuf);

    impl Temp {
        fn new() -> Self {
            let mut nonce = [0; 16];
            getrandom::fill(&mut nonce).unwrap();
            Self(std::env::temp_dir().join(format!("lk-control-{:x}", u128::from_ne_bytes(nonce))))
        }
    }

    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn version_renewal_rejects_rollback_and_survives_restart() {
        let temp = Temp::new();
        let credential = temp.0.join("credential");
        {
            let mut node = ControlNode::open(&temp.0, "test-1", "node-a", &credential).unwrap();
            assert!(ControlNode::open(&temp.0, "test-1", "node-a", &credential).is_err());
            node.apply(snapshot(41, 10, 40), 11).unwrap();
            node.apply(snapshot(42, 20, 50), 21).unwrap();
            assert!(node.apply(snapshot(41, 30, 60), 31).is_err());
            let mut rewritten = snapshot(42, 30, 60);
            rewritten.projects[0]["secret"] = "rewritten".into();
            assert!(node.apply(rewritten, 31).is_err());
            node.apply(snapshot(42, 30, 60), 31).unwrap();
            assert_eq!(node.stored.snapshot.as_ref().unwrap().valid_until, 60);
            assert_eq!(node.stored.server_epoch, 1);
        }
        assert!(ControlNode::open(&temp.0, "other", "node-a", &credential).is_err());
        let node = ControlNode::open(&temp.0, "test-1", "node-a", &credential).unwrap();
        assert_eq!(node.applied_version(), Some(42));
        assert_eq!(node.stored.server_epoch, 2);
        assert!(!node.control_confirmed());
        assert_eq!(
            std::fs::metadata(temp.0.join("control.json"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    #[test]
    fn invalid_snapshots_and_disk_failure_do_not_confirm_a_version() {
        let temp = Temp::new();
        let mut node =
            ControlNode::open(&temp.0, "test-1", "node-a", &temp.0.join("credential")).unwrap();
        assert!(node.apply(snapshot(41, 10, 20), 20).is_err());
        let mut duplicate = snapshot(41, 10, 20);
        duplicate.projects.push(duplicate.projects[0].clone());
        assert!(node.apply(duplicate, 11).is_err());
        std::fs::create_dir(temp.0.join("control.pending")).unwrap();
        assert!(matches!(
            node.apply(snapshot(41, 10, 20), 11),
            Err(Error::Storage)
        ));
        assert_eq!(node.applied_version(), None);
        assert!(!node.control_confirmed());
    }

    #[tokio::test]
    #[ignore = "requires the pinned Kotlin/Native SPEC-0078 WSS fixture"]
    async fn kotlin_native_wss_registration_snapshot_ack_and_heartbeat() {
        let endpoint = std::env::var("LK_CLOUD_TEST_WSS").unwrap();
        let ca_file = PathBuf::from(std::env::var_os("LK_CLOUD_TEST_CA").unwrap());
        let other_ca = PathBuf::from(std::env::var_os("LK_CLOUD_TEST_OTHER_CA").unwrap());
        assert!(matches!(
            Wire::connect(&endpoint, Some(&other_ca)).await,
            Err(Error::Tls)
        ));
        let temp = Temp::new();
        let credential = temp.0.join("credential");
        let mut node = ControlNode::open(&temp.0, "test-1", "node-a", &credential).unwrap();
        std::fs::write(&credential, "fixture-only").unwrap();
        node.supervise(
            &endpoint,
            Some(&ca_file),
            "fixture-build",
            tokio::time::sleep(Duration::from_secs(5)),
        )
        .await;
        assert_eq!(node.applied_version(), Some(42));
        assert_eq!(node.stored.generation, 2);
        assert!(!node.control_confirmed());
    }
}
