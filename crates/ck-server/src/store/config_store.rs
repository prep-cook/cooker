//! The config store: every setting, provider key, client key and model router as one
//! row of a SQLite database, written in transactions that each make one
//! numbered revision. The runtime reads the [`Config`] the rows compile to
//! ([`resources::config`]).
//!
//! Disk connections exist only for storage operations; accepted business state
//! lives in immutable memory snapshots. One owner gate serializes domain mutations.
//! A fresh complete image replaces the private checkpoint and published file;
//! neither destination is an input to normal encoding.
//! Each completed write publishes an immutable, standalone snapshot at `--db`.
//! The private recovery checkpoint lives in OS tmp storage. History redacts secrets;
//! logical deletion does not promise physical disk erasure.

mod commit;
mod db;
mod encode;
mod history;
mod keyring;
mod notifications;
mod published;
pub mod resources;
mod state;

#[cfg(test)]
use db::head;
use db::{all, stored_kind};
use history::shown_change;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError, RwLock};

pub(crate) use state::{Pending, State, Writer};

use ck_core::config::Config;
use rusqlite::{Connection, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use sha2::{Digest, Sha256};

pub use keyring::{KeyRing, KeyState, Refused, Signer};
pub use notifications::{ConfigurationChange, ConfigurationSubscription, ConfigurationView};
pub use published::AlreadyOwned;
pub use resources::{Fault, Kind, Resource};

pub struct ConfigStore {
    state: RwLock<Arc<State>>,
    writer: Mutex<Option<state::Pending>>,
    // Owner-derived dispatch fence: a consumed grant requires these source bytes.
    grant_sources: RwLock<BTreeMap<String, Option<[u8; 32]>>>,
    held_logins: Mutex<Vec<crate::management::api::HeldLogin>>,
    held_login_sources: RwLock<std::collections::BTreeSet<String>>,
    rejected_login_sources: RwLock<BTreeMap<String, [u8; 32]>>,
    // Transient dispatch upgrades are released by Writer, after admission unlocks.
    admission_pins: Mutex<Vec<Arc<dyn std::any::Any + Send + Sync>>>,
    subscriptions: Arc<Mutex<notifications::Subscriptions>>,
    // Only in-memory stores retain a connection: closing it would discard their state.
    conn: Option<Mutex<Connection>>,
    path: Option<PathBuf>,
    disk: Option<published::Disk>,
}

pub(crate) enum StoreConnection<'a> {
    Memory(std::sync::MutexGuard<'a, Connection>),
    Disk {
        conn: Connection,
        // The live connection closes before the checkpoint may be replaced.
        _storage: std::sync::MutexGuard<'a, ()>,
    },
}

impl std::ops::Deref for StoreConnection<'_> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        match self {
            Self::Memory(conn) => conn,
            Self::Disk { conn, .. } => conn,
        }
    }
}

impl std::ops::DerefMut for StoreConnection<'_> {
    fn deref_mut(&mut self) -> &mut Connection {
        match self {
            Self::Memory(conn) => conn,
            Self::Disk { conn, .. } => conn,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Precondition {
    Revision(u64),
    AnyRevision(Vec<u64>),
    Exists,
}

/// One write: an operation on one resource, or on the document at a path.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Op {
    Check {
        kind: String,
        id: String,
        revisions: Option<Vec<u64>>,
    },
    /// Creates or replaces a resource. `if_rev` requires its current revision,
    /// `if_absent` that it does not exist. A new list entry without `ord` goes last.
    Put {
        kind: String,
        id: String,
        spec: Json,
        #[serde(default)]
        ord: Option<i64>,
        #[serde(default)]
        description: Option<String>,
        #[serde(default)]
        if_rev: Option<u64>,
        #[serde(default)]
        if_absent: bool,
    },
    /// Merges `patch` into an existing resource's spec (RFC 7396: `null` removes a key).
    Patch {
        kind: String,
        id: String,
        patch: Json,
        #[serde(default)]
        if_rev: Option<u64>,
    },
    Delete {
        kind: String,
        id: String,
        #[serde(default)]
        if_rev: Option<u64>,
    },
    /// Moves a list entry before or after another of its kind; neither moves it last.
    Move {
        kind: String,
        id: String,
        #[serde(default)]
        before: Option<String>,
        #[serde(default)]
        after: Option<String>,
        #[serde(default)]
        if_rev: Option<u64>,
    },
    /// Sets the value at a slash-separated path of the document `GET
    /// /admin/v1/effective/config` shows. `expect`, when sent, must equal what that
    /// document holds there now (an absent value equals `null`, `[]` and `{}`).
    Set {
        path: String,
        value: Json,
        #[serde(default, deserialize_with = "present")]
        expect: Option<Json>,
    },
    /// Removes the value at a document path.
    Unset {
        path: String,
        #[serde(default, deserialize_with = "present")]
        expect: Option<Json>,
    },
}

/// Tells an explicit `null` from an absent field.
fn present<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Json>, D::Error> {
    Json::deserialize(d).map(Some)
}

#[derive(Debug, Clone, Serialize)]
pub struct Revision {
    pub rev: u64,
    pub at: String,
    pub actor: String,
    pub message: String,
    pub digest: String,
}

/// One resource a revision changed, with secrets hidden. `None` before is a creation,
/// `None` after a deletion.
#[derive(Debug, Clone, Serialize)]
pub struct Change {
    pub kind: Kind,
    pub id: String,
    pub before: Option<Json>,
    pub after: Option<Json>,
}

/// A successful commit or dry run.
pub struct Committed {
    /// The new revision, or the current one when nothing changed or on a dry run.
    pub rev: u64,
    /// Whether a revision was written.
    pub written: bool,
    pub changes: Vec<Change>,
    pub config: Config,
    pub digest: String,
    /// The config file text the resources compiled to (secrets included).
    pub text: String,
    pub resources: Vec<Resource>,
}

impl std::fmt::Debug for Committed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Committed")
            .field("rev", &self.rev)
            .field("written", &self.written)
            .field("changes", &self.changes.len())
            .finish_non_exhaustive()
    }
}

/// The config the store holds now.
pub struct Loaded {
    pub rev: u64,
    pub config: Config,
    pub digest: String,
    /// The config file text the resources compiled to (secrets included).
    pub text: String,
    pub resources: Vec<Resource>,
}

#[derive(Debug)]
pub struct CommitError {
    pub status: u16,
    pub code: &'static str,
    /// Boxed: the error travels in every store `Result`, which stays small.
    pub fault: Box<Fault>,
}

impl CommitError {
    fn new(status: u16, code: &'static str, resource: Option<String>, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            fault: Box::new(Fault::new(resource, message)),
        }
    }
    /// A 422 for a config that cannot be stored.
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(422, "invalid_config", None, message)
    }
    fn at(mut self, path: &str) -> Self {
        self.fault.pointer = Some(format!("/{}", path.trim_matches('/')));
        self
    }
    fn db(e: impl std::fmt::Display) -> Self {
        Self::new(500, "store_failed", None, e.to_string())
    }
}

impl std::fmt::Display for CommitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.fault)
    }
}

impl std::error::Error for CommitError {}

impl ConfigStore {
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    pub(crate) fn conn(&self) -> anyhow::Result<StoreConnection<'_>> {
        self.connect(false)
    }

    fn connect(&self, writing: bool) -> anyhow::Result<StoreConnection<'_>> {
        match &self.conn {
            Some(conn) => Ok(StoreConnection::Memory(
                conn.lock().unwrap_or_else(PoisonError::into_inner),
            )),
            None => self
                .disk
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("missing working database"))?
                .connection(self, writing),
        }
    }

    /// Whether nothing was ever stored: a first start imports `config.yaml` then.
    pub fn is_empty(&self) -> anyhow::Result<bool> {
        Ok(self.current().revisions.is_empty())
    }

    /// A coherent immutable accepted version, independent of storage availability.
    pub fn snapshot(&self) -> anyhow::Result<(u64, Vec<Resource>)> {
        let state = self.current();
        Ok((state.rev(), state.resources.as_ref().clone()))
    }

    /// The config the stored resources compile to.
    pub fn load(&self) -> Result<Loaded, Fault> {
        let (rev, resources) = self.snapshot().map_err(|e| Fault::new(None, e.to_string()))?;
        let (config, text) = resources::config(&resources).map_err(|f| culprit(&resources, &resources, f))?;
        Ok(Loaded {
            rev,
            config,
            digest: public_digest(&resources),
            text,
            resources,
        })
    }
}

fn fault_422(f: Fault) -> CommitError {
    CommitError {
        status: 422,
        code: f.code.unwrap_or("invalid_config"),
        fault: Box::new(f),
    }
}

/// A document-wide fault, tied to the first changed resource that fails on its own.
fn culprit(current: &[Resource], next: &[Resource], fault: Fault) -> Fault {
    if fault.resource.is_some() {
        return fault;
    }
    let changed = next
        .iter()
        .filter(|r| !current.iter().any(|c| c == *r) || current.len() == next.len() && current == next);
    for r in changed {
        if let Err(mut alone) = resources::config(std::slice::from_ref(r)) {
            alone.resource = Some(r.key());
            return alone;
        }
    }
    fault
}

fn digest(text: &str) -> String {
    Sha256::digest(text.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn public_digest(resources: &[Resource]) -> String {
    let mut shown: Vec<_> = resources.iter().map(resources::redacted).collect();
    shown.sort_by(|a, b| (a.kind, a.ord, &a.id).cmp(&(b.kind, b.ord, &b.id)));
    for r in &mut shown {
        r.rev = 0;
        r.created_rev = 0;
    }
    let mut public = serde_json::to_value(&shown).expect("JSON resources");
    public.sort_all_objects();
    digest(&public.to_string())
}

#[cfg(test)]
mod tests;
