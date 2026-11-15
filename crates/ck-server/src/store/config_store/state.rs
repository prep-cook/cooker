//! Lossless accepted state. SQLite is a bootstrap decoder and fresh image
//! encoder, never a source for online business queries or candidates.
use super::*;
use std::collections::BTreeMap;
use std::sync::{Arc, MutexGuard};

#[derive(Clone)]
pub(crate) struct Document {
    pub bytes: Arc<Vec<u8>>,
    pub rev: i64,
}

#[derive(Clone)]
pub(super) struct HistoryChange {
    pub kind: Kind,
    pub id: String,
    pub before: Option<String>,
    pub after: Option<String>,
}

#[derive(Clone)]
pub(crate) struct State {
    pub resources: Arc<Vec<Resource>>,
    pub(super) revisions: BTreeMap<u64, Arc<Revision>>,
    pub(super) changes: BTreeMap<u64, Arc<Vec<HistoryChange>>>,
    pub documents: BTreeMap<String, Document>,
    pub metadata: BTreeMap<String, String>,
    pub cooldowns: BTreeMap<String, String>,
    pub envelope_keys: Arc<super::KeyRing>,
}

impl State {
    /// Called only while bootstrapping (or explicitly seeding a test fixture).
    pub(super) fn decode(conn: &Connection) -> anyhow::Result<Self> {
        let resources = Arc::new(all(conn)?);
        let mut query = conn.prepare("SELECT rev, at, actor, message, digest FROM revision ORDER BY rev")?;
        let revisions = query
            .query_map([], |r| {
                let rev = r.get::<_, i64>(0)? as u64;
                Ok((
                    rev,
                    Arc::new(Revision {
                        rev,
                        at: r.get(1)?,
                        actor: r.get(2)?,
                        message: r.get(3)?,
                        digest: r.get(4)?,
                    }),
                ))
            })?
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let mut changes: BTreeMap<u64, Arc<Vec<HistoryChange>>> = BTreeMap::new();
        let mut query = conn.prepare("SELECT rev, kind, id, before, after FROM change ORDER BY rev, kind, id")?;
        for row in query.query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)? as u64,
                HistoryChange {
                    kind: stored_kind(&r.get::<_, String>(1)?)?,
                    id: r.get(2)?,
                    before: r.get(3)?,
                    after: r.get(4)?,
                },
            ))
        })? {
            let (rev, change) = row?;
            Arc::make_mut(changes.entry(rev).or_default()).push(change);
        }
        let mut query = conn.prepare("SELECT name, bytes, rev FROM credential_document ORDER BY name")?;
        let documents = query
            .query_map([], |r| {
                Ok((
                    r.get(0)?,
                    Document {
                        bytes: Arc::new(r.get(1)?),
                        rev: r.get(2)?,
                    },
                ))
            })?
            .collect::<Result<_, _>>()?;
        let mut query = conn.prepare("SELECT name, value FROM credential_meta ORDER BY name")?;
        let (envelope_keys, metadata) = super::KeyRing::decode(
            query
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
                .collect::<Result<_, _>>()?,
        )?;
        let mut query = conn.prepare("SELECT id, record FROM credential_cooldown ORDER BY id")?;
        let cooldowns = query
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<Result<_, _>>()?;
        Ok(Self {
            resources,
            revisions,
            changes,
            documents,
            metadata,
            cooldowns,
            envelope_keys: Arc::new(envelope_keys),
        })
    }

    pub fn rev(&self) -> u64 {
        self.revisions.last_key_value().map_or(0, |(rev, _)| *rev)
    }

    pub fn put_document(&mut self, name: String, bytes: Option<Vec<u8>>) {
        match bytes {
            Some(bytes) => {
                if self.documents.get(&name).is_some_and(|held| *held.bytes == bytes) {
                    return;
                }
                let rev = self.documents.get(&name).map_or(1, |held| held.rev + 1);
                self.documents.insert(
                    name,
                    Document {
                        bytes: Arc::new(bytes),
                        rev,
                    },
                );
            }
            None => {
                self.documents.remove(&name);
            }
        }
    }
}

/// A durable candidate retains its original configuration event even after the
/// canonical pointer advances. Required projection retry is not a new commit.
#[derive(Clone)]
pub(crate) struct Pending {
    pub(crate) state: Arc<State>,
    pub(super) configuration: Option<Arc<ConfigurationView>>,
}

/// The owner unlocks before component teardown can join owner-dependent workers.
pub(crate) struct Writer<'a> {
    store: &'a ConfigStore,
    pending: Option<MutexGuard<'a, Option<Pending>>>,
}
impl std::ops::Deref for Writer<'_> {
    type Target = Option<Pending>;
    fn deref(&self) -> &Self::Target {
        self.pending.as_ref().unwrap()
    }
}
impl std::ops::DerefMut for Writer<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.pending.as_mut().unwrap()
    }
}
impl Drop for Writer<'_> {
    fn drop(&mut self) {
        let pins = std::mem::take(&mut *self.store.admission_pins.lock().unwrap_or_else(PoisonError::into_inner));
        drop(self.pending.take());
        drop(pins);
    }
}

impl ConfigStore {
    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn fixture_execute(&self, sql: &str) -> anyhow::Result<()> {
        let mut pending = self.writer()?;
        // Seed fixture SQL from accepted state, never a stale/bypassed checkpoint.
        let mut conn = Connection::open_in_memory()?;
        db::initialize(&mut conn, false, true)?;
        self.current().write_rows(&mut conn)?;
        conn.execute_batch(sql)?;
        let state = State::decode(&conn)?;
        conn.close().map_err(|(_, error)| error)?;
        self.accept(state, &mut pending)
    }

    pub(crate) fn current(&self) -> Arc<State> {
        self.state.read().unwrap_or_else(PoisonError::into_inner).clone()
    }

    /// One owner gate for every domain. Reconcile a retained checkpoint before a
    /// new candidate can be derived; a failed retry leaves ordinary writes closed.
    pub(crate) fn writer(&self) -> anyhow::Result<Writer<'_>> {
        let mut pending = self.grant_writer()?;
        self.finish_logins(&mut pending)?;
        if let Some(retained) = pending.clone() {
            self.persist(&retained, &mut pending)?;
            self.install_accepted(&retained)?;
            *pending = None;
        }
        Ok(pending)
    }

    /// Only an already-consumed result may enter without first recovering storage.
    /// Network work and ordinary commands must enter `writer` instead.
    pub(crate) fn grant_writer(&self) -> anyhow::Result<Writer<'_>> {
        self.check_reentry()?;
        Ok(Writer {
            store: self,
            pending: Some(self.writer.lock().unwrap_or_else(PoisonError::into_inner)),
        })
    }

    pub(crate) fn block_grant_source(&self, name: String) {
        self.grant_sources
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(name, None);
    }

    pub(crate) fn hold_login(&self, grant: crate::management::api::HeldLogin) {
        // Before the legacy renderer identifies its exact filename, withhold the
        // provider's existing sources. Failure to create a render workspace must
        // not permit dispatch of a token this returned login may have invalidated.
        let pending = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let current = pending.as_ref().map_or_else(|| self.current(), |p| p.state.clone());
        let mut blocked = self.held_login_sources.write().unwrap_or_else(PoisonError::into_inner);
        for (name, document) in &current.documents {
            if serde_json::from_slice::<serde_json::Value>(&document.bytes)
                .ok()
                .and_then(|raw| raw.get("type").and_then(serde_json::Value::as_str).map(str::to_owned))
                .is_some_and(|provider| {
                    crate::management::api::oauth_provider_key(&provider)
                        == crate::management::api::oauth_provider_key(&grant.provider)
                })
            {
                blocked.insert(name.clone());
            }
        }
        drop(blocked);
        self.held_logins
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(grant);
    }

    pub(crate) fn reject_login_sources(&self, current: &State, destinations: &BTreeMap<String, [u8; 32]>) {
        let mut rejected = self
            .rejected_login_sources
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        for (name, hash) in destinations {
            if let Some(document) = current.documents.get(name)
                && crate::runtime::state::sha256(&document.bytes) == *hash
                && !ck_core::config::credentials::scan_documents(
                    &ck_core::config::Config::default(),
                    vec![(std::path::PathBuf::from(name), document.bytes.as_ref().clone())],
                )
                .credentials
                .is_empty()
            {
                rejected.insert(name.clone(), *hash);
            }
        }
    }

    fn withhold_logins<'a>(
        &self,
        current: &State,
        grants: impl Iterator<Item = &'a crate::management::api::HeldLogin>,
    ) {
        let providers = grants
            .map(|grant| crate::management::api::oauth_provider_key(&grant.provider))
            .collect::<Vec<_>>();
        let mut blocked = self.held_login_sources.write().unwrap_or_else(PoisonError::into_inner);
        blocked.clear();
        for (name, document) in &current.documents {
            if serde_json::from_slice::<serde_json::Value>(&document.bytes)
                .ok()
                .and_then(|raw| raw.get("type").and_then(serde_json::Value::as_str).map(str::to_owned))
                .is_some_and(|provider| providers.contains(&crate::management::api::oauth_provider_key(&provider)))
            {
                blocked.insert(name.clone());
            }
        }
    }

    fn finish_logins(&self, pending: &mut Option<Pending>) -> anyhow::Result<()> {
        let grants = std::mem::take(&mut *self.held_logins.lock().unwrap_or_else(PoisonError::into_inner));
        let mut grants = grants.into_iter();
        while let Some(grant) = grants.next() {
            let current = pending.as_ref().map_or_else(|| self.current(), |p| p.state.clone());
            // A panic/local effect retains the only result. A terminal refusal
            // cannot become an unsatisfiable prerequisite for administrative repair.
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| grant.finish(&current, pending)));
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) if error.is::<crate::management::api::LoginRejected>() => {
                    grant.reject(&current, self);
                }
                error => {
                    let mut held = self.held_logins.lock().unwrap_or_else(PoisonError::into_inner);
                    held.push(grant);
                    held.extend(grants);
                    self.withhold_logins(&current, held.iter());
                    drop(held);
                    return match error {
                        Ok(Err(error)) => Err(error),
                        Err(panic) => std::panic::resume_unwind(panic),
                        _ => unreachable!(),
                    };
                }
            }
            self.withhold_logins(&current, grants.as_slice().iter());
        }
        Ok(())
    }

    /// Whether any source is fenced at all, so dispatch checks of unfenced credentials
    /// skip building their document names (each request examines every candidate).
    pub(crate) fn any_grant_source(&self) -> bool {
        !self
            .rejected_login_sources
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
            || !self
                .held_login_sources
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .is_empty()
            || !self
                .grant_sources
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .is_empty()
    }

    pub(crate) fn grant_source(&self, name: &str) -> Option<Option<[u8; 32]>> {
        if self
            .rejected_login_sources
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(name)
        {
            return Some(None);
        }
        if self
            .held_login_sources
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(name)
        {
            return Some(None);
        }
        self.grant_sources
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .copied()
    }

    /// A consumed grant is retained even before checkpoint installation succeeds.
    /// There is no power-loss durability promise until storage accepts the bytes.
    pub(crate) fn accept_grant(
        &self,
        candidate: State,
        name: String,
        pending: &mut Option<Pending>,
    ) -> anyhow::Result<()> {
        self.accept_grants(candidate, &[name], pending)
    }

    /// A completed provider batch remains atomic even when only preservation is
    /// possible. Every member derives from the latest pending candidate.
    pub(crate) fn accept_grants(
        &self,
        candidate: State,
        names: &[String],
        pending: &mut Option<Pending>,
    ) -> anyhow::Result<()> {
        self.retain_grants(candidate, names, pending)?;
        self.resolve_grants(pending)
    }

    pub(crate) fn retain_grants(
        &self,
        candidate: State,
        names: &[String],
        pending: &mut Option<Pending>,
    ) -> anyhow::Result<()> {
        self.check_keys(&candidate, pending)?;
        let configuration = match pending.as_ref() {
            Some(retained) => retained.configuration.clone(),
            None => self.changed_configuration(&candidate)?,
        };
        {
            let mut rejected = self
                .rejected_login_sources
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            for name in names {
                rejected.remove(name);
            }
        }
        {
            let mut sources = self.grant_sources.write().unwrap_or_else(PoisonError::into_inner);
            for name in names {
                sources.insert(
                    name.clone(),
                    candidate
                        .documents
                        .get(name)
                        .map(|document| Sha256::digest(document.bytes.as_slice()).into()),
                );
            }
        }
        let retained = Pending {
            state: Arc::new(candidate),
            configuration,
        };
        *pending = Some(retained.clone());
        if let Some(disk) = &self.disk {
            disk.recovery(true);
        }
        Ok(())
    }

    pub(crate) fn resolve_grants(&self, pending: &mut Option<Pending>) -> anyhow::Result<()> {
        let Some(retained) = pending.clone() else { return Ok(()) };
        self.persist(&retained, pending)?;
        self.install_accepted(&retained)?;
        *pending = None;
        Ok(())
    }

    /// Called only by an invoked adapter, while its Writer owns admission.
    pub(crate) fn pin_configuration_owner<T: Send + Sync + 'static>(&self, owner: Arc<T>) {
        self.admission_pins
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(owner);
    }

    /// Install the complete checkpoint before publication. Only a candidate whose
    /// checkpoint was replaced becomes pending, including metadata-sync failure.
    pub(crate) fn accept(&self, candidate: State, pending: &mut Option<Pending>) -> anyhow::Result<()> {
        self.check_keys(&candidate, pending)?;
        let retained = Pending {
            configuration: self.changed_configuration(&candidate)?,
            state: Arc::new(candidate),
        };
        self.persist(&retained, pending)?;
        // Even a memory-only owner must retain an incomplete required projection.
        *pending = Some(retained.clone());
        self.install_accepted(&retained)?;
        *pending = None;
        Ok(())
    }

    fn changed_configuration(&self, candidate: &State) -> anyhow::Result<Option<Arc<ConfigurationView>>> {
        let previous = self.current();
        if previous.rev() == candidate.rev() && previous.resources == candidate.resources {
            return Ok(None);
        }
        Ok(Some(Self::configuration_view(
            candidate,
            ConfigurationChange::Committed,
        )?))
    }

    fn install_accepted(&self, retained: &Pending) -> anyhow::Result<()> {
        self.rejected_login_sources
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|name, hash| {
                retained
                    .state
                    .documents
                    .get(name)
                    .is_some_and(|document| crate::runtime::state::sha256(&document.bytes) == *hash)
            });
        {
            let mut sources = self.grant_sources.write().unwrap_or_else(PoisonError::into_inner);
            for (name, hash) in sources.iter_mut() {
                // A deletion remains a fence until a legitimate new source is adopted.
                *hash = retained
                    .state
                    .documents
                    .get(name)
                    .map(|document| Sha256::digest(document.bytes.as_slice()).into());
            }
        }
        *self.state.write().unwrap_or_else(PoisonError::into_inner) = retained.state.clone();
        if let Some(view) = &retained.configuration {
            self.dispatch_configuration(view.clone())?;
        }
        if let Some(disk) = &self.disk {
            disk.recovery(false);
        }
        Ok(())
    }
}
