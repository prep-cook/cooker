//! Raw credential source documents in the config database. The private, stable
//! working directory is a cache for existing provider file protocols; only
//! SQLite is durable, and these rows never enter config revisions or exports.

use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{Context as _, bail};

use crate::store::config_store::{ConfigStore, State};
use crate::store::cooldown_store::{Backend, Record};

pub struct CredentialDb {
    store: Arc<ConfigStore>,
    dir: PathBuf,
}

/// Absolute lexical identity for credential documents and their refresh locks.
/// Windows canonicalization adds a verbatim disk prefix; ordinary writers do not.
pub fn credential_path_key(path: &Path) -> std::io::Result<PathBuf> {
    let absolute = std::path::absolute(path)?;
    let mut clean = PathBuf::new();
    for component in absolute.components() {
        match component {
            #[cfg(windows)]
            Component::Prefix(prefix) if matches!(prefix.kind(), std::path::Prefix::VerbatimDisk(_)) => {
                let std::path::Prefix::VerbatimDisk(drive) = prefix.kind() else {
                    unreachable!()
                };
                clean.push(format!("{}:", char::from(drive)));
            }
            Component::CurDir => {}
            Component::ParentDir => {
                if matches!(clean.components().next_back(), Some(Component::Normal(_))) {
                    clean.pop();
                }
            }
            component => clean.push(component.as_os_str()),
        }
    }
    Ok(clean)
}

impl CredentialDb {
    /// Recreates the working copies from SQLite.
    pub fn initialize(store: Arc<ConfigStore>) -> anyhow::Result<Arc<Self>> {
        let path = store
            .path()
            .context("credential persistence requires an on-disk database")?;
        // Resolve aliases before deriving both the working path and refresh-lock identity.
        let mut dir = credential_path_key(&std::fs::canonicalize(path)?)?.into_os_string();
        dir.push(".credentials");
        let db = Arc::new(Self {
            store,
            dir: PathBuf::from(dir),
        });
        private_dir(&db.dir)?;
        let _init = ck_exec::codex_siwc_bind::lock_file(&db.dir.join(".initialize")).map_err(anyhow::Error::msg)?;
        db.materialize()?;
        Ok(db)
    }

    pub fn auth_dir(&self) -> PathBuf {
        self.dir.clone()
    }

    fn name(&self, path: &Path) -> anyhow::Result<String> {
        let absolute = credential_path_key(path)?;
        let (prefix, path) = match absolute.strip_prefix(&self.dir) {
            Ok(relative) if !relative.as_os_str().is_empty() => ("local:", relative),
            _ => ("external:", absolute.as_path()),
        };
        Ok(format!(
            "{prefix}{}",
            path.to_str().context("credential path is not UTF-8")?
        ))
    }

    fn path(&self, name: &str) -> anyhow::Result<PathBuf> {
        if let Some(relative) = name.strip_prefix("local:") {
            let relative = Path::new(relative);
            anyhow::ensure!(
                !relative.as_os_str().is_empty()
                    && relative
                        .components()
                        .all(|p| matches!(p, std::path::Component::Normal(_))),
                "invalid stored credential path"
            );
            return Ok(self.dir.join(relative));
        }
        if let Some(absolute) = name.strip_prefix("external:").map(Path::new)
            && absolute.is_absolute()
        {
            return Ok(absolute.to_owned());
        }
        bail!("invalid stored credential path")
    }

    pub(crate) fn retain_login(
        &self,
        documents: Vec<(PathBuf, Option<Vec<u8>>)>,
        pending: &mut Option<crate::store::config_store::Pending>,
    ) -> anyhow::Result<()> {
        let mut candidate = pending
            .as_ref()
            .map_or_else(|| self.store.current(), |p| p.state.clone())
            .as_ref()
            .clone();
        let mut names = Vec::new();
        for (path, bytes) in documents {
            let name = self.name(&path)?;
            candidate.put_document(name.clone(), bytes);
            names.push(name);
        }
        self.store.retain_grants(candidate, &names, pending)
    }

    pub(crate) fn login_sources(
        &self,
        current: &State,
    ) -> anyhow::Result<std::collections::BTreeMap<PathBuf, Option<[u8; 32]>>> {
        current
            .documents
            .iter()
            .map(|(name, doc)| Ok((self.path(name)?, Some(crate::runtime::state::sha256(&doc.bytes)))))
            .collect()
    }

    pub(crate) fn hydrate_login_render(&self, current: &State, root: &Path) -> anyhow::Result<()> {
        for (name, document) in &current.documents {
            let path = self.path(name)?;
            if path.parent() == Some(self.dir.as_path()) {
                materialize(
                    &root.join(path.file_name().context("invalid login source")?),
                    &document.bytes,
                )?;
            }
        }
        Ok(())
    }

    pub(crate) fn render_login(
        &self,
        current: &State,
        paths: &[PathBuf],
        expected: &mut Option<std::collections::BTreeMap<PathBuf, Option<[u8; 32]>>>,
        resolved: &mut std::collections::BTreeMap<String, [u8; 32]>,
        write: &mut dyn FnMut() -> Result<(), String>,
    ) -> anyhow::Result<Vec<(PathBuf, Option<Vec<u8>>)>> {
        let destinations = paths
            .iter()
            .map(|path| {
                Ok(self
                    .dir
                    .join(path.file_name().context(crate::management::api::LoginRejected)?))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let sources = destinations
            .iter()
            .map(|path| {
                let hash = current
                    .documents
                    .get(&self.name(path)?)
                    .map(|doc| crate::runtime::state::sha256(&doc.bytes));
                Ok((path.clone(), hash))
            })
            .collect::<anyhow::Result<std::collections::BTreeMap<_, _>>>()?;
        if let Some(expected) = expected {
            for path in &destinations {
                if let Some(Some(hash)) = expected.get(path) {
                    resolved.insert(self.name(path)?, *hash);
                }
            }
            anyhow::ensure!(
                sources
                    .iter()
                    .all(|(path, hash)| expected.get(path).copied().flatten() == *hash),
                crate::management::api::LoginRejected
            );
        } else {
            *expected = Some(sources);
        }
        for (path, destination) in paths.iter().zip(&destinations) {
            self.store.block_grant_source(self.name(destination)?);
            if let Some(document) = current.documents.get(&self.name(destination)?) {
                materialize(path, &document.bytes)?;
            }
        }
        write().map_err(crate::management::api::login_render_error)?;
        paths
            .iter()
            .zip(destinations)
            .map(|(path, destination)| {
                let bytes = match std::fs::read(path) {
                    Ok(bytes) => Some(bytes),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                    Err(error) => return Err(error.into()),
                };
                Ok((destination, bytes))
            })
            .collect()
    }

    /// Original source bytes from the owner's authorized immutable view.
    pub fn read(&self, path: &Path) -> anyhow::Result<Option<Vec<u8>>> {
        let state = self.store.current();
        Ok(state
            .documents
            .get(&self.name(path)?)
            .map(|document| document.bytes.as_ref().clone()))
    }

    pub fn documents(&self) -> anyhow::Result<Vec<(PathBuf, Vec<u8>)>> {
        let state = self.store.current();
        state
            .documents
            .iter()
            .map(|(name, document)| Ok((self.path(name)?, document.bytes.as_ref().clone())))
            .collect()
    }

    /// Synthesizes all canonical documents through the same parser as legacy file scans.
    pub fn scan(&self, config: &ck_core::config::Config) -> anyhow::Result<ck_core::config::credentials::AuthScan> {
        Ok(ck_core::config::credentials::scan_documents(config, self.documents()?))
    }

    /// Commits before any caller may publish the new credential. The owner gate
    /// covers candidate derivation and materialization through durable publication.
    pub fn save(&self, path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
        self.save_checked(path, bytes, None).map(|_| ())
    }

    /// Checks the accepted source-byte hash under the owner gate before writing.
    /// `None` expects an absent document; `false` leaves SQL and memory untouched.
    pub fn save_if_current(&self, path: &Path, bytes: &[u8], expected: Option<[u8; 32]>) -> anyhow::Result<bool> {
        self.save_checked(path, bytes, Some(expected))
    }

    fn save_checked(&self, path: &Path, bytes: &[u8], expected: Option<Option<[u8; 32]>>) -> anyhow::Result<bool> {
        self.command()?.save_checked(path, bytes, expected)
    }

    /// Admission precedes runtime reload/per-ID guards. Internal persistence reuses
    /// this context instead of recursively entering the owner (including recovery).
    pub(crate) fn command(&self) -> anyhow::Result<CredentialCommand<'_>> {
        Ok(CredentialCommand {
            db: self,
            pending: self.store.writer()?,
            grant: false,
            deferred: false,
            preservation_source: None,
        })
    }

    /// Preservation only: callers have already consumed a non-repeatable grant.
    pub(crate) fn granted_command(&self) -> anyhow::Result<CredentialCommand<'_>> {
        Ok(CredentialCommand {
            db: self,
            pending: self.store.grant_writer()?,
            grant: true,
            deferred: false,
            preservation_source: None,
        })
    }

    pub(crate) fn required_source(&self, path: &Path) -> Option<Option<[u8; 32]>> {
        if !self.store.any_grant_source() {
            return None;
        }
        self.name(path).ok().and_then(|name| self.store.grant_source(&name))
    }

    /// Recovery admission for new work, without holding the owner over network I/O.
    pub(crate) fn admit_work(&self) -> anyhow::Result<()> {
        drop(self.store.writer()?);
        Ok(())
    }

    /// Hydrates canonical documents, runs provider writers, then commits
    /// their final source documents together under the owner gate. A failed
    /// write or transaction restores the unchanged canonical working documents.
    pub fn write_files<T>(&self, paths: &[PathBuf], write: impl FnOnce() -> anyhow::Result<T>) -> anyhow::Result<T> {
        self.command()?.write_files(paths, write)
    }

    /// A missing cache file does not prevent deletion and cannot resurrect the
    /// document on restart. File and SQL changes finish before acknowledgement.
    pub fn delete(&self, path: &Path) -> anyhow::Result<()> {
        self.write_files(std::slice::from_ref(&path.to_owned()), || remove(path))
    }

    /// Refresh callers invoke this under the stable document lock before spending
    /// a refresh token, restoring the owner's latest accepted source bytes first.
    pub fn restore(&self, path: &Path) -> anyhow::Result<()> {
        let _writer = self.store.writer()?;
        self.restore_from(&self.store.current(), path)
    }

    fn restore_from(&self, state: &State, path: &Path) -> anyhow::Result<()> {
        let bytes = state
            .documents
            .get(&self.name(path)?)
            .map(|document| document.bytes.as_slice());
        match bytes {
            Some(bytes) => materialize(path, bytes),
            None => remove(path),
        }
    }

    /// Rewrites every document's cache file from the accepted state, unless a document's
    /// lock is held: then nothing is written and that document's path is returned. Every
    /// refresh takes a document's lock and only then restores the document through the
    /// store writer, which this holds throughout; management holders write under `disk`,
    /// which the caller holds. A lock found free here therefore means no refresh of that
    /// document is in flight, and none can restore it before the repair ends, so each
    /// probe releases its lock at once instead of keeping one descriptor per document.
    pub(crate) fn repair_cache(&self) -> anyhow::Result<Option<PathBuf>> {
        let _writer = self.store.grant_writer()?;
        let current = self.store.current();
        let paths = current
            .documents
            .keys()
            .map(|name| self.path(name))
            .collect::<anyhow::Result<Vec<_>>>()?;
        for path in &paths {
            let lock = ck_exec::codex_siwc_bind::open_lock_file(path).map_err(anyhow::Error::msg)?;
            if !ck_exec::codex_siwc_bind::try_lock(&lock).map_err(anyhow::Error::msg)? {
                return Ok(Some(path.clone()));
            }
            ck_exec::codex_siwc_bind::unlock(&lock).map_err(anyhow::Error::msg)?;
        }
        for path in &paths {
            self.restore_from(&current, path)?;
        }
        Ok(None)
    }

    /// Recreates documents and clears abandoned cache files. Each restoration
    /// holds the owner gate through the copy; it cannot overwrite a newer save.
    pub fn materialize(&self) -> anyhow::Result<()> {
        private_dir(&self.dir)?;
        let mut paths: BTreeSet<PathBuf> = json_files(&self.dir)?.into_iter().collect();
        paths.extend(self.documents()?.into_iter().map(|(path, _)| path));
        for path in paths {
            self.restore(&path)?;
        }
        Ok(())
    }
}

/// One admitted credential operation; not a second owner or an ambient context.
pub(crate) struct CredentialCommand<'a> {
    db: &'a CredentialDb,
    pending: crate::store::config_store::Writer<'a>,
    grant: bool,
    deferred: bool,
    preservation_source: Option<PathBuf>,
}

impl CredentialCommand<'_> {
    pub(crate) fn authorize_completed_source(&mut self, path: &Path, expected: [u8; 32]) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.grant
                && self
                    .read(path)?
                    .is_some_and(|bytes| crate::runtime::state::sha256(&bytes) == expected),
            "completed grant source changed"
        );
        self.preservation_source = Some(path.to_owned());
        Ok(())
    }

    pub(crate) fn preserves_source(&self, path: &Path) -> bool {
        self.grant && self.preservation_source.as_deref() == Some(path)
    }

    pub(crate) fn is_grant(&self) -> bool {
        self.grant
    }

    pub(crate) fn defer_grant_publication(&mut self) {
        self.deferred = self.grant;
    }

    pub(crate) fn resolve_grant_publication(&mut self) -> anyhow::Result<()> {
        if std::mem::take(&mut self.deferred) {
            self.db.store.resolve_grants(&mut self.pending)?;
        }
        Ok(())
    }

    pub(crate) fn restore(&self, path: &Path) -> anyhow::Result<()> {
        self.db.restore_from(&self.current(), path)
    }

    /// A provider's completed initial result enters through this explicit context;
    /// ordinary uploads and host saves still obtain command(), never grant_writer.
    pub(crate) fn write_files<T>(
        &mut self,
        paths: &[PathBuf],
        write: impl FnOnce() -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        let result = (|| {
            let current = self.current();
            for path in paths {
                self.db.restore_from(&current, path)?;
            }
            let value = write()?;
            let mut candidate = current.as_ref().clone();
            let mut names = Vec::with_capacity(paths.len());
            for path in paths {
                let bytes = match std::fs::read(path) {
                    Ok(bytes) => Some(bytes),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                    Err(e) => return Err(e).with_context(|| format!("read credential {}", path.display())),
                };
                let name = self.db.name(path)?;
                candidate.put_document(name.clone(), bytes);
                names.push(name);
            }
            if self.grant {
                self.db.store.accept_grants(candidate, &names, &mut self.pending)?;
            } else {
                self.db.store.accept(candidate, &mut self.pending)?;
            }
            Ok(value)
        })();
        if result.is_err() {
            for path in paths {
                let _ = self.db.restore_from(&self.db.store.current(), path);
            }
        }
        result
    }

    /// An already-rendered provider batch is retained before touching cache files.
    pub(crate) fn save_documents(&mut self, documents: &[(PathBuf, Vec<u8>)]) -> anyhow::Result<()> {
        let changes: Vec<_> = documents
            .iter()
            .map(|(path, bytes)| (path.clone(), Some(bytes.as_slice())))
            .collect();
        self.apply_documents(&changes)
    }

    /// One owner commit includes raw replacements and explicit source-fenced removals.
    pub(crate) fn apply_documents(&mut self, documents: &[(PathBuf, Option<&[u8]>)]) -> anyhow::Result<()> {
        let result = (|| {
            let mut candidate = self.current().as_ref().clone();
            let mut names = Vec::with_capacity(documents.len());
            for (path, bytes) in documents {
                let name = self.db.name(path)?;
                candidate.put_document(name.clone(), bytes.map(<[u8]>::to_vec));
                names.push(name);
            }
            let materialize_documents = || -> anyhow::Result<()> {
                for (path, bytes) in documents {
                    match bytes {
                        Some(bytes) => materialize(path, bytes)?,
                        None => match std::fs::remove_file(path) {
                            Ok(()) => {}
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                            Err(error) => return Err(error).context("remove credential cache"),
                        },
                    }
                }
                Ok(())
            };
            if self.grant {
                self.db.store.accept_grants(candidate, &names, &mut self.pending)?;
                materialize_documents()?;
            } else {
                materialize_documents()?;
                self.db.store.accept(candidate, &mut self.pending)?;
            }
            Ok(())
        })();
        if result.is_err() {
            for (path, _) in documents {
                let _ = self.db.restore_from(&self.db.store.current(), path);
            }
        }
        result
    }

    fn current(&self) -> Arc<State> {
        self.pending
            .as_ref()
            .map_or_else(|| self.db.store.current(), |pending| pending.state.clone())
    }

    pub(crate) fn read(&self, path: &Path) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self
            .current()
            .documents
            .get(&self.db.name(path)?)
            .map(|document| document.bytes.as_ref().clone()))
    }

    pub(crate) fn save(&mut self, path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
        self.save_checked(path, bytes, None).map(|_| ())
    }

    pub(crate) fn save_checked(
        &mut self,
        path: &Path,
        bytes: &[u8],
        expected: Option<Option<[u8; 32]>>,
    ) -> anyhow::Result<bool> {
        let result = (|| {
            let current = self.current();
            let name = self.db.name(path)?;
            if let Some(expected) = expected {
                let held = current
                    .documents
                    .get(&name)
                    .map(|document| <sha2::Sha256 as sha2::Digest>::digest(document.bytes.as_slice()).into());
                if held != expected {
                    return Ok(false);
                }
            }
            let mut candidate = current.as_ref().clone();
            candidate.put_document(name.clone(), Some(bytes.to_vec()));
            if self.grant {
                if self.deferred {
                    self.db.store.retain_grants(candidate, &[name], &mut self.pending)?;
                } else {
                    self.db.store.accept_grant(candidate, name, &mut self.pending)?;
                    materialize(path, bytes)?;
                }
            } else {
                materialize(path, bytes)?;
                self.db.store.accept(candidate, &mut self.pending)?;
            }
            Ok(true)
        })();
        if result.is_err() {
            // Keep admission during repair; recursively calling restore would wait
            // on our own owner gate and could replay a required projection here.
            let _ = self.db.restore_from(&self.db.store.current(), path);
        }
        result
    }
}

impl Backend for CredentialDb {
    fn load(&self) -> Result<Vec<Record>, String> {
        self.store
            .current()
            .cooldowns
            .values()
            .map(|record| serde_json::from_str(record).map_err(|e| e.to_string()))
            .collect()
    }

    fn save(&self, records: Vec<Record>, now: SystemTime) -> Result<(), String> {
        let save = || -> anyhow::Result<()> {
            let mut pending = self.store.writer()?;
            let current = self.store.current();
            let mut candidate = current.as_ref().clone();
            candidate.cooldowns = cooldowns(records, now)?;
            self.store.accept(candidate, &mut pending)
        };
        save().map_err(|e| e.to_string())
    }
}

fn cooldowns(records: Vec<Record>, now: SystemTime) -> anyhow::Result<std::collections::BTreeMap<String, String>> {
    records
        .into_iter()
        .map(|mut record| {
            record.updated_at = Some(now);
            Ok((
                serde_json::to_string(&(&record.auth_id, &record.model))?,
                serde_json::to_string(&record)?,
            ))
        })
        .collect()
}

fn create_dir(dir: &Path) -> anyhow::Result<()> {
    let mut create = std::fs::DirBuilder::new();
    create.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut create, 0o700);
    create.create(dir)?;
    anyhow::ensure!(
        std::fs::symlink_metadata(dir)?.file_type().is_dir(),
        "credential working directory is not a directory"
    );
    Ok(())
}

fn private_dir(dir: &Path) -> anyhow::Result<()> {
    create_dir(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if std::fs::metadata(dir)?.permissions().mode() & 0o777 != 0o700 {
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
        }
    }
    Ok(())
}

fn materialize(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    // Avoid replacing equal files: provenance and file times
    // remain stable when a config-only reload hydrates the credential cache.
    if std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_file())
        && std::fs::read(path).ok().as_deref() == Some(bytes)
    {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        create_dir(parent)?;
    }
    atomic_write(path, bytes).map_err(anyhow::Error::msg)
}

/// Go `atomicWriteFile`: a 0600 temp file beside `path`, renamed over it.
pub(crate) fn atomic_write(path: &Path, data: &[u8]) -> Result<(), String> {
    use std::io::Write as _;
    let dir = match path.parent() {
        Some(d) if !d.as_os_str().is_empty() => d,
        _ => Path::new("."),
    };
    create_private_dir(dir).map_err(|e| format!("create auth directory: {}", path_error("mkdir", dir, &e)))?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    // Like Go `os.CreateTemp`: a fresh name until one is unused.
    let mut attempt = 0u32;
    let (tmp_path, mut tmp) = loop {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.subsec_nanos());
        let candidate = dir.join(format!(".credential-{}{nanos}{attempt}.tmp", std::process::id()));
        match options.open(&candidate) {
            Ok(file) => break (candidate, file),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && attempt < 10_000 => attempt += 1,
            Err(e) => {
                return Err(format!("create temp auth file: {}", path_error("open", &candidate, &e)));
            }
        }
    };
    let result = tmp
        .write_all(data)
        .map_err(|e| format!("write temp auth file: {}", path_error("write", &tmp_path, &e)))
        .and_then(|()| {
            drop(tmp);
            std::fs::rename(&tmp_path, path).map_err(|e| format!("rename temp auth file: {e}"))
        });
    let _ = std::fs::remove_file(&tmp_path);
    result
}

/// `os.MkdirAll(dir, 0o700)`.
fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder.create(dir)
}

/// Go's `*fs.PathError` text: `op path: errno`.
fn path_error(op: &str, path: &Path, e: &std::io::Error) -> String {
    format!("{op} {}: {}", path.display(), errno_text(e))
}

/// Go's `syscall.Errno` text: strerror in lower case, without Rust's suffix.
fn errno_text(e: &std::io::Error) -> String {
    let text = e.to_string();
    let text = text.split(" (os error").next().unwrap_or_default();
    let mut chars = text.chars();
    chars
        .next()
        .map_or_else(String::new, |c| c.to_lowercase().chain(chars).collect())
}

fn remove(path: &Path) -> anyhow::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("remove credential working file {}", path.display())),
    }
}

fn json_files(dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e).with_context(|| format!("read credential directory {}", dir.display())),
    };
    for entry in entries {
        let entry = entry?;
        let ty = entry.file_type()?;
        if ty.is_dir() {
            out.extend(json_files(&entry.path())?);
        } else if (ty.is_file() || (ty.is_symlink() && std::fs::metadata(entry.path()).is_ok_and(|m| m.is_file())))
            && entry
                .path()
                .extension()
                .is_some_and(|ext| ext.as_encoded_bytes().eq_ignore_ascii_case(b"json"))
        {
            out.push(entry.path());
        }
    }
    out.sort();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn directory() -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "cooker-credential-db-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn rendered_grant_batch_is_durable_before_cache_materialization() {
        let root = directory();
        let store = Arc::new(crate::testing::config_store(&root.join("cooker.db")).unwrap());
        let db = CredentialDb::initialize(store.clone()).unwrap();
        let documents = vec![
            (
                db.auth_dir().join("a.json"),
                br#"{"type":"private","token":"opaque-a"}"#.to_vec(),
            ),
            (
                db.auth_dir().join("b.json"),
                br#"{"type":"private","token":"opaque-b"}"#.to_vec(),
            ),
        ];
        // A real cache failure must not drop the completed batch before retention.
        std::fs::remove_dir_all(db.auth_dir()).unwrap();
        std::fs::write(db.auth_dir(), b"private materialization fault").unwrap();
        assert!(db.granted_command().unwrap().save_documents(&documents).is_err());
        assert_eq!(db.documents().unwrap(), documents);
        let backup = root.join("backup.db");
        std::fs::copy(store.path().unwrap(), &backup).unwrap();
        let cold = Arc::new(crate::testing::config_store(&backup).unwrap());
        let recovered = CredentialDb::initialize(cold.clone()).unwrap();
        for ((path, bytes), (recovered_path, recovered_bytes)) in documents.iter().zip(recovered.documents().unwrap()) {
            assert_eq!(path.file_name(), recovered_path.file_name());
            assert_eq!(bytes, &recovered_bytes);
        }
        std::fs::remove_file(db.auth_dir()).unwrap();
        db.materialize().unwrap();
        for (path, bytes) in &documents {
            assert_eq!(&std::fs::read(path).unwrap(), bytes);
        }
        drop((recovered, cold, db, store));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn consumed_grant_survives_a_checkpoint_error_and_merges_the_next_result() {
        use crate::testing::installation_effects::{self, Stage};
        let root = directory();
        let store = Arc::new(crate::testing::config_store(&root.join("cooker.db")).unwrap());
        store.import_yaml("server: {port: 8317}\n", "test", "initial").unwrap();
        let db = CredentialDb::initialize(store.clone()).unwrap();
        let a = db.auth_dir().join("a.json");
        let b = db.auth_dir().join("b.json");
        db.save(&a, br#"{"refresh_token":"R0","opaque":[1,2]}"#).unwrap();
        db.save(&b, br#"{"refresh_token":"S0"}"#).unwrap();
        let live = PathBuf::from(crate::testing::database_connection(&store).path().unwrap());
        let failed = installation_effects::once(&live, Stage::BeforeReplace, || {
            Err(std::io::Error::other("checkpoint fault"))
        });
        let r1 = br#"{"refresh_token":"R1","opaque":[1,2]}"#;
        assert!(db.granted_command().unwrap().save(&a, r1).is_err());
        drop(failed);
        let blocked = crate::testing::block_publication(store.path().unwrap());
        assert!(
            store
                .import_yaml("server: {port: 8318}\n", "test", "unrelated")
                .is_err()
        );
        assert!(
            db.granted_command()
                .unwrap()
                .save(&b, br#"{"refresh_token":"S1"}"#)
                .is_err()
        );
        assert_eq!(
            db.read(&a).unwrap().unwrap(),
            br#"{"refresh_token":"R0","opaque":[1,2]}"#
        );
        drop(blocked);
        db.admit_work().unwrap();
        assert_eq!(db.read(&a).unwrap().unwrap(), r1);
        assert_eq!(db.read(&b).unwrap().unwrap(), br#"{"refresh_token":"S1"}"#);
        assert_eq!(
            store.load().unwrap().config.port,
            8317,
            "uninstalled ordinary mutation must not revive"
        );
        drop((db, store));
        assert!(!live.exists(), "settled owner cleans its own workspace");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn unresolved_grant_checkpoint_survives_drop_and_another_database_initialization() {
        let root = directory();
        let path = root.join("cooker.db");
        let store = Arc::new(crate::testing::config_store(&path).unwrap());
        store.import_yaml("server: {port: 8317}\n", "test", "history").unwrap();
        let db = CredentialDb::initialize(store.clone()).unwrap();
        let auth = db.auth_dir().join("a.json");
        db.save(&auth, br#"{"refresh_token":"R0"}"#).unwrap();
        let live = PathBuf::from(crate::testing::database_connection(&store).path().unwrap());
        std::fs::create_dir(root.join("other")).unwrap();
        let blocked = crate::testing::block_publication(&path);
        assert!(
            db.granted_command()
                .unwrap()
                .save(&auth, br#"{"refresh_token":"R1"}"#)
                .is_err()
        );
        drop((db, store));
        assert!(
            live.exists(),
            "normal last-owner drop must retain unresolved recovery data"
        );
        let other = crate::testing::config_store(&root.join("other/another.db")).unwrap();
        assert!(live.exists(), "initialization must not sweep another namespace's grant");
        drop(other);
        drop(blocked);
        let resumed = Arc::new(crate::testing::config_store(&path).unwrap());
        let db = CredentialDb::initialize(resumed.clone()).unwrap();
        assert_eq!(
            db.read(&db.auth_dir().join("a.json")).unwrap().unwrap(),
            br#"{"refresh_token":"R1"}"#
        );
        assert_eq!(resumed.load().unwrap().config.port, 8317);
        assert_eq!(resumed.revisions(10, None).unwrap().len(), 1);
        drop((db, resumed));
        assert!(
            !live.exists(),
            "successful resume returns to ordinary own-workspace cleanup"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn failed_publication_keeps_the_newer_grant_until_an_owner_retry() {
        let root = directory();
        let path = root.join("cooker.db");
        let store = Arc::new(crate::testing::config_store(&path).unwrap());
        store.import_yaml("server: {port: 8317}\n", "test", "").unwrap();
        let db = CredentialDb::initialize(store.clone()).unwrap();
        let auth = db.auth_dir().join("auth.json");
        let old = br#"{"type":"private","access_token":"test-R0"}"#;
        let grant = br#"{"type":"private","access_token":"test-R1"}"#;
        db.save(&auth, old).unwrap();
        let accepted_revision = store.load().unwrap().rev;
        let published = std::fs::read(&path).unwrap();

        let blocked = crate::testing::block_publication(&path);
        assert!(db.save(&auth, grant).is_err());
        assert_eq!(db.read(&auth).unwrap().as_deref(), Some(old.as_slice()));
        {
            let checkpoint = crate::testing::database_connection(&store);
            let retained: Vec<u8> = checkpoint
                .query_row(
                    "SELECT bytes FROM credential_document WHERE name = 'local:auth.json'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(retained, grant);
            assert_eq!(
                checkpoint
                    .pragma_query_value(None, "integrity_check", |row| row.get::<_, String>(0))
                    .unwrap(),
                "ok"
            );
        }
        assert!(
            store
                .import_yaml("server: {port: 8318}\n", "test", "unrelated")
                .is_err()
        );
        assert_eq!(store.load().unwrap().rev, accepted_revision);
        assert_eq!(store.load().unwrap().config.port, 8317);
        assert_eq!(std::fs::read(&path).unwrap(), published);
        drop(blocked);

        // An owner retry first publishes the retained checkpoint, then derives
        // the unrelated change from that state rather than resurrecting R0.
        store.import_yaml("server: {port: 8318}\n", "test", "retry").unwrap();
        assert_eq!(db.read(&auth).unwrap().as_deref(), Some(grant.as_slice()));
        let copy = root.join("standalone.db");
        std::fs::copy(&path, &copy).unwrap();
        let recovered_store = Arc::new(crate::testing::config_store(&copy).unwrap());
        let recovered = CredentialDb::initialize(recovered_store.clone()).unwrap();
        assert_eq!(recovered_store.load().unwrap().config.port, 8318);
        assert_eq!(
            recovered
                .read(&recovered.auth_dir().join("auth.json"))
                .unwrap()
                .as_deref(),
            Some(grant.as_slice())
        );
        drop((recovered, recovered_store, db, store));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn initialize_save_reopen_documents_round_trip() {
        let root = directory();
        let path = root.join("cooker.db");
        let db = CredentialDb::initialize(Arc::new(crate::testing::config_store(&path).unwrap())).unwrap();
        let local = db.auth_dir().join("auth.json");
        let nested = db.auth_dir().join("nested/auth.json");
        let external = root.join("external.json");
        for path in [&local, &nested, &external] {
            db.save(path, b"{\"type\":\"private\"}").unwrap();
            assert_eq!(db.path(&db.name(path).unwrap()).unwrap(), *path);
        }
        assert_eq!(db.name(&local).unwrap(), "local:auth.json");
        assert!(db.name(&nested).unwrap().starts_with("local:nested"));
        assert!(db.name(&external).unwrap().starts_with("external:"));
        let before = db.documents().unwrap();
        drop(db);
        let db = CredentialDb::initialize(Arc::new(crate::testing::config_store(&path).unwrap())).unwrap();
        assert_eq!(db.documents().unwrap(), before);
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn save_replaces_fifo_symlink_without_reading_target() {
        use std::os::unix::fs::{FileTypeExt as _, symlink};

        let root = directory();
        let db =
            CredentialDb::initialize(Arc::new(crate::testing::config_store(&root.join("cooker.db")).unwrap())).unwrap();
        let path = db.auth_dir().join("auth.json");
        db.save(&path, b"{\"token\":\"before\"}").unwrap();
        std::fs::remove_file(&path).unwrap();
        let fifo = root.join("pipe");
        let name = std::ffi::CString::new(fifo.as_os_str().as_encoded_bytes()).unwrap();
        // SAFETY: valid NUL-terminated path in this test's private directory.
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        symlink(&fifo, &path).unwrap();

        let bytes = b"{\"token\":\"after\"}";
        let (tx, rx) = std::sync::mpsc::channel();
        let (writer_db, writer_path) = (db.clone(), path.clone());
        let writer = std::thread::spawn(move || tx.send(writer_db.save(&writer_path, bytes)).unwrap());
        rx.recv_timeout(std::time::Duration::from_secs(2))
            .expect("saving must replace the FIFO symlink without opening it")
            .unwrap();
        writer.join().unwrap();

        assert!(std::fs::symlink_metadata(&path).unwrap().file_type().is_file());
        assert!(std::fs::symlink_metadata(&fifo).unwrap().file_type().is_fifo());
        assert_eq!(db.read(&path).unwrap().as_deref(), Some(bytes.as_slice()));
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn unix_path_keys_clean_local_nested_and_external_names() {
        let db = CredentialDb {
            store: Arc::new(ConfigStore::memory_with("")),
            dir: "/cache".into(),
        };
        for (path, name, clean) in [
            ("/cache/./auth.json", "local:auth.json", "/cache/auth.json"),
            (
                "/cache/sub/../nested/auth.json",
                "local:nested/auth.json",
                "/cache/nested/auth.json",
            ),
            ("/outside/../external.json", "external:/external.json", "/external.json"),
        ] {
            assert_eq!(db.name(Path::new(path)).unwrap(), name);
            assert_eq!(db.path(name).unwrap(), Path::new(clean));
        }
        assert_eq!(
            credential_path_key(Path::new("./auth.json")).unwrap(),
            std::env::current_dir().unwrap().join("auth.json")
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_disk_and_verbatim_path_keys_round_trip() {
        let db = CredentialDb {
            store: Arc::new(ConfigStore::memory_with("")),
            dir: r"C:\cache".into(),
        };
        for path in [r"C:\cache\nested\..\auth.json", r"\\?\C:\cache\nested\..\auth.json"] {
            assert_eq!(
                credential_path_key(Path::new(path)).unwrap(),
                Path::new(r"C:\cache\auth.json")
            );
            assert_eq!(db.name(Path::new(path)).unwrap(), "local:auth.json");
            assert_eq!(db.path("local:auth.json").unwrap(), Path::new(r"C:\cache\auth.json"));
        }
        let path = Path::new(r"\\?\D:\external\auth.json");
        assert_eq!(db.name(path).unwrap(), r"external:D:\external\auth.json");
        assert_eq!(
            db.path(&db.name(path).unwrap()).unwrap(),
            Path::new(r"D:\external\auth.json")
        );
    }

    #[test]
    fn raw_documents_and_deletions_survive_a_discarded_cache() {
        let root = directory();
        let original = b"{\n  \"type\":\"opaque-type\",\"opaque\":{\"secret\":\"first\"}\n}\n";
        let path = root.join("cooker.db");
        let store = Arc::new(crate::testing::config_store(&path).unwrap());
        store
            .import_yaml("server: {port: 8317}\n", "test", "preinitialized config")
            .unwrap();
        let config_head = store.snapshot().unwrap().0;
        let db = CredentialDb::initialize(store.clone()).unwrap();
        let auth = db.auth_dir().join("auth.json");
        db.save(&auth, original).unwrap();
        assert_eq!(db.read(&auth).unwrap().unwrap(), original);
        let updated = b"{\"type\":\"opaque-type\",\"opaque\":[1,2],\"access_token\":\"second\"}\n";
        db.save(&auth, updated).unwrap();
        assert_eq!(store.snapshot().unwrap().0, config_head);
        assert!(!store.load().unwrap().text.contains("second"));
        let dir = db.auth_dir();
        drop(db);
        drop(store);
        std::fs::remove_dir_all(&dir).unwrap();
        let db = CredentialDb::initialize(Arc::new(crate::testing::config_store(&path).unwrap())).unwrap();
        assert_eq!(std::fs::read(&auth).unwrap(), updated);
        std::fs::remove_file(&auth).unwrap();
        db.delete(&auth).unwrap();
        drop(db);
        std::fs::remove_dir_all(&dir).unwrap();
        let db = CredentialDb::initialize(Arc::new(crate::testing::config_store(&path).unwrap())).unwrap();
        assert!(db.read(&auth).unwrap().is_none());
        assert!(!auth.exists());
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn canonical_scan_preserves_external_documents_and_auth_indexes_across_restart() {
        let root = directory();
        let path = root.join("cooker.db");
        let store = Arc::new(crate::testing::config_store(&path).unwrap());
        store.import_yaml("oauth: {}\n", "test", "").unwrap();
        let db = CredentialDb::initialize(store.clone()).unwrap();
        let nested = db.auth_dir().join("nested/auth.json");
        let external = root.join("explicit.json");
        let bytes = br#"{"type":"claude","access_token":"durable","opaque":{"extension":"kept"}}"#;
        db.save(&nested, bytes).unwrap();
        db.save(&external, bytes).unwrap();
        let mut cfg = store.load().unwrap().config;
        cfg.auth_dir = db.auth_dir();
        let identities = |db: &CredentialDb| {
            db.scan(&cfg)
                .unwrap()
                .credentials
                .iter()
                .map(|c| (c.id.clone(), ck_core::config::credentials::auth_index(c)))
                .collect::<Vec<_>>()
        };
        let before = identities(&db);
        assert_eq!(before.len(), 2);
        let dir = db.auth_dir();
        drop(db);
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
        std::fs::remove_file(&external).unwrap();
        let db = CredentialDb::initialize(Arc::new(crate::testing::config_store(&path).unwrap())).unwrap();
        assert_eq!(identities(&db), before);
        assert_eq!(db.read(&nested).unwrap().unwrap(), bytes);
        assert_eq!(std::fs::read(&external).unwrap(), bytes);
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn failed_batch_is_atomic_and_restores_working_documents() {
        let root = directory();
        let store = Arc::new(crate::testing::config_store(&root.join("cooker.db")).unwrap());
        let db = CredentialDb::initialize(store.clone()).unwrap();
        let first = db.auth_dir().join("first.json");
        let second = db.auth_dir().join("second.json");
        db.save(&first, b"{\"old\":true}").unwrap();
        let blocked = crate::testing::block_checkpoint_install(&store);
        assert!(
            db.write_files(&[first.clone(), second.clone()], || {
                std::fs::write(&first, b"{\"new\":true}")?;
                std::fs::write(&second, b"{\"new\":true}")?;
                Ok(())
            })
            .is_err()
        );
        assert_eq!(db.read(&first).unwrap().unwrap(), b"{\"old\":true}");
        assert_eq!(std::fs::read(&first).unwrap(), b"{\"old\":true}");
        assert!(db.read(&second).unwrap().is_none());
        assert!(!second.exists());
        drop(blocked);
        drop(db);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cooldown_records_survive_removal_of_working_directory() {
        let root = directory();
        let path = root.join("cooker.db");
        let db = CredentialDb::initialize(Arc::new(crate::testing::config_store(&path).unwrap())).unwrap();
        let records = vec![Record {
            auth_id: "auth.json".into(),
            model: "model".into(),
            reason: "quota".into(),
            ..Record::default()
        }];
        Backend::save(&*db, records, SystemTime::now()).unwrap();
        let dir = db.auth_dir();
        drop(db);
        std::fs::remove_dir_all(dir).unwrap();
        let db = CredentialDb::initialize(Arc::new(crate::testing::config_store(&path).unwrap())).unwrap();
        let saved = Backend::load(&*db).unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(
            (&saved[0].auth_id, &saved[0].model, &saved[0].reason),
            (&"auth.json".to_owned(), &"model".to_owned(), &"quota".to_owned())
        );
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }
}
