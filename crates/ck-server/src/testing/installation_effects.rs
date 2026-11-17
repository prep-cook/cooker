//! One-shot, destination-scoped filesystem effect faults/barriers. Test builds
//! only: callers exercise actual public commands, never private state layouts.
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub(crate) enum Stage {
    BeforeReplace,
    AfterReplace,
    RuntimeDerivedTail,
    /// A credential batch has probed its document locks and still holds `disk` and the
    /// store command; keyed by the auth directory.
    CredentialBatchProbed,
    /// A login save found this document's lock held and is about to wait for it; keyed
    /// by the document.
    LoginRepairBusy,
}

type Effect = Box<dyn FnOnce() -> std::io::Result<()> + Send>;
type Key = (PathBuf, Stage);
fn effects() -> &'static Mutex<HashMap<Key, Effect>> {
    static EFFECTS: OnceLock<Mutex<HashMap<Key, Effect>>> = OnceLock::new();
    EFFECTS.get_or_init(Mutex::default)
}

pub(crate) struct Guard(Key);
impl Drop for Guard {
    fn drop(&mut self) {
        let removed = effects().lock().unwrap().remove(&self.0);
        drop(removed);
    }
}

pub(crate) fn once(path: &Path, stage: Stage, effect: impl FnOnce() -> std::io::Result<()> + Send + 'static) -> Guard {
    let key = (path.to_owned(), stage);
    assert!(
        effects()
            .lock()
            .unwrap()
            .insert(key.clone(), Box::new(effect))
            .is_none()
    );
    Guard(key)
}

pub(crate) fn run(path: &Path, stage: Stage) -> std::io::Result<()> {
    let effect = effects().lock().unwrap().remove(&(path.to_owned(), stage));
    if let Some(effect) = effect {
        effect()?;
    }
    Ok(())
}
