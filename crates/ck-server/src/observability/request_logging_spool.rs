//! Capture files: an upstream [`Section`] of the request log, kept in memory and then
//! in a [`Spool`] file. Spools hold captured bytes as they arrive; a section's URLs are
//! redacted over the whole section as it is copied into the log (owner ruling Q18 and
//! decisions 2026-11-15 and 2026-11-16), so no URL split across chunks or writes escapes
//! redaction, and a spooled section is never read into memory whole.

use super::trim;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;

/// What a section prints instead of its content once a spool write failed: the bytes
/// that did reach the spool are not logged, so no part of a URL prints without the rest.
pub(super) const DROPPED: &[u8] = b"[capture dropped: write failed]";

/// A private capture file in the log directory. Its name is removed as soon as it is
/// created, so the unredacted bytes it holds are never visible in the directory and go
/// away with the handle, even when the process dies.
pub(super) struct Spool {
    pub(super) file: File,
}
impl Spool {
    pub(super) fn new(dir: &Path, prefix: &str) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(format!("{prefix}-{}.tmp", uuid::Uuid::new_v4()));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)?;
        std::fs::remove_file(&path)?;
        Ok(Self { file })
    }

    /// Copies everything written to the file into `out`, its URLs redacted
    /// ([`ck_wire::redact::copy`]), followed by `after`, as one text.
    pub(super) fn copy_redacted(&self, after: &[u8], out: &mut dyn Write) -> io::Result<()> {
        let mut file = &self.file;
        file.seek(io::SeekFrom::Start(0))?;
        ck_wire::redact::copy(&mut file.chain(after), out)
    }

    /// Everything written to the file, read from its start whatever the write position.
    pub(super) fn contents(&self) -> io::Result<Vec<u8>> {
        let mut bytes = vec![0; usize::try_from(self.file.metadata()?.len()).map_err(io::Error::other)?];
        self.file.read_exact_at(&mut bytes, 0)?;
        Ok(bytes)
    }
}

/// In-memory bytes an upstream section keeps before it moves to its spool file.
pub(super) const SECTION_MEMORY: usize = 64 << 10;
/// Long enough for every section header that `section` looks for.
const SECTION_HEAD: usize = 32;

/// An append-only section of the log (API REQUEST, API RESPONSE, API WEBSOCKET TIMELINE
/// or the downstream WEBSOCKET TIMELINE). Go appends these to file-backed sources
/// (request_logging.go attachRequestLogSources), so a long stream or WebSocket session
/// does not grow in memory. Here a section buffers up to SECTION_MEMORY bytes in memory
/// and moves them to a spool file whenever the next append would pass that. If a spool
/// write fails, the section drops its content and every later append, and prints
/// [`DROPPED`].
pub(super) struct Section {
    prefix: &'static str,
    pub(super) memory: Vec<u8>,
    pub(super) spool: Option<Spool>,
    dropped: bool,
    /// The first SECTION_HEAD bytes of the section.
    pub(super) head: Vec<u8>,
    /// Newlines at the end of the section.
    pub(super) trailing: usize,
    /// Every byte so far is whitespace (the section trims to nothing).
    pub(super) blank: bool,
}

impl Section {
    pub(super) fn new(prefix: &'static str) -> Self {
        Self {
            prefix,
            memory: Vec::new(),
            spool: None,
            dropped: false,
            head: Vec::new(),
            trailing: 0,
            blank: true,
        }
    }

    /// A section that holds `bytes` in memory, for a buffer that is already
    /// in memory as a whole.
    pub(super) fn from_vec(prefix: &'static str, bytes: Vec<u8>) -> Self {
        let mut section = Self::new(prefix);
        section.track(&bytes);
        section.memory = bytes;
        section
    }

    pub(super) fn is_empty(&self) -> bool {
        !self.dropped && self.spool.is_none() && self.memory.is_empty()
    }

    pub(super) fn dropped(&self) -> bool {
        self.dropped
    }

    fn track(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        let room = SECTION_HEAD.saturating_sub(self.head.len());
        self.head.extend_from_slice(&bytes[..room.min(bytes.len())]);
        let newlines = bytes.iter().rev().take_while(|&&b| b == b'\n').count();
        self.trailing = if newlines == bytes.len() {
            self.trailing + newlines
        } else {
            newlines
        };
        self.blank &= trim(bytes).is_empty();
    }

    pub(super) fn append(&mut self, dir: &Path, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        self.track(bytes);
        if self.dropped {
            return;
        }
        if self.memory.len() + bytes.len() <= SECTION_MEMORY {
            self.memory.extend_from_slice(bytes);
        } else if self.spill(dir, bytes).is_err() {
            self.dropped = true;
            self.memory = Vec::new();
            self.spool = None;
        }
    }

    /// Moves the buffered bytes and then `bytes` to the spool file.
    fn spill(&mut self, dir: &Path, bytes: &[u8]) -> io::Result<()> {
        let spool = match self.spool.take() {
            Some(spool) => spool,
            None => Spool::new(dir, self.prefix)?,
        };
        let spool = self.spool.insert(spool);
        spool.file.write_all(&self.memory)?;
        // Appends only reach memory while it stays within SECTION_MEMORY, so
        // the kept allocation is bounded too.
        self.memory.clear();
        spool.file.write_all(bytes)
    }

    /// Writes the section, its URLs redacted over the whole section, or [`DROPPED`].
    pub(super) fn write_to(&self, out: &mut dyn Write) -> io::Result<()> {
        if self.dropped {
            return out.write_all(DROPPED);
        }
        match &self.spool {
            Some(spool) => spool.copy_redacted(&self.memory, out),
            None => out.write_all(&ck_wire::redact::bytes(&self.memory)),
        }
    }
}
