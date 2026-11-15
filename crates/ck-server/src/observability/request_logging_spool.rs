//! Capture files: an upstream [`Section`] of the request log, kept in memory and then
//! in a [`Spool`] file. Every spool writes through `ck_wire::redact`, so this is where
//! captured bytes reach the disk.

use super::trim;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

/// A capture file in the log directory. Everything reaches it through the URL
/// redactor, so no URL credential is written to disk even for a moment (owner ruling
/// Q18); a reader ends the writer first, which writes a reference still held back.
pub(super) struct Spool {
    pub(super) file: ck_wire::redact::Writer<File>,
    pub(super) temp: Temp,
}
impl Spool {
    pub(super) fn new(dir: &Path, prefix: &str) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(format!("{prefix}-{}.tmp", uuid::Uuid::new_v4()));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        // Go's os.OpenFile modes; Windows ignores them.
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        let file = options.open(&path)?;
        Ok(Self {
            file: ck_wire::redact::Writer::new(file),
            temp: Temp { path },
        })
    }
}

/// A spool file's path, removed on drop.
pub(super) struct Temp {
    pub(super) path: PathBuf,
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// In-memory bytes an upstream section keeps before it moves to its spool file.
pub(super) const SECTION_MEMORY: usize = 64 << 10;
/// Long enough for every section header that `section` looks for.
const SECTION_HEAD: usize = 32;

/// An append-only upstream section of the log (API REQUEST, API RESPONSE or
/// API WEBSOCKET TIMELINE). Go appends these to file-backed sources
/// (request_logging.go attachRequestLogSources), so a long stream or WebSocket
/// session does not grow in memory. Here a section stays in memory until it
/// passes SECTION_MEMORY bytes, then the buffered bytes and everything after
/// them go to a spool file in the log directory. If the spool cannot be
/// written, the section keeps the rest in memory, as Go's writeAttemptResponse
/// falls back to a strings.Builder.
pub(super) struct Section {
    prefix: &'static str,
    pub(super) memory: Vec<u8>,
    pub(super) spool: Option<Spool>,
    /// Bytes at the start of the spool file that belong to the section; a
    /// failed write can leave more after them, which are never read.
    spilled: u64,
    spill_failed: bool,
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
            spilled: 0,
            spill_failed: false,
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
        self.spilled == 0 && self.memory.is_empty()
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
        if !self.spill_failed && self.memory.len() + bytes.len() > SECTION_MEMORY {
            if self.spill(dir, bytes).is_ok() {
                return;
            }
            self.spill_failed = true;
        }
        self.memory.extend_from_slice(bytes);
    }

    /// Moves the buffered bytes and then `bytes` to the spool file. On error,
    /// whatever did not reach the file is still in memory or in `bytes`, except a
    /// reference the spool's redactor was still holding back.
    fn spill(&mut self, dir: &Path, bytes: &[u8]) -> io::Result<()> {
        if self.spool.is_none() {
            self.spool = Some(Spool::new(dir, self.prefix)?);
        }
        let file = &mut self.spool.as_mut().unwrap().file;
        file.write_all(&self.memory)?;
        self.spilled += self.memory.len() as u64;
        // Appends only reach memory while it stays within SECTION_MEMORY, so
        // the kept allocation is bounded too.
        self.memory.clear();
        file.write_all(bytes)?;
        self.spilled += bytes.len() as u64;
        Ok(())
    }

    /// Writes the reference the spool's redactor may still hold back; nothing is
    /// appended afterwards.
    pub(super) fn end(&mut self) -> io::Result<()> {
        match &mut self.spool {
            Some(spool) if !self.spill_failed => spool.file.end(),
            _ => Ok(()),
        }
    }

    pub(super) fn write_to(&self, out: &mut dyn Write) -> io::Result<()> {
        if let Some(spool) = self.spool.as_ref().filter(|_| self.spilled > 0) {
            let written = spool.file.written();
            let copied = io::copy(&mut File::open(&spool.temp.path)?.take(written), out)?;
            if copied != written {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("{} spool is shorter than written", self.prefix),
                ));
            }
        }
        out.write_all(&self.memory)
    }
}
