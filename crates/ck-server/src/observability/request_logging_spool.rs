//! Capture files: an upstream [`Section`] of the request log, kept in memory and then
//! in a [`Spool`] file. Spools hold captured bytes as they arrive; a section's URLs are
//! redacted over the whole section as it is copied into the log (owner ruling Q18 and
//! decisions 2026-11-15 and 2026-11-16), so a URL split across chunks or writes is seen
//! whole, and a spooled section is never read into memory whole. A WebSocket timeline
//! fails closed at each message end instead ([`Section`]).

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
///
/// A WebSocket timeline is a framed section: captured messages ([`Section::piece`])
/// with the text the log inserts between them ([`Section::insert`]). Each frame is a tag
/// byte and a little-endian `u64` length before its bytes, so the messages are redacted
/// as one text with a cut at each of their ends ([`ck_wire::redact::Pieces`]); a
/// whole-section pass would see the inserted text inside a URL split across messages.
pub(super) struct Section {
    prefix: &'static str,
    framed: bool,
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
            framed: false,
            memory: Vec::new(),
            spool: None,
            dropped: false,
            head: Vec::new(),
            trailing: 0,
            blank: true,
        }
    }

    /// A framed section, for a WebSocket timeline.
    pub(super) fn framed(prefix: &'static str) -> Self {
        Self {
            framed: true,
            ..Self::new(prefix)
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
        debug_assert!(!self.framed, "a framed section takes pieces and inserts");
        if bytes.is_empty() {
            return;
        }
        self.track(bytes);
        self.store(dir, bytes);
    }

    /// Text the log inserts between the captured messages of a framed section.
    pub(super) fn insert(&mut self, dir: &Path, text: &[u8]) {
        self.frame(dir, INSERT, text);
    }

    /// A captured message of a framed section.
    pub(super) fn piece(&mut self, dir: &Path, bytes: &[u8]) {
        self.frame(dir, PIECE, bytes);
    }

    fn frame(&mut self, dir: &Path, tag: u8, bytes: &[u8]) {
        debug_assert!(self.framed, "an unframed section takes appends");
        if bytes.is_empty() {
            return;
        }
        self.track(bytes);
        let mut head = [tag; FRAME_HEAD];
        head[1..].copy_from_slice(&(bytes.len() as u64).to_le_bytes());
        self.store(dir, &head);
        self.store(dir, bytes);
    }

    fn store(&mut self, dir: &Path, bytes: &[u8]) {
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
            Some(spool) if self.framed => {
                let mut file = &spool.file;
                file.seek(io::SeekFrom::Start(0))?;
                write_frames(&mut file.chain(&self.memory[..]), out)
            }
            Some(spool) => spool.copy_redacted(&self.memory, out),
            None if self.framed => write_frames(&mut &self.memory[..], out),
            None => ck_wire::redact::write(&self.memory, out),
        }
    }
}

const INSERT: u8 = 0;
const PIECE: u8 = 1;
/// A frame's tag byte and length.
const FRAME_HEAD: usize = 9;

/// Writes the frames of a framed section, redacted.
fn write_frames(input: &mut dyn Read, out: &mut dyn Write) -> io::Result<()> {
    let mut pieces = ck_wire::redact::Pieces::default();
    let mut in_piece = false;
    let mut head = [0; FRAME_HEAD];
    let mut buf = vec![0; 64 << 10];
    loop {
        match input.read_exact(&mut head) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error),
        }
        let mut frame = (&mut *input).take(u64::from_le_bytes(head[1..].try_into().unwrap_or_default()));
        if head[0] == PIECE {
            in_piece = true;
            loop {
                match frame.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => pieces.push(&buf[..n], out)?,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(error) => return Err(error),
                }
            }
        } else {
            if std::mem::take(&mut in_piece) {
                pieces.cut(out)?;
            }
            ck_wire::redact::copy(&mut frame, out)?;
        }
    }
    pieces.finish(out)
}
