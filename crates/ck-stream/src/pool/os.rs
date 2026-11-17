use std::ffi::CString;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

pub(super) struct Backing {
    fd: OwnedFd,
    directory: OwnedFd,
    unlink_on_drop: Option<CString>,
}

fn owned(fd: RawFd) -> io::Result<OwnedFd> {
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: successful open/openat returned a new, exclusively owned descriptor.
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }
}

fn private_directory(path: &Path) -> io::Result<OwnedFd> {
    let components: Vec<_> = path.components().collect();
    if components.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "empty pool directory"));
    }
    let start = if path.is_absolute() { c"/" } else { c"." };
    let flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC;
    // SAFETY: start is NUL terminated, and open returns a new descriptor.
    let mut directory = owned(unsafe { libc::open(start.as_ptr(), flags) })?;
    for (index, component) in components.iter().enumerate() {
        let name = match component {
            Component::RootDir | Component::CurDir => continue,
            Component::Normal(name) => CString::new(name.as_bytes())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL in pool directory"))?,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "parent traversal in pool directory",
                ));
            }
        };
        // SAFETY: directory is pinned by its fd; no component follows a symlink.
        let mut next = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), flags) };
        if next < 0 && index + 1 == components.len() && io::Error::last_os_error().raw_os_error() == Some(libc::ENOENT)
        {
            // SAFETY: name is one relative component in the pinned parent directory.
            let made = unsafe { libc::mkdirat(directory.as_raw_fd(), name.as_ptr(), 0o700) };
            if made < 0 && io::Error::last_os_error().raw_os_error() != Some(libc::EEXIST) {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: same pinned parent and no-follow flags as above.
            next = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), flags) };
        }
        directory = owned(next)?;
    }
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: stat points to writable storage; fd remains valid.
    if unsafe { libc::fstat(directory.as_raw_fd(), stat.as_mut_ptr()) } < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful fstat initialized all fields inspected here.
    let stat = unsafe { stat.assume_init() };
    // SAFETY: geteuid takes no arguments and does not mutate process state.
    if stat.st_uid != unsafe { libc::geteuid() } || stat.st_mode & 0o7777 != 0o700 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "pool directory must be owned by the effective user with mode 0700",
        ));
    }
    Ok(directory)
}

impl Backing {
    pub(super) fn open(path: &Path) -> io::Result<Self> {
        Self::create(private_directory(path)?, true)
    }

    fn create(directory: OwnedFd, try_tmpfile: bool) -> io::Result<Self> {
        if try_tmpfile {
            // SAFETY: directory fd is valid, dot is NUL terminated, mode is supplied.
            let fd = unsafe {
                libc::openat(
                    directory.as_raw_fd(),
                    c".".as_ptr(),
                    libc::O_TMPFILE | libc::O_RDWR | libc::O_CLOEXEC,
                    0o600,
                )
            };
            if fd >= 0 {
                let backing = Self {
                    fd: owned(fd)?,
                    directory,
                    unlink_on_drop: None,
                };
                backing.set_mode()?;
                return Ok(backing);
            }
            let error = io::Error::last_os_error();
            if !matches!(
                error.raw_os_error(),
                Some(libc::EOPNOTSUPP | libc::EINVAL | libc::EISDIR | libc::ENOSYS)
            ) {
                return Err(error);
            }
        }
        loop {
            let sequence = NEXT_FILE.fetch_add(1, Ordering::Relaxed);
            let name = CString::new(format!(".ck-stream-{}-{sequence}", std::process::id())).unwrap();
            // SAFETY: name is one component; O_EXCL forbids adopting an existing file
            // or following a symlink. The directory was validated and stays pinned.
            let fd = unsafe {
                libc::openat(
                    directory.as_raw_fd(),
                    name.as_ptr(),
                    libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_RDWR | libc::O_CLOEXEC,
                    0o600,
                )
            };
            if fd < 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() == Some(libc::EEXIST) {
                    continue;
                }
                return Err(error);
            }
            let mut backing = Self {
                fd: owned(fd)?,
                directory,
                unlink_on_drop: Some(name),
            };
            backing.set_mode()?;
            // SAFETY: only the exclusively created pathname in the pinned directory
            // is removed. If unlink is unsupported/denied, Drop owns that pathname.
            if unsafe {
                libc::unlinkat(
                    backing.directory.as_raw_fd(),
                    backing.unlink_on_drop.as_ref().unwrap().as_ptr(),
                    0,
                )
            } == 0
            {
                backing.unlink_on_drop = None;
            }
            return Ok(backing);
        }
    }

    fn set_mode(&self) -> io::Result<()> {
        // SAFETY: this descriptor owns a newly created private file; fchmod avoids
        // process-global umask changes and enforces exactly 0600 before data exists.
        if unsafe { libc::fchmod(self.fd(), 0o600) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub(super) fn fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }

    pub(super) fn grow(&self, offset: usize, len: usize) -> io::Result<()> {
        // SAFETY: Pool checked offsets/lengths against off_t and mapping bounds.
        if unsafe { libc::fallocate(self.fd(), 0, offset as libc::off_t, len as libc::off_t) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub(super) fn punch(&self, offset: usize, len: usize) -> io::Result<()> {
        // SAFETY: Pool exclusively owns all free frames covering these whole pages
        // and holds cold ownership until this operation finishes.
        if unsafe {
            libc::fallocate(
                self.fd(),
                libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
                offset as libc::off_t,
                len as libc::off_t,
            )
        } < 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn open_exclusive(path: &Path) -> io::Result<Self> {
        Self::create(private_directory(path)?, false)
    }
}

impl Drop for Backing {
    fn drop(&mut self) {
        if let Some(name) = &self.unlink_on_drop {
            // SAFETY: this object retains the exact parent fd and exclusively
            // created name; cleanup never scans or removes another pool's files.
            unsafe { libc::unlinkat(self.directory.as_raw_fd(), name.as_ptr(), 0) };
        }
    }
}

pub(super) fn populate(address: *mut u8, len: usize) -> io::Result<()> {
    // SAFETY: Pool supplies a page-aligned address and length inside a live,
    // writable file mapping. This call populates pages without exposing references.
    if unsafe { libc::madvise(address.cast(), len, libc::MADV_POPULATE_WRITE) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn file_mode(file: &Backing) -> u32 {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: file fd is live and the output pointer is writable.
    assert_eq!(unsafe { libc::fstat(file.fd(), stat.as_mut_ptr()) }, 0);
    // SAFETY: fstat succeeded and initialized the inspected fields.
    unsafe { stat.assume_init().st_mode & 0o7777 }
}
