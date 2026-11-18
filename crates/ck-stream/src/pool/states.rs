use super::{Mapping, invalid};
use crate::FRAME_BYTES;
use std::io;
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, Ordering};

/// Per-frame counters, one cache line per frame. Leases on different threads update the
/// counts of neighbouring frames; packed, every update moved a shared line between cores.
#[repr(align(64))]
pub(super) struct FrameState {
    /// References: the owner's plus one per reader lease. Zero means free.
    pub(super) refs: AtomicU32,
    /// Bytes from the frame's start whose pages are populated, so writing them cannot
    /// fault on tmpfs exhaustion. Only the frame's current owner changes it.
    pub(super) ready: AtomicU32,
}

/// Per-frame states in private anonymous memory, which stays unbacked until a frame is
/// first used; a heap allocation with this alignment would be written zero in full.
pub(super) struct FrameStates {
    pub(super) mapping: Mapping,
    len: usize,
}

impl FrameStates {
    /// States for `len` frames with the first `populated` marked wholly populated.
    pub(super) fn new(len: usize, populated: u32) -> io::Result<Self> {
        let bytes = len.checked_mul(std::mem::size_of::<FrameState>()).ok_or_else(invalid)?;
        // SAFETY: a fresh private anonymous mapping aliases nothing.
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                bytes,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if base == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        // Decay drops the states of punched frames page by page. A transparent huge page
        // would keep a whole 2 MiB of them resident, so the mapping asks for base pages.
        // The advice is best-effort: a kernel without transparent huge pages rejects it
        // (EINVAL) and maps base pages anyway, and any refusal costs memory, not storage.
        // SAFETY: the range is the fresh mapping above; the advice only selects page size.
        unsafe { libc::madvise(base, bytes, libc::MADV_NOHUGEPAGE) };
        let states = Self {
            mapping: Mapping {
                base: NonNull::new(base.cast()).ok_or_else(invalid)?,
                len: bytes,
            },
            len,
        };
        for state in &states[..populated as usize] {
            state.ready.store(FRAME_BYTES as u32, Ordering::Relaxed);
        }
        Ok(states)
    }
}

impl FrameStates {
    /// Zero the states of frames `first..=last`, which are free and cold, by dropping the
    /// whole pages that hold only them; the next touch maps a zero page. Without this, one
    /// cache line per frame of a past burst stays resident.
    pub(super) fn discard(&self, first: u32, last: u32, page: usize) -> io::Result<()> {
        let size = std::mem::size_of::<FrameState>();
        let start = (first as usize * size).next_multiple_of(page);
        let end = (last as usize + 1) * size / page * page;
        if end <= start {
            return Ok(());
        }
        // SAFETY: the page-aligned range lies inside this private anonymous mapping. Zero
        // bytes are the state of a free, cold frame, which every frame in it already holds,
        // and the decay pass calling this holds the cold list, so no other thread writes them.
        let result = unsafe {
            libc::madvise(
                self.mapping.base.as_ptr().add(start).cast(),
                end - start,
                libc::MADV_DONTNEED,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

impl Deref for FrameStates {
    type Target = [FrameState];
    fn deref(&self) -> &[FrameState] {
        // SAFETY: the page-aligned mapping holds `len` states for its whole life. Zero
        // bytes are valid atomics, and all access goes through them.
        unsafe { std::slice::from_raw_parts(self.mapping.base.as_ptr().cast(), self.len) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Make this thread's `madvise(.., MADV_NOHUGEPAGE)` fail with EINVAL, as on a kernel
    /// without transparent huge pages. Seccomp filters bind only the installing thread.
    fn refuse_nohugepage() {
        let stmt = |code, k| libc::sock_filter { code, jt: 0, jf: 0, k };
        let jump = |k, jt, jf| libc::sock_filter {
            code: (libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K) as u16,
            jt,
            jf,
            k,
        };
        let load = (libc::BPF_LD | libc::BPF_W | libc::BPF_ABS) as u16;
        let ret = (libc::BPF_RET | libc::BPF_K) as u16;
        // seccomp_data: the syscall number at offset 0, the low half of args[2] at 32.
        let mut program = [
            stmt(load, 0),
            jump(libc::SYS_madvise as u32, 0, 3),
            stmt(load, 32),
            jump(libc::MADV_NOHUGEPAGE as u32, 0, 1),
            stmt(ret, libc::SECCOMP_RET_ERRNO | libc::EINVAL as u32),
            stmt(ret, libc::SECCOMP_RET_ALLOW),
        ];
        let filter = libc::sock_fprog {
            len: program.len() as u16,
            filter: program.as_mut_ptr(),
        };
        // SAFETY: both calls affect only this thread; the program outlives the second call.
        unsafe {
            assert_eq!(libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0), 0);
            assert_eq!(libc::prctl(libc::PR_SET_SECCOMP, libc::SECCOMP_MODE_FILTER, &filter), 0);
        }
    }

    #[test]
    fn states_open_where_huge_page_advice_is_refused() {
        std::thread::spawn(|| {
            refuse_nohugepage();
            let probe = FrameStates::new(1024, 0).map(|states| {
                // SAFETY: advice on the live mapping; it changes no contents.
                let refused = unsafe {
                    libc::madvise(
                        states.mapping.base.as_ptr().cast(),
                        states.mapping.len,
                        libc::MADV_NOHUGEPAGE,
                    )
                };
                (refused, states[0].refs.load(Ordering::Relaxed))
            });
            // The filter refuses the advice, and the states open and read as free anyway.
            assert_eq!(probe.unwrap(), (-1, 0));
        })
        .join()
        .unwrap();
    }
}
