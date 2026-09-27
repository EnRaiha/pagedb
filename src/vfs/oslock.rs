//! Advisory path locking shared by every filesystem-backed native VFS.
//!
//! One protocol, one implementation. A lock is two layers: a process-wide
//! table that excludes every handle in this process, and an OS-level lock that
//! excludes other processes — `fcntl` OFD locks (`F_OFD_SETLK`) on Linux,
//! classic `F_SETLK` on other Unix, and `LockFileEx` on Windows. On targets
//! with neither, only the in-process layer applies.
//!
//! The table is keyed by resolved lock-file path, so all VFS instances share
//! it. macOS `F_SETLK` never conflicts within one process. This table enforces
//! the conflict instead.
//! Each entry owns the process's single OS lock on its file. One descriptor
//! means an early close can never drop an `F_SETLK` lock.
//!
//! Both layers live here rather than in each backend deliberately. The store's
//! single-writer guarantee rests on the `.writer.lock` sentinel, and two
//! backends may hold that sentinel at the same time — a process that got an
//! `io_uring` ring and one that fell back to the thread pool are running
//! different `Vfs` implementations over the same directory. If they locked by
//! different rules, the two would not exclude each other and both could write
//! the same store. Sharing the code makes that class of divergence impossible.
//!
//! `unsafe` is permitted here for the platform lock primitives (fcntl on Unix,
//! `LockFileEx` on Windows).
#![allow(unsafe_code)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::Result;
use crate::errors::PagedbError;

#[cfg(any(unix, windows))]
use super::blocking::offload;

// ---------------------------------------------------------------------------
// In-process lock state machine (guards single-process re-entry on all
// targets, and is the only guard where the OS offers no advisory lock).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum LockState {
    Free,
    Exclusive,
    Shared(u32),
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum LockKind {
    Exclusive,
    Shared,
}

/// One lock domain: its holders in this process and the OS lock they share.
struct EntryState {
    mode: LockState,
    #[cfg(any(unix, windows))]
    os: Option<OsLock>,
}

struct InProcLockEntry {
    /// Serializes taking the OS lock for a free domain.
    gate: tokio::sync::Mutex<()>,
    state: Mutex<EntryState>,
}

impl InProcLockEntry {
    fn new() -> Self {
        Self {
            gate: tokio::sync::Mutex::new(()),
            state: Mutex::new(EntryState {
                mode: LockState::Free,
                #[cfg(any(unix, windows))]
                os: None,
            }),
        }
    }

    /// Join a held domain. `false` means it is free and needs the OS lock.
    fn try_join(&self, kind: LockKind) -> Result<bool> {
        let mut state = self.state.lock();
        match (kind, state.mode) {
            (_, LockState::Free) => Ok(false),
            (LockKind::Shared, LockState::Shared(n)) => {
                state.mode = LockState::Shared(n + 1);
                Ok(true)
            }
            _ => Err(PagedbError::AlreadyLocked),
        }
    }

    /// Record the first holder of a free domain.
    fn install(&self, kind: LockKind, #[cfg(any(unix, windows))] os: OsLock) {
        let mut state = self.state.lock();
        state.mode = match kind {
            LockKind::Exclusive => LockState::Exclusive,
            LockKind::Shared => LockState::Shared(1),
        };
        #[cfg(any(unix, windows))]
        {
            state.os = Some(os);
        }
    }

    /// Give one hold back. The last holder releases the OS lock.
    fn leave(&self, kind: LockKind) {
        let mut state = self.state.lock();
        state.mode = match (kind, state.mode) {
            (LockKind::Shared, LockState::Shared(n)) if n > 1 => LockState::Shared(n - 1),
            _ => LockState::Free,
        };
        #[cfg(any(unix, windows))]
        if matches!(state.mode, LockState::Free) {
            state.os = None;
        }
    }
}

/// Every lock domain in the process, keyed by resolved lock-file path.
/// Entries are never removed, so no acquirer races a removal.
static LOCK_TABLE: std::sync::LazyLock<Mutex<BTreeMap<PathBuf, Arc<InProcLockEntry>>>> =
    std::sync::LazyLock::new(|| Mutex::new(BTreeMap::new()));

fn entry(key: PathBuf) -> Arc<InProcLockEntry> {
    LOCK_TABLE
        .lock()
        .entry(key)
        .or_insert_with(|| Arc::new(InProcLockEntry::new()))
        .clone()
}

// ---------------------------------------------------------------------------
// Unix cross-process lock via fcntl.
// ---------------------------------------------------------------------------

/// On Unix, holds an open file descriptor whose advisory lock is released when
/// this struct is dropped (fd close triggers lock release).
///
/// On Linux the lock is an **OFD** (open file description) lock
/// (`F_OFD_SETLK`), which is owned by the open file description rather than the
/// process. This avoids two notorious `F_SETLK` (process-associated) footguns
/// that can drop or defeat a writer lock and let a second opener corrupt the
/// store:
///
/// 1. *Release-on-any-close*: a process `F_SETLK` lock is dropped the moment
///    the process closes **any** fd to that inode, not just the locking fd — an
///    unrelated open/close of the lock path silently frees the lock.
/// 2. *Self-non-conflict*: a second `F_SETLK` request from the **same** process
///    succeeds instead of conflicting, so two VFS instances (with independent
///    in-process lock tables) can both "acquire" and double-open.
///
/// OFD locks are per-description: closing other fds does not release them, and
/// two open descriptions conflict even within one process. They also conflict
/// with traditional `F_SETLK` record locks, so a mixed deployment still
/// excludes correctly.
///
/// Non-Linux Unix (e.g. macOS, which lacks OFD locks) falls back to `F_SETLK`.
#[cfg(unix)]
struct OsFcntlHandle {
    _file: std::fs::File,
}

#[cfg(unix)]
impl OsFcntlHandle {
    fn try_acquire(path: &std::path::Path, kind: LockKind) -> Result<Self> {
        use std::os::unix::io::AsRawFd;

        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .map_err(PagedbError::Io)?;

        let fd = file.as_raw_fd();
        // SAFETY: F_WRLCK and F_RDLCK are small positive constants that fit
        // in i16 on every platform where libc defines them.
        #[allow(clippy::cast_possible_truncation)]
        let l_type = match kind {
            LockKind::Exclusive => libc::F_WRLCK as libc::c_short,
            LockKind::Shared => libc::F_RDLCK as libc::c_short,
        };

        // SAFETY: SEEK_SET == 0, which always fits in i16.
        #[allow(clippy::cast_possible_truncation)]
        let flock = libc::flock {
            l_type,
            l_whence: libc::SEEK_SET as libc::c_short,
            l_start: 0,
            l_len: 0,
            l_pid: 0,
        };

        // Prefer OFD locks on Linux (see the type doc); fall back to classic
        // process locks elsewhere. Both use the identical `flock` payload and
        // the same non-blocking `*_SETLK` semantics (EAGAIN/EACCES == conflict).
        #[cfg(target_os = "linux")]
        let cmd = libc::F_OFD_SETLK;
        #[cfg(not(target_os = "linux"))]
        let cmd = libc::F_SETLK;

        // SAFETY: `fd` is valid (owned by `file` above which stays alive past
        // this call); `flock` is a plain C struct fully initialised above.
        // The command is non-blocking: EAGAIN/EACCES means another open
        // description (OFD) or process (F_SETLK) holds a conflicting lock.
        let rc = unsafe { libc::fcntl(fd, cmd, &flock) };
        if rc == -1 {
            let err = std::io::Error::last_os_error();
            let raw = err.raw_os_error().unwrap_or(0);
            if raw == libc::EAGAIN || raw == libc::EACCES {
                return Err(PagedbError::AlreadyLocked);
            }
            return Err(PagedbError::Io(err));
        }
        Ok(Self { _file: file })
    }
}

// SAFETY: The raw fd is valid across threads; we do not share it between
// threads — the struct is moved as a whole.
#[cfg(unix)]
unsafe impl Send for OsFcntlHandle {}

// ---------------------------------------------------------------------------
// Windows cross-process lock via LockFileEx.
// ---------------------------------------------------------------------------

/// On Windows, holds an open file whose byte-range advisory lock (`LockFileEx`)
/// is explicitly released on drop via `UnlockFileEx`, then the file is closed.
#[cfg(windows)]
struct OsLockFileExHandle {
    file: std::fs::File,
}

#[cfg(windows)]
impl OsLockFileExHandle {
    fn try_acquire(path: &std::path::Path, kind: LockKind) -> Result<Self> {
        use std::os::windows::fs::OpenOptionsExt;
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Foundation::{ERROR_IO_PENDING, ERROR_LOCK_VIOLATION};
        use windows_sys::Win32::Storage::FileSystem::LockFileEx;
        use windows_sys::Win32::Storage::FileSystem::{
            LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY,
        };
        use windows_sys::Win32::System::IO::OVERLAPPED;

        // FILE_SHARE_READ | FILE_SHARE_WRITE (0x1 | 0x2 = 0x3): multiple
        // processes must be able to open the same lock file simultaneously so
        // they can all call LockFileEx and contend against each other.
        const FILE_SHARE_READ_WRITE: u32 = 0x0000_0003;

        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .share_mode(FILE_SHARE_READ_WRITE)
            .open(path)
            .map_err(PagedbError::Io)?;

        let handle = file.as_raw_handle() as windows_sys::Win32::Foundation::HANDLE;

        let flags = match kind {
            LockKind::Exclusive => LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            LockKind::Shared => LOCKFILE_FAIL_IMMEDIATELY,
        };

        // SAFETY: `handle` is valid and owned by `file` which is alive for the
        // duration of this call. `overlapped` is zero-initialised — LockFileEx
        // requires a pointer to OVERLAPPED even when LOCKFILE_FAIL_IMMEDIATELY
        // is set (the call completes synchronously in that mode); zeroing all
        // fields is the correct initialisation for a synchronous, non-event
        // OVERLAPPED. We cover bytes [0, u64::MAX) which is the conventional
        // "whole file" range. `dwreserved` must be 0 per MSDN.
        let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
        let rc = unsafe { LockFileEx(handle, flags, 0, u32::MAX, u32::MAX, &mut overlapped) };

        if rc == 0 {
            // SAFETY: Calling GetLastError immediately after a failed Win32
            // call is the documented pattern; no other OS calls intervene.
            let err_code = unsafe { windows_sys::Win32::Foundation::GetLastError() };
            if err_code == ERROR_LOCK_VIOLATION || err_code == ERROR_IO_PENDING {
                return Err(PagedbError::AlreadyLocked);
            }
            return Err(PagedbError::Io(std::io::Error::last_os_error()));
        }

        Ok(Self { file })
    }
}

#[cfg(windows)]
impl Drop for OsLockFileExHandle {
    fn drop(&mut self) {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::UnlockFileEx;
        use windows_sys::Win32::System::IO::OVERLAPPED;

        let handle = self.file.as_raw_handle() as windows_sys::Win32::Foundation::HANDLE;
        // SAFETY: `handle` is valid (file is still open at drop time — the
        // file field is dropped after this impl returns). Zero-initialised
        // OVERLAPPED is required by UnlockFileEx for a synchronous call.
        // Unlocking the full [0, u64::MAX) byte range matches what LockFileEx
        // locked. Ignoring the return value on Drop is intentional: we cannot
        // propagate errors from Drop, and a failed unlock during process exit
        // is harmless because Windows releases all file locks when the handle
        // is closed.
        let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
        let _ = unsafe { UnlockFileEx(handle, 0, u32::MAX, u32::MAX, &mut overlapped) };
        // `self.file` is dropped here, closing the handle.
    }
}

// SAFETY: The raw HANDLE is valid across threads; we do not share it between
// threads — the struct is moved as a whole.
#[cfg(windows)]
unsafe impl Send for OsLockFileExHandle {}

/// The OS lock primitive for this target. Unix and Windows are mutually
/// exclusive cfgs, so this names exactly one type wherever it exists.
#[cfg(unix)]
type OsLock = OsFcntlHandle;
#[cfg(windows)]
type OsLock = OsLockFileExHandle;

// ---------------------------------------------------------------------------
// Public lock handle.
// ---------------------------------------------------------------------------

/// Advisory lock returned by every native backend's `lock_exclusive` and
/// `lock_shared`. The last holder of a file releases the OS lock on drop.
pub struct NativeLockHandle {
    entry: Arc<InProcLockEntry>,
    kind: LockKind,
}

impl Drop for NativeLockHandle {
    fn drop(&mut self) {
        self.entry.leave(self.kind);
    }
}

/// Acquire an advisory lock on the sentinel file at `lock_path`, creating the
/// file and its directory when absent.
pub(crate) async fn acquire(lock_path: PathBuf, kind: LockKind) -> Result<NativeLockHandle> {
    #[cfg(any(unix, windows))]
    {
        // Filesystem calls, so off the async thread.
        let key = offload(move || resolve_lock_key(&lock_path)).await?;
        let entry = entry(key.clone());
        {
            let _gate = entry.gate.lock().await;
            if !entry.try_join(kind)? {
                // Creating the sentinel file can stall on the filesystem.
                let os = offload(move || OsLock::try_acquire(&key, kind)).await?;
                entry.install(kind, os);
            }
        }
        Ok(NativeLockHandle { entry, kind })
    }
    #[cfg(not(any(unix, windows)))]
    {
        let entry = entry(lock_path);
        {
            let _gate = entry.gate.lock().await;
            if !entry.try_join(kind)? {
                entry.install(kind);
            }
        }
        Ok(NativeLockHandle { entry, kind })
    }
}

/// Canonical directory plus file name, so two spellings of one path match.
#[cfg(any(unix, windows))]
fn resolve_lock_key(lock_path: &std::path::Path) -> Result<PathBuf> {
    let parent = lock_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    std::fs::create_dir_all(parent).map_err(PagedbError::Io)?;
    let parent = std::fs::canonicalize(parent).map_err(PagedbError::Io)?;
    let name = lock_path.file_name().ok_or_else(|| {
        PagedbError::Io(std::io::Error::other(format!(
            "lock path has no file name: {}",
            lock_path.display()
        )))
    })?;
    Ok(parent.join(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lock_file(dir: &tempfile::TempDir, name: &str) -> PathBuf {
        dir.path().join(name)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_exclusive_lock_excludes_every_other_kind() {
        let dir = tempfile::tempdir().unwrap();
        let _held = acquire(lock_file(&dir, "a.lock"), LockKind::Exclusive)
            .await
            .unwrap();
        assert!(matches!(
            acquire(lock_file(&dir, "a.lock"), LockKind::Exclusive).await,
            Err(PagedbError::AlreadyLocked)
        ));
        assert!(matches!(
            acquire(lock_file(&dir, "a.lock"), LockKind::Shared).await,
            Err(PagedbError::AlreadyLocked)
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shared_holds_stack_and_only_the_last_release_frees_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let first = acquire(lock_file(&dir, "a.lock"), LockKind::Shared)
            .await
            .unwrap();
        let second = acquire(lock_file(&dir, "a.lock"), LockKind::Shared)
            .await
            .unwrap();

        drop(first);
        assert!(
            matches!(
                acquire(lock_file(&dir, "a.lock"), LockKind::Exclusive).await,
                Err(PagedbError::AlreadyLocked)
            ),
            "one shared holder remains, so exclusive must still be refused"
        );

        drop(second);
        acquire(lock_file(&dir, "a.lock"), LockKind::Exclusive)
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn two_spellings_of_one_lock_file_share_one_domain() {
        let dir = tempfile::tempdir().unwrap();
        let _held = acquire(lock_file(&dir, "a.lock"), LockKind::Exclusive)
            .await
            .unwrap();
        let respelled = dir.path().join("sub").join("..").join("a.lock");
        std::fs::create_dir_all(dir.path().join("sub")).unwrap();
        assert!(
            matches!(
                acquire(respelled, LockKind::Exclusive).await,
                Err(PagedbError::AlreadyLocked)
            ),
            "a second spelling of the path must not bypass the holder"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn distinct_lock_files_are_distinct_domains() {
        let dir = tempfile::tempdir().unwrap();
        let _a = acquire(lock_file(&dir, "a.lock"), LockKind::Exclusive)
            .await
            .unwrap();
        acquire(lock_file(&dir, "b.lock"), LockKind::Exclusive)
            .await
            .unwrap();
    }
}
