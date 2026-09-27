//! Every public open applies its mode's discipline.
//!
//! A handle's mode decides three things at open:
//! - which sentinel it holds
//! - whether a missing store bootstraps or refuses
//! - whether a page that fails authentication is re-read
//!
//! The unpromoted-restore refusal is covered in `restored_store_modes.rs`.
//!
//! These tests hold every public entry point to the same answers. This
//! includes `Db::open_existing_with_counterpart_kek`, which resumes an
//! interrupted KEK-changing rekey as a Standalone writer.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use pagedb::vfs::memory::MemVfs;
use pagedb::vfs::{OpenMode, ReadReq, Vfs, VfsFile, WriteReq};
use pagedb::{Db, DbMode, OpenOptions, PagedbError, RealmId};

const PAGE: usize = 4096;
const KEK: [u8; 32] = [9u8; 32];
/// Authenticates nothing in these stores.
/// A counterpart key matters only when the primary key cannot open the header.
/// Any other value works here.
const COUNTERPART_KEK: [u8; 32] = [8u8; 32];
const REALM: RealmId = RealmId::new([1u8; 16]);
const MAIN_DB: &str = "/main.db";
/// Enough rows to build a multi-level tree.
/// A point read after reopen misses the small buffer pool and reaches the VFS.
const ROWS: u32 = 3000;

fn row_key(i: u32) -> Vec<u8> {
    format!("key-{i:06}").into_bytes()
}

async fn seed_store(vfs: MemVfs) {
    let db = Db::open(vfs, KEK, PAGE, REALM, OpenOptions::default())
        .await
        .unwrap();
    let mut w = db.begin_write().await.unwrap();
    for i in 0..ROWS {
        w.put(&row_key(i), &[0x5a; 64]).await.unwrap();
    }
    w.commit().await.unwrap();
}

fn small_pool() -> OpenOptions {
    OpenOptions::default().with_buffer_pool_pages(16)
}

// ── Page-read retry ─────────────────────────────────────────────────────────

/// Corrupts every `main.db` page read past the A/B header slots once armed.
/// Records the offset of each read.
/// The pager reads a page once per attempt, so offsets count the retries.
#[derive(Clone)]
struct TamperingVfs {
    inner: MemVfs,
    armed: Arc<AtomicBool>,
    reads: Arc<Mutex<Vec<u64>>>,
}

impl TamperingVfs {
    fn new(inner: MemVfs) -> Self {
        Self {
            inner,
            armed: Arc::new(AtomicBool::new(false)),
            reads: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    fn tampered_reads(&self) -> Vec<u64> {
        self.reads.lock().unwrap().clone()
    }
}

struct TamperingFile<F> {
    inner: F,
    is_main_db: bool,
    armed: Arc<AtomicBool>,
    reads: Arc<Mutex<Vec<u64>>>,
}

impl<F> TamperingFile<F> {
    /// Flip one byte in the page body. The page header still parses, so the
    /// read fails authentication rather than header decoding.
    fn tamper(&self, offset: u64, buf: &mut [u8]) {
        let past_headers = offset >= 2 * PAGE as u64;
        if self.is_main_db && past_headers && self.armed.load(Ordering::SeqCst) {
            self.reads.lock().unwrap().push(offset);
            let mid = buf.len() / 2;
            buf[mid] ^= 0xff;
        }
    }
}

impl Vfs for TamperingVfs {
    type File = TamperingFile<<MemVfs as Vfs>::File>;
    type LockHandle = <MemVfs as Vfs>::LockHandle;

    async fn open(&self, path: &str, mode: OpenMode) -> pagedb::Result<Self::File> {
        Ok(TamperingFile {
            inner: self.inner.open(path, mode).await?,
            is_main_db: path == MAIN_DB,
            armed: self.armed.clone(),
            reads: self.reads.clone(),
        })
    }

    async fn remove(&self, path: &str) -> pagedb::Result<()> {
        self.inner.remove(path).await
    }

    async fn rename(&self, from: &str, to: &str) -> pagedb::Result<()> {
        self.inner.rename(from, to).await
    }

    async fn list_dir(&self, path: &str) -> pagedb::Result<Vec<String>> {
        self.inner.list_dir(path).await
    }

    async fn mkdir_all(&self, path: &str) -> pagedb::Result<()> {
        self.inner.mkdir_all(path).await
    }

    async fn sync_dir(&self, path: &str) -> pagedb::Result<()> {
        self.inner.sync_dir(path).await
    }

    async fn lock_exclusive(&self, path: &str) -> pagedb::Result<Self::LockHandle> {
        self.inner.lock_exclusive(path).await
    }

    async fn lock_shared(&self, path: &str) -> pagedb::Result<Self::LockHandle> {
        self.inner.lock_shared(path).await
    }
}

impl<F: VfsFile + Sync> VfsFile for TamperingFile<F> {
    async fn read_at(&self, offset: u64, buf: &mut [u8]) -> pagedb::Result<usize> {
        let n = self.inner.read_at(offset, buf).await?;
        self.tamper(offset, &mut buf[..n]);
        Ok(n)
    }

    async fn read_at_vectored(&self, reqs: &mut [ReadReq<'_>]) -> pagedb::Result<()> {
        self.inner.read_at_vectored(reqs).await?;
        for req in reqs.iter_mut() {
            self.tamper(req.offset, req.buf);
        }
        Ok(())
    }

    async fn write_at(&mut self, offset: u64, buf: &[u8]) -> pagedb::Result<usize> {
        self.inner.write_at(offset, buf).await
    }

    async fn write_at_vectored(&mut self, reqs: &[WriteReq<'_>]) -> pagedb::Result<()> {
        self.inner.write_at_vectored(reqs).await
    }

    async fn sync(&mut self) -> pagedb::Result<()> {
        self.inner.sync().await
    }

    async fn truncate(&mut self, len: u64) -> pagedb::Result<()> {
        self.inner.truncate(len).await
    }

    async fn len(&self) -> pagedb::Result<u64> {
        self.inner.len().await
    }

    async fn is_empty(&self) -> pagedb::Result<bool> {
        self.inner.is_empty().await
    }

    fn supports_direct_io(&self) -> bool {
        self.inner.supports_direct_io()
    }
}

/// Reads a row through `db` with every `main.db` page read corrupted.
/// Returns the offsets of the tampered reads.
///
/// The first page the read misses on fails authentication.
/// Every tampered read must target that one page.
/// The count is the number of attempts.
async fn attempts_on_a_failed_page(db: &Db<TamperingVfs>, vfs: &TamperingVfs) -> usize {
    let reader = db.begin_read().await.unwrap();
    vfs.arm();
    let err = match reader.get(&row_key(ROWS / 2)).await {
        Ok(_) => panic!("every page read is corrupted, so the read must fail"),
        Err(err) => err,
    };
    assert!(
        matches!(err, PagedbError::Corruption(_)),
        "a page that fails authentication must report corruption, got {err:?}"
    );
    let reads = vfs.tampered_reads();
    assert!(
        !reads.is_empty(),
        "the read never reached the VFS, so it proves nothing"
    );
    assert!(
        reads.iter().all(|offset| *offset == reads[0]),
        "every attempt must target the failed page, got offsets {reads:?}"
    );
    reads.len()
}

#[tokio::test(flavor = "current_thread")]
async fn a_standalone_open_reads_a_failed_page_once() {
    let mem = MemVfs::new();
    seed_store(mem.clone()).await;
    let vfs = TamperingVfs::new(mem);
    let db = Db::open(vfs.clone(), KEK, PAGE, REALM, small_pool())
        .await
        .unwrap();
    assert_eq!(db.mode(), DbMode::Standalone);

    assert_eq!(attempts_on_a_failed_page(&db, &vfs).await, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn a_read_only_open_reads_a_failed_page_once() {
    let mem = MemVfs::new();
    seed_store(mem.clone()).await;
    let vfs = TamperingVfs::new(mem);
    let db = Db::open_read_only(vfs.clone(), KEK, PAGE, REALM, small_pool())
        .await
        .unwrap();

    assert_eq!(attempts_on_a_failed_page(&db, &vfs).await, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn a_counterpart_kek_open_reads_a_failed_page_once() {
    let mem = MemVfs::new();
    seed_store(mem.clone()).await;
    let vfs = TamperingVfs::new(mem);
    let db = Db::open_existing_with_counterpart_kek(
        vfs.clone(),
        KEK,
        COUNTERPART_KEK,
        PAGE,
        REALM,
        small_pool(),
    )
    .await
    .unwrap();
    assert_eq!(db.mode(), DbMode::Standalone);

    assert_eq!(attempts_on_a_failed_page(&db, &vfs).await, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn an_observer_open_retries_a_failed_page_the_configured_count() {
    let mem = MemVfs::new();
    seed_store(mem.clone()).await;
    let vfs = TamperingVfs::new(mem);
    let db = Db::open_observer(
        vfs.clone(),
        KEK,
        PAGE,
        REALM,
        small_pool().with_observer_retry_count(2),
    )
    .await
    .unwrap();

    assert_eq!(attempts_on_a_failed_page(&db, &vfs).await, 3);
}

// ── Sentinels and missing stores ──────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn a_counterpart_kek_open_holds_the_writer_sentinel() {
    let vfs = MemVfs::new();
    seed_store(vfs.clone()).await;
    let _resumed = Db::open_existing_with_counterpart_kek(
        vfs.clone(),
        KEK,
        COUNTERPART_KEK,
        PAGE,
        REALM,
        OpenOptions::default(),
    )
    .await
    .unwrap();

    let second = Db::open(vfs, KEK, PAGE, REALM, OpenOptions::default()).await;
    assert!(
        matches!(second, Err(PagedbError::AlreadyOpen)),
        "a second writer must be refused while a counterpart-key handle is open, got {:?}",
        second.map(|db| db.mode())
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_counterpart_kek_open_is_refused_while_a_writer_is_open() {
    let vfs = MemVfs::new();
    seed_store(vfs.clone()).await;
    let _writer = Db::open(vfs.clone(), KEK, PAGE, REALM, OpenOptions::default())
        .await
        .unwrap();

    let resumed = Db::open_existing_with_counterpart_kek(
        vfs,
        KEK,
        COUNTERPART_KEK,
        PAGE,
        REALM,
        OpenOptions::default(),
    )
    .await;
    assert!(
        matches!(resumed, Err(PagedbError::AlreadyOpen)),
        "a counterpart-key open must not attach beside a live writer, got {:?}",
        resumed.map(|db| db.mode())
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_counterpart_kek_open_of_a_missing_store_reports_not_found() {
    let vfs = MemVfs::new();

    let resumed = Db::open_existing_with_counterpart_kek(
        vfs.clone(),
        KEK,
        COUNTERPART_KEK,
        PAGE,
        REALM,
        OpenOptions::default(),
    )
    .await;
    assert!(
        matches!(resumed, Err(PagedbError::NotFound)),
        "resuming a rekey needs an existing store, got {:?}",
        resumed.map(|db| db.mode())
    );
    let bootstrapped = vfs.open(MAIN_DB, OpenMode::Read).await.is_ok();
    assert!(!bootstrapped, "a refused open must not create main.db");
}
