//! `rekey_into_writer` forks a restored directory into an independent
//! Standalone writer under a fresh identity.
//!
//! The fork re-encrypts every page and segment under a new `kek_salt` and KEK.
//! It writes the result into a replacement `main.db` and publishes it by rename.
//!
//! These tests cover:
//! - what the fork carries
//! - the identity it gets
//! - who can fork
//! - both sides of the rename commit point

mod common;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use pagedb::vfs::tokio_backend::TokioVfs;
use pagedb::vfs::{OpenMode, Vfs, VfsFile};
use pagedb::{Db, DbMode, OpenOptions, PagedbError};

use common::{KEK, PAGE, REALM, SEGMENT, describe, open_read_only, open_standalone, restore_into};

const NEW_KEK: [u8; 32] = [7u8; 32];

/// The `kek_salt` recorded in the active header of `dir/main.db`. Every page
/// key derives from it, so two stores with different salts share no key.
fn header_kek_salt(dir: &Path) -> [u8; 16] {
    let bytes = std::fs::read(dir.join("main.db")).unwrap();
    let mut salt = [0u8; 16];
    salt.copy_from_slice(&bytes[32..48]);
    salt
}

/// Everything the source held is readable through `db`.
/// The segment keeps its page contents but gets a fresh id.
async fn assert_carries_source_data(db: &Db<TokioVfs>, source_segment: [u8; 16]) {
    let reader = db.begin_read().await.unwrap();
    assert_eq!(
        reader.get(b"k").await.unwrap().as_deref(),
        Some(b"v".as_slice())
    );
    drop(reader);
    let segment = db.open_segment(REALM, SEGMENT).await.unwrap();
    assert!(segment.read_page(1).await.unwrap().starts_with(b"page-one"));
    assert!(segment.read_page(2).await.unwrap().starts_with(b"page-two"));
    assert_ne!(
        segment.meta().segment_id,
        source_segment,
        "a forked segment must be re-encrypted under a fresh id"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_restored_store_forks_into_an_independent_writer() {
    let dst = tempfile::tempdir().unwrap();
    let source_segment = restore_into(dst.path()).await;

    let fork = open_read_only(dst.path())
        .await
        .unwrap()
        .rekey_into_writer(NEW_KEK)
        .await
        .unwrap();
    assert_eq!(fork.mode(), DbMode::Standalone);
    assert_carries_source_data(&fork, source_segment).await;

    let mut w = fork.begin_write().await.unwrap();
    w.put(b"after-fork", b"written").await.unwrap();
    w.commit().await.unwrap();
    drop(fork);

    let reopened = Db::open(
        TokioVfs::new(dst.path()),
        NEW_KEK,
        PAGE,
        REALM,
        OpenOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(reopened.mode(), DbMode::Standalone);
    assert_carries_source_data(&reopened, source_segment).await;
    let reader = reopened.begin_read().await.unwrap();
    assert_eq!(
        reader.get(b"after-fork").await.unwrap().as_deref(),
        Some(b"written".as_slice())
    );
    drop(reader);

    let old_segment = dst
        .path()
        .join("seg")
        .join(pagedb::hex::to_hex_lower(&source_segment));
    assert!(
        !old_segment.exists(),
        "the source-identity segment must be swept once the fork is open"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_fork_under_the_same_kek_shares_no_key_with_its_source() {
    let dst = tempfile::tempdir().unwrap();
    let _ = restore_into(dst.path()).await;
    let source_salt = header_kek_salt(dst.path());

    let fork = open_read_only(dst.path())
        .await
        .unwrap()
        .rekey_into_writer(KEK)
        .await
        .unwrap();
    drop(fork);

    assert_ne!(
        header_kek_salt(dst.path()),
        source_salt,
        "a fork keyed from the source salt would derive the source's page keys"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_forked_store_does_not_open_under_the_source_kek() {
    let dst = tempfile::tempdir().unwrap();
    let _ = restore_into(dst.path()).await;
    drop(
        open_read_only(dst.path())
            .await
            .unwrap()
            .rekey_into_writer(NEW_KEK)
            .await
            .unwrap(),
    );

    let under_source_key = open_standalone(dst.path()).await;
    assert!(
        matches!(under_source_key, Err(PagedbError::KeyMismatch)),
        "the fork is keyed by the new KEK only, got {}",
        describe(&under_source_key)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_follower_store_forks_into_an_independent_writer() {
    let dst = tempfile::tempdir().unwrap();
    let source_segment = restore_into(dst.path()).await;
    let follower = open_read_only(dst.path())
        .await
        .unwrap()
        .promote_to_follower()
        .await
        .unwrap();

    let fork = follower.rekey_into_writer(NEW_KEK).await.unwrap();
    assert_eq!(fork.mode(), DbMode::Standalone);
    assert_carries_source_data(&fork, source_segment).await;
}

#[tokio::test(flavor = "current_thread")]
async fn a_fork_is_refused_while_another_reader_holds_the_store() {
    let dst = tempfile::tempdir().unwrap();
    let _ = restore_into(dst.path()).await;
    let other_reader = open_read_only(dst.path()).await.unwrap();

    let fork = open_read_only(dst.path())
        .await
        .unwrap()
        .rekey_into_writer(NEW_KEK)
        .await;
    assert!(
        matches!(fork, Err(PagedbError::ReadersPresent)),
        "the fork replaces main.db and must not run under another reader, got {}",
        describe(&fork)
    );
    drop(other_reader);

    let standalone = open_standalone(dst.path()).await;
    assert!(
        matches!(standalone, Err(PagedbError::RestoredNotPromoted)),
        "a refused fork must leave the directory restored, got {}",
        describe(&standalone)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_standalone_handle_cannot_fork() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_standalone(dir.path()).await.unwrap();

    let fork = db.rekey_into_writer(NEW_KEK).await;
    assert!(
        matches!(
            fork,
            Err(PagedbError::WrongMode {
                operation: "rekey_into_writer",
                required: DbMode::ReadOnly,
                actual: DbMode::Standalone,
            })
        ),
        "an original store already has its own identity, got {}",
        describe(&fork)
    );
}

/// Where [`ForkFaultVfs`] interrupts a fork.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ForkFault {
    /// Fails the rename that publishes the replacement `main.db`.
    /// The fork stops at its commit point.
    BeforePublication,
    /// Lets the rename land, then fails the directory sync after it.
    /// The fork's `main.db` is live, but the pager never adopts its segments.
    AfterPublication,
}

#[derive(Clone)]
struct ForkFaultVfs {
    inner: TokioVfs,
    fault: ForkFault,
    published: Arc<AtomicBool>,
}

impl ForkFaultVfs {
    fn new(dir: &Path, fault: ForkFault) -> Self {
        Self {
            inner: TokioVfs::new(dir),
            fault,
            published: Arc::new(AtomicBool::new(false)),
        }
    }
}

fn injected(what: &str) -> PagedbError {
    PagedbError::Io(std::io::Error::other(format!(
        "injected fork fault: {what}"
    )))
}

impl Vfs for ForkFaultVfs {
    type File = <TokioVfs as Vfs>::File;
    type LockHandle = <TokioVfs as Vfs>::LockHandle;

    async fn open(&self, path: &str, mode: OpenMode) -> pagedb::Result<Self::File> {
        self.inner.open(path, mode).await
    }

    async fn remove(&self, path: &str) -> pagedb::Result<()> {
        self.inner.remove(path).await
    }

    async fn rename(&self, from: &str, to: &str) -> pagedb::Result<()> {
        let publishes_fork = from.ends_with("main.db.fork");
        if publishes_fork && self.fault == ForkFault::BeforePublication {
            return Err(injected("publication rename"));
        }
        self.inner.rename(from, to).await?;
        if publishes_fork {
            self.published.store(true, Ordering::SeqCst);
        }
        Ok(())
    }

    async fn list_dir(&self, path: &str) -> pagedb::Result<Vec<String>> {
        self.inner.list_dir(path).await
    }

    async fn mkdir_all(&self, path: &str) -> pagedb::Result<()> {
        self.inner.mkdir_all(path).await
    }

    async fn sync_dir(&self, path: &str) -> pagedb::Result<()> {
        if self.fault == ForkFault::AfterPublication && self.published.swap(false, Ordering::SeqCst)
        {
            return Err(injected("sync after publication"));
        }
        self.inner.sync_dir(path).await
    }

    async fn lock_exclusive(&self, path: &str) -> pagedb::Result<Self::LockHandle> {
        self.inner.lock_exclusive(path).await
    }

    async fn lock_shared(&self, path: &str) -> pagedb::Result<Self::LockHandle> {
        self.inner.lock_shared(path).await
    }

    fn root_path(&self) -> Option<&Path> {
        Vfs::root_path(&self.inner)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_fork_that_fails_before_publication_leaves_the_store_restored() {
    let dst = tempfile::tempdir().unwrap();
    let _ = restore_into(dst.path()).await;
    // A scratch left by an earlier interrupted fork must not block this one.
    {
        let vfs = TokioVfs::new(dst.path());
        let mut stale = vfs
            .open("/main.db.fork", OpenMode::CreateNew)
            .await
            .unwrap();
        stale.write_at(0, b"stale fork scratch").await.unwrap();
        stale.sync().await.unwrap();
    }

    let vfs = ForkFaultVfs::new(dst.path(), ForkFault::BeforePublication);
    let reader = Db::open_read_only(vfs, KEK, PAGE, REALM, OpenOptions::default())
        .await
        .unwrap();
    let fork = reader.rekey_into_writer(NEW_KEK).await;
    assert!(fork.is_err(), "the injected rename failure must surface");

    let standalone = open_standalone(dst.path()).await;
    assert!(
        matches!(standalone, Err(PagedbError::RestoredNotPromoted)),
        "an unpublished fork must leave the directory restored, got {}",
        describe(&standalone)
    );
    let reopened = open_read_only(dst.path()).await.unwrap();
    let reader = reopened.begin_read().await.unwrap();
    assert_eq!(
        reader.get(b"k").await.unwrap().as_deref(),
        Some(b"v".as_slice())
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_fork_interrupted_after_publication_opens_with_every_segment() {
    let dst = tempfile::tempdir().unwrap();
    let source_segment = restore_into(dst.path()).await;

    let vfs = ForkFaultVfs::new(dst.path(), ForkFault::AfterPublication);
    let reader = Db::open_read_only(vfs, KEK, PAGE, REALM, OpenOptions::default())
        .await
        .unwrap();
    let fork = reader.rekey_into_writer(NEW_KEK).await;
    assert!(
        fork.is_err(),
        "the injected post-publication fault must surface"
    );

    // The rename landed, so the fork is the store now. Its first open adopts
    // the segments the fork wrote and publishes the ones its catalog names.
    let reopened = Db::open(
        TokioVfs::new(dst.path()),
        NEW_KEK,
        PAGE,
        REALM,
        OpenOptions::default(),
    )
    .await
    .unwrap();
    assert_carries_source_data(&reopened, source_segment).await;
}
