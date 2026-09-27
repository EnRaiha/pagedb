//! A restored directory keeps its restore mode across reopens.
//!
//! `restore_from` copies `main.db` byte for byte.
//! The copy shares the source's `file_id`, `kek_salt`, and `mk_epoch`: one DEK and one nonce space.
//! If both directories take independent writes, nonces repeat under one key.
//! The restore mode in the A/B header prevents that.
//!
//! A restored directory reopens read-only, or as a Follower after `promote_to_follower`.
//! Every Standalone open is refused with `RestoredNotPromoted`.
//! The fork out of that state is covered in `restored_store_fork.rs`.

mod common;

use pagedb::vfs::tokio_backend::TokioVfs;
use pagedb::{Db, DbMode, OpenOptions, PagedbError};

use common::{KEK, PAGE, REALM, describe, open_read_only, open_standalone, restore_into};

/// Authenticates nothing in these stores.
/// A counterpart key matters only when the primary key cannot open the header.
/// Any other value works here.
const COUNTERPART_KEK: [u8; 32] = [8u8; 32];

#[tokio::test(flavor = "current_thread")]
async fn a_restored_store_is_refused_as_a_standalone_writer() {
    let dst = tempfile::tempdir().unwrap();
    let _ = restore_into(dst.path()).await;

    let standalone = open_standalone(dst.path()).await;
    assert!(
        matches!(standalone, Err(PagedbError::RestoredNotPromoted)),
        "a restored copy shares the source nonce space and must not open as a writer, got {}",
        describe(&standalone)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_restored_store_reopens_read_only() {
    let dst = tempfile::tempdir().unwrap();
    let _ = restore_into(dst.path()).await;

    let reopened = open_read_only(dst.path()).await.unwrap();
    assert_eq!(reopened.mode(), DbMode::ReadOnly);
    let reader = reopened.begin_read().await.unwrap();
    assert_eq!(
        reader.get(b"k").await.unwrap().as_deref(),
        Some(b"v".as_slice())
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_restored_store_stays_restored_after_a_read_only_reopen() {
    let dst = tempfile::tempdir().unwrap();
    let _ = restore_into(dst.path()).await;
    drop(open_read_only(dst.path()).await.unwrap());

    let standalone = open_standalone(dst.path()).await;
    assert!(
        matches!(standalone, Err(PagedbError::RestoredNotPromoted)),
        "a read-only reopen must not clear the restore mode, got {}",
        describe(&standalone)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_follower_store_is_refused_as_a_standalone_writer() {
    let dst = tempfile::tempdir().unwrap();
    let _ = restore_into(dst.path()).await;
    let follower = open_read_only(dst.path())
        .await
        .unwrap()
        .promote_to_follower()
        .await
        .unwrap();
    assert_eq!(follower.mode(), DbMode::Follower);
    drop(follower);

    let standalone = open_standalone(dst.path()).await;
    assert!(
        matches!(standalone, Err(PagedbError::RestoredNotPromoted)),
        "a Follower keeps the source identity and must not reopen as an independent writer, got {}",
        describe(&standalone)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_counterpart_kek_open_refuses_a_restored_store() {
    let dst = tempfile::tempdir().unwrap();
    let _ = restore_into(dst.path()).await;

    let resumed = Db::open_existing_with_counterpart_kek(
        TokioVfs::new(dst.path()),
        KEK,
        COUNTERPART_KEK,
        PAGE,
        REALM,
        OpenOptions::default(),
    )
    .await;
    assert!(
        matches!(resumed, Err(PagedbError::RestoredNotPromoted)),
        "a counterpart-key open is a Standalone writer and must refuse a restored store, got {}",
        describe(&resumed)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_follower_store_reopens_as_a_follower() {
    let dst = tempfile::tempdir().unwrap();
    let _ = restore_into(dst.path()).await;
    drop(
        open_read_only(dst.path())
            .await
            .unwrap()
            .promote_to_follower()
            .await
            .unwrap(),
    );

    let follower = Db::open_follower(
        TokioVfs::new(dst.path()),
        KEK,
        PAGE,
        REALM,
        OpenOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(follower.mode(), DbMode::Follower);
    let reader = follower.begin_read().await.unwrap();
    assert_eq!(
        reader.get(b"k").await.unwrap().as_deref(),
        Some(b"v".as_slice())
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_restored_store_opened_as_a_follower_stays_refused_as_a_writer() {
    let dst = tempfile::tempdir().unwrap();
    let _ = restore_into(dst.path()).await;
    let follower = Db::open_follower(
        TokioVfs::new(dst.path()),
        KEK,
        PAGE,
        REALM,
        OpenOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(follower.mode(), DbMode::Follower);
    drop(follower);

    let standalone = open_standalone(dst.path()).await;
    assert!(
        matches!(standalone, Err(PagedbError::RestoredNotPromoted)),
        "a Follower open must not clear the restore mode, got {}",
        describe(&standalone)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_follower_open_of_a_missing_store_reports_not_found() {
    let dir = tempfile::tempdir().unwrap();

    let follower = Db::open_follower(
        TokioVfs::new(dir.path()),
        KEK,
        PAGE,
        REALM,
        OpenOptions::default(),
    )
    .await;
    assert!(
        matches!(follower, Err(PagedbError::NotFound)),
        "a Follower tracks an existing store and never creates one, got {}",
        describe(&follower)
    );
}
