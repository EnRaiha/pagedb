// SPDX-License-Identifier: Apache-2.0
#![cfg(all(target_arch = "wasm32", target_os = "unknown"))]

use std::time::Duration;

use gloo_timers::future::TimeoutFuture;
use pagedb::vfs::memory::MemVfs;
use pagedb::{CommitId, Db, OpenOptions, PagedbError, RealmId, RetainPolicy};
use wasm_bindgen_test::wasm_bindgen_test;

const PAGE: usize = 4096;
const KEK: [u8; 32] = [7; 32];
const REALM: RealmId = RealmId::new([1; 16]);

async fn open_with_policy(policy: RetainPolicy) -> Db<MemVfs> {
    Db::open(
        MemVfs::new(),
        KEK,
        PAGE,
        REALM,
        OpenOptions::default().with_commit_history_retain(policy),
    )
    .await
    .unwrap()
}

async fn write_values(db: &Db<MemVfs>, count: u8) -> Vec<CommitId> {
    let mut ids = Vec::with_capacity(usize::from(count));
    for value in 0..count {
        let mut write = db.begin_write().await.unwrap();
        write.put(b"key", &[value]).await.unwrap();
        ids.push(write.commit().await.unwrap());
    }
    ids
}

async fn assert_snapshot(db: &Db<MemVfs>, commit: CommitId, value: u8) {
    let read = db.begin_read_at(commit).await.unwrap();
    assert_eq!(read.commit_id(), commit);
    assert_eq!(
        read.get(b"key").await.unwrap().as_deref(),
        Some([value].as_slice())
    );
}

async fn assert_gone(db: &Db<MemVfs>, commit: CommitId, oldest: CommitId) {
    assert!(matches!(
        db.begin_read_at(commit).await,
        Err(PagedbError::CommitGone { commit: gone, oldest_available })
            if gone == commit && oldest_available == oldest
    ));
}

#[wasm_bindgen_test]
async fn disabled_history_writes_and_reads_latest() {
    let db = open_with_policy(RetainPolicy::Disabled).await;
    let ids = write_values(&db, 3).await;

    let read = db.begin_read().await.unwrap();
    assert_eq!(read.commit_id(), ids[2]);
    assert_eq!(
        read.get(b"key").await.unwrap().as_deref(),
        Some([2].as_slice())
    );
    drop(read);
    assert_snapshot(&db, ids[2], 2).await;
    for &commit in &ids[..2] {
        assert_gone(&db, commit, ids[2]).await;
    }
}

#[wasm_bindgen_test]
async fn count_history_keeps_newest_snapshots() {
    let db = open_with_policy(RetainPolicy::Count(2)).await;
    let ids = write_values(&db, 5).await;

    for &commit in &ids[..3] {
        assert_gone(&db, commit, ids[3]).await;
    }
    assert_snapshot(&db, ids[3], 3).await;
    assert_snapshot(&db, ids[4], 4).await;
}

#[wasm_bindgen_test]
async fn unbounded_history_preserves_snapshots_across_reopen() {
    let vfs = MemVfs::new();
    let options = OpenOptions::default().with_commit_history_retain(RetainPolicy::Unbounded);
    let db = Db::open(vfs.clone(), KEK, PAGE, REALM, options.clone())
        .await
        .unwrap();
    let ids = write_values(&db, 4).await;
    for (value, &commit) in (0_u8..).zip(&ids) {
        assert_snapshot(&db, commit, value).await;
    }
    drop(db);

    let reopened = Db::open(vfs, KEK, PAGE, REALM, options).await.unwrap();
    for (value, &commit) in (0_u8..).zip(&ids) {
        assert_snapshot(&reopened, commit, value).await;
    }
}

#[wasm_bindgen_test]
async fn age_history_keeps_recent_snapshots() {
    let db = open_with_policy(RetainPolicy::Age(Duration::from_secs(3600))).await;
    let ids = write_values(&db, 3).await;

    for (value, &commit) in (0_u8..).zip(&ids) {
        assert_snapshot(&db, commit, value).await;
    }
}

#[wasm_bindgen_test]
async fn age_history_prunes_expired_snapshot() {
    let db = open_with_policy(RetainPolicy::Age(Duration::ZERO)).await;
    let first = write_values(&db, 1).await[0];
    assert_snapshot(&db, first, 0).await;

    // Cross a full second so the first timestamp precedes the pruning threshold.
    TimeoutFuture::new(1100).await;
    let mut write = db.begin_write().await.unwrap();
    write.put(b"key", &[1]).await.unwrap();
    let latest = write.commit().await.unwrap();

    assert_gone(&db, first, latest).await;
    assert_snapshot(&db, latest, 1).await;
}
