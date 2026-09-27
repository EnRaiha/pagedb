//! Helpers shared by the restored-directory integration suites.

use std::path::Path;

use pagedb::vfs::tokio_backend::TokioVfs;
use pagedb::{Db, DbMode, OpenOptions, RealmId, SegmentKind, SegmentPageKind};

pub const PAGE: usize = 4096;
pub const KEK: [u8; 32] = [9u8; 32];
pub const REALM: RealmId = RealmId::new([1u8; 16]);
pub const SEGMENT: &str = "engine.idx";

/// Seeds a source store with one row and a two-page segment, then snapshots it.
/// Restores the snapshot into `dst` and drops every handle so the directory is at rest.
/// Returns the source segment's id.
pub async fn restore_into(dst: &Path) -> [u8; 16] {
    let src_dir = tempfile::tempdir().unwrap();
    let snap_dir = tempfile::tempdir().unwrap();

    let source = Db::open(
        TokioVfs::new(src_dir.path()),
        KEK,
        PAGE,
        REALM,
        OpenOptions::default(),
    )
    .await
    .unwrap();
    let mut segment = source
        .create_segment(REALM, SegmentKind::Unspecified)
        .await
        .unwrap();
    segment
        .append_page(SegmentPageKind::Data, b"page-one")
        .await
        .unwrap();
    segment
        .append_page(SegmentPageKind::Data, b"page-two")
        .await
        .unwrap();
    let segment_meta = segment.seal().await.unwrap();
    let mut w = source.begin_write().await.unwrap();
    w.put(b"k", b"v").await.unwrap();
    w.link_segment(SEGMENT, &segment_meta).await.unwrap();
    w.commit().await.unwrap();
    source.snapshot_to(snap_dir.path()).await.unwrap();
    drop(source);

    let restored = Db::<TokioVfs>::restore_from(snap_dir.path(), dst, OpenOptions::default(), KEK)
        .await
        .unwrap();
    assert_eq!(restored.mode(), DbMode::ReadOnly);
    segment_meta.segment_id
}

pub async fn open_standalone(dir: &Path) -> pagedb::Result<Db<TokioVfs>> {
    Db::open(TokioVfs::new(dir), KEK, PAGE, REALM, OpenOptions::default()).await
}

pub async fn open_read_only(dir: &Path) -> pagedb::Result<Db<TokioVfs>> {
    Db::open_read_only(TokioVfs::new(dir), KEK, PAGE, REALM, OpenOptions::default()).await
}

pub fn describe(result: &pagedb::Result<Db<TokioVfs>>) -> String {
    match result {
        Ok(db) => format!("Ok({:?})", db.mode()),
        Err(err) => format!("Err({err:?})"),
    }
}
