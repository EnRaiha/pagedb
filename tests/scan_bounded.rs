// SPDX-License-Identifier: Apache-2.0
//! Snapshot prefix scans with payload-byte and record budgets.

use pagedb::vfs::memory::MemVfs;
use pagedb::{Db, OpenOptions, RealmId, ScanLimit};

async fn open_db() -> Db<MemVfs> {
    let db = Db::open(
        MemVfs::new(),
        [9; 32],
        4096,
        RealmId::new([1; 16]),
        OpenOptions::default(),
    )
    .await
    .unwrap();
    let mut write = db.begin_write().await.unwrap();
    for (key, value) in [(b"p:a", b"one"), (b"p:b", b"two"), (b"q:a", b"end")] {
        write.put(key, value).await.unwrap();
    }
    write.commit().await.unwrap();
    db
}

#[cfg_attr(
    all(target_arch = "wasm32", target_os = "unknown"),
    wasm_bindgen_test::wasm_bindgen_test
)]
#[cfg_attr(
    not(all(target_arch = "wasm32", target_os = "unknown")),
    tokio::test(flavor = "current_thread")
)]
async fn bounded_prefix_scan_reports_exact_budget_completion() {
    let db = open_db().await;
    let read = db.begin_read().await.unwrap();
    let batch = read
        .scan_prefix_from_bounded(b"p:", b"p:", 2, 12)
        .await
        .unwrap();
    assert_eq!(batch.entries.len(), 2);
    assert_eq!(batch.entries[0].0.as_ref(), b"p:a");
    assert_eq!(batch.entries[0].1.as_ref(), b"one");
    assert_eq!(batch.entries[1].0.as_ref(), b"p:b");
    assert_eq!(batch.entries[1].1.as_ref(), b"two");
    assert_eq!(batch.limit, None);
}

#[cfg_attr(
    all(target_arch = "wasm32", target_os = "unknown"),
    wasm_bindgen_test::wasm_bindgen_test
)]
#[cfg_attr(
    not(all(target_arch = "wasm32", target_os = "unknown")),
    tokio::test(flavor = "current_thread")
)]
async fn bounded_prefix_scan_reports_each_budget() {
    let db = open_db().await;
    let read = db.begin_read().await.unwrap();
    for (records, bytes, limit) in [
        (1, 12, ScanLimit::Records),
        (2, 6, ScanLimit::Bytes),
        (1, 6, ScanLimit::Records),
        (2, 11, ScanLimit::Bytes),
    ] {
        let batch = read
            .scan_prefix_from_bounded(b"p:", b"p:", records, bytes)
            .await
            .unwrap();
        assert_eq!(batch.entries.len(), 1);
        assert_eq!(batch.entries[0].0.as_ref(), b"p:a");
        assert_eq!(batch.entries[0].1.as_ref(), b"one");
        assert_eq!(batch.limit, Some(limit));
    }
    let batch = read
        .scan_prefix_from_bounded(b"p:", b"p:", 2, 5)
        .await
        .unwrap();
    assert!(batch.entries.is_empty());
    assert_eq!(batch.limit, Some(ScanLimit::Bytes));
    let batch = read
        .scan_prefix_from_bounded(b"p:", b"p:", 2, 0)
        .await
        .unwrap();
    assert!(batch.entries.is_empty());
    assert_eq!(batch.limit, Some(ScanLimit::Bytes));
}

#[cfg_attr(
    all(target_arch = "wasm32", target_os = "unknown"),
    wasm_bindgen_test::wasm_bindgen_test
)]
#[cfg_attr(
    not(all(target_arch = "wasm32", target_os = "unknown")),
    tokio::test(flavor = "current_thread")
)]
async fn bounded_prefix_scan_includes_start_and_resumes_without_duplicates() {
    let db = open_db().await;
    let read = db.begin_read().await.unwrap();
    let first = read
        .scan_prefix_from_bounded(b"p:", b"", 1, 6)
        .await
        .unwrap();
    assert_eq!(first.entries[0].0.as_ref(), b"p:a");
    assert_eq!(first.limit, Some(ScanLimit::Records));
    let mut start = first.entries[0].0.to_vec();
    start.push(0);
    let next = read
        .scan_prefix_from_bounded(b"p:", &start, 1, 6)
        .await
        .unwrap();
    assert_eq!(next.entries.len(), 1);
    assert_eq!(next.entries[0].0.as_ref(), b"p:b");
    assert_eq!(next.limit, None);
    let inclusive = read
        .scan_prefix_from_bounded(b"p:", b"p:b", 1, 6)
        .await
        .unwrap();
    assert_eq!(inclusive, next);
}

#[cfg_attr(
    all(target_arch = "wasm32", target_os = "unknown"),
    wasm_bindgen_test::wasm_bindgen_test
)]
#[cfg_attr(
    not(all(target_arch = "wasm32", target_os = "unknown")),
    tokio::test(flavor = "current_thread")
)]
async fn bounded_prefix_scan_distinguishes_zero_records_from_empty_range() {
    let db = open_db().await;
    let read = db.begin_read().await.unwrap();
    let batch = read
        .scan_prefix_from_bounded(b"p:", b"p:", 0, 0)
        .await
        .unwrap();
    assert!(batch.entries.is_empty());
    assert_eq!(batch.limit, Some(ScanLimit::Records));
    let batch = read
        .scan_prefix_from_bounded(b"missing", b"", 0, 0)
        .await
        .unwrap();
    assert!(batch.entries.is_empty());
    assert_eq!(batch.limit, None);
}

#[cfg_attr(
    all(target_arch = "wasm32", target_os = "unknown"),
    wasm_bindgen_test::wasm_bindgen_test
)]
#[cfg_attr(
    not(all(target_arch = "wasm32", target_os = "unknown")),
    tokio::test(flavor = "current_thread")
)]
async fn bounded_prefix_scan_pages_with_one_snapshot() {
    let db = open_db().await;
    let read = db.begin_read().await.unwrap();
    let first = read
        .scan_prefix_from_bounded(b"p:", b"p:", 1, 6)
        .await
        .unwrap();
    let mut write = db.begin_write().await.unwrap();
    write.put(b"p:b", b"new").await.unwrap();
    write.put(b"p:c", b"add").await.unwrap();
    write.commit().await.unwrap();
    let next = read
        .scan_prefix_from_bounded(b"p:", b"p:b", 2, 12)
        .await
        .unwrap();
    assert_eq!(first.entries[0].1.as_ref(), b"one");
    assert_eq!(next.entries.len(), 1);
    assert_eq!(next.entries[0].1.as_ref(), b"two");
    assert_eq!(next.limit, None);
}

#[cfg_attr(
    all(target_arch = "wasm32", target_os = "unknown"),
    wasm_bindgen_test::wasm_bindgen_test
)]
#[cfg_attr(
    not(all(target_arch = "wasm32", target_os = "unknown")),
    tokio::test(flavor = "current_thread")
)]
async fn bounded_prefix_scan_crosses_leaf_boundaries() {
    let db = open_db().await;
    let value = vec![42; 900];
    let mut write = db.begin_write().await.unwrap();
    for index in 0..32 {
        write
            .put(format!("r:{index:02}").as_bytes(), &value)
            .await
            .unwrap();
    }
    write.commit().await.unwrap();
    let read = db.begin_read().await.unwrap();
    let first = read
        .scan_prefix_from_bounded(b"r:", b"r:", 16, 16 * 904)
        .await
        .unwrap();
    assert_eq!(first.entries.len(), 16);
    assert_eq!(first.limit, Some(ScanLimit::Records));
    for (index, (key, bytes)) in first.entries.iter().enumerate() {
        assert_eq!(key.as_ref(), format!("r:{index:02}").as_bytes());
        assert_eq!(bytes.as_ref(), value.as_slice());
    }
    let rest = read
        .scan_prefix_from_bounded(b"r:", b"r:16", 16, 16 * 904)
        .await
        .unwrap();
    assert_eq!(rest.entries.len(), 16);
    assert_eq!(rest.entries[0].0.as_ref(), b"r:16");
    assert_eq!(rest.entries[15].0.as_ref(), b"r:31");
    assert_eq!(rest.limit, None);
}

#[cfg_attr(
    all(target_arch = "wasm32", target_os = "unknown"),
    wasm_bindgen_test::wasm_bindgen_test
)]
#[cfg_attr(
    not(all(target_arch = "wasm32", target_os = "unknown")),
    tokio::test(flavor = "current_thread")
)]
async fn bounded_prefix_scan_accepts_overflow_at_exact_byte_budget() {
    let db = open_db().await;
    let value = vec![42; 8192];
    let mut write = db.begin_write().await.unwrap();
    write.put(b"large", &value).await.unwrap();
    write.commit().await.unwrap();
    let read = db.begin_read().await.unwrap();
    let batch = read
        .scan_prefix_from_bounded(b"large", b"large", 1, 8197)
        .await
        .unwrap();
    assert_eq!(batch.entries.len(), 1);
    assert_eq!(batch.entries[0].0.as_ref(), b"large");
    assert_eq!(batch.entries[0].1.as_ref(), value.as_slice());
    assert_eq!(batch.limit, None);
}
