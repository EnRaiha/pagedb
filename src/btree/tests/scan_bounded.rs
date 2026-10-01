// SPDX-License-Identifier: Apache-2.0
//! Metadata-only rejection of unreadable overflow values.

use bytes::Bytes;

use super::basic::fresh_pager;
use crate::btree::BTree;
use crate::btree::leaf::{Leaf, LeafValue};
use crate::btree::node::body_capacity;
use crate::pager::PageKind;
use crate::vfs::memory::MemVfs;
use crate::{RealmId, ScanLimit};

const PAGE: usize = 4096;

async fn fixture(records: Vec<(Vec<u8>, LeafValue)>) -> BTree<MemVfs> {
    let pager = fresh_pager().await;
    let realm = RealmId::new([1; 16]);
    let mut leaf = Leaf::new();
    for (key, value) in records {
        leaf.upsert(&key, value);
    }
    let mut body = vec![0; body_capacity(PAGE)];
    leaf.encode(&mut body).unwrap();
    pager
        .write_main_page(4, realm, PageKind::BTreeLeaf, &body)
        .await
        .unwrap();
    BTree::open(pager, realm, 4, 901, PAGE)
}

fn unreadable(total_len: u64) -> LeafValue {
    // Page 900 is absent, so resolving this value returns an error.
    LeafValue::Overflow {
        total_len,
        root_page_id: 900,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn bounded_scan_rejects_oversized_overflow_without_reading_chain() {
    let tree = fixture(vec![(b"p:a".to_vec(), unreadable(8192))]).await;
    assert!(tree.get(b"p:a").await.is_err());

    let batch = tree
        .collect_prefix_batch_bounded(b"p:", b"p:", 1, 8194)
        .await
        .unwrap();
    assert!(batch.entries.is_empty());
    assert_eq!(batch.limit, Some(ScanLimit::Bytes));
}

#[tokio::test(flavor = "current_thread")]
async fn bounded_scan_zero_records_does_not_resolve_values() {
    let tree = fixture(vec![(b"p:a".to_vec(), unreadable(8192))]).await;
    let batch = tree
        .collect_prefix_batch_bounded(b"p:", b"p:", 0, usize::MAX)
        .await
        .unwrap();
    assert!(batch.entries.is_empty());
    assert_eq!(batch.limit, Some(ScanLimit::Records));
}

#[tokio::test(flavor = "current_thread")]
async fn bounded_scan_record_lookahead_does_not_resolve_values() {
    let tree = fixture(vec![
        (b"p:a".to_vec(), LeafValue::Inline(Bytes::from_static(b"x"))),
        (b"p:b".to_vec(), unreadable(8192)),
    ])
    .await;
    let batch = tree
        .collect_prefix_batch_bounded(b"p:", b"p:", 1, usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        batch.entries,
        vec![(Bytes::from_static(b"p:a"), Bytes::from_static(b"x"))]
    );
    assert_eq!(batch.limit, Some(ScanLimit::Records));
}

#[tokio::test(flavor = "current_thread")]
async fn bounded_scan_prefix_end_does_not_resolve_unrelated_values() {
    let tree = fixture(vec![
        (b"p:a".to_vec(), LeafValue::Inline(Bytes::from_static(b"x"))),
        (b"q:a".to_vec(), unreadable(8192)),
    ])
    .await;
    assert!(tree.get(b"q:a").await.is_err());
    let batch = tree
        .collect_prefix_batch_bounded(b"p:", b"p:", 1, 4)
        .await
        .unwrap();
    assert_eq!(batch.entries.len(), 1);
    assert_eq!(batch.entries[0].0.as_ref(), b"p:a");
    assert_eq!(batch.entries[0].1.as_ref(), b"x");
    assert_eq!(batch.limit, None);
}

#[tokio::test(flavor = "current_thread")]
async fn bounded_scan_rejects_key_plus_value_length_overflow() {
    let tree = fixture(vec![(b"p:a".to_vec(), unreadable(u64::MAX))]).await;
    let batch = tree
        .collect_prefix_batch_bounded(b"p:", b"p:", 2, usize::MAX)
        .await
        .unwrap();
    assert!(batch.entries.is_empty());
    assert_eq!(batch.limit, Some(ScanLimit::Bytes));
}

#[tokio::test(flavor = "current_thread")]
async fn bounded_scan_rejects_cumulative_byte_overflow() {
    let total_len = u64::try_from(usize::MAX - b"p:b".len()).unwrap();
    let tree = fixture(vec![
        (b"p:a".to_vec(), LeafValue::Inline(Bytes::from_static(b"x"))),
        (b"p:b".to_vec(), unreadable(total_len)),
    ])
    .await;
    let batch = tree
        .collect_prefix_batch_bounded(b"p:", b"p:", 2, usize::MAX)
        .await
        .unwrap();
    assert_eq!(batch.entries.len(), 1);
    assert_eq!(batch.entries[0].0.as_ref(), b"p:a");
    assert_eq!(batch.limit, Some(ScanLimit::Bytes));
}
