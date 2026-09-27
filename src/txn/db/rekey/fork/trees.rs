//! Streaming the live data and catalog trees into the fork.

use bytes::Bytes;

use crate::Result;
use crate::btree::BTree;
use crate::catalog::codec::{Catalog, CatalogRowKind};
use crate::errors::PagedbError;
use crate::pager::anchor::HeaderCursor;
use crate::pager::header::commit_header;
use crate::txn::db::{Db, WriterState};
use crate::vfs::Vfs;

use super::segments::copy_segment;

/// Records read from a source tree per batch. One batch is all the fork holds
/// of the source. The destination holds one leaf plus one internal node per
/// level. Neither scales with the size of the store.
const FORK_RECORD_BATCH: usize = 256;

/// Copy the state `state` describes from `source` into `fork`.
/// Then write the fork's header naming it.
///
/// Reads authenticate under the source key through the source pager. Writes
/// seal under the fork key through the fork pager. The two never share a
/// cache. No page of one is ever served as a page of the other.
pub(super) async fn copy_live_state<V: Vfs + Clone>(
    source: &Db<V>,
    state: &WriterState,
    fork: &Db<V>,
) -> Result<()> {
    let first_page = fork.writer.lock().await.next_page_id;

    let mut main = BTree::open(
        fork.pager.clone(),
        fork.realm_id,
        0,
        first_page,
        fork.page_size,
    );
    copy_main_tree(source, state, &mut main).await?;

    let mut catalog = BTree::open(
        fork.pager.clone(),
        fork.realm_id,
        0,
        main.next_page_id(),
        fork.page_size,
    );
    copy_catalog(source, state, fork, &mut catalog).await?;

    fork.pager.flush_main(fork.realm_id).await?;
    publish_fork_header(state, fork, &main, &catalog).await
}

/// Copy every record of the source data tree, unchanged.
async fn copy_main_tree<V: Vfs + Clone>(
    source: &Db<V>,
    state: &WriterState,
    dest: &mut BTree<V>,
) -> Result<()> {
    let src = BTree::open(
        source.pager.clone(),
        source.realm_id,
        state.root_page_id,
        state.next_page_id,
        source.page_size,
    );
    let mut loader = dest.bulk_loader()?;
    let mut cursor: Vec<u8> = Vec::new();
    loop {
        let batch = src.collect_batch_from(&cursor, FORK_RECORD_BATCH).await?;
        let Some((last_key, _)) = batch.last() else {
            break;
        };
        cursor.clear();
        cursor.extend_from_slice(last_key);
        // The exact successor of `last_key` in the key ordering.
        cursor.push(0);
        let exhausted = batch.len() < FORK_RECORD_BATCH;
        let rows: Vec<(Vec<u8>, Bytes)> = batch
            .into_iter()
            .map(|(key, value)| (key.to_vec(), value))
            .collect();
        loader.push_batch(rows).await?;
        if exhausted {
            break;
        }
    }
    loader.finish().await
}

/// Copy the catalog, re-encrypting each linked segment under a fresh id.
///
/// Quota and counter rows carry over unchanged. A segment row is rewritten
/// to name its re-encrypted replacement. The retired compaction watermark is
/// dropped, as compaction drops it. A rekey intent or rekey progress row
/// names the source identity and an unfinished rekey. The fork refuses it.
async fn copy_catalog<V: Vfs + Clone>(
    source: &Db<V>,
    state: &WriterState,
    fork: &Db<V>,
    dest: &mut BTree<V>,
) -> Result<()> {
    let src = BTree::open(
        source.pager.clone(),
        source.realm_id,
        state.catalog_root_page_id,
        state.next_page_id,
        source.page_size,
    );
    let mut loader = dest.bulk_loader()?;
    let mut cursor: Vec<u8> = Vec::new();
    loop {
        let batch = src.collect_batch_from(&cursor, FORK_RECORD_BATCH).await?;
        let Some((last_key, _)) = batch.last() else {
            break;
        };
        cursor.clear();
        cursor.extend_from_slice(last_key);
        cursor.push(0);
        let exhausted = batch.len() < FORK_RECORD_BATCH;

        let mut rows: Vec<(Vec<u8>, Bytes)> = Vec::with_capacity(batch.len());
        for (key, value) in batch {
            let Some(&kind) = key.first() else {
                return Err(PagedbError::catalog_row_invalid("catalog.key"));
            };
            let value =
                if kind == CatalogRowKind::Quota as u8 || kind == CatalogRowKind::Counter as u8 {
                    value
                } else if kind == CatalogRowKind::Segment as u8 {
                    let meta = Catalog::decode_segment_meta(&value)?;
                    let copied = copy_segment(source, fork, &meta).await?;
                    Bytes::copy_from_slice(&Catalog::encode_segment_meta(&copied))
                } else if kind == CatalogRowKind::CompactionState as u8 {
                    continue;
                } else if kind == CatalogRowKind::RekeyState as u8
                    || kind == CatalogRowKind::RekeySegmentProgress as u8
                {
                    return Err(PagedbError::rekey_state_invalid(
                        "rekey_into_writer.rekey_pending",
                    ));
                } else {
                    return Err(PagedbError::catalog_row_invalid("catalog.row_kind"));
                };
            rows.push((key.to_vec(), value));
        }
        if !rows.is_empty() {
            loader.push_batch(rows).await?;
        }
        if exhausted {
            break;
        }
    }
    loader.finish().await
}

/// Write the header that makes the fork a complete Standalone store.
///
/// The commit id continues from the source. Ids an embedder already holds
/// stay ordered. Commit history starts empty: its rows name source pages the
/// fork does not carry.
async fn publish_fork_header<V: Vfs + Clone>(
    state: &WriterState,
    fork: &Db<V>,
    main: &BTree<V>,
    catalog: &BTree<V>,
) -> Result<()> {
    let commit_id = state
        .latest_commit_id
        .checked_add(1)
        .ok_or_else(|| PagedbError::arithmetic_overflow("fork commit id"))?;
    let mut fork_state = fork.writer.lock().await;
    fork_state.root_page_id = main.root_page_id();
    fork_state.catalog_root_page_id = catalog.root_page_id();
    fork_state.catalog_root_txn_id = commit_id;
    fork_state.next_page_id = catalog.next_page_id();
    fork_state.latest_commit_id = commit_id;
    fork_state.commit_retain_policy_tag = state.commit_retain_policy_tag;
    fork_state.commit_retain_policy_value = state.commit_retain_policy_value;

    let cursor = fork.pager.header_cursor()?;
    let seq = cursor.next_seq()?;
    let counter_anchor = fork.pager.pending_anchor();
    let fields = fork.state_header_fields(&fork_state, seq, counter_anchor)?;
    let hk = fork.hk.read().clone();
    let slot = commit_header(
        &*fork.vfs,
        &fork.main_db_path,
        &hk,
        &fields,
        cursor.slot,
        fork.page_size,
    )
    .await?;
    fork.pager.note_header_written(HeaderCursor { slot, seq });
    Ok(())
}
