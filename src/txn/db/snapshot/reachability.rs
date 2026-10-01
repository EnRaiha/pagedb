// SPDX-License-Identifier: Apache-2.0

//! Published and reader-visible page sets.

use super::super::core::{Db, ReaderSnapshot};
use crate::pager::Pager;
use crate::vfs::Vfs;
use std::collections::BTreeSet;
use std::sync::Arc;

/// Every page the data and catalog trees rooted at the two ids reach.
///
/// Takes the `Pager` explicitly rather than reading it off the `Db`: an apply
/// walks the *target*'s roots, and those live in a staged image that only a
/// dedicated read-only view is pointed at.
pub(super) async fn collect_tree_page_ids<V: Vfs + Clone>(
    pager: &Arc<Pager<V>>,
    realm_id: crate::RealmId,
    page_size: usize,
    active_root_page_id: u64,
    catalog_root_page_id: u64,
    next_page_id: u64,
) -> crate::Result<BTreeSet<u64>> {
    let mut page_ids = BTreeSet::new();
    for root_page_id in [active_root_page_id, catalog_root_page_id] {
        if root_page_id == 0 {
            continue;
        }
        let tree = crate::btree::BTree::open(
            pager.clone(),
            realm_id,
            root_page_id,
            next_page_id,
            page_size,
        );
        tree.collect_all_page_ids(&mut page_ids).await?;
    }
    Ok(page_ids)
}

/// Every page a published state occupies: the reader-visible data and catalog
/// trees, plus the free-list chain and commit-history tree hanging off the same
/// header.
///
/// This is deliberately wider than the set that defines a delta. A delta is
/// target-reachable minus base-*reader-visible*, because those are the only
/// pages both sides of the protocol can name; the free-list chain and
/// commit-history tree are the follower's own and invisible to the producer.
/// Subtracting the wider set when deciding which pages a delta should contain
/// would turn every page the producer legitimately allocated over a
/// follower-local page into an unexplained set mismatch.
///
/// A verbatim export needs the wider set instead, as a length floor: the copied
/// `main.db` must physically contain every page its header names, not only the
/// ones a reader can walk to.
pub(super) async fn collect_published_page_ids<V: Vfs + Clone>(
    db: &Db<V>,
    snapshot: ReaderSnapshot,
) -> crate::Result<BTreeSet<u64>> {
    let mut published = collect_tree_page_ids(
        &db.pager,
        db.realm_id,
        db.page_size,
        snapshot.root_page_id,
        snapshot.catalog_root_page_id,
        snapshot.next_page_id,
    )
    .await?;
    if snapshot.commit_history_root_page_id != 0 {
        let history = crate::btree::BTree::open(
            db.pager.clone(),
            db.realm_id,
            snapshot.commit_history_root_page_id,
            snapshot.next_page_id,
            db.page_size,
        );
        history.collect_all_page_ids(&mut published).await?;
    }
    if snapshot.free_list_root_page_id != 0 {
        let (_entries, chain_pages) = crate::pager::freelist::read_chain(
            &db.pager,
            db.realm_id,
            snapshot.free_list_root_page_id,
        )
        .await?;
        published.extend(chain_pages);
    }
    Ok(published)
}
