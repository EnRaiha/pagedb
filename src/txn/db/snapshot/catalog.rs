// SPDX-License-Identifier: Apache-2.0

//! Catalog segment lookup at explicit roots.

use super::super::core::Db;
use crate::pager::Pager;
use crate::vfs::Vfs;
use std::collections::BTreeSet;
use std::sync::Arc;

impl<V: Vfs + Clone> Db<V> {
    /// Segment rows of the catalog tree rooted at `catalog_root_page_id`.
    ///
    /// Reads the catalog at an explicit root rather than through a `ReadTxn`, so
    /// a caller comparing a base and a target catalog can hold both at once —
    /// and so the base side can be drawn from the same published snapshot its
    /// page sets came from.
    pub(super) async fn catalog_segment_metas(
        &self,
        pager: &Arc<Pager<V>>,
        catalog_root_page_id: u64,
        next_page_id: u64,
    ) -> crate::Result<Vec<crate::catalog::codec::SegmentMeta>> {
        if catalog_root_page_id == 0 {
            return Ok(Vec::new());
        }
        let tree = crate::btree::BTree::open(
            pager.clone(),
            self.realm_id,
            catalog_root_page_id,
            next_page_id,
            self.page_size,
        );
        let rows = tree
            .scan_prefix(&[crate::catalog::codec::CatalogRowKind::Segment as u8])
            .await?;
        let mut entries = Vec::with_capacity(rows.len());
        for (_, value) in rows {
            entries.push(crate::catalog::codec::Catalog::decode_segment_meta(&value)?);
        }
        Ok(entries)
    }

    pub(super) async fn catalog_segment_ids(
        &self,
        pager: &Arc<Pager<V>>,
        catalog_root_page_id: u64,
        next_page_id: u64,
    ) -> crate::Result<BTreeSet<[u8; 16]>> {
        Ok(self
            .catalog_segment_metas(pager, catalog_root_page_id, next_page_id)
            .await?
            .into_iter()
            .map(|meta| meta.segment_id)
            .collect())
    }
}
