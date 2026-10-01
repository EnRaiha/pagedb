// SPDX-License-Identifier: Apache-2.0
//! Prefix scans with record and payload-byte budgets.

use bytes::Bytes;

use crate::btree::leaf::LeafValue;
use crate::errors::PagedbError;
use crate::vfs::Vfs;
use crate::{Result, ScanBatch, ScanLimit};

use super::core::{BTree, SeenPageIds};

impl<V: Vfs> BTree<V> {
    pub(crate) async fn collect_prefix_batch_bounded(
        &self,
        prefix: &[u8],
        start: &[u8],
        max_records: usize,
        max_bytes: usize,
    ) -> Result<ScanBatch> {
        let mut batch = ScanBatch {
            entries: Vec::new(),
            limit: None,
        };
        if self.root_page_id == 0 {
            return Ok(batch);
        }
        let start = start.max(prefix);
        let mut path = self.path_to_leaf_for_key(start).await?;
        let mut seen_leaves = SeenPageIds::new("btree_scan");
        let mut used_bytes = 0_usize;
        loop {
            let leaf_id = path
                .last()
                .copied()
                .ok_or_else(|| PagedbError::node_body_malformed("scan.path"))?;
            seen_leaves.insert(leaf_id)?;
            let leaf = self.read_leaf(leaf_id).await?;
            for (key, value) in &leaf.records {
                if key.as_slice() < start {
                    continue;
                }
                if !key.starts_with(prefix) {
                    return Ok(batch);
                }
                if batch.entries.len() == max_records {
                    batch.limit = Some(ScanLimit::Records);
                    return Ok(batch);
                }
                let value_len = match value {
                    LeafValue::Inline(bytes) => Some(bytes.len()),
                    LeafValue::Overflow { total_len, .. } => usize::try_from(*total_len).ok(),
                };
                let next_bytes = value_len
                    .and_then(|len| key.len().checked_add(len))
                    .and_then(|len| used_bytes.checked_add(len));
                let Some(next_bytes) = next_bytes.filter(|&len| len <= max_bytes) else {
                    batch.limit = Some(ScanLimit::Bytes);
                    return Ok(batch);
                };
                let resolved = self.resolve_leaf_value(value).await?;
                batch.entries.push((Bytes::copy_from_slice(key), resolved));
                used_bytes = next_bytes;
            }
            match self.next_leaf_after(&path).await? {
                Some(next_path) => path = next_path,
                None => return Ok(batch),
            }
        }
    }
}
