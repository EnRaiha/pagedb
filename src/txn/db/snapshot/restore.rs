// SPDX-License-Identifier: Apache-2.0

//! Read-only restoration and artifact authentication.

use super::super::core::Db;
use super::ownership::{claim_empty_destination, cleanup_failed_snapshot};
use super::reachability::collect_tree_page_ids;
use crate::snapshot::export::{SnapshotManifest, open_manifest};
use crate::vfs::Vfs;
use crate::vfs::tokio_backend::TokioVfs;
use std::collections::BTreeSet;
use tokio::fs;

async fn validate_restored_snapshot(
    manifest: &SnapshotManifest,
    restored: &Db<TokioVfs>,
) -> crate::Result<()> {
    let snapshot = *restored.snapshot.read();
    if snapshot.commit_id != manifest.target_commit {
        return Err(crate::errors::PagedbError::snapshot_incompatible(
            "target_commit",
        ));
    }
    if snapshot.next_page_id != manifest.next_page_id_at_target {
        return Err(crate::errors::PagedbError::snapshot_incompatible(
            "next_page_id_at_target",
        ));
    }
    if snapshot.root_page_id != manifest.target_active_root_page_id {
        return Err(crate::errors::PagedbError::snapshot_incompatible(
            "target_active_root_page_id",
        ));
    }
    if snapshot.catalog_root_page_id != manifest.target_catalog_root_page_id {
        return Err(crate::errors::PagedbError::snapshot_incompatible(
            "target_catalog_root_page_id",
        ));
    }

    collect_tree_page_ids(
        &restored.pager,
        restored.realm_id,
        restored.page_size,
        snapshot.root_page_id,
        snapshot.catalog_root_page_id,
        snapshot.next_page_id,
    )
    .await?;

    let expected_segments = restored.list_segments(restored.realm_id, "").await?;
    let expected_ids: BTreeSet<[u8; 16]> = expected_segments
        .iter()
        .map(|meta| meta.segment_id)
        .collect();
    let mut actual_ids = BTreeSet::new();
    let mut entries = fs::read_dir(restored.vfs.root_path().join("seg"))
        .await
        .map_err(crate::errors::PagedbError::Io)?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(crate::errors::PagedbError::Io)?
    {
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| crate::errors::PagedbError::snapshot_artifact_invalid("segment.name"))?;
        let segment_id = crate::hex::parse_hex::<16>(&name)
            .ok_or_else(|| crate::errors::PagedbError::snapshot_artifact_invalid("segment.name"))?;
        actual_ids.insert(segment_id);
    }
    if actual_ids != expected_ids
        || u32::try_from(actual_ids.len()).ok() != Some(manifest.segments_count)
    {
        return Err(crate::errors::PagedbError::snapshot_artifact_invalid(
            "segments",
        ));
    }

    let mmap_limit = u64::try_from(restored.options.mmap_view_scratch_bytes).unwrap_or(u64::MAX);
    for meta in expected_segments {
        let reader = crate::segment::reader::SegmentReader::open_internal(
            restored.pager.clone(),
            meta.clone(),
            restored.mmap_bytes_in_use.clone(),
            mmap_limit,
        )
        .await?;
        for page_id in 1..meta.page_count.saturating_sub(1) {
            let _ = reader.read_page(page_id).await?;
        }
    }
    Ok(())
}

impl<V: Vfs + Clone> Db<V> {
    /// Associated fn. Copy snapshot from `src_path` into `dst_path` and return
    /// a `Db` in `DbMode::ReadOnly`.
    ///
    /// Verifies the manifest HK-MAC using `kek` before copying; returns
    /// `Corruption` on failure.
    pub async fn restore_from(
        src_path: &std::path::Path,
        dst_path: &std::path::Path,
        options: crate::options::OpenOptions,
        kek: impl Into<crate::crypto::SecretKey>,
    ) -> crate::Result<crate::txn::db::Db<crate::vfs::tokio_backend::TokioVfs>> {
        let kek = kek.into();
        let _span = tracing::debug_span!("snapshot.apply");

        // Verify and parse manifest.
        let manifest_src = src_path.join("manifest");
        let manifest = open_manifest(&manifest_src, kek.as_bytes()).await?;
        if manifest.kind != 0 {
            return Err(crate::errors::PagedbError::snapshot_incompatible("kind"));
        }
        let ownership = claim_empty_destination(dst_path).await?;

        let restore_result = async {
            // Create destination directory.
            fs::create_dir_all(dst_path)
                .await
                .map_err(crate::errors::PagedbError::Io)?;
            let seg_dst = dst_path.join("seg");
            fs::create_dir_all(&seg_dst)
                .await
                .map_err(crate::errors::PagedbError::Io)?;

            // Copy the already length- and MAC-validated manifest.
            fs::copy(&manifest_src, dst_path.join("manifest"))
                .await
                .map_err(crate::errors::PagedbError::Io)?;

            // Copy main.db.
            fs::copy(src_path.join("main.db"), dst_path.join("main.db"))
                .await
                .map_err(crate::errors::PagedbError::Io)?;

            // Copy segment files without collapsing directory or entry errors.
            // Names are screened here, before anything opens the destination: a
            // file that is not a segment identity is an undeclared sidecar, and
            // letting open-time recovery meet it first reports artifact
            // corruption as whatever error that recovery path happens to raise.
            let seg_src = src_path.join("seg");
            let mut copied_segments: u32 = 0;
            match fs::read_dir(&seg_src).await {
                Ok(mut entries) => {
                    while let Some(entry) = entries
                        .next_entry()
                        .await
                        .map_err(crate::errors::PagedbError::Io)?
                    {
                        let name = entry.file_name().into_string().map_err(|_| {
                            crate::errors::PagedbError::snapshot_artifact_invalid("segment.name")
                        })?;
                        if crate::hex::parse_hex::<16>(&name).is_none() {
                            return Err(crate::errors::PagedbError::snapshot_artifact_invalid(
                                "segment.name",
                            ));
                        }
                        fs::copy(entry.path(), seg_dst.join(&name))
                            .await
                            .map_err(crate::errors::PagedbError::Io)?;
                        copied_segments = copied_segments.checked_add(1).ok_or_else(|| {
                            crate::errors::PagedbError::snapshot_artifact_invalid("segments_count")
                        })?;
                    }
                }
                Err(error)
                    if error.kind() == std::io::ErrorKind::NotFound
                        && manifest.segments_count == 0 => {}
                Err(error) => return Err(crate::errors::PagedbError::Io(error)),
            }
            if copied_segments != manifest.segments_count {
                return Err(crate::errors::PagedbError::snapshot_artifact_invalid(
                    "segments",
                ));
            }

            // Open and authenticate every manifest-referenced root and segment
            // before returning a usable handle.
            let page_size = usize::try_from(manifest.page_size)
                .map_err(|_| crate::errors::PagedbError::snapshot_incompatible("page_size"))?;
            let realm_id = crate::RealmId(manifest.realm_id);
            let dst_vfs = TokioVfs::new(dst_path);
            // The copy shares the source's nonce space, so it must never open
            // as a Standalone writer. The stamp precedes the first open, so no
            // handle ever sees the copy without it.
            crate::txn::db::restore_mode::stamp_read_only(
                &dst_vfs,
                "/main.db",
                page_size,
                crate::txn::db::restore_mode::HeaderKey::Kek(&kek),
            )
            .await?;
            let restored =
                Db::<TokioVfs>::open_read_only(dst_vfs, kek, page_size, realm_id, options).await?;
            validate_restored_snapshot(&manifest, &restored).await?;
            Ok(restored)
        }
        .await;
        if restore_result.is_err() {
            cleanup_failed_snapshot(dst_path, ownership).await;
        }
        restore_result
    }
}
