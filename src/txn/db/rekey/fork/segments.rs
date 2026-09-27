//! Re-encrypting one linked segment into the fork.

use crate::Result;
use crate::catalog::codec::SegmentMeta;
use crate::errors::PagedbError;
use crate::segment::reader::SegmentReader;
use crate::segment::writer::SegmentWriter;
use crate::txn::db::Db;
use crate::vfs::Vfs;

/// Copy the segment `meta` names from `source` into a fresh segment of `fork`.
/// Returns the replacement's metadata.
///
/// Page ids, the extent-index layout, the manifest, kind, and evictability
/// all carry over. An engine's own index into the segment stays valid.
/// The replacement stays in the fork directory. The first Standalone open of
/// the fork's `main.db` adopts it into staging. Catalog repair then publishes
/// it like any staged segment a commit names.
pub(super) async fn copy_segment<V: Vfs + Clone>(
    source: &Db<V>,
    fork: &Db<V>,
    meta: &SegmentMeta,
) -> Result<SegmentMeta> {
    let limit = u64::try_from(source.options.mmap_view_scratch_bytes).unwrap_or(u64::MAX);
    let reader = SegmentReader::open_internal(
        source.pager.clone(),
        meta.clone(),
        source.mmap_bytes_in_use.clone(),
        limit,
    )
    .await?;
    let footer = reader.authenticated_footer();
    let segment_id = crate::crypto::random::segment_id()?;
    let mut writer = SegmentWriter::create_fork_internal(
        fork.pager.clone(),
        meta,
        segment_id,
        fork.file_id,
        footer.fields.index_start_page,
        footer.fields.index_page_count,
    )
    .await?;
    writer.set_manifest(&footer.manifest)?;
    // Pages stream one at a time. The copy never holds a whole segment.
    for page_id in 1..meta.page_count.saturating_sub(1) {
        let (kind, body) = reader.read_authenticated_page(page_id).await?;
        let copied_page_id = writer.append_rekey_page(kind, &body).await?;
        if copied_page_id != page_id {
            return Err(PagedbError::rekey_state_invalid(
                "rekey_into_writer.page_id_ordering",
            ));
        }
    }
    let copied = writer.seal().await?;
    drop(reader);
    if copied.page_count != meta.page_count
        || copied.format_version != meta.format_version
        || copied.segment_kind != meta.segment_kind
        || copied.evictable != meta.evictable
    {
        return Err(PagedbError::rekey_state_invalid(
            "rekey_into_writer.segment_metadata",
        ));
    }

    Ok(copied)
}
