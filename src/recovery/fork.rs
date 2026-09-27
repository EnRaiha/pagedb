//! Segments a `rekey_into_writer` fork wrote before its `main.db` was live.
//!
//! The fork writes its segments in [`FORK_DIR`], which the open-time orphan
//! scan skips. A fork that never publishes leaves the restored directory
//! openable. Once the fork's `main.db` is live, its catalog names those
//! segments. Moving them into staging hands them to catalog repair, which
//! publishes each one the catalog names and sweeps the rest.

use crate::Result;
use crate::errors::PagedbError;
use crate::segment::writer::{FORK_DIR, STAGING_DIR};
use crate::vfs::Vfs;

/// Move every fork segment into staging. Returns the number moved.
///
/// Callers must hold write authority over a store whose `main.db` is the
/// fork's. Callers must run catalog repair afterwards.
pub(crate) async fn adopt_forked_segments<V: Vfs>(vfs: &V) -> Result<u64> {
    let names = forked_segment_names(vfs).await?;
    if names.is_empty() {
        return Ok(0);
    }
    vfs.mkdir_all(STAGING_DIR).await?;
    let mut count: u64 = 0;
    for name in names {
        vfs.rename(
            &format!("{FORK_DIR}/{name}"),
            &format!("{STAGING_DIR}/{name}"),
        )
        .await?;
        count += 1;
    }
    vfs.sync_dir(STAGING_DIR).await?;
    vfs.sync_dir(FORK_DIR).await?;
    Ok(count)
}

/// Remove every fork segment. Returns the number removed.
///
/// A fork calls this before it starts, to drop what an earlier attempt
/// wrote without publishing.
pub(crate) async fn discard_forked_segments<V: Vfs>(vfs: &V) -> Result<u64> {
    let names = forked_segment_names(vfs).await?;
    if names.is_empty() {
        return Ok(0);
    }
    let mut count: u64 = 0;
    for name in names {
        vfs.remove(&format!("{FORK_DIR}/{name}")).await?;
        count += 1;
    }
    vfs.sync_dir(FORK_DIR).await?;
    Ok(count)
}

/// Names in the fork directory that this crate writes: segment identities.
/// Anything else is left alone.
async fn forked_segment_names<V: Vfs>(vfs: &V) -> Result<Vec<String>> {
    let entries = match vfs.list_dir(FORK_DIR).await {
        Ok(entries) => entries,
        Err(PagedbError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Vec::new());
        }
        Err(error) => return Err(error),
    };
    Ok(entries
        .into_iter()
        .filter(|name| crate::hex::parse_hex::<16>(name).is_some())
        .collect())
}
