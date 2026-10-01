// SPDX-License-Identifier: Apache-2.0

//! Snapshot destination claims and cleanup.

use tokio::fs;

/// What a failed export or restore is allowed to delete.
///
/// Cleanup must undo what the operation created and nothing else. A caller that
/// pre-created an empty output directory — `mkdir -p /backups/snap-1`, then
/// snapshot into it — still owns that directory after a failure, so the
/// distinction is recorded up front rather than inferred afterwards from an
/// `io::ErrorKind` that any number of unrelated operations also produce.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum DestinationOwnership {
    /// The directory did not exist; a failure removes it entirely.
    Created,
    /// The directory already existed and was empty; a failure removes only the
    /// contents written into it.
    PreExisting,
}

/// Require an empty destination and record whether it already existed.
///
/// A non-empty destination is refused rather than merged into: a snapshot
/// artifact describes one exact state, and mixing it with unrelated pre-existing
/// pages or segment files produces a directory that authenticates as neither.
pub(super) async fn claim_empty_destination(
    dst_path: &std::path::Path,
) -> crate::Result<DestinationOwnership> {
    match fs::read_dir(dst_path).await {
        Ok(mut entries) => {
            if entries
                .next_entry()
                .await
                .map_err(crate::errors::PagedbError::Io)?
                .is_some()
            {
                return Err(crate::errors::PagedbError::Io(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("snapshot destination is not empty: {}", dst_path.display()),
                )));
            }
            Ok(DestinationOwnership::PreExisting)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(DestinationOwnership::Created)
        }
        Err(error) => Err(crate::errors::PagedbError::Io(error)),
    }
}

/// Roll back a failed export or restore, leaving the destination reusable.
///
/// Errors are discarded deliberately: the caller is already returning the
/// failure that matters, and a cleanup error must not replace it with something
/// less diagnostic.
pub(super) async fn cleanup_failed_snapshot(
    dst_path: &std::path::Path,
    ownership: DestinationOwnership,
) {
    match ownership {
        DestinationOwnership::Created => {
            let _ = fs::remove_dir_all(dst_path).await;
        }
        DestinationOwnership::PreExisting => {
            let Ok(mut entries) = fs::read_dir(dst_path).await else {
                return;
            };
            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                if entry.file_type().await.is_ok_and(|kind| kind.is_dir()) {
                    let _ = fs::remove_dir_all(&path).await;
                } else {
                    let _ = fs::remove_file(&path).await;
                }
            }
        }
    }
}
