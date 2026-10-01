// SPDX-License-Identifier: Apache-2.0

//! Exact-extent file copying for full snapshots.

use crate::Result;
use crate::errors::PagedbError;
use std::path::Path;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Copy exactly the captured file extent and reject a premature EOF.
pub(super) async fn copy_file_extent(src_path: &Path, dst_path: &Path, extent: u64) -> Result<u64> {
    let mut src = fs::File::open(src_path).await.map_err(PagedbError::Io)?;
    let mut dst = fs::File::create(dst_path).await.map_err(PagedbError::Io)?;
    let mut buffer = vec![0u8; 64 * 1024];
    let mut remaining = extent;
    while remaining > 0 {
        let limit = usize::try_from(remaining)
            .map_or(buffer.len(), |remaining| remaining.min(buffer.len()));
        let read = src
            .read(&mut buffer[..limit])
            .await
            .map_err(PagedbError::Io)?;
        if read == 0 {
            return Err(PagedbError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "snapshot source {} ended after {} of {extent} bytes; retry export from an intact source",
                    src_path.display(),
                    extent - remaining,
                ),
            )));
        }
        dst.write_all(&buffer[..read])
            .await
            .map_err(PagedbError::Io)?;
        remaining -= read as u64;
    }
    dst.flush().await.map_err(PagedbError::Io)?;
    dst.sync_all().await.map_err(PagedbError::Io)?;
    Ok(extent)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn captured_extent_excludes_trailing_growth_and_rejects_early_eof() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let destination = dir.path().join("destination");
        fs::write(&source, b"abcdef").await.unwrap();
        assert_eq!(copy_file_extent(&source, &destination, 3).await.unwrap(), 3);
        assert_eq!(fs::read(&destination).await.unwrap(), b"abc");
        let error = copy_file_extent(&source, &destination, 7)
            .await
            .unwrap_err();
        assert!(
            matches!(error, PagedbError::Io(error) if error.kind() == std::io::ErrorKind::UnexpectedEof)
        );
    }
}
