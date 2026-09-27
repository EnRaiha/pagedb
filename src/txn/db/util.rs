//! Free helper functions shared across DB submodules: page-size encoding and
//! VFS root extraction.

use crate::Result;
use crate::errors::PagedbError;
#[cfg(not(target_arch = "wasm32"))]
use crate::vfs::Vfs;

pub(super) fn page_size_log2(page_size: usize) -> Result<u8> {
    match page_size {
        4096 => Ok(12),
        8192 => Ok(13),
        16384 => Ok(14),
        32768 => Ok(15),
        65536 => Ok(16),
        _ => Err(PagedbError::Unsupported),
    }
}

/// Extract the filesystem root path from a `Vfs` instance.
///
/// Returns `Unsupported` for in-memory or non-filesystem VFS backends.
#[cfg(not(target_arch = "wasm32"))]
pub(super) fn get_vfs_root<V: Vfs + Clone>(vfs: &V) -> Result<std::path::PathBuf> {
    vfs.root_path()
        .map(std::path::Path::to_path_buf)
        .ok_or(PagedbError::Unsupported)
}
