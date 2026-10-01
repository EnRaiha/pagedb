//! Snapshot export and incremental restore: full and delta transfer across DB instances.

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod apply;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod export;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

mod types;

pub use types::{ApplyStats, SnapshotStats};

#[cfg(not(target_arch = "wasm32"))]
mod copy_extent;
