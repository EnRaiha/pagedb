//! Snapshot read transactions.

mod scan_bounds;
mod txn;

pub use scan_bounds::{ScanBatch, ScanLimit};
pub use txn::ReadTxn;
