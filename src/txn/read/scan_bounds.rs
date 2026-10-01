// SPDX-License-Identifier: Apache-2.0
//! Results and limits for bounded prefix scans.

use bytes::Bytes;

/// The budget that excludes the next matching record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanLimit {
    /// The batch contains the maximum number of records.
    Records,
    /// The next key and complete value exceed the remaining byte budget.
    Bytes,
}

/// Owned records from one snapshot, bounded by count and key-plus-value bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanBatch {
    /// Matching entries in ascending key order.
    pub entries: Vec<(Bytes, Bytes)>,
    /// `Some` means a matching record remains unread because a budget excludes it.
    /// `None` means the matching range ends, including at an exact budget boundary.
    pub limit: Option<ScanLimit>,
}
