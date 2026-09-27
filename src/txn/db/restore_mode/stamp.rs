//! Stamping a copied `main.db` as a restored directory.

use crate::Result;
use crate::crypto::SecretKey;
use crate::crypto::keys::DerivedKey;
use crate::errors::PagedbError;
use crate::pager::header::{
    ActiveSlot, authenticate_slot, authenticate_slot_with_kek, commit_header, read_header_slot,
};
use crate::pager::structural_header::MainDbHeaderFields;
use crate::vfs::Vfs;
use crate::vfs::types::OpenMode;

use super::values::READ_ONLY;

/// Key material that authenticates a `main.db` header.
pub(in crate::txn::db) enum HeaderKey<'a> {
    /// The embedder's KEK. Each slot's HK derives from that slot's own salt
    /// and epoch.
    Kek(&'a SecretKey),
    /// The HK of the handle that wrote the file.
    Hk(&'a DerivedKey),
}

/// Mark the `main.db` at `path` as a copy that admits no Standalone writer.
///
/// Both A/B slots get `restore_mode = READ_ONLY` and a fresh MAC, including
/// the slot an open selects. With both slots stamped, neither the newer slot
/// nor a fallback to the older one admits a writer. Only the
/// HMAC-authenticated header changes, never an AEAD page, so the stamp
/// consumes no nonce.
pub(in crate::txn::db) async fn stamp_read_only<V: Vfs>(
    vfs: &V,
    path: &str,
    page_size: usize,
    key: HeaderKey<'_>,
) -> Result<()> {
    let slot_b_offset = u64::try_from(page_size)
        .map_err(|_| PagedbError::Io(std::io::Error::other("page_size > u64")))?;
    let mut buf_a = vec![0u8; page_size];
    let mut buf_b = vec![0u8; page_size];
    {
        let mut file = vfs.open(path, OpenMode::Read).await?;
        read_header_slot(&mut file, 0, &mut buf_a).await?;
        read_header_slot(&mut file, slot_b_offset, &mut buf_b).await?;
    }

    let mut selected: Option<(MainDbHeaderFields, DerivedKey)> = None;
    for buf in [&buf_a, &buf_b] {
        let Some((fields, hk)) = decode_slot(buf, page_size, &key)? else {
            continue;
        };
        if selected
            .as_ref()
            .is_none_or(|(current, _)| fields.seq > current.seq)
        {
            selected = Some((fields, hk));
        }
    }
    let Some((mut fields, hk)) = selected else {
        return Err(super::super::open::header_probe::unverifiable_header_cause(
            &buf_a, &buf_b, page_size,
        ));
    };

    fields.restore_mode = READ_ONLY;
    // `commit_header` writes the slot after `previous`: B first names A, then
    // A names B.
    commit_header(vfs, path, &hk, &fields, ActiveSlot::B, page_size).await?;
    commit_header(vfs, path, &hk, &fields, ActiveSlot::A, page_size).await?;
    Ok(())
}

/// Authenticate one slot under `key`. Returns `None` when `key` does not
/// verify it.
fn decode_slot(
    buf: &[u8],
    page_size: usize,
    key: &HeaderKey<'_>,
) -> Result<Option<(MainDbHeaderFields, DerivedKey)>> {
    match key {
        HeaderKey::Kek(kek) => authenticate_slot_with_kek(buf, kek, page_size),
        HeaderKey::Hk(hk) => {
            Ok(authenticate_slot(buf, hk, page_size)?.map(|fields| (fields, (*hk).clone())))
        }
    }
}
