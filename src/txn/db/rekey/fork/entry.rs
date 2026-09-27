//! The public fork operation: sentinel trade, replacement build, publication
//! by rename, and reopen as a Standalone writer.

use crate::Result;
use crate::crypto::SecretKey;
use crate::errors::PagedbError;
use crate::txn::db::{Db, DbModeCapabilities};
use crate::txn::mode::DbMode;
use crate::vfs::{Vfs, remove_if_present};

use super::trees::copy_live_state;

impl<V: Vfs + Clone> Db<V> {
    /// Fork this directory into an independent Standalone writer.
    ///
    /// A restored directory shares its source's key and nonce space. The fork
    /// re-encrypts every page and segment under a fresh `file_id`, a fresh
    /// `kek_salt`, and `new_kek`, then renames the result over `main.db`.
    /// Nothing is sealed under the source key. `new_kek` can equal the old KEK.
    ///
    /// - Requires a `ReadOnly` or `Follower` handle and no other frozen reader.
    /// - Keeps keys, values, segments, counters, and quotas. Commit history
    ///   starts empty.
    /// - Accepts no further incremental snapshot from the source.
    /// - Holds two buffer pools at peak: the source's and the fork's.
    /// - The rename is the commit point. An earlier error leaves the directory
    ///   restored and readable. The handle is consumed either way.
    pub async fn rekey_into_writer(mut self, new_kek: impl Into<SecretKey>) -> Result<Self> {
        let new_kek = new_kek.into();
        self.ensure_usable()?;
        self.require_mode(
            "rekey_into_writer",
            DbMode::ReadOnly,
            DbModeCapabilities::forks_into_writer,
        )?;
        self.take_exclusive_writer_sentinel("fork").await?;

        // An earlier attempt that never published can leave its scratch and
        // segments behind. Nothing names either, so they are dropped first.
        let scratch = format!("{}.fork", self.main_db_path);
        remove_if_present(&*self.vfs, &scratch).await?;
        crate::recovery::fork::discard_forked_segments(&*self.vfs).await?;
        if let Err(error) = self.build_fork(&new_kek, &scratch).await {
            // Cleanup is best-effort. The directory stays intact either way.
            // The next attempt removes a leftover scratch before it starts.
            let _ = self.vfs.remove(&scratch).await;
            return Err(error);
        }

        // Close the cached `main.db` handle first. On Windows an open handle
        // blocks the rename. On Unix an open handle still reads the replaced
        // inode.
        self.pager.close_main_handle().await;
        self.vfs.rename(&scratch, &self.main_db_path).await?;
        self.vfs.sync_dir(self.main_db_parent_dir()).await?;

        let locks = std::mem::take(&mut self.sentinel_locks);
        let vfs = V::clone(&*self.vfs);
        let page_size = self.page_size;
        let realm = self.realm_id;
        let options = self.options.clone();
        drop(self);

        let mut db = Self::open_existing_inner_with_counterpart(
            vfs,
            new_kek,
            None,
            page_size,
            realm,
            options,
            DbMode::Standalone,
        )
        .await?;
        db.mode = DbMode::Standalone;
        db.sentinel_locks = locks;
        db.lock_required = true;
        crate::diag::reopened(db.latest_commit().0);
        Ok(db)
    }

    /// Build the complete replacement `main.db` at `scratch`, with its
    /// segments promoted under fresh ids. Nothing the live store references
    /// is touched.
    async fn build_fork(&self, new_kek: &SecretKey, scratch: &str) -> Result<()> {
        let state = self.writer.lock().await;
        // An apply journal names segment ids and pages under the source
        // identity. It must finish, through a Follower open, before a fork.
        if state.pending_apply_journal_id != [0; 16] {
            return Err(PagedbError::rekey_state_invalid(
                "rekey_into_writer.apply_journal_pending",
            ));
        }
        let fork = Self::bootstrap_unlocked_at(
            V::clone(&*self.vfs),
            new_kek,
            self.page_size,
            self.realm_id,
            self.options.clone(),
            self.cipher_id,
            scratch.to_string(),
        )
        .await?;
        copy_live_state(self, &state, &fork).await
    }
}
