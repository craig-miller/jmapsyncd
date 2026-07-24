pub mod email;
pub mod mailbox;

use crate::config::Account;
use crate::db::Database;
use anyhow::{Context, Result};
use jmap_client::client::Client;
use log::info;
use std::time::Instant;

use self::email::EmailSyncStats;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct SyncStats {
    pub email: EmailSyncStats,
    pub elapsed_ms: u64,
}

/// Sync a single account end-to-end: mailbox tree first (B.2), then emails
/// against the resulting tree (B.3). Client is passed in already-authenticated
/// so callers (daemon SSE loop, one-shot sync command) can keep a long-lived
/// per-account Client across many ticks.
pub async fn sync_account(
    client: &Client,
    acct: &Account,
    db: &Database,
) -> Result<SyncStats> {
    let mail_cfg = acct.mail.as_ref().with_context(|| {
        format!(
            "account {:?} has no [accounts.mail] section; nothing to sync",
            acct.name
        )
    })?;

    let start = Instant::now();
    let mailboxes = mailbox::sync_mailboxes(client, db, acct).await?;
    let email = email::sync_emails(client, db, &mailboxes, &mail_cfg.path).await?;
    let elapsed_ms = start.elapsed().as_millis() as u64;

    info!(
        "[{}] sync tick complete in {}ms ({} emails: {} new, {} updated, {} moved, {} deleted, {} bytes)",
        acct.name,
        elapsed_ms,
        email.created + email.updated + email.moved + email.deleted,
        email.created,
        email.updated,
        email.moved,
        email.deleted,
        email.bytes_downloaded,
    );
    Ok(SyncStats { email, elapsed_ms })
}
