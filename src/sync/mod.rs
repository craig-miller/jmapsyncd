pub mod email;
pub mod mailbox;

use crate::config::Account;
use crate::db::Database;
use anyhow::{Context, Result};
use jmap_client::client::Client;
use log::{debug, info, warn};
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
///
/// When `dry_run` is true, every filesystem + database mutation is skipped;
/// read-side JMAP calls (Mailbox/get, Email/query, Email/get, Blob/download)
/// still run so the stats reflect the actual would-have shape. Each skipped
/// write logs `[dry-run] would ...` at debug level.
pub async fn sync_account(
    client: &Client,
    acct: &Account,
    db: &Database,
    dry_run: bool,
) -> Result<SyncStats> {
    let mail_cfg = acct.mail.as_ref().with_context(|| {
        format!(
            "account {:?} has no [accounts.mail] section; nothing to sync",
            acct.name
        )
    })?;

    let start = Instant::now();
    let mailboxes = mailbox::sync_mailboxes(client, db, acct, dry_run).await?;
    let email =
        email::sync_emails(client, db, &mailboxes, &mail_cfg.path, dry_run).await?;
    let elapsed_ms = start.elapsed().as_millis() as u64;

    info!(
        "[{}] {}sync tick complete in {}ms ({} emails: {} new, {} updated, {} moved, {} deleted, {} bytes)",
        acct.name,
        if dry_run { "[dry-run] " } else { "" },
        elapsed_ms,
        email.created + email.updated + email.moved + email.deleted,
        email.created,
        email.updated,
        email.moved,
        email.deleted,
        email.bytes_downloaded,
    );

    if !dry_run && email.created + email.updated + email.moved + email.deleted > 0 {
        if let Some(hook) = mail_cfg.post_sync_hook.clone() {
            let account_name = acct.name.clone();
            tokio::spawn(async move {
                match tokio::process::Command::new("sh")
                    .arg("-c")
                    .arg(&hook)
                    .status()
                    .await
                {
                    Ok(s) if s.success() => {
                        debug!("[{}] post-sync hook OK: {}", account_name, hook)
                    }
                    Ok(s) => warn!("[{}] post-sync hook exit {}: {}", account_name, s, hook),
                    Err(e) => warn!(
                        "[{}] post-sync hook spawn failed: {}: {}",
                        account_name, e, hook
                    ),
                }
            });
        }
    }

    Ok(SyncStats { email, elapsed_ms })
}
