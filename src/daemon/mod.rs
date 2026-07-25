use crate::config::Account;
use crate::db::Database;
use crate::sync;
use futures_util::StreamExt;
use jmap_client::DataType;
use jmap_client::client::Client;
use jmap_client::event_source::PushNotification;
use log::{debug, error, info, warn};
use std::time::Duration;
use tokio::signal::unix::{SignalKind, signal};
use tokio::task::{LocalSet, spawn_local};
use tokio_util::sync::CancellationToken;

use crate::config::Config;

const SSE_CONNECT_BACKOFF: Duration = Duration::from_secs(5);
const SSE_STREAM_DROP_BACKOFF: Duration = Duration::from_secs(1);
const SSE_PING_SECONDS: u32 = 60;

/// Long-running SSE loop for one account. Never returns while `cancel` is
/// live; exits cleanly when the token fires.
///
/// The `Client` is passed by value — this task owns it for its lifetime.
/// Multi-account supervision is C.3's concern; each account gets its own
/// tokio::task holding its own Client + Database + cancel-child token.
pub async fn run_account_sse_loop(
    client: Client,
    acct: Account,
    db: Database,
    cancel: CancellationToken,
) {
    let acct_name = acct.name.clone();
    let mut last_event_id: Option<String> = None;

    loop {
        if cancel.is_cancelled() {
            info!("[{acct_name}] cancel received; SSE loop exiting");
            return;
        }

        // Sync-on-connect: catch anything that landed while we were
        // disconnected. First-boot sync also runs here.
        if let Err(e) = sync::sync_account(&client, &acct, &db, false).await {
            error!("[{acct_name}] initial sync failed: {e:#}");
        }

        // event_source's types: None means server-side wildcard (all types).
        // We only care about mail-side changes; being explicit keeps the
        // wire minimal + logs cleaner.
        let types_iter = [DataType::Email, DataType::EmailDelivery, DataType::Mailbox];
        let stream_result = client
            .event_source(
                Some(types_iter),
                false, // close_after_state=false: long-lived stream
                Some(SSE_PING_SECONDS),
                last_event_id.as_deref(),
            )
            .await;

        let mut stream = match stream_result {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    "[{acct_name}] SSE connect failed: {e:#}; retry in {}s",
                    SSE_CONNECT_BACKOFF.as_secs()
                );
                sleep_or_cancel(SSE_CONNECT_BACKOFF, &cancel).await;
                continue;
            }
        };

        info!("[{acct_name}] SSE connected");

        // Polling fallback timer — fires every poll_interval_secs while
        // SSE is up but silent. When SSE fires reliably, these ticks are
        // redundant no-ops. Zero disables. `interval.tick()` fires
        // immediately by default; consume that so the first real tick
        // happens after `poll_interval_secs`, not on entry.
        let mut poll_interval = if acct.poll_interval_secs > 0 {
            let mut i = tokio::time::interval(Duration::from_secs(acct.poll_interval_secs));
            i.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            i.tick().await;
            Some(i)
        } else {
            None
        };

        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    info!("[{acct_name}] cancel received mid-stream; SSE loop exiting");
                    return;
                }
                _ = maybe_tick(poll_interval.as_mut()) => {
                    debug!("[{acct_name}] poll timer fired; triggering sync");
                    if let Err(e) = sync::sync_account(&client, &acct, &db, false).await {
                        error!("[{acct_name}] poll-triggered sync failed: {e:#}");
                    }
                }
                item = stream.next() => {
                    match item {
                        None => {
                            // Stream ended cleanly (server hung up).
                            debug!("[{acct_name}] SSE stream closed by server; reconnecting");
                            break;
                        }
                        Some(Err(e)) => {
                            warn!("[{acct_name}] SSE stream error: {e:#}; reconnecting");
                            break;
                        }
                        Some(Ok(PushNotification::StateChange(chg))) => {
                            if let Some(id) = chg.id() {
                                last_event_id = Some(id.to_string());
                            }
                            debug!("[{acct_name}] SSE StateChange; triggering sync");
                            if let Err(e) = sync::sync_account(&client, &acct, &db, false).await {
                                error!("[{acct_name}] sync failed: {e:#}");
                            }
                        }
                        Some(Ok(PushNotification::CalendarAlert(_))) => {
                            // Not a mail concern; ignore.
                        }
                    }
                }
            }
        }

        sleep_or_cancel(SSE_STREAM_DROP_BACKOFF, &cancel).await;
    }
}

/// Wait for the next tick of an optional interval. When `interval` is
/// None (polling disabled), this future never resolves — the select! arm
/// becomes inert without conditional compilation.
async fn maybe_tick(interval: Option<&mut tokio::time::Interval>) {
    match interval {
        Some(i) => {
            i.tick().await;
        }
        None => std::future::pending::<()>().await,
    }
}

/// Sleep for `d`, or return early if cancelled — never let the loop wait
/// past a shutdown.
async fn sleep_or_cancel(d: Duration, cancel: &CancellationToken) {
    tokio::select! {
        _ = tokio::time::sleep(d) => {}
        _ = cancel.cancelled() => {}
    }
}

/// Long-running daemon: spawns one supervised task per enabled account,
/// waits for SIGTERM or SIGINT, then fires the shared CancellationToken
/// so every task exits cleanly.
///
/// Bad client-build for one account is logged and skipped, not fatal —
/// a single misconfigured account shouldn't take the daemon down.
pub async fn run_daemon(config: Config) -> anyhow::Result<()> {
    // Database (rusqlite Connection) is !Sync, so the sync_account futures
    // aren't Send. Run all account tasks on a LocalSet — single-threaded
    // per-thread, non-Send futures OK. In practice each account is fully
    // I/O bound (JMAP over reqwest yields on every network op), so
    // single-threading is fine for the account counts we expect (1-3).
    let local = LocalSet::new();
    local.run_until(run_daemon_inner(config)).await
}

async fn run_daemon_inner(config: Config) -> anyhow::Result<()> {
    let cancel = CancellationToken::new();

    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    let mut spawned = 0usize;

    for acct in config.accounts.into_iter().filter(|a| a.enabled) {
        if acct.mail.is_none() {
            log::warn!(
                "[{}] no [accounts.mail] section; skipping (nothing to sync to)",
                acct.name
            );
            continue;
        }

        let db_path = config.db_dir.join(format!("{}.sqlite", acct.name));
        if let Some(parent) = db_path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                log::error!(
                    "[{}] cannot create db_dir {}: {e}; skipping account",
                    acct.name,
                    parent.display()
                );
                continue;
            }
        }
        let db = match crate::db::Database::open(&db_path) {
            Ok(d) => d,
            Err(e) => {
                log::error!(
                    "[{}] failed to open database at {}: {e:#}; skipping account",
                    acct.name,
                    db_path.display()
                );
                continue;
            }
        };

        let client = match crate::jmap::client_from_account(&acct).await {
            Ok(c) => c,
            Err(e) => {
                log::error!(
                    "[{}] failed to authenticate JMAP client: {e:#}; skipping account",
                    acct.name
                );
                continue;
            }
        };

        let task_cancel = cancel.child_token();
        let acct_name = acct.name.clone();
        let handle = spawn_local(async move {
            run_account_sse_loop(client, acct, db, task_cancel).await;
            log::info!("[{acct_name}] task exited");
        });
        handles.push(handle);
        spawned += 1;
    }

    if spawned == 0 {
        anyhow::bail!("no accounts to run; enable at least one account in config");
    }

    log::info!("daemon running with {spawned} account task(s); waiting for SIGTERM/SIGINT");

    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;
    tokio::select! {
        _ = sigterm.recv() => log::info!("received SIGTERM"),
        _ = sigint.recv()  => log::info!("received SIGINT"),
    }

    log::info!("shutting down; cancelling account tasks");
    cancel.cancel();

    for h in handles {
        if let Err(e) = h.await {
            log::error!("account task join error: {e}");
        }
    }
    log::info!("all account tasks exited; daemon done");
    Ok(())
}
/// One-shot sync: bring up every enabled account (or just the named one),
/// run `sync_account` once each, then exit. Reuses the same per-account
/// bring-up plumbing as `run_daemon` (Database open + client build), but
/// no SSE loop, no cancel token, no signal wait.
///
/// Returns `Err` if any per-account sync errored, or if `account_filter`
/// is set and matches no enabled account. Per-account bring-up failures
/// (bad db path, bad JMAP token) are logged and skipped; only the sync
/// pass itself counts against the error tally — same policy as
/// `run_daemon` for bring-up, stricter for the sync itself since a
/// one-shot is meant to succeed or fail visibly.
pub async fn run_sync_once(
    config: Config,
    account_filter: Option<&str>,
    dry_run: bool,
) -> anyhow::Result<()> {
    let local = LocalSet::new();
    local
        .run_until(run_sync_once_inner(config, account_filter, dry_run))
        .await
}

async fn run_sync_once_inner(
    config: Config,
    account_filter: Option<&str>,
    dry_run: bool,
) -> anyhow::Result<()> {
    let mut matched = 0usize;
    let mut errors = 0usize;

    for acct in config.accounts.into_iter().filter(|a| a.enabled) {
        if let Some(name) = account_filter {
            if acct.name != name {
                continue;
            }
        }
        matched += 1;

        if acct.mail.is_none() {
            log::warn!(
                "[{}] no [accounts.mail] section; skipping (nothing to sync to)",
                acct.name
            );
            continue;
        }

        let db_path = config.db_dir.join(format!("{}.sqlite", acct.name));
        if let Some(parent) = db_path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                log::error!(
                    "[{}] cannot create db_dir {}: {e}; skipping account",
                    acct.name,
                    parent.display()
                );
                continue;
            }
        }
        let db = match crate::db::Database::open(&db_path) {
            Ok(d) => d,
            Err(e) => {
                log::error!(
                    "[{}] failed to open database at {}: {e:#}; skipping account",
                    acct.name,
                    db_path.display()
                );
                continue;
            }
        };

        let client = match crate::jmap::client_from_account(&acct).await {
            Ok(c) => c,
            Err(e) => {
                log::error!(
                    "[{}] failed to authenticate JMAP client: {e:#}; skipping account",
                    acct.name
                );
                continue;
            }
        };

        match sync::sync_account(&client, &acct, &db, dry_run).await {
            Ok(_) => {}
            Err(e) => {
                log::error!("[{}] sync failed: {e:#}", acct.name);
                errors += 1;
            }
        }
    }

    if let Some(name) = account_filter {
        if matched == 0 {
            anyhow::bail!("no enabled account named {name:?}");
        }
    } else if matched == 0 {
        anyhow::bail!("no enabled accounts to sync");
    }

    if errors > 0 {
        anyhow::bail!("{errors} account sync(s) failed");
    }

    Ok(())
}
