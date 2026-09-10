use crate::config::Account;
use crate::db::Database;
use crate::sync;

mod resilience;
mod single_instance;
mod submit;
use futures_util::StreamExt;
use jmap_client::DataType;
use jmap_client::client::Client;
use jmap_client::event_source::PushNotification;
use log::{debug, error, info, warn};
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::path::Path;
use std::time::Duration;
use std::sync::Arc;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::mpsc;
use tokio::task::{LocalSet, spawn_local};
use tokio_util::sync::CancellationToken;

use crate::config::Config;

const ACCOUNT_RETRY_BASE: Duration = Duration::from_secs(5);
const ACCOUNT_RETRY_CAP: Duration = Duration::from_secs(300);
const SSE_STREAM_DROP_BACKOFF: Duration = Duration::from_secs(1);
const SSE_PING_SECONDS: u32 = 60;
const MAILDIR_DEBOUNCE: Duration = Duration::from_millis(500);

/// Long-running SSE loop for one account. Never returns while `cancel` is
/// live; exits cleanly when the token fires.
///
/// The `Client` is passed by value — this task owns it for its lifetime.
/// Multi-account supervision is C.3's concern; each account gets its own
/// tokio::task holding its own Client + Database + cancel-child token.
pub async fn run_account_sse_loop(
    client: Arc<Client>,
    acct: Account,
    db: Database,
    cancel: CancellationToken,
) {
    let acct_name = acct.name.clone();
    let mut last_event_id: Option<String> = None;
    let mut retry_backoff = resilience::RetryBackoff::new(
        ACCOUNT_RETRY_BASE,
        ACCOUNT_RETRY_CAP,
    );
    let mut failure_notifier = resilience::FailureNotifier::new(&acct_name, &acct.jmap_host);

    // Optional Maildir watcher — the local-side counterpart to SSE. Fires
    // a sync tick whenever anything under the Maildir tree changes so that
    // a client (aerc, notmuch, mutt, plain rm) causing a local mutation
    // reaches JMAP within a debounce window (~500ms) instead of waiting
    // for the SSE-silent poll fallback. Held here for the loop's lifetime
    // so the underlying inotify subscription stays live across reconnects.
    let mut watch = match spawn_maildir_watch_for_account(&acct, cancel.clone()) {
        Ok(rx) => rx,
        Err(e) => {
            warn!("[{acct_name}] maildir watch setup failed: {e:#}; continuing poll-only");
            None
        }
    };

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
                let delay = retry_backoff.take_delay();
                warn!(
                    "[{acct_name}] SSE connect failed: {e:#}; waiting for connectivity or retrying in {}s",
                    delay.as_secs()
                );
                failure_notifier.failed().await;
                if !resilience::wait_for_connectivity_or_delay(delay, &cancel).await {
                    return;
                }
                continue;
            }
        };

        retry_backoff.reset();
        failure_notifier.recovered().await;
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

        let mut reconnect_after_failure = false;
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
                _ = maybe_watch(watch.as_mut()) => {
                    debug!("[{acct_name}] maildir change detected; triggering sync");
                    if let Err(e) = sync::sync_account(&client, &acct, &db, false).await {
                        error!("[{acct_name}] watch-triggered sync failed: {e:#}");
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
                            failure_notifier.failed().await;
                            reconnect_after_failure = true;
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

        if reconnect_after_failure {
            let delay = retry_backoff.take_delay();
            if !resilience::wait_for_connectivity_or_delay(delay, &cancel).await {
                return;
            }
        } else {
            sleep_or_cancel(SSE_STREAM_DROP_BACKOFF, &cancel).await;
        }
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

/// Same shape as `maybe_tick` but for the Maildir watcher's debounced
/// channel: when the watcher isn't set up (no `[accounts.mail]` section
/// or `watch_maildir = false`), the arm stays inert forever.
async fn maybe_watch(rx: Option<&mut MaildirWatchRx>) {
    match rx {
        Some(r) => {
            let _ = r.rx.recv().await;
        }
        None => std::future::pending::<()>().await,
    }
}

/// Owns the notify Watcher (kept alive so its inotify subscription
/// doesn't drop) plus the debounced tick channel the SSE loop reads.
/// The watcher itself talks on its own OS thread; the debouncer task
/// coalesces bursts into one signal per MAILDIR_DEBOUNCE window.
struct MaildirWatchRx {
    _watcher: RecommendedWatcher,
    rx: mpsc::Receiver<()>,
}

fn spawn_maildir_watch_for_account(
    acct: &Account,
    cancel: CancellationToken,
) -> anyhow::Result<Option<MaildirWatchRx>> {
    let Some(mail_cfg) = acct.mail.as_ref() else {
        return Ok(None);
    };
    if !mail_cfg.watch_maildir {
        return Ok(None);
    }
    let w = spawn_maildir_watch(&mail_cfg.path, MAILDIR_DEBOUNCE, cancel)?;
    info!(
        "[{}] watching maildir {} for local changes (debounce {}ms)",
        acct.name,
        mail_cfg.path.display(),
        MAILDIR_DEBOUNCE.as_millis()
    );
    Ok(Some(w))
}

fn spawn_maildir_watch(
    mail_path: &Path,
    debounce: Duration,
    cancel: CancellationToken,
) -> anyhow::Result<MaildirWatchRx> {
    // Raw notify events land in `raw_tx`; the debouncer coalesces them
    // into `tick_tx`, which the SSE loop consumes. Bounded channels: if
    // the sync tick backs up, dropping newer events is fine — the next
    // tick reads full state from disk anyway.
    let (raw_tx, mut raw_rx) = mpsc::channel::<()>(64);
    let (tick_tx, tick_rx) = mpsc::channel::<()>(4);

    let mut watcher: RecommendedWatcher = notify::recommended_watcher(
        move |res: notify::Result<notify::Event>| {
            let Ok(event) = res else { return };
            if matches!(
                event.kind,
                EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
            ) {
                // blocking_send from notify's OS thread — the tokio
                // channel's blocking API is what this is for. try_send
                // would drop under a full buffer; we want at-least-one
                // signal per burst, so accept a brief block on the
                // notify thread.
                let _ = raw_tx.blocking_send(());
            }
        },
    )?;
    watcher.watch(mail_path, RecursiveMode::Recursive)?;

    spawn_local(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                first = raw_rx.recv() => {
                    if first.is_none() {
                        return;
                    }
                    // Wait out the debounce, then drain everything else
                    // that piled up — one tick per burst.
                    tokio::time::sleep(debounce).await;
                    while raw_rx.try_recv().is_ok() {}
                    if tick_tx.send(()).await.is_err() {
                        return;
                    }
                }
            }
        }
    });

    Ok(MaildirWatchRx {
        _watcher: watcher,
        rx: tick_rx,
    })
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
/// Client setup for each account is supervised independently. A transient
/// startup failure is visible to the user and retries without taking down the
/// daemon or preventing other accounts from running.
pub async fn run_daemon(config: Config) -> anyhow::Result<()> {
    // Single-instance guard. Held for the full daemon lifetime; the
    // kernel releases the flock automatically when the process exits,
    // including on SIGKILL. See  module header for why.
    let _lock = single_instance::acquire()?;
    info!("[daemon] acquired single-instance lock (pid={})", std::process::id());

    // Database (rusqlite Connection) is !Sync, so the sync_account futures
    // aren't Send. Run all account tasks on a LocalSet — single-threaded
    // per-thread, non-Send futures OK. In practice each account is fully
    // I/O bound (JMAP over reqwest yields on every network op), so
    // single-threading is fine for the account counts we expect (1-3).
    let local = LocalSet::new();
    local.run_until(run_daemon_inner(config)).await
}

async fn run_account_supervisor(acct: Account, db: Database, cancel: CancellationToken) {
    let acct_name = acct.name.clone();
    let mut retry_backoff =
        resilience::RetryBackoff::new(ACCOUNT_RETRY_BASE, ACCOUNT_RETRY_CAP);
    let mut failure_notifier = resilience::FailureNotifier::new(&acct_name, &acct.jmap_host);

    let client = loop {
        let result = tokio::select! {
            _ = cancel.cancelled() => return,
            result = crate::jmap::client_from_account(&acct) => result,
        };

        match result {
            Ok(client) => {
                retry_backoff.reset();
                failure_notifier.recovered().await;
                break Arc::new(client);
            }
            Err(error) => {
                let delay = retry_backoff.take_delay();
                log::error!(
                    "[{acct_name}] JMAP client setup failed: {error:#}; waiting for connectivity or retrying in {}s",
                    delay.as_secs()
                );
                failure_notifier.failed().await;
                if !resilience::wait_for_connectivity_or_delay(delay, &cancel).await {
                    return;
                }
            }
        }
    };

    // Send-path Phase E: if the account has [accounts.submit], spawn a
    // second per-account task that watches Outbox/ + Failed/. Both tasks
    // share one authenticated client and connection pool.
    let submit_handle = if acct.submit.is_some() {
        let submit_cancel = cancel.child_token();
        let submit_acct = acct.clone();
        let submit_client = Arc::clone(&client);
        let submit_name = acct_name.clone();
        Some(spawn_local(async move {
            submit::run_account_submit_loop(submit_client, submit_acct, submit_cancel).await;
            log::info!("[{submit_name}/submit] task exited");
        }))
    } else {
        None
    };

    run_account_sse_loop(client, acct, db, cancel).await;
    if let Some(handle) = submit_handle {
        match handle.await {
            Ok(()) => {}
            Err(error) => log::error!("[{acct_name}/submit] task join error: {error}"),
        }
    }
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

        let task_cancel = cancel.child_token();
        let acct_name = acct.name.clone();
        let handle = spawn_local(async move {
            run_account_supervisor(acct, db, task_cancel).await;
            log::info!("[{acct_name}] supervised account task exited");
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
