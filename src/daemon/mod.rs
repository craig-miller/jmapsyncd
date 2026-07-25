use crate::config::Account;
use crate::db::Database;
use crate::sync;
use futures_util::StreamExt;
use jmap_client::DataType;
use jmap_client::client::Client;
use jmap_client::event_source::PushNotification;
use log::{debug, error, info, warn};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

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
        if let Err(e) = sync::sync_account(&client, &acct, &db).await {
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
                    if let Err(e) = sync::sync_account(&client, &acct, &db).await {
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
                            if let Err(e) = sync::sync_account(&client, &acct, &db).await {
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
