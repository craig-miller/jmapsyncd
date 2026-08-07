//! Per-account Outbox watcher — send-path Phase E (real submission).
//!
//! Runs alongside the SSE sync loop for any account with an
//! `[accounts.submit]` block. Watches the account's `Outbox/` and
//! `Failed/` Maildirs, enumerates queued messages, and drives them
//! through the JMAP submission chain (Blob/upload + Email/import +
//! EmailSubmission/set with onSuccessUpdateEmail).
//!
//! Design notes:
//!
//! - **Good-citizen posture.** All requests to the JMAP server are
//!   serial per account (no in-flight parallelism). Failures back off
//!   exponentially with jitter and NEVER retry-storm. Permanent
//!   failures (4xx-class) move once to `Failed/` and are never
//!   re-attempted without user action (drag-back).
//!
//! - **Startup context resolution.** `SubmitContext::resolve` runs one
//!   Identity/get + one Mailbox/get on daemon startup, caches the
//!   identity_id / drafts_id / sent_id / scheduled folder id.
//!   Never refreshed while the loop is up — a full daemon restart is
//!   the recovery path if the server-side identity changes.
//!
//! - **Retry schedule.** `RetrySchedule` is an in-memory BTreeMap keyed
//!   by due `Instant`, with a basename-index for cheap rescheduling.
//!   `wait_next()` returns a future that resolves at the earliest due
//!   time, or stays pending forever when the schedule is empty.
//!
//! - **Sidecar `in_flight` field.** Set at the moment of the first
//!   request (`Blob/upload`) and cleared on any terminal outcome.
//!   Startup scan surfaces any lingering `in_flight` sidecar older
//!   than STARTUP_INFLIGHT_STALENESS as a LOUD warning: the daemon
//!   crashed mid-submit, and we can't tell whether the server saw
//!   the request. We retry (may duplicate) — losing mail is worse
//!   than the small window of duplicate-on-crash.
//!
//! - **Drag-back rename pairing.** A `Failed → Outbox` move fires
//!   two inotify events: rename-from (Failed side) then rename-to
//!   (Outbox side), a few ms apart. We keep a short-lived
//!   `HashMap<basename, PendingRename>` cache with a
//!   RENAME_PAIR_WINDOW timeout. If the pair completes, we treat
//!   it as a single drag-back: strip Family-2 diagnostic headers
//!   from the file atomically (tmp+rename) and re-enqueue.
//!
//! - **Family-2 headers** (`X-JMAP-Failure`, `X-JMAP-Failed-At`,
//!   `X-JMAP-Ulid`): injected only on a permanent-failure move to
//!   Failed/, stripped on drag-back. Never visible on the wire.

use crate::config::{Account, ScheduledFolder, SubmitConfig};
use crate::jmap::restrict_using;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use jmap_client::{
    client::Client,
    core::{
        error::{MethodError, MethodErrorType},
        response::{IdentityGetResponse, MailboxGetResponse, MethodResponse},
        set::SetObject,
    },
    mailbox::Role,
    Error as JmapError,
};
use log::{debug, error, info, warn};
use notify::{
    event::{ModifyKind, RenameMode},
    EventKind, RecommendedWatcher, RecursiveMode, Watcher,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::mpsc, task::spawn_local};
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

// ---------------------------------------------------------------------------
// Tuning constants
// ---------------------------------------------------------------------------

/// Debounce window for coalescing rapid same-file inotify events.
const EVENT_DEBOUNCE: Duration = Duration::from_millis(100);

/// How long a `RenameFrom` event waits for its paired `RenameTo`
/// before we give up and treat the from-event as a plain remove.
/// 500ms is generous — the two events usually arrive within a few ms.
const RENAME_PAIR_WINDOW: Duration = Duration::from_millis(500);

/// Sidecars with `in_flight` older than this at startup produce a loud
/// warning about a possible mid-submit crash.
const STARTUP_INFLIGHT_STALENESS: Duration = Duration::from_secs(60);

/// Jitter amplitude on backoff — ±20% of the computed delay. Guards
/// against thundering-herd when several messages retry simultaneously
/// after a network recovery.
const BACKOFF_JITTER: f64 = 0.20;

/// Cap on how many attempts we double for. 2^30 seconds is ~34 years;
/// this is only about u64 overflow safety, not a real cap on retries.
const BACKOFF_ATTEMPT_CAP: u32 = 30;

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Per-account outbox watcher. Long-running; returns when `cancel` fires.
/// A bring-up failure (no identity match, no drafts folder, watcher setup
/// error) is logged and the loop returns — one broken account should not
/// tear the daemon down.
pub async fn run_account_submit_loop(
    client: Arc<Client>,
    acct: Account,
    cancel: CancellationToken,
) {
    let acct_name = acct.name.clone();
    if let Err(e) = run_account_submit_loop_inner(client, acct, cancel).await {
        error!("[{acct_name}/submit] loop terminated: {e:#}");
    }
}

async fn run_account_submit_loop_inner(
    client: Arc<Client>,
    acct: Account,
    cancel: CancellationToken,
) -> Result<()> {
    let acct_name = acct.name.clone();

    let Some(submit_cfg) = acct.submit.as_ref() else {
        debug!("[{acct_name}] no [accounts.submit] block; submit loop not spawned");
        return Ok(());
    };
    let Some(mail_cfg) = acct.mail.as_ref() else {
        warn!("[{acct_name}] [accounts.submit] present but no [accounts.mail]; skip");
        return Ok(());
    };

    let mail_root = mail_cfg.path.clone();
    let outbox = mail_root.join("Outbox");
    let failed = mail_root.join("Failed");

    // Ensure both Maildirs exist so the watch subscription can attach.
    for base in [&outbox, &failed] {
        for sub in ["tmp", "new", "cur"] {
            std::fs::create_dir_all(base.join(sub))
                .with_context(|| format!("create {}", base.join(sub).display()))?;
        }
    }

    let sidecar_dir = sidecar_dir_for(&acct_name)?;
    std::fs::create_dir_all(&sidecar_dir)
        .with_context(|| format!("create {}", sidecar_dir.display()))?;

    // Resolve identity + mailbox ids up-front. Fail-fast if the config
    // doesn't match anything on the server — better to know at startup
    // than at first submit.
    let ctx = SubmitContext::resolve(&client, submit_cfg).await
        .with_context(|| format!("[{acct_name}] resolve submit context"))?;
    info!(
        "[{acct_name}/submit] context: identity={} drafts={} sent={} scheduled={}",
        ctx.identity_id,
        ctx.drafts_id,
        ctx.sent_id,
        ctx.scheduled_file_target_id
            .as_ref()
            .map(String::as_str)
            .unwrap_or("<falls-back-to-sent>"),
    );

    // Loudly warn on any in-flight sidecars — we crashed mid-submit
    // previously. Then treat them as pending (may duplicate on server).
    warn_on_stale_inflight(&sidecar_dir, &acct_name);

    // Seed known-failures from disk so drag-back detection survives
    // daemon restarts.
    let mut known_failures = scan_basenames(&failed);
    info!("[{acct_name}/submit] known failures at startup: {}", known_failures.len());

    // Startup scan: schedule every Outbox file at its sidecar's
    // recorded next_at (or immediately if none).
    let mut retry_queue = RetrySchedule::new();
    let mut pending_renames: HashMap<String, PendingRename> = HashMap::new();
    let now_epoch_secs = epoch_secs_now();
    for path in scan_maildir_files(&outbox) {
        let basename = strip_flags(&filename_of(&path));
        let sidecar = SidecarState::load(&sidecar_dir, &basename);
        let due_secs = sidecar
            .retry
            .as_ref()
            .and_then(|r| r.next_at_epoch_secs())
            .unwrap_or(now_epoch_secs);
        let delay = Duration::from_secs(due_secs.saturating_sub(now_epoch_secs));
        retry_queue.schedule(basename.clone(), Instant::now() + delay);
        info!(
            "[{acct_name}/submit] startup enqueue: {basename} in {}s — {}",
            delay.as_secs(),
            sidecar.format_log(),
        );
    }

    // Watcher — separate raw event stream, debounced.
    let watcher_rx = spawn_outbox_watch(&outbox, &failed, EVENT_DEBOUNCE, cancel.clone())
        .with_context(|| format!("[{acct_name}] outbox watch setup"))?;
    let mut rx = watcher_rx.rx;
    let _watcher_keep_alive = watcher_rx._watcher;

    info!(
        "[{acct_name}/submit] watching {} + {}",
        outbox.display(),
        failed.display()
    );

    loop {
        // Sweep any timed-out pending-rename entries so they don't
        // linger past the pair window (they degrade to plain removes).
        expire_pending_renames(&mut pending_renames, &mut known_failures, &acct_name);

        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                info!("[{acct_name}/submit] cancel; loop exiting");
                return Ok(());
            }
            _ = retry_queue.wait_next() => {
                for basename in retry_queue.drain_due() {
                    if cancel.is_cancelled() {
                        return Ok(());
                    }
                    handle_submit_attempt(
                        &client, &ctx, submit_cfg,
                        &sidecar_dir, &outbox, &failed,
                        &basename, &mut retry_queue,
                        &acct_name,
                    ).await;
                }
            }
            maybe_ev = rx.recv() => {
                let Some(ev) = maybe_ev else {
                    info!("[{acct_name}/submit] watcher channel closed; loop exiting");
                    return Ok(());
                };
                handle_event(
                    &acct_name, &outbox, &sidecar_dir,
                    &mut known_failures, &mut pending_renames, &mut retry_queue,
                    ev,
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// SubmitContext — cached identity + mailbox IDs
// ---------------------------------------------------------------------------

struct SubmitContext {
    account_id: String,
    identity_id: String,
    drafts_id: String,
    sent_id: String,
    /// When scheduled sends should land in a folder OTHER than Sent
    /// on submission success, this is that folder's id. `None` means
    /// "use sent_id for scheduled sends too" (portable fallback).
    scheduled_file_target_id: Option<String>,
}

impl SubmitContext {
    async fn resolve(client: &Client, submit_cfg: &SubmitConfig) -> Result<Self> {
        let account_id = client.default_account_id().to_string();

        // ---- Identity ------------------------------------------------
        let identity_id = if let Some(id) = &submit_cfg.identity_id {
            id.clone()
        } else {
            let mut req = client.build();
            restrict_using(&mut req);
            req.get_identity();
            let mut resp: IdentityGetResponse = req.send_single().await
                .context("Identity/get")?;
            let identities = resp.take_list();
            identities
                .into_iter()
                .find(|i| {
                    i.email()
                        .map(|e| e.eq_ignore_ascii_case(&submit_cfg.from_address))
                        .unwrap_or(false)
                })
                .and_then(|i| i.id().map(String::from))
                .ok_or_else(|| {
                    anyhow!(
                        "no JMAP Identity matches from_address {:?} — check `pass show` \
                         picks up the right account, and that the identity exists in \
                         Fastmail's Settings → Sending identities",
                        submit_cfg.from_address,
                    )
                })?
        };

        // ---- Mailboxes: drafts, sent, and (optionally) Scheduled -----
        let mut req = client.build();
        restrict_using(&mut req);
        req.get_mailbox();
        let mut resp: MailboxGetResponse = req.send_single().await
            .context("Mailbox/get")?;
        let mailboxes = resp.take_list();

        let drafts_id = mailboxes
            .iter()
            .find(|m| matches!(m.role(), Role::Drafts))
            .and_then(|m| m.id().map(String::from))
            .ok_or_else(|| {
                anyhow!("no Drafts mailbox (role=drafts) on the account — send path cannot start")
            })?;
        let sent_id = mailboxes
            .iter()
            .find(|m| matches!(m.role(), Role::Sent))
            .and_then(|m| m.id().map(String::from))
            .ok_or_else(|| {
                anyhow!("no Sent mailbox (role=sent) on the account — send path cannot start")
            })?;

        let scheduled_file_target_id = match &submit_cfg.scheduled_folder {
            ScheduledFolder::Sent => None,
            ScheduledFolder::Auto => find_mailbox_id_by_name(&mailboxes, "Scheduled"),
            ScheduledFolder::Named(name) => {
                let found = find_mailbox_id_by_name(&mailboxes, name);
                if found.is_none() {
                    warn!(
                        "scheduled_folder = {name:?} but no such mailbox found; \
                         scheduled sends will fall back to Sent"
                    );
                }
                found
            }
        };

        Ok(Self {
            account_id,
            identity_id,
            drafts_id,
            sent_id,
            scheduled_file_target_id,
        })
    }

    /// Mailbox id the message should file into on submission success.
    fn file_target_id(&self, has_send_at: bool) -> &str {
        if has_send_at {
            self.scheduled_file_target_id.as_deref().unwrap_or(&self.sent_id)
        } else {
            &self.sent_id
        }
    }
}

fn find_mailbox_id_by_name(
    mailboxes: &[jmap_client::mailbox::Mailbox<jmap_client::Get>],
    target: &str,
) -> Option<String> {
    mailboxes
        .iter()
        .find(|m| m.name().map(|n| n.eq_ignore_ascii_case(target)).unwrap_or(false))
        .and_then(|m| m.id().map(String::from))
}

// ---------------------------------------------------------------------------
// RetrySchedule — in-memory backoff queue
// ---------------------------------------------------------------------------

struct RetrySchedule {
    schedule: BTreeMap<Instant, HashSet<String>>,
    index: HashMap<String, Instant>,
}

impl RetrySchedule {
    fn new() -> Self {
        Self {
            schedule: BTreeMap::new(),
            index: HashMap::new(),
        }
    }

    /// Schedule (or reschedule) `basename` for the given instant.
    /// A previously-scheduled entry is moved cleanly.
    fn schedule(&mut self, basename: String, when: Instant) {
        self.remove(&basename);
        self.schedule
            .entry(when)
            .or_insert_with(HashSet::new)
            .insert(basename.clone());
        self.index.insert(basename, when);
    }

    fn remove(&mut self, basename: &str) {
        if let Some(prev) = self.index.remove(basename) {
            if let Some(set) = self.schedule.get_mut(&prev) {
                set.remove(basename);
                if set.is_empty() {
                    self.schedule.remove(&prev);
                }
            }
        }
    }

    /// Pop all entries whose due-time is at or before now.
    fn drain_due(&mut self) -> Vec<String> {
        let now = Instant::now();
        let mut due = Vec::new();
        while let Some((&when, _)) = self.schedule.iter().next() {
            if when > now {
                break;
            }
            let set = self.schedule.remove(&when).unwrap_or_default();
            for name in &set {
                self.index.remove(name);
            }
            due.extend(set);
        }
        due
    }

    /// Future that resolves at the earliest due-time. When the schedule
    /// is empty, stays pending forever (select! arm becomes inert).
    async fn wait_next(&self) {
        match self.schedule.iter().next() {
            Some((&when, _)) => {
                let now = Instant::now();
                if when <= now {
                    return;
                }
                tokio::time::sleep_until(tokio::time::Instant::from_std(when)).await;
            }
            None => std::future::pending::<()>().await,
        }
    }
}

/// Backoff schedule: base * 2^(attempts-1), capped, jittered.
/// `attempts` is the count AFTER this failure (so the first retry
/// gets `base_secs`; second gets `base_secs * 2`; etc).
fn backoff_delay(attempts: u32, base_secs: u64, cap_secs: u64) -> Duration {
    let exp = attempts.saturating_sub(1).min(BACKOFF_ATTEMPT_CAP);
    let raw = base_secs.saturating_mul(1u64.checked_shl(exp).unwrap_or(u64::MAX));
    let raw = raw.min(cap_secs).max(1);
    let jitter = raw as f64 * BACKOFF_JITTER * jitter_fraction();
    let jittered = (raw as f64 + jitter).max(1.0);
    Duration::from_secs_f64(jittered)
}

/// Cheap pseudo-random -1..=1 from the current wall clock. We don't
/// need cryptographic quality; we want to break up simultaneous retries.
fn jitter_fraction() -> f64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    (nanos as f64 / 1_000_000_000.0) * 2.0 - 1.0
}

// ---------------------------------------------------------------------------
// Submit outcome + error classification
// ---------------------------------------------------------------------------

enum SubmitOutcome {
    /// All method calls succeeded; entry is done.
    Success,
    /// Retry with backoff. String is the human-readable reason.
    Transient(String),
    /// Move to Failed. String becomes the `X-JMAP-Failure` header value.
    Permanent(String),
}

impl SubmitOutcome {
    fn from_jmap_error(e: JmapError) -> Self {
        match e {
            // Any network / TLS / body-read error: retry with backoff.
            JmapError::Transport(err) => {
                let msg = format!("transport: {err}");
                if let Some(status) = err.status() {
                    if status.is_client_error() && status.as_u16() != 429 {
                        return SubmitOutcome::Permanent(format!("http {}: {err}", status.as_u16()));
                    }
                }
                SubmitOutcome::Transient(msg)
            }
            // ProblemDetails is a JMAP-level HTTP error object.
            // Only status >= 500 or 429 count as transient; 4xx else is permanent.
            JmapError::Problem(pd) => {
                let status = pd.status().unwrap_or(0);
                let msg = format!(
                    "problem status={} title={:?}",
                    status,
                    pd.title().unwrap_or("")
                );
                if status >= 500 || status == 429 || status == 408 {
                    SubmitOutcome::Transient(msg)
                } else {
                    SubmitOutcome::Permanent(msg)
                }
            }
            JmapError::Server(msg) => {
                // Ambiguous — the server said something the crate couldn't
                // parse. Treat as transient (safer than moving to Failed
                // on a parse fluke) but log loud.
                warn!("jmap-client Error::Server (ambiguous, treating as transient): {msg}");
                SubmitOutcome::Transient(format!("server: {msg}"))
            }
            JmapError::Method(me) => Self::from_method_error(&me),
            JmapError::Set(se) => {
                // Not expected at request level — SetError normally arrives
                // inside SetResponse.not_created. Treat as permanent since
                // it's specific enough to say something is wrong with the
                // record.
                SubmitOutcome::Permanent(format!("set: {se:?}"))
            }
            JmapError::Parse(err) => {
                warn!("jmap-client Error::Parse (treating as transient): {err}");
                SubmitOutcome::Transient(format!("parse: {err}"))
            }
            JmapError::Internal(msg) => {
                warn!("jmap-client Error::Internal (treating as transient): {msg}");
                SubmitOutcome::Transient(format!("internal: {msg}"))
            }
            JmapError::WebSocket(err) => {
                // Not our transport (we use HTTP) but jmap-client's
                // default-features build compiles the enum variant
                // unconditionally. Treat as transient — a WebSocket
                // error is always a connection issue, never a
                // per-message reject.
                warn!("jmap-client Error::WebSocket (treating as transient): {err}");
                SubmitOutcome::Transient(format!("websocket: {err}"))
            }
        }
    }

    fn from_method_error(me: &MethodError) -> Self {
        use MethodErrorType as T;
        match me.error() {
            // Server-side hiccups: retry.
            T::ServerUnavailable | T::ServerFail | T::ServerPartialFail => {
                SubmitOutcome::Transient(format!("method {}", me))
            }
            // Anything else at method level is a client-error: our
            // request is malformed / not allowed / not found. Permanent.
            _ => SubmitOutcome::Permanent(format!("method {}", me)),
        }
    }

}


// ---------------------------------------------------------------------------
// submit_one + outcome dispatch
// ---------------------------------------------------------------------------

async fn handle_submit_attempt(
    client: &Client,
    ctx: &SubmitContext,
    submit_cfg: &SubmitConfig,
    sidecar_dir: &Path,
    outbox: &Path,
    failed: &Path,
    basename: &str,
    retry_queue: &mut RetrySchedule,
    acct_name: &str,
) {
    let (msg_path, msg_bytes) = match locate_and_read_outbox_message(outbox, basename) {
        Ok(x) => x,
        Err(_) => {
            debug!("[{acct_name}/submit] {basename}: file vanished; dropping from queue");
            let _ = std::fs::remove_file(sidecar_dir.join(format!("{basename}.json")));
            return;
        }
    };
    let mut sidecar = SidecarState::load(sidecar_dir, basename);

    // Mark in_flight for crash-recovery accounting; save before the network I/O.
    sidecar.in_flight = Some(iso_now());
    if let Err(e) = sidecar.save(sidecar_dir, basename) {
        warn!("[{acct_name}/submit] {basename}: sidecar in_flight save: {e:#}");
    }

    let outcome = submit_one(client, ctx, &msg_bytes, &sidecar).await;

    match outcome {
        SubmitOutcome::Success => {
            info!("[{acct_name}/submit] {basename}: submitted");
            let _ = std::fs::remove_file(&msg_path);
            let _ = std::fs::remove_file(sidecar_dir.join(format!("{basename}.json")));
        }
        SubmitOutcome::Transient(reason) => {
            let attempts = sidecar
                .retry
                .as_ref()
                .map(|r| r.attempts.saturating_add(1))
                .unwrap_or(1);
            let delay = backoff_delay(attempts, submit_cfg.retry_base_secs, submit_cfg.retry_cap_secs);
            let due_at_secs = epoch_secs_now().saturating_add(delay.as_secs());
            sidecar.in_flight = None;
            sidecar.retry = Some(RetryState {
                attempts,
                next_at: Some(iso_from_epoch_secs(due_at_secs)),
                last_error: Some(reason.clone()),
            });
            if let Err(e) = sidecar.save(sidecar_dir, basename) {
                warn!("[{acct_name}/submit] {basename}: sidecar backoff save: {e:#}");
            }
            warn!(
                "[{acct_name}/submit] {basename}: transient (attempt={attempts}, next in {}s) — {reason}",
                delay.as_secs()
            );
            retry_queue.schedule(basename.to_string(), Instant::now() + delay);
        }
        SubmitOutcome::Permanent(reason) => {
            error!("[{acct_name}/submit] {basename}: permanent — {reason}");
            let ulid = Ulid::new().to_string();
            match move_to_failed_atomic(&msg_path, failed, basename, &reason, &ulid) {
                Ok(final_path) => {
                    info!(
                        "[{acct_name}/submit] {basename}: moved to {}",
                        final_path.display()
                    );
                }
                Err(e) => {
                    error!(
                        "[{acct_name}/submit] {basename}: mv-to-Failed failed: {e:#}. \
                         Leaving file in Outbox but disabling retry to avoid a spam loop. \
                         Investigate and delete manually."
                    );
                }
            }
            let _ = std::fs::remove_file(sidecar_dir.join(format!("{basename}.json")));
        }
    }
}

async fn submit_one(
    client: &Client,
    ctx: &SubmitContext,
    msg_bytes: &[u8],
    sidecar: &SidecarState,
) -> SubmitOutcome {
    // ---- Envelope sanity check (Bug #4) -----------------------------
    // Reject sidecars with an empty envelope BEFORE touching the wire.
    // A default-constructed SidecarState (e.g. a hand-created file, or
    // one left over from Phase D testing before jmapqueue was writing
    // real envelopes) parses as `from: ""`, `to: []` — Fastmail rejects
    // those with a nested-JSON-pointer SetError that pre-Bug-#3 hard-
    // failed to parse and pre-Bug-#1 didn't even reach the submission
    // endpoint. Cheaper and more accurate to fail Permanent locally.
    if let Some(reason) = envelope_validation_error(&sidecar.envelope) {
        return SubmitOutcome::Permanent(reason);
    }

    // ---- Parse send_at if present -----------------------------------
    let send_at_dt = match sidecar.send_at.as_deref() {
        None => None,
        Some(s) => match DateTime::parse_from_rfc3339(s) {
            Ok(dt) => Some(dt.with_timezone(&Utc)),
            Err(e) => {
                return SubmitOutcome::Permanent(format!(
                    "sidecar send_at {s:?} not a valid RFC3339 timestamp: {e}"
                ));
            }
        },
    };

    // ---- Blob upload -------------------------------------------------
    let upload = match client
        .upload(Some(&ctx.account_id), msg_bytes.to_vec(), Some("message/rfc822"))
        .await
    {
        Ok(u) => u,
        Err(e) => return SubmitOutcome::from_jmap_error(e),
    };
    let blob_id = upload.blob_id().to_string();
    debug!("uploaded blob {blob_id}");

    // ---- Chained Email/import + EmailSubmission/set ------------------
    let mut req = client.build();
    restrict_using(&mut req);

    // Email/import: place in Drafts with $draft keyword.
    let email_create_id = {
        let import = req.import_email().account_id(&ctx.account_id).email(&blob_id);
        import.mailbox_ids([&ctx.drafts_id]);
        import.keywords(["$draft"]);
        import.create_id()
    };

    // EmailSubmission/set: reference the just-imported email by
    // creation-id (`#i0`); set identity + envelope + optional sendAt.
    let submission_create_id = {
        let set_req = req.set_email_submission();
        let create = set_req.create();
        create.email_id(format!("#{}", email_create_id));
        create.identity_id(&ctx.identity_id);
        create.envelope(
            sidecar.envelope.from.as_str(),
            sidecar
                .envelope
                .to
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>(),
        );
        if let Some(dt) = send_at_dt {
            create.send_at(dt);
        }
        create.create_id().unwrap_or_else(|| "c0".to_string())
    };

    // onSuccessUpdateEmail: file into Sent (or Scheduled folder),
    // clear the Drafts placement, drop $draft, add $seen.
    let file_target = ctx.file_target_id(send_at_dt.is_some()).to_string();
    {
        let set_req = req.set_email_submission();
        let args = set_req.arguments();
        let update = args.on_success_update_email(&submission_create_id);
        update.mailbox_id(&ctx.drafts_id, false);
        update.mailbox_id(&file_target, true);
        update.keyword("$draft", false);
        update.keyword("$seen", true);
    }

    // ---- Send + interpret -------------------------------------------
    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => return SubmitOutcome::from_jmap_error(e),
    };

    for tagged in resp.unwrap_method_responses() {
        match tagged.unwrap_method_response() {
            MethodResponse::Error(me) => return SubmitOutcome::from_method_error(&me),
            MethodResponse::ImportEmail(mut import_resp) => {
                // EmailImportResponse doesn't expose the same helpers as
                // SetResponse, so we drive it via its two id accessors.
                // `.created(id)` returns the SetError as an Err when the
                // id is in the not_created bucket.
                let not_created: Vec<String> = import_resp
                    .not_created_ids()
                    .map(|it| it.cloned().collect())
                    .unwrap_or_default();
                if let Some(first) = not_created.first().cloned() {
                    return match import_resp.created(&first) {
                        Err(e) => SubmitOutcome::Permanent(format!("Email/import: {e}")),
                        Ok(_) => SubmitOutcome::Permanent(format!(
                            "Email/import not_created: {not_created:?}"
                        )),
                    };
                }
                let created: Vec<String> = import_resp
                    .created_ids()
                    .map(|it| it.cloned().collect())
                    .unwrap_or_default();
                if created.is_empty() {
                    return SubmitOutcome::Permanent(
                        "Email/import returned no created entry".into(),
                    );
                }
            }
            MethodResponse::SetEmailSubmission(set_resp) => {
                if let Err(e) = set_resp.unwrap_create_errors() {
                    return SubmitOutcome::Permanent(format!("EmailSubmission/set: {e}"));
                }
                if !set_resp.has_created() {
                    return SubmitOutcome::Permanent(
                        "EmailSubmission/set returned no created entry".into(),
                    );
                }
            }
            // Other method responses in the chain aren't errors.
            _ => {}
        }
    }

    SubmitOutcome::Success
}

// ---------------------------------------------------------------------------
// Event handling + drag-back with paired-rename
// ---------------------------------------------------------------------------

/// A `RenameFrom` we've seen but whose paired `RenameTo` hasn't arrived
/// yet. Sits in the pending-rename cache for RENAME_PAIR_WINDOW.
struct PendingRename {
    side: OutboxSide,
    seen_at: Instant,
}

fn handle_event(
    acct_name: &str,
    outbox: &Path,
    sidecar_dir: &Path,
    known_failures: &mut HashSet<String>,
    pending_renames: &mut HashMap<String, PendingRename>,
    retry_queue: &mut RetrySchedule,
    ev: OutboxEvent,
) {
    let filename = match ev.path.file_name().and_then(|s| s.to_str()) {
        Some(n) => n.to_string(),
        None => return,
    };
    let basename = strip_flags(&filename);

    if filename.starts_with('.') {
        return;
    }

    match (ev.side, ev.kind, ev.subdir.as_str()) {
        // ---- Outbox side ---------------------------------------------
        (OutboxSide::Outbox, RawEventKind::CreateOrRenameTo, _) => {
            // Was this the To half of a Failed→Outbox drag-back?
            if let Some(pending) = pending_renames.remove(&basename) {
                if matches!(pending.side, OutboxSide::Failed) {
                    // Paired: perform atomic Family-2 header strip on
                    // the Outbox file, then enqueue.
                    known_failures.remove(&basename);
                    if let Err(e) = strip_family2_headers_in_place(&ev.path) {
                        warn!(
                            "[{acct_name}/submit] drag-back {basename}: header strip: {e:#}"
                        );
                    }
                    // Reset the sidecar's retry state — user says try again.
                    reset_sidecar_for_retry(sidecar_dir, &basename);
                    info!("[{acct_name}/submit] drag-back Failed→Outbox: {basename}");
                    retry_queue.schedule(basename, Instant::now());
                    return;
                }
                // The pending was from the Outbox side — self-rename
                // (rare; MUA reflow). Treat as fresh queue.
            }
            info!("[{acct_name}/submit] queued: {basename}");
            retry_queue.schedule(basename, Instant::now());
        }
        (OutboxSide::Outbox, RawEventKind::Remove, _) => {
            debug!("[{acct_name}/submit] outbox unlinked: {basename}");
            retry_queue.remove(&basename);
        }
        (OutboxSide::Outbox, RawEventKind::RenameFrom, _) => {
            // File is leaving Outbox — could be a move to Failed (our
            // own action, or a user relocate). Cache and wait.
            pending_renames.insert(
                basename.clone(),
                PendingRename {
                    side: OutboxSide::Outbox,
                    seen_at: Instant::now(),
                },
            );
        }

        // ---- Failed side ---------------------------------------------
        (OutboxSide::Failed, RawEventKind::CreateOrRenameTo, _) => {
            known_failures.insert(basename.clone());
            retry_queue.remove(&basename);
            info!("[{acct_name}/submit] moved to Failed/: {basename}");
        }
        (OutboxSide::Failed, RawEventKind::Remove, _) => {
            if known_failures.remove(&basename) {
                debug!("[{acct_name}/submit] Failed/{basename} unlinked");
            }
        }
        (OutboxSide::Failed, RawEventKind::RenameFrom, _) => {
            // Drag-back is starting: file is leaving Failed. Wait for
            // the paired RenameTo on Outbox.
            pending_renames.insert(
                basename.clone(),
                PendingRename {
                    side: OutboxSide::Failed,
                    seen_at: Instant::now(),
                },
            );
        }
    }

    let _ = outbox;
}

/// Discard rename-from cache entries whose paired rename-to never
/// arrived within RENAME_PAIR_WINDOW. A stale entry means the file
/// really did just leave that side (delete, external mv to elsewhere).
fn expire_pending_renames(
    pending_renames: &mut HashMap<String, PendingRename>,
    known_failures: &mut HashSet<String>,
    _acct_name: &str,
) {
    let now = Instant::now();
    let expired: Vec<String> = pending_renames
        .iter()
        .filter(|(_, p)| now.duration_since(p.seen_at) > RENAME_PAIR_WINDOW)
        .map(|(k, _)| k.clone())
        .collect();
    for name in expired {
        if let Some(pending) = pending_renames.remove(&name) {
            if matches!(pending.side, OutboxSide::Failed) {
                // Truly left Failed. Drop from the tracked set.
                known_failures.remove(&name);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Family-2 headers: inject on move-to-Failed, strip on drag-back
// ---------------------------------------------------------------------------

/// Rewrite the on-disk file with all `X-JMAP-Failure`, `X-JMAP-Failed-At`,
/// and `X-JMAP-Ulid` headers removed. Atomic: write to `<path>.tmp` in
/// the same directory, `fsync`, rename over the original.
fn strip_family2_headers_in_place(path: &Path) -> Result<()> {
    let bytes = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let stripped = strip_family2_headers(&bytes);
    if stripped == bytes {
        return Ok(());
    }
    let tmp_path = tmp_neighbour(path);
    let mut f = std::fs::File::create(&tmp_path)
        .with_context(|| format!("create {}", tmp_path.display()))?;
    f.write_all(&stripped)?;
    f.sync_all()?;
    drop(f);
    std::fs::rename(&tmp_path, path)
        .with_context(|| format!("rename {} → {}", tmp_path.display(), path.display()))?;
    Ok(())
}

fn tmp_neighbour(path: &Path) -> PathBuf {
    let mut base = path.to_path_buf();
    let name = base
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("message");
    let new_name = format!(".{name}.jmapsyncd-tmp");
    base.set_file_name(new_name);
    base
}

/// Return the message bytes with the three Family-2 header lines removed.
/// RFC 5322-aware (folded continuations are treated as part of the header).
fn strip_family2_headers(bytes: &[u8]) -> Vec<u8> {
    let (header, body) = split_header_body(bytes);
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < header.len() {
        // Find end-of-line
        let line_end = find_line_end(&header[i..]);
        let line_slice = &header[i..i + line_end];
        // How many following folded lines to skip along with this one?
        let mut skip = line_end;
        while i + skip < header.len() {
            let next_start = i + skip;
            if starts_with_folded_continuation(&header[next_start..]) {
                skip += find_line_end(&header[next_start..]);
            } else {
                break;
            }
        }
        if is_family2_header_line(line_slice) {
            // Drop this header (and any folded continuation lines).
        } else {
            out.extend_from_slice(&header[i..i + skip]);
        }
        i += skip;
    }
    out.extend_from_slice(body);
    out
}

/// Prepend the three Family-2 headers to the message (before any
/// existing headers). Returns a fresh Vec<u8> with the augmented bytes.
fn inject_family2_headers(bytes: &[u8], reason: &str, ulid: &str) -> Vec<u8> {
    let now = iso_now();
    // Sanitize the reason into a single header line — collapse any
    // whitespace so we never emit a folded/bogus header value.
    let sanitized_reason: String = reason
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let prefix = format!(
        "X-JMAP-Failure: {sanitized_reason}\r\nX-JMAP-Failed-At: {now}\r\nX-JMAP-Ulid: {ulid}\r\n"
    );
    let mut out = Vec::with_capacity(bytes.len() + prefix.len());
    out.extend_from_slice(prefix.as_bytes());
    out.extend_from_slice(bytes);
    out
}

fn is_family2_header_line(line: &[u8]) -> bool {
    for name in ["X-JMAP-Failure:", "X-JMAP-Failed-At:", "X-JMAP-Ulid:"] {
        if line_starts_with_header_ci(line, name.as_bytes()) {
            return true;
        }
    }
    false
}

/// Case-insensitive header-name prefix match (name includes the colon).
fn line_starts_with_header_ci(line: &[u8], name_with_colon: &[u8]) -> bool {
    if line.len() < name_with_colon.len() {
        return false;
    }
    line[..name_with_colon.len()]
        .iter()
        .zip(name_with_colon.iter())
        .all(|(a, b)| a.eq_ignore_ascii_case(b))
}

fn starts_with_folded_continuation(rest: &[u8]) -> bool {
    matches!(rest.first(), Some(b' ' | b'\t'))
}

fn find_line_end(bytes: &[u8]) -> usize {
    // Advance to (and past) the next LF; if the line ends in CRLF, we
    // still consume just up to (and including) the LF.
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'\n' {
            return i + 1;
        }
    }
    bytes.len()
}

/// Split at the first blank line (CRLFCRLF or LFLF). Returns
/// `(header_including_blank_line, body)`. When there's no blank line
/// (malformed message), treat the whole thing as header, empty body.
fn split_header_body(bytes: &[u8]) -> (&[u8], &[u8]) {
    // Look for \r\n\r\n first, then fall back to \n\n.
    if let Some(pos) = find_subseq(bytes, b"\r\n\r\n") {
        let end = pos + 4;
        return (&bytes[..end], &bytes[end..]);
    }
    if let Some(pos) = find_subseq(bytes, b"\n\n") {
        let end = pos + 2;
        return (&bytes[..end], &bytes[end..]);
    }
    (bytes, &[])
}

fn find_subseq(bytes: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > bytes.len() {
        return None;
    }
    bytes.windows(needle.len()).position(|w| w == needle)
}

// ---------------------------------------------------------------------------
// Move-to-Failed
// ---------------------------------------------------------------------------

/// Atomically move `<src>` → `<failed>/new/<basename>`, injecting the
/// Family-2 diagnostic headers into the payload. Writes to
/// `<failed>/tmp/<basename>` first, fsyncs, then renames.
fn move_to_failed_atomic(
    src: &Path,
    failed_dir: &Path,
    basename: &str,
    reason: &str,
    ulid: &str,
) -> Result<PathBuf> {
    let bytes = std::fs::read(src).with_context(|| format!("read {}", src.display()))?;
    let annotated = inject_family2_headers(&bytes, reason, ulid);

    let tmp_path = failed_dir.join("tmp").join(basename);
    let new_path = failed_dir.join("new").join(basename);
    std::fs::create_dir_all(failed_dir.join("tmp"))?;
    std::fs::create_dir_all(failed_dir.join("new"))?;

    {
        let mut f = std::fs::File::create(&tmp_path)
            .with_context(|| format!("create {}", tmp_path.display()))?;
        f.write_all(&annotated)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp_path, &new_path)
        .with_context(|| format!("rename {} → {}", tmp_path.display(), new_path.display()))?;
    // Unlink the source (in the Outbox side). If the source and dest
    // are the same file (should never happen, guarded by our path
    // construction), the rename above already moved it and there's
    // nothing to unlink.
    if src != new_path {
        let _ = std::fs::remove_file(src);
    }
    Ok(new_path)
}

// ---------------------------------------------------------------------------
// Sidecar state
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, Serialize, Default)]
struct SidecarState {
    envelope: Envelope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    send_at: Option<String>,
    #[serde(default)]
    submitted_at: String,
    #[serde(default)]
    account: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry: Option<RetryState>,
    /// Set at the moment of the first network call for this attempt;
    /// cleared on any terminal outcome. A stale in_flight at startup
    /// = daemon crashed mid-submit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    in_flight: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Default)]
struct Envelope {
    #[serde(default)]
    from: String,
    #[serde(default)]
    to: Vec<String>,
}

/// Pre-submit envelope sanity check. Returns `Some(reason)` if the
/// envelope is missing addresses SMTP requires (a sender and at least
/// one non-blank recipient); `None` if the envelope is submittable.
/// Whitespace-only strings count as absent so that a hand-authored
/// sidecar with `"   "` in a field can't sneak past this check.
fn envelope_validation_error(env: &Envelope) -> Option<String> {
    if env.from.trim().is_empty() {
        return Some("sidecar envelope missing `from`".to_string());
    }
    if env.to.iter().all(|s| s.trim().is_empty()) {
        return Some("sidecar envelope missing `to`".to_string());
    }
    None
}

/// What `submit_one` should do when Fastmail rejects `Email/import`
/// with `alreadyExists` — i.e. Blob/upload deduplicated the message
/// body against a message that's already on the server. The right
/// answer depends on where the pre-existing copy currently lives and
/// whether a pending `EmailSubmission` already targets it.
///
/// See Bug #8 in `jmapsyncd-phase-e-bug-catalog.md`. Standards
/// reference: RFC 8621 §4.9 (Email/import), §7 (EmailSubmission).
#[derive(Debug, PartialEq, Eq)]
enum ReconcileAction {
    /// Email is already in the account's Sent mailbox — the server
    /// delivered it. Nothing more to do on this side; the sync loop
    /// will pull the Sent copy down into the local Sent maildir
    /// naturally, so aerc sees it without any special handling.
    AlreadyDone,
    /// Email is still in Drafts and at least one `EmailSubmission` is
    /// pending. The user's drag-back expressed "make server state
    /// match my sidecar," so cancel every pending submission (allowed
    /// while `undoStatus == pending` — RFC 8621 §7.2) and create a
    /// fresh submission with the sidecar's current `sendAt`. This
    /// correctly handles both "same schedule, just retry" and "I
    /// dragged this back to reschedule" without needing a local cache.
    CancelAndResubmit {
        destroy_ids: Vec<String>,
        send_at: Option<DateTime<Utc>>,
    },
    /// Email is still in Drafts but no pending submission exists —
    /// user wants to send now (or on the sidecar's schedule). Create a
    /// fresh submission referencing the existing emailId directly.
    CreateSubmission {
        send_at: Option<DateTime<Utc>>,
    },
}

/// Decide what to do about a message the server already has. Pure
/// over `(server state, sidecar intent)`; the call site fetches state
/// via a chained `Email/get` + `EmailSubmission/query` and translates
/// the returned `ReconcileAction` into another JMAP round-trip.
///
/// The `AlreadyDone` branch takes priority over `CancelAndResubmit`:
/// if the email appears in Sent at all (even alongside Drafts, which
/// shouldn't happen but can under bizarre client concurrency) we
/// treat the send as having succeeded. Better to no-op and let the
/// user check than to cancel a finalized send and try to re-send it.
fn reconcile_action(
    email_mailbox_ids: &[String],
    pending_submission_ids: &[String],
    sent_id: &str,
    sidecar_send_at: Option<DateTime<Utc>>,
) -> ReconcileAction {
    if email_mailbox_ids.iter().any(|m| m == sent_id) {
        return ReconcileAction::AlreadyDone;
    }
    if !pending_submission_ids.is_empty() {
        return ReconcileAction::CancelAndResubmit {
            destroy_ids: pending_submission_ids.to_vec(),
            send_at: sidecar_send_at,
        };
    }
    ReconcileAction::CreateSubmission {
        send_at: sidecar_send_at,
    }
}

#[derive(Debug, Deserialize, Serialize, Default)]
struct RetryState {
    #[serde(default)]
    attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    next_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_error: Option<String>,
}

impl RetryState {
    fn next_at_epoch_secs(&self) -> Option<u64> {
        self.next_at
            .as_deref()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc).timestamp().max(0) as u64)
    }
}

impl SidecarState {
    fn load(sidecar_dir: &Path, basename: &str) -> Self {
        let path = sidecar_dir.join(format!("{basename}.json"));
        let Ok(bytes) = std::fs::read(&path) else {
            return SidecarState::default();
        };
        serde_json::from_slice(&bytes).unwrap_or_else(|e| {
            warn!("sidecar {} malformed ({e}); treating as empty", path.display());
            SidecarState::default()
        })
    }

    fn save(&self, sidecar_dir: &Path, basename: &str) -> Result<()> {
        let final_path = sidecar_dir.join(format!("{basename}.json"));
        let tmp_path = sidecar_dir.join(format!(".{basename}.json.tmp"));
        let bytes = serde_json::to_vec_pretty(self)?;
        {
            let mut f = std::fs::File::create(&tmp_path)?;
            f.write_all(&bytes)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp_path, &final_path)?;
        Ok(())
    }

    fn format_log(&self) -> String {
        let retry_note = self
            .retry
            .as_ref()
            .filter(|r| r.attempts > 0)
            .map(|r| {
                format!(
                    " retry attempts={} next_at={}",
                    r.attempts,
                    r.next_at.as_deref().unwrap_or("<unset>")
                )
            })
            .unwrap_or_default();
        let send_at_note = self
            .send_at
            .as_ref()
            .map(|s| format!(" send_at={s}"))
            .unwrap_or_default();
        let in_flight_note = self
            .in_flight
            .as_ref()
            .map(|s| format!(" in_flight={s}"))
            .unwrap_or_default();
        format!(
            "from={} to={:?}{send_at_note}{retry_note}{in_flight_note}",
            self.envelope.from, self.envelope.to
        )
    }
}

fn reset_sidecar_for_retry(sidecar_dir: &Path, basename: &str) {
    let mut sc = SidecarState::load(sidecar_dir, basename);
    sc.retry = None;
    sc.in_flight = None;
    if let Err(e) = sc.save(sidecar_dir, basename) {
        warn!("reset sidecar for {basename}: {e:#}");
    }
}

fn warn_on_stale_inflight(sidecar_dir: &Path, acct_name: &str) {
    let Ok(rd) = std::fs::read_dir(sidecar_dir) else {
        return;
    };
    let now = SystemTime::now();
    for entry in rd.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if !name.ends_with(".json") || name.starts_with('.') {
            continue;
        }
        let basename = name.trim_end_matches(".json");
        let sc = SidecarState::load(sidecar_dir, basename);
        let Some(inflight_iso) = sc.in_flight.as_deref() else {
            continue;
        };
        let stale = DateTime::parse_from_rfc3339(inflight_iso)
            .ok()
            .and_then(|dt| {
                let inflight_sys = UNIX_EPOCH + Duration::from_secs(dt.timestamp().max(0) as u64);
                now.duration_since(inflight_sys).ok()
            })
            .map(|d| d >= STARTUP_INFLIGHT_STALENESS)
            .unwrap_or(true);
        if stale {
            warn!(
                "[{acct_name}/submit] {basename}: possible mid-submit crash (in_flight since {inflight_iso}). \
                 Will retry — RECIPIENT MAY SEE A DUPLICATE. \
                 Check Fastmail Sent + Scheduled + submission log before letting the retry proceed."
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Notify → tokio bridge (per-file events)
// ---------------------------------------------------------------------------

struct OutboxWatchRx {
    _watcher: RecommendedWatcher,
    rx: mpsc::Receiver<OutboxEvent>,
}

#[derive(Debug)]
struct OutboxEvent {
    side: OutboxSide,
    path: PathBuf,
    subdir: String,
    kind: RawEventKind,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
enum RawEventKind {
    CreateOrRenameTo,
    Remove,
    RenameFrom,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
enum OutboxSide {
    Outbox,
    Failed,
}

fn spawn_outbox_watch(
    outbox: &Path,
    failed: &Path,
    debounce: Duration,
    cancel: CancellationToken,
) -> Result<OutboxWatchRx> {
    let (raw_tx, mut raw_rx) = mpsc::channel::<OutboxEvent>(256);
    let (deb_tx, deb_rx) = mpsc::channel::<OutboxEvent>(64);

    let outbox_ab = outbox.canonicalize().unwrap_or_else(|_| outbox.to_path_buf());
    let failed_ab = failed.canonicalize().unwrap_or_else(|_| failed.to_path_buf());
    let outbox_owned = outbox_ab.clone();
    let failed_owned = failed_ab.clone();

    let mut watcher: RecommendedWatcher = notify::recommended_watcher(
        move |res: notify::Result<notify::Event>| {
            let Ok(event) = res else {
                return;
            };
            let raw_kind = classify_event(&event.kind);
            let Some(raw_kind) = raw_kind else {
                return;
            };
            for path in event.paths.iter() {
                let (side, subdir) = match classify_path(path, &outbox_owned, &failed_owned) {
                    Some(x) => x,
                    None => continue,
                };
                let ev = OutboxEvent {
                    side,
                    path: path.clone(),
                    subdir,
                    kind: raw_kind,
                };
                let _ = raw_tx.blocking_send(ev);
            }
        },
    )?;
    watcher
        .watch(outbox, RecursiveMode::Recursive)
        .with_context(|| format!("watch {}", outbox.display()))?;
    watcher
        .watch(failed, RecursiveMode::Recursive)
        .with_context(|| format!("watch {}", failed.display()))?;

    // Debounce identical (side, basename, kind) events within `debounce`.
    spawn_local(async move {
        let mut last: HashMap<(OutboxSide, String, RawEventKind), Instant> = HashMap::new();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                ev = raw_rx.recv() => {
                    let Some(ev) = ev else { return };
                    let key = (
                        ev.side,
                        ev.path
                            .file_name()
                            .and_then(|s| s.to_str())
                            .unwrap_or("")
                            .to_string(),
                        ev.kind,
                    );
                    let now = Instant::now();
                    if let Some(prev) = last.get(&key) {
                        if now.duration_since(*prev) < debounce {
                            continue;
                        }
                    }
                    last.insert(key, now);
                    if deb_tx.send(ev).await.is_err() {
                        return;
                    }
                }
            }
        }
    });

    Ok(OutboxWatchRx {
        _watcher: watcher,
        rx: deb_rx,
    })
}

fn classify_event(kind: &EventKind) -> Option<RawEventKind> {
    match kind {
        EventKind::Create(_) => Some(RawEventKind::CreateOrRenameTo),
        EventKind::Remove(_) => Some(RawEventKind::Remove),
        EventKind::Modify(ModifyKind::Name(mode)) => match mode {
            RenameMode::From => Some(RawEventKind::RenameFrom),
            RenameMode::To => Some(RawEventKind::CreateOrRenameTo),
            // "Both" and "Any" carry both source + dest paths in
            // event.paths; treat as CreateOrRenameTo so the destination
            // side of the rename triggers a queue action. The source
            // path will be filtered out by classify_path anyway (it's
            // no longer present on disk).
            _ => Some(RawEventKind::CreateOrRenameTo),
        },
        _ => None,
    }
}

fn classify_path(
    path: &Path,
    outbox_ab: &Path,
    failed_ab: &Path,
) -> Option<(OutboxSide, String)> {
    let (side, base) = if path.starts_with(outbox_ab) {
        (OutboxSide::Outbox, outbox_ab)
    } else if path.starts_with(failed_ab) {
        (OutboxSide::Failed, failed_ab)
    } else {
        return None;
    };
    let rel = path.strip_prefix(base).ok()?;
    let subdir = rel.iter().next()?.to_str()?.to_string();
    if !matches!(subdir.as_str(), "new" | "cur") {
        return None;
    }
    if rel.components().count() < 2 {
        return None;
    }
    Some((side, subdir))
}

// ---------------------------------------------------------------------------
// Filesystem helpers
// ---------------------------------------------------------------------------

fn locate_and_read_outbox_message(outbox: &Path, basename: &str) -> Result<(PathBuf, Vec<u8>)> {
    // Try new/ first (jmapqueue's landing site), then cur/ (post-MUA-move
    // variants — with any :2,flags suffix).
    for sub in ["new", "cur"] {
        let dir = outbox.join(sub);
        // Fast path: exact-name match
        let exact = dir.join(basename);
        if exact.is_file() {
            let bytes = std::fs::read(&exact)?;
            return Ok((exact, bytes));
        }
        // Slow path: cur/ files carry :2,flags suffix; scan for prefix.
        let prefix = format!("{basename}:2,");
        if let Ok(rd) = std::fs::read_dir(&dir) {
            for entry in rd.flatten() {
                let path = entry.path();
                let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
                    continue;
                };
                if name.starts_with(&prefix) {
                    let bytes = std::fs::read(&path)?;
                    return Ok((path, bytes));
                }
            }
        }
    }
    Err(anyhow!("no file matching {basename} in Outbox/{{new,cur}}"))
}

fn scan_maildir_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for sub in ["new", "cur"] {
        let dir = root.join(sub);
        let Ok(rd) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in rd.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            if name.starts_with('.') {
                continue;
            }
            if path.is_file() {
                out.push(path);
            }
        }
    }
    out
}

fn scan_basenames(root: &Path) -> HashSet<String> {
    scan_maildir_files(root)
        .iter()
        .filter_map(|p| p.file_name().and_then(|s| s.to_str()))
        .map(strip_flags)
        .collect()
}

fn filename_of(p: &Path) -> String {
    p.file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_string()
}

fn strip_flags(name: &str) -> String {
    match name.rsplit_once(":2,") {
        Some((base, _)) => base.to_string(),
        None => name.to_string(),
    }
}

fn sidecar_dir_for(account: &str) -> Result<PathBuf> {
    let base = dirs::state_dir()
        .context("no XDG_STATE_HOME (dirs::state_dir returned None)")?;
    Ok(base.join("jmapsyncd").join(account).join("outbox-meta"))
}

// ---------------------------------------------------------------------------
// Time helpers
// ---------------------------------------------------------------------------

fn iso_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn iso_from_epoch_secs(secs: u64) -> String {
    DateTime::<Utc>::from_timestamp(secs as i64, 0)
        .unwrap_or_else(Utc::now)
        .to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn epoch_secs_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ---- strip_flags -----------------------------------------------
    #[test]
    fn strip_flags_variants() {
        assert_eq!(strip_flags("foo.bar.host"), "foo.bar.host");
        assert_eq!(strip_flags("foo.bar.host:2,"), "foo.bar.host");
        assert_eq!(strip_flags("foo.bar.host:2,S"), "foo.bar.host");
        assert_eq!(strip_flags("foo.bar.host:2,SR"), "foo.bar.host");
    }

    // ---- scan_maildir_files ----------------------------------------
    #[test]
    fn scan_maildir_files_skips_tmp_and_dotfiles() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("Outbox");
        for sub in ["tmp", "new", "cur"] {
            std::fs::create_dir_all(root.join(sub)).unwrap();
        }
        std::fs::write(root.join("new/message1"), b"m1").unwrap();
        std::fs::write(root.join("cur/message2:2,S"), b"m2").unwrap();
        std::fs::write(root.join("tmp/mid-write"), b"partial").unwrap();
        std::fs::write(root.join("new/.dotfile"), b"hidden").unwrap();

        let found: HashSet<String> = scan_maildir_files(&root)
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap().to_string())
            .collect();
        assert_eq!(
            found,
            ["message1", "message2:2,S"]
                .iter()
                .map(|s| s.to_string())
                .collect()
        );
    }

    // ---- classify_path ---------------------------------------------
    #[test]
    fn classify_path_recognizes_outbox_and_failed() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("Outbox/new")).unwrap();
        std::fs::create_dir_all(tmp.path().join("Failed/new")).unwrap();
        let outbox = tmp.path().join("Outbox").canonicalize().unwrap();
        let failed = tmp.path().join("Failed").canonicalize().unwrap();

        let ob_file = outbox.join("new/foo");
        std::fs::write(&ob_file, b"").unwrap();
        let (side, subdir) = classify_path(&ob_file, &outbox, &failed).unwrap();
        assert_eq!(side, OutboxSide::Outbox);
        assert_eq!(subdir, "new");

        let f_file = failed.join("new/bar");
        std::fs::write(&f_file, b"").unwrap();
        let (side, subdir) = classify_path(&f_file, &outbox, &failed).unwrap();
        assert_eq!(side, OutboxSide::Failed);
        assert_eq!(subdir, "new");
    }

    #[test]
    fn classify_path_rejects_tmp_events() {
        let tmp = tempfile::tempdir().unwrap();
        let outbox = tmp.path().join("Outbox");
        let failed = tmp.path().join("Failed");
        std::fs::create_dir_all(outbox.join("tmp")).unwrap();
        let tmp_file = outbox.join("tmp/mid-write");
        std::fs::write(&tmp_file, b"").unwrap();
        assert!(classify_path(&tmp_file, &outbox, &failed).is_none());
    }

    // ---- RetrySchedule --------------------------------------------
    #[test]
    fn retry_schedule_basic() {
        let mut q = RetrySchedule::new();
        let now = Instant::now();
        q.schedule("a".into(), now);
        q.schedule("b".into(), now + Duration::from_secs(1));
        // Reschedule 'a' further out.
        q.schedule("a".into(), now + Duration::from_secs(2));
        let due = q.drain_due();
        assert!(due.is_empty(), "nothing due at t=0 anymore");
        // Fast-forward past b's due time by scheduling something at
        // an earlier instant, then drain.
        q.schedule("c".into(), now.checked_sub(Duration::from_secs(5)).unwrap_or(now));
        let due = q.drain_due();
        assert!(due.contains(&"c".to_string()));
    }

    #[test]
    fn retry_schedule_remove_clears_index_and_bucket() {
        let mut q = RetrySchedule::new();
        let when = Instant::now() + Duration::from_secs(60);
        q.schedule("x".into(), when);
        q.remove("x");
        assert!(!q.index.contains_key("x"));
        assert!(q.schedule.get(&when).is_none());
    }

    // ---- backoff_delay ---------------------------------------------
    #[test]
    fn backoff_delay_grows_then_caps() {
        let a1 = backoff_delay(1, 30, 7200);
        // ~30s ±20%: 24..=36
        assert!(a1.as_secs_f64() >= 24.0 && a1.as_secs_f64() <= 36.0);
        let a2 = backoff_delay(2, 30, 7200);
        // ~60s ±20%: 48..=72
        assert!(a2.as_secs_f64() >= 48.0 && a2.as_secs_f64() <= 72.0);
        let capped = backoff_delay(20, 30, 7200);
        // Cap at 7200; jitter still applies within ±20%.
        assert!(capped.as_secs_f64() <= 7200.0 * 1.201);
        assert!(capped.as_secs_f64() >= 7200.0 * 0.799);
    }

    // ---- header inject / strip ------------------------------------
    fn body_bytes() -> &'static [u8] {
        b"To: dest@example\r\nFrom: me@example\r\nSubject: hi\r\n\r\nbody line\r\n"
    }

    #[test]
    fn inject_family2_prepends_three_headers() {
        let out = inject_family2_headers(body_bytes(), "notFound", "01H123");
        let s = std::str::from_utf8(&out).unwrap();
        assert!(s.starts_with("X-JMAP-Failure: notFound\r\n"));
        assert!(s.contains("X-JMAP-Failed-At: "));
        assert!(s.contains("X-JMAP-Ulid: 01H123\r\n"));
        assert!(s.ends_with("body line\r\n"));
    }

    #[test]
    fn strip_family2_removes_all_three() {
        let annotated = inject_family2_headers(body_bytes(), "identityId notFound", "01HULID");
        let stripped = strip_family2_headers(&annotated);
        // Round-trip: stripped == original
        assert_eq!(stripped, body_bytes().to_vec());
    }

    #[test]
    fn strip_family2_preserves_case_variants() {
        let msg = b"x-jmap-failure: broke\r\nX-JMAP-Failed-At: today\r\nX-JMAP-ULID: xxx\r\nSubject: hi\r\n\r\nbody";
        let stripped = strip_family2_headers(msg);
        let s = std::str::from_utf8(&stripped).unwrap();
        assert!(!s.contains("X-JMAP-Failure"));
        assert!(!s.contains("x-jmap-failure"));
        assert!(!s.contains("X-JMAP-Failed-At"));
        assert!(!s.contains("X-JMAP-ULID"));
        assert!(s.starts_with("Subject: hi\r\n"));
    }

    #[test]
    fn strip_family2_leaves_body_alone() {
        let msg = b"Subject: hi\r\n\r\nX-JMAP-Failure: this-is-in-the-body\r\n";
        let stripped = strip_family2_headers(msg);
        assert_eq!(stripped, msg.to_vec());
    }

    #[test]
    fn strip_family2_handles_folded_continuation() {
        let msg = b"X-JMAP-Failure: reason\r\n continued-fold\r\nSubject: hi\r\n\r\nbody";
        let stripped = strip_family2_headers(msg);
        let s = std::str::from_utf8(&stripped).unwrap();
        assert!(!s.contains("X-JMAP-Failure"));
        assert!(!s.contains("continued-fold"));
        assert!(s.starts_with("Subject: hi\r\n"));
    }

    #[test]
    fn inject_family2_sanitizes_multiline_reason() {
        // A reason with embedded newlines must not break the header block.
        let out = inject_family2_headers(body_bytes(), "line1\r\nline2\tblah", "01H");
        let s = std::str::from_utf8(&out).unwrap();
        let header_line = s.lines().next().unwrap();
        assert!(header_line.starts_with("X-JMAP-Failure: line1 line2 blah"));
    }

    // ---- SidecarState r/w -----------------------------------------
    #[test]
    fn sidecar_roundtrip_with_in_flight() {
        let tmp = tempfile::tempdir().unwrap();
        let mut sc = SidecarState {
            envelope: Envelope {
                from: "a@b".into(),
                to: vec!["c@d".into()],
            },
            send_at: Some("2026-08-01T09:00:00Z".into()),
            submitted_at: "2026-08-01T08:59:00Z".into(),
            account: "personal".into(),
            retry: None,
            in_flight: None,
        };
        sc.save(tmp.path(), "m1").unwrap();

        let loaded = SidecarState::load(tmp.path(), "m1");
        assert_eq!(loaded.envelope.from, "a@b");
        assert_eq!(loaded.envelope.to, vec!["c@d".to_string()]);
        assert_eq!(loaded.send_at.as_deref(), Some("2026-08-01T09:00:00Z"));
        assert!(loaded.in_flight.is_none());

        sc.in_flight = Some("2026-08-01T09:05:00Z".into());
        sc.save(tmp.path(), "m1").unwrap();
        let reloaded = SidecarState::load(tmp.path(), "m1");
        assert_eq!(
            reloaded.in_flight.as_deref(),
            Some("2026-08-01T09:05:00Z")
        );
    }

    #[test]
    fn sidecar_missing_file_returns_default() {
        let tmp = tempfile::tempdir().unwrap();
        let sc = SidecarState::load(tmp.path(), "does-not-exist");
        assert_eq!(sc.envelope.from, "");
        assert!(sc.envelope.to.is_empty());
    }

    // ---- SubmitOutcome classification -----------------------------
    #[test]
    fn method_error_server_unavailable_is_transient() {
        let me = MethodError {
            p_type: MethodErrorType::ServerUnavailable,
        };
        assert!(matches!(
            SubmitOutcome::from_method_error(&me),
            SubmitOutcome::Transient(_)
        ));
    }

    #[test]
    fn method_error_forbidden_is_permanent() {
        let me = MethodError {
            p_type: MethodErrorType::Forbidden,
        };
        assert!(matches!(
            SubmitOutcome::from_method_error(&me),
            SubmitOutcome::Permanent(_)
        ));
    }

    #[test]
    fn method_error_account_read_only_is_permanent() {
        let me = MethodError {
            p_type: MethodErrorType::AccountReadOnly,
        };
        assert!(matches!(
            SubmitOutcome::from_method_error(&me),
            SubmitOutcome::Permanent(_)
        ));
    }

    // -----------------------------------------------------------------
    // Bug #4: envelope validation must happen BEFORE the Blob upload,
    // so a default-empty SidecarState (`from: ""`, `to: []`) never
    // reaches the wire. Pre-fix the daemon uploaded a blob + kicked
    // off an Email/import that Fastmail then rejected, wasting a
    // round-trip per retry tick.
    // -----------------------------------------------------------------

    #[test]
    fn envelope_valid_returns_none() {
        let env = Envelope {
            from: "a@b".into(),
            to: vec!["c@d".into()],
        };
        assert!(super::envelope_validation_error(&env).is_none());
    }

    #[test]
    fn envelope_empty_from_returns_reason() {
        let env = Envelope {
            from: "".into(),
            to: vec!["c@d".into()],
        };
        let reason = super::envelope_validation_error(&env).expect("must reject");
        assert!(reason.contains("from"), "reason should name `from`: {reason}");
    }

    #[test]
    fn envelope_whitespace_from_returns_reason() {
        let env = Envelope {
            from: "   \t\n".into(),
            to: vec!["c@d".into()],
        };
        let reason = super::envelope_validation_error(&env).expect("must reject");
        assert!(reason.contains("from"));
    }

    #[test]
    fn envelope_empty_to_returns_reason() {
        let env = Envelope {
            from: "a@b".into(),
            to: vec![],
        };
        let reason = super::envelope_validation_error(&env).expect("must reject");
        assert!(reason.contains("to"), "reason should name `to`: {reason}");
    }

    #[test]
    fn envelope_all_whitespace_to_returns_reason() {
        // A `to` list containing only blank strings is functionally
        // empty — SMTP would refuse it. Treat it as absent.
        let env = Envelope {
            from: "a@b".into(),
            to: vec!["".into(), "  ".into(), "\t".into()],
        };
        let reason = super::envelope_validation_error(&env).expect("must reject");
        assert!(reason.contains("to"));
    }

    #[test]
    fn envelope_partial_whitespace_to_passes() {
        // At least one recipient is a real address — that's a legal
        // envelope. The blank entries are somebody else's problem
        // (probably a UX gap in the composer), not ours to reject.
        let env = Envelope {
            from: "a@b".into(),
            to: vec!["".into(), "c@d".into()],
        };
        assert!(super::envelope_validation_error(&env).is_none());
    }

    #[test]
    fn envelope_default_construction_is_rejected() {
        // The exact shape that leaked through pre-fix: a serde
        // default-constructed SidecarState — `from: ""`, `to: []`.
        // Locks down that this specific starting state can never be
        // treated as submittable again.
        let env = Envelope::default();
        assert!(super::envelope_validation_error(&env).is_some());
    }

    // -----------------------------------------------------------------
    // Bug #8: reconcile_action must translate (server state, sidecar
    // intent) into the RFC-compliant next step on Email/import
    // `alreadyExists`. Locks down every branch so future refactors
    // can't silently regress into "always resubmit" or "always no-op".
    // -----------------------------------------------------------------

    fn sent() -> String { "MSENT01".to_string() }
    fn drafts() -> String { "MDRFT01".to_string() }

    #[test]
    fn reconcile_email_in_sent_is_already_done() {
        // Fastmail moved the message to Sent — delivery happened.
        // Nothing to submit. Sync loop pulls the Sent copy down.
        let action = super::reconcile_action(
            &[sent()],
            &["ES-pending".into()],  // even ignored if a stale query races
            &sent(),
            None,
        );
        assert_eq!(action, super::ReconcileAction::AlreadyDone);
    }

    #[test]
    fn reconcile_email_in_both_sent_and_drafts_still_already_done() {
        // Edge case: mailbox membership shows both. AlreadyDone wins;
        // finalized send beats "there's a pending draft" ambiguity.
        let action = super::reconcile_action(
            &[drafts(), sent()],
            &[],
            &sent(),
            None,
        );
        assert_eq!(action, super::ReconcileAction::AlreadyDone);
    }

    #[test]
    fn reconcile_drafts_with_pending_cancels_and_resubmits() {
        // Classic drag-back-to-reschedule: message is queued with a
        // sendAt; user wants a new one. Destroy the pending submission
        // and create a fresh one carrying the sidecar's new sendAt.
        let ts = chrono::DateTime::<Utc>::from_timestamp(1_800_000_000, 0);
        let action = super::reconcile_action(
            &[drafts()],
            &["ES-old-1".into(), "ES-old-2".into()],
            &sent(),
            ts,
        );
        assert_eq!(
            action,
            super::ReconcileAction::CancelAndResubmit {
                destroy_ids: vec!["ES-old-1".into(), "ES-old-2".into()],
                send_at: ts,
            }
        );
    }

    #[test]
    fn reconcile_drafts_no_pending_creates_new_submission() {
        // Message sits in Drafts with no submission pointing at it —
        // user's drag-back means "send this." Create submission.
        let action = super::reconcile_action(
            &[drafts()],
            &[],
            &sent(),
            None,
        );
        assert_eq!(
            action,
            super::ReconcileAction::CreateSubmission { send_at: None }
        );
    }

    #[test]
    fn reconcile_passes_send_at_through_unchanged() {
        // Whatever the sidecar says, the action carries it verbatim —
        // reconcile_action doesn't normalize or default sendAt.
        let ts = chrono::DateTime::<Utc>::from_timestamp(1_900_000_000, 0);
        let action = super::reconcile_action(&[drafts()], &[], &sent(), ts);
        assert_eq!(
            action,
            super::ReconcileAction::CreateSubmission { send_at: ts }
        );
    }

    #[test]
    fn reconcile_no_mailbox_membership_still_creates_submission() {
        // Defensive: if Email/get returned no mailboxIds (shouldn't
        // happen — every email lives somewhere — but a partial
        // response could look like this), treat as "not in Sent" and
        // fall through to CreateSubmission rather than AlreadyDone,
        // so we don't silently drop a legit re-submit.
        let action = super::reconcile_action(&[], &[], &sent(), None);
        assert_eq!(
            action,
            super::ReconcileAction::CreateSubmission { send_at: None }
        );
    }
}
