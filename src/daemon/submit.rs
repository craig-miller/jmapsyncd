//! Per-account Outbox watcher — send-path Phase D.
//!
//! Runs alongside the SSE sync loop for any account with an
//! `[accounts.submit]` block. Watches the account's `Outbox/` and
//! `Failed/` Maildirs, enumerates queued messages, and — in this
//! phase — logs what it would do. Phase E replaces the logging with
//! the actual JMAP `EmailSubmission` chain plus header-injection on
//! permanent failures.
//!
//! Design notes:
//!
//! - Files in `Outbox/tmp/` are intentionally ignored (mid-write).
//!   jmapqueue's atomic-rename lands the final file in `Outbox/new/`,
//!   which is what we act on. MUAs that display the file may then
//!   move it to `Outbox/cur/` with a `:2,` flag suffix; we watch
//!   both new/ and cur/ and treat basenames as the identity.
//!
//! - Sidecar meta lives at
//!   `~/.local/state/jmapsyncd/<account>/outbox-meta/<basename>.json`.
//!   The basename in the sidecar name is the filename *without* any
//!   `:2,` flags suffix (jmapqueue only writes to `new/`, so the
//!   sidecar name always matches the raw filename at creation).
//!
//! - Drag-back detection: any base-filename that appeared in `Failed/`
//!   at some point during this loop's lifetime is tracked in
//!   `known_failures`. When that same base-filename shows up in
//!   `Outbox/{new,cur}`, we log a drag-back intent (Phase E will
//!   strip the injected `X-JMAP-*` headers + reset backoff meta).
//!
//!   Known limitation for Phase D: on a `mv Failed/x Outbox/x`,
//!   notify emits two events (Failed-side rename-from, then
//!   Outbox-side rename-to). We currently process them in order,
//!   which removes `x` from `known_failures` on the rename-from
//!   before the rename-to can trigger the drag-back branch — so
//!   the log shows `unlinked (user cleanup)` + `queued` instead
//!   of `drag-back`. Log-only Phase D tolerates this because the
//!   eventual side effect (would-submit) is the same. Phase E's
//!   real drag-back handler needs to strip `X-JMAP-*` headers
//!   atomically, so it will pair rename-from + rename-to events
//!   via a short-lived pending-rename cache indexed by basename.

use crate::config::Account;
use anyhow::{Context, Result};
use log::{debug, info, warn};
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher, event::CreateKind};
use serde::Deserialize;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::spawn_local;
use tokio_util::sync::CancellationToken;

/// Debounce window for coalescing rapid inotify events on the same
/// file (typical case: aerc moves `new/foo` → `cur/foo:2,` right after
/// jmapqueue writes it; two events land within milliseconds).
const EVENT_DEBOUNCE: Duration = Duration::from_millis(100);

/// Log-only per-account outbox watcher. Long-running; returns when
/// `cancel` fires. Bring-up failures (missing mail block, watch setup
/// error) are logged and returned as `Ok(())` — one broken account
/// shouldn't tear down the daemon.
pub async fn run_account_submit_loop(acct: Account, cancel: CancellationToken) {
    if acct.submit.is_none() {
        debug!(
            "[{}] no [accounts.submit] block; submit loop not spawned",
            acct.name
        );
        return;
    }
    let Some(mail_cfg) = acct.mail.as_ref() else {
        warn!(
            "[{}] [accounts.submit] present but no [accounts.mail]; submit loop cannot start",
            acct.name
        );
        return;
    };
    let acct_name = acct.name.clone();
    let mail_root = mail_cfg.path.clone();
    let outbox = mail_root.join("Outbox");
    let failed = mail_root.join("Failed");

    // Ensure both dirs exist so the watcher subscription succeeds. Same
    // maildir layout jmapqueue creates on demand (tmp/new/cur under each).
    for base in [&outbox, &failed] {
        for sub in ["tmp", "new", "cur"] {
            if let Err(e) = std::fs::create_dir_all(base.join(sub)) {
                warn!(
                    "[{acct_name}] cannot create {}: {e}; submit loop cannot start",
                    base.join(sub).display()
                );
                return;
            }
        }
    }

    let sidecar_dir = match sidecar_dir_for(&acct_name) {
        Ok(d) => d,
        Err(e) => {
            warn!("[{acct_name}] no XDG state dir: {e:#}; submit loop cannot start");
            return;
        }
    };
    if let Err(e) = std::fs::create_dir_all(&sidecar_dir) {
        warn!(
            "[{acct_name}] cannot create sidecar dir {}: {e}; submit loop cannot start",
            sidecar_dir.display()
        );
        return;
    }

    // Seed the known-failures set from disk so drag-back detection
    // survives daemon restarts.
    let mut known_failures: HashSet<String> = scan_basenames(&failed);
    info!(
        "[{acct_name}/submit] known failures at startup: {}",
        known_failures.len()
    );

    // Startup scan of Outbox — anything queued while the daemon was
    // down, plus any real drag-backs that happened offline.
    for path in scan_maildir_files(&outbox) {
        let basename = strip_flags(&filename_of(&path));
        let intent = load_intent(&sidecar_dir, &basename);
        if known_failures.contains(&basename) {
            info!(
                "[{acct_name}/submit] startup drag-back: {basename} (would strip X-JMAP-* headers + resubmit) — {intent}"
            );
            known_failures.remove(&basename);
        } else {
            info!("[{acct_name}/submit] startup pending: {basename} — {intent}");
        }
    }

    // Watcher — separate raw event stream per side so we can classify
    // by directory in the debounce loop.
    let watcher_rx = match spawn_outbox_watch(&outbox, &failed, EVENT_DEBOUNCE, cancel.clone()) {
        Ok(rx) => rx,
        Err(e) => {
            warn!(
                "[{acct_name}] outbox watch setup failed: {e:#}; submit loop cannot start"
            );
            return;
        }
    };
    let mut rx = watcher_rx.rx;
    let _watcher_keep_alive = watcher_rx._watcher;

    info!(
        "[{acct_name}/submit] watching {} + {} (log-only in Phase D)",
        outbox.display(),
        failed.display()
    );

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("[{acct_name}/submit] cancel received; loop exiting");
                return;
            }
            maybe_ev = rx.recv() => {
                let Some(ev) = maybe_ev else {
                    info!("[{acct_name}/submit] watcher channel closed; loop exiting");
                    return;
                };
                handle_event(
                    &acct_name,
                    &outbox,
                    &failed,
                    &sidecar_dir,
                    &mut known_failures,
                    ev,
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Event handling
// ---------------------------------------------------------------------------

fn handle_event(
    acct_name: &str,
    outbox: &Path,
    failed: &Path,
    sidecar_dir: &Path,
    known_failures: &mut HashSet<String>,
    ev: OutboxEvent,
) {
    let filename = match ev.path.file_name().and_then(|s| s.to_str()) {
        Some(n) => n.to_string(),
        None => return,
    };
    let basename = strip_flags(&filename);

    // Dot-files: emacs backup residue, editor swap files, etc. tmp/ is
    // already filtered upstream in classify_path.
    if filename.starts_with('.') {
        return;
    }

    match (ev.side, ev.kind_is_create_or_move_to, ev.kind_is_remove) {
        (OutboxSide::Outbox, true, _) => {
            // A file appearing in Outbox/{new,cur} — either a fresh
            // jmapqueue write or a drag-back from Failed/.
            let intent = load_intent(sidecar_dir, &basename);
            if known_failures.remove(&basename) {
                info!(
                    "[{acct_name}/submit] drag-back Failed→Outbox: {basename} \
                     (would strip X-JMAP-* headers + resubmit) — {intent}"
                );
            } else {
                info!("[{acct_name}/submit] queued: {basename} — {intent}");
            }
        }
        (OutboxSide::Outbox, false, true) => {
            // File removed from Outbox — either an MUA :delete on a
            // queued message, or (in Phase E) our own unlink after a
            // successful submission. Log-only for Phase D.
            debug!("[{acct_name}/submit] outbox unlinked: {basename}");
        }
        (OutboxSide::Failed, true, _) => {
            known_failures.insert(basename.clone());
            info!("[{acct_name}/submit] moved to Failed/: {basename}");
        }
        (OutboxSide::Failed, false, true) => {
            if known_failures.remove(&basename) {
                info!("[{acct_name}/submit] Failed/{basename} unlinked (user cleanup)");
            }
        }
        _ => {}
    }
    let _ = (outbox, failed); // silence unused params (kept for future use)
}

// ---------------------------------------------------------------------------
// Sidecar loading + intent formatting
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct Sidecar {
    envelope: Envelope,
    #[serde(default)]
    send_at: Option<String>,
    #[serde(default)]
    account: String,
    #[serde(default)]
    retry: Option<RetryState>,
}

#[derive(Deserialize)]
struct Envelope {
    from: String,
    to: Vec<String>,
}

#[derive(Deserialize)]
struct RetryState {
    #[serde(default)]
    attempts: u32,
    #[serde(default)]
    next_at: Option<String>,
}

/// Human-readable one-liner for logs. Silently returns a placeholder if
/// the sidecar is missing or malformed — a missing sidecar is a bug we
/// want visible in logs, not a reason to skip the event.
fn load_intent(sidecar_dir: &Path, basename: &str) -> String {
    let path = sidecar_dir.join(format!("{basename}.json"));
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(_) => return format!("<no sidecar at {}>", path.display()),
    };
    let sc: Sidecar = match serde_json::from_slice(&bytes) {
        Ok(s) => s,
        Err(e) => return format!("<sidecar {} malformed: {e}>", path.display()),
    };
    let retry_note = sc
        .retry
        .as_ref()
        .filter(|r| r.attempts > 0)
        .map(|r| {
            format!(
                " retry attempt={} next_at={}",
                r.attempts,
                r.next_at.as_deref().unwrap_or("<unset>")
            )
        })
        .unwrap_or_default();
    let send_at_note = sc
        .send_at
        .as_ref()
        .map(|s| format!(" send_at={s}"))
        .unwrap_or_default();
    format!(
        "from={} to={:?}{send_at_note}{retry_note}",
        sc.envelope.from, sc.envelope.to
    )
}

// ---------------------------------------------------------------------------
// notify → tokio bridge (per-file events, not just tick pings)
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
    kind_is_create_or_move_to: bool,
    kind_is_remove: bool,
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
            let (kind_create, kind_remove) = classify_event(&event.kind);
            // Only transitional events (create / remove / rename) advance
            // state. Data/metadata modifies on already-tracked files are
            // ignored — otherwise cp / touch / editor saves cause spurious
            // "moved to Failed/" repeats in the log.
            if !kind_create && !kind_remove {
                return;
            }
            for path in event.paths.iter() {
                let (side, subdir) =
                    match classify_path(path, &outbox_owned, &failed_owned) {
                        Some(x) => x,
                        None => continue,
                    };
                let outbox_event = OutboxEvent {
                    side,
                    path: path.clone(),
                    subdir,
                    kind_is_create_or_move_to: kind_create,
                    kind_is_remove: kind_remove,
                };
                let _ = raw_tx.blocking_send(outbox_event);
            }
        },
    )?;
    watcher
        .watch(outbox, RecursiveMode::Recursive)
        .with_context(|| format!("watch {}", outbox.display()))?;
    watcher
        .watch(failed, RecursiveMode::Recursive)
        .with_context(|| format!("watch {}", failed.display()))?;

    // Debounce loop: coalesce same-path events within `debounce` — keeps
    // the log tidy when an MUA move fires several inotify events on the
    // same file in quick succession. Debounce is per (side, basename)
    // pair so a Failed→Outbox drag-back doesn't get swallowed by a
    // preceding Failed unlink.
    spawn_local(async move {
        use std::collections::HashMap;
        use std::time::Instant;
        let mut last: HashMap<(OutboxSide, String), Instant> = HashMap::new();
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
                    );
                    let now = std_now();
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

fn classify_event(kind: &EventKind) -> (bool, bool) {
    use notify::event::{ModifyKind, RenameMode};
    let _ = CreateKind::File; // silence unused import if match arms shift
    match kind {
        EventKind::Create(_) => (true, false),
        EventKind::Remove(_) => (false, true),
        EventKind::Modify(ModifyKind::Name(mode)) => match mode {
            // rename-from = source is going away; treat as remove
            RenameMode::From => (false, true),
            // rename-to = target just materialized; treat as create
            RenameMode::To => (true, false),
            // Backends that fire "Both" or "Any" carry both paths in
            // event.paths; the caller sees both events. Treat as create
            // for the destination path (matching new_ event semantics).
            _ => (true, false),
        },
        // Modify(Data | Metadata) — file was written to or touched.
        // Not a state transition; ignored (returns (false, false)).
        _ => (false, false),
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
    // `tmp/` is a mid-write staging area — jmapqueue writes there then
    // atomically renames into `new/`. Filtering these events at the
    // notify callback (before they enter the debounce map) keeps them
    // from stealing the debounce slot from the real `new/` rename that
    // follows a fraction of a millisecond later.
    if !matches!(subdir.as_str(), "new" | "cur") {
        return None;
    }
    // Only events on the file itself, not the subdir.
    if rel.components().count() < 2 {
        return None;
    }
    Some((side, subdir))
}

// ---------------------------------------------------------------------------
// Filesystem helpers
// ---------------------------------------------------------------------------

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

/// Strip the standard Maildir `:2,<flags>` suffix so drag-back
/// bookkeeping and sidecar lookups work regardless of whether the file
/// has been moved to `cur/` yet.
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

// `std::time::Instant::now()` — wrapped in a helper so the tests can
// stay deterministic if we ever need to swap in a mock clock.
fn std_now() -> std::time::Instant {
    std::time::Instant::now()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_flags_variants() {
        assert_eq!(strip_flags("foo.bar.host"), "foo.bar.host");
        assert_eq!(strip_flags("foo.bar.host:2,"), "foo.bar.host");
        assert_eq!(strip_flags("foo.bar.host:2,S"), "foo.bar.host");
        assert_eq!(strip_flags("foo.bar.host:2,SR"), "foo.bar.host");
    }

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

    #[test]
    fn scan_basenames_strips_flags() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("Failed");
        for sub in ["tmp", "new", "cur"] {
            std::fs::create_dir_all(root.join(sub)).unwrap();
        }
        std::fs::write(root.join("new/abc.host"), b"").unwrap();
        std::fs::write(root.join("cur/def.host:2,S"), b"").unwrap();

        let names = scan_basenames(&root);
        assert!(names.contains("abc.host"));
        assert!(names.contains("def.host"));
        assert_eq!(names.len(), 2);
    }

    #[test]
    fn classify_path_recognizes_outbox_and_failed() {
        let tmp = tempfile::tempdir().unwrap();
        let outbox = tmp.path().join("Outbox").canonicalize();
        let failed = tmp.path().join("Failed").canonicalize();
        std::fs::create_dir_all(tmp.path().join("Outbox/new")).unwrap();
        std::fs::create_dir_all(tmp.path().join("Failed/new")).unwrap();
        let outbox = outbox.unwrap_or_else(|_| tmp.path().join("Outbox"));
        let failed = failed.unwrap_or_else(|_| tmp.path().join("Failed"));

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
        // tmp/ is jmapqueue's staging dir; the rename to new/ is the
        // one signal we want. Rejecting tmp events upstream prevents
        // them from stealing the debounce slot from the paired new/
        // event that follows a fraction of a millisecond later.
        let tmp = tempfile::tempdir().unwrap();
        let outbox = tmp.path().join("Outbox");
        let failed = tmp.path().join("Failed");
        std::fs::create_dir_all(outbox.join("tmp")).unwrap();
        let tmp_file = outbox.join("tmp/mid-write");
        std::fs::write(&tmp_file, b"").unwrap();
        assert!(classify_path(&tmp_file, &outbox, &failed).is_none());
    }

    #[test]
    fn classify_path_rejects_unknown_subdir() {
        let tmp = tempfile::tempdir().unwrap();
        let outbox = tmp.path().join("Outbox");
        let failed = tmp.path().join("Failed");
        std::fs::create_dir_all(outbox.join("bogus")).unwrap();
        let bogus = outbox.join("bogus/x");
        std::fs::write(&bogus, b"").unwrap();
        assert!(classify_path(&bogus, &outbox, &failed).is_none());
    }

    #[test]
    fn classify_path_rejects_outside_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let outbox = tmp.path().join("Outbox");
        let failed = tmp.path().join("Failed");
        let elsewhere = tmp.path().join("Inbox/new/foo");
        std::fs::create_dir_all(elsewhere.parent().unwrap()).unwrap();
        std::fs::write(&elsewhere, b"").unwrap();
        assert!(classify_path(&elsewhere, &outbox, &failed).is_none());
    }

    #[test]
    fn load_intent_missing_sidecar_returns_placeholder() {
        let tmp = tempfile::tempdir().unwrap();
        let s = load_intent(tmp.path(), "does-not-exist");
        assert!(s.contains("no sidecar"));
    }

    #[test]
    fn load_intent_formats_full_sidecar() {
        let tmp = tempfile::tempdir().unwrap();
        let sidecar_dir = tmp.path();
        std::fs::write(
            sidecar_dir.join("m1.json"),
            r#"{"envelope":{"from":"a@b","to":["c@d"]},"send_at":"2026-08-01T09:00:00Z","account":"personal","retry":{"attempts":0}}"#,
        ).unwrap();
        let s = load_intent(sidecar_dir, "m1");
        assert!(s.contains("from=a@b"));
        assert!(s.contains("c@d"));
        assert!(s.contains("send_at=2026-08-01T09:00:00Z"));
        assert!(!s.contains("retry"));
    }

    #[test]
    fn load_intent_surfaces_retry_state() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join("m2.json"),
            r#"{"envelope":{"from":"a@b","to":["c@d"]},"account":"personal","retry":{"attempts":3,"next_at":"2026-08-01T10:00:00Z"}}"#,
        ).unwrap();
        let s = load_intent(tmp.path(), "m2");
        assert!(s.contains("attempt=3"));
        assert!(s.contains("next_at=2026-08-01T10:00:00Z"));
    }

    #[test]
    fn load_intent_malformed_returns_error_placeholder() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("bad.json"), b"not-json").unwrap();
        let s = load_intent(tmp.path(), "bad");
        assert!(s.contains("malformed"));
    }
}
