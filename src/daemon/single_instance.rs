//! Single-instance guard for the daemon.
//!
//! The Portage `.desktop` autostart entry fires every time the user's
//! niri session starts, and dex-spawned processes are orphaned to init
//! rather than tied to the session's cgroup. `KillUserProcesses=no` on
//! elogind then keeps them running across logout. So without a lock,
//! each re-login would spawn another `jmapsyncd daemon` alongside the
//! surviving orphan — SQLite races on the shared per-account DB, both
//! instances race on Outbox files (double sends), and both open
//! independent SSE streams (multiplied API traffic).
//!
//! `flock(LOCK_EX | LOCK_NB)` on a well-known path solves it: advisory
//! POSIX file lock, released automatically by the kernel when the FD
//! closes, including on SIGKILL / abrupt exit — no stale-lock cleanup
//! ever needed. Only the `daemon` subcommand acquires it; the `sync`
//! one-shot and `jmapqueue` intentionally don't.

use anyhow::{Context, Result, anyhow};
use fs2::FileExt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Default lockfile path: `${XDG_STATE_HOME}/jmapsyncd/jmapsyncd.lock`.
/// Same anchor `dirs::state_dir()` used by the outbox-meta sidecars, so
/// the state directory is a single, well-defined per-user location.
fn default_lock_path() -> Result<PathBuf> {
    let base = dirs::state_dir()
        .context("no XDG_STATE_HOME (dirs::state_dir returned None)")?;
    Ok(base.join("jmapsyncd").join("jmapsyncd.lock"))
}

/// Acquire the default single-instance lock. Returns the locked `File`;
/// the caller MUST keep it alive for the daemon's lifetime (drop closes
/// the FD, kernel releases the flock).
pub fn acquire() -> Result<File> {
    acquire_at(&default_lock_path()?)
}

/// Test-injectable variant. Same semantics as `acquire`, at a caller-
/// supplied path.
pub fn acquire_at(path: &Path) -> Result<File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!("cannot create lockfile parent dir {}", parent.display())
        })?;
    }

    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)
        .with_context(|| format!("cannot open lockfile {}", path.display()))?;

    match file.try_lock_exclusive() {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
            // Best-effort: read the holder's PID for a helpful error. If
            // the file is empty (holder crashed after taking the lock
            // but before writing) or unreadable, fall back to "unknown".
            let mut buf = String::new();
            let holder = File::open(path)
                .and_then(|mut f| f.read_to_string(&mut buf).map(|_| buf))
                .ok()
                .and_then(|s| s.trim().parse::<u32>().ok())
                .map(|p| p.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            return Err(anyhow!(
                "jmapsyncd daemon already running (pid={holder}); refusing to start a second instance"
            ));
        }
        Err(e) => {
            return Err(e).with_context(|| {
                format!("flock failed on {}", path.display())
            });
        }
    }

    // Truncate + write our own PID for the next contender's diagnostic.
    file.set_len(0)
        .with_context(|| format!("cannot truncate lockfile {}", path.display()))?;
    file.seek(SeekFrom::Start(0))
        .with_context(|| format!("cannot rewind lockfile {}", path.display()))?;
    writeln!(file, "{}", std::process::id())
        .with_context(|| format!("cannot write pid to lockfile {}", path.display()))?;
    file.sync_all().ok();
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn acquire_writes_current_pid() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("jmapsyncd.lock");
        let _lock = acquire_at(&path).expect("first acquire");
        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(
            contents.trim().parse::<u32>().unwrap(),
            std::process::id()
        );
    }

    #[test]
    fn second_acquire_fails_while_first_held() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("jmapsyncd.lock");
        let _first = acquire_at(&path).expect("first acquire");
        let err = acquire_at(&path).expect_err("second acquire must fail");
        let msg = format!("{err:#}");
        assert!(msg.contains("already running"), "unexpected msg: {msg}");
        assert!(
            msg.contains(&std::process::id().to_string()),
            "expected pid {} in msg: {msg}",
            std::process::id()
        );
    }

    #[test]
    fn drop_releases_lock() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("jmapsyncd.lock");
        {
            let _first = acquire_at(&path).expect("first acquire");
        } // drop closes fd → kernel releases flock
        // Second acquire in the same process must now succeed.
        let _second = acquire_at(&path).expect("second acquire after drop");
    }

    #[test]
    fn creates_missing_parent_dir() {
        let dir = tempdir().unwrap();
        let nested = dir.path().join("nonexistent-parent").join("jmapsyncd.lock");
        assert!(!nested.parent().unwrap().exists());
        let _lock = acquire_at(&nested).expect("acquire must create parent");
        assert!(nested.exists());
    }
}
