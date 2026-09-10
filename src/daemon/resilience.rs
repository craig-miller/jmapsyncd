use log::{debug, info, warn};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

const NETWORK_MANAGER_WAIT_SECS: &str = "2073600";
const NOTIFICATION_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) struct RetryBackoff {
    base: Duration,
    cap: Duration,
    next: Duration,
}

impl RetryBackoff {
    pub(super) fn new(base: Duration, cap: Duration) -> Self {
        Self {
            base,
            cap,
            next: base,
        }
    }

    pub(super) fn take_delay(&mut self) -> Duration {
        let delay = self.next;
        self.next = self.next.saturating_mul(2).min(self.cap);
        delay
    }

    pub(super) fn reset(&mut self) {
        self.next = self.base;
    }
}

pub(super) struct FailureNotifier {
    account: String,
    server: String,
    failed: bool,
}

impl FailureNotifier {
    pub(super) fn new(account: &str, jmap_host: &str) -> Self {
        Self {
            account: display_label(account),
            server: server_label(jmap_host),
            failed: false,
        }
    }

    pub(super) async fn failed(&mut self) {
        if self.failed {
            return;
        }
        self.failed = true;

        send_notification(
            "normal",
            "network-error-symbolic",
            &format!("{} eMail Down", self.account),
            &format!("{} server is unreachable.", self.server),
        )
        .await;
    }

    pub(super) async fn recovered(&mut self) {
        if !self.failed {
            return;
        }
        self.failed = false;

        let icon = connected_icon();
        send_notification(
            "normal",
            &icon,
            &format!("{} eMail Up", self.account),
            &format!("{} server is reachable.", self.server),
        )
        .await;
    }
}

/// Wait until NetworkManager reports an active connection. If NetworkManager
/// or nm-online is unavailable, fall back to the supplied retry delay. When the
/// network is already active, enforce the delay so service/authentication
/// failures cannot create a tight retry loop. If the machine stays offline
/// longer than the delay, retry immediately when connectivity returns.
pub(super) async fn wait_for_connectivity_or_delay(
    minimum_delay: Duration,
    cancel: &CancellationToken,
) -> bool {
    let started = Instant::now();

    match wait_for_network_manager(cancel).await {
        NetworkWait::Cancelled => return false,
        NetworkWait::Online => {
            info!("[daemon] NetworkManager reports connectivity; retrying when backoff permits");
        }
        NetworkWait::Unavailable => {
            debug!("[daemon] nm-online unavailable; using timed retry fallback");
        }
    }

    let remaining = minimum_delay.saturating_sub(started.elapsed());
    if !remaining.is_zero() {
        tokio::select! {
            _ = tokio::time::sleep(remaining) => {}
            _ = cancel.cancelled() => return false,
        }
    }
    !cancel.is_cancelled()
}

enum NetworkWait {
    Online,
    Unavailable,
    Cancelled,
}

async fn wait_for_network_manager(cancel: &CancellationToken) -> NetworkWait {
    let mut child = match Command::new("nm-online")
        .args(["--quiet", "--timeout", NETWORK_MANAGER_WAIT_SECS])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            debug!("[daemon] could not start nm-online: {error}");
            return NetworkWait::Unavailable;
        }
    };

    let status = tokio::select! {
        result = child.wait() => result,
        _ = cancel.cancelled() => {
            if let Err(error) = child.kill().await {
                debug!("[daemon] could not stop nm-online during shutdown: {error}");
            }
            return NetworkWait::Cancelled;
        }
    };

    match status {
        Ok(status) if status.success() => NetworkWait::Online,
        Ok(status) => {
            debug!("[daemon] nm-online exited with {status}; using timed retry fallback");
            NetworkWait::Unavailable
        }
        Err(error) => {
            debug!("[daemon] waiting for nm-online failed: {error}");
            NetworkWait::Unavailable
        }
    }
}

fn connected_icon() -> String {
    let mut data_roots = Vec::new();
    if let Some(data_home) = std::env::var_os("XDG_DATA_HOME") {
        data_roots.push(PathBuf::from(data_home));
    } else if let Some(home) = std::env::var_os("HOME") {
        data_roots.push(PathBuf::from(home).join(".local/share"));
    }

    if let Some(data_dirs) = std::env::var_os("XDG_DATA_DIRS") {
        data_roots.extend(std::env::split_paths(&data_dirs));
    } else {
        data_roots.extend([
            PathBuf::from("/usr/local/share"),
            PathBuf::from("/usr/share"),
        ]);
    }

    for data_root in data_roots {
        let path = data_root.join("icons/hicolor/scalable/status/network-connected-symbolic.svg");
        if path.is_file() {
            return path.to_string_lossy().into_owned();
        }
    }

    "network-connected-symbolic".to_string()
}

fn server_label(jmap_host: &str) -> String {
    let authority = jmap_host
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(jmap_host)
        .split('/')
        .next()
        .unwrap_or(jmap_host);
    let hostname = authority.split(':').next().unwrap_or(authority);
    let labels: Vec<_> = hostname
        .split('.')
        .filter(|label| !label.is_empty())
        .collect();
    let provider = if labels.len() >= 2 {
        labels[labels.len() - 2]
    } else {
        hostname
    };
    display_label(provider)
}

fn display_label(value: &str) -> String {
    let mut characters = value.chars();
    match characters.next() {
        Some(first) => first.to_uppercase().collect::<String>() + characters.as_str(),
        None => value.to_string(),
    }
}

async fn send_notification(urgency: &str, icon: &str, summary: &str, body: &str) {
    let mut child = match Command::new("notify-send")
        .args([
            "--app-name=jmapsyncd",
            &format!("--urgency={urgency}"),
            &format!("--icon={icon}"),
            summary,
            body,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            debug!("[daemon] could not start notify-send: {error}");
            return;
        }
    };

    match tokio::time::timeout(NOTIFICATION_TIMEOUT, child.wait()).await {
        Ok(Ok(status)) if status.success() => {}
        Ok(Ok(status)) => warn!("[daemon] notify-send exited with {status}"),
        Ok(Err(error)) => warn!("[daemon] waiting for notify-send failed: {error}"),
        Err(_) => {
            warn!("[daemon] notify-send timed out");
            if let Err(error) = child.kill().await {
                debug!("[daemon] could not stop notify-send after timeout: {error}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notification_labels_come_from_account_and_host() {
        assert_eq!(display_label("personal"), "Personal");
        assert_eq!(server_label("api.fastmail.com"), "Fastmail");
        assert_eq!(server_label("https://jmap.daylite.com/session"), "Daylite");
    }

    #[test]
    fn retry_backoff_grows_caps_and_resets() {
        let mut backoff = RetryBackoff::new(Duration::from_secs(5), Duration::from_secs(20));

        assert_eq!(backoff.take_delay(), Duration::from_secs(5));
        assert_eq!(backoff.take_delay(), Duration::from_secs(10));
        assert_eq!(backoff.take_delay(), Duration::from_secs(20));
        assert_eq!(backoff.take_delay(), Duration::from_secs(20));

        backoff.reset();
        assert_eq!(backoff.take_delay(), Duration::from_secs(5));
    }
}
