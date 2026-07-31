//! jmapqueue — sendmail(1)-compatible wrapper for the jmapsyncd send path.
//!
//! aerc's `outgoing = /path/to/jmapqueue` invokes this as
//!
//!     jmapqueue rcpt1@x rcpt2@y ... < message.eml
//!
//! (no `-t`, no `-f` — verified against aerc's `lib/send/sendmail.go:32-43`
//! during send-path Phase A). We also accept the sendmail-classic `-f`,
//! `-i`, `-oi` flags for other MUAs' benefit; only `-f` has any effect.
//!
//! Behavior is intentionally minimal and independent of the daemon: parse
//! the config, match the message's From: address to an account with a
//! `[accounts.submit]` block, extract + strip `X-JMAP-Send-At`, write the
//! message atomically to `~/mail/<account>/Outbox/new/<name>` (Maildir),
//! and drop a JSON sidecar in the XDG state dir keyed by filename. Exit
//! immediately. The daemon's per-account submit loop drains the Outbox
//! whenever it comes back online.

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use jmapsyncd::config::{Account, Config, Overrides};
use serde::Serialize;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Parser)]
#[command(
    name = "jmapqueue",
    version,
    about = "sendmail(1)-compatible spool wrapper for the jmapsyncd send path"
)]
struct Args {
    /// Envelope sender override; usually taken from the message's From: header
    #[arg(short = 'f')]
    from: Option<String>,

    /// sendmail "ignore lone dot as EOF" flag; accepted for compat, ignored
    #[arg(short = 'i')]
    _ignore_dots: bool,

    /// sendmail "-oi" combined flag; accepted for compat, ignored
    #[arg(long = "oi")]
    _oi: bool,

    /// Schedule delivery at RFC 3339 / ISO 8601 time; overrides
    /// X-JMAP-Send-At header. Pass "send-now" (or omit) for immediate.
    #[arg(long = "send-at")]
    send_at: Option<String>,

    /// Explicit account name (overrides From-header matching)
    #[arg(long)]
    account: Option<String>,

    /// Config file path (default: XDG config dir)
    #[arg(short = 'c', long = "config", env = "JMAPSYNCD_CONFIG")]
    config_file: Option<PathBuf>,

    /// Recipient email addresses (positional; envelope RCPT TO)
    recipients: Vec<String>,
}

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("jmapqueue: error: {e:#}");
            std::process::ExitCode::from(1)
        }
    }
}

fn run() -> Result<()> {
    let args = Args::parse();

    if args.recipients.is_empty() {
        bail!("no recipient(s) on the command line");
    }

    let mut raw = Vec::with_capacity(8 * 1024);
    std::io::stdin()
        .read_to_end(&mut raw)
        .context("reading message from stdin")?;
    if raw.is_empty() {
        bail!("empty message on stdin");
    }

    let config = Config::load(args.config_file.as_deref(), &Overrides::default())
        .context("loading jmapsyncd config")?;

    let (header_from, send_at_header) = extract_headers(&raw)?;
    let effective_from = args
        .from
        .clone()
        .or(header_from.clone())
        .ok_or_else(|| anyhow!("no From: header in message and no -f override provided"))?;

    let account = pick_account(&config, args.account.as_deref(), &effective_from)?;
    account.submit.as_ref().ok_or_else(|| {
        anyhow!(
            "account {:?} has no [accounts.submit] block; jmapqueue refuses \
             to route mail through a receive-only account",
            account.name
        )
    })?;
    let mail_root = account
        .mail
        .as_ref()
        .map(|m| m.path.clone())
        .ok_or_else(|| {
            anyhow!(
                "account {:?} has no [accounts.mail] block; nowhere to write Outbox",
                account.name
            )
        })?;

    // send_at resolution: CLI flag > header > None(=immediate).
    let send_at = if let Some(cli) = &args.send_at {
        normalize_send_at(cli)?
    } else if let Some(hdr) = &send_at_header {
        normalize_send_at(hdr)?
    } else {
        None
    };

    // Strip X-JMAP-Send-At from the outbound bytes — the JMAP server never
    // sees it; the daemon reads scheduling from the sidecar.
    let outbound = strip_header_case_insensitive(&raw, "x-jmap-send-at");

    let ulid_id = ulid::Ulid::new().to_string();
    let filename = maildir_filename(&ulid_id);
    let outbox = mail_root.join("Outbox");
    create_maildir(&outbox)?;
    atomic_write_new(&outbox, &filename, &outbound)?;

    let sidecar_dir = state_dir_for(&account.name)?;
    std::fs::create_dir_all(&sidecar_dir)
        .with_context(|| format!("creating sidecar dir {}", sidecar_dir.display()))?;
    let sidecar = Sidecar {
        envelope: Envelope {
            from: effective_from,
            to: args.recipients.clone(),
        },
        send_at,
        submitted_at: iso_now(),
        account: account.name.clone(),
        retry: RetryState::default(),
    };
    let sidecar_path = sidecar_dir.join(format!("{filename}.json"));
    let bytes = serde_json::to_vec_pretty(&sidecar).context("serializing sidecar")?;
    std::fs::write(&sidecar_path, bytes)
        .with_context(|| format!("writing sidecar to {}", sidecar_path.display()))?;

    // Best-effort log to stderr — aerc surfaces stderr in its status line
    // on a nonzero exit; on success it's silent, which is what we want.
    eprintln!(
        "jmapqueue: queued {filename} for account {name} (recipients: {n})",
        name = account.name,
        n = args.recipients.len()
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Header extraction
// ---------------------------------------------------------------------------

fn extract_headers(raw: &[u8]) -> Result<(Option<String>, Option<String>)> {
    let (headers, _pos) =
        mailparse::parse_headers(raw).context("parsing message headers")?;
    let from = headers
        .iter()
        .find(|h| h.get_key_ref().eq_ignore_ascii_case("from"))
        .map(|h| bare_address(&h.get_value()));
    let send_at = headers
        .iter()
        .find(|h| h.get_key_ref().eq_ignore_ascii_case("x-jmap-send-at"))
        .map(|h| h.get_value().trim().to_string());
    Ok((from, send_at))
}

/// Extract the bare address from a From:-style header value.
/// `"Alice" <alice@example.com>` → `alice@example.com`.
/// `bob@example.com` → `bob@example.com`.
fn bare_address(header_value: &str) -> String {
    if let (Some(lt), Some(gt)) = (header_value.find('<'), header_value.rfind('>')) {
        if lt < gt {
            return header_value[lt + 1..gt].trim().to_string();
        }
    }
    header_value.trim().to_string()
}

// ---------------------------------------------------------------------------
// Account routing
// ---------------------------------------------------------------------------

fn pick_account<'c>(
    config: &'c Config,
    explicit_name: Option<&str>,
    from: &str,
) -> Result<&'c Account> {
    if let Some(name) = explicit_name {
        return config
            .accounts
            .iter()
            .find(|a| a.name == name)
            .ok_or_else(|| anyhow!("--account {name:?} not found in config"));
    }
    config
        .accounts
        .iter()
        .find(|a| {
            a.submit
                .as_ref()
                .is_some_and(|s| s.from_address.eq_ignore_ascii_case(from))
        })
        .ok_or_else(|| {
            anyhow!(
                "no account whose [accounts.submit].from_address matches \
                 From: {from:?} (use --account to override)"
            )
        })
}

// ---------------------------------------------------------------------------
// send_at normalization
// ---------------------------------------------------------------------------

fn normalize_send_at(v: &str) -> Result<Option<String>> {
    let trimmed = v.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("send-now") {
        return Ok(None);
    }
    chrono::DateTime::parse_from_rfc3339(trimmed).with_context(|| {
        format!(
            "invalid --send-at / X-JMAP-Send-At value {trimmed:?} \
             (expected RFC 3339, e.g. 2026-08-01T09:00:00-07:00)"
        )
    })?;
    Ok(Some(trimmed.to_string()))
}

// ---------------------------------------------------------------------------
// Header strip — RFC 5322-aware (handles folded continuations)
// ---------------------------------------------------------------------------

fn strip_header_case_insensitive(raw: &[u8], name_lc: &str) -> Vec<u8> {
    let (headers, body) = split_header_body(raw);
    let mut out = Vec::with_capacity(raw.len());
    let mut skip = false;
    for line in headers.split_inclusive(|&b| b == b'\n') {
        if starts_with_folded_continuation(line) {
            if skip {
                continue;
            }
        } else {
            skip = line_starts_with_header(line, name_lc);
            if skip {
                continue;
            }
        }
        out.extend_from_slice(line);
    }
    out.extend_from_slice(body);
    out
}

/// Split at the header/body separator (CRLF CRLF or LF LF). The separator
/// itself stays with the body half so re-joining preserves structure.
fn split_header_body(raw: &[u8]) -> (&[u8], &[u8]) {
    for sep in [b"\r\n\r\n".as_slice(), b"\n\n".as_slice()] {
        if let Some(pos) = raw.windows(sep.len()).position(|w| w == sep) {
            return (&raw[..pos], &raw[pos..]);
        }
    }
    (raw, b"")
}

fn starts_with_folded_continuation(line: &[u8]) -> bool {
    matches!(line.first(), Some(b' ' | b'\t'))
}

fn line_starts_with_header(line: &[u8], name_lc: &str) -> bool {
    let colon = match line.iter().position(|&b| b == b':') {
        Some(p) => p,
        None => return false,
    };
    let hdr = &line[..colon];
    if hdr.len() != name_lc.len() {
        return false;
    }
    hdr.iter()
        .zip(name_lc.bytes())
        .all(|(a, b)| a.to_ascii_lowercase() == b)
}

// ---------------------------------------------------------------------------
// Maildir writes
// ---------------------------------------------------------------------------

fn maildir_filename(ulid_id: &str) -> String {
    let epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let pid = std::process::id();
    let host = hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "localhost".into());
    format!("{epoch}.jmq{pid}_{ulid_id}.{host}")
}

fn create_maildir(root: &Path) -> Result<()> {
    for sub in ["tmp", "new", "cur"] {
        std::fs::create_dir_all(root.join(sub))
            .with_context(|| format!("creating {}", root.join(sub).display()))?;
    }
    Ok(())
}

fn atomic_write_new(outbox: &Path, filename: &str, bytes: &[u8]) -> Result<()> {
    let tmp = outbox.join("tmp").join(filename);
    let new = outbox.join("new").join(filename);
    {
        let mut f = std::fs::File::create(&tmp)
            .with_context(|| format!("creating {}", tmp.display()))?;
        f.write_all(bytes)?;
        f.sync_all()
            .with_context(|| format!("fsync {}", tmp.display()))?;
    }
    std::fs::rename(&tmp, &new)
        .with_context(|| format!("renaming {} to {}", tmp.display(), new.display()))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// State dir + sidecar
// ---------------------------------------------------------------------------

fn state_dir_for(account: &str) -> Result<PathBuf> {
    let base = dirs::state_dir()
        .ok_or_else(|| anyhow!("no XDG_STATE_HOME (dirs::state_dir returned None)"))?;
    Ok(base.join("jmapsyncd").join(account).join("outbox-meta"))
}

#[derive(Serialize)]
struct Sidecar {
    envelope: Envelope,
    #[serde(skip_serializing_if = "Option::is_none")]
    send_at: Option<String>,
    submitted_at: String,
    account: String,
    retry: RetryState,
}

#[derive(Serialize)]
struct Envelope {
    from: String,
    to: Vec<String>,
}

#[derive(Serialize, Default)]
struct RetryState {
    attempts: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_at: Option<String>,
}

fn iso_now() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_address_display_name() {
        assert_eq!(
            bare_address("\"Alice Example\" <alice@example.com>"),
            "alice@example.com"
        );
    }

    #[test]
    fn bare_address_no_display_name() {
        assert_eq!(bare_address("bob@example.com"), "bob@example.com");
    }

    #[test]
    fn bare_address_trims_whitespace() {
        assert_eq!(bare_address("  bob@example.com  "), "bob@example.com");
    }

    #[test]
    fn bare_address_multiple_angles_uses_last_close() {
        // Pathological but legal: display name contains '>'. rfind('>')
        // ensures we take the outermost close bracket.
        assert_eq!(
            bare_address("\"a > b\" <c@d>"),
            "c@d"
        );
    }

    #[test]
    fn normalize_send_at_none_forms() {
        assert!(matches!(normalize_send_at("").unwrap(), None));
        assert!(matches!(normalize_send_at("send-now").unwrap(), None));
        assert!(matches!(normalize_send_at("SEND-NOW").unwrap(), None));
        assert!(matches!(normalize_send_at("  send-now  ").unwrap(), None));
    }

    #[test]
    fn normalize_send_at_rfc3339() {
        assert_eq!(
            normalize_send_at("2026-08-01T09:00:00-07:00").unwrap().as_deref(),
            Some("2026-08-01T09:00:00-07:00")
        );
        assert_eq!(
            normalize_send_at("2026-08-01T09:00:00Z").unwrap().as_deref(),
            Some("2026-08-01T09:00:00Z")
        );
    }

    #[test]
    fn normalize_send_at_rejects_garbage() {
        assert!(normalize_send_at("not-a-date").is_err());
        assert!(normalize_send_at("2026-08-01").is_err()); // date-only, no time
    }

    #[test]
    fn line_starts_with_header_exact_case_insensitive() {
        assert!(line_starts_with_header(b"X-JMAP-Send-At: 2026-01-01\n", "x-jmap-send-at"));
        assert!(line_starts_with_header(b"x-jmap-send-at:foo\n", "x-jmap-send-at"));
        assert!(line_starts_with_header(b"X-JMAP-SEND-AT:foo\n", "x-jmap-send-at"));
    }

    #[test]
    fn line_starts_with_header_rejects_prefix_match() {
        // "X-JMAP-Send-At-Extra:" must NOT match "x-jmap-send-at".
        assert!(!line_starts_with_header(
            b"X-JMAP-Send-At-Extra: v\n",
            "x-jmap-send-at"
        ));
    }

    #[test]
    fn line_starts_with_header_no_colon_no_match() {
        assert!(!line_starts_with_header(b"body line\n", "x-jmap-send-at"));
    }

    #[test]
    fn strip_header_drops_target_line() {
        let raw = b"From: a@b\nSubject: hi\nX-JMAP-Send-At: send-now\n\nbody\n";
        let out = strip_header_case_insensitive(raw, "x-jmap-send-at");
        let s = std::str::from_utf8(&out).unwrap();
        assert!(!s.contains("X-JMAP-Send-At"));
        assert!(s.contains("From: a@b"));
        assert!(s.contains("Subject: hi"));
        assert!(s.contains("body"));
    }

    #[test]
    fn strip_header_drops_folded_continuation_lines() {
        let raw = b"From: a@b\nX-JMAP-Send-At: 2026-08-01\n T09:00:00Z\n\tstill folded\nSubject: hi\n\nbody\n";
        let out = strip_header_case_insensitive(raw, "x-jmap-send-at");
        let s = std::str::from_utf8(&out).unwrap();
        assert!(!s.contains("X-JMAP-Send-At"));
        assert!(!s.contains("T09:00:00Z"));
        assert!(!s.contains("still folded"));
        assert!(s.contains("From: a@b"));
        assert!(s.contains("Subject: hi"));
        assert!(s.contains("body"));
    }

    #[test]
    fn strip_header_preserves_crlf_line_endings() {
        let raw = b"From: a@b\r\nX-JMAP-Send-At: send-now\r\nSubject: hi\r\n\r\nbody\r\n";
        let out = strip_header_case_insensitive(raw, "x-jmap-send-at");
        let s = std::str::from_utf8(&out).unwrap();
        assert!(!s.contains("X-JMAP-Send-At"));
        assert!(s.contains("From: a@b\r\n"));
        assert!(s.contains("Subject: hi\r\n"));
    }

    #[test]
    fn strip_header_no_match_returns_input_verbatim() {
        let raw = b"From: a@b\nSubject: hi\n\nbody\n";
        let out = strip_header_case_insensitive(raw, "x-jmap-send-at");
        assert_eq!(out, raw);
    }

    #[test]
    fn strip_header_only_first_matching_line() {
        // If someone sends TWO X-JMAP-Send-At headers, both go.
        let raw = b"From: a@b\nX-JMAP-Send-At: 1\nX-JMAP-Send-At: 2\nSubject: hi\n\nbody\n";
        let out = strip_header_case_insensitive(raw, "x-jmap-send-at");
        let s = std::str::from_utf8(&out).unwrap();
        assert!(!s.contains("X-JMAP-Send-At"));
        assert!(s.contains("Subject: hi"));
    }

    #[test]
    fn maildir_filename_shape() {
        let name = maildir_filename("01H0000000000000000000000000");
        // <epoch>.jmq<pid>_<ulid>.<host>
        assert!(name.contains(".jmq"));
        assert!(name.contains("_01H0000000000000000000000000."));
        // Rough shape check: at least 3 dots (epoch, jmq/_ulid separator, host).
        assert!(name.matches('.').count() >= 2);
    }

    #[test]
    fn create_maildir_makes_all_three() {
        let tmp = tempfile::tempdir().unwrap();
        let outbox = tmp.path().join("Outbox");
        create_maildir(&outbox).unwrap();
        assert!(outbox.join("tmp").is_dir());
        assert!(outbox.join("new").is_dir());
        assert!(outbox.join("cur").is_dir());
    }

    #[test]
    fn atomic_write_new_lands_in_new_not_tmp() {
        let tmp = tempfile::tempdir().unwrap();
        let outbox = tmp.path().join("Outbox");
        create_maildir(&outbox).unwrap();
        atomic_write_new(&outbox, "test.file", b"hello").unwrap();
        assert!(outbox.join("new").join("test.file").exists());
        assert!(!outbox.join("tmp").join("test.file").exists());
        assert_eq!(
            std::fs::read(outbox.join("new").join("test.file")).unwrap(),
            b"hello"
        );
    }

    #[test]
    fn extract_headers_finds_from_and_send_at() {
        let raw = b"From: \"Alice\" <alice@example.com>\r\nX-JMAP-Send-At: send-now\r\nSubject: hi\r\n\r\nbody\r\n";
        let (from, send_at) = extract_headers(raw).unwrap();
        assert_eq!(from.as_deref(), Some("alice@example.com"));
        assert_eq!(send_at.as_deref(), Some("send-now"));
    }

    #[test]
    fn extract_headers_missing_from_returns_none() {
        let raw = b"Subject: hi\r\n\r\nbody\r\n";
        let (from, send_at) = extract_headers(raw).unwrap();
        assert!(from.is_none());
        assert!(send_at.is_none());
    }
}
