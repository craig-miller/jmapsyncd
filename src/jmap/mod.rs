use crate::config::{Account, TokenSource};
use anyhow::{Context, Result, bail};
use jmap_client::URI;
use jmap_client::client::{Client, Credentials};
use jmap_client::core::request::Request;
use std::time::Duration;

/// Restrict a request's `using` capability list to what jmapsyncd
/// actually uses (Core + Mail + Submission). jmap-client 0.4 declares
/// the full URI enum by default — including :sieve and :websocket,
/// which Fastmail rejects with a 400 (`error:unknownCapability`)
/// because they aren't on that account's advertised capability set.
///
/// Submission is required for `Identity/get`, `EmailSubmission/set`,
/// `EmailSubmission/query`, and the `EmailDelivery` push-type on
/// Fastmail. Adding it to the shared restrict is cheaper than a
/// per-call-site distinction and correct for every Fastmail-ish
/// server we care about (send is not optional for a mail account).
///
/// TODO: derive this from the session's advertised capabilities so any
/// JMAP provider works without having to know its supported URIs
/// ahead of time.
pub fn restrict_using(request: &mut Request<'_>) {
    apply_using_restriction(&mut request.using);
}

/// Pure inner: takes the `using` vec directly so the invariant is
/// testable without spinning up a live Client. Callers should reach
/// for `restrict_using(&mut request)` instead of this — it exists
/// so the tests below can lock down which capabilities we advertise.
fn apply_using_restriction(using: &mut Vec<URI>) {
    using.clear();
    using.push(URI::Core);
    using.push(URI::Mail);
    using.push(URI::Submission);
}

pub async fn client_from_account(acct: &Account) -> Result<Client> {
    let token = resolve_token(&acct.token)
        .with_context(|| format!("resolving JMAP token for account {:?}", acct.name))?;

    // If jmap_host already has a scheme (dev/testing: http://127.0.0.1:...),
    // use it verbatim; otherwise default to https:// for production.
    let url = if acct.jmap_host.starts_with("http://") || acct.jmap_host.starts_with("https://") {
        acct.jmap_host.clone()
    } else {
        format!("https://{}", acct.jmap_host)
    };

    // jmap-client's default redirect policy rejects any target host not in
    // its trusted_hosts set — so Fastmail's same-origin
    // /.well-known/jmap -> /jmap/session redirect gets blocked ("Aborting
    // redirect request to unknown host"). Pass the JMAP host explicitly
    // so same-origin redirects follow while cross-origin ones still error.
    let trusted_host = extract_host(&url)
        .with_context(|| format!("parsing JMAP host from {url}"))?;

    Client::new()
        .credentials(Credentials::Bearer(token))
        .follow_redirects([trusted_host])
        .timeout(Duration::from_secs(acct.timeout_secs))
        .connect(&url)
        .await
        .with_context(|| format!("connecting to JMAP endpoint {url}"))
}

/// Pull the host portion out of a URL string, ignoring scheme, port, and
/// path. Small, dependency-free parser — url::Url would be a whole crate
/// dep for one call site.
fn extract_host(url: &str) -> Result<String> {
    let after_scheme = url
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(url);
    let host = after_scheme.split(['/', ':']).next().unwrap_or("");
    if host.is_empty() {
        bail!("could not extract host from {url:?}");
    }
    Ok(host.to_string())
}

fn resolve_token(source: &TokenSource) -> Result<String> {
    let raw = match source {
        TokenSource::Inline { jmap_token } => return Ok(jmap_token.clone()),
        TokenSource::File { jmap_token_file } => std::fs::read_to_string(jmap_token_file)
            .with_context(|| format!("reading token file {}", jmap_token_file.display()))?,
        TokenSource::Cmd { jmap_token_cmd } => {
            let output = std::process::Command::new("/bin/sh")
                .arg("-c")
                .arg(jmap_token_cmd)
                .output()
                .with_context(|| format!("spawning jmap_token_cmd: {jmap_token_cmd}"))?;
            if !output.status.success() {
                bail!(
                    "jmap_token_cmd exited with status {}: {}",
                    output.status,
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            }
            String::from_utf8(output.stdout).context("jmap_token_cmd stdout was not UTF-8")?
        }
    };
    Ok(raw.trim_end_matches(['\n', '\r']).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::PathBuf;

    fn inline(token: &str) -> TokenSource {
        TokenSource::Inline {
            jmap_token: token.to_string(),
        }
    }

    fn file_src(path: PathBuf) -> TokenSource {
        TokenSource::File {
            jmap_token_file: path,
        }
    }

    fn cmd_src(cmd: &str) -> TokenSource {
        TokenSource::Cmd {
            jmap_token_cmd: cmd.to_string(),
        }
    }

    #[test]
    fn inline_returns_verbatim() {
        assert_eq!(resolve_token(&inline("abc123")).unwrap(), "abc123");
    }

    #[test]
    fn inline_preserves_internal_whitespace() {
        assert_eq!(resolve_token(&inline("a b\tc")).unwrap(), "a b\tc");
    }

    #[test]
    fn file_reads_and_trims_trailing_newline() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("token");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "file-token").unwrap();
        drop(f);
        assert_eq!(resolve_token(&file_src(path)).unwrap(), "file-token");
    }

    #[test]
    fn file_trims_crlf() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("token");
        std::fs::write(&path, "windows-token\r\n").unwrap();
        assert_eq!(resolve_token(&file_src(path)).unwrap(), "windows-token");
    }

    #[test]
    fn file_missing_errors() {
        let err = resolve_token(&file_src(PathBuf::from("/nonexistent/token"))).unwrap_err();
        assert!(format!("{err:#}").contains("reading token file"));
    }

    #[test]
    fn cmd_stdout_trimmed() {
        assert_eq!(
            resolve_token(&cmd_src("printf 'cmd-token\\n'")).unwrap(),
            "cmd-token"
        );
    }

    #[test]
    fn cmd_supports_pipes() {
        assert_eq!(
            resolve_token(&cmd_src("echo one two three | cut -d' ' -f2")).unwrap(),
            "two"
        );
    }

    #[test]
    fn cmd_nonzero_exit_errors() {
        let err = resolve_token(&cmd_src("false")).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("jmap_token_cmd exited"));
    }

    #[test]
    fn cmd_stderr_included_in_error() {
        let err = resolve_token(&cmd_src("echo boom >&2; exit 1")).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("boom"));
    }

    #[test]
    fn extract_host_strips_scheme_port_and_path() {
        assert_eq!(extract_host("api.fastmail.com").unwrap(), "api.fastmail.com");
        assert_eq!(extract_host("https://api.fastmail.com").unwrap(), "api.fastmail.com");
        assert_eq!(
            extract_host("https://api.fastmail.com/jmap/session").unwrap(),
            "api.fastmail.com"
        );
        assert_eq!(extract_host("http://127.0.0.1:8080/path").unwrap(), "127.0.0.1");
    }

    #[test]
    fn extract_host_empty_errors() {
        assert!(extract_host("").is_err());
        assert!(extract_host("https://").is_err());
    }

    // -----------------------------------------------------------------
    // Bug #1: restrict_using must include URI::Submission so
    // Identity/get, EmailSubmission/set, EmailSubmission/query, and the
    // EmailDelivery push-type on Fastmail can be issued. Without it,
    // every Phase-E submit round-trips a `urn:ietf:params:jmap:error:
    // unknownCapability` — the daemon then classifies it as a
    // transient network error and retries in a tight loop.
    // -----------------------------------------------------------------

    #[test]
    fn restrict_using_installs_core_mail_submission() {
        // Simulates jmap-client 0.4.2's default `Request::new` `using`
        // vec — 11 URIs, notably including WebSocket + Sieve which
        // Fastmail rejects (unknownCapability 400) when advertised.
        let mut using = vec![
            URI::Core,
            URI::Mail,
            URI::Submission,
            URI::VacationResponse,
            URI::Contacts,
            URI::Calendars,
            URI::WebSocket,
            URI::Sieve,
            URI::Blob,
            URI::Quota,
            URI::Principals,
        ];
        apply_using_restriction(&mut using);
        assert_eq!(
            using,
            vec![URI::Core, URI::Mail, URI::Submission],
            "must expose exactly Core+Mail+Submission and nothing else"
        );
    }

    #[test]
    fn restrict_using_drops_websocket_and_sieve() {
        // Explicit guard on the two URIs that made Fastmail return 400
        // before the restrict landed. Kept separate from the shape
        // assertion above so a future addition (e.g. Blob) doesn't
        // silently mask a regression on these two.
        let mut using = vec![URI::WebSocket, URI::Sieve];
        apply_using_restriction(&mut using);
        assert!(!using.contains(&URI::WebSocket));
        assert!(!using.contains(&URI::Sieve));
    }

    #[test]
    fn restrict_using_is_idempotent() {
        let mut using = vec![URI::Core, URI::Mail, URI::Submission];
        apply_using_restriction(&mut using);
        assert_eq!(using, vec![URI::Core, URI::Mail, URI::Submission]);
    }

    #[test]
    fn restrict_using_from_empty_still_installs_capabilities() {
        let mut using = vec![];
        apply_using_restriction(&mut using);
        assert_eq!(using, vec![URI::Core, URI::Mail, URI::Submission]);
    }
}
