use crate::config::{Account, TokenSource};
use anyhow::{Context, Result, bail};
use jmap_client::client::{Client, Credentials};
use std::time::Duration;

pub async fn client_from_account(acct: &Account) -> Result<Client> {
    let token = resolve_token(&acct.token)
        .with_context(|| format!("resolving JMAP token for account {:?}", acct.name))?;

    let url = format!("https://{}", acct.jmap_host);

    Client::new()
        .credentials(Credentials::Bearer(token))
        .timeout(Duration::from_secs(acct.timeout_secs))
        .connect(&url)
        .await
        .with_context(|| format!("connecting to JMAP endpoint {url}"))
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
}
