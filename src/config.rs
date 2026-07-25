use anyhow::{Context, Result};
use clap::ValueEnum;
use log::LevelFilter;
use serde::Deserialize;
use std::path::PathBuf;

// ---------------------------------------------------------------------------
// Config — final merged config after precedence resolution
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct Config {
    pub db_dir: PathBuf,
    pub accounts: Vec<Account>,
}

impl Config {
    pub fn load(path: Option<&std::path::Path>, overrides: &Overrides) -> Result<Self> {
        let config_path = match path {
            Some(p) => p.to_path_buf(),
            None => dirs::config_dir()
                .context("no platform config directory available; use --config to specify a path")?
                .join("jmapsyncd")
                .join("config.toml"),
        };
        let file_config = ConfigFile::load(&config_path)?;

        let db_dir = match overrides.db_dir.clone().or(file_config.db_dir) {
            Some(d) => d,
            None => dirs::data_dir()
                .context("no platform data directory available; use --db-dir to specify a path")?
                .join("jmapsyncd"),
        };

        Ok(Config {
            db_dir,
            accounts: file_config.accounts,
        })
    }
}

#[derive(Debug, Default)]
pub struct Overrides {
    pub db_dir: Option<PathBuf>,
}

// ---------------------------------------------------------------------------
// ConfigFile — TOML file deserialization (paths auto-expanded)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigFile {
    #[serde(default, deserialize_with = "helpers::expand_opt_path")]
    db_dir: Option<PathBuf>,
    #[serde(default)]
    accounts: Vec<Account>,
}

impl ConfigFile {
    fn load(path: &std::path::Path) -> Result<Self> {
        let content =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        toml::from_str(&content).with_context(|| format!("parsing {}", path.display()))
    }
}

// ---------------------------------------------------------------------------
// Account — one JMAP account in the config
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct Account {
    pub name: String,
    #[serde(default = "helpers::default_true")]
    pub enabled: bool,
    pub jmap_host: String,
    pub jmap_user: String,
    #[serde(flatten)]
    pub token: TokenSource,
    #[serde(default = "helpers::default_timeout_secs")]
    pub timeout_secs: u64,
    #[serde(default = "helpers::default_poll_interval_secs")]
    pub poll_interval_secs: u64,
    pub mail: Option<MailConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum TokenSource {
    Inline {
        jmap_token: String,
    },
    File {
        #[serde(deserialize_with = "helpers::expand_path")]
        jmap_token_file: PathBuf,
    },
    Cmd {
        jmap_token_cmd: String,
    },
}

// ---------------------------------------------------------------------------
// MailConfig — per-account maildir + JMAP mail settings
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MailConfig {
    #[serde(deserialize_with = "helpers::expand_path")]
    pub path: PathBuf,
    #[serde(default)]
    pub sync_mode: SyncMode,
    #[serde(default = "helpers::default_true")]
    pub subscribed_only: bool,
    pub box_filter: Option<Vec<String>>,
    pub tls: Option<TlsConfig>,
    #[serde(default)]
    pub box_mapping: Vec<BoxMapping>,
}

// ---------------------------------------------------------------------------
// Leaf config types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    #[serde(default, deserialize_with = "helpers::expand_opt_path")]
    pub ca_file: Option<PathBuf>,
    #[serde(default, deserialize_with = "helpers::expand_opt_path")]
    pub client_cert: Option<PathBuf>,
    #[serde(default, deserialize_with = "helpers::expand_opt_path")]
    pub client_key: Option<PathBuf>,
    pub fingerprint: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct BoxMapping {
    pub remote: String,
    pub local: String,
}

#[derive(Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SyncMode {
    #[default]
    Mirror,
    TwoWay,
}

#[derive(Debug, Default, Deserialize, Clone, Copy, PartialEq, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    #[default]
    Info,
    Warn,
    Error,
}

impl From<LogLevel> for LevelFilter {
    fn from(level: LogLevel) -> Self {
        match level {
            LogLevel::Trace => LevelFilter::Trace,
            LogLevel::Debug => LevelFilter::Debug,
            LogLevel::Info => LevelFilter::Info,
            LogLevel::Warn => LevelFilter::Warn,
            LogLevel::Error => LevelFilter::Error,
        }
    }
}

pub(crate) mod helpers {
    use serde::Deserialize;
    use serde::de;
    use std::path::PathBuf;

    pub fn expand_path<'de, D>(deserializer: D) -> Result<PathBuf, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        shellexpand::full(&s)
            .map(|e| PathBuf::from(e.as_ref()))
            .map_err(de::Error::custom)
    }

    pub fn expand_opt_path<'de, D>(deserializer: D) -> Result<Option<PathBuf>, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        Option::<String>::deserialize(deserializer)?
            .map(|s| shellexpand::full(&s).map(|e| PathBuf::from(e.as_ref())))
            .transpose()
            .map_err(de::Error::custom)
    }

    pub fn default_true() -> bool {
        true
    }

    pub fn default_timeout_secs() -> u64 {
        30
    }

    pub fn default_poll_interval_secs() -> u64 {
        300
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn minimal_config() {
        let toml_str = r#"
[[accounts]]
name = "personal"
jmap_host = "api.fastmail.com"
jmap_user = "user@fastmail.com"
jmap_token = "sekret"
"#;
        let config: ConfigFile = toml::from_str(toml_str).unwrap();
        assert_eq!(config.accounts.len(), 1);
        let acct = &config.accounts[0];
        assert_eq!(acct.name, "personal");
        assert!(acct.enabled);
        assert!(
            matches!(acct.token, TokenSource::Inline { ref jmap_token } if jmap_token == "sekret")
        );
        assert!(acct.mail.is_none());
    }

    #[test]
    fn full_config() {
        let toml_str = r#"
db_dir = "/tmp/jmapsyncd"

[[accounts]]
name = "work"
enabled = true
jmap_host = "jmap.work.com"
jmap_user = "me@work.com"
jmap_token_file = "~/.config/jmapsyncd/tokens/work"
timeout_secs = 30

[accounts.mail]
path = "~/Mail/work"
sync_mode = "two_way"
subscribed_only = false
box_filter = ["INBOX", "Sent*"]

[accounts.mail.tls]
ca_file = "/etc/ssl/certs/ca-certificates.crt"
client_cert = "~/certs/client.pem"
client_key = "~/certs/client.key"
fingerprint = "SHA256:abc123"

[[accounts.mail.box_mapping]]
remote = "Sent Items"
local = "Sent"

[[accounts.mail.box_mapping]]
remote = "Deleted Messages"
local = "Trash"
"#;
        let config: ConfigFile = toml::from_str(toml_str).unwrap();
        assert_eq!(config.db_dir, Some(PathBuf::from("/tmp/jmapsyncd")));
        assert_eq!(config.accounts.len(), 1);

        let acct = &config.accounts[0];
        assert_eq!(acct.name, "work");
        assert!(acct.enabled);
        assert_eq!(acct.jmap_host, "jmap.work.com");
        assert_eq!(acct.jmap_user, "me@work.com");
        assert_eq!(acct.timeout_secs, 30);

        let mail = acct.mail.as_ref().unwrap();
        assert_eq!(mail.sync_mode, SyncMode::TwoWay);
        assert!(!mail.subscribed_only);
        assert_eq!(
            mail.box_filter,
            Some(vec!["INBOX".to_string(), "Sent*".to_string()])
        );

        let tls = mail.tls.as_ref().unwrap();
        assert_eq!(tls.fingerprint.as_deref(), Some("SHA256:abc123"));

        assert_eq!(mail.box_mapping.len(), 2);
        assert_eq!(mail.box_mapping[0].remote, "Sent Items");
        assert_eq!(mail.box_mapping[0].local, "Sent");
    }

    #[test]
    fn rejects_unknown_mail_fields() {
        let toml_str = r#"
[[accounts]]
name = "test"
jmap_host = "x.com"
jmap_user = "u@x.com"
jmap_token = "t"

[accounts.mail]
path = "~/Mail"
unknown = true
"#;
        let result: Result<ConfigFile, _> = toml::from_str(toml_str);
        assert!(result.is_err());
    }

    #[test]
    fn token_at_least_one_required() {
        // Zero tokens — fails deserialization
        let result: Result<ConfigFile, _> = toml::from_str(
            r#"
[[accounts]]
name = "test"
jmap_host = "x.com"
jmap_user = "u@x.com"
"#,
        );
        assert!(result.is_err());

        // One token — succeeds
        let result: Result<ConfigFile, _> = toml::from_str(
            r#"
[[accounts]]
name = "test"
jmap_host = "x.com"
jmap_user = "u@x.com"
jmap_token = "t"
"#,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn token_resolution() {
        let config: ConfigFile = toml::from_str(
            r#"
[[accounts]]
name = "test"
jmap_host = "x.com"
jmap_user = "u@x.com"
jmap_token = "t"
"#,
        )
        .unwrap();
        match &config.accounts[0].token {
            TokenSource::Inline { jmap_token } => assert_eq!(jmap_token, "t"),
            _ => panic!("expected Inline"),
        }

        let config: ConfigFile = toml::from_str(
            r#"
[[accounts]]
name = "test"
jmap_host = "x.com"
jmap_user = "u@x.com"
jmap_token_file = "/some/path"
"#,
        )
        .unwrap();
        match &config.accounts[0].token {
            TokenSource::File { jmap_token_file } => {
                assert_eq!(jmap_token_file, &PathBuf::from("/some/path"))
            }
            _ => panic!("expected File"),
        }

        let config: ConfigFile = toml::from_str(
            r#"
[[accounts]]
name = "test"
jmap_host = "x.com"
jmap_user = "u@x.com"
jmap_token_cmd = "get-token"
"#,
        )
        .unwrap();
        match &config.accounts[0].token {
            TokenSource::Cmd { jmap_token_cmd } => assert_eq!(jmap_token_cmd, "get-token"),
            _ => panic!("expected Cmd"),
        }
    }

    #[test]
    fn default_sync_mode() {
        let toml_str = r#"
[[accounts]]
name = "test"
jmap_host = "x.com"
jmap_user = "u@x.com"
jmap_token = "t"

[accounts.mail]
path = "~/Mail"
"#;
        let config: ConfigFile = toml::from_str(toml_str).unwrap();
        let mail = config.accounts[0].mail.as_ref().unwrap();
        assert_eq!(mail.sync_mode, SyncMode::Mirror);
    }

    #[test]
    fn subscribed_only_defaults_to_true() {
        let config: ConfigFile = toml::from_str(
            r#"
[[accounts]]
name = "test"
jmap_host = "x.com"
jmap_user = "u@x.com"
jmap_token = "t"

[accounts.mail]
path = "/tmp/test"
"#,
        )
        .unwrap();
        assert!(config.accounts[0].mail.as_ref().unwrap().subscribed_only);
    }

    #[test]
    fn path_expansion_home() {
        let home = std::env::var("HOME").unwrap();
        let config: ConfigFile = toml::from_str(
            r#"
[[accounts]]
name = "test"
jmap_host = "x.com"
jmap_user = "u@x.com"
jmap_token = "t"

[accounts.mail]
path = "~/test"
"#,
        )
        .unwrap();
        assert_eq!(
            config.accounts[0].mail.as_ref().unwrap().path,
            PathBuf::from(format!("{home}/test"))
        );
    }

    #[test]
    fn path_expansion_identity() {
        let config: ConfigFile = toml::from_str(
            r#"
[[accounts]]
name = "test"
jmap_host = "x.com"
jmap_user = "u@x.com"
jmap_token_file = "/already/absolute"
"#,
        )
        .unwrap();
        match &config.accounts[0].token {
            TokenSource::File { jmap_token_file } => {
                assert_eq!(jmap_token_file, &PathBuf::from("/already/absolute"))
            }
            _ => panic!("expected File"),
        }
    }

    #[test]
    fn config_file_loading() {
        let tmp = tempfile::tempdir().unwrap();
        let config_path = tmp.path().join("config.toml");
        let mut f = std::fs::File::create(&config_path).unwrap();
        writeln!(
            f,
            r#"
[[accounts]]
name = "test"
jmap_host = "x.com"
jmap_user = "u@x.com"
jmap_token = "t"
"#
        )
        .unwrap();
        drop(f);

        let config = ConfigFile::load(&config_path).unwrap();
        assert_eq!(config.accounts.len(), 1);
    }

    #[test]
    fn enabled_defaults_to_true_via_serde() {
        let config: ConfigFile = toml::from_str(
            r#"
[[accounts]]
name = "test"
jmap_host = "x.com"
jmap_user = "u@x.com"
jmap_token = "t"
"#,
        )
        .unwrap();
        assert!(config.accounts[0].enabled);
    }

    #[test]
    fn enabled_false_when_set() {
        let config: ConfigFile = toml::from_str(
            r#"
[[accounts]]
name = "test"
enabled = false
jmap_host = "x.com"
jmap_user = "u@x.com"
jmap_token = "t"
"#,
        )
        .unwrap();
        assert!(!config.accounts[0].enabled);
    }

    #[test]
    fn multiple_accounts() {
        let toml_str = r#"
[[accounts]]
name = "personal"
jmap_host = "a.com"
jmap_user = "u@a.com"
jmap_token = "t1"

[[accounts]]
name = "work"
jmap_host = "b.com"
jmap_user = "u@b.com"
jmap_token = "t2"
"#;
        let config: ConfigFile = toml::from_str(toml_str).unwrap();
        assert_eq!(config.accounts.len(), 2);
        assert_eq!(config.accounts[0].name, "personal");
        assert_eq!(config.accounts[1].name, "work");
    }

    #[test]
    fn config_file_not_found() {
        let result = ConfigFile::load(std::path::Path::new("/nonexistent/config.toml"));
        assert!(result.is_err());
    }

    #[test]
    fn invalid_toml() {
        let tmp = tempfile::tempdir().unwrap();
        let config_path = tmp.path().join("config.toml");
        std::fs::write(&config_path, "this is not toml [[[").unwrap();
        let result = ConfigFile::load(&config_path);
        assert!(result.is_err());
    }

    #[test]
    fn load_with_explicit_path() {
        let tmp = tempfile::tempdir().unwrap();
        let config_path = tmp.path().join("config.toml");
        let mut f = std::fs::File::create(&config_path).unwrap();
        writeln!(
            f,
            r#"
[[accounts]]
name = "test"
jmap_host = "x.com"
jmap_user = "u@x.com"
jmap_token = "t"
"#
        )
        .unwrap();
        drop(f);

        let overrides = Overrides { db_dir: None };
        let config = Config::load(Some(&config_path), &overrides).unwrap();
        assert_eq!(config.accounts.len(), 1);
        assert_eq!(config.accounts[0].name, "test");
        assert_eq!(config.accounts[0].jmap_host, "x.com");
    }

    #[test]
    fn load_uses_file_db_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let config_path = tmp.path().join("config.toml");
        let mut f = std::fs::File::create(&config_path).unwrap();
        writeln!(
            f,
            r#"
db_dir = "/from/file"

[[accounts]]
name = "test"
jmap_host = "x.com"
jmap_user = "u@x.com"
jmap_token = "t"
"#
        )
        .unwrap();
        drop(f);

        let overrides = Overrides { db_dir: None };
        let config = Config::load(Some(&config_path), &overrides).unwrap();
        assert_eq!(config.db_dir, PathBuf::from("/from/file"));
    }

    #[test]
    fn load_override_takes_precedence_over_file_db_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let config_path = tmp.path().join("config.toml");
        let mut f = std::fs::File::create(&config_path).unwrap();
        writeln!(
            f,
            r#"
db_dir = "/from/file"

[[accounts]]
name = "test"
jmap_host = "x.com"
jmap_user = "u@x.com"
jmap_token = "t"
"#
        )
        .unwrap();
        drop(f);

        let overrides = Overrides {
            db_dir: Some(PathBuf::from("/override")),
        };
        let config = Config::load(Some(&config_path), &overrides).unwrap();
        assert_eq!(config.db_dir, PathBuf::from("/override"));
    }

    #[test]
    fn load_with_missing_file_errors() {
        let overrides = Overrides { db_dir: None };
        let result = Config::load(
            Some(std::path::Path::new("/nonexistent/config.toml")),
            &overrides,
        );
        assert!(result.is_err());
    }

    #[test]
    fn load_with_no_path_errors() {
        let overrides = Overrides { db_dir: None };
        let result = Config::load(None, &overrides);
        assert!(result.is_err());
    }

    #[test]
    fn poll_interval_defaults_to_300() {
        let config: ConfigFile = toml::from_str(
            r#"
[[accounts]]
name = "test"
jmap_host = "x.com"
jmap_user = "u@x.com"
jmap_token = "t"
"#,
        )
        .unwrap();
        assert_eq!(config.accounts[0].poll_interval_secs, 300);
    }

    #[test]
    fn poll_interval_explicit_override() {
        let config: ConfigFile = toml::from_str(
            r#"
[[accounts]]
name = "test"
jmap_host = "x.com"
jmap_user = "u@x.com"
jmap_token = "t"
poll_interval_secs = 0
"#,
        )
        .unwrap();
        assert_eq!(config.accounts[0].poll_interval_secs, 0);
    }
}
