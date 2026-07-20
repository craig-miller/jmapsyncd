# jmapsyncd — zentoo fork

This branch (`zentoo`) is a downstream fork of [`julianandrews/jmapsyncd`](https://github.com/julianandrews/jmapsyncd). It rebases onto upstream `main` periodically.

Upstream's [`PLAN.md`](./PLAN.md) is designed but only partially implemented: Tasks 1 (config) and 2 (database schema) are complete; Tasks 3 (core sync engine) and 4 (daemon loop + SSE + polling) are stubs. This fork implements those two tasks so the daemon actually syncs mail.

JMAP ↔ Maildir sync daemon.

## Quick start

```bash
cargo install --path .

mkdir -p ~/.config/jmapsyncd
cat > ~/.config/jmapsyncd/config.toml << 'EOF'
[[accounts]]
name = "personal"
jmap_host = "api.fastmail.com"
jmap_user = "user@fastmail.com"
jmap_token = "your-api-token"

[accounts.mail]
path = "~/Mail/personal"
EOF

jmapsyncd sync      # one-shot sync
jmapsyncd daemon    # long-running daemon
```

## Configuration

Config file: `~/.config/jmapsyncd/config.toml` (set via `--config` or `JMAPSYNCD_CONFIG`).

```toml
# Global settings
db_dir = "~/.local/share/jmapsyncd"

[[accounts]]
name = "personal"
enabled = true
jmap_host = "api.fastmail.com"
jmap_user = "user@fastmail.com"

# Token — exactly one of:
jmap_token = ""                # inline
# jmap_token_file = "~/.config/jmapsyncd/tokens/personal"  # file path
# jmap_token_cmd = "get-token"                              # command

timeout_secs = 30
poll_interval_secs = 300       # zentoo fork: SSE fallback polling; 0 disables

[accounts.mail]
path = "~/Mail/personal"
sync_mode = "mirror"         # "mirror" (pull-only) or "two_way"
subscribed_only = true       # only sync subscribed mailboxes
box_filter = ["INBOX"]       # override subscribed_only with globs

  [accounts.mail.tls]
  ca_file = "/etc/ssl/certs/ca-certificates.crt"
  # client_cert = "~/.config/jmapsyncd/cert.pem"
  # client_key  = "~/.config/jmapsyncd/key.pem"
  # fingerprint = "SHA256:..."

[[accounts.mail.box_mapping]]
remote = "Sent Items"
local  = "Sent"
```

All paths support `~`, `$VAR`, and `${VAR:-default}` expansion.

## Building

```bash
cargo build --release
./target/release/jmapsyncd --help
```

## What the fork adds

A stack of single-purpose, rebase-friendly commits on top of upstream:

1. **`jmap-client` 0.2 → 0.4.2 migration.** Bumps the Stalwart JMAP client library to its current release. Needed by the SSE-push loop and incremental `Email/changes` fetch below.
2. **Task 3: core sync engine.** `sync_mailboxes` — three-way diff of the mailbox tree against `Mailbox/get` / `Mailbox/changes`. `sync_emails` — per-mailbox `Email/query` + `Email/get` + `Blob/download`, JMAP-keyword ↔ Maildir-flag mapping (`$seen`/`$flagged`/`$answered`/`$draft` → `S`/`F`/`R`/`D`), primary-mailbox selection per PLAN.md's role priority. Mirror-strict deletion (server delete → local delete); btrfs snapshots are the safety net.
3. **Task 4: daemon loop + SSE + fallback polling.** Per-account `tokio` task combining `EventSource` push notifications with a `poll_interval_secs` timer (default 300s, zero disables). Graceful `SIGTERM` / `SIGINT` shutdown via `tokio::signal` + a shared `CancellationToken`. `sync` one-shot with `--dry-run`.

Landing intent: upstream PRs (one per phase) as they mature. This branch remains the shipping cut for the [zentoo overlay](https://github.com/craig-miller/zentoo-overlay)'s `net-mail/jmapsyncd` ebuild until upstream cuts a release.
