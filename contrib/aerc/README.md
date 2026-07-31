# aerc integration for jmapsyncd

Reference configuration + compose template for wiring
[aerc](https://aerc-mail.org) into `jmapsyncd` + `jmapqueue` as a full
send/receive terminal MUA.

## What this is

- **jmapsyncd** — daemon that mirrors a JMAP account (Fastmail, Stalwart,
  Cyrus, …) to a local Maildir at `~/mail/<account>/`.
- **jmapqueue** — sendmail(1)-compat wrapper that spools outbound mail to
  `~/.local/state/jmapsyncd/<account>/spool/`; the daemon submits it via
  JMAP `EmailSubmission/set` (immediate and scheduled).
- **This directory** — aerc glue: a compose template that seeds the
  `X-JMAP-Send-At` scheduling header, accounts.conf examples for both
  common backends, and an optional post-send notification hook.

## Prerequisites

- `jmapsyncd` and `jmapqueue` installed and on PATH (see the top-level
  README for source-install or your distro's package).
- `aerc` ≥ 0.20.  Notmuch support in aerc is a compile-time option; if you
  pick the notmuch backend below, make sure your aerc was built with it
  (Gentoo: `USE=notmuch`; Arch AUR: `aerc-notmuch`).

## Backend choice

Two flavors, both wire the same send-side (`outgoing = jmapqueue` under
each account section):

1. **notmuch** — `accounts.conf.notmuch.example`.  Sub-second full-text
   search even on large stores; requires notmuch installed and
   `~/.config/notmuch/default/config` pointing at the Maildir root;
   folders map via a `map.conf` query file.
2. **maildir** — `accounts.conf.maildir.example`.  No notmuch dependency;
   aerc walks the Maildir to discover folders; search is aerc's built-in
   (fine for smaller stores, slower on 100k+ messages).

You can switch later — the JMAP integration bits (`sendmail`,
`trusted-authres`, the template) don't move between the two.

## Setup: notmuch backend

1. Install notmuch.
2. Write `~/.config/notmuch/default/config`:
   ```
   [database]
   path=~/mail/personal

   [new]
   tags=unread;inbox;
   ```
   (Adjust `path` to match your `jmapsyncd` `mail.path`.)
3. Initial index: `notmuch new` (subsequent reindexing is triggered by
   `jmapsyncd`'s `post_sync_hook` if you wire one — see the top-level
   README).
4. Copy the accounts example:
   ```
   cp accounts.conf.notmuch.example ~/.config/aerc/accounts.conf
   ```
   Edit the personal details (`from`, plus rename `[personal]` to whatever
   account name you prefer).
5. Write `~/.config/aerc/map.conf` — folder to notmuch-query map:
   ```
   INBOX   = folder:personal/Inbox
   Archive = folder:personal/Archive
   Drafts  = folder:personal/Drafts
   Sent    = folder:personal/Sent
   Spam    = folder:personal/Spam
   Trash   = folder:personal/Trash
   Unread  = tag:unread
   ```
   (`folder:` names match the Maildir subdirectories jmapsyncd creates.)
6. Continue to **Wire the template**.

## Setup: maildir backend

1. Copy the accounts example:
   ```
   cp accounts.conf.maildir.example ~/.config/aerc/accounts.conf
   ```
   Edit the personal details and the `source` path if your `mail.path`
   differs.
2. Continue to **Wire the template**.

## Wire the template

1. Copy the template file:
   ```
   mkdir -p ~/.config/aerc/templates
   cp templates/new_message ~/.config/aerc/templates/new_message
   ```
2. Merge `aerc.conf.snippet` into `~/.config/aerc/aerc.conf` — adds:
   ```
   [compose]
   edit-headers = true

   [templates]
   new-message = new_message
   ```

Start aerc, hit `:compose`.  Your editor opens on the compose buffer with
the full RFC 5322 header block visible, including:

```
X-Mailer: aerc 0.20.x
X-JMAP-Send-At: send-now
```

Write, `:send` — `jmapqueue` spools it, the daemon submits via JMAP.

## Scheduled send

Edit the `X-JMAP-Send-At` value in the compose buffer:

| Value | Effect |
|---|---|
| `send-now` (or delete the header entirely) | Deliver immediately |
| ISO8601 timestamp with timezone            | Schedule delivery for that time |

Timezone is required.  Examples:

```
X-JMAP-Send-At: 2026-08-05T09:00:00Z            (UTC)
X-JMAP-Send-At: 2026-08-05T09:00:00-05:00       (EST)
X-JMAP-Send-At: 2026-08-05T09:00:00+02:00       (CEST)
```

The JMAP server (Fastmail, Stalwart, …) holds the message until the
timestamp.  `jmapqueue` reads and strips the header before submit.  The
filing to Sent happens in the same JMAP request as the submission
(`EmailSubmission.onSuccessUpdateEmail`), so no unfiled-Draft window.

## Cancel a scheduled send

Works up until the send-at timestamp:

```
jmapsyncd submissions list --state scheduled
jmapsyncd submissions cancel <submission-id>
```

To cancel a message that's still spooled locally (daemon offline, or
scheduled sends the server hasn't accepted yet), remove the spool file:

```
jmapsyncd submissions list --state pending
jmapsyncd submissions purge --state pending <ulid>
```

## Optional: post-send notification

```
cp mail-sent-hook.sh ~/.config/aerc/mail-sent-hook.sh
chmod +x ~/.config/aerc/mail-sent-hook.sh
```

Wire in `~/.config/aerc/aerc.conf`:

```
[hooks]
mail-sent = ~/.config/aerc/mail-sent-hook.sh
```

The hook receives `AERC_ACCOUNT`, `AERC_FROM_*`, `AERC_SUBJECT`, `AERC_TO`.
"Sent" from aerc's perspective means `jmapqueue` returned success — i.e.
the message is spooled locally.  Actual JMAP delivery happens on the next
daemon drain.

## Where files land

| File | Destination |
|---|---|
| `templates/new_message`               | `~/.config/aerc/templates/new_message` |
| `aerc.conf.snippet` (merged)          | `~/.config/aerc/aerc.conf` |
| `accounts.conf.<backend>.example`     | `~/.config/aerc/accounts.conf` |
| `mail-sent-hook.sh` (optional)        | `~/.config/aerc/mail-sent-hook.sh` |

## Where jmapqueue's spool + logs live

- Spool root:  `~/.local/state/jmapsyncd/<account>/spool/{pending,sent,failed}/`
  - `pending/` — accepted from jmapqueue, not yet submitted (daemon
    offline, backoff, or scheduled send waiting for the timestamp).
  - `sent/`    — daemon-confirmed delivered.  Kept as a local record;
    housekeep with `jmapsyncd submissions purge --state sent
    --older-than 7d`.
  - `failed/`  — JMAP rejected (bad envelope, quota, etc.).  Paired
    `<ulid>.error.json` sidecar has the server response.
- Daemon log: `journalctl --user -u jmapsyncd` (systemd/OpenRC unit) or
  `~/.local/state/jmapsyncd/jmapsyncd.log`.

## Beyond aerc

`contrib/<mua>/` is the pattern.  Neomutt, mutt, meli, or a hand-rolled
client that just wants `popen("sendmail -t")` all work the same — set
`outgoing = jmapqueue`, drop a template with the `X-JMAP-Send-At` header,
done.  The daemon and wrapper are MUA-agnostic; this directory is one
worked example.
