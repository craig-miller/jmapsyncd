use crate::db::{
    Database, generate_id,
    models::{EmailMailboxRow, EmailRow, MailboxRow},
};
use anyhow::{Context, Result};
use jmap_client::Get;
use jmap_client::client::Client;
use jmap_client::core::response::EmailGetResponse;
use jmap_client::email::{self, Email, Property};
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

// Fastmail returns Email/query in pages; 256 is a reasonable middle ground
// between per-round-trip cost and total request count.
const EMAIL_QUERY_PAGE_SIZE: usize = 256;

// Email/get chunk size — 100 keeps individual responses small enough that a
// mid-run error only loses a small batch. Increase if latency-bound.
const EMAIL_GET_CHUNK_SIZE: usize = 100;

// JMAP role -> primary-mailbox priority for emails in multiple mailboxes.
// From PLAN.md: inbox > sent > drafts > archive > trash > junk > no-role.
const ROLE_PRIORITY: &[&str] = &["inbox", "sent", "drafts", "archive", "trash", "junk"];

// JMAP keyword -> Maildir flag mapping.
const KEYWORD_MAP: &[(&str, char)] = &[
    ("$seen", 'S'),
    ("$flagged", 'F'),
    ("$answered", 'R'),
    ("$draft", 'D'),
];

#[derive(Default, Debug, PartialEq, Eq)]
pub struct EmailSyncStats {
    pub created: usize,
    pub updated: usize,
    pub moved: usize,
    pub deleted: usize,
    pub bytes_downloaded: u64,
}

pub async fn sync_emails(
    client: &Client,
    db: &Database,
    mailboxes: &[MailboxRow],
    mail_root: &Path,
    dry_run: bool,
) -> Result<EmailSyncStats> {
    if mailboxes.is_empty() {
        debug!("no mailboxes materialized; skipping email sync");
        return Ok(EmailSyncStats::default());
    }

    // jmap_id -> MailboxRow. Skip mailboxes without a jmap_id (locally-
    // created edge cases; shouldn't happen after B.2 but be defensive).
    let mailbox_by_jmap: HashMap<String, &MailboxRow> = mailboxes
        .iter()
        .filter_map(|m| m.jmap_id.as_ref().map(|j| (j.clone(), m)))
        .collect();

    // TODO(perf): switch to Email/changes when we have a stored state
    // cursor. Full-fetch every sync scales poorly past a few thousand
    // emails; needs the sync_state kv table + first-sync state anchor.
    let server_ids = fetch_all_email_ids(client, &mailbox_by_jmap).await?;

    let local_by_jmap: HashMap<String, EmailRow> = db
        .get_all_emails()?
        .into_iter()
        .filter_map(|e| e.jmap_id.clone().map(|j| (j, e)))
        .collect();

    let mut stats = EmailSyncStats::default();

    // Deletes: any local row whose jmap_id is not in the server set.
    for (jmap_id, row) in &local_by_jmap {
        if !server_ids.contains(jmap_id) {
            delete_email(db, mail_root, row, dry_run)?;
            stats.deleted += 1;
        }
    }

    // Hydrate + apply in chunks. Sort ids for deterministic behaviour + so
    // .chunks() is stable across runs (helps test reproduction).
    let mut hydration_order: Vec<String> = server_ids.iter().cloned().collect();
    hydration_order.sort();
    for chunk in hydration_order.chunks(EMAIL_GET_CHUNK_SIZE) {
        let hydrated = fetch_email_chunk(client, chunk).await?;
        for email in hydrated {
            apply_email(
                client,
                db,
                mail_root,
                &mailbox_by_jmap,
                &local_by_jmap,
                &email,
                &mut stats,
                dry_run,
            )
            .await?;
        }
    }

    info!(
        "{}email sync: {} new, {} updated, {} moved, {} deleted, {} bytes downloaded",
        if dry_run { "[dry-run] " } else { "" },
        stats.created,
        stats.updated,
        stats.moved,
        stats.deleted,
        stats.bytes_downloaded,
    );
    Ok(stats)
}

// ---------------------------------------------------------------------------
// JMAP fetch
// ---------------------------------------------------------------------------

async fn fetch_all_email_ids(
    client: &Client,
    mailbox_by_jmap: &HashMap<String, &MailboxRow>,
) -> Result<HashSet<String>> {
    let mut ids = HashSet::new();
    for mailbox_jmap_id in mailbox_by_jmap.keys() {
        let per_mailbox = page_query_ids(client, mailbox_jmap_id).await?;
        debug!(
            "mailbox {mailbox_jmap_id}: {} email ids",
            per_mailbox.len()
        );
        for id in per_mailbox {
            ids.insert(id);
        }
    }
    Ok(ids)
}

async fn page_query_ids(client: &Client, mailbox_jmap_id: &str) -> Result<Vec<String>> {
    let mut all = Vec::new();
    let mut position: i32 = 0;
    loop {
        let mut request = client.build();
        crate::jmap::restrict_using(&mut request);
        request
            .query_email()
            .filter(email::query::Filter::in_mailbox(mailbox_jmap_id.to_string()))
            .position(position)
            .limit(EMAIL_QUERY_PAGE_SIZE)
            .calculate_total(false);
        let mut resp = request
            .send_query_email()
            .await
            .with_context(|| format!("Email/query mailbox={mailbox_jmap_id} pos={position}"))?;
        let page = resp.take_ids();
        let page_len = page.len();
        all.extend(page);
        if page_len < EMAIL_QUERY_PAGE_SIZE {
            break;
        }
        position += page_len as i32;
    }
    Ok(all)
}

async fn fetch_email_chunk(client: &Client, ids: &[String]) -> Result<Vec<Email<Get>>> {
    let mut request = client.build();
    crate::jmap::restrict_using(&mut request);
    request.get_email().ids(ids.iter().cloned()).properties([
        Property::Id,
        Property::BlobId,
        Property::MailboxIds,
        Property::Keywords,
        Property::Size,
        Property::ReceivedAt,
        Property::MessageId,
        Property::Subject,
    ]);
    let mut resp: EmailGetResponse = request
        .send_single()
        .await
        .with_context(|| format!("Email/get chunk of {} ids", ids.len()))?;
    let not_found = resp.take_not_found();
    if !not_found.is_empty() {
        warn!(
            "Email/get reported {} ids notFound (possibly deleted mid-sync)",
            not_found.len()
        );
    }
    Ok(resp.take_list())
}

// ---------------------------------------------------------------------------
// Three-state dispatch
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn apply_email(
    client: &Client,
    db: &Database,
    mail_root: &Path,
    mailbox_by_jmap: &HashMap<String, &MailboxRow>,
    local_by_jmap: &HashMap<String, EmailRow>,
    email: &Email<Get>,
    stats: &mut EmailSyncStats,
    dry_run: bool,
) -> Result<()> {
    let jmap_id = match email.id() {
        Some(id) => id.to_string(),
        None => {
            warn!("Email/get returned entry with no id; skipping");
            return Ok(());
        }
    };
    let mailbox_jmap_ids: Vec<&str> = email.mailbox_ids();
    let primary_mailbox = match pick_primary_mailbox(&mailbox_jmap_ids, mailbox_by_jmap) {
        Some(m) => m,
        None => {
            // All of this email's mailboxes are filtered out of our sync set
            // (subscribed_only / box_filter). If there's a local row, drop
            // it — the email is no longer in scope.
            if let Some(existing) = local_by_jmap.get(&jmap_id) {
                debug!(
                    "email {jmap_id} has no in-scope mailbox; treating as delete"
                );
                delete_email(db, mail_root, existing, dry_run)?;
                stats.deleted += 1;
            }
            return Ok(());
        }
    };

    let keywords = email.keywords();
    let flags = keywords_to_flag_string(&keywords);
    let keywords_json = serialize_keywords(&keywords);
    let received_ts = email.received_at().unwrap_or_else(unix_now);

    match local_by_jmap.get(&jmap_id) {
        None => {
            let bytes = insert_email(
                client,
                db,
                mail_root,
                &jmap_id,
                email,
                primary_mailbox,
                &mailbox_jmap_ids,
                mailbox_by_jmap,
                &flags,
                &keywords_json,
                received_ts,
                dry_run,
            )
            .await?;
            stats.created += 1;
            stats.bytes_downloaded += bytes;
        }
        Some(existing) => {
            let primary_changed = existing.primary_mailbox != primary_mailbox.id;
            let flags_changed = current_flags_from_path(&existing.file_path) != flags;
            let keywords_changed = existing.keywords.as_deref() != Some(keywords_json.as_str());
            let memberships_changed =
                memberships_differ(db, &existing.id, &mailbox_jmap_ids, mailbox_by_jmap)?;

            if primary_changed {
                move_email(
                    db,
                    mail_root,
                    existing,
                    primary_mailbox,
                    &mailbox_jmap_ids,
                    mailbox_by_jmap,
                    &flags,
                    &keywords_json,
                    received_ts,
                    dry_run,
                )?;
                stats.moved += 1;
            } else if flags_changed || keywords_changed || memberships_changed {
                update_email_in_place(
                    db,
                    mail_root,
                    existing,
                    primary_mailbox,
                    &mailbox_jmap_ids,
                    mailbox_by_jmap,
                    &flags,
                    &keywords_json,
                    dry_run,
                )?;
                stats.updated += 1;
            }
            // else: no-op, nothing changed
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// INSERT
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn insert_email(
    client: &Client,
    db: &Database,
    mail_root: &Path,
    jmap_id: &str,
    email: &Email<Get>,
    primary_mailbox: &MailboxRow,
    mailbox_jmap_ids: &[&str],
    mailbox_by_jmap: &HashMap<String, &MailboxRow>,
    flags: &str,
    keywords_json: &str,
    received_ts: i64,
    dry_run: bool,
) -> Result<u64> {
    let blob_id = email
        .blob_id()
        .with_context(|| format!("email {jmap_id} has no blobId"))?;
    // Blob/download still runs under dry-run so stats.bytes_downloaded
    // reflects the real transfer size we'd have paid for. Only the disk
    // write is skipped inside download_and_write.
    let (rel_path, bytes) = download_and_write(
        client,
        blob_id,
        mail_root,
        &primary_mailbox.path,
        received_ts,
        flags,
        dry_run,
    )
    .await?;

    if dry_run {
        debug!(
            "[dry-run] would create email {} at {} ({} bytes, subject: {:?})",
            jmap_id,
            rel_path,
            bytes,
            email.subject()
        );
        return Ok(bytes);
    }

    let row = EmailRow {
        id: generate_id(),
        jmap_id: Some(jmap_id.to_string()),
        message_id: email.message_id().and_then(|list| list.first()).cloned(),
        file_path: rel_path.clone(),
        primary_mailbox: primary_mailbox.id.clone(),
        keywords: Some(keywords_json.to_string()),
        jmap_state: None,
        size: Some(email.size() as i64),
        last_sync: Some(unix_now()),
        is_dirty: false,
    };
    db.insert_email(&row)?;
    insert_memberships(db, &row.id, &primary_mailbox.id, mailbox_jmap_ids, mailbox_by_jmap)?;
    debug!(
        "created email {} at {} (subject: {:?})",
        jmap_id, rel_path, email.subject()
    );
    Ok(bytes)
}

// ---------------------------------------------------------------------------
// UPDATE — same primary, keyword/membership drift
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn update_email_in_place(
    db: &Database,
    mail_root: &Path,
    existing: &EmailRow,
    primary_mailbox: &MailboxRow,
    mailbox_jmap_ids: &[&str],
    mailbox_by_jmap: &HashMap<String, &MailboxRow>,
    flags: &str,
    keywords_json: &str,
    dry_run: bool,
) -> Result<()> {
    // Rename file in place if flags changed. Filename base stays; suffix
    // updates to the new :2,{flags} string.
    let new_rel = rebuild_path_with_flags(&existing.file_path, flags);
    if dry_run {
        debug!(
            "[dry-run] would update email {} in place ({} -> {})",
            existing.jmap_id.as_deref().unwrap_or("<no jmap_id>"),
            existing.file_path,
            new_rel
        );
        return Ok(());
    }
    if new_rel != existing.file_path {
        let old_abs = mail_root.join(&existing.file_path);
        let new_abs = mail_root.join(&new_rel);
        if old_abs.exists() {
            std::fs::rename(&old_abs, &new_abs).with_context(|| {
                format!(
                    "renaming flags {} -> {}",
                    old_abs.display(),
                    new_abs.display()
                )
            })?;
        }
    }

    let mut new_row = existing.clone();
    new_row.file_path = new_rel;
    new_row.keywords = Some(keywords_json.to_string());
    new_row.last_sync = Some(unix_now());
    db.update_email(&new_row)?;

    reconcile_memberships(
        db,
        &existing.id,
        &primary_mailbox.id,
        mailbox_jmap_ids,
        mailbox_by_jmap,
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// MOVE — primary mailbox changed
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn move_email(
    db: &Database,
    mail_root: &Path,
    existing: &EmailRow,
    new_primary: &MailboxRow,
    mailbox_jmap_ids: &[&str],
    mailbox_by_jmap: &HashMap<String, &MailboxRow>,
    flags: &str,
    keywords_json: &str,
    received_ts: i64,
    dry_run: bool,
) -> Result<()> {
    // Reuse the existing file's UUID part to preserve dedup identity if the
    // caller ever grew hardlink logic; the ts:{flags} portions can change.
    let uuid_part = extract_uuid(&existing.file_path).unwrap_or_else(new_uuid_string);
    let new_basename = format!("{received_ts}.{uuid_part}");
    let new_rel = format!(
        "{}/cur/{new_basename}:2,{flags}",
        new_primary.path
    );
    if dry_run {
        debug!(
            "[dry-run] would move email {} ({} -> {})",
            existing.jmap_id.as_deref().unwrap_or("<no jmap_id>"),
            existing.file_path,
            new_rel
        );
        return Ok(());
    }
    let new_abs = mail_root.join(&new_rel);
    if let Some(parent) = new_abs.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("mkdir -p {}", parent.display()))?;
    }

    let old_abs = mail_root.join(&existing.file_path);
    if old_abs.exists() {
        std::fs::rename(&old_abs, &new_abs)
            .with_context(|| format!("moving {} -> {}", old_abs.display(), new_abs.display()))?;
    }

    let mut new_row = existing.clone();
    new_row.file_path = new_rel;
    new_row.primary_mailbox = new_primary.id.clone();
    new_row.keywords = Some(keywords_json.to_string());
    new_row.last_sync = Some(unix_now());
    db.update_email(&new_row)?;

    reconcile_memberships(
        db,
        &existing.id,
        &new_primary.id,
        mailbox_jmap_ids,
        mailbox_by_jmap,
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

fn delete_email(
    db: &Database,
    mail_root: &Path,
    row: &EmailRow,
    dry_run: bool,
) -> Result<()> {
    let abs = mail_root.join(&row.file_path);
    if dry_run {
        debug!(
            "[dry-run] would delete email {} at {}",
            row.jmap_id.as_deref().unwrap_or("<no jmap_id>"),
            abs.display()
        );
        return Ok(());
    }
    if abs.exists() {
        if let Err(e) = std::fs::remove_file(&abs) {
            warn!("could not remove {} ({e})", abs.display());
        }
    }
    db.delete_email(&row.id)?;
    debug!("deleted email {}", row.jmap_id.as_deref().unwrap_or("<no jmap_id>"));
    Ok(())
}

// ---------------------------------------------------------------------------
// Memberships
// ---------------------------------------------------------------------------

fn insert_memberships(
    db: &Database,
    email_id: &str,
    primary_local_id: &str,
    mailbox_jmap_ids: &[&str],
    mailbox_by_jmap: &HashMap<String, &MailboxRow>,
) -> Result<()> {
    for jid in mailbox_jmap_ids {
        if let Some(mb) = mailbox_by_jmap.get(*jid) {
            db.insert_email_mailbox(&EmailMailboxRow {
                email_id: email_id.to_string(),
                mailbox_id: mb.id.clone(),
                is_primary: mb.id == primary_local_id,
            })?;
        }
    }
    Ok(())
}

fn reconcile_memberships(
    db: &Database,
    email_id: &str,
    primary_local_id: &str,
    mailbox_jmap_ids: &[&str],
    mailbox_by_jmap: &HashMap<String, &MailboxRow>,
) -> Result<()> {
    // Delete-all + insert-all: correct + cheap for typical <10-mailbox
    // memberships. Diff-based reconcile is fussier to write and buys
    // little at these sizes.
    let existing = db.get_email_mailboxes_by_email(email_id)?;
    for row in existing {
        db.delete_email_mailbox(&row.email_id, &row.mailbox_id)?;
    }
    insert_memberships(db, email_id, primary_local_id, mailbox_jmap_ids, mailbox_by_jmap)
}

fn memberships_differ(
    db: &Database,
    email_id: &str,
    server_mailbox_jmap_ids: &[&str],
    mailbox_by_jmap: &HashMap<String, &MailboxRow>,
) -> Result<bool> {
    let existing: HashSet<String> = db
        .get_email_mailboxes_by_email(email_id)?
        .into_iter()
        .map(|r| r.mailbox_id)
        .collect();
    let expected: HashSet<String> = server_mailbox_jmap_ids
        .iter()
        .filter_map(|jid| mailbox_by_jmap.get(*jid).map(|m| m.id.clone()))
        .collect();
    Ok(existing != expected)
}

// ---------------------------------------------------------------------------
// Primary mailbox selection
// ---------------------------------------------------------------------------

fn pick_primary_mailbox<'a>(
    mailbox_jmap_ids: &[&str],
    mailbox_by_jmap: &HashMap<String, &'a MailboxRow>,
) -> Option<&'a MailboxRow> {
    let mut candidates: Vec<&MailboxRow> = mailbox_jmap_ids
        .iter()
        .filter_map(|jid| mailbox_by_jmap.get(*jid).copied())
        .collect();
    if candidates.is_empty() {
        return None;
    }
    candidates.sort_by(|a, b| {
        role_rank(a.role.as_deref())
            .cmp(&role_rank(b.role.as_deref()))
            .then(
                a.sort_order
                    .unwrap_or(i64::MAX)
                    .cmp(&b.sort_order.unwrap_or(i64::MAX)),
            )
            .then_with(|| a.name.cmp(&b.name))
    });
    candidates.into_iter().next()
}

fn role_rank(role: Option<&str>) -> usize {
    role.and_then(|r| ROLE_PRIORITY.iter().position(|p| *p == r))
        .unwrap_or(ROLE_PRIORITY.len())
}

// ---------------------------------------------------------------------------
// Keywords + Maildir flags
// ---------------------------------------------------------------------------

fn keywords_to_flag_string(keywords: &[&str]) -> String {
    let mut flags: Vec<char> = keywords
        .iter()
        .filter_map(|k| KEYWORD_MAP.iter().find(|(kw, _)| kw == k).map(|(_, f)| *f))
        .collect();
    flags.sort();
    flags.dedup();
    flags.into_iter().collect()
}

fn serialize_keywords(keywords: &[&str]) -> String {
    // Sort for deterministic output — round-trip stability + easier diffing.
    let mut sorted: Vec<&&str> = keywords.iter().collect();
    sorted.sort();
    let map: std::collections::BTreeMap<&str, bool> =
        sorted.iter().map(|k| (**k, true)).collect();
    serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string())
}

fn current_flags_from_path(path: &str) -> String {
    // Filename format: {ts}.{uuid}:2,{flags}
    path.rsplit_once(":2,")
        .map(|(_, tail)| tail.to_string())
        .unwrap_or_default()
}

fn rebuild_path_with_flags(path: &str, flags: &str) -> String {
    match path.rsplit_once(":2,") {
        Some((base, _)) => format!("{base}:2,{flags}"),
        None => format!("{path}:2,{flags}"),
    }
}

fn extract_uuid(path: &str) -> Option<String> {
    let filename = path.rsplit('/').next()?;
    let base = filename.split(":2,").next()?;
    base.split_once('.').map(|(_, uuid)| uuid.to_string())
}

fn new_uuid_string() -> String {
    uuid::Uuid::new_v4().to_string()
}

// ---------------------------------------------------------------------------
// Blob download + atomic write
// ---------------------------------------------------------------------------

async fn download_and_write(
    client: &Client,
    blob_id: &str,
    mail_root: &Path,
    primary_path: &str,
    ts: i64,
    flags: &str,
    dry_run: bool,
) -> Result<(String, u64)> {
    let bytes = client
        .download(blob_id)
        .await
        .with_context(|| format!("Blob/download {blob_id}"))?;
    let size = bytes.len() as u64;

    let uuid = new_uuid_string();
    let basename = format!("{ts}.{uuid}");
    let mailbox_dir = mail_root.join(primary_path);
    let final_rel = format!("{primary_path}/cur/{basename}:2,{flags}");

    if dry_run {
        // Blob downloaded (for realistic bytes stat) but no mkdir/write/rename.
        return Ok((final_rel, size));
    }

    let tmp_abs = mailbox_dir.join("tmp").join(&basename);
    let final_abs = mail_root.join(&final_rel);

    std::fs::create_dir_all(mailbox_dir.join("tmp"))
        .with_context(|| format!("mkdir tmp for {}", mailbox_dir.display()))?;
    std::fs::create_dir_all(mailbox_dir.join("cur"))
        .with_context(|| format!("mkdir cur for {}", mailbox_dir.display()))?;
    std::fs::write(&tmp_abs, &bytes)
        .with_context(|| format!("writing {}", tmp_abs.display()))?;
    std::fs::rename(&tmp_abs, &final_abs)
        .with_context(|| format!("finalizing {} -> {}", tmp_abs.display(), final_abs.display()))?;

    Ok((final_rel, size))
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[allow(dead_code)]
fn abs_from_rel(mail_root: &Path, rel: &str) -> PathBuf {
    mail_root.join(rel)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn mb(id: &str, role: Option<&str>, sort_order: i64, name: &str) -> MailboxRow {
        MailboxRow {
            id: id.to_string(),
            jmap_id: Some(format!("j-{id}")),
            name: name.to_string(),
            parent_id: None,
            role: role.map(String::from),
            sort_order: Some(sort_order),
            path: name.to_string(),
            jmap_state: None,
        }
    }

    fn build_map<'a>(mbs: &'a [MailboxRow]) -> HashMap<String, &'a MailboxRow> {
        mbs.iter()
            .filter_map(|m| m.jmap_id.as_ref().map(|j| (j.clone(), m)))
            .collect()
    }

    #[test]
    fn keywords_to_flags_alphabetical_and_dedup() {
        assert_eq!(
            keywords_to_flag_string(&["$flagged", "$seen"]),
            "FS"
        );
        assert_eq!(keywords_to_flag_string(&["$draft"]), "D");
        // Unknown keywords ignored.
        assert_eq!(
            keywords_to_flag_string(&["$muted", "$seen", "custom"]),
            "S"
        );
        // Duplicates collapse (defensive; server shouldn't return dups).
        assert_eq!(
            keywords_to_flag_string(&["$seen", "$seen"]),
            "S"
        );
        // Empty in, empty out.
        assert_eq!(keywords_to_flag_string(&[]), "");
    }

    #[test]
    fn serialize_keywords_is_sorted_json() {
        let json = serialize_keywords(&["$seen", "$flagged"]);
        // BTreeMap keys land alphabetical: $flagged then $seen.
        assert_eq!(json, r#"{"$flagged":true,"$seen":true}"#);
        assert_eq!(serialize_keywords(&[]), "{}");
    }

    #[test]
    fn primary_mailbox_by_role_priority() {
        let mbs = vec![
            mb("archive", Some("archive"), 10, "Archive"),
            mb("inbox", Some("inbox"), 20, "INBOX"),
            mb("sent", Some("sent"), 5, "Sent"),
        ];
        let map = build_map(&mbs);
        let ids = vec!["j-archive", "j-inbox", "j-sent"];
        let winner = pick_primary_mailbox(&ids, &map).unwrap();
        assert_eq!(winner.id, "inbox"); // inbox wins over sent + archive
    }

    #[test]
    fn primary_mailbox_sort_order_fallback_when_no_role() {
        let mbs = vec![
            mb("a", None, 10, "AAA"),
            mb("b", None, 5, "BBB"),
        ];
        let map = build_map(&mbs);
        let ids = vec!["j-a", "j-b"];
        let winner = pick_primary_mailbox(&ids, &map).unwrap();
        // Neither has a role → lowest sort_order wins.
        assert_eq!(winner.id, "b");
    }

    #[test]
    fn primary_mailbox_alphabetical_tiebreak() {
        let mbs = vec![
            mb("a", None, 5, "BBB"),
            mb("b", None, 5, "AAA"),
        ];
        let map = build_map(&mbs);
        let ids = vec!["j-a", "j-b"];
        let winner = pick_primary_mailbox(&ids, &map).unwrap();
        assert_eq!(winner.name, "AAA"); // alphabetical on tied sort_order
    }

    #[test]
    fn primary_mailbox_returns_none_when_all_filtered_out() {
        let mbs: Vec<MailboxRow> = Vec::new();
        let map = build_map(&mbs);
        let ids = vec!["j-notsynced"];
        assert!(pick_primary_mailbox(&ids, &map).is_none());
    }

    #[test]
    fn primary_mailbox_skips_unsynced_and_picks_from_synced_only() {
        let mbs = vec![mb("archive", Some("archive"), 10, "Archive")];
        let map = build_map(&mbs);
        // Email is in one synced + one unsynced mailbox → picks the synced one.
        let ids = vec!["j-archive", "j-hidden"];
        let winner = pick_primary_mailbox(&ids, &map).unwrap();
        assert_eq!(winner.id, "archive");
    }

    #[test]
    fn current_flags_from_path_parses() {
        assert_eq!(
            current_flags_from_path("INBOX/cur/12345.abc:2,FRS"),
            "FRS"
        );
        assert_eq!(current_flags_from_path("no-flags"), "");
    }

    #[test]
    fn rebuild_path_swaps_flags() {
        assert_eq!(
            rebuild_path_with_flags("INBOX/cur/12345.abc:2,FRS", "S"),
            "INBOX/cur/12345.abc:2,S"
        );
        // No existing suffix -> append.
        assert_eq!(
            rebuild_path_with_flags("INBOX/cur/12345.abc", "S"),
            "INBOX/cur/12345.abc:2,S"
        );
    }

    #[test]
    fn extract_uuid_from_maildir_path() {
        assert_eq!(
            extract_uuid("INBOX/cur/12345.abc-def-123:2,S"),
            Some("abc-def-123".to_string())
        );
        assert_eq!(extract_uuid("no-dot"), None);
    }

    #[test]
    fn role_rank_orders_correctly() {
        assert!(role_rank(Some("inbox")) < role_rank(Some("sent")));
        assert!(role_rank(Some("junk")) < role_rank(None));
        assert!(role_rank(Some("unknown-role")) == role_rank(None));
    }
}
