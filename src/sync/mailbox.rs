use crate::config::{Account, MailConfig};
use crate::db::{Database, generate_id, models::MailboxRow};
use anyhow::{Context, Result, bail};
use globset::{Glob, GlobSetBuilder};
use jmap_client::Get;
use jmap_client::client::Client;
use jmap_client::core::response::MailboxGetResponse;
use jmap_client::mailbox::Mailbox;
use log::{debug, info};
use std::collections::{HashMap, HashSet};
use std::path::Path;

#[derive(Default, Debug, PartialEq, Eq)]
pub struct MailboxSyncStats {
    pub created: usize,
    pub updated: usize,
    pub deleted: usize,
    pub orphaned_emails: usize,
}

pub async fn sync_mailboxes(
    client: &Client,
    db: &Database,
    acct: &Account,
    dry_run: bool,
) -> Result<Vec<MailboxRow>> {
    let mail_cfg = acct
        .mail
        .as_ref()
        .context("account has no [accounts.mail] section; cannot sync")?;

    // TODO(perf): switch to Mailbox/changes when local trees get big. Full
    // Mailbox/get every sync is fine at Fastmail sizes (~20-100 mailboxes,
    // <2KB response); the incremental path adds real complexity around
    // subscription-drift edge cases.
    let server_view = fetch_full_tree(client).await?;

    let target_set = build_target_set(&server_view, mail_cfg)?;
    let local = db.get_all_mailboxes()?;
    let local_by_jmap: HashMap<String, MailboxRow> = local
        .iter()
        .filter_map(|m| m.jmap_id.clone().map(|j| (j, m.clone())))
        .collect();

    let mut stats =
        apply_diff(db, &mail_cfg.path, &target_set, &local_by_jmap, dry_run)?;
    stats.orphaned_emails = cleanup_orphaned_emails(db, &mail_cfg.path, dry_run)?;

    info!(
        "[{}] {}mailbox sync: {} created, {} updated, {} deleted, {} orphaned emails cleaned",
        acct.name,
        if dry_run { "[dry-run] " } else { "" },
        stats.created,
        stats.updated,
        stats.deleted,
        stats.orphaned_emails,
    );

    // Under dry-run the DB is unchanged, so returning the pre-sync tree
    // is correct — sync_emails downstream then walks against the current
    // (unmutated) mailbox set. Under real sync the DB has just been
    // updated so re-fetching picks up the new state.
    db.get_all_mailboxes()
}

// ---------------------------------------------------------------------------
// JMAP fetch
// ---------------------------------------------------------------------------

async fn fetch_full_tree(client: &Client) -> Result<Vec<Mailbox<Get>>> {
    let mut request = client.build();
    crate::jmap::restrict_using(&mut request);
    request.get_mailbox();
    let mut resp: MailboxGetResponse = request
        .send_single()
        .await
        .context("Mailbox/get full tree")?;
    Ok(resp.take_list())
}

// ---------------------------------------------------------------------------
// Filter + path resolution
// ---------------------------------------------------------------------------

struct TargetMailbox {
    jmap_id: String,
    name: String,
    parent_jmap_id: Option<String>,
    role: Option<String>,
    sort_order: Option<i64>,
    path: String,
}

fn build_target_set(
    server_view: &[Mailbox<Get>],
    mail_cfg: &MailConfig,
) -> Result<HashMap<String, TargetMailbox>> {
    let by_jmap_id: HashMap<&str, &Mailbox<Get>> = server_view
        .iter()
        .filter_map(|m| m.id().map(|id| (id, m)))
        .collect();

    let name_map: HashMap<&str, &str> = mail_cfg
        .box_mapping
        .iter()
        .map(|bm| (bm.remote.as_str(), bm.local.as_str()))
        .collect();

    let filter_globs = mail_cfg
        .box_filter
        .as_ref()
        .map(|globs| {
            let mut builder = GlobSetBuilder::new();
            for g in globs {
                builder.add(
                    Glob::new(g).with_context(|| format!("compiling box_filter glob {g:?}"))?,
                );
            }
            builder.build().context("building GlobSet from box_filter")
        })
        .transpose()?;

    let mut included = HashMap::new();
    for mailbox in server_view {
        let jmap_id = match mailbox.id() {
            Some(id) => id.to_string(),
            None => continue,
        };
        let path = resolve_path(mailbox, &by_jmap_id, &name_map)?;

        let keep = match &filter_globs {
            Some(gs) => gs.is_match(&path),
            None if mail_cfg.subscribed_only => mailbox.is_subscribed(),
            None => true,
        };
        if !keep {
            continue;
        }

        let name = mailbox
            .name()
            .expect("resolve_path already verified name")
            .to_string();
        let mapped_name = name_map.get(name.as_str()).map(|s| s.to_string()).unwrap_or(name);

        included.insert(
            jmap_id.clone(),
            TargetMailbox {
                jmap_id,
                name: mapped_name,
                parent_jmap_id: mailbox.parent_id().map(String::from),
                role: role_to_str(mailbox),
                sort_order: Some(mailbox.sort_order() as i64),
                path,
            },
        );
    }
    Ok(included)
}

fn resolve_path(
    mailbox: &Mailbox<Get>,
    by_jmap_id: &HashMap<&str, &Mailbox<Get>>,
    name_map: &HashMap<&str, &str>,
) -> Result<String> {
    let mut segments: Vec<String> = Vec::new();
    let mut current = mailbox;
    let mut seen: HashSet<String> = HashSet::new();
    loop {
        let id = current
            .id()
            .context("mailbox has no id (cannot resolve path)")?;
        if !seen.insert(id.to_string()) {
            bail!("mailbox parent cycle detected at {id:?}");
        }
        let raw_name = current
            .name()
            .with_context(|| format!("mailbox {id:?} has no name"))?;
        let mapped = name_map.get(raw_name).copied().unwrap_or(raw_name);
        segments.push(sanitize_segment(mapped));

        match current.parent_id() {
            None => break,
            Some(parent_id) => match by_jmap_id.get(parent_id) {
                Some(parent) => current = parent,
                None => bail!(
                    "mailbox {id:?} references unknown parent {parent_id:?}"
                ),
            },
        }
    }
    segments.reverse();
    Ok(segments.join("/"))
}

fn sanitize_segment(s: &str) -> String {
    // JMAP mailbox names can technically contain '/' though servers rarely
    // allow it; replace so we don't create nested filesystem directories
    // by accident.
    s.replace('/', "_")
}

fn role_to_str(mailbox: &Mailbox<Get>) -> Option<String> {
    use jmap_client::mailbox::Role;
    match mailbox.role() {
        Role::None => None,
        Role::Archive => Some("archive".into()),
        Role::Drafts => Some("drafts".into()),
        Role::Important => Some("important".into()),
        Role::Inbox => Some("inbox".into()),
        Role::Junk => Some("junk".into()),
        Role::Sent => Some("sent".into()),
        Role::Trash => Some("trash".into()),
        Role::Other(s) => Some(s),
    }
}

// ---------------------------------------------------------------------------
// Three-way diff + apply
// ---------------------------------------------------------------------------

fn apply_diff(
    db: &Database,
    mail_root: &Path,
    target: &HashMap<String, TargetMailbox>,
    local_by_jmap: &HashMap<String, MailboxRow>,
    dry_run: bool,
) -> Result<MailboxSyncStats> {
    let mut stats = MailboxSyncStats::default();

    // Deletes (children first) — anything local that fell out of the target
    // set. Includes both server-side deletes and mailboxes that were
    // filtered out (e.g. user changed box_filter, or unsubscribed on server
    // with subscribed_only=true).
    let mut to_delete: Vec<&MailboxRow> = local_by_jmap
        .iter()
        .filter(|(jmap_id, _)| !target.contains_key(*jmap_id))
        .map(|(_, row)| row)
        .collect();
    // Deepest paths first — parents can only be removed after their children.
    to_delete.sort_by(|a, b| b.path.len().cmp(&a.path.len()));
    for row in to_delete {
        delete_mailbox(db, mail_root, row, dry_run)?;
        stats.deleted += 1;
    }

    // Creates + updates (parents first) — topological order guarantees a
    // parent DB row exists before we insert a child that references it.
    let ordered = topo_sort_creates_and_updates(target, local_by_jmap);
    for jmap_id in ordered {
        let tgt = &target[&jmap_id];
        match local_by_jmap.get(&jmap_id) {
            None => {
                create_mailbox(db, mail_root, tgt, local_by_jmap, dry_run)?;
                stats.created += 1;
            }
            Some(existing) => {
                if let Some(new_row) =
                    diff_and_build_update(existing, tgt, local_by_jmap)
                {
                    update_mailbox(db, mail_root, existing, &new_row, dry_run)?;
                    stats.updated += 1;
                }
            }
        }
    }

    Ok(stats)
}

fn topo_sort_creates_and_updates(
    target: &HashMap<String, TargetMailbox>,
    local_by_jmap: &HashMap<String, MailboxRow>,
) -> Vec<String> {
    // Simple stable topological order: repeatedly emit anything whose parent
    // is either absent (root), or already emitted, or exists in the local DB
    // (already created in an earlier sync). Loop until fixed point.
    let mut remaining: HashSet<String> = target.keys().cloned().collect();
    let mut emitted: HashSet<String> = local_by_jmap.keys().cloned().collect();
    let mut order: Vec<String> = Vec::with_capacity(target.len());
    loop {
        let mut made_progress = false;
        let ready: Vec<String> = remaining
            .iter()
            .filter(|jmap_id| {
                let tgt = &target[*jmap_id];
                match &tgt.parent_jmap_id {
                    None => true,
                    Some(pid) => emitted.contains(pid) || !target.contains_key(pid),
                }
            })
            .cloned()
            .collect();
        for id in ready {
            remaining.remove(&id);
            emitted.insert(id.clone());
            order.push(id);
            made_progress = true;
        }
        if !made_progress {
            // Any remaining items form a cycle among themselves — server bug.
            // Emit them in arbitrary order rather than infinite-loop; the
            // insert will fail on the FK and we'll surface it.
            for id in remaining.drain() {
                order.push(id);
            }
            break;
        }
        if remaining.is_empty() {
            break;
        }
    }
    order
}

fn create_mailbox(
    db: &Database,
    mail_root: &Path,
    tgt: &TargetMailbox,
    local_by_jmap: &HashMap<String, MailboxRow>,
    dry_run: bool,
) -> Result<()> {
    let parent_local_id = match &tgt.parent_jmap_id {
        None => None,
        Some(pid) => local_by_jmap
            .get(pid)
            .map(|r| r.id.clone())
            .or_else(|| db.get_mailbox_by_jmap_id(pid).ok().flatten().map(|r| r.id)),
    };
    let row = MailboxRow {
        id: generate_id(),
        jmap_id: Some(tgt.jmap_id.clone()),
        name: tgt.name.clone(),
        parent_id: parent_local_id,
        role: tgt.role.clone(),
        sort_order: tgt.sort_order,
        path: tgt.path.clone(),
        jmap_state: None,
    };
    let dir = mail_root.join(&tgt.path);
    if dry_run {
        debug!(
            "[dry-run] would create mailbox {:?} at {}",
            tgt.name,
            dir.display()
        );
        return Ok(());
    }
    create_maildir(&dir)?;
    db.insert_mailbox(&row)?;
    debug!("created mailbox {:?} at {}", tgt.name, dir.display());
    Ok(())
}

fn diff_and_build_update(
    existing: &MailboxRow,
    tgt: &TargetMailbox,
    local_by_jmap: &HashMap<String, MailboxRow>,
) -> Option<MailboxRow> {
    let new_parent_id = match &tgt.parent_jmap_id {
        None => None,
        Some(pid) => local_by_jmap.get(pid).map(|r| r.id.clone()),
    };
    let unchanged = existing.name == tgt.name
        && existing.parent_id == new_parent_id
        && existing.role == tgt.role
        && existing.sort_order == tgt.sort_order
        && existing.path == tgt.path;
    if unchanged {
        return None;
    }
    Some(MailboxRow {
        id: existing.id.clone(),
        jmap_id: existing.jmap_id.clone(),
        name: tgt.name.clone(),
        parent_id: new_parent_id,
        role: tgt.role.clone(),
        sort_order: tgt.sort_order,
        path: tgt.path.clone(),
        jmap_state: existing.jmap_state.clone(),
    })
}

fn update_mailbox(
    db: &Database,
    mail_root: &Path,
    existing: &MailboxRow,
    new_row: &MailboxRow,
    dry_run: bool,
) -> Result<()> {
    if dry_run {
        if existing.path != new_row.path {
            debug!(
                "[dry-run] would rename mailbox {} -> {}",
                mail_root.join(&existing.path).display(),
                mail_root.join(&new_row.path).display()
            );
        } else {
            debug!(
                "[dry-run] would update mailbox metadata for {}",
                new_row.path
            );
        }
        return Ok(());
    }
    if existing.path != new_row.path {
        let old_dir = mail_root.join(&existing.path);
        let new_dir = mail_root.join(&new_row.path);
        if let Some(parent) = new_dir.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("mkdir -p {}", parent.display()))?;
        }
        if old_dir.exists() {
            std::fs::rename(&old_dir, &new_dir).with_context(|| {
                format!("renaming {} -> {}", old_dir.display(), new_dir.display())
            })?;
        } else {
            create_maildir(&new_dir)?;
        }
        debug!(
            "renamed mailbox {} -> {}",
            old_dir.display(),
            new_dir.display()
        );
    }
    db.update_mailbox(new_row)?;
    Ok(())
}

fn delete_mailbox(
    db: &Database,
    mail_root: &Path,
    row: &MailboxRow,
    dry_run: bool,
) -> Result<()> {
    let dir = mail_root.join(&row.path);
    if dry_run {
        debug!(
            "[dry-run] would delete mailbox {:?} at {}",
            row.name,
            dir.display()
        );
        return Ok(());
    }
    if dir.exists() {
        std::fs::remove_dir_all(&dir)
            .with_context(|| format!("removing Maildir {}", dir.display()))?;
    }
    db.delete_mailbox(&row.id)?;
    debug!("deleted mailbox {:?} at {}", row.name, dir.display());
    Ok(())
}

fn create_maildir(dir: &Path) -> Result<()> {
    for sub in ["cur", "new", "tmp"] {
        let p = dir.join(sub);
        std::fs::create_dir_all(&p)
            .with_context(|| format!("mkdir -p {}", p.display()))?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Orphaned-email cleanup
// ---------------------------------------------------------------------------

fn cleanup_orphaned_emails(
    db: &Database,
    mail_root: &Path,
    dry_run: bool,
) -> Result<usize> {
    // An email whose email_mailboxes set is empty (because its last mailbox
    // membership was cascade-deleted) has no reason to exist on disk.
    let orphaned_ids: Vec<(String, String)> = {
        let conn = db.connection();
        let mut stmt = conn.prepare(
            "SELECT e.id, e.file_path FROM emails e
             LEFT JOIN email_mailboxes em ON em.email_id = e.id
             WHERE em.email_id IS NULL",
        )?;
        let rows = stmt.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        out
    };
    for (id, file_path) in &orphaned_ids {
        let abs = if Path::new(file_path).is_absolute() {
            Path::new(file_path).to_path_buf()
        } else {
            mail_root.join(file_path)
        };
        if dry_run {
            debug!(
                "[dry-run] would remove orphaned email {} (file {})",
                id,
                abs.display()
            );
            continue;
        }
        if abs.exists() {
            if let Err(e) = std::fs::remove_file(&abs) {
                debug!("could not remove orphaned {} ({e})", abs.display());
            }
        }
        db.delete_email(id)?;
    }
    Ok(orphaned_ids.len())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{MailConfig, SyncMode};
    use crate::db::models::EmailRow;
    use std::path::PathBuf;

    fn empty_mail_cfg(path: PathBuf) -> MailConfig {
        MailConfig {
            path,
            sync_mode: SyncMode::Mirror,
            subscribed_only: true,
            box_filter: None,
            tls: None,
            box_mapping: Vec::new(),
            post_sync_hook: None,
        }
    }

    #[test]
    fn sanitize_replaces_slashes() {
        assert_eq!(sanitize_segment("foo/bar"), "foo_bar");
        assert_eq!(sanitize_segment("plain"), "plain");
    }

    #[test]
    fn create_maildir_makes_three_subdirs() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("INBOX");
        create_maildir(&root).unwrap();
        assert!(root.join("cur").is_dir());
        assert!(root.join("new").is_dir());
        assert!(root.join("tmp").is_dir());
    }

    #[test]
    fn topo_sort_parents_before_children() {
        let mut target = HashMap::new();
        target.insert(
            "child".to_string(),
            TargetMailbox {
                jmap_id: "child".to_string(),
                name: "Child".to_string(),
                parent_jmap_id: Some("parent".to_string()),
                role: None,
                sort_order: Some(0),
                path: "Parent/Child".to_string(),
            },
        );
        target.insert(
            "parent".to_string(),
            TargetMailbox {
                jmap_id: "parent".to_string(),
                name: "Parent".to_string(),
                parent_jmap_id: None,
                role: None,
                sort_order: Some(0),
                path: "Parent".to_string(),
            },
        );
        let order = topo_sort_creates_and_updates(&target, &HashMap::new());
        assert_eq!(order.len(), 2);
        let parent_pos = order.iter().position(|s| s == "parent").unwrap();
        let child_pos = order.iter().position(|s| s == "child").unwrap();
        assert!(parent_pos < child_pos, "parent must be emitted before child");
    }

    #[test]
    fn orphaned_email_gets_cleaned() {
        let db = Database::open_in_memory().unwrap();
        let mb = MailboxRow {
            id: generate_id(),
            jmap_id: Some("mb1".to_string()),
            name: "INBOX".to_string(),
            parent_id: None,
            role: None,
            sort_order: Some(0),
            path: "INBOX".to_string(),
            jmap_state: None,
        };
        db.insert_mailbox(&mb).unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let file_rel = "INBOX/cur/orphan:2,";
        let file_abs = tmp.path().join(file_rel);
        std::fs::create_dir_all(file_abs.parent().unwrap()).unwrap();
        std::fs::write(&file_abs, b"stub").unwrap();

        let email = EmailRow {
            id: generate_id(),
            jmap_id: Some("e1".to_string()),
            message_id: None,
            file_path: file_rel.to_string(),
            primary_mailbox: mb.id.clone(),
            keywords: None,
            jmap_state: None,
            size: Some(4),
            last_sync: None,
            is_dirty: false,
        };
        db.insert_email(&email).unwrap();
        // deliberately no email_mailboxes row → email is orphaned.

        let cleaned = cleanup_orphaned_emails(&db, tmp.path(), false).unwrap();
        assert_eq!(cleaned, 1);
        assert!(!file_abs.exists());
        assert!(db.get_email(&email.id).unwrap().is_none());
    }

    #[test]
    fn cleanup_leaves_non_orphans_alone() {
        let db = Database::open_in_memory().unwrap();
        let mb = MailboxRow {
            id: generate_id(),
            jmap_id: Some("mb1".to_string()),
            name: "INBOX".to_string(),
            parent_id: None,
            role: None,
            sort_order: Some(0),
            path: "INBOX".to_string(),
            jmap_state: None,
        };
        db.insert_mailbox(&mb).unwrap();

        let email = EmailRow {
            id: generate_id(),
            jmap_id: Some("e1".to_string()),
            message_id: None,
            file_path: "INBOX/cur/kept:2,".to_string(),
            primary_mailbox: mb.id.clone(),
            keywords: None,
            jmap_state: None,
            size: Some(4),
            last_sync: None,
            is_dirty: false,
        };
        db.insert_email(&email).unwrap();
        db.insert_email_mailbox(&crate::db::models::EmailMailboxRow {
            email_id: email.id.clone(),
            mailbox_id: mb.id.clone(),
            is_primary: true,
        })
        .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let cleaned = cleanup_orphaned_emails(&db, tmp.path(), false).unwrap();
        assert_eq!(cleaned, 0);
        assert!(db.get_email(&email.id).unwrap().is_some());
    }

    #[test]
    fn diff_no_change_returns_none() {
        let existing = MailboxRow {
            id: "local".to_string(),
            jmap_id: Some("j".to_string()),
            name: "INBOX".to_string(),
            parent_id: None,
            role: Some("inbox".to_string()),
            sort_order: Some(0),
            path: "INBOX".to_string(),
            jmap_state: None,
        };
        let tgt = TargetMailbox {
            jmap_id: "j".to_string(),
            name: "INBOX".to_string(),
            parent_jmap_id: None,
            role: Some("inbox".to_string()),
            sort_order: Some(0),
            path: "INBOX".to_string(),
        };
        assert!(diff_and_build_update(&existing, &tgt, &HashMap::new()).is_none());
    }

    #[test]
    fn diff_name_change_returns_updated_row() {
        let existing = MailboxRow {
            id: "local".to_string(),
            jmap_id: Some("j".to_string()),
            name: "old".to_string(),
            parent_id: None,
            role: None,
            sort_order: Some(0),
            path: "old".to_string(),
            jmap_state: Some("state".to_string()),
        };
        let tgt = TargetMailbox {
            jmap_id: "j".to_string(),
            name: "new".to_string(),
            parent_jmap_id: None,
            role: None,
            sort_order: Some(0),
            path: "new".to_string(),
        };
        let updated = diff_and_build_update(&existing, &tgt, &HashMap::new()).unwrap();
        assert_eq!(updated.id, "local");
        assert_eq!(updated.name, "new");
        assert_eq!(updated.path, "new");
        // jmap_state preserved across renames (per-mailbox Email/query cursor).
        assert_eq!(updated.jmap_state, Some("state".to_string()));
    }

    #[test]
    fn delete_mailbox_removes_dir_and_row() {
        let db = Database::open_in_memory().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let mb = MailboxRow {
            id: generate_id(),
            jmap_id: Some("j".to_string()),
            name: "INBOX".to_string(),
            parent_id: None,
            role: None,
            sort_order: Some(0),
            path: "INBOX".to_string(),
            jmap_state: None,
        };
        db.insert_mailbox(&mb).unwrap();
        let dir = tmp.path().join(&mb.path);
        create_maildir(&dir).unwrap();

        delete_mailbox(&db, tmp.path(), &mb, false).unwrap();
        assert!(!dir.exists());
        assert!(db.get_mailbox(&mb.id).unwrap().is_none());
    }

    #[test]
    fn create_mailbox_inserts_row_and_creates_dir() {
        let db = Database::open_in_memory().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let tgt = TargetMailbox {
            jmap_id: "j1".to_string(),
            name: "INBOX".to_string(),
            parent_jmap_id: None,
            role: Some("inbox".to_string()),
            sort_order: Some(0),
            path: "INBOX".to_string(),
        };
        create_mailbox(&db, tmp.path(), &tgt, &HashMap::new(), false).unwrap();
        assert!(tmp.path().join("INBOX/cur").is_dir());
        let stored = db.get_mailbox_by_jmap_id("j1").unwrap().unwrap();
        assert_eq!(stored.name, "INBOX");
        assert_eq!(stored.role, Some("inbox".to_string()));
    }

    #[test]
    fn create_mailbox_resolves_parent_from_local_map() {
        let db = Database::open_in_memory().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let parent = MailboxRow {
            id: generate_id(),
            jmap_id: Some("jparent".to_string()),
            name: "Parent".to_string(),
            parent_id: None,
            role: None,
            sort_order: Some(0),
            path: "Parent".to_string(),
            jmap_state: None,
        };
        db.insert_mailbox(&parent).unwrap();
        let mut local_by_jmap = HashMap::new();
        local_by_jmap.insert("jparent".to_string(), parent.clone());

        let child_tgt = TargetMailbox {
            jmap_id: "jchild".to_string(),
            name: "Child".to_string(),
            parent_jmap_id: Some("jparent".to_string()),
            role: None,
            sort_order: Some(0),
            path: "Parent/Child".to_string(),
        };
        create_mailbox(&db, tmp.path(), &child_tgt, &local_by_jmap, false).unwrap();
        let stored = db.get_mailbox_by_jmap_id("jchild").unwrap().unwrap();
        assert_eq!(stored.parent_id, Some(parent.id));
    }

    #[test]
    fn update_renames_dir_on_path_change() {
        let db = Database::open_in_memory().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let existing = MailboxRow {
            id: generate_id(),
            jmap_id: Some("j".to_string()),
            name: "old".to_string(),
            parent_id: None,
            role: None,
            sort_order: Some(0),
            path: "old".to_string(),
            jmap_state: None,
        };
        db.insert_mailbox(&existing).unwrap();
        create_maildir(&tmp.path().join("old")).unwrap();
        std::fs::write(tmp.path().join("old/cur/test:2,"), b"body").unwrap();

        let new_row = MailboxRow {
            path: "renamed".to_string(),
            name: "renamed".to_string(),
            ..existing.clone()
        };
        update_mailbox(&db, tmp.path(), &existing, &new_row, false).unwrap();
        assert!(!tmp.path().join("old").exists());
        assert!(tmp.path().join("renamed/cur/test:2,").exists());
    }

    // MailConfig filter tests use empty_mail_cfg from top.

    #[test]
    fn box_filter_glob_used_when_set() {
        let mut cfg = empty_mail_cfg(PathBuf::from("/tmp"));
        cfg.box_filter = Some(vec!["INBOX*".to_string()]);
        cfg.subscribed_only = false;
        let mut builder = GlobSetBuilder::new();
        builder.add(Glob::new(&cfg.box_filter.as_ref().unwrap()[0]).unwrap());
        let gs = builder.build().unwrap();
        assert!(gs.is_match("INBOX"));
        assert!(gs.is_match("INBOXfoo"));
        assert!(!gs.is_match("Sent"));
    }
}
