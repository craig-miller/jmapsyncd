//! End-to-end integration test exercising sync_account against a wiremock
//! JMAP server serving realistic (RFC 8620 / 8621) wire responses.
//!
//! Scope: one comprehensive test covering the full pipeline
//! (Mailbox/get -> Email/query per-mailbox -> Email/get -> Blob/download ->
//! Maildir write -> DB rows). Adding scenario-specific tests (deletion,
//! keyword change, rename) is straightforward on top of this scaffold once
//! the base is proven.

use jmapsyncd::config::{Account, MailConfig, SyncMode, TokenSource};
use jmapsyncd::db::Database;
use jmapsyncd::jmap;
use jmapsyncd::sync;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use wiremock::matchers::{method, path};
use wiremock::{Match, Mock, MockServer, Request, ResponseTemplate};

const ACCOUNT_ID: &str = "u1";

// ---------------------------------------------------------------------------
// Custom matcher: dispatch by JMAP method name in methodCalls[0][0].
// jmap-client's send_single puts exactly one method in methodCalls per POST.
// ---------------------------------------------------------------------------

struct JmapMethodIs(&'static str);

impl Match for JmapMethodIs {
    fn matches(&self, request: &Request) -> bool {
        let body: Value = match serde_json::from_slice(&request.body) {
            Ok(v) => v,
            Err(_) => return false,
        };
        body["methodCalls"][0][0].as_str() == Some(self.0)
    }
}

// Match Email/query by the mailbox id in filter.inMailbox. Used to
// distinguish an Email/query for INBOX from one for Archive.
struct EmailQueryFor(&'static str);

impl Match for EmailQueryFor {
    fn matches(&self, request: &Request) -> bool {
        let body: Value = match serde_json::from_slice(&request.body) {
            Ok(v) => v,
            Err(_) => return false,
        };
        body["methodCalls"][0][0].as_str() == Some("Email/query")
            && body["methodCalls"][0][1]["filter"]["inMailbox"].as_str() == Some(self.0)
    }
}

// ---------------------------------------------------------------------------
// Session (RFC 8620 §2) — mock server's own URL populates the templates so
// jmap-client's Client::download() routes back to us.
// ---------------------------------------------------------------------------

fn session_response(base_url: &str) -> Value {
    json!({
        "capabilities": {
            "urn:ietf:params:jmap:core": {
                "maxObjectsInGet": 4096,
                "maxObjectsInSet": 4096,
                "maxCallsInRequest": 50,
                "maxSizeUpload": 250_000_000,
                "maxSizeRequest": 10_000_000,
                "maxConcurrentUpload": 10,
                "maxConcurrentRequests": 10,
                "collationAlgorithms": ["i;ascii-casemap"]
            },
            "urn:ietf:params:jmap:mail": {}
        },
        "accounts": {
            ACCOUNT_ID: {
                "name": "test@example.com",
                "isPersonal": true,
                "isReadOnly": false,
                "accountCapabilities": {
                    "urn:ietf:params:jmap:core": {},
                    "urn:ietf:params:jmap:mail": {}
                }
            }
        },
        "primaryAccounts": {
            "urn:ietf:params:jmap:core": ACCOUNT_ID,
            "urn:ietf:params:jmap:mail": ACCOUNT_ID
        },
        "apiUrl":         format!("{base_url}/jmap/api/"),
        "downloadUrl":    format!("{base_url}/jmap/download/{{accountId}}/{{blobId}}/{{name}}"),
        "uploadUrl":      format!("{base_url}/jmap/upload/{{accountId}}/"),
        "eventSourceUrl": format!("{base_url}/jmap/event/"),
        "state": "s1",
        "username": "test@example.com"
    })
}

// ---------------------------------------------------------------------------
// JMAP method-response helpers — each wraps a single-method response inside
// the top-level `{methodResponses, sessionState}` envelope.
// ---------------------------------------------------------------------------

fn respond(method_name: &str, args: Value) -> Value {
    json!({
        "methodResponses": [[method_name, args, "c1"]],
        "sessionState": "s1"
    })
}

fn mailbox_get_body() -> Value {
    respond(
        "Mailbox/get",
        json!({
            "accountId": ACCOUNT_ID,
            "state": "mstate1",
            "list": [
                {
                    "id": "mb-inbox",
                    "name": "Inbox",
                    "parentId": null,
                    "role": "inbox",
                    "sortOrder": 10,
                    "isSubscribed": true,
                    "totalEmails": 2,
                    "unreadEmails": 1,
                    "totalThreads": 2,
                    "unreadThreads": 1,
                    "myRights": {
                        "mayReadItems": true, "mayAddItems": true,
                        "mayRemoveItems": true, "maySetSeen": true,
                        "maySetKeywords": true, "mayCreateChild": true,
                        "mayRename": true, "mayDelete": false, "maySubmit": true
                    }
                },
                {
                    "id": "mb-sent",
                    "name": "Sent",
                    "parentId": null,
                    "role": "sent",
                    "sortOrder": 20,
                    "isSubscribed": true,
                    "totalEmails": 1, "unreadEmails": 0,
                    "totalThreads": 1, "unreadThreads": 0,
                    "myRights": {
                        "mayReadItems": true, "mayAddItems": true,
                        "mayRemoveItems": true, "maySetSeen": true,
                        "maySetKeywords": true, "mayCreateChild": true,
                        "mayRename": true, "mayDelete": false, "maySubmit": true
                    }
                },
                {
                    "id": "mb-archive",
                    "name": "Archive",
                    "parentId": null,
                    "role": "archive",
                    "sortOrder": 30,
                    "isSubscribed": true,
                    "totalEmails": 1, "unreadEmails": 0,
                    "totalThreads": 1, "unreadThreads": 0,
                    "myRights": {
                        "mayReadItems": true, "mayAddItems": true,
                        "mayRemoveItems": true, "maySetSeen": true,
                        "maySetKeywords": true, "mayCreateChild": true,
                        "mayRename": true, "mayDelete": false, "maySubmit": true
                    }
                }
            ],
            "notFound": []
        }),
    )
}

fn email_query_body(mailbox_id: &str, ids: &[&str]) -> Value {
    respond(
        "Email/query",
        json!({
            "accountId": ACCOUNT_ID,
            "queryState": format!("qs-{mailbox_id}"),
            "canCalculateChanges": true,
            "position": 0,
            "ids": ids,
        }),
    )
}

// Emails:
//   e-inbox      -> INBOX only, $seen, blob b-inbox
//   e-cross      -> INBOX + Archive, $seen + $flagged, blob b-cross (primary=INBOX per role)
//   e-sent       -> Sent only, no keywords, blob b-sent
fn email_get_body() -> Value {
    respond(
        "Email/get",
        json!({
            "accountId": ACCOUNT_ID,
            "state": "estate1",
            "list": [
                {
                    "id": "e-inbox",
                    "blobId": "b-inbox",
                    "threadId": "t1",
                    "mailboxIds": {"mb-inbox": true},
                    "keywords": {"$seen": true},
                    "size": 512,
                    "receivedAt": "2026-01-15T10:30:00Z",
                    "messageId": ["<inbox-msg@example.com>"],
                    "subject": "Hello from INBOX"
                },
                {
                    "id": "e-cross",
                    "blobId": "b-cross",
                    "threadId": "t2",
                    "mailboxIds": {"mb-inbox": true, "mb-archive": true},
                    "keywords": {"$seen": true, "$flagged": true},
                    "size": 1024,
                    "receivedAt": "2026-01-16T11:00:00Z",
                    "messageId": ["<cross-msg@example.com>"],
                    "subject": "Cross-filed"
                },
                {
                    "id": "e-sent",
                    "blobId": "b-sent",
                    "threadId": "t3",
                    "mailboxIds": {"mb-sent": true},
                    "keywords": {},
                    "size": 256,
                    "receivedAt": "2026-01-17T09:00:00Z",
                    "messageId": ["<sent-msg@example.com>"],
                    "subject": "Sent item"
                }
            ],
            "notFound": []
        }),
    )
}

// ---------------------------------------------------------------------------
// Config + client wiring
// ---------------------------------------------------------------------------

fn build_account(jmap_host: &str, mail_path: PathBuf) -> Account {
    Account {
        name: "test".to_string(),
        enabled: true,
        jmap_host: jmap_host.to_string(),
        jmap_user: "test@example.com".to_string(),
        token: TokenSource::Inline {
            jmap_token: "test-token".to_string(),
        },
        timeout_secs: 30,
        mail: Some(MailConfig {
            path: mail_path,
            sync_mode: SyncMode::Mirror,
            subscribed_only: true,
            box_filter: None,
            tls: None,
            box_mapping: Vec::new(),
        }),
    }
}

// ---------------------------------------------------------------------------
// The test
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_initial_sync_end_to_end() {
    let server = MockServer::start().await;
    let base_url = server.uri();

    // 1. Session (jmap-client fetches this via .connect())
    Mock::given(method("GET"))
        .and(path("/.well-known/jmap"))
        .respond_with(ResponseTemplate::new(200).set_body_json(session_response(&base_url)))
        .mount(&server)
        .await;

    // 2. Mailbox/get — full tree (B.2's first call)
    Mock::given(method("POST"))
        .and(path("/jmap/api/"))
        .and(JmapMethodIs("Mailbox/get"))
        .respond_with(ResponseTemplate::new(200).set_body_json(mailbox_get_body()))
        .mount(&server)
        .await;

    // 3. Email/query — one mock per mailbox (differentiated by filter.inMailbox)
    for (mb_id, email_ids) in [
        ("mb-inbox", &["e-inbox", "e-cross"][..]),
        ("mb-sent", &["e-sent"][..]),
        ("mb-archive", &["e-cross"][..]),
    ] {
        Mock::given(method("POST"))
            .and(path("/jmap/api/"))
            .and(EmailQueryFor(mb_id))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(email_query_body(mb_id, email_ids)),
            )
            .mount(&server)
            .await;
    }

    // 4. Email/get — chunked; only one chunk here since we have < 100 emails
    Mock::given(method("POST"))
        .and(path("/jmap/api/"))
        .and(JmapMethodIs("Email/get"))
        .respond_with(ResponseTemplate::new(200).set_body_json(email_get_body()))
        .mount(&server)
        .await;

    // 5. Blob/download — one per unique blobId (e-cross downloaded once,
    // filed under its primary mailbox only).
    for (blob_id, body) in [
        ("b-inbox", "From: a@x\r\nSubject: Hello from INBOX\r\n\r\nbody-inbox"),
        ("b-cross", "From: b@x\r\nSubject: Cross-filed\r\n\r\nbody-cross"),
        ("b-sent", "From: c@x\r\nSubject: Sent item\r\n\r\nbody-sent"),
    ] {
        Mock::given(method("GET"))
            .and(path(format!(
                "/jmap/download/{ACCOUNT_ID}/{blob_id}/none"
            )))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body.as_bytes()))
            .mount(&server)
            .await;
    }

    // Run sync
    let tmp = tempfile::tempdir().unwrap();
    let mail_root = tmp.path().to_path_buf();
    let account = build_account(&base_url, mail_root.clone());
    let db = Database::open_in_memory().unwrap();

    let client = jmap::client_from_account(&account)
        .await
        .expect("client_from_account should succeed against mock");
    let stats = sync::sync_account(&client, &account, &db)
        .await
        .expect("sync_account should succeed against mock");

    // --------------------- Assertions -----------------------

    // DB has 3 mailboxes
    let mbs = db.get_all_mailboxes().unwrap();
    assert_eq!(mbs.len(), 3, "3 mailboxes should be persisted");
    let mb_names: Vec<&str> = mbs.iter().map(|m| m.name.as_str()).collect();
    assert!(mb_names.contains(&"Inbox"));
    assert!(mb_names.contains(&"Sent"));
    assert!(mb_names.contains(&"Archive"));

    // DB has 3 unique emails (e-cross is in two mailboxes but one row)
    let emails = db.get_all_emails().unwrap();
    assert_eq!(emails.len(), 3, "3 unique emails should be persisted");

    // Maildir dirs exist with cur/new/tmp
    for mb_name in ["Inbox", "Sent", "Archive"] {
        for sub in ["cur", "new", "tmp"] {
            assert!(
                mail_root.join(mb_name).join(sub).is_dir(),
                "{mb_name}/{sub}/ should exist"
            );
        }
    }

    // Primary mailbox = INBOX for e-cross (role priority: inbox > archive)
    let inbox_row = mbs.iter().find(|m| m.name == "Inbox").unwrap();
    let archive_row = mbs.iter().find(|m| m.name == "Archive").unwrap();
    let e_cross = emails
        .iter()
        .find(|e| e.jmap_id.as_deref() == Some("e-cross"))
        .expect("e-cross row exists");
    assert_eq!(
        e_cross.primary_mailbox, inbox_row.id,
        "e-cross primary should be Inbox (role priority beats Archive)"
    );
    assert!(
        e_cross.file_path.starts_with("Inbox/cur/"),
        "e-cross file must be under Inbox/cur/, got {}",
        e_cross.file_path
    );

    // Flag mapping: e-cross has $seen + $flagged -> Maildir flags FS
    assert!(
        e_cross.file_path.ends_with(":2,FS"),
        "e-cross should have :2,FS suffix, got {}",
        e_cross.file_path
    );

    // File on disk actually contains the blob bytes we mocked.
    let cross_abs = mail_root.join(&e_cross.file_path);
    let disk = std::fs::read_to_string(&cross_abs).unwrap();
    assert!(disk.contains("body-cross"));

    // e-inbox has $seen only -> :2,S; single mailbox membership
    let e_inbox = emails
        .iter()
        .find(|e| e.jmap_id.as_deref() == Some("e-inbox"))
        .unwrap();
    assert!(e_inbox.file_path.ends_with(":2,S"));
    assert!(e_inbox.file_path.starts_with("Inbox/cur/"));

    // e-sent has no keywords -> :2, (empty flags)
    let e_sent = emails
        .iter()
        .find(|e| e.jmap_id.as_deref() == Some("e-sent"))
        .unwrap();
    assert!(
        e_sent.file_path.ends_with(":2,"),
        "e-sent should have :2, (empty flags), got {}",
        e_sent.file_path
    );
    assert!(e_sent.file_path.starts_with("Sent/cur/"));

    // email_mailboxes: e-cross has 2 rows (Inbox primary, Archive secondary)
    let cross_memberships = db.get_email_mailboxes_by_email(&e_cross.id).unwrap();
    assert_eq!(cross_memberships.len(), 2);
    let inbox_membership = cross_memberships
        .iter()
        .find(|m| m.mailbox_id == inbox_row.id)
        .unwrap();
    let archive_membership = cross_memberships
        .iter()
        .find(|m| m.mailbox_id == archive_row.id)
        .unwrap();
    assert!(inbox_membership.is_primary);
    assert!(!archive_membership.is_primary);

    // Stats sanity
    assert_eq!(stats.email.created, 3);
    assert_eq!(stats.email.deleted, 0);
    assert_eq!(stats.email.updated, 0);
    assert!(stats.email.bytes_downloaded > 0);
    assert!(stats.elapsed_ms < 60_000); // sanity: less than a minute

    // No junk under tmp/ (atomic write cleaned up)
    for mb_name in ["Inbox", "Sent", "Archive"] {
        let tmp_dir = mail_root.join(mb_name).join("tmp");
        let entries: Vec<_> = std::fs::read_dir(&tmp_dir).unwrap().collect();
        assert!(
            entries.is_empty(),
            "tmp/ should be empty after successful writes, {mb_name}/tmp has {} entries",
            entries.len()
        );
    }
}

#[allow(dead_code)]
fn ensure_dir(path: &Path) {
    std::fs::create_dir_all(path).unwrap();
}
