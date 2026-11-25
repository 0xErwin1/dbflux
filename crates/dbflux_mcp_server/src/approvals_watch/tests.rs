use std::collections::HashSet;
use std::time::Duration;

use dbflux_approval::{
    ExecutionPlan, InMemoryPendingExecutionStore, PendingExecutionStore, PendingStatus,
};
use dbflux_policy::ExecutionClassification;
use serde_json::{Value, json};
use uuid::Uuid;

use super::*;

fn plan(actor_id: &str) -> ExecutionPlan {
    ExecutionPlan {
        connection_id: "conn-a".to_string(),
        actor_id: actor_id.to_string(),
        tool_id: "delete_records".to_string(),
        classification: ExecutionClassification::Destructive,
        payload: json!({ "table": "items" }),
    }
}

fn far_future() -> Option<i64> {
    Some(now_epoch_ms() + 3_600_000)
}

fn kinds(events: &[WatchEvent]) -> Vec<(WatchEventKind, String)> {
    events
        .iter()
        .map(|event| (event.kind, event.pending_id.clone()))
        .collect()
}

#[test]
fn announces_pending_at_start_and_new_requests_once() {
    let mut store = InMemoryPendingExecutionStore::default();
    let first = store.create_pending(&plan("agent"), far_future()).unwrap();
    let mut tracked = HashSet::new();

    let events = poll_events(&store, &mut tracked, None, now_epoch_ms()).unwrap();
    assert_eq!(
        kinds(&events),
        vec![(WatchEventKind::Pending, first.id.to_string())]
    );
    assert!(
        poll_events(&store, &mut tracked, None, now_epoch_ms())
            .unwrap()
            .is_empty(),
        "an announced request is not announced again"
    );

    let second = store.create_pending(&plan("agent"), far_future()).unwrap();
    let events = poll_events(&store, &mut tracked, None, now_epoch_ms()).unwrap();
    assert_eq!(
        kinds(&events),
        vec![(WatchEventKind::Pending, second.id.to_string())]
    );
}

#[test]
fn reports_approved_and_rejected_with_reason() {
    let mut store = InMemoryPendingExecutionStore::default();
    let approved = store.create_pending(&plan("agent"), far_future()).unwrap();
    let rejected = store.create_pending(&plan("agent"), far_future()).unwrap();
    let mut tracked = HashSet::new();
    poll_events(&store, &mut tracked, None, now_epoch_ms()).unwrap();

    store
        .update_status(approved.id, PendingStatus::Approved)
        .unwrap();
    store
        .record_rejection(rejected.id, Some("wrong table"))
        .unwrap();

    let events = poll_events(&store, &mut tracked, None, now_epoch_ms()).unwrap();
    let find = |kind| events.iter().find(|event| event.kind == kind).unwrap();

    assert_eq!(events.len(), 2);
    assert_eq!(
        find(WatchEventKind::Approved).to_json(),
        json!({
            "event": "approved",
            "pending_id": approved.id.to_string(),
            "tool_id": "delete_records",
            "actor_id": "agent",
            "connection_id": "conn-a",
        })
    );
    assert_eq!(
        find(WatchEventKind::Rejected).to_json()["reason"],
        json!("wrong table")
    );
    assert!(tracked.is_empty(), "decided requests are no longer tracked");
}

#[test]
fn reports_expired_for_an_undecided_request_and_for_a_stale_approval() {
    let mut store = InMemoryPendingExecutionStore::default();
    // Already past its deadline, so no wall-clock race decides the outcome;
    // the watcher announced both while they were still pending.
    let past = Some(now_epoch_ms() - 1_000);
    let undecided = store.create_pending(&plan("agent"), past).unwrap();
    let approved = store.create_pending(&plan("agent"), past).unwrap();
    let mut tracked = HashSet::from([undecided.id, approved.id]);
    store
        .update_status(approved.id, PendingStatus::Approved)
        .unwrap();

    let events = poll_events(&store, &mut tracked, None, now_epoch_ms()).unwrap();

    assert!(
        events
            .iter()
            .all(|event| event.kind == WatchEventKind::Expired)
    );
    let ids: HashSet<_> = events
        .iter()
        .map(|event| event.pending_id.clone())
        .collect();
    assert_eq!(
        ids,
        HashSet::from([undecided.id.to_string(), approved.id.to_string()])
    );
}

#[test]
fn client_filter_hides_other_actors_requests() {
    let mut store = InMemoryPendingExecutionStore::default();
    store.create_pending(&plan("other"), far_future()).unwrap();
    let mine = store.create_pending(&plan("mine"), far_future()).unwrap();
    let mut tracked = HashSet::new();

    let events = poll_events(&store, &mut tracked, Some("mine"), now_epoch_ms()).unwrap();
    assert_eq!(
        kinds(&events),
        vec![(WatchEventKind::Pending, mine.id.to_string())]
    );
}

#[test]
fn single_event_covers_every_outcome_and_the_client_filter() {
    let mut store = InMemoryPendingExecutionStore::default();
    let pending = store.create_pending(&plan("agent"), far_future()).unwrap();
    let rejected = store.create_pending(&plan("agent"), far_future()).unwrap();
    store.record_rejection(rejected.id, Some("no")).unwrap();
    let used = store.create_pending(&plan("agent"), far_future()).unwrap();
    store
        .update_status(used.id, PendingStatus::Consumed)
        .unwrap();
    let now = now_epoch_ms();
    let kind = |id: &str, client| single_event(&store, id, client, now).unwrap().kind;

    assert_eq!(kind(&pending.id.to_string(), None), WatchEventKind::Pending);
    assert_eq!(
        kind(&rejected.id.to_string(), None),
        WatchEventKind::Rejected
    );
    assert_eq!(kind(&used.id.to_string(), None), WatchEventKind::NotFound);
    assert_eq!(
        kind(&Uuid::new_v4().to_string(), None),
        WatchEventKind::NotFound
    );
    assert_eq!(kind("nope", None), WatchEventKind::NotFound);
    assert_eq!(
        kind(&pending.id.to_string(), Some("someone-else")),
        WatchEventKind::NotFound
    );
}

#[test]
fn not_found_line_carries_only_the_event_and_the_id() {
    let store = InMemoryPendingExecutionStore::default();
    let event = single_event(&store, "nope", None, 0).unwrap();
    assert_eq!(
        event.to_json(),
        json!({ "event": "not_found", "pending_id": "nope" })
    );
}

// --- single-id mode against a real SQLite store --------------------------

fn sqlite_store(
    directory: &tempfile::TempDir,
) -> dbflux_storage::pending_executions::SqlitePendingExecutionStore {
    dbflux_storage::bootstrap::StorageRuntime::for_path(directory.path().join("dbflux.db"))
        .expect("open storage")
        .pending_executions()
        .expect("open pending store")
}

fn watch_single_id(
    store: &dyn PendingExecutionStore,
    pending_id: &str,
    timeout: Duration,
) -> (i32, Vec<Value>) {
    let options = WatchOptions {
        pending_id: Some(pending_id.to_string()),
        client_id: None,
        timeout: Some(timeout),
        poll_interval: Duration::from_millis(10),
    };
    let mut out = Vec::new();
    let code = run_watch(store, &options, &mut out);
    let lines = String::from_utf8(out)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).expect("each line is JSON"))
        .collect();
    (code, lines)
}

#[test]
fn single_id_mode_exit_codes_and_lines_on_a_sqlite_store() {
    let directory = tempfile::tempdir().unwrap();
    let mut writer = sqlite_store(&directory);
    let watcher = sqlite_store(&directory);
    let short = Duration::from_millis(100);

    let unknown = Uuid::new_v4().to_string();
    let (code, lines) = watch_single_id(&watcher, &unknown, short);
    assert_eq!(code, 1);
    assert_eq!(
        lines,
        vec![json!({ "event": "not_found", "pending_id": unknown })]
    );

    let waiting = writer.create_pending(&plan("agent"), far_future()).unwrap();
    let (code, lines) = watch_single_id(&watcher, &waiting.id.to_string(), short);
    assert_eq!(code, 124);
    assert_eq!(
        lines,
        vec![json!({ "event": "still_pending", "pending_id": waiting.id.to_string() })]
    );

    // Decided while the watcher is polling.
    let deciding = waiting.id;
    let decider = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(60));
        writer
            .record_rejection(deciding, Some("wrong table"))
            .unwrap();
        writer
    });
    let (code, lines) = watch_single_id(&watcher, &waiting.id.to_string(), Duration::from_secs(10));
    let mut writer = decider.join().unwrap();
    assert_eq!(code, 0);
    assert_eq!(
        lines,
        vec![json!({
            "event": "rejected",
            "pending_id": waiting.id.to_string(),
            "tool_id": "delete_records",
            "actor_id": "agent",
            "connection_id": "conn-a",
            "reason": "wrong table",
        })]
    );

    let approved = writer.create_pending(&plan("agent"), far_future()).unwrap();
    writer
        .update_status(approved.id, PendingStatus::Approved)
        .unwrap();
    let (code, lines) = watch_single_id(&watcher, &approved.id.to_string(), short);
    assert_eq!((code, lines[0]["event"].clone()), (0, json!("approved")));

    let stale = writer
        .create_pending(&plan("agent"), Some(now_epoch_ms() - 1))
        .unwrap();
    let (code, lines) = watch_single_id(&watcher, &stale.id.to_string(), short);
    assert_eq!((code, lines[0]["event"].clone()), (0, json!("expired")));
}

#[test]
fn stream_mode_ends_with_exit_zero_when_the_timeout_elapses() {
    let mut store = InMemoryPendingExecutionStore::default();
    let waiting = store.create_pending(&plan("agent"), far_future()).unwrap();
    let options = WatchOptions {
        pending_id: None,
        client_id: None,
        timeout: Some(Duration::from_millis(50)),
        poll_interval: Duration::from_millis(10),
    };
    let mut out = Vec::new();

    assert_eq!(run_watch(&store, &options, &mut out), 0);
    let lines: Vec<Value> = String::from_utf8(out)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["event"], json!("pending"));
    assert_eq!(lines[0]["pending_id"], json!(waiting.id.to_string()));
}

#[test]
fn parses_watch_arguments_and_rejects_bad_ones() {
    let args = |items: &[&str]| {
        items
            .iter()
            .map(|item| item.to_string())
            .collect::<Vec<_>>()
    };

    assert_eq!(
        parse_watch_args(&args(&[
            "watch",
            "--pending-id",
            "x",
            "--client-id",
            "c",
            "--timeout",
            "5"
        ])),
        Ok(WatchArgs::Watch {
            pending_id: Some("x".into()),
            client_id: Some("c".into()),
            timeout_secs: Some(5),
        })
    );
    assert_eq!(
        parse_watch_args(&args(&["watch", "--help"])),
        Ok(WatchArgs::Help)
    );
    assert!(parse_watch_args(&args(&["watch", "--timeout", "soon"])).is_err());
    assert!(parse_watch_args(&args(&["watch", "--pending-id"])).is_err());
    assert!(parse_watch_args(&args(&["watch", "--bogus"])).is_err());
    assert!(parse_watch_args(&args(&["list"])).is_err());
    assert!(parse_watch_args(&args(&[])).is_err());
}
