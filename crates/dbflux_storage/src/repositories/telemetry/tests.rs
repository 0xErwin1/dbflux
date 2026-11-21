use super::*;
use dbflux_telemetry::{Acknowledgement, Architecture, Build, Channel, Feature, Level, Platform};

fn clock() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .expect("fixture clock")
        .with_timezone(&Utc)
}

fn queue(state: &mut TelemetryState) -> Result<(), TelemetryRepositoryError> {
    state
        .schedule(
            clock(),
            &Build {
                version: "1.2.3".into(),
                channel: Channel::Stable,
            },
            Platform::Linux,
            Architecture::Amd64,
            "en_US",
            None,
        )
        .map_err(|_| TelemetryRepositoryError::Update)
}

#[test]
fn telemetry_invalid_frozen_payload_is_rejected_before_update_closure() {
    let connection = rusqlite::Connection::open_in_memory().expect("scratch database");
    MigrationRegistry::new()
        .run_all(&connection)
        .expect("migrate");
    let repository = TelemetryRepository::new(connection);
    let state = repository
        .update(clock(), |state| {
            state.dismiss_disclosure();
            state.set_level(Level::Basic);
            state.increment(Feature::QueryRun);
            queue(state)
        })
        .expect("valid state");
    let mut persisted = serde_json::to_value(state).expect("state JSON");
    let basic = persisted["pending"]
        .as_array_mut()
        .expect("pending")
        .iter_mut()
        .find(|delivery| delivery["basic"] == true)
        .expect("basic");
    basic["json"] = basic["json"]
        .as_str()
        .expect("frozen body")
        .replace("\"query_run\":1", "\"query_run\":1,\"query_run\":2")
        .into();
    let corrupt = serde_json::to_string(&persisted).expect("corruption fixture");
    let raw: &Connection = repository.connection.borrow();
    raw.execute(
        "UPDATE cfg_telemetry SET state_json = ?1 WHERE id = 1",
        [&corrupt],
    )
    .expect("inject scratch corruption");
    let called = std::cell::Cell::new(false);
    let error = repository
        .update(clock(), |_| {
            called.set(true);
            Ok(())
        })
        .expect_err("reject before publication");
    assert!(!called.get());
    assert_eq!(error.to_string(), "invalid persisted telemetry state");
    let raw: &Connection = repository.connection.borrow();
    let stored: String = raw
        .query_row(
            "SELECT state_json FROM cfg_telemetry WHERE id = 1",
            [],
            |row| row.get(0),
        )
        .expect("unchanged row");
    assert_eq!(stored, corrupt);
}

#[test]
fn telemetry_restart_preserves_exact_payload_and_new_increments() {
    let directory = tempfile::tempdir().expect("scratch directory");
    let path = directory.path().join("telemetry.sqlite");
    let connection = rusqlite::Connection::open(&path).expect("open");
    MigrationRegistry::new()
        .run_all(&connection)
        .expect("migrate");
    let repository = TelemetryRepository::new(connection);
    let first = repository
        .update(clock(), |state| {
            state.dismiss_disclosure();
            state.set_level(Level::Basic);
            state.increment(Feature::QueryRun);
            queue(state)
        })
        .expect("queue atomic snapshot");
    repository
        .update(clock(), |state| {
            state.increment(Feature::QueryRun);
            Ok(())
        })
        .expect("new usage");
    drop(repository);
    let repository = TelemetryRepository::new(rusqlite::Connection::open(&path).expect("restart"));
    let restarted = repository.update(clock(), queue).expect("retry");
    assert_eq!(restarted.pending(), first.pending());
    let ack = Acknowledgement::parse(
        200,
        br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"duplicate"}"#,
    )
    .expect("ack");
    repository
        .update(clock(), |state| {
            for delivery in first.pending() {
                state
                    .acknowledge(delivery.id(), &ack, clock())
                    .map_err(|_| TelemetryRepositoryError::Update)?;
            }
            Ok(())
        })
        .expect("commit receipts");
    let next = repository
        .update(clock(), |state| {
            state
                .schedule(
                    clock() + chrono::Duration::days(7),
                    &Build {
                        version: "1.2.3".into(),
                        channel: Channel::Stable,
                    },
                    Platform::Linux,
                    Architecture::Amd64,
                    "en",
                    None,
                )
                .map_err(|_| TelemetryRepositoryError::Update)
        })
        .expect("next snapshot");
    assert_eq!(next.pending().len(), 1);
    let json: serde_json::Value = serde_json::from_str(next.pending()[0].json()).expect("payload");
    assert_eq!(json["features"]["query_run"], 1);
    let off = repository
        .update(clock(), |state| {
            state.set_level(Level::Off);
            Ok(())
        })
        .expect("off transaction");
    assert!(off.pending().is_empty());
    drop(repository);
    let repository = TelemetryRepository::new(rusqlite::Connection::open(&path).expect("restart"));
    assert_eq!(
        repository.update(clock(), |_| Ok(())).expect("load off"),
        off
    );
}

#[test]
fn telemetry_rows_stored_with_off_or_basic_still_load_with_their_level() {
    for (stored, level, queued) in [("Off", Level::Off, 0), ("Basic", Level::Basic, 2)] {
        let connection = rusqlite::Connection::open_in_memory().expect("scratch database");
        MigrationRegistry::new()
            .run_all(&connection)
            .expect("migrate");
        let repository = TelemetryRepository::new(connection);
        let initial = repository
            .update(clock(), |state| {
                state.dismiss_disclosure();
                Ok(())
            })
            .expect("new row");
        assert_eq!(initial.level(), Level::Basic, "Basic is the default");

        let mut row = serde_json::to_value(&initial).expect("state JSON");
        row["level"] = stored.into();
        let raw: &Connection = repository.connection.borrow();
        raw.execute(
            "UPDATE cfg_telemetry SET state_json = ?1 WHERE id = 1",
            [row.to_string()],
        )
        .expect("row written by an earlier build");

        let loaded = repository.update(clock(), queue).expect("load stored row");
        assert_eq!(loaded.level(), level, "{stored}");
        assert_eq!(loaded.pending().len(), queued, "{stored}");
    }
}

#[test]
fn telemetry_racing_initialization_and_increments_are_serialized() {
    let directory = tempfile::tempdir().expect("scratch directory");
    let path = directory.path().join("telemetry.sqlite");
    let connection = rusqlite::Connection::open(&path).expect("open");
    MigrationRegistry::new()
        .run_all(&connection)
        .expect("migrate");
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let threads: Vec<_> = (0..2)
        .map(|_| {
            let path = path.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                // Reach the barrier even when setup fails, so the other worker
                // is never left waiting; the failure surfaces right after it.
                let setup = rusqlite::Connection::open(path).and_then(|connection| {
                    connection.busy_timeout(std::time::Duration::from_secs(5))?;
                    Ok(connection)
                });
                barrier.wait();
                let repository = TelemetryRepository::new(setup.expect("worker connection"));
                repository
                    .update(clock(), |state| {
                        state.dismiss_disclosure();
                        state.set_level(Level::Basic);
                        state.increment(Feature::McpTool);
                        Ok(())
                    })
                    .expect("serialized update")
                    .install_id()
            })
        })
        .collect();
    let identities: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().expect("worker"))
        .collect();
    assert_eq!(identities[0], identities[1]);
    let repository = TelemetryRepository::new(connection);
    let state = repository
        .update(clock(), queue)
        .expect("snapshot both updates");
    let json: serde_json::Value =
        serde_json::from_str(state.pending()[1].json()).expect("basic payload");
    assert_eq!(json["features"]["mcp_tool"], 2);
}

#[test]
fn telemetry_corrupt_state_does_not_reset_identity_or_leak_content() {
    let connection = rusqlite::Connection::open_in_memory().expect("scratch database");
    MigrationRegistry::new()
        .run_all(&connection)
        .expect("migrate");
    connection
        .execute(
            "INSERT INTO cfg_telemetry VALUES (1, 1, 'private fixture invalid json')",
            [],
        )
        .expect("corruption fixture");
    let repository = TelemetryRepository::new(connection);
    let error = repository
        .update(clock(), |_| Ok(()))
        .expect_err("fail closed");
    assert_eq!(error.to_string(), "invalid persisted telemetry state");
}
use crate::migrations::MigrationRegistry;

#[test]
fn telemetry_restart_and_rollback() {
    let directory = tempfile::tempdir().expect("scratch directory");
    let path = directory.path().join("telemetry.sqlite");
    let now = clock();
    let id;
    {
        let connection = rusqlite::Connection::open(&path).expect("open");
        MigrationRegistry::new()
            .run_all(&connection)
            .expect("migrate");
        let repository = TelemetryRepository::new(connection);
        id = repository
            .update(now, |_| Ok(()))
            .expect("initialize")
            .install_id();
        let failed = repository.update(now, |state| {
            state.reset_identity(now);
            Err(TelemetryRepositoryError::Update)
        });
        assert!(failed.is_err());
        assert_eq!(
            repository
                .update(now, |_| Ok(()))
                .expect("load")
                .install_id(),
            id
        );
    }
    let connection = rusqlite::Connection::open(&path).expect("reopen");
    let repository = TelemetryRepository::new(connection);
    assert_eq!(
        repository
            .update(now, |_| Ok(()))
            .expect("restart")
            .install_id(),
        id
    );
}

#[test]
fn telemetry_independent_connections_share_atomic_state() {
    let directory = tempfile::tempdir().expect("scratch directory");
    let path = directory.path().join("telemetry.sqlite");
    let connection = rusqlite::Connection::open(&path).expect("open");
    MigrationRegistry::new()
        .run_all(&connection)
        .expect("migrate");
    let repository = TelemetryRepository::new(connection);
    let other = TelemetryRepository::new(rusqlite::Connection::open(path).expect("open second"));
    let now = clock();
    let first = repository.update(now, |_| Ok(())).expect("initialize");
    let second = other
        .update(now, |state| {
            state.dismiss_disclosure();
            Ok(())
        })
        .expect("update");
    assert_eq!(first.install_id(), second.install_id());
    assert_eq!(
        repository.update(now, |_| Ok(())).expect("read latest"),
        second
    );
}

#[test]
fn telemetry_load_reads_the_committed_state_without_writing() {
    let connection = rusqlite::Connection::open_in_memory().expect("scratch database");
    MigrationRegistry::new()
        .run_all(&connection)
        .expect("migrate");
    let repository = TelemetryRepository::new(connection);
    assert!(repository.load().expect("empty").is_none());

    let stored = repository
        .update(clock(), |state| {
            state.dismiss_disclosure();
            state.set_level(Level::Off);
            Ok(())
        })
        .expect("write");
    assert_eq!(repository.load().expect("load"), Some(stored));
}
