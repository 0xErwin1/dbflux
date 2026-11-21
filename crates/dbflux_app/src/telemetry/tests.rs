use super::*;
use dbflux_core::observability::usage::UsageFeature;
use dbflux_storage::bootstrap::StorageRuntime;
fn now() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .expect("clock")
        .with_timezone(&Utc)
}
fn fixture(
    config: TelemetryConfig,
) -> (
    TelemetryScheduler,
    dbflux_audit::AuditService,
    StorageRuntime,
) {
    // Never use the default app database. The runtime owns its scratch
    // directory and removes it when the returned storage drops, so callers
    // keep that third value alive for as long as the scheduler.
    let storage = StorageRuntime::in_memory().expect("scratch storage");
    let audit =
        dbflux_audit::AuditService::new_sqlite(storage.dbflux_db_path()).expect("scratch audit");
    let scheduler = TelemetryScheduler::new(
        storage.telemetry(),
        config,
        BuildIdentity::published(
            "1.2.3",
            ReleaseChannel::Stable,
            Platform::Linux,
            Architecture::Amd64,
        ),
        Arc::new(audit.clone()),
        {
            let audit = audit.clone();
            Arc::new(move || audit.is_enabled())
        },
        now(),
    )
    .expect("scheduler");
    (scheduler, audit, storage)
}
fn configured() -> (
    TelemetryScheduler,
    dbflux_audit::AuditService,
    StorageRuntime,
) {
    fixture(
        TelemetryConfig::from_values(Some("https://example.invalid/v1/events"), None, false)
            .expect("config"),
    )
}
/// Build pings only: the default Basic queue also holds the weekly report
/// and a Minimal queue the weekly `active`, so a per-build assertion must
/// not count every delivery.
fn pending_pings(scheduler: &TelemetryScheduler) -> usize {
    scheduler
        .state()
        .pending()
        .iter()
        .filter(|delivery| delivery.json().contains("\"event\":\"ping\""))
        .count()
}
fn job(scheduler: &mut TelemetryScheduler, time: DateTime<Utc>) -> DeliveryJob {
    let Preparation::Job(job) = scheduler.prepare(time, "es_AR").expect("prepare") else {
        panic!("expected job");
    };
    job
}
fn acknowledged(job: &DeliveryJob) -> DeliveryCompletion {
    job.attempt_with(now(), |dispatch| {
        assert!(dispatch());
        Ok(Some(
            Acknowledgement::parse(
                200,
                br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored"}"#,
            )
            .expect("ack"),
        ))
    })
}
fn usage_ticket(
    scheduler: &mut TelemetryScheduler,
) -> dbflux_core::observability::usage::UsageTicket {
    scheduler
        .usage_source(now())
        .expect("source")
        .expect("enabled")
        .ticket()
        .expect("capture")
        .expect("ticket")
}
fn fail_writes(storage: &StorageRuntime) {
    storage.dbflux_db().execute_batch("CREATE TRIGGER fail_telemetry BEFORE UPDATE ON cfg_telemetry BEGIN SELECT RAISE(FAIL, 'fixture'); END;").expect("failure injection");
}
fn restore_writes(storage: &StorageRuntime) {
    storage
        .dbflux_db()
        .execute_batch("DROP TRIGGER fail_telemetry;")
        .expect("restore fixture writes");
}
#[test]
fn telemetry_bridge_requires_basic_disclosure_and_live_audit() {
    let (mut scheduler, audit, _storage) = configured();
    assert!(scheduler.usage_source(now()).expect("off").is_none());
    scheduler.set_basic(true, now()).expect("basic");
    assert!(
        scheduler
            .usage_source(now())
            .expect("undisclosed")
            .is_none()
    );
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    let ticket = usage_ticket(&mut scheduler);
    audit.set_enabled(false);
    assert!(
        !ticket
            .record(UsageFeature::QueryRun)
            .expect("live audit gate")
    );
    assert!(
        scheduler
            .usage_source(now())
            .expect("disabled audit")
            .is_none()
    );
    assert!(matches!(
        scheduler.prepare(now(), "en"),
        Err(TelemetryError::Audit)
    ));
    assert_eq!(pending_pings(&scheduler), 0);
}
#[test]
fn telemetry_bridge_batches_six_once_and_ack_preserves_newer_credits() {
    let (mut scheduler, _, storage) = configured();
    scheduler.set_basic(true, now()).expect("basic");
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    let ticket = usage_ticket(&mut scheduler);
    for feature in UsageFeature::ALL {
        assert!(ticket.record(feature).expect("credit"));
        assert!(!ticket.clone().record(feature).expect("duplicate"));
        assert!(usage_ticket(&mut scheduler).record(feature).expect("fresh"));
    }
    let revision_before: i64 = storage
        .dbflux_db()
        .query_row("SELECT revision FROM cfg_telemetry WHERE id=1", [], |row| {
            row.get(0)
        })
        .expect("revision");
    scheduler.flush_usage(now()).expect("atomic batch");
    let revision_after: i64 = storage
        .dbflux_db()
        .query_row("SELECT revision FROM cfg_telemetry WHERE id=1", [], |row| {
            row.get(0)
        })
        .expect("revision");
    assert_eq!(revision_after, revision_before + 1);
    let ping = job(&mut scheduler, now());
    scheduler
        .complete(acknowledged(&ping), now())
        .expect("ping");
    let basic = job(&mut scheduler, now());
    let frozen: serde_json::Value = serde_json::from_str(basic.payload()).expect("json");
    assert_eq!(frozen["features"].as_object().expect("six").len(), 6);
    assert!(
        frozen["features"]
            .as_object()
            .expect("six")
            .values()
            .all(|value| *value == 2)
    );
    assert!(
        usage_ticket(&mut scheduler)
            .record(UsageFeature::QueryRun)
            .expect("newer")
    );
    scheduler.flush_usage(now()).expect("newer batch");
    scheduler
        .complete(acknowledged(&basic), now())
        .expect("ack snapshot");
    let next = job(&mut scheduler, now() + chrono::Duration::days(7));
    let value: serde_json::Value = serde_json::from_str(next.payload()).expect("json");
    assert_eq!(value["features"]["query_run"], 1);
}
#[test]
fn telemetry_bridge_count_save_failure_retries_same_epoch_exactly_once() {
    let (mut scheduler, _, storage) = configured();
    scheduler.set_basic(true, now()).expect("basic");
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    let ticket = usage_ticket(&mut scheduler);
    assert!(ticket.record(UsageFeature::ScriptRun).expect("credit"));
    fail_writes(&storage);
    assert_eq!(scheduler.flush_usage(now()), Err(TelemetryError::Storage));
    assert!(
        !ticket
            .record(UsageFeature::QueryRun)
            .expect("disabled until retry")
    );
    restore_writes(&storage);
    assert!(scheduler.usage_source(now()).expect("retry gate").is_none());
    scheduler.flush_usage(now()).expect("retry");
    scheduler.flush_usage(now()).expect("no double credit");
    let ping = job(&mut scheduler, now());
    scheduler
        .complete(acknowledged(&ping), now())
        .expect("ping");
    let basic = job(&mut scheduler, now());
    let value: serde_json::Value = serde_json::from_str(basic.payload()).expect("json");
    assert_eq!(value["features"]["script_run"], 1);
    assert!(value["features"].get("query_run").is_none());
}
#[test]
fn telemetry_bridge_epoch_fences_off_reset_rotation_failures_and_drop() {
    for scenario in 0..5 {
        let (mut scheduler, _, storage) = configured();
        scheduler.set_basic(true, now()).expect("basic");
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        let ticket = usage_ticket(&mut scheduler);
        assert!(ticket.record(UsageFeature::QueryRun).expect("old credit"));
        let old_id = scheduler.state().install_id();
        match scenario {
            0 => {
                scheduler.set_basic(false, now()).expect("off");
                scheduler.set_basic(true, now()).expect("on");
            }
            1 => {
                scheduler.reset(now()).expect("reset");
            }
            2 => {
                // The identity no longer rotates on a deadline, so time
                // passing is not an epoch change. Preparation at any date
                // leaves the tickets of the current identity valid; only an
                // explicit reset fences them.
                scheduler
                    .prepare(now() + chrono::Duration::days(400), "en")
                    .expect("prepare after more than a year");
            }
            _ => {
                fail_writes(&storage);
                let result = if scenario == 3 {
                    scheduler.set_basic(false, now())
                } else {
                    scheduler.reset(now())
                };
                assert_eq!(result, Err(TelemetryError::Storage));
                restore_writes(&storage);
                assert!(
                    scheduler.prepare(now(), "en").is_err(),
                    "failure fence survives next tick"
                );
                scheduler.set_basic(true, now()).expect("durable recovery");
            }
        }
        if scenario == 2 {
            // Nothing rotated: the ticket of this identity is still valid.
            assert_eq!(scheduler.state().install_id(), old_id);
            assert!(ticket.record(UsageFeature::ScriptRun).expect("same epoch"));
        } else {
            assert!(!ticket.record(UsageFeature::ScriptRun).expect("stale"));
        }
        if matches!(scenario, 1 | 4) {
            assert_ne!(scheduler.state().install_id(), old_id);
        }
        assert!(
            usage_ticket(&mut scheduler)
                .record(UsageFeature::ScriptRun)
                .expect("fresh epoch")
        );
        scheduler.flush_usage(now()).expect("flush");
        let value = serde_json::to_value(scheduler.state()).expect("state");
        assert!(value["counters"].get("query_run").is_none());
        // Scenario 2 kept its identity, so its earlier credit survived and
        // the fresh ticket added one more.
        let expected = if scenario == 2 { 2 } else { 1 };
        assert_eq!(value["counters"]["script_run"], expected);
    }
}

#[test]
fn telemetry_failed_withdrawal_cancels_queued_basic_before_storage() {
    let (mut scheduler, _, storage) = configured();
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    scheduler.set_basic(true, now()).expect("basic");
    let ping = job(&mut scheduler, now());
    scheduler
        .complete(acknowledged(&ping), now())
        .expect("ping");
    let basic = job(&mut scheduler, now());
    storage.dbflux_db().execute_batch("CREATE TRIGGER fail_telemetry BEFORE UPDATE ON cfg_telemetry BEGIN SELECT RAISE(FAIL, 'fixture'); END;").expect("failure injection");
    assert_eq!(
        scheduler.set_basic(false, now()),
        Err(TelemetryError::Storage)
    );
    assert_eq!(basic.phase.load(Ordering::SeqCst), CANCELLED);
    assert!(scheduler.prepare(now(), "en").is_err());
}

#[test]
fn telemetry_last_report_reads_byte_exact_payload_from_durable_audit_after_ack() {
    let (mut scheduler, audit, storage) = configured();
    assert_eq!(last_report(&audit).expect("empty audit"), None);

    scheduler.disclosure_dismissed(now()).expect("dismiss");
    scheduler.set_basic(true, now()).expect("basic");
    let ping = job(&mut scheduler, now());
    scheduler
        .complete(acknowledged(&ping), now())
        .expect("ack clears the ping");
    let basic = job(&mut scheduler, now());
    scheduler
        .complete(acknowledged(&basic), now())
        .expect("ack clears the basic delivery");
    assert_eq!(pending_pings(&scheduler), 0);

    // Reopen the real scratch-backed service: the report must survive in
    // the durable audit, not in any in-memory state.
    drop(audit);
    let reopened =
        dbflux_audit::AuditService::new_sqlite(storage.dbflux_db_path()).expect("reopen");
    let report = last_report(&reopened)
        .expect("query")
        .expect("confirmed report");
    assert_eq!(report.payload, basic.payload());
    assert_eq!(report.payload_len, basic.payload().len());
    assert_eq!(report.level.as_deref(), Some("basic"));
    assert_eq!(report.correlation, Some(basic.correlation_id()));
}

#[test]
fn telemetry_last_report_confirms_only_stored_or_duplicate_outcomes() {
    let (mut scheduler, audit, _storage) = configured();
    scheduler.disclosure_dismissed(now()).expect("dismiss");

    // A failed delivery leaves attempt + failure outcome events: no report.
    let failed = job(&mut scheduler, now());
    let completion = failed.attempt_with(now(), |dispatch| {
        assert!(dispatch());
        Err(TransportError::Timeout)
    });
    scheduler.complete(completion, now()).expect("recorded");
    assert_eq!(last_report(&audit).expect("query"), None);

    // A later confirmed delivery is the newest report.
    let delivered = job(&mut scheduler, now() + chrono::Duration::minutes(3));
    scheduler
        .complete(acknowledged(&delivered), now())
        .expect("ack");
    let report = last_report(&audit).expect("query").expect("report");
    assert_eq!(report.payload, delivered.payload());
    assert_eq!(report.correlation, Some(delivered.correlation_id()));
}

/// One confirmed delivery followed by four failed attempts (attempt +
/// failure-outcome rows each) must still surface the confirmed report:
/// the SQL filter selects success outcomes before the row bound.
#[test]
fn telemetry_last_report_survives_later_failure_and_pending_rows() {
    let (mut scheduler, audit, _storage) = configured();
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    scheduler.set_basic(true, now()).expect("basic");

    let delivered = job(&mut scheduler, now());
    scheduler
        .complete(acknowledged(&delivered), now())
        .expect("ack");

    for attempt in [1_u32, 5, 10, 20, 40, 80, 160, 320] {
        let failed = job(
            &mut scheduler,
            now() + chrono::Duration::minutes(attempt as i64),
        );
        let completion = failed.attempt_with(
            now() + chrono::Duration::minutes(attempt as i64),
            |dispatch| {
                assert!(dispatch());
                Err(TransportError::Timeout)
            },
        );
        scheduler.complete(completion, now()).expect("recorded");
    }

    let report = last_report(&audit)
        .expect("query")
        .expect("confirmed report");
    assert_eq!(report.payload, delivered.payload());
    assert_eq!(report.correlation, Some(delivered.correlation_id()));
}

/// A success-labeled row whose payload is not a real closed-schema v1
/// delivery is never a report; the older real confirmed payload wins.
#[test]
fn telemetry_last_report_skips_success_rows_with_fake_payloads() {
    let (mut scheduler, audit, _storage) = configured();
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    let delivered = job(&mut scheduler, now());
    scheduler
        .complete(acknowledged(&delivered), now())
        .expect("ack");

    for fake in [
        "{\"event\":\"ping\"}",
        "any string is not a delivery",
        r#"{"schema":"dbflux.telemetry-event/v1","event":"ping"}"#,
    ] {
        audit
            .record(
                EventRecord::new(
                    dbflux_core::chrono::Utc::now().timestamp_millis(),
                    EventSeverity::Info,
                    EventCategory::System,
                    EventOutcome::Success,
                )
                .with_action("telemetry.delivery")
                .with_summary("Telemetry delivery")
                .with_details_json(
                    serde_json::json!({
                        "payload": fake,
                        "level": "off",
                        "phase": "outcome",
                        "code": "stored"
                    })
                    .to_string(),
                ),
            )
            .expect("fake-success row stored");
    }

    let report = last_report(&audit).expect("query").expect("real report");
    assert_eq!(report.payload, delivered.payload());
    assert_eq!(report.correlation, Some(delivered.correlation_id()));
}

/// The canonical `dbflux_telemetry::validate_payload` accepts exactly the
/// production wire shapes: the real payloads the scheduler freezes, never
/// arbitrary look-alikes.
#[test]
fn telemetry_report_payload_validation_matches_the_real_wire_schema() {
    let (mut scheduler, _, _storage) = configured();
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    let ping = job(&mut scheduler, now());
    assert!(dbflux_telemetry::validate_payload(ping.payload()).is_ok());

    scheduler.set_basic(true, now()).expect("basic");
    scheduler
        .complete(acknowledged(&ping), now())
        .expect("ack ping");
    let basic = job(&mut scheduler, now());
    assert!(dbflux_telemetry::validate_payload(basic.payload()).is_ok());

    assert!(dbflux_telemetry::validate_payload("{\"event\":\"ping\"}").is_err());
    assert!(dbflux_telemetry::validate_payload("not json").is_err());
    // Double-encoded JSON is a string, not a delivery object.
    assert!(dbflux_telemetry::validate_payload(r#""{\"event\":\"ping\"}""#).is_err());
}

#[test]
fn telemetry_last_report_skips_malformed_rows_and_bounds_the_query() {
    let (mut scheduler, audit, _storage) = configured();
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    let delivered = job(&mut scheduler, now());
    scheduler
        .complete(acknowledged(&delivered), now())
        .expect("ack");

    audit
        .record(
            EventRecord::new(
                // Newer than the confirmed outcome (recorded at the wall
                // clock), so a reader that inspects only the newest candidate
                // fails this test.
                (Utc::now() + chrono::Duration::hours(1)).timestamp_millis(),
                EventSeverity::Info,
                EventCategory::System,
                EventOutcome::Success,
            )
            .with_action("telemetry.delivery")
            .with_summary("Telemetry delivery")
            .with_details_json(
                serde_json::json!({"phase": "outcome", "code": "stored"}).to_string(),
            ),
        )
        .expect("row without payload stored");

    // The malformed newer row is skipped, not reported or fatal.
    let report = last_report(&audit).expect("query").expect("report");
    assert_eq!(report.payload, delivered.payload());
}

#[test]
fn telemetry_report_source_is_send_for_background_queries() {
    fn require_send<T: Send>() {}
    require_send::<dbflux_audit::AuditService>();
}

#[test]
fn telemetry_prepare_persists_frozen_job_only_after_disclosure() {
    let (mut scheduler, _, storage) = configured();
    assert!(!scheduler.is_disclosure_dismissed());
    assert!(matches!(
        scheduler.prepare(now(), "es_AR").expect("undisclosed"),
        Preparation::Skipped(SkipReason::NotDue)
    ));
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    assert!(scheduler.is_disclosure_dismissed());
    let job = job(&mut scheduler, now());
    assert_eq!(scheduler.state().pending()[0].json(), job.payload());
    let persisted = storage
        .telemetry()
        .update(now(), |_| Ok(()))
        .expect("reload current");
    assert!(persisted.disclosure_dismissed());
    assert_eq!(persisted.pending()[0].json(), job.payload());
    assert!(matches!(
        scheduler.prepare(now(), "es_AR").expect("single flight"),
        Preparation::Skipped(SkipReason::Busy)
    ));
}
#[test]
fn telemetry_later_build_startup_preserves_disclosure_and_prepares_without_welcome() {
    let (mut scheduler, audit, storage) = configured();
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    let first = job(&mut scheduler, now());
    scheduler
        .complete(acknowledged(&first), now())
        .expect("complete");
    drop(first);
    drop(scheduler);
    let mut restarted = TelemetryScheduler::new(
        storage.telemetry(),
        TelemetryConfig::from_values(Some("https://example.invalid/v1/events"), None, false)
            .expect("config"),
        BuildIdentity::published(
            "1.2.4",
            ReleaseChannel::Stable,
            Platform::Linux,
            Architecture::Amd64,
        ),
        Arc::new(audit.clone()),
        Arc::new(move || audit.is_enabled()),
        now(),
    )
    .expect("restart");
    assert!(restarted.is_disclosure_dismissed());
    // A Minimal queue holds the new build's ping and the weekly active, in
    // either order: walk it until the new build is the one prepared.
    let mut saw_new_build = false;
    for _ in 0..4 {
        let Ok(Preparation::Job(job)) = restarted.prepare(now(), "en") else {
            break;
        };
        if job.payload().contains("1.2.4") {
            saw_new_build = true;
            break;
        }
        restarted
            .complete(acknowledged(&job), now())
            .expect("clear the other delivery");
    }
    assert!(saw_new_build, "the new build must be prepared");
}

/// Capturing a usage ticket happens on the foreground for every counted
/// action. With a stable identity there is no epoch to persist, so the
/// capture must be a read: a write there put a SQLite `BEGIN IMMEDIATE` on
/// the UI thread per action.
#[test]
fn capturing_a_usage_ticket_does_not_write_to_storage() {
    let (mut scheduler, _, storage) = configured();
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    scheduler.set_basic(true, now()).expect("basic");
    fail_writes(&storage);
    assert!(
        scheduler
            .usage_source(now())
            .expect("capture must not need a writable store")
            .is_some()
    );
    restore_writes(&storage);
}

#[test]
fn telemetry_dropping_owner_cancels_unstarted_job() {
    let (mut scheduler, _, _storage) = configured();
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    let queued = job(&mut scheduler, now());
    drop(scheduler);
    let completion = queued.attempt_with(now(), |_| panic!("owner dropped before dispatch"));
    assert_eq!(completion.code(), "cancelled");
}
#[test]
fn telemetry_owned_jobs_and_completions_are_send() {
    fn require_send<T: Send>() {}
    require_send::<DeliveryJob>();
    require_send::<DeliveryCompletion>();
}
#[test]
fn telemetry_build_and_injected_configuration_are_strict() {
    assert!(
        BuildIdentity::published(
            "0.9.0-dev.0",
            ReleaseChannel::current(),
            Platform::Linux,
            Architecture::Amd64
        )
        .is_err()
    );
    if env!("CARGO_PKG_VERSION") == "0.9.0-dev.0" {
        assert!(BuildIdentity::compiled().is_err());
    }
    assert!(
        BuildIdentity::published(
            "1.2.3",
            ReleaseChannel::Rc,
            Platform::Linux,
            Architecture::Amd64
        )
        .is_err()
    );
    assert!(
        BuildIdentity::published(
            "1.2.3-rc.1",
            ReleaseChannel::Rc,
            Platform::Linux,
            Architecture::Amd64
        )
        .is_ok()
    );
    assert!(
        BuildIdentity::published(
            "1.2.3-nightly+abc1234",
            ReleaseChannel::Nightly,
            Platform::Linux,
            Architecture::Amd64
        )
        .is_ok()
    );
    let endpoint = Some("http://127.0.0.1:12345/v1/events");
    assert!(TelemetryConfig::from_values(endpoint, Some("1"), false).is_err());
    assert!(TelemetryConfig::from_values(endpoint, None, true).is_err());
    assert_eq!(
        TelemetryConfig::from_values(endpoint, Some("1"), true).is_ok(),
        cfg!(debug_assertions)
    );
    assert!(TelemetryConfig::from_values(Some("https://example.invalid"), None, false).is_err());
    let (mut scheduler, _, _storage) = configured();
    scheduler.identity = BuildIdentity::compiled();
    if scheduler.identity.is_err() {
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        assert!(matches!(
            scheduler.prepare(now(), "en"),
            Ok(Preparation::Skipped(SkipReason::UnsupportedBuild))
        ));
        assert_eq!(pending_pings(&scheduler), 0);
    }
}
#[test]
fn telemetry_audit_roundtrip_preserves_full_ping_and_basic_payloads() {
    let (mut scheduler, audit, storage) = configured();
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    scheduler.set_basic(true, now()).expect("basic");
    for feature in [
        Feature::QueryRun,
        Feature::VisualQueryBuilder,
        Feature::DashboardOpen,
        Feature::ScriptRun,
        Feature::McpTool,
        Feature::SchemaInspect,
    ] {
        scheduler.increment(feature, now()).expect("increment");
    }
    let mut payloads = Vec::new();
    for _ in 0..2 {
        let job = job(&mut scheduler, now());
        let completion = acknowledged(&job);
        assert_eq!(completion.code(), "stored");
        payloads.push((job.payload().to_owned(), job.correlation_id().to_string()));
        scheduler.complete(completion, now()).expect("complete");
    }
    // Reopen the real scratch-backed service, rather than trusting record's
    // return value or an in-memory recording fake.
    drop(audit);
    let reopened =
        dbflux_audit::AuditService::new_sqlite(storage.dbflux_db_path()).expect("reopen audit");
    assert!(reopened.redact_sensitive());
    let events = reopened
        .query_extended(&dbflux_audit::query::AuditQueryFilter::default())
        .expect("persisted events");
    assert_eq!(events.len(), 4);
    for (payload, correlation) in &payloads {
        let related: Vec<_> = events
            .iter()
            .filter(|event| event.correlation_id.as_ref() == Some(correlation))
            .collect();
        assert_eq!(related.len(), 2);
        let mut phases = HashSet::new();
        for event in related {
            assert_eq!(event.category.as_deref(), Some("system"));
            assert_eq!(event.action.as_deref(), Some("telemetry.delivery"));
            assert_eq!(event.summary.as_deref(), Some("Telemetry delivery"));
            let details_json = event.details_json.as_deref().expect("details");
            assert!(details_json.len() < 64 * 1024);
            assert!(!details_json.contains("example.invalid"));
            let details: serde_json::Value =
                serde_json::from_str(details_json).expect("details JSON");
            assert_eq!(details["payload"].as_str(), Some(payload.as_str()));
            phases.insert(details["phase"].as_str().expect("phase").to_string());
        }
        assert_eq!(
            phases,
            HashSet::from(["attempt".to_string(), "outcome".to_string()])
        );
    }
    let basic: serde_json::Value = serde_json::from_str(&payloads[1].0).expect("basic JSON");
    assert_eq!(basic["system_language"], "es");
    assert_eq!(basic["country"], "ZZ");
    assert_eq!(basic["features"].as_object().expect("six counts").len(), 6);
    assert!(
        basic["features"]
            .as_object()
            .expect("counts")
            .values()
            .all(|value| value == 1)
    );
}
#[test]
fn telemetry_retries_are_identical_and_current_counters_survive_ack() {
    let (mut scheduler, _, storage) = configured();
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    scheduler.set_basic(true, now()).expect("basic");
    scheduler
        .increment(Feature::QueryRun, now())
        .expect("initial count");
    let ping = job(&mut scheduler, now());
    scheduler
        .complete(acknowledged(&ping), now())
        .expect("ping ack");
    let first = job(&mut scheduler, now());
    let payload = first.payload().to_owned();
    // Independent foreground transaction represents usage arriving after
    // queueing; completion must not write the scheduler's old snapshot.
    storage
        .telemetry()
        .update(now(), |state| {
            state.increment(Feature::QueryRun);
            Ok(())
        })
        .expect("concurrent count");
    let failed = first.attempt_with(now(), |dispatch| {
        assert!(dispatch());
        Err(TransportError::Timeout)
    });
    scheduler.complete(failed, now()).expect("backoff");
    assert!(matches!(
        scheduler.prepare(now(), "fr_FR"),
        Ok(Preparation::Skipped(SkipReason::NotDue))
    ));
    let later = now() + chrono::Duration::minutes(3);
    let retry = job(&mut scheduler, later);
    assert_eq!(retry.payload(), payload);
    assert_ne!(retry.correlation_id(), first.correlation_id());
    let duplicate = retry.attempt_with(later, |dispatch| {
        assert!(dispatch());
        Ok(Some(
            Acknowledgement::parse(
                200,
                br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"duplicate"}"#,
            )
            .expect("duplicate ack"),
        ))
    });
    scheduler
        .complete(duplicate, later)
        .expect("duplicate completion");
    let next = job(&mut scheduler, later + chrono::Duration::days(7));
    let next_payload: serde_json::Value = serde_json::from_str(next.payload()).expect("next basic");
    assert_eq!(next_payload["features"]["query_run"], 1);
}
#[test]
fn telemetry_off_cancels_queued_basic_and_clears_counters() {
    let (mut scheduler, _, _storage) = configured();
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    scheduler.set_basic(true, now()).expect("basic");
    let ping = job(&mut scheduler, now());
    scheduler
        .complete(acknowledged(&ping), now())
        .expect("ping");
    let queued = job(&mut scheduler, now());
    scheduler
        .increment(Feature::QueryRun, now())
        .expect("new count");
    scheduler.set_basic(false, now()).expect("off");
    let unchanged = scheduler.state().clone();
    let completion = queued.attempt_with(now(), |_| panic!("cancelled job must not dispatch"));
    assert_eq!(completion.code(), "cancelled");
    assert_eq!(
        scheduler.complete(completion, now()).expect("stale ack"),
        CompletionDisposition::Stale
    );
    assert_eq!(scheduler.state(), &unchanged);
    scheduler.set_basic(true, now()).expect("on again");
    let basic = job(&mut scheduler, now());
    let value: serde_json::Value = serde_json::from_str(basic.payload()).expect("basic");
    assert!(value["features"].as_object().expect("counts").is_empty());
}
#[test]
fn telemetry_off_prepares_nothing_after_the_disclosure_is_dismissed() {
    let (mut scheduler, audit, _storage) = configured();
    scheduler.set_level(Level::Off, now()).expect("off");
    scheduler.disclosure_dismissed(now()).expect("dismiss");

    for time in [now(), now() + chrono::Duration::days(8)] {
        assert!(matches!(
            scheduler.prepare(time, "en").expect("prepare"),
            Preparation::Skipped(SkipReason::NotDue)
        ));
    }

    assert_eq!(pending_pings(&scheduler), 0);
    assert!(scheduler.usage_source(now()).expect("off").is_none());
    let events = audit
        .query_extended(&dbflux_audit::query::AuditQueryFilter::default())
        .expect("audit");
    assert!(events.is_empty(), "Off records no delivery attempt");
}

#[test]
fn telemetry_minimal_level_sends_one_ping_and_never_counts_usage() {
    let (mut scheduler, audit, _storage) = configured();
    assert_eq!(
        scheduler.state().level(),
        Level::Basic,
        "Basic is the default"
    );
    scheduler.set_level(Level::Minimal, now()).expect("minimal");
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    assert!(scheduler.usage_source(now()).expect("minimal").is_none());

    let ping = job(&mut scheduler, now());
    let payload: serde_json::Value = serde_json::from_str(ping.payload()).expect("payload");
    assert_eq!(payload["event"], "ping");
    scheduler
        .complete(acknowledged(&ping), now())
        .expect("ping");

    // Eight days later the weekly signal is due, and at Minimal it is
    // `active`, never a Basic report.
    let weekly = scheduler
        .prepare(now() + chrono::Duration::days(8), "en")
        .expect("weekly");
    let Preparation::Job(weekly) = weekly else {
        panic!("the weekly signal is due at Minimal");
    };
    let wire: serde_json::Value = serde_json::from_str(weekly.payload()).expect("wire");
    assert_eq!(wire["event"], "active");
    let report = last_report(&audit).expect("lookup").expect("report");
    assert_eq!(report.level.as_deref(), Some("minimal"));
}

#[test]
fn telemetry_switching_to_off_cancels_a_queued_ping() {
    let (mut scheduler, _, _storage) = configured();
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    let queued = job(&mut scheduler, now());

    scheduler.set_level(Level::Off, now()).expect("off");

    assert_eq!(pending_pings(&scheduler), 0);
    let completion = queued.attempt_with(now(), |_| panic!("cancelled ping must not dispatch"));
    assert_eq!(completion.code(), "cancelled");
    assert_eq!(
        scheduler.complete(completion, now()).expect("stale"),
        CompletionDisposition::Stale
    );
    assert!(matches!(
        scheduler.prepare(now(), "en").expect("off"),
        Preparation::Skipped(SkipReason::NotDue)
    ));
}

#[test]
fn telemetry_switching_basic_to_minimal_cancels_only_the_queued_report() {
    let (mut scheduler, _, _storage) = configured();
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    scheduler.set_level(Level::Basic, now()).expect("basic");
    let ping = job(&mut scheduler, now());
    scheduler
        .complete(acknowledged(&ping), now())
        .expect("ping");
    let report = job(&mut scheduler, now());

    scheduler.set_level(Level::Minimal, now()).expect("minimal");

    assert_eq!(report.phase.load(Ordering::SeqCst), CANCELLED);
    assert_eq!(pending_pings(&scheduler), 0);
    assert!(scheduler.usage_source(now()).expect("minimal").is_none());
}

#[test]
fn telemetry_off_and_reset_allow_bounded_inflight_ack_without_stale_write() {
    for reset in [false, true] {
        let (mut scheduler, _, _storage) = configured();
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        scheduler.set_basic(true, now()).expect("basic");
        let ping = job(&mut scheduler, now());
        scheduler
            .complete(acknowledged(&ping), now())
            .expect("ping");
        let basic = job(&mut scheduler, now());
        let completion = basic.attempt_with(now(), |dispatch| {
            assert!(dispatch());
            if reset {
                scheduler.reset(now()).expect("reset while dispatched");
            } else {
                scheduler
                    .set_basic(false, now())
                    .expect("off while dispatched");
            }
            assert!(matches!(
                scheduler.prepare(now(), "en"),
                Ok(Preparation::Skipped(SkipReason::Busy))
            ));
            Ok(Some(
                Acknowledgement::parse(
                    200,
                    br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored"}"#,
                )
                .expect("ack"),
            ))
        });
        let unchanged = scheduler.state().clone();
        assert_eq!(
            scheduler
                .complete(completion, now())
                .expect("stale completion"),
            CompletionDisposition::Stale
        );
        assert_eq!(scheduler.state(), &unchanged);
        let next = scheduler.prepare(now(), "en").expect("next");
        if reset {
            let Preparation::Job(job) = next else {
                panic!("basic after reset");
            };
            let payload: serde_json::Value = serde_json::from_str(job.payload()).expect("payload");
            assert_eq!(
                payload["event"], "basic",
                "reset must preserve acknowledged ping history"
            );
        } else {
            assert!(matches!(next, Preparation::Skipped(SkipReason::NotDue)));
        }
    }
}
#[test]
fn telemetry_checks_lease_again_immediately_before_dispatch() {
    let (mut scheduler, audit, _storage) = configured();
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    let queued = job(&mut scheduler, now());
    let completion = queued.attempt_with(now(), |dispatch| {
        scheduler.reset(now()).expect("reset during construction");
        assert!(!dispatch());
        Ok(None)
    });
    assert_eq!(completion.code(), "cancelled");
    let events = audit
        .query_extended(&dbflux_audit::query::AuditQueryFilter::default())
        .expect("audit");
    assert_eq!(events.len(), 2, "attempt plus cancelled outcome");
    assert_eq!(
        scheduler.complete(completion, now()).expect("cancelled"),
        CompletionDisposition::Stale
    );
}
#[test]
fn telemetry_disabled_audit_is_fail_closed_before_io() {
    let (mut scheduler, audit, _storage) = configured();
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    let queued = job(&mut scheduler, now());
    audit.set_enabled(false);
    let completion = queued.attempt_with(now(), |_| panic!("no durable attempt, no I/O"));
    assert_eq!(completion.code(), "audit_failure");
    assert_eq!(
        scheduler.complete(completion, now()),
        Err(TelemetryError::Audit)
    );
    assert_eq!(pending_pings(&scheduler), 1);
}
#[test]
fn telemetry_failed_outcome_audit_does_not_acknowledge() {
    let (mut scheduler, audit, _storage) = configured();
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    let queued = job(&mut scheduler, now());
    let completion = queued.attempt_with(now(), |dispatch| {
        assert!(dispatch());
        audit.set_enabled(false);
        Ok(Some(
            Acknowledgement::parse(
                200,
                br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored"}"#,
            )
            .expect("ack"),
        ))
    });
    assert_eq!(completion.code(), "audit_failure");
    assert_eq!(
        scheduler.complete(completion, now()),
        Err(TelemetryError::Audit)
    );
    assert_eq!(scheduler.state().pending()[0].json(), queued.payload());
}
#[test]
fn telemetry_background_unwind_releases_dispatch_lease() {
    let (mut scheduler, _, _storage) = configured();
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    let queued = job(&mut scheduler, now());
    let payload = queued.payload().to_owned();
    let worker = std::thread::spawn(move || {
        queued.attempt_with(now(), |dispatch| {
            assert!(dispatch());
            panic!("injected executor unwind");
        })
    });
    assert!(worker.join().is_err());
    let replacement = job(&mut scheduler, now());
    assert_eq!(replacement.payload(), payload);
    // Successful owned execution also uses the actual scratch AuditService
    // from a background thread, returning no storage handle to the owner.
    let completion = std::thread::spawn(move || acknowledged(&replacement))
        .join()
        .expect("background job");
    scheduler
        .complete(completion, now())
        .expect("foreground completion");
    assert_eq!(pending_pings(&scheduler), 0);
}
#[test]
fn telemetry_dropped_owner_update_releases_finished_completion_lease() {
    let (mut scheduler, _, _storage) = configured();
    scheduler.set_level(Level::Minimal, now()).expect("minimal");
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    let queued = job(&mut scheduler, now());
    let payload = queued.payload().to_owned();
    let id = queued.delivery_id();
    let phase = queued.phase.clone();
    let completion = std::thread::spawn(move || acknowledged(&queued))
        .join()
        .expect("background attempt");
    assert_eq!(phase.load(Ordering::SeqCst), FINISHED);
    // A failed weak-entity update drops its uncalled closure after await,
    // including the completion it would have passed to the surviving owner.
    let owner_update = move || completion;
    drop(owner_update);
    let retry = job(&mut scheduler, now());
    assert_eq!(retry.delivery_id(), id);
    assert_eq!(retry.payload(), payload);
    assert!(!Arc::ptr_eq(&phase, &retry.phase));
    assert_eq!(pending_pings(&scheduler), 1);
    scheduler
        .complete(acknowledged(&retry), now())
        .expect("retry completion");
    assert_eq!(pending_pings(&scheduler), 0);
    // The ping is settled; what remains due is the weekly active.
    match scheduler.prepare(now(), "en") {
        Ok(Preparation::Job(job)) => assert!(
            job.payload().contains("active"),
            "only the weekly signal may still be due"
        ),
        _ => panic!("unexpected preparation"),
    }
}

#[test]
fn telemetry_old_completion_drop_cannot_cancel_replacement_after_reset() {
    let (mut scheduler, _, _storage) = configured();
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    let old = job(&mut scheduler, now());
    scheduler.reset(now()).expect("reset old identity");
    let replacement = job(&mut scheduler, now());
    let stale = old.attempt_with(now(), |_| panic!("cancelled before transport"));
    assert!(!Arc::ptr_eq(&stale.phase, &replacement.phase));
    drop(stale);
    assert_eq!(replacement.phase.load(Ordering::SeqCst), QUEUED);
    assert!(matches!(
        scheduler.prepare(now(), "en"),
        Ok(Preparation::Skipped(SkipReason::Busy))
    ));
    scheduler
        .complete(acknowledged(&replacement), now())
        .expect("replacement completion");
    assert_eq!(pending_pings(&scheduler), 0);
}

#[test]
fn telemetry_stale_finished_completion_drop_cannot_cancel_same_id_generation() {
    let (mut scheduler, _, _storage) = configured();
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    let current = job(&mut scheduler, now());
    // Model a delayed finished result from a previous attempt of the same
    // immutable delivery. Only the per-attempt Arc distinguishes leases.
    let old_phase = Arc::new(AtomicU8::new(FINISHED));
    let stale = DeliveryCompletion {
        id: current.delivery_id(),
        correlation: Uuid::new_v4(),
        phase: old_phase.clone(),
        result: AttemptOutcome::Cancelled,
    };
    assert!(!Arc::ptr_eq(&stale.phase, &current.phase));
    drop(stale);
    assert_eq!(old_phase.load(Ordering::SeqCst), CANCELLED);
    assert_eq!(current.phase.load(Ordering::SeqCst), QUEUED);
    assert!(matches!(
        scheduler.prepare(now(), "en"),
        Ok(Preparation::Skipped(SkipReason::Busy))
    ));
    scheduler
        .complete(acknowledged(&current), now())
        .expect("current completion");
    assert_eq!(pending_pings(&scheduler), 0);
}

#[test]
fn telemetry_successful_completion_does_not_cancel_next_delivery() {
    let (mut scheduler, _, _storage) = configured();
    scheduler.set_level(Level::Minimal, now()).expect("minimal");
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    let ping = job(&mut scheduler, now());
    let completion = acknowledged(&ping);
    let old_phase = completion.phase.clone();
    assert_eq!(
        scheduler
            .complete(completion, now())
            .expect("ping completion"),
        CompletionDisposition::Applied
    );
    assert_eq!(pending_pings(&scheduler), 0);
    // The weekly active queued at Minimal is unrelated to the next Basic
    // delivery, so clear it before switching levels.
    if let Ok(Preparation::Job(weekly)) = scheduler.prepare(now(), "en") {
        scheduler
            .complete(acknowledged(&weekly), now())
            .expect("weekly active");
    }
    scheduler.set_basic(true, now()).expect("basic");
    let basic = job(&mut scheduler, now());
    assert!(!Arc::ptr_eq(&old_phase, &basic.phase));
    drop(ping);
    assert_eq!(basic.phase.load(Ordering::SeqCst), QUEUED);
    scheduler
        .complete(acknowledged(&basic), now())
        .expect("basic completion");
    assert_eq!(pending_pings(&scheduler), 0);
}

#[test]
fn telemetry_completion_storage_failure_releases_lease_without_acknowledging() {
    let (mut scheduler, _, storage) = configured();
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    let queued = job(&mut scheduler, now());
    let completion = acknowledged(&queued);
    let connection = storage.dbflux_db();
    connection
        .execute_batch("BEGIN IMMEDIATE")
        .expect("injected transaction contention");
    assert_eq!(
        scheduler.complete(completion, now()),
        Err(TelemetryError::Storage)
    );
    connection
        .execute_batch("ROLLBACK")
        .expect("end injected contention");
    let retry = job(&mut scheduler, now());
    assert_eq!(retry.payload(), queued.payload());
}
#[test]
fn telemetry_old_cancelled_completion_cannot_release_new_lease() {
    let (mut scheduler, _, _storage) = configured();
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    let old = job(&mut scheduler, now());
    scheduler.reset(now()).expect("cancel old queue");
    let new = job(&mut scheduler, now());
    assert_ne!(old.delivery_id(), new.delivery_id());
    let stale = old.attempt_with(now(), |_| panic!("cancelled old job"));
    assert_eq!(
        scheduler.complete(stale, now()).expect("stale"),
        CompletionDisposition::Stale
    );
    assert!(matches!(
        scheduler.prepare(now(), "en"),
        Ok(Preparation::Skipped(SkipReason::Busy))
    ));
    scheduler
        .complete(acknowledged(&new), now())
        .expect("new completion");
}
#[test]
fn telemetry_dropped_job_releases_lease_and_permanent_failure_is_not_retried() {
    let (mut scheduler, _, _storage) = configured();
    scheduler.set_level(Level::Minimal, now()).expect("minimal");
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    let queued = job(&mut scheduler, now());
    let payload = queued.payload().to_owned();
    drop(queued);
    let replacement = job(&mut scheduler, now());
    assert_eq!(replacement.payload(), payload);
    let completion = replacement.attempt_with(now(), |dispatch| {
        assert!(dispatch());
        Err(TransportError::Conflict)
    });
    assert_eq!(completion.code(), "conflict");
    scheduler
        .complete(completion, now())
        .expect("permanent rejection");
    // The rejected ping is not retried; the weekly active is unrelated and
    // may still be prepared.
    match scheduler.prepare(now() + chrono::Duration::days(1), "en") {
        Ok(Preparation::Job(job)) => assert!(
            job.payload().contains("active"),
            "a permanently rejected ping must not be prepared again"
        ),
        _ => panic!("unexpected preparation"),
    }
    assert_eq!(pending_pings(&scheduler), 1);
}
/// The 60 s tick runs on the UI thread: when nothing is due it must not
/// open a SQLite write transaction. The injected trigger makes any write
/// fail, so success proves the tick only read the cached state.
#[test]
fn telemetry_idle_prepare_does_not_write_storage() {
    let (mut scheduler, _, storage) = configured();
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    while let Ok(Preparation::Job(due)) = scheduler.prepare(now(), "en") {
        scheduler
            .complete(acknowledged(&due), now())
            .expect("settle every due delivery");
    }
    fail_writes(&storage);
    let later = now() + chrono::Duration::minutes(1);
    assert!(matches!(
        scheduler
            .prepare(later, "en")
            .expect("idle tick needs no write"),
        Preparation::Skipped(SkipReason::NotDue)
    ));
    restore_writes(&storage);
}
#[test]
fn telemetry_prepare_before_the_disclosure_does_not_write_storage() {
    let (mut scheduler, _, storage) = configured();
    fail_writes(&storage);
    assert!(matches!(
        scheduler.prepare(now(), "en").expect("nothing to schedule"),
        Preparation::Skipped(SkipReason::NotDue)
    ));
    restore_writes(&storage);
}
#[test]
fn telemetry_prepare_still_writes_when_a_delivery_becomes_due() {
    let (mut scheduler, _, storage) = configured();
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    while let Ok(Preparation::Job(due)) = scheduler.prepare(now(), "en") {
        scheduler
            .complete(acknowledged(&due), now())
            .expect("settle every due delivery");
    }
    let week_later = now() + chrono::Duration::days(8);
    let Preparation::Job(weekly) = scheduler.prepare(week_later, "en").expect("weekly") else {
        panic!("the weekly report is due");
    };
    let persisted = storage
        .telemetry()
        .update(week_later, |_| Ok(()))
        .expect("reload");
    assert_eq!(persisted.pending()[0].json(), weekly.payload());
}
#[test]
fn telemetry_compiled_endpoint_is_the_production_collector() {
    let config = TelemetryConfig::resolve(None, None, false, None).expect("default");
    assert_eq!(
        config.endpoint,
        Some(
            Endpoint::parse(Some("https://telemetry.dbflux.dev/v1/events"), false)
                .expect("production endpoint")
        )
    );
    assert_eq!(
        DEFAULT_TELEMETRY_ENDPOINT,
        "https://telemetry.dbflux.dev/v1/events"
    );
}
#[test]
fn telemetry_endpoint_override_replaces_the_default_under_the_transport_rules() {
    let custom = "https://collector.example.test/v1/events";
    let config = TelemetryConfig::resolve(Some(custom), None, false, None).expect("override");
    assert_eq!(
        config.endpoint,
        Some(Endpoint::parse(Some(custom), false).expect("custom"))
    );
    for rejected in [
        "http://collector.example.test/v1/events",
        "https://collector.example.test/other",
        "https://collector.example.test/v1/events?x=1",
        "",
    ] {
        assert!(
            TelemetryConfig::resolve(Some(rejected), None, false, None).is_err(),
            "{rejected:?} must be rejected, never fall back to the default"
        );
    }
}
#[test]
fn telemetry_kill_switch_beats_the_compiled_default_and_any_override() {
    for switch in ["off", "OFF", " Off "] {
        let config = TelemetryConfig::resolve(None, None, false, Some(switch)).expect("off");
        assert!(config.endpoint.is_none(), "{switch:?}");
        let config = TelemetryConfig::resolve(
            Some("https://collector.example.test/v1/events"),
            None,
            false,
            Some(switch),
        )
        .expect("off with override");
        assert!(config.endpoint.is_none(), "{switch:?}");
    }
    for other in ["on", "1", ""] {
        let config = TelemetryConfig::resolve(None, None, false, Some(other)).expect("other");
        assert!(config.endpoint.is_some(), "{other:?} is not a kill switch");
    }
}
#[test]
fn telemetry_kill_switch_prepares_nothing_even_after_the_disclosure() {
    let config = TelemetryConfig::resolve(None, None, false, Some("off")).expect("off");
    let (mut scheduler, _, _storage) = fixture(config);
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    assert!(!scheduler.endpoint_configured());
    assert!(matches!(
        scheduler.prepare(now(), "en").expect("no endpoint"),
        Preparation::Skipped(SkipReason::NoEndpoint)
    ));
}
#[test]
fn telemetry_level_off_sends_nothing_with_the_compiled_default_endpoint() {
    let config = TelemetryConfig::resolve(None, None, false, None).expect("default");
    let (mut scheduler, _, _storage) = fixture(config);
    scheduler.set_level(Level::Off, now()).expect("off");
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    assert!(matches!(
        scheduler.prepare(now(), "en").expect("off"),
        Preparation::Skipped(SkipReason::NotDue)
    ));
    assert!(scheduler.state().pending().is_empty());
}
#[test]
fn telemetry_a_panicking_request_becomes_a_retryable_failure() {
    let outcome = guarded_request(|| panic!("injected transport panic"));
    assert_eq!(outcome.err(), Some(TransportError::Network));
    assert!(TransportError::Network.retryable());
    let outcome = guarded_request(|| Err(TransportError::Conflict));
    assert_eq!(outcome.err(), Some(TransportError::Conflict));
}
#[test]
fn telemetry_default_endpoint_is_unavailable() {
    let (mut scheduler, _, _storage) = fixture(TelemetryConfig::default());
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    assert!(matches!(
        scheduler.prepare(now(), "en").expect("no endpoint"),
        Preparation::Skipped(SkipReason::NoEndpoint)
    ));
    assert_eq!(pending_pings(&scheduler), 0);
}

#[test]
fn telemetry_bridge_tickets_die_with_their_owner() {
    let (mut scheduler, _, _storage) = configured();
    scheduler.set_basic(true, now()).expect("basic");
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    let ticket = usage_ticket(&mut scheduler);
    let source = scheduler.usage.source();
    drop(scheduler);
    assert!(source.ticket().expect("dead owner").is_none());
    assert!(!ticket.record(UsageFeature::QueryRun).expect("stale"));
}

#[test]
fn telemetry_kill_switch_wins_over_an_unreadable_endpoint_override() {
    let lookup = |name: &str| match name {
        "DBFLUX_TELEMETRY" => Ok(Some("off".to_owned())),
        _ => Err(TelemetryError::InvalidConfiguration),
    };
    let config = TelemetryConfig::from_lookup(lookup, false).expect("disabled, not invalid");
    assert!(config.endpoint.is_none());

    let enabled = |name: &str| match name {
        "DBFLUX_TELEMETRY_ENDPOINT" => Err(TelemetryError::InvalidConfiguration),
        _ => Ok(None),
    };
    assert_eq!(
        TelemetryConfig::from_lookup(enabled, false).err(),
        Some(TelemetryError::InvalidConfiguration),
        "an invalid override still fails closed when delivery is enabled"
    );
}

#[test]
fn telemetry_off_committed_by_another_owner_stops_a_backed_off_report() {
    let (mut scheduler, _, storage) = configured();
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    let queued = job(&mut scheduler, now());
    let completion = queued.attempt_with(now(), |dispatch| {
        assert!(dispatch());
        Err(TransportError::Network)
    });
    scheduler.complete(completion, now()).expect("backoff");
    assert!(!scheduler.state().pending().is_empty());

    // A second process on the same database commits Off.
    storage
        .telemetry()
        .update(now(), |state| {
            state.set_level(Level::Off);
            Ok(())
        })
        .expect("other owner saves Off");

    let later = now() + chrono::Duration::days(2);
    assert!(matches!(
        scheduler.prepare(later, "en").expect("prepare"),
        Preparation::Skipped(SkipReason::NotDue)
    ));
    assert_eq!(scheduler.state().level(), Level::Off);
    assert!(scheduler.state().pending().is_empty());
}

#[test]
fn telemetry_permanent_rejection_is_suppressed_even_when_completion_fails_to_store() {
    let (mut scheduler, _, storage) = configured();
    scheduler.set_level(Level::Minimal, now()).expect("minimal");
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    let queued = job(&mut scheduler, now());
    let rejected_id = queued.delivery_id();
    let completion = queued.attempt_with(now(), |dispatch| {
        assert!(dispatch());
        Err(TransportError::Conflict)
    });
    fail_writes(&storage);
    assert_eq!(
        scheduler.complete(completion, now()),
        Err(TelemetryError::Storage)
    );
    restore_writes(&storage);
    match scheduler.prepare(now() + chrono::Duration::minutes(5), "en") {
        Ok(Preparation::Job(next)) => assert_ne!(
            next.delivery_id(),
            rejected_id,
            "the rejected delivery must not be sent again this session"
        ),
        Ok(Preparation::Skipped(_)) => {}
        Err(error) => panic!("unexpected {error:?}"),
    }
}

#[test]
fn telemetry_switching_to_basic_cancels_an_unstarted_active_lease() {
    let (mut scheduler, _, _storage) = configured();
    scheduler.set_level(Level::Minimal, now()).expect("minimal");
    scheduler.disclosure_dismissed(now()).expect("dismiss");
    let mut queued = Vec::new();
    while let Ok(Preparation::Job(next)) = scheduler.prepare(now(), "en") {
        let is_active = next.payload().contains("\"event\":\"active\"");
        if is_active {
            queued.push(next);
            break;
        }
        scheduler
            .complete(acknowledged(&next), now())
            .expect("settle the ping");
    }
    let active = queued.pop().expect("an active delivery was prepared");

    scheduler.set_level(Level::Basic, now()).expect("basic");

    assert_eq!(active.phase.load(Ordering::SeqCst), CANCELLED);
    assert!(
        !scheduler
            .state()
            .pending()
            .iter()
            .any(|delivery| delivery.is_active())
    );
}
