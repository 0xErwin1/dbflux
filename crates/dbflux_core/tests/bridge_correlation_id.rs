/// Round-trip integration test for the tracing-to-audit bridge correlation_id
/// extraction (REQ-UE-8, SC-6).
///
/// Verifies that a `tracing::error!(correlation_id = ..., ...)` event arrives at
/// the audit sink with the value populated on `EventRecord.correlation_id` —
/// NOT buried inside `details_json`. Without this contract the "View in Audit"
/// toast action and the badge-click correlation filter would silently return
/// no rows.
#[cfg(feature = "tracing-bridge")]
mod correlation_id_round_trip {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use dbflux_core::observability::source::{EventSink, EventSinkError};
    use dbflux_core::observability::tracing_bridge::{BridgeConfig, FmtWriter, init_tracing};
    use dbflux_core::observability::{EventRecord, EventSeverity};

    struct CapturingSink {
        events: Arc<Mutex<Vec<EventRecord>>>,
    }

    impl EventSink for CapturingSink {
        fn record(&self, event: EventRecord) -> Result<EventRecord, EventSinkError> {
            self.events.lock().unwrap().push(event.clone());
            Ok(event)
        }
    }

    #[test]
    fn correlation_id_is_extracted_into_event_record_slot() {
        let handle = init_tracing(BridgeConfig {
            include_audit_layer: true,
            fmt_writer: FmtWriter::Stderr,
            min_level: EventSeverity::Info,
            env_filter_default: "info",
            ..BridgeConfig::default()
        })
        .expect("tracing subscriber init failed");

        let events: Arc<Mutex<Vec<EventRecord>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = CapturingSink {
            events: events.clone(),
        };
        handle
            .install_sink(Arc::new(sink))
            .expect("sink install failed");

        let expected_id = "01952f7a-c8de-7000-8000-000000000001";

        // The bridge gates on `target.starts_with("dbflux")`; this is also the
        // exact target that `report_error` uses in production.
        tracing::error!(
            target: "dbflux_ui::user_error",
            correlation_id = %expected_id,
            kind            = "storage",
            outcome         = "failure",
            action          = "user_error",
            "round-trip test event",
        );

        // Give the drain thread time to forward the record to the sink.
        std::thread::sleep(Duration::from_millis(200));

        let captured = events.lock().unwrap();
        let matching: Vec<&EventRecord> = captured
            .iter()
            .filter(|e| e.correlation_id.as_deref() == Some(expected_id))
            .collect();

        assert_eq!(
            matching.len(),
            1,
            "expected exactly one event with correlation_id = {expected_id}; got {} captured events overall",
            captured.len()
        );

        let record = matching[0];

        // correlation_id must land on the typed slot, not in details_json.
        if let Some(details) = record.details_json.as_deref() {
            assert!(
                !details.contains(expected_id),
                "correlation_id leaked into details_json: {details}"
            );
        }
    }
}
