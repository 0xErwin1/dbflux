// The real frozen v1 wire payload (closed schema of dbflux_telemetry),
// stored by dbflux_app/src/telemetry `record` under "payload" as a JSON
// string. These are formatter tests: they prove the display, not the
// protocol.
const TELEMETRY_PAYLOAD: &str = r#"{"schema":"dbflux.telemetry-event/v1","install_id":"0d8f8a2e-9c34-4f5a-8d1e-2b3c4d5e6f70","delivery_id":"1e9f9b3f-0d45-406b-9e2f-3c4d5e6f7081","sent_at":"2026-01-01T00:00:00Z","version":"1.2.3","channel":"stable","os":"linux","arch":"amd64","event":"ping"}"#;
const TELEMETRY_DETAILS: &str = r#"{"payload":"{\"schema\":\"dbflux.telemetry-event/v1\",\"install_id\":\"0d8f8a2e-9c34-4f5a-8d1e-2b3c4d5e6f70\",\"delivery_id\":\"1e9f9b3f-0d45-406b-9e2f-3c4d5e6f7081\",\"sent_at\":\"2026-01-01T00:00:00Z\",\"version\":\"1.2.3\",\"channel\":\"stable\",\"os\":\"linux\",\"arch\":\"amd64\",\"event\":\"ping\"}","level":"basic","phase":"attempt","code":"stored","attempt_id":"0a1b2c3d-4e5f-4a6b-8c9d-0e1f2a3b4c5d"}"#;

#[test]
fn telemetry_delivery_payload_renders_as_nested_object_not_escaped_string() {
    let displayed = super::super::AuditDocument::display_event_details(
        Some("telemetry.delivery"),
        TELEMETRY_DETAILS,
    );

    assert!(
        displayed.contains("\"install_id\":"),
        "payload should render as a nested object, got: {displayed}"
    );
    assert!(
        !displayed.contains("\\\"install_id\\\""),
        "payload should not stay an escaped string, got: {displayed}"
    );
    // Metadata of the event stays visible and unchanged.
    for metadata in [
        "\"level\": \"basic\"",
        "\"phase\": \"attempt\"",
        "\"code\": \"stored\"",
        "\"attempt_id\": \"0a1b2c3d-4e5f-4a6b-8c9d-0e1f2a3b4c5d\"",
    ] {
        assert!(
            displayed.contains(metadata),
            "metadata {metadata} missing from display, got: {displayed}"
        );
    }
}

#[test]
fn telemetry_delivery_large_payload_renders_pretty_multiline() {
    let payload = TELEMETRY_PAYLOAD;
    let details = format!(
        r#"{{"payload":{},"level":"off","phase":"outcome","code":"stored"}}"#,
        serde_json::to_string(payload).expect("fixture serializes"),
    );

    let displayed =
        super::super::AuditDocument::display_event_details(Some("telemetry.delivery"), &details);

    assert!(
        displayed.contains("\n"),
        "large payload should pretty-print over multiple lines, got: {displayed}"
    );
    assert!(!displayed.contains("\\\"install_id\\\""));
}

#[test]
fn unrelated_actions_keep_the_existing_details_presentation() {
    let expected = super::super::AuditDocument::display_json(TELEMETRY_DETAILS);

    for action in [Some("query.run"), Some("other.action"), None] {
        let displayed =
            super::super::AuditDocument::display_event_details(action, TELEMETRY_DETAILS);

        assert_eq!(
            displayed, expected,
            "action {action:?} must keep the existing presentation"
        );
        assert!(displayed.contains("\\\"install_id\\\""));
    }
}

#[test]
fn telemetry_delivery_decodes_only_the_payload_key() {
    let details = r#"{"payload":"{\"install_id\":\"abc\"}","config":"{\"secret_key\":\"hush\"}"}"#;

    let displayed =
        super::super::AuditDocument::display_event_details(Some("telemetry.delivery"), details);

    assert!(displayed.contains("\"install_id\":"));
    assert!(
        displayed.contains("\\\"secret_key\\\""),
        "unrelated JSON-looking strings must stay untouched, got: {displayed}"
    );
}

#[test]
fn telemetry_delivery_malformed_details_fall_back_to_existing_presentation() {
    let malformed = "not json {";

    let displayed =
        super::super::AuditDocument::display_event_details(Some("telemetry.delivery"), malformed);

    assert_eq!(displayed, malformed);
}

#[test]
fn telemetry_delivery_malformed_payload_falls_back_to_existing_presentation() {
    let details = r#"{"payload":"{not json","level":"basic"}"#;

    let displayed =
        super::super::AuditDocument::display_event_details(Some("telemetry.delivery"), details);

    assert_eq!(
        displayed,
        super::super::AuditDocument::display_json(details)
    );
    assert!(
        displayed.contains("\"{not json\""),
        "malformed payload must stay a displayed string, got: {displayed}"
    );
}

#[test]
fn telemetry_delivery_non_object_payload_falls_back_to_existing_presentation() {
    let details = r#"{"payload":"5","level":"basic"}"#;

    let displayed =
        super::super::AuditDocument::display_event_details(Some("telemetry.delivery"), details);

    assert_eq!(
        displayed,
        super::super::AuditDocument::display_json(details)
    );
}

#[test]
fn telemetry_delivery_already_object_payload_is_not_mangled() {
    let details = r#"{"payload":{"install_id":"abc"},"level":"basic"}"#;

    let displayed =
        super::super::AuditDocument::display_event_details(Some("telemetry.delivery"), details);

    assert_eq!(
        displayed,
        super::super::AuditDocument::display_json(details)
    );
    assert!(displayed.contains("\"install_id\": \"abc\""));
}

#[test]
fn telemetry_delivery_missing_payload_falls_back_to_existing_presentation() {
    let details = r#"{"level":"basic","phase":"attempt"}"#;

    let displayed =
        super::super::AuditDocument::display_event_details(Some("telemetry.delivery"), details);

    assert_eq!(
        displayed,
        super::super::AuditDocument::display_json(details)
    );
}

#[test]
fn display_event_details_never_mutates_the_stored_details_string() {
    let stored = TELEMETRY_DETAILS.to_string();
    let before = stored.clone();

    let displayed =
        super::super::AuditDocument::display_event_details(Some("telemetry.delivery"), &stored);

    assert_eq!(stored, before, "stored details must stay untouched");
    assert!(displayed.contains("\"install_id\":"));
}
