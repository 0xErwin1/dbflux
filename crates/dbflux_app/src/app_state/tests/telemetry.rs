use super::*;

#[test]
fn telemetry_app_source_is_absent_uninitialized_and_degraded() {
    let runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory().expect("scratch storage");
    let mut state = AppState::new_with_storage_runtime(runtime).expect("app");
    let now = chrono::Utc::now();
    assert!(state.telemetry_usage_source(now).is_none());
    state.audit_degraded = true;
    state
        .initialize_telemetry(crate::telemetry::TelemetryConfig::default(), now)
        .expect("initialize");
    state
        .set_telemetry_level(crate::telemetry::TelemetryLevel::Basic, now)
        .expect("basic");
    state
        .telemetry_mut()
        .expect("scheduler")
        .disclosure_dismissed(now)
        .expect("dismiss");
    assert!(state.telemetry_usage_source(now).is_none());
    assert!(
        state
            .telemetry_mut()
            .expect("scheduler")
            .usage_source(now)
            .expect("degraded gate")
            .is_none()
    );
}
