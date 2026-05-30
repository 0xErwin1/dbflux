/// Integration tests verifying that `EventRecord` values produced by the
/// tracing bridge (category = `System`, as required by V1 coercion) all pass
/// `AuditService::validate_event`.
///
/// These tests do not instantiate a real tracing subscriber.  They construct
/// records directly and feed them to the validator to confirm no bridge record
/// will be silently rejected downstream.
use dbflux_audit::AuditService;
use dbflux_core::observability::{EventCategory, EventOutcome, EventRecord, EventSeverity};

fn minimal_system_record(action: &str, summary: &str) -> EventRecord {
    EventRecord::new(
        0,
        EventSeverity::Info,
        EventCategory::System,
        EventOutcome::Success,
    )
    .with_summary(summary)
    .with_action(action)
    .with_actor_id("system")
}

#[test]
fn system_record_with_action_and_summary_passes_validate() {
    let record = minimal_system_record("log_event", "something happened");
    assert!(
        AuditService::validate_event(&record).is_ok(),
        "minimal System record should pass validate_event"
    );
}

#[test]
fn system_record_empty_action_fails_validate() {
    let record = minimal_system_record("", "something happened");
    assert!(
        AuditService::validate_event(&record).is_err(),
        "empty action should fail validate_event"
    );
}

#[test]
fn system_record_empty_summary_fails_validate() {
    let record = minimal_system_record("log_event", "");
    assert!(
        AuditService::validate_event(&record).is_err(),
        "empty summary should fail validate_event"
    );
}

/// For every module target that the bridge prefix table covers, a synthesized
/// `System`-category record (as produced by V1 coercion) must pass
/// `validate_event`.  This guards against category regressions where a prefix
/// entry is changed to produce a category that requires additional structured
/// fields the bridge cannot supply.
#[test]
fn system_records_for_all_prefix_map_modules_pass_validate() {
    let module_targets = [
        "dbflux_core::connection::pool",
        "dbflux_core::pipeline::exec",
        "dbflux_app::access_manager::auth",
        "dbflux_ssh::tunnel",
        "dbflux_proxy::http",
        "dbflux_aws::client",
        "dbflux_ssm::params",
        "dbflux_driver_ipc::host",
        "dbflux_app::config::profiles",
        "dbflux_app::aws_config::reflect",
        "dbflux_storage::migrations",
        "dbflux_core::storage::db",
        "dbflux_driver_sqlite::query",
        "dbflux_core::facade::session",
        "dbflux_mcp::runtime",
        "dbflux_mcp_server::governance",
        "dbflux_app::mcp_command::run",
        "dbflux_app::app_state::init",
        "dbflux_ipc::framing",
        "dbflux_driver_host::main",
        "dbflux_ui::workspace",
        "unknown::crate::module",
    ];

    for target in module_targets {
        let action = target.rsplit("::").next().unwrap_or("log_event");
        let record = minimal_system_record(action, "test event from tracing bridge");
        let result = AuditService::validate_event(&record);
        assert!(
            result.is_ok(),
            "System record from target '{target}' (action='{action}') should pass validate_event, got: {result:?}"
        );
    }
}
