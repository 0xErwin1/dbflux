//! Proves `InfluxLanguageService` is actually consulted through
//! `dbflux_core::classify_query_for_language_with_service` — the public API
//! surface that gained fixed-language delegation. This is the lowest-level
//! function the governance layer's classifier (`classify_query_for_governance`
//! in `dbflux_core::query::safety`) is built on.
//!
//! `classify_query_for_governance` takes an `Option<&dyn LanguageService>`
//! parameter that MCP call sites (`dbflux_mcp::handlers::query::handle_query_tool`,
//! `dbflux_mcp_server::tools::query::run_explain_request`,
//! `dbflux_mcp_server::tools::scripts::detect_execution_classification`) fill
//! with the live connection's `Connection::language_service()` when one is
//! available. This crate has no access to a live MCP connection, so it stays
//! focused on the driver-level delegation mechanism those call sites rely on.

use dbflux_core::{
    ExecutionClassification, QueryLanguage, classify_query_for_language_with_service,
};
use dbflux_driver_influxdb::language_service::InfluxLanguageService;

#[test]
fn influxql_drop_database_classifies_destructive_through_the_delegated_public_api() {
    let service = InfluxLanguageService;

    let classification = classify_query_for_language_with_service(
        &QueryLanguage::InfluxQuery,
        "DROP DATABASE mydb",
        Some(&service),
    );

    assert_eq!(classification, ExecutionClassification::Destructive);
}

#[test]
fn influxql_select_still_classifies_read_through_the_delegated_public_api() {
    let service = InfluxLanguageService;

    let classification = classify_query_for_language_with_service(
        &QueryLanguage::InfluxQuery,
        "SELECT * FROM cpu",
        Some(&service),
    );

    assert_eq!(classification, ExecutionClassification::Read);
}

#[test]
fn without_a_service_influxquery_keeps_the_pre_delegation_write_fallback() {
    let classification =
        dbflux_core::classify_query_for_language(&QueryLanguage::InfluxQuery, "DROP DATABASE mydb");

    assert_eq!(classification, ExecutionClassification::Write);
}
