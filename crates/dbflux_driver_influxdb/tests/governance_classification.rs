//! Proves `InfluxLanguageService` is actually consulted through
//! `dbflux_core::classify_query_for_language_with_service` — the public API
//! surface that gained fixed-language delegation. This is the lowest-level
//! function the governance layer's classifier (`classify_query_for_governance`
//! in `dbflux_core::query::safety`) is built on.
//!
//! NOTE: `classify_query_for_governance`, which is what MCP/policy call
//! directly (`dbflux_mcp::handlers::query::handle_query_tool`,
//! `dbflux_mcp_server::tools::query`), does not thread any connection or
//! `LanguageService` through — it always passes `None` for `service`. So a
//! `DROP DATABASE` executed through the live MCP path still classifies as the
//! pre-delegation default (`Write`) today; wiring the actor's live
//! `Connection::language_service()` into that call is a separate, larger
//! change (spanning `dbflux_mcp`/`dbflux_app`) outside this crate's scope.
//! This test targets the API this repo's delegation mechanism is built on,
//! which is what a future connection-aware governance call would use.

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
