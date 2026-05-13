//! Live integration tests for the InfluxDB driver.
//!
//! All tests are `#[ignore]` — they require a running Docker daemon and are
//! invoked explicitly:
//!
//!   cargo test -p dbflux_driver_influxdb --test live_integration -- --ignored
//!
//! Tests spin up real InfluxDB containers via testcontainers and verify the full
//! round-trip from connection to query result.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err
)]

use dbflux_core::{
    Connection, ConnectionProfile, DbConfig, DbDriver, DbError, ExecutionContext,
    ExecutionSourceContext, InfluxVersion, QueryRequest,
};
use dbflux_driver_influxdb::InfluxDriver;
use dbflux_test_support::containers;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn make_v2_profile(endpoint: &str, bucket: &str, org: &str) -> ConnectionProfile {
    ConnectionProfile::new_with_driver(
        "influxdb-v2-test",
        dbflux_core::DbKind::InfluxDB,
        "builtin:influxdb",
        DbConfig::InfluxDB {
            version: InfluxVersion::V2,
            url: endpoint.to_string(),
            org: Some(org.to_string()),
            bucket_or_database: bucket.to_string(),
            retention_policy: None,
            user: None,
            request_timeout_seconds: None,
        },
    )
}

fn make_v1_profile(endpoint: &str, database: &str) -> ConnectionProfile {
    ConnectionProfile::new_with_driver(
        "influxdb-v1-test",
        dbflux_core::DbKind::InfluxDB,
        "builtin:influxdb",
        DbConfig::InfluxDB {
            version: InfluxVersion::V1,
            url: endpoint.to_string(),
            org: None,
            bucket_or_database: database.to_string(),
            retention_policy: None,
            user: None,
            request_timeout_seconds: None,
        },
    )
}

fn connect_v2(
    endpoint: &str,
    bucket: &str,
    org: &str,
    token: &str,
) -> Result<Box<dyn Connection>, DbError> {
    let driver = InfluxDriver::new();
    let profile = make_v2_profile(endpoint, bucket, org);
    let secret = dbflux_core::secrecy::SecretString::new(token.to_string().into());
    driver.connect_with_secrets(&profile, Some(&secret), None)
}

fn connect_v1(endpoint: &str, database: &str) -> Result<Box<dyn Connection>, DbError> {
    let driver = InfluxDriver::new();
    let profile = make_v1_profile(endpoint, database);
    driver.connect_with_secrets(&profile, None, None)
}

/// Write line-protocol data to InfluxDB v2 via the `/api/v2/write` endpoint.
fn write_v2_line_protocol(
    endpoint: &str,
    token: &str,
    org: &str,
    bucket: &str,
    lines: &str,
) -> Result<(), DbError> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| DbError::connection_failed(e.to_string()))?;

    let url = format!(
        "{}/api/v2/write?org={}&bucket={}&precision=ms",
        endpoint.trim_end_matches('/'),
        urlencoding::encode(org),
        urlencoding::encode(bucket),
    );

    let resp = client
        .post(&url)
        .header("Authorization", format!("Token {token}"))
        .header("Content-Type", "text/plain; charset=utf-8")
        .body(lines.to_string())
        .send()
        .map_err(|e| DbError::connection_failed(e.to_string()))?;

    if resp.status().is_success() || resp.status().as_u16() == 204 {
        Ok(())
    } else {
        let status = resp.status().as_u16();
        let body = resp.text().unwrap_or_default();
        Err(DbError::query_failed(format!(
            "write failed: HTTP {status}: {body}"
        )))
    }
}

/// Write line-protocol data to InfluxDB v1 via the `/write` endpoint.
fn write_v1_line_protocol(endpoint: &str, database: &str, lines: &str) -> Result<(), DbError> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| DbError::connection_failed(e.to_string()))?;

    let url = format!(
        "{}/write?db={}&precision=ms",
        endpoint.trim_end_matches('/'),
        urlencoding::encode(database),
    );

    let resp = client
        .post(&url)
        .header("Content-Type", "text/plain; charset=utf-8")
        .body(lines.to_string())
        .send()
        .map_err(|e| DbError::connection_failed(e.to_string()))?;

    if resp.status().is_success() || resp.status().as_u16() == 204 {
        Ok(())
    } else {
        let status = resp.status().as_u16();
        let body = resp.text().unwrap_or_default();
        Err(DbError::query_failed(format!(
            "write failed: HTTP {status}: {body}"
        )))
    }
}

/// Write line-protocol data to InfluxDB v1, creating the database first.
fn setup_v1_database(endpoint: &str, database: &str) -> Result<(), DbError> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| DbError::connection_failed(e.to_string()))?;

    let url = format!(
        "{}/query?q={}",
        endpoint.trim_end_matches('/'),
        urlencoding::encode(&format!("CREATE DATABASE {database}")),
    );

    let resp = client
        .post(&url)
        .send()
        .map_err(|e| DbError::connection_failed(e.to_string()))?;

    if resp.status().as_u16() < 300 {
        Ok(())
    } else {
        let status = resp.status().as_u16();
        let body = resp.text().unwrap_or_default();
        Err(DbError::query_failed(format!(
            "CREATE DATABASE failed: HTTP {status}: {body}"
        )))
    }
}

/// Build a query request with an explicit collection window (triggers time injection).
fn query_with_window(sql: &str) -> QueryRequest {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    let start_ms = now_ms - 60 * 60 * 1000; // 1 hour ago
    let end_ms = now_ms + 60 * 1000; // 1 minute in the future

    let ctx = ExecutionContext {
        source: Some(ExecutionSourceContext::CollectionWindow {
            targets: vec![],
            start_ms,
            end_ms,
            query_mode: Some("influxql".to_string()),
        }),
        ..Default::default()
    };

    QueryRequest::new(sql).with_execution_context(Some(ctx))
}

/// Build a Flux query request with window injection.
fn flux_query_with_window(flux: &str) -> QueryRequest {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    let start_ms = now_ms - 60 * 60 * 1000;
    let end_ms = now_ms + 60 * 1000;

    let ctx = ExecutionContext {
        source: Some(ExecutionSourceContext::CollectionWindow {
            targets: vec![],
            start_ms,
            end_ms,
            query_mode: Some("flux".to_string()),
        }),
        ..Default::default()
    };

    QueryRequest::new(flux).with_execution_context(Some(ctx))
}

/// Generate N line-protocol points for measurement `metric`.
///
/// Uses millisecond timestamps; the write helper passes `precision=ms` to the
/// InfluxDB v2 write API so no nanosecond conversion is needed.
fn generate_metric_points(count: usize) -> String {
    let base_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    (0..count)
        .map(|i| {
            let ts_ms = base_ms - (i as i64) * 1000;
            format!("metric,host=server{} value={}i {}", i % 5, i, ts_ms)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

// ---------------------------------------------------------------------------
// D.4.2 — v2 InfluxQL happy path with time injection
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn v2_influxql_happy_path_with_time_injection() -> Result<(), DbError> {
    containers::with_influxdb_v2(|cfg| {
        let lines = generate_metric_points(100);
        write_v2_line_protocol(&cfg.endpoint, &cfg.token, &cfg.org, &cfg.bucket, &lines)?;

        // Allow InfluxDB a moment to index the written data.
        std::thread::sleep(Duration::from_millis(500));

        let conn = connect_v2(&cfg.endpoint, &cfg.bucket, &cfg.org, &cfg.token)?;

        let dbs = conn.list_databases()?;
        assert!(
            !dbs.is_empty(),
            "list_databases must return at least the test bucket"
        );

        let req = query_with_window("SELECT * FROM metric LIMIT 10");
        let result = conn.execute(&req)?;

        assert_eq!(
            result.rows.len(),
            10,
            "must return exactly 10 rows with LIMIT 10"
        );
        assert!(
            result.resolved_window.is_some(),
            "time injection must produce a resolved_window"
        );

        let extra = result
            .metadata_extra
            .as_ref()
            .expect("metadata_extra must be populated");
        assert_eq!(
            extra.get("language").and_then(|v| v.as_str()),
            Some("influxql")
        );
        assert_eq!(extra.get("version").and_then(|v| v.as_str()), Some("v2"));
        assert_eq!(
            extra.get("injected_window").and_then(|v| v.as_bool()),
            Some(true)
        );

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// D.4.3 — v2 Flux happy path with time injection
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn v2_flux_happy_path_with_time_injection() -> Result<(), DbError> {
    containers::with_influxdb_v2(|cfg| {
        let lines = generate_metric_points(20);
        write_v2_line_protocol(&cfg.endpoint, &cfg.token, &cfg.org, &cfg.bucket, &lines)?;

        std::thread::sleep(Duration::from_millis(500));

        let conn = connect_v2(&cfg.endpoint, &cfg.bucket, &cfg.org, &cfg.token)?;

        let flux = format!(
            r#"from(bucket: "{}")
  |> filter(fn: (r) => r._measurement == "metric")
  |> limit(n: 5)"#,
            cfg.bucket
        );

        let req = flux_query_with_window(&flux);
        let result = conn.execute(&req)?;

        assert!(!result.rows.is_empty(), "Flux query must return rows");
        assert!(
            result.resolved_window.is_some(),
            "time injection must produce a resolved_window for Flux"
        );

        let extra = result
            .metadata_extra
            .as_ref()
            .expect("metadata_extra must be set");
        assert_eq!(extra.get("language").and_then(|v| v.as_str()), Some("flux"));
        assert_eq!(extra.get("version").and_then(|v| v.as_str()), Some("v2"));

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// D.4.4 — v2 explicit time range → no injection
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn v2_explicit_time_range_no_injection() -> Result<(), DbError> {
    containers::with_influxdb_v2(|cfg| {
        let lines = generate_metric_points(10);
        write_v2_line_protocol(&cfg.endpoint, &cfg.token, &cfg.org, &cfg.bucket, &lines)?;

        std::thread::sleep(Duration::from_millis(500));

        let conn = connect_v2(&cfg.endpoint, &cfg.bucket, &cfg.org, &cfg.token)?;

        // Explicit time predicate in WHERE — injection must be skipped.
        let req = query_with_window("SELECT * FROM metric WHERE time > now() - 1h");
        let result = conn.execute(&req)?;

        assert!(
            result.resolved_window.is_none(),
            "explicit WHERE time must suppress injection: resolved_window must be None"
        );

        let extra = result
            .metadata_extra
            .as_ref()
            .expect("metadata_extra must be set");
        assert_eq!(
            extra.get("injected_window").and_then(|v| v.as_bool()),
            Some(false),
            "injected_window must be false when the query already has a time predicate"
        );

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// D.4.5 — v2 bad query: malformed InfluxQL → clean error message
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn v2_bad_query_surfaces_clean_error() -> Result<(), DbError> {
    containers::with_influxdb_v2(|cfg| {
        let conn = connect_v2(&cfg.endpoint, &cfg.bucket, &cfg.org, &cfg.token)?;

        let req = QueryRequest::new("THIS IS NOT VALID INFLUXQL !!!!");
        let err = conn.execute(&req).expect_err("malformed query must fail");

        let msg = err.to_string().to_ascii_lowercase();
        assert!(
            msg.contains("bad request") || msg.contains("error") || msg.contains("parse"),
            "error message must be actionable (not raw debug): {msg}"
        );

        // Must NOT be raw Rust debug output.
        assert!(
            !msg.contains("error { ") && !msg.contains("kind:"),
            "error must not leak raw Rust debug output: {msg}"
        );

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// D.4.6 — v2 auth failure → DbError::AuthFailed with clean message
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn v2_wrong_token_returns_auth_failed() -> Result<(), DbError> {
    containers::with_influxdb_v2(|cfg| {
        // Connect with a deliberately wrong token.
        let conn = connect_v2(&cfg.endpoint, &cfg.bucket, &cfg.org, "WRONG-TOKEN-XYZ")?;

        let req = QueryRequest::new("SELECT * FROM metric");
        let err = conn.execute(&req).expect_err("wrong token must fail");

        assert!(
            matches!(err, DbError::AuthFailed(_)),
            "wrong token must produce DbError::AuthFailed, got: {err:?}"
        );

        let msg = err.to_string().to_ascii_lowercase();
        assert!(
            msg.contains("unauthorized") || msg.contains("auth") || msg.contains("token"),
            "auth error message must be descriptive: {msg}"
        );

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// D.4.7 — v1 happy path
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn v1_influxql_happy_path() -> Result<(), DbError> {
    containers::with_influxdb_v1(|cfg| {
        let database = "testdb";
        setup_v1_database(&cfg.endpoint, database)?;

        let lines = generate_metric_points(20);
        write_v1_line_protocol(&cfg.endpoint, database, &lines)?;

        std::thread::sleep(Duration::from_millis(500));

        let conn = connect_v1(&cfg.endpoint, database)?;

        let dbs = conn.list_databases()?;
        assert!(
            dbs.iter().any(|db| db.name == database),
            "list_databases must include '{database}', got: {dbs:?}"
        );

        let req = query_with_window("SELECT * FROM metric LIMIT 5");
        let result = conn.execute(&req)?;

        assert_eq!(result.rows.len(), 5, "LIMIT 5 must return 5 rows");
        assert!(
            result.resolved_window.is_some(),
            "time injection must produce a resolved_window"
        );

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// D.4.8 — v1 Flux rejected synchronously (no HTTP call needed)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn v1_flux_query_rejected_without_http() -> Result<(), DbError> {
    containers::with_influxdb_v1(|cfg| {
        let database = "testdb";
        setup_v1_database(&cfg.endpoint, database)?;

        let conn = connect_v1(&cfg.endpoint, database)?;

        // Flux on v1 must fail immediately (before any HTTP call) because
        // resolve_language() rejects it. We record the time to verify it's fast.
        let before = std::time::Instant::now();

        let ctx = ExecutionContext {
            source: Some(ExecutionSourceContext::CollectionWindow {
                targets: vec![],
                start_ms: 0,
                end_ms: 1,
                query_mode: Some("flux".to_string()),
            }),
            ..Default::default()
        };
        let req = QueryRequest::new("from(bucket: \"x\") |> range(start: -1h)")
            .with_execution_context(Some(ctx));
        let err = conn.execute(&req).expect_err("Flux on v1 must fail");

        let elapsed = before.elapsed();

        assert!(
            matches!(err, DbError::QueryFailed(_)),
            "Flux on v1 must produce DbError::QueryFailed, got: {err:?}"
        );
        let msg = err.to_string().to_ascii_lowercase();
        assert!(
            msg.contains("flux") && msg.contains("not supported"),
            "error must mention Flux not supported: {msg}"
        );

        // Verify the rejection was synchronous (no HTTP round-trip needed).
        assert!(
            elapsed < Duration::from_secs(2),
            "Flux rejection must be synchronous (took {}ms)",
            elapsed.as_millis()
        );

        Ok(())
    })
}

// ---------------------------------------------------------------------------
// D.4.9 — Audit emission integration: metadata_extra fields on v2 InfluxQL query
// ---------------------------------------------------------------------------

#[test]
#[ignore = "requires Docker daemon"]
fn v2_influxql_metadata_extra_contains_audit_fields() -> Result<(), DbError> {
    containers::with_influxdb_v2(|cfg| {
        let lines = generate_metric_points(5);
        write_v2_line_protocol(&cfg.endpoint, &cfg.token, &cfg.org, &cfg.bucket, &lines)?;

        std::thread::sleep(Duration::from_millis(500));

        let conn = connect_v2(&cfg.endpoint, &cfg.bucket, &cfg.org, &cfg.token)?;

        let req = query_with_window("SELECT * FROM metric LIMIT 3");
        let result = conn.execute(&req)?;

        let extra = result
            .metadata_extra
            .as_ref()
            .expect("metadata_extra must be populated for InfluxDB queries");

        // Required fields per REQ-9 / D.3.2 spec.
        assert_eq!(
            extra.get("language").and_then(|v| v.as_str()),
            Some("influxql"),
            "language field must be 'influxql'"
        );
        assert_eq!(
            extra.get("version").and_then(|v| v.as_str()),
            Some("v2"),
            "version field must be 'v2'"
        );
        assert!(
            extra.contains_key("bucket_or_database"),
            "bucket_or_database field must be present"
        );
        assert!(
            extra.contains_key("injected_window"),
            "injected_window field must be present"
        );
        assert_eq!(
            extra.get("injected_window").and_then(|v| v.as_bool()),
            Some(true),
            "injected_window must be true when the window was injected"
        );
        assert!(
            extra.contains_key("resolved_window_start_ms"),
            "resolved_window_start_ms must be present when window was injected"
        );
        assert!(
            extra.contains_key("resolved_window_end_ms"),
            "resolved_window_end_ms must be present when window was injected"
        );

        Ok(())
    })
}
