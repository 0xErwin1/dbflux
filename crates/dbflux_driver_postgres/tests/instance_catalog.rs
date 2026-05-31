#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err
)]

use dbflux_core::{ColumnKind, ConnectionProfile, DbConfig, DbDriver, DbError, QueryRequest};
use dbflux_driver_postgres::PostgresDriver;
use dbflux_test_support::containers;
use std::time::Duration;

fn connect(uri: String) -> Result<Box<dyn dbflux_core::Connection>, DbError> {
    let driver = PostgresDriver::new();
    let profile = ConnectionProfile::new(
        "live-postgres-catalog",
        DbConfig::Postgres {
            use_uri: true,
            uri: Some(uri),
            host: String::new(),
            port: 5432,
            user: String::new(),
            database: "postgres".to_string(),
            ssl_mode: Some("prefer".to_string()),
            ssl_root_cert_path: None,
            ssl_client_cert_path: None,
            ssl_client_key_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        },
    );

    containers::retry_db_operation(Duration::from_secs(30), || -> Result<_, DbError> {
        let conn = driver.connect(&profile)?;
        conn.ping()?;
        Ok(conn)
    })
}

fn metric_req(metric_id: &str) -> QueryRequest {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    QueryRequest {
        execution_context: Some(dbflux_core::ExecutionContext {
            source: Some(dbflux_core::ExecutionSourceContext::InstanceMetricQuery {
                metric_id: metric_id.to_string(),
                start_ms: now_ms - 60_000,
                end_ms: now_ms,
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn inspector_req(metric_id: &str) -> QueryRequest {
    QueryRequest {
        execution_context: Some(dbflux_core::ExecutionContext {
            source: Some(
                dbflux_core::ExecutionSourceContext::InstanceInspectorQuery {
                    metric_id: metric_id.to_string(),
                },
            ),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
#[ignore = "requires Docker daemon"]
fn static_catalog_shape_matches_expected_metrics() {
    let metrics = dbflux_driver_postgres::instance_catalog::PgInstanceCatalog::static_metrics();
    assert!(!metrics.is_empty());
    assert!(metrics.iter().all(|m| m.default_refresh_secs >= 10));
    assert!(metrics.iter().any(|m| m.id == "pg.tps"));
    assert!(metrics.iter().any(|m| m.id == "pg.cache_hit_ratio"));
    assert!(metrics.iter().any(|m| m.id == "pg.active_connections"));
}

#[test]
#[ignore = "requires Docker daemon"]
fn fetch_pg_tps_column_shape_is_timestamp_then_float() {
    containers::with_postgres_url(|uri| -> Result<(), DbError> {
        let conn = connect(uri)?;
        let result = conn.execute(&metric_req("pg.tps"))?;

        assert!(!result.columns.is_empty(), "result must have columns");
        assert_eq!(
            result.columns[0].kind,
            ColumnKind::Timestamp,
            "first column must be Timestamp"
        );
        for col in &result.columns[1..] {
            assert_eq!(
                col.kind,
                ColumnKind::Float,
                "column {:?} must be Float",
                col.name
            );
        }
        assert!(
            !result.rows.is_empty(),
            "must return at least one data point"
        );

        Ok(())
    })
    .unwrap();
}

#[test]
#[ignore = "requires Docker daemon"]
fn fetch_active_connections_returns_current_connection() {
    containers::with_postgres_url(|uri| -> Result<(), DbError> {
        let conn = connect(uri)?;
        let result = conn.execute(&metric_req("pg.active_connections"))?;

        assert!(!result.rows.is_empty());
        assert_eq!(result.columns[0].kind, ColumnKind::Timestamp);
        assert_eq!(result.columns[1].kind, ColumnKind::Float);

        Ok(())
    })
    .unwrap();
}

#[test]
#[ignore = "requires Docker daemon"]
fn fetch_activity_snapshot_is_non_empty() {
    containers::with_postgres_url(|uri| -> Result<(), DbError> {
        let conn = connect(uri)?;
        let result = conn.execute(&inspector_req("pg.activity"))?;

        assert!(
            !result.rows.is_empty(),
            "pg_stat_activity must have at least the current connection"
        );
        assert!(!result.columns.is_empty());

        Ok(())
    })
    .unwrap();
}

#[test]
#[ignore = "requires Docker daemon"]
fn fetch_locks_snapshot_has_expected_columns() {
    containers::with_postgres_url(|uri| -> Result<(), DbError> {
        let conn = connect(uri)?;
        let result = conn.execute(&inspector_req("pg.locks"))?;

        assert!(!result.columns.is_empty());
        let col_names: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(col_names.contains(&"pid"));
        assert!(col_names.contains(&"mode"));
        assert!(col_names.contains(&"granted"));

        Ok(())
    })
    .unwrap();
}

/// BF2: all numeric metric fetches must return Ok (not panic) on a fresh Postgres
/// instance that has no user tables yet, where aggregate queries over
/// pg_statio_user_tables return NULL instead of a number.
#[test]
#[ignore = "requires Docker daemon"]
fn all_numeric_metrics_return_ok_on_empty_instance() {
    containers::with_postgres_url(|uri| -> Result<(), DbError> {
        let conn = connect(uri)?;

        let numeric_metrics = [
            "pg.tps",
            "pg.cache_hit_ratio",
            "pg.active_connections",
            "pg.idle_connections",
            "pg.blocks_read",
        ];

        for metric_id in numeric_metrics {
            let result = conn.execute(&metric_req(metric_id));
            assert!(
                result.is_ok(),
                "metric {metric_id} must not panic/error on empty instance; got: {:?}",
                result.err()
            );

            let result = result.unwrap();
            assert!(
                !result.rows.is_empty(),
                "metric {metric_id} must return at least one row"
            );

            if metric_id == "pg.cache_hit_ratio" {
                if let Some(dbflux_core::Value::Float(ratio)) = result.rows[0].get(1) {
                    assert!(
                        *ratio >= 0.0 && *ratio <= 100.0,
                        "cache_hit_ratio must be in [0, 100] on empty instance; got {ratio}"
                    );
                }
            }
        }

        Ok(())
    })
    .unwrap();
}

#[test]
#[ignore = "requires Docker daemon"]
fn pg_stat_statements_gating_logic() {
    let metrics_without =
        dbflux_driver_postgres::instance_catalog::PgInstanceCatalog::metrics_with_probe(false);
    let metrics_with =
        dbflux_driver_postgres::instance_catalog::PgInstanceCatalog::metrics_with_probe(true);

    assert!(
        metrics_with.len() > metrics_without.len(),
        "pg_stat_statements probe must add metrics"
    );
    assert!(
        metrics_with
            .iter()
            .any(|m| m.id.contains("stat_statements")),
        "pg_stat_statements metric must appear when probe is true"
    );
    assert!(
        !metrics_without
            .iter()
            .any(|m| m.id.contains("stat_statements")),
        "pg_stat_statements metric must be absent when probe is false"
    );
}
