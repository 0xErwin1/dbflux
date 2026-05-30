#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err
)]

use dbflux_core::{
    ColumnKind, ConnectionProfile, DbConfig, DbDriver, DbError, DbKind, QueryRequest,
};
use dbflux_driver_mysql::MysqlDriver;
use dbflux_test_support::containers;
use std::time::Duration;

fn connect(uri: String) -> Result<Box<dyn dbflux_core::Connection>, DbError> {
    let driver = MysqlDriver::new(DbKind::MySQL);
    let profile = ConnectionProfile::new(
        "live-mysql-catalog",
        DbConfig::MySQL {
            use_uri: true,
            uri: Some(uri),
            host: String::new(),
            port: 3306,
            user: String::new(),
            database: Some("mysql".to_string()),
            ssl_mode: Some("disabled".to_string()),
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
fn fetch_mysql_queries_metric_column_shape() {
    containers::with_mysql_url(|uri| -> Result<(), DbError> {
        let conn = connect(uri)?;
        let result = conn.execute(&metric_req("mysql.queries_per_sec"))?;

        assert!(!result.columns.is_empty(), "must have columns");
        assert_eq!(
            result.columns[0].kind,
            ColumnKind::Timestamp,
            "first column must be Timestamp"
        );
        assert_eq!(
            result.columns[1].kind,
            ColumnKind::Float,
            "second column must be Float"
        );
        assert!(!result.rows.is_empty(), "must have at least one data point");

        Ok(())
    })
    .unwrap();
}

#[test]
#[ignore = "requires Docker daemon"]
fn fetch_mysql_threads_connected_metric() {
    containers::with_mysql_url(|uri| -> Result<(), DbError> {
        let conn = connect(uri)?;
        let result = conn.execute(&metric_req("mysql.threads_connected"))?;

        assert!(!result.rows.is_empty());
        assert_eq!(result.columns[0].kind, ColumnKind::Timestamp);
        assert_eq!(result.columns[1].kind, ColumnKind::Float);

        Ok(())
    })
    .unwrap();
}

#[test]
#[ignore = "requires Docker daemon"]
fn fetch_processlist_inspector_has_current_connection() {
    containers::with_mysql_url(|uri| -> Result<(), DbError> {
        let conn = connect(uri)?;
        let result = conn.execute(&inspector_req("mysql.processlist"))?;

        assert!(!result.columns.is_empty());
        let col_names: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(col_names.contains(&"id"));
        assert!(col_names.contains(&"command"));

        Ok(())
    })
    .unwrap();
}

#[test]
#[ignore = "requires Docker daemon"]
fn static_metrics_coverage_matches_pivoted_counters() {
    let metrics = dbflux_driver_mysql::instance_catalog::MysqlInstanceCatalog::static_metrics();
    let counters = dbflux_driver_mysql::instance_catalog::PIVOTED_COUNTERS;

    assert_eq!(metrics.len(), counters.len());
    for m in &metrics {
        assert!(m.default_refresh_secs >= 10);
    }
}
