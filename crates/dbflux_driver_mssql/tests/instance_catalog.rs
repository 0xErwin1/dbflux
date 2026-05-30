#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err
)]

use dbflux_core::{ColumnKind, ConnectionProfile, DbConfig, DbDriver, DbError, QueryRequest};
use dbflux_driver_mssql::MssqlDriver;
use dbflux_test_support::containers;
use std::time::Duration;

fn connect(uri: String) -> Result<Box<dyn dbflux_core::Connection>, DbError> {
    let driver = MssqlDriver::new();
    let profile = ConnectionProfile::new(
        "live-mssql-catalog",
        DbConfig::SqlServer {
            use_uri: true,
            uri: Some(uri),
            host: String::new(),
            port: 1433,
            user: String::new(),
            database: None,
            instance: None,
            ssl_mode: Some("on".to_string()),
            trust_server_certificate: true,
            ssl_root_cert_path: None,
            ssh_tunnel: None,
            ssh_tunnel_profile_id: None,
        },
    );

    containers::retry_db_operation(Duration::from_secs(60), || -> Result<_, DbError> {
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
fn fetch_batch_requests_column_shape() {
    containers::with_mssql_url(|uri| -> Result<(), DbError> {
        let conn = connect(uri)?;
        let result = conn.execute(&metric_req("mssql.batch_requests_per_sec"))?;

        assert!(!result.columns.is_empty());
        assert_eq!(result.columns[0].kind, ColumnKind::Timestamp);
        assert_eq!(result.columns[1].kind, ColumnKind::Float);
        assert!(!result.rows.is_empty());

        Ok(())
    })
    .unwrap();
}

#[test]
#[ignore = "requires Docker daemon"]
fn fetch_user_connections_metric() {
    containers::with_mssql_url(|uri| -> Result<(), DbError> {
        let conn = connect(uri)?;
        let result = conn.execute(&metric_req("mssql.user_connections"))?;

        assert!(!result.rows.is_empty());

        Ok(())
    })
    .unwrap();
}

#[test]
#[ignore = "requires Docker daemon"]
fn fetch_active_sessions_inspector() {
    containers::with_mssql_url(|uri| -> Result<(), DbError> {
        let conn = connect(uri)?;
        let result = conn.execute(&inspector_req("mssql.active_sessions"))?;

        assert!(!result.columns.is_empty());

        Ok(())
    })
    .unwrap();
}

#[test]
#[ignore = "requires Docker daemon"]
fn all_static_metrics_return_valid_result() {
    containers::with_mssql_url(|uri| -> Result<(), DbError> {
        let conn = connect(uri)?;
        let metrics = dbflux_driver_mssql::instance_catalog::MssqlInstanceCatalog::static_metrics();

        for m in &metrics {
            let result = conn.execute(&metric_req(&m.id));
            assert!(result.is_ok(), "metric {:?} must succeed", m.id);
        }

        Ok(())
    })
    .unwrap();
}
