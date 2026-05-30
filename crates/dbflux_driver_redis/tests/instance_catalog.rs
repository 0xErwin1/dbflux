#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err
)]

use dbflux_core::{ColumnKind, ConnectionProfile, DbConfig, DbDriver, DbError, QueryRequest};
use dbflux_driver_redis::RedisDriver;
use dbflux_test_support::containers;
use std::time::Duration;

fn connect(uri: String) -> Result<Box<dyn dbflux_core::Connection>, DbError> {
    let driver = RedisDriver::new();
    let profile = ConnectionProfile::new(
        "live-redis-catalog",
        DbConfig::Redis {
            use_uri: true,
            uri: Some(uri),
            host: String::new(),
            port: 6379,
            user: None,
            password: None,
            database: None,
            ssl_mode: None,
            ssl_root_cert_path: None,
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
            source: Some(dbflux_core::ExecutionSourceContext::InstanceInspectorQuery {
                metric_id: metric_id.to_string(),
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
#[ignore = "requires Docker daemon"]
fn fetch_connected_clients_metric_column_shape() {
    containers::with_redis_url(|uri| -> Result<(), DbError> {
        let conn = connect(uri)?;
        let result = conn.execute(&metric_req("redis.connected_clients"))?;

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
fn fetch_used_memory_metric() {
    containers::with_redis_url(|uri| -> Result<(), DbError> {
        let conn = connect(uri)?;
        let result = conn.execute(&metric_req("redis.used_memory"))?;

        assert!(!result.rows.is_empty());

        Ok(())
    })
    .unwrap();
}

#[test]
#[ignore = "requires Docker daemon"]
fn fetch_client_list_inspector_redacts_sensitive_fields() {
    containers::with_redis_url(|uri| -> Result<(), DbError> {
        let conn = connect(uri)?;
        let result = conn.execute(&inspector_req("redis.client_list"))?;

        assert!(!result.columns.is_empty());

        // Each row should not expose raw addr values
        for row in &result.rows {
            for value in row {
                if let dbflux_core::Value::Text(text) = value {
                    // The raw IP format looks like "127.0.0.1:PORT"
                    // If a value looks like an IP:port it should be "[redacted]"
                    if text.contains("127.0.0.1:") {
                        assert_eq!(text, "[redacted]", "addr field must be redacted");
                    }
                }
            }
        }

        Ok(())
    })
    .unwrap();
}

#[test]
#[ignore = "requires Docker daemon"]
fn all_static_metrics_return_valid_result() {
    containers::with_redis_url(|uri| -> Result<(), DbError> {
        let conn = connect(uri)?;
        let metrics = dbflux_driver_redis::instance_catalog::RedisInstanceCatalog::static_metrics();

        for m in &metrics {
            let result = conn.execute(&metric_req(&m.id));
            assert!(result.is_ok(), "metric {:?} must succeed", m.id);
        }

        Ok(())
    })
    .unwrap();
}
